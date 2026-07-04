//! `Git2BlobStore` — shared backend that handles type-erased per-id
//! repositories and the low-level blob / commit operations.
//!
//! Deduplicates code between `Git2BlueprintStore` and
//! `Git2EnhanceConfigStore`: each typed store holds one of these
//! internally and delegates `id → repo` management and mechanical blob
//! read/write to it.
//!
//! This layer is type-erased — everything moves as YAML bytes plus
//! commit-message strings. Type-specific serialisation, version
//! computation, and commit-message shape are all owned by the caller.

use super::types::*;
use git2::{Repository, Signature};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Ref name used as the single branch head inside every per-id bare
/// repository.
pub(crate) const PER_REPO_REF: &str = "refs/heads/main";

/// Root manager for per-id repositories. Each id's repository is
/// lazily initialised at `<root>/<id>/.git/`.
pub(crate) struct Git2BlobStore {
    root: PathBuf,
    blob_name: String,
    repos: Mutex<HashMap<String, Arc<Mutex<Repository>>>>,
}

/// Materials pulled off a head commit — YAML bytes, the metadata lines
/// in the commit message, the timestamp, and the commit hash. The
/// caller deserialises the YAML into `T` and parses `commit_msg` to
/// recover the rationale and version.
pub(crate) struct HeadCommit {
    /// Raw YAML bytes of the stored blob (a serialized Blueprint or
    /// similar payload), as a `String`.
    pub yaml: String,
    /// The full commit message, including the metadata lines the
    /// caller parses (rationale, version, epoch, etc.).
    pub commit_msg: String,
    /// Commit timestamp, milliseconds since epoch.
    pub ts_ms: i64,
    /// Hex-encoded commit object id.
    pub commit_hash_hex: String,
}

impl Git2BlobStore {
    /// Open the per-id repo root, creating the directory if needed.
    /// Individual per-id bare repositories are opened lazily on first
    /// access.
    pub fn open_or_init(
        root: impl AsRef<Path>,
        blob_name: impl Into<String>,
    ) -> Result<Self, BlueprintStoreError> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            blob_name: blob_name.into(),
            repos: Mutex::new(HashMap::new()),
        })
    }

    /// The root directory under which each id's `<root>/<id>/.git/` bare
    /// repository lives.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn get_or_open_repo(&self, id: &str) -> Result<Arc<Mutex<Repository>>, BlueprintStoreError> {
        {
            let cache = self.repos.lock().unwrap();
            if let Some(r) = cache.get(id) {
                return Ok(r.clone());
            }
        }
        let repo_path = self.root.join(id);
        std::fs::create_dir_all(&repo_path)?;
        let repo = match Repository::open_bare(&repo_path) {
            Ok(r) => r,
            Err(_) => Repository::init_bare(&repo_path)?,
        };
        let arc = Arc::new(Mutex::new(repo));
        self.repos
            .lock()
            .unwrap()
            .insert(id.to_string(), arc.clone());
        Ok(arc)
    }

    fn try_open_existing(&self, id: &str) -> Option<Arc<Mutex<Repository>>> {
        {
            let cache = self.repos.lock().unwrap();
            if let Some(r) = cache.get(id) {
                return Some(r.clone());
            }
        }
        let repo_path = self.root.join(id);
        if !repo_path.exists() {
            return None;
        }
        let repo = Repository::open_bare(&repo_path).ok()?;
        let arc = Arc::new(Mutex::new(repo));
        self.repos
            .lock()
            .unwrap()
            .insert(id.to_string(), arc.clone());
        Some(arc)
    }

    fn signature(&self) -> Result<Signature<'static>, BlueprintStoreError> {
        Ok(Signature::now("swarm-engine", "engine@local")?)
    }

    /// Pull the blob, commit message, `ts_ms`, and commit hash hex out
    /// of the head commit — skipping archive marker commits
    /// (`archive: true|false`) so the returned `HeadCommit` always
    /// carries the underlying Blueprint content. A missing id returns
    /// `HeadEmpty`.
    pub fn read_head(
        &self,
        id: &str,
        head_empty_id: BlueprintId,
    ) -> Result<HeadCommit, BlueprintStoreError> {
        let repo_arc = self
            .try_open_existing(id)
            .ok_or_else(|| BlueprintStoreError::HeadEmpty(head_empty_id.clone()))?;
        let repo = repo_arc.lock().unwrap();
        let head_oid = repo
            .find_reference(PER_REPO_REF)
            .map_err(|_| BlueprintStoreError::HeadEmpty(head_empty_id.clone()))?
            .peel_to_commit()?
            .id();
        let mut walker = repo.revwalk()?;
        walker.push(head_oid)?;
        for oid_res in walker {
            let oid = oid_res?;
            let commit = repo.find_commit(oid)?;
            let msg = commit.message().unwrap_or("");
            if extract_msg_field(msg, "archive").is_some() {
                continue;
            }
            return self.commit_to_head(&repo, &commit);
        }
        Err(BlueprintStoreError::HeadEmpty(head_empty_id))
    }

    /// Walk backwards from head via `revwalk` and find the older commit
    /// whose message contains `match_line`.
    pub fn find_commit_by_msg(
        &self,
        id: &str,
        match_line: &str,
        head_empty_id: BlueprintId,
    ) -> Result<HeadCommit, BlueprintStoreError> {
        let repo_arc = self
            .try_open_existing(id)
            .ok_or_else(|| BlueprintStoreError::HeadEmpty(head_empty_id.clone()))?;
        let repo = repo_arc.lock().unwrap();
        let head_oid = repo
            .find_reference(PER_REPO_REF)
            .map_err(|_| BlueprintStoreError::HeadEmpty(head_empty_id))?
            .peel_to_commit()?
            .id();
        let mut walker = repo.revwalk()?;
        walker.push(head_oid)?;
        for oid_res in walker {
            let oid = oid_res?;
            let commit = repo.find_commit(oid)?;
            let msg = commit.message().unwrap_or("");
            if msg.contains(match_line) {
                return self.commit_to_head(&repo, &commit);
            }
        }
        Err(BlueprintStoreError::Other(format!(
            "commit not found for match: {}",
            match_line
        )))
    }

    /// Walk from head via `revwalk` and return up to `limit` commit
    /// messages, newest-to-oldest. Missing repositories return an empty
    /// `Vec`.
    pub fn history_msgs(&self, id: &str, limit: usize) -> Result<Vec<String>, BlueprintStoreError> {
        let Some(repo_arc) = self.try_open_existing(id) else {
            return Ok(Vec::new());
        };
        let repo = repo_arc.lock().unwrap();
        let head_oid = match repo.find_reference(PER_REPO_REF) {
            Ok(r) => r.peel_to_commit()?.id(),
            Err(_) => return Ok(Vec::new()),
        };
        let mut walker = repo.revwalk()?;
        walker.push(head_oid)?;
        let mut out = Vec::new();
        for oid_res in walker.take(limit) {
            let oid = oid_res?;
            let commit = repo.find_commit(oid)?;
            out.push(commit.message().unwrap_or("").to_string());
        }
        Ok(out)
    }

    /// Append an archive marker commit — reuses the previous head's
    /// tree so the underlying Blueprint YAML is preserved, and records
    /// `archive: true|false\narchived_at_ms: <ts>` in the commit
    /// message. `archive=true` archives, `archive=false` unarchives.
    ///
    /// Errors if the id has no prior head (nothing to archive).
    pub fn write_archive_marker(
        &self,
        id: &str,
        archive: bool,
        ts_ms: i64,
    ) -> Result<(), BlueprintStoreError> {
        let repo_arc = self
            .try_open_existing(id)
            .ok_or_else(|| BlueprintStoreError::IdNotFound(BlueprintId::new(id.to_string())))?;
        let repo = repo_arc.lock().unwrap();
        let head_ref = repo
            .find_reference(PER_REPO_REF)
            .map_err(|_| BlueprintStoreError::HeadEmpty(BlueprintId::new(id.to_string())))?;
        let parent = head_ref.peel_to_commit()?;
        let tree = parent.tree()?;
        let sig = self.signature()?;
        let msg = format!(
            "blueprint archive marker [{id}]\n\n\
             archive: {archive}\n\
             archived_at_ms: {ts_ms}\n",
            id = id,
            archive = archive,
            ts_ms = ts_ms,
        );
        let _ = repo.commit(Some(PER_REPO_REF), &sig, &sig, &msg, &tree, &[&parent])?;
        Ok(())
    }

    /// Return `true` if the id's current head commit carries
    /// `archive: true` in its message. `archive: false` (unarchive
    /// marker) and messages with no marker both return `false`. A
    /// missing id returns `false`.
    pub fn is_archived(&self, id: &str) -> Result<bool, BlueprintStoreError> {
        let Some(repo_arc) = self.try_open_existing(id) else {
            return Ok(false);
        };
        let repo = repo_arc.lock().unwrap();
        let head = match repo.find_reference(PER_REPO_REF) {
            Ok(r) => r.peel_to_commit()?,
            Err(_) => return Ok(false),
        };
        let msg = head.message().unwrap_or("");
        Ok(extract_msg_field(msg, "archive").as_deref() == Some("true"))
    }

    /// Append one commit from a blob (YAML bytes) plus a commit
    /// message, advancing the head ref. Non-blocking: attempts to
    /// acquire the per-id repository lock via `Mutex::try_lock` and
    /// returns [`BlueprintStoreError::LockBusy`] when another writer
    /// holds the lock (the HTTP layer maps this to `429 Too Many
    /// Requests`).
    pub fn try_write_blob_commit(
        &self,
        id: &str,
        yaml: &str,
        commit_msg: &str,
    ) -> Result<(), BlueprintStoreError> {
        let repo_arc = self.get_or_open_repo(id)?;
        let repo = repo_arc
            .try_lock()
            .map_err(|_| BlueprintStoreError::LockBusy)?;

        let blob_oid = repo.blob(yaml.as_bytes())?;
        let mut tb = repo.treebuilder(None)?;
        tb.insert(&self.blob_name, blob_oid, 0o100644)?;
        let tree_oid = tb.write()?;
        let tree = repo.find_tree(tree_oid)?;

        let parent_commits: Vec<git2::Commit> = match repo.find_reference(PER_REPO_REF) {
            Ok(r) => vec![r.peel_to_commit()?],
            Err(_) => Vec::new(),
        };
        let parent_refs: Vec<&git2::Commit> = parent_commits.iter().collect();

        let sig = self.signature()?;
        let _ = repo.commit(
            Some(PER_REPO_REF),
            &sig,
            &sig,
            commit_msg,
            &tree,
            &parent_refs,
        )?;
        Ok(())
    }

    /// Scan the root's subdirectories and return the ids of every bare
    /// repository (a subdir with a `HEAD` file present). When
    /// `include_archived` is `false`, ids whose current head is an
    /// archive marker (`archive: true`) are filtered out.
    pub fn list_ids(&self, include_archived: bool) -> Result<Vec<String>, BlueprintStoreError> {
        let mut out = Vec::new();
        let read_dir = match std::fs::read_dir(&self.root) {
            Ok(r) => r,
            Err(_) => return Ok(out),
        };
        for entry in read_dir.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if !path.join("HEAD").exists() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if !include_archived && self.is_archived(name).unwrap_or(false) {
                continue;
            }
            out.push(name.to_string());
        }
        Ok(out)
    }

    fn commit_to_head(
        &self,
        repo: &Repository,
        commit: &git2::Commit,
    ) -> Result<HeadCommit, BlueprintStoreError> {
        let tree = commit.tree()?;
        let entry = tree.get_name(&self.blob_name).ok_or_else(|| {
            BlueprintStoreError::Other(format!("{} not found in tree", self.blob_name))
        })?;
        let blob = repo.find_blob(entry.id())?;
        let yaml = std::str::from_utf8(blob.content())
            .map_err(|e| BlueprintStoreError::Other(format!("blob utf8: {}", e)))?
            .to_string();
        Ok(HeadCommit {
            yaml,
            commit_msg: commit.message().unwrap_or("").to_string(),
            ts_ms: commit.time().seconds() * 1000,
            commit_hash_hex: commit.id().to_string(),
        })
    }
}

/// Helper used by every typed store: pull the value on a `key:` line
/// out of a commit message.
pub(crate) fn extract_msg_field(msg: &str, key: &str) -> Option<String> {
    let needle = format!("{}:", key);
    for line in msg.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix(&needle) {
            let v = rest.trim();
            if v.is_empty() {
                return None;
            }
            return Some(v.to_string());
        }
    }
    None
}
