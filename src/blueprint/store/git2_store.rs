//! `Git2BlueprintStore` — the git2-rs backend, one repo per id.
//!
//! Internally holds a `Git2BlobStore` (a type-erased per-id repo
//! manager). This file itself only owns the Blueprint-specific bits:
//! commit-message format, canonical YAML, and version computation. On
//! disk the layout is `<root>/<id>/.git/` per-id bare repos, chosen so
//! that GC, backup, and migration units line up cleanly.

use super::git2_blob_store::{extract_msg_field, Git2BlobStore, HeadCommit};
use super::types::*;
use super::{blueprint_version, canonical_yaml, BlueprintStore};
use crate::blueprint::Blueprint;
use async_trait::async_trait;
use std::path::Path;

/// Git2-backed `BlueprintStore` — one bare repo per `BlueprintId` under
/// `<root>/<id>/.git/`, delegating the mechanical blob/commit work to a
/// shared `Git2BlobStore`.
pub struct Git2BlueprintStore {
    backend: Git2BlobStore,
}

impl Git2BlueprintStore {
    /// Open (or create) the store rooted at `root`. Per-id repositories
    /// are created lazily on first write/read.
    pub fn open_or_init(root: impl AsRef<Path>) -> Result<Self, BlueprintStoreError> {
        Ok(Self {
            backend: Git2BlobStore::open_or_init(root, "blueprint.yaml")?,
        })
    }

    /// The root directory this store was opened with.
    pub fn root(&self) -> &Path {
        self.backend.root()
    }
}

fn build_commit_msg(
    id: &BlueprintId,
    version: BlueprintVersion,
    metadata: &CommitMetadata,
) -> String {
    format!(
        "blueprint update [{id}]\n\n\
         blueprint_content_hash: {hash}\n\
         patch_content_hash:     {patch}\n\
         epoch_blueprint_id:     {epoch_id}\n\
         epoch_start_version:    {epoch_v}\n\
         epoch_started_at_ms:    {epoch_ts}\n\
         rationale:              {rationale}\n",
        id = id,
        hash = version,
        patch = metadata.patch_hash,
        epoch_id = metadata.epoch_id.blueprint_id,
        epoch_v = metadata.epoch_id.start_version,
        epoch_ts = metadata.epoch_id.started_at_ms,
        rationale = metadata.rationale,
    )
}

fn head_to_traced(
    id: &BlueprintId,
    head: HeadCommit,
) -> Result<Traced<Blueprint>, BlueprintStoreError> {
    let bp: Blueprint = serde_yaml::from_str(&head.yaml)?;
    let version = parse_version(&head.commit_msg)
        .ok_or_else(|| BlueprintStoreError::Other("version not found in commit msg".to_string()))?;
    let _ = id;
    let trace = Trace::new(
        TraceOrigin::Git {
            commit_hash: head.commit_hash_hex,
        },
        version,
        head.ts_ms,
    );
    Ok(Traced::new(bp, trace))
}

fn parse_version(msg: &str) -> Option<BlueprintVersion> {
    extract_msg_field(msg, "blueprint_content_hash")
        .and_then(|hex| ContentHash::from_hex(&hex).ok())
        .map(BlueprintVersion)
}

#[async_trait]
impl BlueprintStore for Git2BlueprintStore {
    fn name(&self) -> &str {
        "git2"
    }

    async fn read_head(&self, id: &BlueprintId) -> Result<Traced<Blueprint>, BlueprintStoreError> {
        if self.backend.is_archived(id.as_str())? {
            return Err(BlueprintStoreError::Archived(id.clone()));
        }
        let head = self.backend.read_head(id.as_str(), id.clone())?;
        head_to_traced(id, head)
    }

    async fn write_new(
        &self,
        id: &BlueprintId,
        new_bp: &Blueprint,
        parents: &[BlueprintVersion],
        metadata: CommitMetadata,
    ) -> Result<BlueprintVersion, BlueprintStoreError> {
        if self.backend.is_archived(id.as_str())? {
            return Err(BlueprintStoreError::Archived(id.clone()));
        }
        let yaml = canonical_yaml(new_bp)?;
        let version = blueprint_version(new_bp)?;
        let msg = build_commit_msg(id, version, &metadata);
        self.backend
            .try_write_blob_commit(id.as_str(), &yaml, &msg)?;
        let _ = parents;
        Ok(version)
    }

    async fn read_version(
        &self,
        id: &BlueprintId,
        version: BlueprintVersion,
    ) -> Result<Traced<Blueprint>, BlueprintStoreError> {
        let match_line = format!("blueprint_content_hash: {}", version.to_hex());
        let head = self
            .backend
            .find_commit_by_msg(id.as_str(), &match_line, id.clone())
            .map_err(|e| match e {
                BlueprintStoreError::Other(_) => BlueprintStoreError::NotFound {
                    id: id.clone(),
                    version,
                },
                other => other,
            })?;
        head_to_traced(id, head)
    }

    async fn history(
        &self,
        id: &BlueprintId,
        limit: usize,
    ) -> Result<Vec<BlueprintVersion>, BlueprintStoreError> {
        let msgs = self.backend.history_msgs(id.as_str(), limit)?;
        Ok(msgs.iter().filter_map(|m| parse_version(m)).collect())
    }

    async fn read_commit_rationale(
        &self,
        id: &BlueprintId,
        version: BlueprintVersion,
    ) -> Result<Option<String>, BlueprintStoreError> {
        let match_line = format!("blueprint_content_hash: {}", version.to_hex());
        match self
            .backend
            .find_commit_by_msg(id.as_str(), &match_line, id.clone())
        {
            Ok(head) => Ok(extract_msg_field(&head.commit_msg, "rationale")),
            Err(BlueprintStoreError::HeadEmpty(_)) | Err(BlueprintStoreError::Other(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn list_ids(&self) -> Result<Vec<BlueprintId>, BlueprintStoreError> {
        Ok(self
            .backend
            .list_ids(false)?
            .into_iter()
            .map(BlueprintId::new)
            .collect())
    }

    async fn archive_id(&self, id: &BlueprintId) -> Result<(), BlueprintStoreError> {
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.backend.write_archive_marker(id.as_str(), true, ts_ms)
    }

    async fn unarchive_id(&self, id: &BlueprintId) -> Result<(), BlueprintStoreError> {
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.backend.write_archive_marker(id.as_str(), false, ts_ms)
    }

    async fn is_archived(&self, id: &BlueprintId) -> Result<bool, BlueprintStoreError> {
        self.backend.is_archived(id.as_str())
    }
}
