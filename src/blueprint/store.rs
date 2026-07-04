//! `BlueprintStore` — the Blueprint VCS abstraction.
//!
//! The interface (the `BlueprintStore` trait) does not leak Git concepts
//! (`commit` / `tree` / `refs`). `BlueprintVersion` is abstracted as a
//! `ContentHash` (blake3), leaving room to pick between Git2, InMemory,
//! and future File / Remote / Lua backends.
//!
//! Current scope:
//! - `Git2BlueprintStore` (`Layout::Single`).
//! - `InMemoryBlueprintStore` (for tests).
//! - Canonical form = `serde_yaml::to_string` — the definitive form.
//! - `ContentHash` = `blake3(canonical bytes)`.

use crate::blueprint::Blueprint;
use async_trait::async_trait;

pub mod types;

pub use types::{
    BlueprintEpoch, BlueprintId, BlueprintStoreError, BlueprintVersion, CommitMetadata,
    ContentHash, Trace, TraceOrigin, TraceRef, Traced,
};

pub mod git2_store;
pub mod inmemory;

pub use git2_store::Git2BlueprintStore;
pub use inmemory::InMemoryBlueprintStore;
pub(crate) mod git2_blob_store;

// ──────────────────────────────────────────────────────────────────────────
// BlueprintStore trait
// ──────────────────────────────────────────────────────────────────────────

/// The Blueprint-VCS abstract interface. Backed by Git2, InMemory,
/// Remote, and so on.
///
/// # Design principles
///
/// - Do not leak Git concepts (`commit` / `tree` / `refs`) into the
///   interface.
/// - Abstract versions as `BlueprintVersion` = `ContentHash` (blake3).
/// - Keep the surface async, matching the engine's existing traits.
#[async_trait]
pub trait BlueprintStore: Send + Sync {
    /// Backend identifier, for logging / diagnostics (e.g. `"git2"`,
    /// `"in-memory"`).
    fn name(&self) -> &str;

    /// Read the current head — the latest commit for this `BlueprintId`.
    async fn read_head(&self, id: &BlueprintId) -> Result<Traced<Blueprint>, BlueprintStoreError>;

    /// Append a new Blueprint. Computes the `ContentHash` and returns the
    /// resulting `BlueprintVersion`.
    async fn write_new(
        &self,
        id: &BlueprintId,
        new_bp: &Blueprint,
        parents: &[BlueprintVersion],
        metadata: CommitMetadata,
    ) -> Result<BlueprintVersion, BlueprintStoreError>;

    /// Look up a past version — used for audit or debug.
    async fn read_version(
        &self,
        id: &BlueprintId,
        version: BlueprintVersion,
    ) -> Result<Traced<Blueprint>, BlueprintStoreError>;

    /// List history newest-to-oldest, up to `limit`; head included.
    async fn history(
        &self,
        id: &BlueprintId,
        limit: usize,
    ) -> Result<Vec<BlueprintVersion>, BlueprintStoreError>;

    /// Return the rationale attached to a commit (its
    /// `CommitMetadata.rationale`, a one-line changelog). Backends that
    /// cannot recover it return `Ok(None)`; the trait's default
    /// implementation returns `None`.
    async fn read_commit_rationale(
        &self,
        _id: &BlueprintId,
        _version: BlueprintVersion,
    ) -> Result<Option<String>, BlueprintStoreError> {
        Ok(None)
    }

    /// List every `BlueprintId` (relevant when the `Layout::Multi` axis
    /// is in use).
    async fn list_ids(&self) -> Result<Vec<BlueprintId>, BlueprintStoreError>;

    /// Archive a `BlueprintId` — logical soft-delete via an archive
    /// marker commit. After archive, [`read_head`] returns
    /// [`BlueprintStoreError::Archived`], [`list_ids`] filters the id
    /// out by default, and downstream resolvers (e.g. `swarm_run(id)`)
    /// hard-reject with the same error.
    ///
    /// Restoring is symmetric — [`unarchive_id`] appends an unarchive
    /// marker commit and re-exposes the id. History is preserved end-to-
    /// end; nothing is physically removed.
    ///
    /// Default implementation returns
    /// [`BlueprintStoreError::Unsupported`]; only backends that support
    /// archive semantics (Git2) override.
    ///
    /// [`read_head`]: BlueprintStore::read_head
    /// [`list_ids`]: BlueprintStore::list_ids
    /// [`unarchive_id`]: BlueprintStore::unarchive_id
    async fn archive_id(&self, _id: &BlueprintId) -> Result<(), BlueprintStoreError> {
        Err(BlueprintStoreError::Unsupported(
            "archive_id is not supported by this backend".into(),
        ))
    }

    /// Reverse of [`archive_id`] — append an unarchive marker commit to
    /// the main head and re-expose the id.
    ///
    /// [`archive_id`]: BlueprintStore::archive_id
    async fn unarchive_id(&self, _id: &BlueprintId) -> Result<(), BlueprintStoreError> {
        Err(BlueprintStoreError::Unsupported(
            "unarchive_id is not supported by this backend".into(),
        ))
    }

    /// Return `true` if the id is currently archived. Default returns
    /// `Ok(false)` for backends that never archive.
    async fn is_archived(&self, _id: &BlueprintId) -> Result<bool, BlueprintStoreError> {
        Ok(false)
    }
}

// ──────────────────────────────────────────────────────────────────────────
// canonical helpers (serde_yaml::to_string Definitive form)
// ──────────────────────────────────────────────────────────────────────────

/// Serialise a Blueprint to canonical YAML bytes — the definitive form.
/// The same output feeds both the `ContentHash` computation and the Git
/// commit blob.
pub fn canonical_yaml(bp: &Blueprint) -> Result<String, BlueprintStoreError> {
    Ok(serde_yaml::to_string(bp)?)
}

/// Compute the Blueprint's `ContentHash` — `blake3` of the canonical
/// YAML bytes.
pub fn blueprint_content_hash(bp: &Blueprint) -> Result<ContentHash, BlueprintStoreError> {
    let yaml = canonical_yaml(bp)?;
    Ok(ContentHash::from_bytes(yaml.as_bytes()))
}

/// Compute the Blueprint's `BlueprintVersion` — a newtype wrapper around
/// `ContentHash`.
pub fn blueprint_version(bp: &Blueprint) -> Result<BlueprintVersion, BlueprintStoreError> {
    Ok(BlueprintVersion(blueprint_content_hash(bp)?))
}
