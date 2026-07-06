//! Type definitions for `BlueprintStore`.
//!
//! - `ContentHash` (blake3, 32 bytes; the axis `BlueprintVersion` is
//!   derived from).
//! - `BlueprintVersion` — a newtype wrapping `ContentHash` to keep the
//!   interface stable.
//! - `BlueprintId` — a human-facing id newtype, default `"main"`.
//! - `Trace` / `Traced<T>` / `TraceRef` / `TraceOrigin`.
//! - `CommitMetadata` — the `write_new` argument.
//! - `BlueprintEpoch` — a session-collision guard, one per Enhance-loop
//!   origin.
//! - `BlueprintStoreError`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

// ──────────────────────────────────────────────────────────────────────────
// ContentHash (blake3 32 bytes)
// ──────────────────────────────────────────────────────────────────────────

/// 32-byte blake3 hash over the canonical bytes — used as the content
/// fingerprint for Blueprints, patches, and similar payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContentHash(
    /// The raw 32-byte digest, hex-encoded on the wire (serde).
    #[serde(with = "hex_bytes32")]
    pub [u8; 32],
);

impl ContentHash {
    /// Hash canonical bytes with blake3.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let h = blake3::hash(bytes);
        ContentHash(*h.as_bytes())
    }

    /// Lowercase hex encoding of the 32 bytes.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse a 32-byte hex string back into a `ContentHash`. Errors if
    /// the string is not valid hex or does not decode to exactly 32
    /// bytes.
    pub fn from_hex(s: &str) -> Result<Self, hex::FromHexError> {
        let v = hex::decode(s)?;
        if v.len() != 32 {
            return Err(hex::FromHexError::InvalidStringLength);
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&v);
        Ok(ContentHash(arr))
    }
}

impl std::fmt::Display for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

mod hex_bytes32 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        let s = String::deserialize(d)?;
        let v = hex::decode(&s).map_err(serde::de::Error::custom)?;
        if v.len() != 32 {
            return Err(serde::de::Error::custom("expected 32-byte hex"));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&v);
        Ok(arr)
    }
}

// ──────────────────────────────────────────────────────────────────────────
// BlueprintVersion (= ContentHash newtype)
// ──────────────────────────────────────────────────────────────────────────

/// Identifier for one generation of a Blueprint. Equivalent to
/// `ContentHash`, but kept as a newtype for interface stability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlueprintVersion(
    /// The underlying content hash.
    pub ContentHash,
);

impl BlueprintVersion {
    /// Hash `bytes` with blake3 and wrap the result as a version.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        BlueprintVersion(ContentHash::from_bytes(bytes))
    }

    /// Lowercase hex encoding of the underlying hash.
    pub fn to_hex(&self) -> String {
        self.0.to_hex()
    }
}

impl std::fmt::Display for BlueprintVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ──────────────────────────────────────────────────────────────────────────
// BlueprintId (human-facing ID newtype)
// ──────────────────────────────────────────────────────────────────────────

// The type converged onto the schema crate in issue #14 — `Blueprint.id`
// and the store layer used to carry two representations of the same
// concept (plain `String` vs a store-local newtype). It is re-exported
// here so every existing `blueprint::store::types::BlueprintId` path
// keeps working.
pub use mlua_swarm_schema::BlueprintId;

// ──────────────────────────────────────────────────────────────────────────
// Trace / Traced<T> / TraceRef / TraceOrigin
// ──────────────────────────────────────────────────────────────────────────

/// Where a value came from — three variants for now: File / Inline / Git.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TraceOrigin {
    /// Built inline (test code, engine-startup defaults, etc.).
    Inline,
    /// Loaded from a file.
    File {
        /// The source file path.
        path: String,
    },
    /// Loaded from the internal Git store (carries a commit hash).
    Git {
        /// The commit hash the value was read from.
        commit_hash: String,
    },
}

/// Lightweight reference to a parent `Trace` — a flat lineage view; the
/// full `Trace` is not expanded inline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceRef {
    /// Where the referenced parent came from.
    pub origin: TraceOrigin,
    /// The parent's content hash.
    pub hash: ContentHash,
}

/// A value's lineage / version / origin metadata. Parents are kept to
/// 0-1 levels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace {
    /// Where this value came from (inline / file / git).
    pub origin: TraceOrigin,
    /// This value's own content-hash-derived version.
    pub version: BlueprintVersion,
    /// Wall-clock timestamp (milliseconds since epoch) when the trace
    /// was recorded.
    pub ts_ms: i64,
    /// Flat references to parent traces (0-1 levels deep; see the type
    /// doc).
    #[serde(default)]
    pub parents: Vec<TraceRef>,
}

impl Trace {
    /// Build a `Trace` with no parents.
    pub fn new(origin: TraceOrigin, version: BlueprintVersion, ts_ms: i64) -> Self {
        Trace {
            origin,
            version,
            ts_ms,
            parents: Vec::new(),
        }
    }

    /// Attach parent lineage references.
    pub fn with_parents(mut self, parents: Vec<TraceRef>) -> Self {
        self.parents = parents;
        self
    }

    /// Downgrade this `Trace` into a lightweight `TraceRef` (origin +
    /// hash only), suitable for use as a parent reference.
    pub fn as_ref(&self) -> TraceRef {
        TraceRef {
            origin: self.origin.clone(),
            hash: self.version.0,
        }
    }
}

/// Wrapper that carries a `Trace` alongside its value. Everything that
/// crosses an external-input boundary lives inside a `Traced<T>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Traced<T> {
    /// The wrapped value (typically a `Blueprint`).
    pub value: T,
    /// The value's lineage / version / origin metadata.
    pub trace: Trace,
}

impl<T> Traced<T> {
    /// Pair a value with its trace.
    pub fn new(value: T, trace: Trace) -> Self {
        Traced { value, trace }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// BlueprintEpoch (session-collision guard, one per Enhance-loop origin)
// ──────────────────────────────────────────────────────────────────────────

/// The origin generation of an Enhance loop. Fixes
/// `(BlueprintId, start_version, ts)` as the audit axis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlueprintEpoch {
    /// The Blueprint series this Enhance loop is operating on.
    pub blueprint_id: BlueprintId,
    /// The version the loop started from.
    pub start_version: BlueprintVersion,
    /// Wall-clock timestamp (milliseconds since epoch) when the loop
    /// started.
    pub started_at_ms: i64,
}

impl BlueprintEpoch {
    /// Fix the `(BlueprintId, start_version, ts)` triple for one
    /// Enhance-loop origin.
    pub fn new(
        blueprint_id: BlueprintId,
        start_version: BlueprintVersion,
        started_at_ms: i64,
    ) -> Self {
        Self {
            blueprint_id,
            start_version,
            started_at_ms,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// CommitMetadata (the `write_new` argument)
// ──────────────────────────────────────────────────────────────────────────

/// Metadata attached to a Blueprint commit — the audit axis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitMetadata {
    /// The Enhance-loop origin this commit belongs to.
    pub epoch_id: BlueprintEpoch,
    /// One-line changelog entry for this commit.
    pub rationale: String,
    /// Content hash of the patch that produced this commit (distinct
    /// from the resulting Blueprint's own `ContentHash`).
    pub patch_hash: ContentHash,
}

impl CommitMetadata {
    /// Placeholder for the first seed commit — no epoch, rationale is
    /// `"seed"`.
    pub fn seed(blueprint_id: BlueprintId, version: BlueprintVersion, ts_ms: i64) -> Self {
        CommitMetadata {
            epoch_id: BlueprintEpoch::new(blueprint_id, version, ts_ms),
            rationale: "seed".to_string(),
            patch_hash: ContentHash([0u8; 32]),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// BlueprintStoreError
// ──────────────────────────────────────────────────────────────────────────

/// Everything that can go wrong in a `BlueprintStore` implementation.
#[derive(Debug, Error)]
pub enum BlueprintStoreError {
    /// A specific `(id, version)` pair has no matching commit.
    #[error("blueprint not found: id={id} version={version}")]
    NotFound {
        /// The series that was looked up.
        id: BlueprintId,
        /// The version that was looked up.
        version: BlueprintVersion,
    },

    /// The `BlueprintId` itself is unknown to the backend.
    #[error("blueprint id not found: {0}")]
    IdNotFound(BlueprintId),

    /// The id exists but has no head commit yet (nothing written).
    #[error("head ref empty: id={0}")]
    HeadEmpty(BlueprintId),

    /// Serialising the Blueprint to canonical YAML failed.
    #[error("canonical serialize failed: {0}")]
    Canonical(#[from] serde_yaml::Error),

    /// The underlying `git2` operation failed.
    #[error("git2 backend failed: {0}")]
    Git2(#[from] git2::Error),

    /// A filesystem operation failed.
    #[error("io failed: {0}")]
    Io(#[from] std::io::Error),

    /// A computed content hash did not match the expected one.
    #[error("hash mismatch: expected={expected}, actual={actual}")]
    HashMismatch {
        /// The hash that was expected.
        expected: BlueprintVersion,
        /// The hash that was actually computed.
        actual: BlueprintVersion,
    },

    /// The requested operation doesn't make sense for the store's
    /// current `Layout` (e.g. multi-id operations on a `Layout::Single`
    /// backend).
    #[error("invalid layout for operation: {0}")]
    InvalidLayout(String),

    /// The id is archived; callers must `unarchive_id` before using it.
    #[error("blueprint is archived: id={0}")]
    Archived(BlueprintId),

    /// Another writer currently holds the per-id write lock (non-blocking
    /// `try_lock` failed). Callers should retry; the HTTP layer maps this
    /// to `429 Too Many Requests`.
    #[error("blueprint store: per-id write lock busy")]
    LockBusy,

    /// The backend does not implement this operation (see the trait's
    /// default method bodies, e.g. `archive_id` / `unarchive_id`).
    #[error("operation not supported by this backend: {0}")]
    Unsupported(String),

    /// Catch-all for backend-specific failures that don't fit another
    /// variant.
    #[error("other: {0}")]
    Other(String),
}
