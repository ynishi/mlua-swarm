//! `IssueStore` — the persistence abstraction for enhance requests (Issues).
//!
//! Same layer as `BPStore`. Dedicated to CRUD and status lookup on
//! Issues. The old `enhance::issue::IssueSource`'s acquire/release
//! queue semantics are gone — the shape now has `EnhancePP` fetch
//! directly.
//!
//! Current scope:
//!
//! - `InMemoryIssueStore` — process-volatile; noted as a carry.
//! - Persistent backends (SQLite / Git / mini-app / …) are future carries.

use crate::blueprint::store::BlueprintId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

pub mod inmemory;
pub use inmemory::InMemoryIssueStore;

// ──────────────────────────────────────────────────────────────────────────
// IssueId / IssuePayload / IssueStatus
// ──────────────────────────────────────────────────────────────────────────

/// Issue identifier — the human-facing id for an enhance request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IssueId(pub String);

impl IssueId {
    /// Wrap an arbitrary string as an id.
    pub fn new<S: Into<String>>(s: S) -> Self {
        Self(s.into())
    }

    /// Borrow the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for IssueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The unit of work the Enhance loop processes — a request that
/// says "please modify Blueprint X".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuePayload {
    /// Issue identifier.
    pub issue_id: IssueId,
    /// The Blueprint to be modified.
    pub blueprint_id: BlueprintId,
    /// Modification intent / context — the natural-language prompt
    /// passed on to the `PatchSpawner`.
    pub intent: String,
}

/// Lifecycle state of an Issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueStatus {
    /// Submitted, not yet processed.
    Pending,
    /// In progress.
    InFlight,
    /// Complete — patch applied. Carries the new BP commit id.
    Applied {
        /// The new Blueprint commit id after the patch was applied.
        new_version: String,
    },
    /// Rejected. Carries a reason.
    Rejected {
        /// Why the Issue was rejected.
        reason: String,
    },
}

/// Errors surfaced by an [`IssueStore`] implementation.
#[derive(Debug, Error)]
pub enum IssueStoreError {
    /// No Issue exists for the given id.
    #[error("issue not found: {0}")]
    NotFound(IssueId),

    /// `create` was called with an id that is already stored.
    #[error("issue already exists: {0}")]
    Duplicate(IssueId),

    /// Backend-specific failure not covered by the other variants.
    #[error("other: {0}")]
    Other(String),
}

// ──────────────────────────────────────────────────────────────────────────
// IssueStore trait
// ──────────────────────────────────────────────────────────────────────────

/// Persistence interface for Issues — same layer as `BPStore`.
#[async_trait]
pub trait IssueStore: Send + Sync {
    /// Backend name — for diagnostics/logging.
    fn name(&self) -> &str;

    /// Submit a new Issue with `status = Pending`.
    async fn create(&self, payload: IssuePayload) -> Result<(), IssueStoreError>;

    /// Fetch the Issue body.
    async fn get(&self, id: &IssueId) -> Result<IssuePayload, IssueStoreError>;

    /// Fetch the Issue's status; returns `NotFound` when absent.
    async fn status(&self, id: &IssueId) -> Result<IssueStatus, IssueStoreError>;

    /// List every Issue in insertion order — for audit and debug.
    async fn list(&self) -> Result<Vec<(IssueId, IssueStatus)>, IssueStoreError>;

    /// Pop one pending Issue (FIFO) — used by `EnhancePP` for
    /// dispatch. Transitions the status to `InFlight` on pop. Returns
    /// `Ok(None)` when there is no work.
    async fn pop_pending(&self) -> Result<Option<IssuePayload>, IssueStoreError>;

    /// Update an Issue's status — the terminal transitions to
    /// `Applied` / `Rejected` and so on.
    async fn update_status(&self, id: &IssueId, status: IssueStatus)
        -> Result<(), IssueStoreError>;
}

// ──────────────────────────────────────────────────────────────────────────
// Shared inner state used by the InMemory backend.
// ──────────────────────────────────────────────────────────────────────────

#[derive(Default)]
pub(crate) struct Inner {
    /// Insertion order — audit / list use.
    pub(crate) order: Vec<IssueId>,
    pub(crate) payloads: HashMap<IssueId, IssuePayload>,
    pub(crate) statuses: HashMap<IssueId, IssueStatus>,
    /// Pending FIFO queue.
    pub(crate) pending: std::collections::VecDeque<IssueId>,
}

pub(crate) type SharedInner = Mutex<Inner>;
