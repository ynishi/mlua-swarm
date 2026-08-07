//! `OperatorSessionStore` — persistence for Operator login-flow sessions.
//!
//! One row per minted `POST /v1/operators` session: the sid / bearer token /
//! role aliases / capability manifest / mint time. This is the record that
//! lets a single-server restart keep every logged-in Operator logged in —
//! the sibling stores (task / run / replay / trace / …) already persist,
//! and `RunRecord.operator_sid` persists a *pointer* into this session
//! space, so leaving the sessions themselves process-volatile stranded
//! every restored run pin on a `404 unknown sid` after restart.
//!
//! Deliberately **not** persisted: the WS adapter state (`tx` sender,
//! `pending` oneshot map). Both are process-lifetime objects with no
//! meaningful serialized form — an empty rebuild on the client's next WS
//! connect (the existing reconnect path) is the correct restoration.
//!
//! Current scope:
//!
//! - [`InMemoryOperatorSessionStore`] — process-volatile default.
//! - [`SqliteOperatorSessionStore`] — file-backed persistence via
//!   `rusqlite-isle` (same shape as [`crate::store::task::SqliteTaskStore`]).

use crate::types::{OperatorRef, SessionId};
use crate::AgentProviderManifest;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

pub mod inmemory;
pub mod sqlite;
pub use inmemory::InMemoryOperatorSessionStore;
pub use sqlite::SqliteOperatorSessionStore;

// ──────────────────────────────────────────────────────────────────────────
// OperatorSessionRecord
// ──────────────────────────────────────────────────────────────────────────

/// One persisted Operator login-flow session.
///
/// Field-for-field the durable subset of the server's
/// `OperatorSessionEntry` — everything except the process-lifetime WS
/// adapter state (`ws_session`), which is rebuilt empty on reconnect.
///
/// # The bearer token is never stored
///
/// [`token_digest`](Self::token_digest) holds
/// `hex(SHA-256(bearer))` — the same fingerprint shape the `/v1/sessions`
/// path already keys its store by, for the same reason ("the sid handed to
/// the client is the token nonce itself (a bearer secret), so the server
/// never uses it as a map key"; see `mse_server::SessionStore`). Every
/// consumer of this record only ever *compares* a presented bearer
/// ([`verify_bearer`](Self::verify_bearer)), so nothing downstream needs
/// the plaintext — it exists only inside `POST /v1/operators`, between
/// minting and the mint response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorSessionRecord {
    /// Server-minted session id (`S-<hex>`).
    pub sid: SessionId,
    /// `hex(SHA-256(bearer))` of the auth token required on the WS upgrade
    /// and admin routes. Derive with [`Self::digest_of`]; compare with
    /// [`Self::verify_bearer`]. The plaintext bearer is deliberately absent
    /// (see the type doc).
    pub token_digest: String,
    /// Role aliases claimed exclusively by this session. Each element
    /// serializes as the plain role string it always was — the wire and
    /// at-rest forms are unchanged by the [`OperatorRef`] typing.
    pub roles: Vec<OperatorRef>,
    /// Provider-owned effective capability manifest submitted at join.
    pub capability_manifest: Option<AgentProviderManifest>,
    /// Unix epoch seconds when `POST /v1/operators` minted this session.
    pub joined_at_secs: u64,
}

impl OperatorSessionRecord {
    /// Digest a plaintext bearer into the at-rest shape
    /// ([`Self::token_digest`]).
    ///
    /// Callers mint a bearer with
    /// [`operator_bearer_token`](crate::types::operator_bearer_token) and
    /// keep the plaintext only long enough to answer the mint request.
    pub fn digest_of(bearer: &str) -> String {
        crate::types::token_fingerprint(bearer)
    }

    /// Constant-time check of a presented bearer against
    /// [`Self::token_digest`].
    ///
    /// The comparison runs over the two digests (fixed-width hex), so it
    /// carries no timing signal about the bearer itself.
    pub fn verify_bearer(&self, bearer: &str) -> bool {
        crate::types::ct_eq(
            self.token_digest.as_bytes(),
            Self::digest_of(bearer).as_bytes(),
        )
    }
}

/// Errors surfaced by an [`OperatorSessionStore`] implementation.
#[derive(Debug, Error)]
pub enum OperatorSessionStoreError {
    /// No session exists for the given sid.
    #[error("operator session not found: {0}")]
    NotFound(SessionId),

    /// Backend-specific failure not covered by the other variants.
    #[error("other: {0}")]
    Other(String),
}

// ──────────────────────────────────────────────────────────────────────────
// OperatorSessionStore trait
// ──────────────────────────────────────────────────────────────────────────

/// Persistence interface for Operator login-flow sessions.
///
/// Write-through contract on the server side: `POST /v1/operators` calls
/// [`put`](Self::put) before answering the mint, teardown (`DELETE
/// /v1/operators/:sid` / `by-role`) calls [`delete`](Self::delete), and a
/// fresh boot calls [`list`](Self::list) once to rehydrate its in-memory
/// session map.
#[async_trait]
pub trait OperatorSessionStore: Send + Sync {
    /// Backend name — for diagnostics/logging.
    fn name(&self) -> &str;

    /// Insert or replace the row for `record.sid`. Upsert semantics: sids
    /// are freshly minted so a same-sid overwrite only happens on a
    /// deliberate re-put of the same session.
    async fn put(&self, record: OperatorSessionRecord) -> Result<(), OperatorSessionStoreError>;

    /// Delete the row for `sid`. `NotFound` when no such row exists.
    async fn delete(&self, sid: &SessionId) -> Result<(), OperatorSessionStoreError>;

    /// List every persisted session, ascending by `joined_at_secs` (mint
    /// order, stable for deterministic rehydration).
    async fn list(&self) -> Result<Vec<OperatorSessionRecord>, OperatorSessionStoreError>;
}

// ──────────────────────────────────────────────────────────────────────────
// Shared inner state used by the InMemory backend.
// ──────────────────────────────────────────────────────────────────────────

#[derive(Default)]
pub(crate) struct Inner {
    /// Insertion order — used as a stable tie-break under `list()`.
    pub(crate) order: Vec<SessionId>,
    pub(crate) records: HashMap<SessionId, OperatorSessionRecord>,
}

pub(crate) type SharedInner = Mutex<Inner>;
