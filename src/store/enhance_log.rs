//! `EnhanceLogStore` — append-only log of one `LogEntry` per Enhance-flow
//! invocation.
//!
//! Used to preserve, for later inspection and dogfooding, "which issue
//! patched which blueprint with which ops, what the four verifier axes
//! decided, and what the rationale was".
//!
//! Design:
//!
//! - **Append-only K-V.** Appending the same `issue_id` twice returns a
//!   `Conflict`; the existing entry is immutable.
//! - List by `blueprint_id` — the historical enhance trace for one
//!   Blueprint.
//! - Get by `issue_id` — the record for a single issue.
//! - Every entry carries a timestamp (ms epoch); lists come back sorted
//!   ascending by timestamp.
//!
//! Persistence is out of scope here — the caller's `dispatch_one` pairs
//! the `append` with `bp_store.write_new`. Timestamps are stamped
//! caller-side, where the epoch is already known.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

use crate::blueprint::store::BlueprintId;
use crate::store::issue::IssueId;

/// Verdict from a single verifier axis (carried forward from
/// `committer.lua`'s `verdicts_summary`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VerdictSummary {
    /// Verifier axis name (e.g. `"des"`, `"canonical"`, `"noop"`, `"agent-ref"`).
    pub axis: String,
    /// `"pass"` or `"deny"`.
    pub status: String,
    /// Evidence when `pass`; reason when `deny`.
    pub detail: String,
}

/// Trace of one enhance invocation. Both `Applied` and `Rejected` cases
/// produce a single entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EnhanceLogEntry {
    /// The Issue that triggered this enhance invocation.
    pub issue_id: IssueId,
    /// The Blueprint targeted by the patch.
    pub blueprint_id: BlueprintId,
    /// Content hash of the Blueprint before this invocation.
    pub prev_hash: String,
    /// `Applied` — the hash of the patched form. `Rejected` — `""` (no
    /// application happened).
    pub new_hash: String,
    /// The natural-language intent that drove the patch spawner.
    pub intent: String,
    /// The committer's rationale for the applied/rejected verdict.
    pub rationale: String,
    /// Per-axis verifier verdicts (one per entry in `verifier_axes`).
    pub verdicts: Vec<VerdictSummary>,
    /// `"applied"` or `"rejected"`.
    pub status: String,
    /// For `Rejected`: deny reasons in `"axis: reason"` form.
    pub reasons: Vec<String>,
    /// Epoch ms — stamped by the caller.
    pub ts_ms: i64,
}

/// Errors surfaced by an [`EnhanceLogStore`] implementation.
#[derive(Debug, Error)]
pub enum EnhanceLogStoreError {
    /// No entry exists for the given `issue_id`.
    #[error("not found: {0:?}")]
    NotFound(IssueId),
    /// `append` was called twice for the same `issue_id`; the store is
    /// append-only, so the existing entry is left untouched.
    #[error("conflict: issue_id {0:?} already appended (append-only)")]
    Conflict(IssueId),
}

/// Append-only persistence interface for [`EnhanceLogEntry`] records.
#[async_trait]
pub trait EnhanceLogStore: Send + Sync {
    /// Backend name — for diagnostics/logging.
    fn name(&self) -> &str;

    /// Append a new entry. Returns `Conflict` if `entry.issue_id` was
    /// already recorded.
    async fn append(&self, entry: EnhanceLogEntry) -> Result<(), EnhanceLogStoreError>;

    /// Fetch the entry for a single Issue.
    async fn get(&self, issue_id: &IssueId) -> Result<EnhanceLogEntry, EnhanceLogStoreError>;

    /// List every entry for a Blueprint, ascending by `ts_ms`.
    async fn list_by_blueprint(
        &self,
        blueprint_id: &BlueprintId,
    ) -> Result<Vec<EnhanceLogEntry>, EnhanceLogStoreError>;

    /// List every entry across all Blueprints, ascending by `ts_ms`.
    async fn list_all(&self) -> Result<Vec<EnhanceLogEntry>, EnhanceLogStoreError>;
}

/// Process-volatile [`EnhanceLogStore`] backed by a `HashMap`. Suitable
/// for tests and single-process defaults; entries are lost on restart.
#[derive(Default)]
pub struct InMemoryEnhanceLogStore {
    inner: Mutex<HashMap<IssueId, EnhanceLogEntry>>,
}

impl InMemoryEnhanceLogStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl EnhanceLogStore for InMemoryEnhanceLogStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    async fn append(&self, entry: EnhanceLogEntry) -> Result<(), EnhanceLogStoreError> {
        let mut guard = self.inner.lock().unwrap();
        if guard.contains_key(&entry.issue_id) {
            return Err(EnhanceLogStoreError::Conflict(entry.issue_id));
        }
        guard.insert(entry.issue_id.clone(), entry);
        Ok(())
    }

    async fn get(&self, issue_id: &IssueId) -> Result<EnhanceLogEntry, EnhanceLogStoreError> {
        self.inner
            .lock()
            .unwrap()
            .get(issue_id)
            .cloned()
            .ok_or_else(|| EnhanceLogStoreError::NotFound(issue_id.clone()))
    }

    async fn list_by_blueprint(
        &self,
        blueprint_id: &BlueprintId,
    ) -> Result<Vec<EnhanceLogEntry>, EnhanceLogStoreError> {
        let mut entries: Vec<EnhanceLogEntry> = self
            .inner
            .lock()
            .unwrap()
            .values()
            .filter(|e| &e.blueprint_id == blueprint_id)
            .cloned()
            .collect();
        entries.sort_by_key(|e| e.ts_ms);
        Ok(entries)
    }

    async fn list_all(&self) -> Result<Vec<EnhanceLogEntry>, EnhanceLogStoreError> {
        let mut entries: Vec<EnhanceLogEntry> =
            self.inner.lock().unwrap().values().cloned().collect();
        entries.sort_by_key(|e| e.ts_ms);
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_entry(issue: &str, bp: &str, ts: i64) -> EnhanceLogEntry {
        EnhanceLogEntry {
            issue_id: IssueId::new(issue),
            blueprint_id: BlueprintId::new(bp.to_string()),
            prev_hash: "00".repeat(32),
            new_hash: "ff".repeat(32),
            intent: "test intent".into(),
            rationale: "test rationale".into(),
            verdicts: vec![VerdictSummary {
                axis: "des".into(),
                status: "pass".into(),
                detail: "ok".into(),
            }],
            status: "applied".into(),
            reasons: vec![],
            ts_ms: ts,
        }
    }

    #[tokio::test]
    async fn append_then_get_returns_same_entry() {
        let s = InMemoryEnhanceLogStore::new();
        let e = mk_entry("i1", "bp-1", 100);
        s.append(e.clone()).await.unwrap();
        let got = s.get(&IssueId::new("i1")).await.unwrap();
        assert_eq!(got, e);
    }

    #[tokio::test]
    async fn get_missing_returns_not_found() {
        let s = InMemoryEnhanceLogStore::new();
        let err = s.get(&IssueId::new("nope")).await.unwrap_err();
        assert!(matches!(err, EnhanceLogStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn append_twice_returns_conflict() {
        let s = InMemoryEnhanceLogStore::new();
        let e = mk_entry("i2", "bp-1", 200);
        s.append(e.clone()).await.unwrap();
        let err = s.append(e).await.unwrap_err();
        assert!(matches!(err, EnhanceLogStoreError::Conflict(_)));
    }

    #[tokio::test]
    async fn list_by_blueprint_filters_and_sorts_by_ts() {
        let s = InMemoryEnhanceLogStore::new();
        s.append(mk_entry("ib1", "bp-a", 300)).await.unwrap();
        s.append(mk_entry("ib2", "bp-a", 100)).await.unwrap();
        s.append(mk_entry("ib3", "bp-b", 200)).await.unwrap();

        let a_only = s
            .list_by_blueprint(&BlueprintId::new("bp-a".to_string()))
            .await
            .unwrap();
        assert_eq!(a_only.len(), 2);
        assert_eq!(a_only[0].issue_id.as_str(), "ib2");
        assert_eq!(a_only[1].issue_id.as_str(), "ib1");

        let b_only = s
            .list_by_blueprint(&BlueprintId::new("bp-b".to_string()))
            .await
            .unwrap();
        assert_eq!(b_only.len(), 1);
        assert_eq!(b_only[0].issue_id.as_str(), "ib3");
    }

    #[tokio::test]
    async fn list_all_returns_all_sorted_by_ts() {
        let s = InMemoryEnhanceLogStore::new();
        s.append(mk_entry("a", "bp-x", 500)).await.unwrap();
        s.append(mk_entry("b", "bp-y", 100)).await.unwrap();
        s.append(mk_entry("c", "bp-z", 300)).await.unwrap();
        let all = s.list_all().await.unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].issue_id.as_str(), "b");
        assert_eq!(all[1].issue_id.as_str(), "c");
        assert_eq!(all[2].issue_id.as_str(), "a");
    }

    #[tokio::test]
    async fn name_is_in_memory() {
        assert_eq!(InMemoryEnhanceLogStore::new().name(), "in-memory");
    }

    #[tokio::test]
    async fn rejected_entry_carries_reasons() {
        let s = InMemoryEnhanceLogStore::new();
        let mut e = mk_entry("ir", "bp-r", 400);
        e.status = "rejected".into();
        e.new_hash = "".into();
        e.reasons = vec!["des: blueprint.id missing".into(), "noop: ...".into()];
        e.verdicts = vec![VerdictSummary {
            axis: "des".into(),
            status: "deny".into(),
            detail: "blueprint.id missing".into(),
        }];
        s.append(e.clone()).await.unwrap();
        let got = s.get(&IssueId::new("ir")).await.unwrap();
        assert_eq!(got.status, "rejected");
        assert_eq!(got.reasons.len(), 2);
        assert!(got.new_hash.is_empty());
    }
}
