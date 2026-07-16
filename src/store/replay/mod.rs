//! `ReplayStore` — per-run log of completed step outputs plus the whole
//! [`Ctx`] snapshot captured at the moment each step passed.
//!
//! # Architecture
//!
//! This module is the Core primitive for restart-equivalent recovery: at
//! the moment `Engine::dispatch_attempt_with_run_ctx` sees a step's Adapter
//! dispatch complete with `DispatchOutcome::Pass(value)`, it appends a
//! [`ReplayEntry`] carrying `(run_id, step_ref, input_hash, occurrence,
//! ctx_snapshot_json, step_output_json)` — no Adapter-external state (WS
//! session id, `worker_handle`, spawner registrations) is persisted. On
//! restart, a fresh `Engine` re-runs the same Blueprint and consults a
//! [`ReplayCursor`] built from the stored entries: a matching `(step_ref,
//! input_hash, occurrence)` returns the stored value verbatim as
//! `DispatchOutcome::Pass`, skipping the worker spawn entirely; a miss
//! dispatches normally.
//!
//! `Ctx` carries `#[serde(skip)] operator: OperatorInfo`, so the trait-object
//! faces (bridges, hooks, operator backends) naturally drop out of the
//! snapshot; on the replay side, `Ctx::deserialize` rebuilds them as
//! `OperatorInfo::default()`, and the fresh `Engine`'s registries repopulate
//! them at dispatch time. This is the "Adapter concern stays in the
//! Adapter" boundary the subtask brief calls out.
//!
//! ## Contract summary
//!
//! - **`ReplayEntry`** — plain data row, `Serialize`/`Deserialize` and
//!   `Clone`. `input_hash` is a hex-encoded SHA-256 over a canonicalized
//!   JSON form of the resolved input (see [`hash_input_value`]);
//!   `occurrence` counts repeated dispatches of the same
//!   `(run, step, input)` triple in order (loop bodies re-visiting the
//!   same step).
//! - **`ReplayStore`** — async trait: `append` writes one row, `list_by_run`
//!   returns every row for a `RunId` in insertion order. The two backends
//!   shipped here are [`InMemoryReplayStore`] (default, process-volatile)
//!   and [`SqliteReplayStore`](sqlite::SqliteReplayStore) (file-backed).
//! - **`ReplayCursor`** — the read-side helper: `from_entries` builds an
//!   in-memory index and `next_occurrence` + `find` are the two calls the
//!   dispatcher makes per attempt.
//!
//! ## Not in scope (this iteration)
//!
//! - HTTP resume trigger, CLI flags, and boot-time auto-resume — these
//!   sit above this Core primitive and are out of scope for this iteration.
//! - Operator/worker session re-registration — the Adapter-external state
//!   is Adapter concern; the fresh factory re-mints handles/tokens.

use crate::core::ctx::Ctx;
use crate::core::state::DispatchOutcome;
use crate::types::{now_unix, RunId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest;
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

pub mod sqlite;

pub use sqlite::SqliteReplayStore;

// ──────────────────────────────────────────────────────────────────────────
// ReplayEntry — one persisted replay row.
// ──────────────────────────────────────────────────────────────────────────

/// One persisted row in the replay log — a step's completion snapshot the
/// dispatcher stored after `DispatchOutcome::Pass`.
///
/// The row's identity is the 4-tuple `(run_id, step_ref, input_hash,
/// occurrence)`; the SQLite backend enforces this as a `UNIQUE` constraint.
/// `ctx_snapshot_json` and `step_output_json` are the two payloads: the
/// former is a full [`Ctx`] serde-JSON with `operator` dropped by the
/// `#[serde(skip)]` on that field, and the latter is the `DispatchOutcome::Pass`
/// value the worker produced (audit + the value the replay hit returns).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayEntry {
    /// The Run this dispatch belonged to (issue #13 hierarchy).
    pub run_id: RunId,
    /// Blueprint step ref that was dispatched (`TaskSpec.agent` at the
    /// engine boundary).
    pub step_ref: String,
    /// Deterministic hex-encoded SHA-256 hash of the resolved input value
    /// — see [`hash_input_value`].
    pub input_hash: String,
    /// Zero-based counter for repeated dispatches of the same
    /// `(run_id, step_ref, input_hash)` triple within one Run.
    pub occurrence: u32,
    /// Serde-JSON of the whole [`Ctx`] at the moment the step passed.
    /// `Ctx.operator` is dropped by the `#[serde(skip)]` on that field —
    /// see this module's doc.
    pub ctx_snapshot_json: String,
    /// Serde-JSON of the `DispatchOutcome::Pass` value.
    pub step_output_json: String,
    /// Unix epoch seconds — when this row was recorded.
    pub created_at: u64,
}

impl ReplayEntry {
    /// Build a fresh entry from the dispatch-completion state, encoding
    /// `ctx` + `step_output` to their JSON representations and stamping
    /// `created_at` to `now_unix()`. Returns a [`ReplayStoreError::Encode`]
    /// if either serialization fails.
    pub fn from_completion(
        run_id: RunId,
        step_ref: impl Into<String>,
        input_hash: impl Into<String>,
        occurrence: u32,
        ctx: &Ctx,
        step_output: &Value,
    ) -> Result<Self, ReplayStoreError> {
        let ctx_snapshot_json = serde_json::to_string(ctx)
            .map_err(|e| ReplayStoreError::Encode(format!("ctx snapshot: {e}")))?;
        let step_output_json = serde_json::to_string(step_output)
            .map_err(|e| ReplayStoreError::Encode(format!("step output: {e}")))?;
        Ok(Self {
            run_id,
            step_ref: step_ref.into(),
            input_hash: input_hash.into(),
            occurrence,
            ctx_snapshot_json,
            step_output_json,
            created_at: now_unix(),
        })
    }

    /// Decode the `step_output_json` back to a [`Value`] — used by the
    /// replay-hit path in `Engine::dispatch_attempt_with_run_ctx` to hand
    /// a `DispatchOutcome::Pass` back to the caller without touching the
    /// Adapter.
    pub fn decode_step_output(&self) -> Result<Value, ReplayStoreError> {
        serde_json::from_str(&self.step_output_json)
            .map_err(|e| ReplayStoreError::Decode(format!("step output: {e}")))
    }

    /// Decode the `ctx_snapshot_json` back to a [`Ctx`]. `Ctx.operator`
    /// is not serialized (`#[serde(skip)]`), so the round-tripped value
    /// carries `OperatorInfo::default()` in that field.
    pub fn decode_ctx_snapshot(&self) -> Result<Ctx, ReplayStoreError> {
        serde_json::from_str(&self.ctx_snapshot_json)
            .map_err(|e| ReplayStoreError::Decode(format!("ctx snapshot: {e}")))
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Errors.
// ──────────────────────────────────────────────────────────────────────────

/// Errors surfaced by a [`ReplayStore`] implementation.
#[derive(Debug, Error)]
pub enum ReplayStoreError {
    /// The backend refused the row because its 4-tuple identity already
    /// exists (`UNIQUE (run_id, step_ref, input_hash, occurrence)` in the
    /// SQLite backend).
    #[error("duplicate replay entry: run_id={run_id} step_ref={step_ref} input_hash={input_hash} occurrence={occurrence}")]
    Duplicate {
        /// The Run this attempt was rejected for.
        run_id: RunId,
        /// The step ref of the rejected attempt.
        step_ref: String,
        /// The input hash of the rejected attempt.
        input_hash: String,
        /// The occurrence counter of the rejected attempt.
        occurrence: u32,
    },
    /// Encoding a Ctx/output to JSON failed while building an entry.
    #[error("encode: {0}")]
    Encode(String),
    /// Decoding a stored JSON payload back to Rust types failed.
    #[error("decode: {0}")]
    Decode(String),
    /// Backend-specific failure not covered by the other variants
    /// (I/O errors, driver errors, etc.).
    #[error("other: {0}")]
    Other(String),
}

// ──────────────────────────────────────────────────────────────────────────
// ReplayStore trait.
// ──────────────────────────────────────────────────────────────────────────

/// Persistence interface for the replay log — one row per completed step
/// per Run. Backends must preserve insertion order for `list_by_run`.
#[async_trait]
pub trait ReplayStore: Send + Sync {
    /// Backend name — for diagnostics/logging.
    fn name(&self) -> &str;

    /// Append one entry. Returns [`ReplayStoreError::Duplicate`] if the
    /// backend already carries a row with the same
    /// `(run_id, step_ref, input_hash, occurrence)`.
    async fn append(&self, entry: ReplayEntry) -> Result<(), ReplayStoreError>;

    /// Return every entry for `run_id`, in insertion order (the order the
    /// dispatcher wrote them).
    async fn list_by_run(&self, run_id: &RunId) -> Result<Vec<ReplayEntry>, ReplayStoreError>;
}

// ──────────────────────────────────────────────────────────────────────────
// InMemoryReplayStore.
// ──────────────────────────────────────────────────────────────────────────

/// Process-volatile [`ReplayStore`] — the default backend. Entries are
/// lost on restart; use [`SqliteReplayStore`](sqlite::SqliteReplayStore)
/// when survive-restart is what the caller wants.
#[derive(Default)]
pub struct InMemoryReplayStore {
    inner: Mutex<InMemoryInner>,
}

#[derive(Default)]
struct InMemoryInner {
    /// Every appended entry, keyed by RunId, in append order per Run.
    by_run: HashMap<RunId, Vec<ReplayEntry>>,
}

impl InMemoryReplayStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ReplayStore for InMemoryReplayStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    async fn append(&self, entry: ReplayEntry) -> Result<(), ReplayStoreError> {
        let mut inner = self.inner.lock().expect("replay store mutex poisoned");
        let rows = inner.by_run.entry(entry.run_id.clone()).or_default();
        if rows.iter().any(|e| {
            e.step_ref == entry.step_ref
                && e.input_hash == entry.input_hash
                && e.occurrence == entry.occurrence
        }) {
            return Err(ReplayStoreError::Duplicate {
                run_id: entry.run_id,
                step_ref: entry.step_ref,
                input_hash: entry.input_hash,
                occurrence: entry.occurrence,
            });
        }
        rows.push(entry);
        Ok(())
    }

    async fn list_by_run(&self, run_id: &RunId) -> Result<Vec<ReplayEntry>, ReplayStoreError> {
        let inner = self.inner.lock().expect("replay store mutex poisoned");
        Ok(inner.by_run.get(run_id).cloned().unwrap_or_default())
    }
}

// ──────────────────────────────────────────────────────────────────────────
// ReplayCursor — dispatcher-side read helper.
// ──────────────────────────────────────────────────────────────────────────

/// Dispatcher-side read helper: an in-memory index of the entries loaded
/// from a `ReplayStore` for one Run, plus the per-`(step_ref, input_hash)`
/// occurrence counter the dispatcher bumps at each call.
///
/// `next_occurrence` and `find` are the two operations the dispatcher
/// performs per attempt: the former advances the counter and returns
/// the current occurrence value, the latter tries to resolve
/// `(step_ref, input_hash, occurrence)` against the loaded entries.
#[derive(Debug, Default)]
pub struct ReplayCursor {
    /// `(step_ref, input_hash, occurrence) → decoded step output`.
    by_key: HashMap<(String, String, u32), Value>,
    /// Per-`(step_ref, input_hash)` occurrence counter — bumped by
    /// `next_occurrence` on every call.
    seen: HashMap<(String, String), u32>,
}

impl ReplayCursor {
    /// Build a cursor from a run's worth of entries. Entries whose
    /// `step_output_json` fails to decode are silently skipped after a
    /// `tracing::warn!` — a corrupt row must not prevent the rest of the
    /// run from replaying.
    pub fn from_entries(entries: Vec<ReplayEntry>) -> Self {
        let mut by_key: HashMap<(String, String, u32), Value> = HashMap::new();
        for entry in entries {
            match entry.decode_step_output() {
                Ok(value) => {
                    by_key.insert(
                        (
                            entry.step_ref.clone(),
                            entry.input_hash.clone(),
                            entry.occurrence,
                        ),
                        value,
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        run_id = %entry.run_id,
                        step_ref = %entry.step_ref,
                        occurrence = entry.occurrence,
                        error = %e,
                        "ReplayCursor::from_entries: skipping row with undecodable step_output"
                    );
                }
            }
        }
        Self {
            by_key,
            seen: HashMap::new(),
        }
    }

    /// Advance the occurrence counter for `(step_ref, input_hash)` and
    /// return the value **for THIS call** (0 on the first call, 1 on the
    /// second, and so on). Consumes one occurrence: two consecutive calls
    /// with the same key return 0 then 1.
    pub fn next_occurrence(&mut self, step_ref: &str, input_hash: &str) -> u32 {
        let key = (step_ref.to_string(), input_hash.to_string());
        let counter = self.seen.entry(key).or_insert(0);
        let current = *counter;
        *counter = counter.saturating_add(1);
        current
    }

    /// Look up a stored `DispatchOutcome::Pass` value by
    /// `(step_ref, input_hash, occurrence)`. `None` = miss (no matching
    /// row was appended for this triple).
    pub fn find(&self, step_ref: &str, input_hash: &str, occurrence: u32) -> Option<Value> {
        self.by_key
            .get(&(step_ref.to_string(), input_hash.to_string(), occurrence))
            .cloned()
    }

    /// Number of `(step_ref, input_hash, occurrence)` entries loaded.
    /// Handy for tests / diagnostics.
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// `true` when no entries are loaded — every `find` will miss.
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// hash_input_value — deterministic input hash.
// ──────────────────────────────────────────────────────────────────────────

/// Compute a deterministic hex-encoded SHA-256 of a resolved input value
/// (`TaskSpec.initial_directive` at the engine boundary).
///
/// The hash is over the canonical JSON serialization produced by
/// `serde_json::to_string(&value)`. `serde_json` preserves the object-key
/// insertion order that `Value::Object` (backed by `serde_json::Map`,
/// which is `IndexMap` by default) already carries — so identical `Value`s
/// hash identically. Callers pass the SAME `Value` shape across the
/// original run and the replay run; nothing in this hash function
/// canonicalizes further, and that is by design: the replay contract is
/// value-identity, not semantic equivalence.
pub fn hash_input_value(value: &Value) -> String {
    let s = serde_json::to_string(value).unwrap_or_else(|_| String::new());
    let mut hasher = sha2::Sha256::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

// ──────────────────────────────────────────────────────────────────────────
// Convenience — outcome → replay entry (used by the dispatcher).
// ──────────────────────────────────────────────────────────────────────────

/// If `outcome` is `DispatchOutcome::Pass(value)`, borrow the value for
/// [`ReplayEntry::from_completion`]; every other outcome (Blocked / err)
/// returns `None` — the dispatcher intentionally does not log those rows
/// to prevent partial-state poisoning of the replay log.
pub fn pass_value(outcome: &DispatchOutcome) -> Option<&Value> {
    match outcome {
        DispatchOutcome::Pass(v) => Some(v),
        _ => None,
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Unit tests.
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ctx::{Ctx, OperatorInfo};
    use crate::types::StepId;
    use serde_json::json;

    fn mk_ctx() -> Ctx {
        let mut ctx = Ctx::new(StepId::new(), 1, "step-a");
        ctx.meta
            .runtime
            .insert("worker_handle".into(), json!("wh-abcd1234"));
        ctx.meta
            .observer
            .insert("trace_id".into(), json!("trace-42"));
        ctx.meta.authz.insert("who".into(), json!("op-1"));
        ctx.meta.loop_ns.insert("iter".into(), json!(0));
        ctx
    }

    #[test]
    fn ctx_serde_round_trip_drops_operator_but_keeps_meta() {
        let mut ctx = mk_ctx();
        // Set a non-default operator id to prove operator field is skipped.
        ctx.operator = OperatorInfo {
            id: "some-operator".to_string(),
            ..OperatorInfo::default()
        };

        let s = serde_json::to_string(&ctx).expect("serialize ctx");
        assert!(
            !s.contains("some-operator"),
            "operator field must be dropped by #[serde(skip)]"
        );

        let back: Ctx = serde_json::from_str(&s).expect("deserialize ctx");
        assert_eq!(back.task_id, ctx.task_id);
        assert_eq!(back.attempt, ctx.attempt);
        assert_eq!(back.agent, ctx.agent);
        assert_eq!(back.meta.runtime, ctx.meta.runtime);
        assert_eq!(back.meta.authz, ctx.meta.authz);
        assert_eq!(back.meta.observer, ctx.meta.observer);
        assert_eq!(back.meta.loop_ns, ctx.meta.loop_ns);
        // operator round-trips to `OperatorInfo::default()` — the `id`
        // slot's default is `"default-automate"` (see `ctx.rs`) and the
        // three trait-object faces are `None`.
        let default_op = OperatorInfo::default();
        assert_eq!(back.operator.id, default_op.id);
        assert!(back.operator.senior_bridge.is_none());
        assert!(back.operator.spawn_hook.is_none());
        assert!(back.operator.operator.is_none());
    }

    #[tokio::test]
    async fn inmemory_append_and_list_preserves_order() {
        let store = InMemoryReplayStore::new();
        let run_id = RunId::new();
        let ctx = mk_ctx();
        let e1 = ReplayEntry::from_completion(
            run_id.clone(),
            "step-a",
            "hash-1",
            0,
            &ctx,
            &json!({ "out": 1 }),
        )
        .unwrap();
        let e2 = ReplayEntry::from_completion(
            run_id.clone(),
            "step-b",
            "hash-2",
            0,
            &ctx,
            &json!({ "out": 2 }),
        )
        .unwrap();
        store.append(e1.clone()).await.unwrap();
        store.append(e2.clone()).await.unwrap();

        let listed = store.list_by_run(&run_id).await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].step_ref, "step-a");
        assert_eq!(listed[1].step_ref, "step-b");
        assert_eq!(listed[0].decode_step_output().unwrap(), json!({ "out": 1 }));
        assert_eq!(listed[1].decode_step_output().unwrap(), json!({ "out": 2 }));
    }

    #[tokio::test]
    async fn inmemory_duplicate_rejected() {
        let store = InMemoryReplayStore::new();
        let run_id = RunId::new();
        let ctx = mk_ctx();
        let e =
            ReplayEntry::from_completion(run_id.clone(), "step-a", "hash-1", 0, &ctx, &json!("v"))
                .unwrap();
        store.append(e.clone()).await.unwrap();
        let err = store.append(e).await.unwrap_err();
        assert!(matches!(err, ReplayStoreError::Duplicate { .. }));
    }

    #[tokio::test]
    async fn inmemory_list_by_run_isolates_runs() {
        let store = InMemoryReplayStore::new();
        let r1 = RunId::new();
        let r2 = RunId::new();
        let ctx = mk_ctx();
        store
            .append(ReplayEntry::from_completion(r1.clone(), "a", "h", 0, &ctx, &json!(1)).unwrap())
            .await
            .unwrap();
        store
            .append(ReplayEntry::from_completion(r2.clone(), "a", "h", 0, &ctx, &json!(2)).unwrap())
            .await
            .unwrap();
        assert_eq!(store.list_by_run(&r1).await.unwrap().len(), 1);
        assert_eq!(store.list_by_run(&r2).await.unwrap().len(), 1);
        assert_eq!(
            store.list_by_run(&r1).await.unwrap()[0]
                .decode_step_output()
                .unwrap(),
            json!(1)
        );
    }

    #[tokio::test]
    async fn cursor_from_entries_hit_and_miss() {
        let store = InMemoryReplayStore::new();
        let run_id = RunId::new();
        let ctx = mk_ctx();
        store
            .append(
                ReplayEntry::from_completion(
                    run_id.clone(),
                    "step-a",
                    "hash-a",
                    0,
                    &ctx,
                    &json!({ "value": "stored-a" }),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let entries = store.list_by_run(&run_id).await.unwrap();
        let cursor = ReplayCursor::from_entries(entries);

        assert_eq!(cursor.len(), 1);
        assert_eq!(
            cursor.find("step-a", "hash-a", 0),
            Some(json!({ "value": "stored-a" }))
        );
        assert!(cursor.find("step-a", "hash-a", 1).is_none());
        assert!(cursor.find("step-b", "hash-a", 0).is_none());
        assert!(cursor.find("step-a", "hash-b", 0).is_none());
    }

    #[test]
    fn cursor_next_occurrence_increments_per_key() {
        let mut cursor = ReplayCursor::default();
        assert_eq!(cursor.next_occurrence("a", "h"), 0);
        assert_eq!(cursor.next_occurrence("a", "h"), 1);
        assert_eq!(cursor.next_occurrence("a", "h"), 2);
        // Different key starts its own counter.
        assert_eq!(cursor.next_occurrence("b", "h"), 0);
        assert_eq!(cursor.next_occurrence("a", "h2"), 0);
    }

    #[tokio::test]
    async fn occurrence_1_replay_row_coexists_with_occurrence_0() {
        // The occurrence counter is what lets the dispatcher log a second
        // dispatch of the SAME (run, step, input) without hitting the
        // UNIQUE constraint. The InMemory store verifies the same rule.
        let store = InMemoryReplayStore::new();
        let run_id = RunId::new();
        let ctx = mk_ctx();
        let e0 = ReplayEntry::from_completion(
            run_id.clone(),
            "step-a",
            "hash-a",
            0,
            &ctx,
            &json!("first"),
        )
        .unwrap();
        let e1 = ReplayEntry::from_completion(
            run_id.clone(),
            "step-a",
            "hash-a",
            1,
            &ctx,
            &json!("second"),
        )
        .unwrap();
        store.append(e0).await.unwrap();
        store
            .append(e1)
            .await
            .expect("occurrence=1 must not collide with occurrence=0");

        let entries = store.list_by_run(&run_id).await.unwrap();
        assert_eq!(entries.len(), 2);
        let cursor = ReplayCursor::from_entries(entries);
        assert_eq!(cursor.find("step-a", "hash-a", 0), Some(json!("first")));
        assert_eq!(cursor.find("step-a", "hash-a", 1), Some(json!("second")));
    }

    #[test]
    fn hash_input_value_deterministic_for_same_json() {
        let v1 = json!({ "a": 1, "b": [2, 3] });
        let v2 = json!({ "a": 1, "b": [2, 3] });
        assert_eq!(hash_input_value(&v1), hash_input_value(&v2));

        // Different value → different hash.
        let v3 = json!({ "a": 2, "b": [2, 3] });
        assert_ne!(hash_input_value(&v1), hash_input_value(&v3));
    }

    #[test]
    fn pass_value_filters_non_pass_outcomes() {
        assert!(pass_value(&DispatchOutcome::Pass(json!("ok"))).is_some());
        assert!(pass_value(&DispatchOutcome::Blocked(json!("no"))).is_none());
        assert!(pass_value(&DispatchOutcome::Cancelled).is_none());
        assert!(pass_value(&DispatchOutcome::Timeout).is_none());
    }
}
