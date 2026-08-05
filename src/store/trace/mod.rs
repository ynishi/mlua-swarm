//! `RunTraceStore` — the persisted per-Run trace rail (`TraceEvent`
//! stream) plus the normalized worker-stats types (`TokenUsage` /
//! [`WorkerStats`]) shared by the trace rail and the
//! [`crate::store::run::StepEntry`] step-stats extension.
//!
//! Two rails observe the same dispatch (issue: per-step run stats):
//!
//! - [`crate::store::run::StepEntry`] — the **terminal summary** of one
//!   dispatched step (write-once, appended by the dispatcher after the
//!   outcome is known; carries duration / usage / model / verdict).
//! - [`TraceEvent`] (this module) — the **in-flight stream** of what is
//!   happening (`core.step_dispatched`, `mw.long_hold_warn`, …),
//!   append-only, per-Run, ordered by `seq`.
//!
//! Everything here is purely observational: a failed append must never
//! fail the dispatch it observes (callers warn-and-swallow — the same
//! fail-open convention as `EngineDispatcher::dispatch`'s
//! `append_step_entry`). The naming is deliberately `Trace`, not `Log`:
//! in the Rust ecosystem "log" collides with the `log`/`tracing`
//! facade crates, and this rail is domain data, not process logging.
//!
//! Kinds are an **open set** of namespaced strings — writers may insert
//! new kinds without a schema migration. Current namespaces:
//!
//! - `core.*` — engine/dispatcher (`run_started` / `step_dispatched` /
//!   `step_completed` / `cancel_requested` / `run_finished`)
//! - `mw.*` — middleware (`long_hold_warn`, …)
//! - `worker.*` — adapter / worker self-reports
//! - `ext.*` — future external writers (Lua flow, enhance flow, tools)
//!
//! Layering invariant (future `mlua-swarm-trace` crate split): this
//! module must not depend on engine types — only `crate::types` ids and
//! serde values.

use crate::types::RunId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;

pub mod sqlite;
pub use sqlite::SqliteRunTraceStore;

// ──────────────────────────────────────────────────────────────────────────
// Normalized worker stats (TokenUsage / WorkerStats)
// ──────────────────────────────────────────────────────────────────────────

/// Aggregated token usage for one worker attempt, normalized across
/// worker kinds (agent-block `agent.run` return / subprocess declared
/// mapping / operator self-report). Field names follow the Anthropic
/// wire convention (`input_tokens` / `output_tokens`) that agent-block
/// already normalizes OpenAI-style responses into.
///
/// **Every wire field is optional on the way in** ([`TokenUsageWire`]):
/// a reporter that only knows one axis (a harness completion notice
/// that surfaces a single token total, an API response that omits the
/// total) still lands a usable usage record instead of being dropped.
/// The stored shape stays the closed 3-field triple — missing splits
/// read as `0`, and a missing `total_tokens` is derived as
/// `input + output` on decode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(from = "TokenUsageWire")]
#[schemars(with = "TokenUsageWire")]
pub struct TokenUsage {
    /// Prompt-side tokens consumed, summed across the attempt's turns.
    /// `0` when the reporter did not split its total.
    pub input_tokens: u64,
    /// Completion-side tokens produced, summed across the attempt's
    /// turns. `0` when the reporter did not split its total.
    pub output_tokens: u64,
    /// `input + output` (kept explicit because some producers report a
    /// total that includes cache-read/creation tokens the two split
    /// fields don't cover). Derived from the splits when the reporter
    /// omits it.
    pub total_tokens: u64,
}

impl TokenUsage {
    /// Build a usage record from three independently-optional parts,
    /// applying the same normalization the wire decode does: absent
    /// splits read as `0`, and an absent total is derived as
    /// `input + output`.
    ///
    /// `None` only when the reporter carried **no** token axis at all —
    /// the caller then records no usage rather than a zeroed one.
    ///
    /// Shared by the hand-rolled extractors that read usage out of a
    /// worker's raw payload (agent-block's `agent.run` return,
    /// subprocess `usage_ptr` declarations) so every axis applies one
    /// normalization rule.
    pub fn from_parts(
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        total_tokens: Option<u64>,
    ) -> Option<Self> {
        if input_tokens.is_none() && output_tokens.is_none() && total_tokens.is_none() {
            return None;
        }
        let input = input_tokens.unwrap_or(0);
        let output = output_tokens.unwrap_or(0);
        Some(Self {
            input_tokens: input,
            output_tokens: output,
            total_tokens: total_tokens.unwrap_or(input + output),
        })
    }
}

/// Deserialization shadow of [`TokenUsage`] — the wire contract, where
/// every token field is optional.
///
/// Exists so partial reports survive the decode: before it, a producer
/// that sent `{"total_tokens": N}` alone failed the whole
/// [`WorkerStats`] decode, and the best-effort ingest sites dropped the
/// entire stats object (model / num_turns included) without a trace.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct TokenUsageWire {
    /// Prompt-side tokens, when the reporter splits them out.
    #[serde(default)]
    pub input_tokens: Option<u64>,
    /// Completion-side tokens, when the reporter splits them out.
    #[serde(default)]
    pub output_tokens: Option<u64>,
    /// Reporter-supplied total; derived from the splits when absent.
    #[serde(default)]
    pub total_tokens: Option<u64>,
}

impl From<TokenUsageWire> for TokenUsage {
    fn from(w: TokenUsageWire) -> Self {
        // An all-absent object (`"usage": {}`) is a degenerate but legal
        // wire value — it lands as an explicit all-zero record rather
        // than an error, matching the "stats never gate the report"
        // invariant.
        TokenUsage::from_parts(w.input_tokens, w.output_tokens, w.total_tokens).unwrap_or(Self {
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
        })
    }
}

/// Normalized per-attempt worker statistics, reported by a worker
/// boundary (spawner fold site / result captor / `POST
/// /v1/worker/submit`) into the engine and folded into the terminal
/// [`crate::store::run::StepEntry`] by the dispatcher.
///
/// The three named fields are the **closed schema** the engine knows;
/// everything worker-kind-specific rides in [`Self::adapter_data`] as
/// raw JSON the engine never interprets (capped at
/// [`TRACE_PAYLOAD_CAP_BYTES`] on fold). Every field is optional —
/// absence must never block a dispatch.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WorkerStats {
    /// Worker kind label (`"agent_block"` / `"subprocess"` /
    /// `"operator"` / …) — set by whichever boundary constructed the
    /// stats, since only that boundary knows its own kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_kind: Option<String>,
    /// The model that served the attempt, when the boundary knows it
    /// (subprocess: the rendered `{model}` placeholder; operator:
    /// self-report; agent-block: spec-declared model if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Normalized token usage, when the boundary can produce one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
    /// Number of LLM turns the attempt ran (agent-block `num_turns`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_turns: Option<u32>,
    /// Worker-kind-specific raw payload (exit code, stderr tail, cache
    /// token detail, …). Observational only — the engine stores it
    /// verbatim (size-capped) and never branches on it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter_data: Option<Value>,
}

impl WorkerStats {
    /// `true` when no field carries information — callers skip
    /// recording an all-empty stats value.
    pub fn is_empty(&self) -> bool {
        self.worker_kind.is_none()
            && self.model.is_none()
            && self.usage.is_none()
            && self.num_turns.is_none()
            && self.adapter_data.is_none()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// TraceEvent / TraceQuery
// ──────────────────────────────────────────────────────────────────────────

/// Byte cap applied to [`TraceEvent::payload`] and
/// [`WorkerStats::adapter_data`] before persisting. Oversized values are
/// replaced by a truncation marker object (see [`cap_payload`]) — the
/// trace rail is an observability artifact, not a blob store.
pub const TRACE_PAYLOAD_CAP_BYTES: usize = 8 * 1024;

/// Default per-Run retention ceiling — appends beyond this many events
/// prune the oldest rows first.
pub const DEFAULT_TRACE_MAX_EVENTS_PER_RUN: usize = 10_000;

/// Default `list` page size when a query sets neither `limit` nor
/// `latest`.
pub const DEFAULT_TRACE_LIST_LIMIT: usize = 1_000;

/// Well-known `TraceEvent.kind` values written by the engine itself.
/// The kind axis is an open set — these constants exist so in-tree
/// writers and tests agree on spelling, not to constrain writers.
pub mod kind {
    /// A Run began dispatching (server-side, once per kick).
    pub const RUN_STARTED: &str = "core.run_started";
    /// The dispatcher is about to spawn a step's worker.
    pub const STEP_DISPATCHED: &str = "core.step_dispatched";
    /// A step reached its terminal outcome (payload carries the status
    /// label + timing summary; the authoritative record is the
    /// `StepEntry` appended in the same breath).
    pub const STEP_COMPLETED: &str = "core.step_completed";
    /// A Run reached its terminal status (payload: `{"status": ...}`).
    pub const RUN_FINISHED: &str = "core.run_finished";
    /// Cancellation was requested for the Run.
    pub const CANCEL_REQUESTED: &str = "core.cancel_requested";
    /// `LongHoldMiddleware` observed a completion above its threshold.
    pub const LONG_HOLD_WARN: &str = "mw.long_hold_warn";
    /// A worker reported a degradation (mirrors the `DegradationEntry`
    /// rail so the trace stream is self-contained).
    pub const WORKER_DEGRADATION: &str = "worker.degradation";
}

/// One persisted trace event — a member of a Run's append-only stream.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TraceEvent {
    /// The Run this event belongs to.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// Per-Run monotonically increasing ordering key, assigned by the
    /// store at append time (1-based).
    pub seq: u64,
    /// Unix epoch milliseconds — when the event was recorded.
    pub ts_ms: i64,
    /// Namespaced kind string (open set; see the module doc).
    pub kind: String,
    /// The Blueprint step ref this event concerns, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_ref: Option<String>,
    /// The attempt number this event concerns, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    /// Free-form JSON payload (capped at [`TRACE_PAYLOAD_CAP_BYTES`]).
    pub payload: Value,
}

/// The caller-supplied half of a [`TraceEvent`] — the store assigns
/// `seq` and `ts_ms` at append time.
#[derive(Debug, Clone)]
pub struct TraceEventDraft {
    /// Namespaced kind string (open set).
    pub kind: String,
    /// The Blueprint step ref this event concerns, if any.
    pub step_ref: Option<String>,
    /// The attempt number this event concerns, if any.
    pub attempt: Option<u32>,
    /// Free-form JSON payload — capped by the store on append.
    pub payload: Value,
}

/// Filter/paging parameters for [`RunTraceStore::list`]. All filters
/// AND together; `latest` and `after` are mutually exclusive with
/// `latest` winning (it answers "show me the tail" regardless of any
/// cursor the caller also carried).
#[derive(Debug, Clone, Default)]
pub struct TraceQuery {
    /// Forward-paging cursor: only events with `seq > after`.
    pub after: Option<u64>,
    /// Page size cap (defaults to [`DEFAULT_TRACE_LIST_LIMIT`]).
    pub limit: Option<usize>,
    /// Tail mode: return the LAST n matching events (still in ascending
    /// `seq` order). Takes precedence over `after`/`limit`.
    pub latest: Option<usize>,
    /// Kind filters — an event matches when its kind equals, or starts
    /// with, ANY entry (prefix match: `"mw."` matches every middleware
    /// kind). Empty = no kind filter.
    pub kinds: Vec<String>,
    /// Exact `step_ref` filter.
    pub step_ref: Option<String>,
    /// Exact `attempt` filter.
    pub attempt: Option<u32>,
}

impl TraceQuery {
    /// Does `event` pass this query's kind/step/attempt filters
    /// (paging axes excluded)?
    fn matches(&self, event: &TraceEvent) -> bool {
        if !self.kinds.is_empty()
            && !self
                .kinds
                .iter()
                .any(|k| event.kind == *k || event.kind.starts_with(k.as_str()))
        {
            return false;
        }
        if let Some(step_ref) = &self.step_ref {
            if event.step_ref.as_deref() != Some(step_ref.as_str()) {
                return false;
            }
        }
        if let Some(attempt) = self.attempt {
            if event.attempt != Some(attempt) {
                return false;
            }
        }
        true
    }

    /// Apply paging (latest wins over after/limit) to an ascending,
    /// already-filtered event list.
    fn page(&self, mut events: Vec<TraceEvent>) -> Vec<TraceEvent> {
        if let Some(n) = self.latest {
            let start = events.len().saturating_sub(n);
            return events.split_off(start);
        }
        if let Some(after) = self.after {
            events.retain(|e| e.seq > after);
        }
        let limit = self.limit.unwrap_or(DEFAULT_TRACE_LIST_LIMIT);
        events.truncate(limit);
        events
    }
}

/// Replace an oversized payload with a truncation marker carrying the
/// original size and a head excerpt, so a runaway writer cannot bloat
/// the trace store. Values at or under [`TRACE_PAYLOAD_CAP_BYTES`] pass
/// through unchanged.
pub fn cap_payload(payload: Value) -> Value {
    let serialized = payload.to_string();
    if serialized.len() <= TRACE_PAYLOAD_CAP_BYTES {
        return payload;
    }
    let head: String = serialized.chars().take(1024).collect();
    serde_json::json!({
        "truncated": true,
        "size_bytes": serialized.len(),
        "head": head,
    })
}

/// Errors surfaced by a [`RunTraceStore`] implementation.
#[derive(Debug, Error)]
pub enum TraceStoreError {
    /// Backend-specific failure.
    #[error("other: {0}")]
    Other(String),
}

// ──────────────────────────────────────────────────────────────────────────
// RunTraceStore trait
// ──────────────────────────────────────────────────────────────────────────

/// Persistence interface for the per-Run trace stream.
#[async_trait]
pub trait RunTraceStore: Send + Sync {
    /// Backend name — for diagnostics/logging.
    fn name(&self) -> &str;

    /// Append one event to `run_id`'s stream, assigning the next `seq`
    /// and stamping `ts_ms`. The store caps `draft.payload` via
    /// [`cap_payload`] and prunes the oldest rows beyond the per-Run
    /// retention ceiling. Appending to an unknown `run_id` is legal —
    /// the trace rail has no foreign-key coupling to `RunStore` (a
    /// trace writer must never fail because Run-row creation raced it).
    async fn append(
        &self,
        run_id: &RunId,
        draft: TraceEventDraft,
    ) -> Result<TraceEvent, TraceStoreError>;

    /// List `run_id`'s events matching `query`, ascending by `seq`.
    async fn list(
        &self,
        run_id: &RunId,
        query: &TraceQuery,
    ) -> Result<Vec<TraceEvent>, TraceStoreError>;

    /// Delete every event belonging to `run_id`, returning the number
    /// of deleted events. Deleting an unknown/empty Run is `Ok(0)`.
    async fn delete_run(&self, run_id: &RunId) -> Result<u64, TraceStoreError>;
}

// ──────────────────────────────────────────────────────────────────────────
// TraceHandle — the pervasive-insertion write port
// ──────────────────────────────────────────────────────────────────────────

/// A cheap, cloneable write handle binding one `run_id` to a
/// [`RunTraceStore`] — the single port through which the dispatcher,
/// middlewares (via `Engine::trace_handle`), server handlers, and any
/// future writer append trace events. Appends are **best-effort**: a
/// store failure is logged at `warn` and swallowed, never propagated
/// (fail-open, matching the `append_step_entry` convention).
#[derive(Clone)]
pub struct TraceHandle {
    run_id: RunId,
    store: Arc<dyn RunTraceStore>,
}

impl TraceHandle {
    /// Bind `run_id` to `store`.
    pub fn new(run_id: RunId, store: Arc<dyn RunTraceStore>) -> Self {
        Self { run_id, store }
    }

    /// The Run this handle appends into.
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    /// Best-effort append (see the struct doc). `kind` should follow
    /// the namespaced open-set convention (`core.*` / `mw.*` /
    /// `worker.*` / `ext.*`).
    pub async fn append(
        &self,
        kind: &str,
        step_ref: Option<&str>,
        attempt: Option<u32>,
        payload: Value,
    ) {
        let draft = TraceEventDraft {
            kind: kind.to_string(),
            step_ref: step_ref.map(str::to_string),
            attempt,
            payload,
        };
        if let Err(e) = self.store.append(&self.run_id, draft).await {
            tracing::warn!(
                run_id = %self.run_id,
                kind = kind,
                error = %e,
                "TraceHandle::append failed (swallowed — trace is observational)"
            );
        }
    }
}

impl std::fmt::Debug for TraceHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceHandle")
            .field("run_id", &self.run_id)
            .field("store", &self.store.name())
            .finish()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// InMemoryRunTraceStore
// ──────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct TraceInner {
    /// Per-Run ascending event lists.
    events: HashMap<RunId, Vec<TraceEvent>>,
    /// Per-Run next `seq` — kept separately from `events.len()` because
    /// retention pruning removes head entries without recycling seqs.
    next_seq: HashMap<RunId, u64>,
}

/// Process-volatile [`RunTraceStore`] — the default when no persistent
/// backend is wired.
pub struct InMemoryRunTraceStore {
    inner: Mutex<TraceInner>,
    max_events_per_run: usize,
}

impl InMemoryRunTraceStore {
    /// Create an empty store with the default retention ceiling.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(TraceInner::default()),
            max_events_per_run: DEFAULT_TRACE_MAX_EVENTS_PER_RUN,
        }
    }

    /// Create an empty store with a custom per-Run retention ceiling
    /// (tests).
    pub fn with_max_events_per_run(max: usize) -> Self {
        Self {
            inner: Mutex::new(TraceInner::default()),
            max_events_per_run: max,
        }
    }
}

impl Default for InMemoryRunTraceStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Unix epoch milliseconds now — trace events are sub-second dense, so
/// the store stamps millis (the coarser `now_unix` seconds stay on the
/// pre-existing `StepEntry.at` / `RunRecord` fields).
pub(crate) fn now_unix_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[async_trait]
impl RunTraceStore for InMemoryRunTraceStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    async fn append(
        &self,
        run_id: &RunId,
        draft: TraceEventDraft,
    ) -> Result<TraceEvent, TraceStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let seq_slot = inner.next_seq.entry(run_id.clone()).or_insert(0);
        *seq_slot += 1;
        let event = TraceEvent {
            run_id: run_id.clone(),
            seq: *seq_slot,
            ts_ms: now_unix_ms(),
            kind: draft.kind,
            step_ref: draft.step_ref,
            attempt: draft.attempt,
            payload: cap_payload(draft.payload),
        };
        let list = inner.events.entry(run_id.clone()).or_default();
        list.push(event.clone());
        if list.len() > self.max_events_per_run {
            let overflow = list.len() - self.max_events_per_run;
            list.drain(..overflow);
        }
        Ok(event)
    }

    async fn list(
        &self,
        run_id: &RunId,
        query: &TraceQuery,
    ) -> Result<Vec<TraceEvent>, TraceStoreError> {
        let inner = self.inner.lock().unwrap();
        let events: Vec<TraceEvent> = inner
            .events
            .get(run_id)
            .map(|list| list.iter().filter(|e| query.matches(e)).cloned().collect())
            .unwrap_or_default();
        Ok(query.page(events))
    }

    async fn delete_run(&self, run_id: &RunId) -> Result<u64, TraceStoreError> {
        let mut inner = self.inner.lock().unwrap();
        inner.next_seq.remove(run_id);
        Ok(inner
            .events
            .remove(run_id)
            .map(|list| list.len() as u64)
            .unwrap_or(0))
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rid(s: &str) -> RunId {
        RunId::parse(s).unwrap()
    }

    fn draft(kind: &str, step_ref: Option<&str>, attempt: Option<u32>) -> TraceEventDraft {
        TraceEventDraft {
            kind: kind.to_string(),
            step_ref: step_ref.map(str::to_string),
            attempt,
            payload: json!({"k": kind}),
        }
    }

    // ── TokenUsage / WorkerStats wire contract ───────────────────────

    #[test]
    fn usage_decodes_from_a_total_only_report() {
        // The canonical operator self-report: a harness completion
        // notice that surfaces one token total and no split. Before the
        // wire shadow this failed the decode and took the whole
        // WorkerStats (model / num_turns included) down with it.
        let stats: WorkerStats = serde_json::from_value(json!({
            "usage": {"total_tokens": 198471},
            "model": "opus",
            "num_turns": 22,
        }))
        .expect("a total-only usage must decode");
        let usage = stats.usage.clone().expect("usage must survive the decode");
        assert_eq!(usage.total_tokens, 198471);
        assert_eq!(usage.input_tokens, 0, "unsplit report reads as 0");
        assert_eq!(usage.output_tokens, 0);
        assert_eq!(stats.model.as_deref(), Some("opus"));
        assert_eq!(stats.num_turns, Some(22));
        assert!(!stats.is_empty());
    }

    #[test]
    fn usage_derives_the_total_from_the_splits() {
        let usage: TokenUsage =
            serde_json::from_value(json!({"input_tokens": 10, "output_tokens": 4}))
                .expect("splits-only usage must decode");
        assert_eq!(usage.total_tokens, 14, "absent total is derived");
    }

    #[test]
    fn usage_keeps_a_reporter_total_that_exceeds_the_splits() {
        // Cache-read/creation tokens live in the reporter's total but
        // not in the two splits — never recompute over the report.
        let usage: TokenUsage = serde_json::from_value(
            json!({"input_tokens": 10, "output_tokens": 4, "total_tokens": 900}),
        )
        .unwrap();
        assert_eq!(usage.total_tokens, 900);
    }

    #[test]
    fn usage_serializes_as_the_closed_triple() {
        let usage = TokenUsage::from_parts(None, None, Some(7)).unwrap();
        assert_eq!(
            serde_json::to_value(&usage).unwrap(),
            json!({"input_tokens": 0, "output_tokens": 0, "total_tokens": 7}),
            "the stored shape stays the 3-field triple"
        );
    }

    #[test]
    fn from_parts_is_none_only_when_no_axis_was_reported() {
        assert_eq!(TokenUsage::from_parts(None, None, None), None);
        assert!(TokenUsage::from_parts(Some(0), None, None).is_some());
    }

    #[test]
    fn empty_usage_object_lands_as_an_explicit_zero_record() {
        // Degenerate but legal: stats must never gate the report, so an
        // empty object decodes rather than erroring.
        let usage: TokenUsage = serde_json::from_value(json!({})).unwrap();
        assert_eq!(usage.total_tokens, 0);
    }

    #[tokio::test]
    async fn append_assigns_monotonic_seq_per_run() {
        let s = InMemoryRunTraceStore::new();
        let e1 = s
            .append(&rid("R-1"), draft("core.run_started", None, None))
            .await
            .unwrap();
        let e2 = s
            .append(
                &rid("R-1"),
                draft("core.step_dispatched", Some("w"), Some(1)),
            )
            .await
            .unwrap();
        let other = s
            .append(&rid("R-2"), draft("core.run_started", None, None))
            .await
            .unwrap();
        assert_eq!(e1.seq, 1);
        assert_eq!(e2.seq, 2);
        assert_eq!(other.seq, 1, "seq is per-Run, not global");
        assert!(e1.ts_ms > 0);
    }

    #[tokio::test]
    async fn list_filters_by_kind_prefix_step_and_attempt() {
        let s = InMemoryRunTraceStore::new();
        let r = rid("R-1");
        s.append(&r, draft("core.run_started", None, None))
            .await
            .unwrap();
        s.append(&r, draft("core.step_dispatched", Some("a"), Some(1)))
            .await
            .unwrap();
        s.append(&r, draft("mw.long_hold_warn", Some("a"), Some(1)))
            .await
            .unwrap();
        s.append(&r, draft("core.step_completed", Some("b"), Some(2)))
            .await
            .unwrap();

        let mw = s
            .list(
                &r,
                &TraceQuery {
                    kinds: vec!["mw.".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(mw.len(), 1);
        assert_eq!(mw[0].kind, "mw.long_hold_warn");

        let step_a = s
            .list(
                &r,
                &TraceQuery {
                    step_ref: Some("a".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(step_a.len(), 2);

        let attempt2 = s
            .list(
                &r,
                &TraceQuery {
                    attempt: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(attempt2.len(), 1);
        assert_eq!(attempt2[0].step_ref.as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn list_paging_after_and_latest() {
        let s = InMemoryRunTraceStore::new();
        let r = rid("R-1");
        for i in 0..5 {
            s.append(&r, draft(&format!("core.e{i}"), None, None))
                .await
                .unwrap();
        }

        let after = s
            .list(
                &r,
                &TraceQuery {
                    after: Some(3),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(after.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![4, 5]);

        let latest = s
            .list(
                &r,
                &TraceQuery {
                    latest: Some(2),
                    // latest must win over a cursor the caller also set.
                    after: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(latest.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![4, 5]);

        let limited = s
            .list(
                &r,
                &TraceQuery {
                    limit: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            limited.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn retention_prunes_oldest_keeping_seq() {
        let s = InMemoryRunTraceStore::with_max_events_per_run(3);
        let r = rid("R-1");
        for i in 0..5 {
            s.append(&r, draft(&format!("core.e{i}"), None, None))
                .await
                .unwrap();
        }
        let all = s.list(&r, &TraceQuery::default()).await.unwrap();
        assert_eq!(all.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3, 4, 5]);
        // A later append keeps counting up — pruning never recycles seqs.
        let e6 = s.append(&r, draft("core.e5", None, None)).await.unwrap();
        assert_eq!(e6.seq, 6);
    }

    #[tokio::test]
    async fn delete_run_removes_stream() {
        let s = InMemoryRunTraceStore::new();
        let r = rid("R-1");
        s.append(&r, draft("core.run_started", None, None))
            .await
            .unwrap();
        s.append(&r, draft("core.run_finished", None, None))
            .await
            .unwrap();
        assert_eq!(s.delete_run(&r).await.unwrap(), 2);
        assert!(s.list(&r, &TraceQuery::default()).await.unwrap().is_empty());
        assert_eq!(s.delete_run(&r).await.unwrap(), 0, "double delete is Ok(0)");
    }

    #[tokio::test]
    async fn oversized_payload_is_truncated_with_marker() {
        let s = InMemoryRunTraceStore::new();
        let r = rid("R-1");
        let big = "x".repeat(TRACE_PAYLOAD_CAP_BYTES + 100);
        let e = s
            .append(
                &r,
                TraceEventDraft {
                    kind: "worker.output".into(),
                    step_ref: None,
                    attempt: None,
                    payload: json!({"blob": big}),
                },
            )
            .await
            .unwrap();
        assert_eq!(e.payload.get("truncated"), Some(&json!(true)));
        assert!(e.payload.get("size_bytes").is_some());
    }

    #[tokio::test]
    async fn trace_handle_appends_best_effort() {
        let store: Arc<dyn RunTraceStore> = Arc::new(InMemoryRunTraceStore::new());
        let handle = TraceHandle::new(rid("R-1"), store.clone());
        handle
            .append(kind::STEP_DISPATCHED, Some("w"), Some(1), json!({}))
            .await;
        let events = store
            .list(&rid("R-1"), &TraceQuery::default())
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, kind::STEP_DISPATCHED);
    }

    #[test]
    fn worker_stats_is_empty_reflects_fields() {
        assert!(WorkerStats::default().is_empty());
        let stats = WorkerStats {
            usage: Some(TokenUsage {
                input_tokens: 1,
                output_tokens: 2,
                total_tokens: 3,
            }),
            ..Default::default()
        };
        assert!(!stats.is_empty());
    }
}
