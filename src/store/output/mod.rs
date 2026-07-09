//! # output_store — Data-plane support layer
//!
//! **Module built on the Data (Big Response handling) vs. Domain (Flow / verdict)
//! separation axis.** Owns the machinery for shuttling big response bodies
//! (4k-token-scale text / file paths / blobs) between SubAgents, and stays out
//! of the Engine's flow control (the BLOCKED/PASS `if`/`else` verdicts). The
//! Domain path — `engine.rs` `submit_output` / `output_tail` / dispatch — is
//! left untouched.
//!
//! ## Why (Data / Domain separation)
//!
//! The Engine's `output_store` `HashMap` plus `Final.ok` extraction in
//! the dispatch path already covers the Domain (verdict flow). Pushing Big Response
//! payloads (LLM answers of several kilotokens, intermediate files, large
//! blobs) through the same path floods MainAI context after only a handful of
//! SubAgents and turns the return-text channel into a file-path junk drawer.
//!
//! Resolution: **MainAI only carries `OutputRef` (a small `out_id`); this
//! module owns the big bodies.** The Domain (flow control) stays inside the
//! Engine, the Data plane (Big Response handling) is completed by this module
//! and its paired `SpawnerLayer`s, and the two do not interfere.
//!
//! Note that the Sub/Main-Agent split is not just a context-size trick — it is
//! a support scaffold for MainAI, which is what keeps a pure Flow orchestrator
//! from becoming either rigid or brittle. Data-plane offloading is one of the
//! things that scaffold needs to work.
//!
//! ## Architecture (three lifecycle axes)
//!
//! Data handling is cut along three lifecycles. Mixing them collapses into
//! Agent hardcoding, non-portability, or unmanaged growth:
//!
//! - **LC1 — Agent authoring (Swarm-independent):** the Agent contract in
//!   `Agent.md` speaks in terms of `$IN_REFS` and a single EMIT tool. It does
//!   not know Swarm-specific paths or ids.
//! - **LC2 — Agent execution (Swarm = runtime environment):** at spawn time
//!   the runtime injects env (`$IN_REFS` = previous `out_id` list, EMIT tool
//!   plus token). The SubAgent POSTs directly to the store, bypassing MainAgent.
//! - **LC3 — Swarm management (this module = Data owner):** intake (EMIT) →
//!   allocate `OutputRef` → register → optional disk persistence. `get(out_id)`
//!   feeds the next spawn's `IN_REFS`.
//!
//! ## Discipline
//!
//! - **SubAgent → MainAgent direct return is forbidden.** Big bodies never
//!   ride the return text; MainAgent only holds an `OutputRef` (small id).
//! - **Write path is the EMIT tool, once.** No file-side channel, no smuggling
//!   through return text — the goal is to remove the "LLM forgets at the tail
//!   of the task" failure mode by construction.
//! - **Same-shape `SpawnerLayer` pattern.** All intake / inject flows through
//!   `middleware/sink.rs` / `middleware/input_inject.rs` (both `SpawnerLayer`
//!   impls). Same shape as `AgentResolver` / `ProjectNameAliasLayer`.
//! - **Multi-in / multi-out is the default assumption**, even when the current
//!   traffic is one or two refs. All handling goes through the sink pattern.
//! - **Zero change to engine core.** Only additive Data-plane wiring; the
//!   Domain path (`submit_output` / `output_tail` / dispatch verdict) stays as
//!   it was.
//!
//! ## History
//!
//! The former standalone `mlua-swarm-output-store` crate was folded into
//! `engine-core` as a module (a Repository/Store sibling to `issue_store` /
//! `blueprint_store`). Independent distribution, separate ownership, and
//! dependency isolation did not justify the crate boundary. The duplicated
//! `OutputEvent` / `ContentRef` in `worker/output.rs` were absorbed here
//! (canonical), and `worker/output.rs` was narrowed to re-exports plus the
//! engine-specific `OutputSink` / `EngineSink`.

pub mod sqlite;
pub use sqlite::SqliteOutputStore;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

/// Errors surfaced by the output store layer.
#[derive(Debug, Error)]
pub enum OutputStoreError {
    /// The given `out_id` is not present in the store.
    #[error("output not found: {0}")]
    NotFound(String),
    /// Internal invariant violation (i.e. an implementation bug).
    #[error("internal: {0}")]
    Internal(String),
}

/// Reference handle for a stored output (the id carried by `IN_REFS` at LC2).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutputRef(
    /// The `out-`-prefixed short id string.
    pub String,
);

impl OutputRef {
    /// Allocate a fresh short reference (`out-` + 10 hex chars).
    ///
    /// Uses the same in-process-unique id form as `wh-` worker handles
    /// (`types::uid_hex`). Short ids are a deliberate trade: legible / cheap
    /// to carry in prompts, not unguessable — access control is the auth
    /// gate's job, not the id's.
    pub fn new() -> Self {
        OutputRef(format!("out-{}", crate::types::uid_hex(5)))
    }
}

impl Default for OutputRef {
    fn default() -> Self {
        Self::new()
    }
}

/// A single output event submitted from a worker into the engine.
///
/// The only event type after `WorkerResult` was folded into this enum. The
/// `SpawnerAdapter` is responsible for turning the wire form (stdout / NDJSON /
/// file path / IPC) into this typed representation at the boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputEvent {
    /// Progress marker (state name / note); carries no payload.
    Progress {
        /// Stage name.
        stage: String,
        /// Optional note.
        note: Option<String>,
    },
    /// Streaming chunk (LLM tokens, partial JSON, etc.).
    Partial {
        /// The chunk itself.
        chunk: ContentRef,
    },
    /// Named artifact (file / blob / intermediate product).
    Artifact {
        /// Artifact name.
        name: String,
        /// Artifact body.
        content: ContentRef,
    },
    /// Terminal event (the former `WorkerResult`). Exactly one per attempt,
    /// emitted last.
    Final {
        /// Output body.
        content: ContentRef,
        /// Transport-level success flag. This is the only piece of information
        /// the engine's dispatch path consults for flow control. Domain-level
        /// verdicts (e.g. `"blocked"`) live as plain data inside `content` and
        /// are consumed by Flow.ir conds (`Eq($.<step>.verdict, Lit(..))`).
        ok: bool,
    },
}

/// How content travels — inline value or file path. Streaming is not carried
/// as its own variant in this iteration.
///
/// The `SpawnerAdapter` picks the appropriate variant at the boundary. When
/// metadata is unavailable it is acceptable to fall back to `Inline` with the
/// raw value, prioritising basic functionality over metadata fidelity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ContentRef {
    /// Inline JSON. Kilobyte-scale, the default for structured data.
    Inline {
        /// JSON body.
        value: Value,
    },
    /// File-path handoff for large / binary / artifact content. File
    /// ownership and cleanup belong to the engine side (carry); the spawner
    /// only hands the path over.
    FileRef {
        /// File path.
        path: PathBuf,
        /// MIME hint.
        mime: Option<String>,
        /// Size hint in bytes.
        size_hint: Option<u64>,
    },
}

impl ContentRef {
    /// Wrap a `serde_json::Value` as an `Inline` content ref.
    pub fn inline(value: Value) -> Self {
        ContentRef::Inline { value }
    }

    /// Wrap raw text as an `Inline` content ref (the common path for a
    /// `ProcessSpawner` running in plain mode).
    pub fn inline_text(text: impl Into<String>) -> Self {
        ContentRef::Inline {
            value: Value::String(text.into()),
        }
    }

    /// `FileRef` helper. Fill in `mime` / `size_hint` when the spawner knows
    /// them.
    pub fn file_ref(
        path: impl Into<PathBuf>,
        mime: Option<String>,
        size_hint: Option<u64>,
    ) -> Self {
        ContentRef::FileRef {
            path: path.into(),
            mime,
            size_hint,
        }
    }
}

/// Metadata for one registered output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputRecord {
    /// Allocated id.
    pub id: OutputRef,
    /// Producing task.
    pub task_id: String,
    /// Attempt number.
    pub attempt: u32,
    /// Producing agent name.
    pub producer_agent: String,
    /// The event itself.
    pub event: OutputEvent,
    /// Parent output refs (chained ids received via handoff).
    pub parent_refs: Vec<OutputRef>,
}

/// The LC3 (Swarm management) interface.
///
/// Backing implementations are pluggable (in-memory, SQLite, filesystem,
/// etc.). The MVP ships only the in-memory backend; SQLite / filesystem
/// backends are a future carry.
#[async_trait]
pub trait OutputStore: Send + Sync {
    /// Intake an event, allocate an id, and register the record. Returns the
    /// freshly allocated ref.
    async fn append(
        &self,
        task_id: &str,
        attempt: u32,
        producer_agent: &str,
        event: OutputEvent,
        parent_refs: Vec<OutputRef>,
    ) -> Result<OutputRef, OutputStoreError>;

    /// Look up a record by id (LC2 `IN_REFS` resolution — the value handed
    /// to the next spawn on handoff).
    async fn get(&self, id: &OutputRef) -> Result<OutputRecord, OutputStoreError>;

    /// Look up the **latest** record emitted under the given producer name
    /// (`out_name` addressing — the logical, agent-based sibling of `get`).
    /// Names are producer-scoped, not task-scoped: the newest emit wins.
    async fn get_latest_by_name(&self, name: &str) -> Result<OutputRecord, OutputStoreError>;

    /// GH #23 Layer 2 — the Run-scoped sibling of [`Self::get_latest_by_name`].
    /// Looks up the **latest** record emitted under `name`, restricted to
    /// one `(task_id, attempt)` run. [`Self::get_latest_by_name`] resolves
    /// across every Run the store has ever seen, so two concurrently
    /// dispatched Runs that happen to share a producer name race each
    /// other (documented as a `KNOWN LIMITATION` in
    /// `crates/mlua-swarm-server/src/projection.rs`); this method closes
    /// that race by construction — the newest emit for `name` *inside*
    /// `(task_id, attempt)` wins, other Runs' emits under the same name
    /// are invisible to it.
    async fn get_latest_by_name_in_run(
        &self,
        task_id: &str,
        attempt: u32,
        name: &str,
    ) -> Result<OutputRecord, OutputStoreError>;

    /// List every record for a given `(task_id, attempt)` pair. Used where
    /// the dispatch path pulls the verdict view.
    async fn list_for_attempt(
        &self,
        task_id: &str,
        attempt: u32,
    ) -> Result<Vec<OutputRecord>, OutputStoreError>;
}

/// MVP implementation — in-memory, and the default for tests and prototyping.
///
/// Production deployments swap in a SQLite or filesystem backend (carry).
#[derive(Debug, Default, Clone)]
pub struct InMemoryOutputStore {
    inner: Arc<Mutex<InMemoryInner>>,
}

#[derive(Debug, Default)]
struct InMemoryInner {
    by_id: HashMap<OutputRef, OutputRecord>,
    by_attempt: HashMap<(String, u32), Vec<OutputRef>>,
    /// producer_agent → emitted refs in insertion order (last = latest).
    by_name: HashMap<String, Vec<OutputRef>>,
    /// `(task_id, attempt, producer_agent)` → emitted refs in insertion
    /// order (last = latest) — the Run-scoped sibling of `by_name` (GH #23
    /// Layer 2, backs [`OutputStore::get_latest_by_name_in_run`]). Same
    /// shape as `by_attempt` plus the name dimension, so a producer name
    /// shared by two concurrent `(task_id, attempt)` Runs never
    /// cross-resolves.
    by_name_run: HashMap<(String, u32, String), Vec<OutputRef>>,
}

impl InMemoryOutputStore {
    /// Construct a fresh, empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl OutputStore for InMemoryOutputStore {
    async fn append(
        &self,
        task_id: &str,
        attempt: u32,
        producer_agent: &str,
        event: OutputEvent,
        parent_refs: Vec<OutputRef>,
    ) -> Result<OutputRef, OutputStoreError> {
        let id = OutputRef::new();
        let record = OutputRecord {
            id: id.clone(),
            task_id: task_id.to_string(),
            attempt,
            producer_agent: producer_agent.to_string(),
            event,
            parent_refs,
        };
        let mut guard = self.inner.lock().await;
        guard.by_id.insert(id.clone(), record);
        guard
            .by_attempt
            .entry((task_id.to_string(), attempt))
            .or_default()
            .push(id.clone());
        guard
            .by_name
            .entry(producer_agent.to_string())
            .or_default()
            .push(id.clone());
        guard
            .by_name_run
            .entry((task_id.to_string(), attempt, producer_agent.to_string()))
            .or_default()
            .push(id.clone());
        Ok(id)
    }

    async fn get(&self, id: &OutputRef) -> Result<OutputRecord, OutputStoreError> {
        let guard = self.inner.lock().await;
        guard
            .by_id
            .get(id)
            .cloned()
            .ok_or_else(|| OutputStoreError::NotFound(id.0.clone()))
    }

    async fn get_latest_by_name(&self, name: &str) -> Result<OutputRecord, OutputStoreError> {
        let guard = self.inner.lock().await;
        let latest = guard
            .by_name
            .get(name)
            .and_then(|ids| ids.last())
            .ok_or_else(|| OutputStoreError::NotFound(name.to_string()))?;
        guard
            .by_id
            .get(latest)
            .cloned()
            .ok_or_else(|| OutputStoreError::Internal(format!("name index dangling: {name}")))
    }

    async fn get_latest_by_name_in_run(
        &self,
        task_id: &str,
        attempt: u32,
        name: &str,
    ) -> Result<OutputRecord, OutputStoreError> {
        let guard = self.inner.lock().await;
        let key = (task_id.to_string(), attempt, name.to_string());
        let latest = guard
            .by_name_run
            .get(&key)
            .and_then(|ids| ids.last())
            .ok_or_else(|| OutputStoreError::NotFound(format!("{task_id}/{attempt}/{name}")))?;
        guard.by_id.get(latest).cloned().ok_or_else(|| {
            OutputStoreError::Internal(format!(
                "name-in-run index dangling: {task_id}/{attempt}/{name}"
            ))
        })
    }

    async fn list_for_attempt(
        &self,
        task_id: &str,
        attempt: u32,
    ) -> Result<Vec<OutputRecord>, OutputStoreError> {
        let guard = self.inner.lock().await;
        let ids = guard
            .by_attempt
            .get(&(task_id.to_string(), attempt))
            .cloned()
            .unwrap_or_default();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(r) = guard.by_id.get(&id) {
                out.push(r.clone());
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn append_then_get_roundtrip() {
        let store = InMemoryOutputStore::new();
        let event = OutputEvent::Final {
            content: ContentRef::Inline {
                value: Value::String("hello".into()),
            },
            ok: true,
        };
        let id = store
            .append("task-1", 1, "agent-a", event.clone(), vec![])
            .await
            .expect("append");
        let got = store.get(&id).await.expect("get");
        assert_eq!(got.id, id);
        assert_eq!(got.task_id, "task-1");
        assert_eq!(got.attempt, 1);
        assert_eq!(got.producer_agent, "agent-a");
        match got.event {
            OutputEvent::Final { ok, .. } => assert!(ok),
            _ => panic!("wrong event variant"),
        }
    }

    #[tokio::test]
    async fn list_for_attempt_orders_by_insertion() {
        let store = InMemoryOutputStore::new();
        let e1 = OutputEvent::Progress {
            stage: "s1".into(),
            note: None,
        };
        let e2 = OutputEvent::Progress {
            stage: "s2".into(),
            note: None,
        };
        let id1 = store.append("t", 1, "a", e1, vec![]).await.expect("append");
        let id2 = store.append("t", 1, "a", e2, vec![]).await.expect("append");
        let list = store.list_for_attempt("t", 1).await.expect("list");
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].id, id1);
        assert_eq!(list[1].id, id2);
    }

    #[tokio::test]
    async fn out_ref_is_short_prefixed_form() {
        let r = OutputRef::new();
        assert!(r.0.starts_with("out-"), "prefix: {}", r.0);
        let hex = &r.0["out-".len()..];
        assert_eq!(hex.len(), 10, "10 hex chars: {}", r.0);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()), "hex: {}", r.0);
    }

    #[tokio::test]
    async fn get_latest_by_name_returns_newest_emit() {
        let store = InMemoryOutputStore::new();
        let e = |s: &str| OutputEvent::Progress {
            stage: s.into(),
            note: None,
        };
        store
            .append("t", 1, "agent-a", e("first"), vec![])
            .await
            .expect("append 1");
        let id2 = store
            .append("t2", 1, "agent-a", e("second"), vec![])
            .await
            .expect("append 2");
        // no producer bleed-through between attempts
        store
            .append("t", 1, "agent-b", e("other"), vec![])
            .await
            .expect("append 3");
        let got = store.get_latest_by_name("agent-a").await.expect("by name");
        assert_eq!(got.id, id2, "latest emit wins");
        assert_eq!(got.task_id, "t2");
    }

    #[tokio::test]
    async fn get_latest_by_name_unknown_returns_not_found() {
        let store = InMemoryOutputStore::new();
        let err = store.get_latest_by_name("nobody").await.unwrap_err();
        assert!(matches!(err, OutputStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn get_latest_by_name_in_run_does_not_cross_resolve_between_runs() {
        let store = InMemoryOutputStore::new();
        let e = |s: &str| OutputEvent::Progress {
            stage: s.into(),
            note: None,
        };
        let id_t1 = store
            .append("t1", 1, "same-producer", e("run-1"), vec![])
            .await
            .expect("append t1");
        let id_t2 = store
            .append("t2", 1, "same-producer", e("run-2"), vec![])
            .await
            .expect("append t2");

        let got_t1 = store
            .get_latest_by_name_in_run("t1", 1, "same-producer")
            .await
            .expect("run t1 lookup");
        assert_eq!(got_t1.id, id_t1, "must not cross-resolve to t2's emit");

        let got_t2 = store
            .get_latest_by_name_in_run("t2", 1, "same-producer")
            .await
            .expect("run t2 lookup");
        assert_eq!(got_t2.id, id_t2, "must not cross-resolve to t1's emit");
    }

    #[tokio::test]
    async fn get_latest_by_name_in_run_returns_newest_within_run() {
        let store = InMemoryOutputStore::new();
        let e = |s: &str| OutputEvent::Progress {
            stage: s.into(),
            note: None,
        };
        store
            .append("t", 1, "p", e("first"), vec![])
            .await
            .expect("append 1");
        let id2 = store
            .append("t", 1, "p", e("second"), vec![])
            .await
            .expect("append 2");
        let got = store
            .get_latest_by_name_in_run("t", 1, "p")
            .await
            .expect("lookup");
        assert_eq!(got.id, id2, "latest emit within the run wins");
    }

    #[tokio::test]
    async fn get_latest_by_name_in_run_wrong_run_returns_not_found() {
        let store = InMemoryOutputStore::new();
        store
            .append(
                "t",
                1,
                "same-producer",
                OutputEvent::Progress {
                    stage: "x".into(),
                    note: None,
                },
                vec![],
            )
            .await
            .expect("append");
        // Right name, wrong attempt — must not fall back to a different run.
        let err = store
            .get_latest_by_name_in_run("t", 2, "same-producer")
            .await
            .unwrap_err();
        assert!(matches!(err, OutputStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn get_not_found_returns_error() {
        let store = InMemoryOutputStore::new();
        let missing = OutputRef("missing".into());
        let err = store.get(&missing).await.unwrap_err();
        assert!(matches!(err, OutputStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn parent_refs_are_persisted() {
        let store = InMemoryOutputStore::new();
        let parent = OutputRef::new();
        let event = OutputEvent::Final {
            content: ContentRef::Inline { value: Value::Null },
            ok: true,
        };
        let id = store
            .append("t", 1, "a", event, vec![parent.clone()])
            .await
            .expect("append");
        let got = store.get(&id).await.expect("get");
        assert_eq!(got.parent_refs, vec![parent]);
    }
}
