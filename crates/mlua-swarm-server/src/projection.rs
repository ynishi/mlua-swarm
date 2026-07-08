//! `McpQueryAdapter` — server-side [`ProjectionAdapter`], and the REST
//! hierarchy that serves a Run's step OUTPUT as metadata + content
//! (`projection-adapter` ST5's HTTP debug plane — replaces the ST2/ST4
//! `GET /v1/tasks/:id/ctx` single-value endpoint / `ProjectionResponse`).
//!
//! # Two consumers, two roles (ST5)
//!
//! - **Worker axis** (`crates/mlua-swarm-server/src/worker.rs`'s `GET
//!   /v1/worker/prompt` handler) — the *primary* supply path. A worker's
//!   fetch payload carries `context.steps: Vec<StepPointer>`, a
//!   `ContextPolicy.steps`-filtered pointer list assembled automatically at
//!   fetch time; no separate tool call needed.
//! - **HTTP debug plane** (this module's `GET
//!   /v1/tasks/:id/runs/:run/steps*` routes) — the content the above
//!   pointers' `content_url` addresses, plus an unfiltered metadata/content
//!   view for operators / humans debugging a run.
//!
//! Both consumers share [`McpQueryAdapter::list_steps`]'s enumeration:
//! every distinct `step_ref` name in `RunRecord.step_entries`, resolved
//! through the Data-plane `OutputStore` (in-flight-safe — see below),
//! **union** `RunRecord.result_ref`'s top-level object keys (the
//! finalized-Run fallback) — a name present in both wins on the Data-plane
//! side (same rule [`McpQueryAdapter::resolve_run`]'s single-key sibling,
//! [`McpQueryAdapter::resolve_async`], already applies). Name-namespace
//! unification (Data-plane producer names vs. flow.ir ctx-path segments)
//! is tracked separately (see the KNOWN LIMITATION note below); this module
//! does not resolve it.
//!
//! # Architecture (subtask-4 rework, carried into ST5)
//!
//! [`McpQueryAdapter`] reads through **two** backings, tried in order:
//!
//! 1. **Data-plane, in-flight-safe** (subtask-4's whole reason for being):
//!    when `key.step` is `Some(producer_agent)` and no explicit `run_id`
//!    pins an older Run, [`McpQueryAdapter::resolve_async`] first tries
//!    `OutputStore::get_latest_by_name(producer_agent)` — the same store
//!    `Engine::submit_output`'s submit-time projection sink dual-writes
//!    into (see `mlua_swarm::core::engine::Engine::submit_output`'s doc).
//!    A hit here can be a **not-yet-finalized** Run's already-submitted
//!    step — the in-flight case this rework exists for.
//! 2. **Persisted `RunRecord.result_ref` fallback** (the pre-rework path,
//!    unchanged): used whenever (1) is skipped (`key.step` is `None`, or
//!    an explicit `run_id` was given) or comes back empty (no Data-plane
//!    record under that producer name yet — e.g. a Run that predates the
//!    engine having an `OutputStore` wired, or `key.step` names a flow.ir
//!    ctx-path segment rather than an agent ref — see the KNOWN
//!    LIMITATION note below).
//!
//! Unlike `crate::operator_ws::session`'s spawn-time
//! [`mlua_swarm::core::projection::FileProjectionAdapter`] hook (which
//! materializes the *spawning* agent's own `AgentContextView`), this
//! adapter's Data-plane path serves **prior steps'** submitted OUTPUT —
//! the pull-supply counterpart to `Engine`'s submit-time file sink.
//!
//! ## KNOWN LIMITATION
//!
//! `OutputStore::get_latest_by_name` is producer-name-scoped, not
//! Run-scoped (see `mlua_swarm::store::output`'s module doc) — it returns
//! the single newest `Final` submitted anywhere under that producer name,
//! across every Run / Task. This adapter narrows the blast radius by only
//! taking this path when an explicit `run_id` did NOT pin an older Run
//! (an explicit pin always uses the Run-scoped `result_ref` fallback
//! instead), but two *concurrent* Runs whose flow.ir happens to dispatch
//! an agent of the identical name can still race each other on this path.
//! This is an accepted, pre-existing characteristic of the Data-plane
//! store (not a new race introduced here) — see
//! `mlua_swarm::store::output::OutputStore::get_latest_by_name`'s doc.
//!
//! [`ProjectionAdapter::fetch`] is a synchronous trait method, but this
//! adapter's backing stores are async. [`McpQueryAdapter::resolve_async`]
//! is the real, native-async implementation; [`step_content`] (the
//! content-plane HTTP handler) calls [`McpQueryAdapter::list_steps`]
//! directly. [`ProjectionAdapter::fetch`] instead bridges to
//! [`McpQueryAdapter::resolve_async`] via `tokio::task::block_in_place` +
//! `Handle::block_on` purely for trait conformance (dependency inversion —
//! this adapter implements the same `core::projection::ProjectionAdapter`
//! trait [`mlua_swarm::core::projection::FileProjectionAdapter`] does, so a
//! caller holding a `dyn ProjectionAdapter` can use either
//! polymorphically); the hot HTTP path never takes that bridge.

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    Json,
};
use mlua_swarm::core::projection::{
    ProjectionAdapter, ProjectionError, ProjectionKey, ProjectionRef,
};
use mlua_swarm::store::output::{ContentRef, OutputEvent, OutputStore, OutputStoreError};
use mlua_swarm::store::run::{RunRecord, RunStore};
use mlua_swarm::{RunId, StepId, TaskId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest as _;
use std::sync::Arc;

use crate::tasks::map_task_store_err;
use crate::{ApiError, AppState};

/// Server-side [`ProjectionAdapter`] backed by an [`OutputStore`]
/// (in-flight-safe, subtask-4) with a [`RunStore`]-backed `result_ref`
/// fallback (see the module doc for the full narrative).
pub struct McpQueryAdapter {
    data_store: Arc<dyn OutputStore>,
    run_store: Arc<dyn RunStore>,
}

/// Which backing produced a [`StepSummary`] / a Worker-axis `StepPointer`
/// — Data-plane wins a name collision (module doc's "Architecture"
/// section).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionSource {
    /// Resolved via the in-flight-safe `OutputStore::get_latest_by_name`
    /// path.
    DataPlane,
    /// Resolved via the persisted `RunRecord.result_ref` fallback (the Run
    /// has finalized, or the name only ever existed there).
    ResultRef,
}

/// One step's resolved OUTPUT value plus its provenance — the shared
/// enumeration result [`McpQueryAdapter::list_steps`] returns, consumed by
/// both this module's HTTP handlers and
/// `crates/mlua-swarm-server/src/worker.rs`'s Worker-axis pointer
/// assembly.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedStep {
    /// The producing step's name (`RunRecord.step_entries[].step_ref`, or
    /// a `RunRecord.result_ref` top-level key).
    pub(crate) name: String,
    /// The resolved OUTPUT value (not yet path-narrowed).
    pub(crate) value: Value,
    /// Which backing produced this entry.
    pub(crate) source: ProjectionSource,
}

/// Extracts a JSON value out of an [`OutputEvent`]'s content, when the
/// event is a `Final` (anything else — `Progress` / `Partial` / `Artifact`
/// sharing the same producer name via the separate `POST /v1/data/emit`
/// axis — is not a submission this adapter serves, so callers treat
/// `None` the same as "no record").
fn final_value(event: &OutputEvent) -> Option<Value> {
    match event {
        OutputEvent::Final { content, .. } => Some(content_to_value(content)),
        _ => None,
    }
}

/// Renders a [`ContentRef`] down to a plain [`Value`] — `Inline` passes
/// its value through verbatim; `FileRef` (large / binary content) becomes
/// a small locator object (this adapter's `v1` scope does not read the
/// file back, matching subtask-4's spec: "locator 返却で可").
fn content_to_value(content: &ContentRef) -> Value {
    match content {
        ContentRef::Inline { value } => value.clone(),
        ContentRef::FileRef {
            path,
            mime,
            size_hint,
        } => serde_json::json!({
            "file_ref": path.to_string_lossy(),
            "mime": mime,
            "size_hint": size_hint,
        }),
    }
}

impl McpQueryAdapter {
    /// Builds an adapter reading through `data_store` (in-flight-safe,
    /// tried first) with `run_store`-backed `result_ref` fallback.
    pub fn new(data_store: Arc<dyn OutputStore>, run_store: Arc<dyn RunStore>) -> Self {
        Self {
            data_store,
            run_store,
        }
    }

    /// Selects the Run `task_id` + `run_id` address: `run_id` when
    /// `Some`, otherwise the most recently created Run for `task_id`
    /// ([`RunStore::list_by_task`] returns oldest-created-first, so its
    /// last element is the latest). [`ProjectionError::NotFound`] covers
    /// every "nothing here" case uniformly: an unparseable `run_id`, an
    /// unknown Run, a `run_id` that names a Run belonging to a *different*
    /// Task, or a Task with no Runs yet.
    async fn resolve_run(
        &self,
        task_id: &TaskId,
        run_id: Option<&str>,
    ) -> Result<RunRecord, ProjectionError> {
        match run_id {
            Some(rid) => {
                let run_id = RunId::parse(rid.to_string())
                    .map_err(|e| ProjectionError::InvalidKey(format!("run_id: {e}")))?;
                let run = self.run_store.get(&run_id).await.map_err(|_| {
                    ProjectionError::NotFound(ProjectionKey {
                        task_id: task_id.to_string(),
                        run_id: Some(rid.to_string()),
                        step: None,
                        path: None,
                    })
                })?;
                if &run.task_id != task_id {
                    return Err(ProjectionError::NotFound(ProjectionKey {
                        task_id: task_id.to_string(),
                        run_id: Some(rid.to_string()),
                        step: None,
                        path: None,
                    }));
                }
                Ok(run)
            }
            None => {
                let mut runs = self.run_store.list_by_task(task_id).await.map_err(|_| {
                    ProjectionError::NotFound(ProjectionKey {
                        task_id: task_id.to_string(),
                        run_id: None,
                        step: None,
                        path: None,
                    })
                })?;
                runs.pop().ok_or_else(|| {
                    ProjectionError::NotFound(ProjectionKey {
                        task_id: task_id.to_string(),
                        run_id: None,
                        step: None,
                        path: None,
                    })
                })
            }
        }
    }

    /// The real, native-async single-key resolve: selects the Run `key`
    /// addresses via [`Self::resolve_run`], then resolves the value —
    /// Data-plane first (in-flight-safe), falling back to the selected
    /// Run's persisted `result_ref` — see the module doc's Architecture
    /// section. Returns the selected [`RunRecord`] alongside the resolved
    /// value so a caller can report which Run actually served the
    /// projection, even when the caller only supplied `task_id`.
    async fn resolve_async(
        &self,
        key: &ProjectionKey,
    ) -> Result<(RunRecord, Value), ProjectionError> {
        let task_id = TaskId::parse(key.task_id.clone())
            .map_err(|e| ProjectionError::InvalidKey(format!("task_id: {e}")))?;
        let run = self.resolve_run(&task_id, key.run_id.as_deref()).await?;

        // Data-plane, in-flight-safe path: only when `step` names a
        // producer agent AND no explicit `run_id` pinned an older Run (see
        // the module doc's KNOWN LIMITATION).
        if key.run_id.is_none() {
            if let Some(step) = &key.step {
                match self.data_store.get_latest_by_name(step).await {
                    Ok(record) => {
                        if let Some(value) = final_value(&record.event) {
                            let narrowed = match &key.path {
                                None => Some(value),
                                Some(_) => {
                                    // Reuse `ProjectionKey::resolve`'s path-walk
                                    // only (the step lookup is already done —
                                    // this value IS the step's own content, not
                                    // a `{step: value}` map to look `step` up
                                    // in again).
                                    let path_only = ProjectionKey {
                                        task_id: key.task_id.clone(),
                                        run_id: key.run_id.clone(),
                                        step: None,
                                        path: key.path.clone(),
                                    };
                                    path_only.resolve(&value).cloned()
                                }
                            };
                            if let Some(value) = narrowed {
                                return Ok((run, value));
                            }
                        }
                    }
                    Err(OutputStoreError::NotFound(_)) => {
                        // No Data-plane record under this producer name —
                        // fall through to the result_ref fallback below.
                    }
                    Err(other) => {
                        return Err(ProjectionError::Io(std::io::Error::other(format!(
                            "OutputStore::get_latest_by_name: {other}"
                        ))));
                    }
                }
            }
        }

        // Fallback: the pre-rework, Run-scoped `result_ref` path.
        let ctx_data = run.result_ref.clone().unwrap_or(Value::Null);
        let value = key
            .resolve(&ctx_data)
            .cloned()
            .ok_or_else(|| ProjectionError::NotFound(key.clone()))?;
        Ok((run, value))
    }

    /// Enumerates every step visible for the Run addressed by `task_id` +
    /// `run_id` (`None` = latest) — the shared enumeration both this
    /// module's HTTP handlers and the Worker axis's pointer assembly
    /// build from (module doc). Returns the selected [`RunRecord`]
    /// alongside the resolved steps.
    pub(crate) async fn list_steps(
        &self,
        task_id: &TaskId,
        run_id: Option<&str>,
    ) -> Result<(RunRecord, Vec<ResolvedStep>), ProjectionError> {
        let run = self.resolve_run(task_id, run_id).await?;
        let steps = self.enumerate_steps(&run).await;
        Ok((run, steps))
    }

    /// Same enumeration as [`Self::list_steps`], addressed directly by an
    /// already-known [`RunId`] (no `task_id` cross-check, no `"latest"`
    /// ambiguity) — the Worker axis's entry point
    /// (`crates/mlua-swarm-server/src/worker.rs`), which already has the
    /// exact Run its own `AgentContextView.run_id` names, from
    /// `Ctx.meta.runtime[RUN_ID_KEY]` (threaded through by
    /// `Engine::dispatch_attempt_with`).
    pub(crate) async fn list_steps_by_run_id(
        &self,
        run_id: &RunId,
    ) -> Result<(RunRecord, Vec<ResolvedStep>), ProjectionError> {
        let run = self.run_store.get(run_id).await.map_err(|_| {
            ProjectionError::NotFound(ProjectionKey {
                task_id: String::new(),
                run_id: Some(run_id.to_string()),
                step: None,
                path: None,
            })
        })?;
        let steps = self.enumerate_steps(&run).await;
        Ok((run, steps))
    }

    /// Data-plane `run.step_entries`' distinct `step_ref` names, resolved
    /// through `OutputStore::get_latest_by_name` — **union**
    /// `run.result_ref`'s top-level object keys not already resolved on
    /// the Data-plane side (Data-plane wins a name collision, matching
    /// [`Self::resolve_async`]'s single-key rule).
    async fn enumerate_steps(&self, run: &RunRecord) -> Vec<ResolvedStep> {
        let mut out = Vec::new();
        let mut attempted = std::collections::HashSet::new();
        let mut resolved_names = std::collections::HashSet::new();

        for entry in &run.step_entries {
            let Some(name) = &entry.step_ref else {
                continue;
            };
            if !attempted.insert(name.clone()) {
                continue;
            }
            if let Ok(record) = self.data_store.get_latest_by_name(name).await {
                if let Some(value) = final_value(&record.event) {
                    out.push(ResolvedStep {
                        name: name.clone(),
                        value,
                        source: ProjectionSource::DataPlane,
                    });
                    resolved_names.insert(name.clone());
                }
            }
        }

        if let Some(Value::Object(map)) = &run.result_ref {
            for (name, value) in map {
                if resolved_names.contains(name) {
                    continue;
                }
                out.push(ResolvedStep {
                    name: name.clone(),
                    value: value.clone(),
                    source: ProjectionSource::ResultRef,
                });
            }
        }

        out
    }
}

impl ProjectionAdapter for McpQueryAdapter {
    fn name(&self) -> &'static str {
        "mcp-query"
    }

    /// `ctx_data` is used only to fail loud up front (mirrors
    /// [`mlua_swarm::core::projection::FileProjectionAdapter::project`]'s
    /// own not-found check) — the returned [`ProjectionRef::Query`]
    /// locator carries `key` itself, not a resolved value; the real lookup
    /// happens later, at [`Self::fetch`] time, against whatever the
    /// addressed Run's backing is *then* (which may differ from
    /// `ctx_data`, e.g. after a re-kick, or once a step submits through
    /// the Data-plane store).
    fn project(
        &self,
        key: &ProjectionKey,
        ctx_data: &Value,
    ) -> Result<ProjectionRef, ProjectionError> {
        if key.task_id.is_empty() {
            return Err(ProjectionError::InvalidKey(
                "task_id must not be empty".to_string(),
            ));
        }
        key.resolve(ctx_data)
            .ok_or_else(|| ProjectionError::NotFound(key.clone()))?;
        Ok(ProjectionRef::Query {
            endpoint: format!(
                "/v1/tasks/{}/runs/{}/steps/{}/content",
                key.task_id,
                key.run_id.as_deref().unwrap_or("latest"),
                key.step.as_deref().unwrap_or("_ctx")
            ),
            key: key.clone(),
        })
    }

    fn fetch(&self, key: &ProjectionKey) -> Result<Value, ProjectionError> {
        // See the module doc: this bridge exists for `ProjectionAdapter`
        // trait conformance only. `block_in_place` requires the Tokio
        // multi-thread runtime flavor (the workspace's `tokio` dependency
        // enables `features = ["full"]`, which includes it).
        let handle = tokio::runtime::Handle::try_current().map_err(|e| {
            ProjectionError::Io(std::io::Error::other(format!(
                "McpQueryAdapter::fetch requires a Tokio runtime: {e}"
            )))
        })?;
        let (_run, value) =
            tokio::task::block_in_place(|| handle.block_on(self.resolve_async(key)))?;
        Ok(value)
    }

    fn pointer_line(&self, r: &ProjectionRef) -> String {
        match r {
            ProjectionRef::Query { endpoint, key } => {
                format!("projection(mcp-query): {endpoint} task_id={}", key.task_id)
            }
            ProjectionRef::File { path } => format!("projection(file): {path}"),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// REST hierarchy: StepList / StepSummary / content plane
// ──────────────────────────────────────────────────────────────────────────

/// Response body for `GET /v1/tasks/:id/runs/:run/steps`.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct StepList {
    /// The addressed Task.
    pub task_id: String,
    /// The Run this list resolved `:run` to (the concrete id, even when
    /// the request path said `latest`).
    pub run_id: String,
    /// Every visible step, unfiltered (the HTTP debug plane serves the
    /// full union — `ContextPolicy.steps` filtering only applies to the
    /// Worker axis's `context.steps` pointer list; see the module doc).
    pub steps: Vec<StepSummary>,
}

/// One step's metadata (operator / debug plane) — `GET
/// /v1/tasks/:id/runs/:run/steps/:step`, and each entry of
/// [`StepList::steps`].
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct StepSummary {
    /// The producing step's name.
    pub name: String,
    /// Byte length of the body [`Self::content_url`] serves (the exact
    /// bytes a `GET` of that URL returns for this same `?path=`, if any).
    pub size_bytes: u64,
    /// MIME type [`Self::content_url`] serves this body as
    /// (`text/markdown; charset=utf-8` when materialized-file-backed,
    /// `application/json` otherwise — see the module doc's Content-Type
    /// rule).
    pub content_type: String,
    /// SHA-256 hex digest of the body, matching the content endpoint's
    /// `ETag` value (`sha256:<hex>`, minus the `sha256:` prefix).
    pub sha256: String,
    /// Which backing produced this entry.
    pub source: ProjectionSource,
    /// Absolute filesystem path to the materialized projection file
    /// (`crate::core::projection::FileProjectionAdapter`'s
    /// `<root>/workspace/tasks/<step_id>/ctx/<name>.md` target), when one
    /// exists AND this entry addresses the whole step (no `?path=`
    /// narrowing — a narrowed fragment is never file-backed). `None`
    /// otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    /// Fetch URL for this step's content (`GET
    /// /v1/tasks/:id/runs/:run/steps/:step/content`, `?path=` echoed when
    /// this entry is narrowed) — absolute (`AppState.base_url`-prefixed)
    /// when the server has a configured base URL, relative otherwise.
    pub content_url: String,
    /// First <= 512 bytes of the body, UTF-8-boundary-safe (never splits
    /// a multi-byte character), with a trailing `…` when truncated.
    pub preview: String,
    /// `true` when [`Self::preview`]'s underlying byte count is shorter
    /// than [`Self::size_bytes`] (the body was truncated to build the
    /// preview).
    pub truncated: bool,
}

/// Query params shared by the metadata and content routes: narrows a
/// single step's value via `$.a.b` dot-path form (the leading `$.` is
/// optional) — same syntax `mlua_swarm::core::projection::ProjectionKey`
/// already establishes.
#[derive(Debug, Deserialize, Default, schemars::JsonSchema)]
pub struct StepPathQuery {
    /// `$.a.b` narrowing within the step's value. `None` = the whole
    /// step value.
    #[serde(default)]
    pub path: Option<String>,
}

/// Narrows `value` by `path` (reuses [`ProjectionKey::resolve`]'s
/// path-walk half — the step lookup is already done, this value IS the
/// step's own content).
fn narrow_step_value(value: &Value, path: Option<&str>) -> Option<Value> {
    match path {
        None => Some(value.clone()),
        Some(p) => {
            let path_only = ProjectionKey {
                task_id: String::new(),
                run_id: None,
                step: None,
                path: Some(p.to_string()),
            };
            path_only.resolve(value).cloned()
        }
    }
}

/// The materialize target [`mlua_swarm::core::projection::FileProjectionAdapter`]
/// writes to for a submission (`<root>/workspace/tasks/<step_id>/ctx/<name>.md`
/// — same convention as that adapter's own `target_path`, reconstructed
/// here because this module resolves `root` for a step *other than* the
/// one materializing it, so it cannot construct the adapter itself
/// key-first).
fn materialized_file_path(root: &str, step_id: &StepId, name: &str) -> std::path::PathBuf {
    std::path::Path::new(root)
        .join("workspace")
        .join("tasks")
        .join(step_id.to_string())
        .join("ctx")
        .join(format!("{name}.md"))
}

/// Resolves the materialized file body for `name` in `run`, when one
/// exists: finds `name`'s most recent [`mlua_swarm::store::run::StepEntry`]
/// (giving its own dispatch `StepId`), resolves that step's own
/// `AgentContextView` root (`work_dir`, falling back to `project_root` —
/// the same fallback order `Engine::submit_output`'s materialize sink
/// uses) via [`mlua_swarm::core::engine::Engine::agent_context_for`], and
/// reads the file at the resulting path back.
///
/// Only tries `attempt = 1` (the common case — a single dispatch per
/// flow.ir Step) — a step retried under the same `StepId` at a later
/// attempt is a known, accepted limitation (matching this module's other
/// KNOWN LIMITATION notes); the entry still resolves via its Data-plane /
/// `result_ref` value, just without a `file_path`.
async fn resolve_materialized_file(
    state: &AppState,
    run: &RunRecord,
    name: &str,
) -> Option<(std::path::PathBuf, Vec<u8>)> {
    let step_id = run
        .step_entries
        .iter()
        .rev()
        .find(|e| e.step_ref.as_deref() == Some(name))
        .map(|e| e.step_id.clone())?;
    let view = state.engine.agent_context_for(&step_id, 1).await?;
    let root = view.work_dir.clone().or(view.project_root.clone())?;
    let path = materialized_file_path(&root, &step_id, name);
    let bytes = std::fs::read(&path).ok()?;
    Some((path, bytes))
}

/// Renders the body [`Self`]'s content endpoint serves for `step`,
/// narrowed by `path` when `Some`: whole-step + materialized-file-backed
/// → the raw file bytes (`text/markdown; charset=utf-8`); anything else →
/// the (possibly narrowed) value as pretty JSON (`application/json`).
/// Returns `None` when `path` is `Some` and does not resolve against
/// `step.value` (the caller's 404 case).
async fn render_step_body(
    state: &AppState,
    run: &RunRecord,
    step: &ResolvedStep,
    path: Option<&str>,
) -> Option<(Vec<u8>, &'static str, Option<String>)> {
    if path.is_none() {
        if let Some((file_path, bytes)) = resolve_materialized_file(state, run, &step.name).await {
            return Some((
                bytes,
                "text/markdown; charset=utf-8",
                Some(file_path.to_string_lossy().into_owned()),
            ));
        }
    }
    let narrowed = narrow_step_value(&step.value, path)?;
    let body = serde_json::to_vec_pretty(&narrowed).ok()?;
    Some((body, "application/json", None))
}

/// First <= 512 bytes of `body`, UTF-8-boundary-safe (never splits a
/// multi-byte character), with a trailing `…` when truncated. Returns
/// `(preview, truncated)`. `body` is expected to be valid UTF-8 (JSON /
/// materialized-markdown text, per [`render_step_body`]'s own two output
/// shapes); a malformed byte sequence falls back to a lossy decode rather
/// than panicking.
fn build_preview(body: &[u8]) -> (String, bool) {
    const MAX_PREVIEW_BYTES: usize = 512;
    if body.len() <= MAX_PREVIEW_BYTES {
        return (String::from_utf8_lossy(body).into_owned(), false);
    }
    let preview = match std::str::from_utf8(body) {
        Ok(s) => {
            let mut end = MAX_PREVIEW_BYTES;
            while end > 0 && !s.is_char_boundary(end) {
                end -= 1;
            }
            s[..end].to_string()
        }
        Err(_) => String::from_utf8_lossy(&body[..MAX_PREVIEW_BYTES]).into_owned(),
    };
    (format!("{preview}…"), true)
}

/// `GET /v1/tasks/:id/runs/:run/steps/:step/content`'s URL — absolute
/// (`base_url`-prefixed) when the server has one configured, relative
/// otherwise. `path` is echoed back as `?path=` verbatim (unencoded — the
/// dot-path syntax this module accepts uses no characters reserved in a
/// URL query component).
fn build_content_url(
    base_url: &Option<Arc<str>>,
    task_id: &TaskId,
    run_id: &RunId,
    name: &str,
    path: Option<&str>,
) -> String {
    let mut url = format!("/v1/tasks/{task_id}/runs/{run_id}/steps/{name}/content");
    if let Some(p) = path {
        url.push_str("?path=");
        url.push_str(p);
    }
    match base_url {
        Some(base) => format!("{}{}", base.trim_end_matches('/'), url),
        None => url,
    }
}

/// Builds the full [`StepSummary`] for `step`, narrowed by `path` when
/// `Some`. `None` when `path` does not resolve (the caller's 404 case).
async fn build_step_summary(
    state: &AppState,
    run: &RunRecord,
    step: &ResolvedStep,
    path: Option<&str>,
) -> Option<StepSummary> {
    let (body, content_type, file_path) = render_step_body(state, run, step, path).await?;
    let sha256 = hex::encode(sha2::Sha256::digest(&body));
    let size_bytes = body.len() as u64;
    let (preview, truncated) = build_preview(&body);
    let content_url = build_content_url(&state.base_url, &run.task_id, &run.id, &step.name, path);
    Some(StepSummary {
        name: step.name.clone(),
        size_bytes,
        content_type: content_type.to_string(),
        sha256,
        source: step.source,
        file_path,
        content_url,
        preview,
        truncated,
    })
}

/// Fields a Worker-axis
/// [`mlua_swarm::core::agent_context::StepPointer`] needs —
/// `crates/mlua-swarm-server/src/worker.rs`'s `GET /v1/worker/prompt`
/// handler builds one per visible, policy-allowed step from this.
/// Reuses the same whole-step body [`render_step_body`] renders for the
/// content endpoint (`path = None`), so `sha256` / `size_bytes` always
/// matches what a `GET` of the returned `content_url` serves. `None`
/// when the body cannot be rendered at all (mirrors this crate's other
/// best-effort projection hooks — never turns a would-have-succeeded
/// fetch into a failure; the caller just omits this step's pointer).
pub(crate) async fn resolve_step_pointer_fields(
    state: &AppState,
    run: &RunRecord,
    step: &ResolvedStep,
) -> Option<(u64, Option<String>, String, String)> {
    let (body, _content_type, file_path) = render_step_body(state, run, step, None).await?;
    let sha256 = hex::encode(sha2::Sha256::digest(&body));
    let size_bytes = body.len() as u64;
    let content_url = build_content_url(&state.base_url, &run.task_id, &run.id, &step.name, None);
    Some((size_bytes, file_path, content_url, sha256))
}

/// Shared resolve: `:id` → `TaskId` (existence-checked against
/// `state.task_store` first, so an unknown Task returns its own 404
/// distinct from an unknown Run) + `:run` (`"latest"` or an explicit
/// `R-<hex>`) → the addressed [`RunRecord`] and its enumerated
/// [`ResolvedStep`]s.
async fn resolve_run_and_steps(
    state: &AppState,
    id: &str,
    run: &str,
) -> Result<(RunRecord, Vec<ResolvedStep>), ApiError> {
    let task_id = TaskId::parse(id.to_string())
        .map_err(|e| ApiError::bad_request(format!("invalid task id: {e}")))?;
    state
        .task_store
        .get(&task_id)
        .await
        .map_err(map_task_store_err)?;
    let adapter = McpQueryAdapter::new(state.data_store.clone(), state.run_store.clone());
    let run_sel = if run == "latest" { None } else { Some(run) };
    adapter
        .list_steps(&task_id, run_sel)
        .await
        .map_err(map_projection_err)
}

/// `GET /v1/tasks/:id/runs/:run/steps` — every step visible for the
/// addressed Run, unfiltered (see the module doc's role split).
pub async fn steps_list(
    State(state): State<AppState>,
    Path((id, run)): Path<(String, String)>,
) -> Result<Json<StepList>, ApiError> {
    let (run_record, steps) = resolve_run_and_steps(&state, &id, &run).await?;
    let mut summaries = Vec::with_capacity(steps.len());
    for step in &steps {
        if let Some(summary) = build_step_summary(&state, &run_record, step, None).await {
            summaries.push(summary);
        }
    }
    Ok(Json(StepList {
        task_id: run_record.task_id.to_string(),
        run_id: run_record.id.to_string(),
        steps: summaries,
    }))
}

/// `GET /v1/tasks/:id/runs/:run/steps/:step?path=$.a.b` — one step's
/// metadata, optionally narrowed.
pub async fn step_get(
    State(state): State<AppState>,
    Path((id, run, step)): Path<(String, String, String)>,
    Query(q): Query<StepPathQuery>,
) -> Result<Json<StepSummary>, ApiError> {
    let (run_record, steps) = resolve_run_and_steps(&state, &id, &run).await?;
    let resolved = steps
        .into_iter()
        .find(|s| s.name == step)
        .ok_or_else(|| ApiError::not_found(format!("step not found: {step}")))?;
    let summary = build_step_summary(&state, &run_record, &resolved, q.path.as_deref())
        .await
        .ok_or_else(|| ApiError::not_found(format!("path not found: {:?}", q.path)))?;
    Ok(Json(summary))
}

/// `GET /v1/tasks/:id/runs/:run/steps/:step/content?path=$.a.b` — the raw
/// body: full bytes, no envelope, no Range support. `Content-Type` and
/// `ETag` follow [`StepSummary::content_type`] / [`StepSummary::sha256`]'s
/// same rules (module doc).
pub async fn step_content(
    State(state): State<AppState>,
    Path((id, run, step)): Path<(String, String, String)>,
    Query(q): Query<StepPathQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let (run_record, steps) = resolve_run_and_steps(&state, &id, &run).await?;
    let resolved = steps
        .into_iter()
        .find(|s| s.name == step)
        .ok_or_else(|| ApiError::not_found(format!("step not found: {step}")))?;
    let (body, content_type, _file_path) =
        render_step_body(&state, &run_record, &resolved, q.path.as_deref())
            .await
            .ok_or_else(|| ApiError::not_found(format!("path not found: {:?}", q.path)))?;
    let sha256 = hex::encode(sha2::Sha256::digest(&body));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type).expect("content_type is a static ASCII literal"),
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"sha256:{sha256}\""))
            .expect("hex digest is ASCII-safe for a header value"),
    );
    Ok((StatusCode::OK, headers, body))
}

fn map_projection_err(e: ProjectionError) -> ApiError {
    match e {
        ProjectionError::NotFound(key) => {
            ApiError::not_found(format!("projection not found for key {key:?}"))
        }
        ProjectionError::InvalidKey(msg) => ApiError::bad_request(msg),
        other => ApiError::engine(other),
    }
}

// ──────────────────────────────────────────────────────────────────────────
// UT
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskLaunchRequest;
    use axum::http::StatusCode;
    use mlua_swarm::application::BlueprintRef;
    use mlua_swarm::blueprint::{
        current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
        CompilerStrategy,
    };
    use mlua_swarm::core::config::EngineCfg;
    use mlua_swarm::core::engine::Engine;
    use mlua_swarm::store::output::InMemoryOutputStore;
    use mlua_swarm::store::run::InMemoryRunStore;
    use mlua_swarm::store::task::InMemoryTaskStore;
    use serde_json::json;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    /// A single-step flow.ir Blueprint that echoes `$.greeting` into
    /// `$.out` (AG_IDENTITY wraps its input as `{"echoed": input}`), so
    /// `result_ref = {"out": {"echoed": <greeting>}}` — enough shape to
    /// exercise `step` + `path` narrowing. Mirrors `tasks.rs`'s own test
    /// helper (duplicated here rather than shared — this crate's
    /// established per-module test-helper convention; see e.g.
    /// `tasks::tests::test_state`).
    fn greeting_blueprint() -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "projection-test-greeting-bp".into(),
            flow: serde_json::from_value(json!({
                "kind": "step",
                "ref": mlua_swarm::worker::baseline::AG_IDENTITY,
                "in": {"op": "path", "at": "$.greeting"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents: vec![AgentDef {
                name: mlua_swarm::worker::baseline::AG_IDENTITY.into(),
                kind: AgentKind::RustFn,
                spec: json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
                profile: None,
                meta: None,
            }],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
        }
    }

    fn test_state() -> AppState {
        let engine = Engine::new_with_layers(EngineCfg::default(), crate::default_layer_registry());
        let compiler = mlua_swarm::Compiler::new(crate::default_registry());
        let launch = Arc::new(mlua_swarm::TaskLaunchService::new(engine.clone(), compiler));
        let data_store: Arc<dyn mlua_swarm::store::output::OutputStore> =
            Arc::new(InMemoryOutputStore::new());
        // subtask-4 / ST2 rework: wire the SAME `OutputStore` into the
        // engine's submit-time projection sink (mirrors
        // `crate::build_router_full`'s own wiring), so tests exercising the
        // Data-plane / in-flight path see ordinary worker submissions land
        // here too, not just explicit `POST /v1/data/emit` calls.
        engine.set_output_store(data_store.clone());
        AppState {
            engine,
            sessions: Arc::new(Mutex::new(crate::SessionStore::default())),
            task_app: Arc::new(mlua_swarm::TaskApplication::new_inline_only(launch)),
            ws_operator_factory: None,
            data_store,
            operator_sessions: Arc::new(Mutex::new(HashMap::new())),
            roles_to_sid: Arc::new(Mutex::new(HashMap::new())),
            task_store: Arc::new(InMemoryTaskStore::new()),
            run_store: Arc::new(InMemoryRunStore::new()),
            base_url: None,
        }
    }

    fn greeting_task_req(greeting: &str) -> TaskLaunchRequest {
        TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(greeting_blueprint()),
            },
            init_ctx: json!({ "greeting": greeting }),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            goal: Some("projection test goal".to_string()),
        }
    }

    // ─── Test 8: steps collection, data-plane ∪ result_ref union ───────────

    #[tokio::test]
    async fn steps_list_returns_data_plane_and_result_ref_union() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hello")))
            .await
            .expect("tasks_start")
            .0;

        let resp = steps_list(
            State(state.clone()),
            Path((posted.task_id.to_string(), "latest".to_string())),
        )
        .await
        .expect("steps_list")
        .0;

        assert_eq!(resp.task_id, posted.task_id.to_string());
        assert_eq!(resp.run_id, posted.run_id.to_string());
        // AG_IDENTITY's own producer name resolves via the Data-plane
        // dual-write; "out" (the flow.ir ctx-path segment) only exists in
        // `result_ref` — both must appear, with the correct `source`.
        let identity_name = mlua_swarm::worker::baseline::AG_IDENTITY;
        let identity_entry = resp
            .steps
            .iter()
            .find(|s| s.name == identity_name)
            .unwrap_or_else(|| panic!("missing {identity_name} in {:?}", resp.steps));
        assert_eq!(identity_entry.source, ProjectionSource::DataPlane);
        let out_entry = resp
            .steps
            .iter()
            .find(|s| s.name == "out")
            .unwrap_or_else(|| panic!("missing \"out\" in {:?}", resp.steps));
        assert_eq!(out_entry.source, ProjectionSource::ResultRef);
    }

    // ─── Test 9: `:run = latest` resolves to newest Run; explicit pin still works ───

    #[tokio::test]
    async fn steps_list_latest_resolves_newest_run_explicit_pin_still_works() {
        let state = test_state();
        let first = crate::tasks_start(State(state.clone()), Json(greeting_task_req("first")))
            .await
            .expect("tasks_start")
            .0;
        let (status, rekicked) = crate::tasks::task_rekick(
            State(state.clone()),
            Path(first.task_id.to_string()),
            Some(Json(crate::tasks::RunKickRequest {
                init_ctx_override: Some(json!({ "greeting": "second" })),
                task_input_override: None,
            })),
        )
        .await
        .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);

        let latest = steps_list(
            State(state.clone()),
            Path((first.task_id.to_string(), "latest".to_string())),
        )
        .await
        .expect("steps_list latest")
        .0;
        assert_eq!(latest.run_id, rekicked.0.run_id.to_string());

        let pinned = steps_list(
            State(state.clone()),
            Path((first.task_id.to_string(), first.run_id.to_string())),
        )
        .await
        .expect("steps_list pinned")
        .0;
        assert_eq!(pinned.run_id, first.run_id.to_string());
    }

    // ─── Test 10: preview <= 512 bytes, UTF-8 boundary safe, truncated flag ───

    #[tokio::test]
    async fn step_get_preview_is_utf8_boundary_safe_and_truncated_flag_is_correct() {
        let state = test_state();
        // A multi-byte fixture: repeat a 3-byte UTF-8 character (U+3042
        // "あ") past the 512-byte preview cap so the boundary-safety guard
        // is actually exercised, then wrap it as the greeting value.
        let long_value = "あ".repeat(300); // 900 bytes
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req(&long_value)))
            .await
            .expect("tasks_start")
            .0;

        let identity_name = mlua_swarm::worker::baseline::AG_IDENTITY;
        let summary = step_get(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                identity_name.to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_get")
        .0;

        assert!(
            summary.preview.len() <= 512 + "…".len(),
            "preview must stay near the 512-byte cap: {} bytes",
            summary.preview.len()
        );
        assert!(
            summary.truncated,
            "a 900-byte body must be reported truncated"
        );
        assert!(
            summary.preview.ends_with('…'),
            "truncated preview must end with an ellipsis: {}",
            summary.preview
        );
        // The boundary-safety guard: a valid `String` never panics on
        // construction from a byte slice that split a multi-byte char —
        // reaching this assertion at all is the proof (an unsafe/naive
        // byte-slice truncation would have panicked above on `str`
        // reconstruction).
        assert!(summary.preview.chars().all(|c| c != '\u{FFFD}'));
    }

    // ─── Test 11: content = full body + Content-Type branch + ETag ────────

    #[tokio::test]
    async fn step_content_in_memory_fallback_is_json_with_matching_etag() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hi")))
            .await
            .expect("tasks_start")
            .0;

        let resp = step_content(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "out".to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_content")
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("content-type header")
            .to_str()
            .expect("ascii");
        assert_eq!(content_type, "application/json");
        let etag = resp
            .headers()
            .get(header::ETAG)
            .expect("etag header")
            .to_str()
            .expect("ascii")
            .to_string();
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let expected_sha = hex::encode(sha2::Sha256::digest(&body_bytes));
        assert_eq!(etag, format!("\"sha256:{expected_sha}\""));
        let parsed: Value = serde_json::from_slice(&body_bytes).expect("valid json body");
        assert_eq!(parsed["echoed"], json!("hi"));
    }

    /// Test 11 (materialized-file half): when the producing step's
    /// submission was materialized to disk (`work_dir` resolved),
    /// `step_content` serves the RAW file bytes as `text/markdown`, not
    /// the in-memory JSON fallback.
    #[tokio::test]
    async fn step_content_materialized_file_is_served_as_markdown() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state();
        let mut req = greeting_task_req("materialized");
        req.work_dir = Some(dir.path().to_string_lossy().into_owned());
        let posted = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("tasks_start")
            .0;

        let identity_name = mlua_swarm::worker::baseline::AG_IDENTITY;
        let resp = step_content(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                identity_name.to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_content")
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("content-type header")
            .to_str()
            .expect("ascii");
        assert_eq!(content_type, "text/markdown; charset=utf-8");
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let body_str = String::from_utf8(body_bytes.to_vec()).expect("utf8 body");
        assert!(
            body_str.contains("```json"),
            "materialized file must carry the fenced json block: {body_str}"
        );
    }

    // ─── Test 12: content `?path=` narrow → application/json fragment ─────

    #[tokio::test]
    async fn step_content_path_narrow_returns_json_fragment() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("narrowed")))
            .await
            .expect("tasks_start")
            .0;

        let resp = step_content(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "out".to_string(),
            )),
            Query(StepPathQuery {
                path: Some("echoed".to_string()),
            }),
        )
        .await
        .expect("step_content narrowed")
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("content-type header")
            .to_str()
            .expect("ascii");
        assert_eq!(content_type, "application/json");
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let parsed: Value = serde_json::from_slice(&body_bytes).expect("valid json body");
        assert_eq!(parsed, json!("narrowed"));
    }

    // ─── Test 13: unknown task / run / step → 404 ───────────────────────────

    #[tokio::test]
    async fn steps_list_unknown_task_returns_404() {
        let state = test_state();
        let err = steps_list(
            State(state),
            Path(("T-does-not-exist".to_string(), "latest".to_string())),
        )
        .await
        .expect_err("unknown task must 404");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn steps_list_unknown_run_returns_404() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hi")))
            .await
            .expect("tasks_start")
            .0;
        let err = steps_list(
            State(state),
            Path((posted.task_id.to_string(), "R-does-not-exist".to_string())),
        )
        .await
        .expect_err("unknown run must 404");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn step_get_unknown_step_returns_404() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hi")))
            .await
            .expect("tasks_start")
            .0;
        let err = step_get(
            State(state),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "does-not-exist".to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect_err("unknown step must 404");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    // ─── Test 14: the old /ctx route is gone ────────────────────────────────

    #[tokio::test]
    async fn old_ctx_route_returns_404_not_found_by_router() {
        let engine = Engine::new(EngineCfg::default());
        let router = mlua_swarm_server_router_for_test(engine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{addr}/v1/tasks/T-anything/ctx"))
            .send()
            .await
            .expect("request");
        assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    }

    /// Local alias so the test above reads as "the crate's router", without
    /// importing `crate::build_router` under a name that shadows this
    /// module's own items.
    fn mlua_swarm_server_router_for_test(engine: Engine) -> axum::Router {
        crate::build_router(engine)
    }

    // ─── McpQueryAdapter: single-key resolve (still exercised standalone) ───

    #[test]
    fn mcp_query_adapter_project_builds_query_ref() {
        let adapter = McpQueryAdapter::new(
            Arc::new(InMemoryOutputStore::new()),
            Arc::new(InMemoryRunStore::new()),
        );
        let key = ProjectionKey {
            task_id: "T-abc".to_string(),
            run_id: None,
            step: Some("planner".to_string()),
            path: None,
        };
        let ctx_data = json!({"planner": {"plan": "do it"}});
        let reference = adapter.project(&key, &ctx_data).expect("project");
        match &reference {
            ProjectionRef::Query { endpoint, key: k } => {
                assert!(endpoint.contains("/steps/planner/content"));
                assert_eq!(k, &key);
            }
            other => panic!("expected Query ref, got {other:?}"),
        }
        let line = adapter.pointer_line(&reference);
        assert!(line.contains("T-abc"));
    }

    #[test]
    fn mcp_query_adapter_project_rejects_key_not_present_in_ctx_data() {
        let adapter = McpQueryAdapter::new(
            Arc::new(InMemoryOutputStore::new()),
            Arc::new(InMemoryRunStore::new()),
        );
        let key = ProjectionKey {
            task_id: "T-abc".to_string(),
            run_id: None,
            step: Some("missing".to_string()),
            path: None,
        };
        let err = adapter.project(&key, &json!({"planner": {}})).unwrap_err();
        assert!(matches!(err, ProjectionError::NotFound(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mcp_query_adapter_fetch_bridges_to_resolve_async() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("bridged")))
            .await
            .expect("tasks_start")
            .0;

        let adapter = McpQueryAdapter::new(state.data_store.clone(), state.run_store.clone());
        let key = ProjectionKey {
            task_id: posted.task_id.to_string(),
            run_id: None,
            step: Some("out".to_string()),
            path: Some("echoed".to_string()),
        };
        // `fetch` is a sync trait method that bridges to `resolve_async`
        // via `block_in_place` + `Handle::block_on` — calling it directly
        // (not via `spawn_blocking`, which runs on the *blocking* pool
        // rather than a runtime worker thread and is not a valid
        // `block_in_place` call site) from this multi-thread-flavor test
        // task is exactly the context the bridge is built for (module
        // doc).
        let value = adapter.fetch(&key).expect("fetch");
        assert_eq!(value, json!("bridged"));
    }

    // ─── subtask-4 / ST2 rework: Data-plane-backed, in-flight-safe query ───

    /// Subtask 4 Test #5: path narrowing works against the Data-plane
    /// `Final` content — `AG_IDENTITY`'s own name (the producer_agent
    /// `Engine::submit_output`'s dual-write submits under; distinct from
    /// `greeting_blueprint`'s flow.ir ctx-path segment `"out"`, which is
    /// what `mcp_query_adapter_fetch_bridges_to_resolve_async` and its
    /// siblings above still exercise via the `result_ref` fallback) is
    /// queryable directly against the Data-plane store, narrowed by
    /// `path`.
    #[tokio::test]
    async fn resolve_async_path_narrows_within_data_plane_final_content() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hi")))
            .await
            .expect("tasks_start")
            .0;

        let adapter = McpQueryAdapter::new(state.data_store.clone(), state.run_store.clone());
        let key = ProjectionKey {
            task_id: posted.task_id.to_string(),
            run_id: None,
            step: Some(mlua_swarm::worker::baseline::AG_IDENTITY.to_string()),
            path: Some("echoed".to_string()),
        };
        let (_run, value) = adapter.resolve_async(&key).await.expect("resolve_async");
        assert_eq!(value, json!("hi"));
    }

    /// Subtask 4 Test #1 (the in-flight scenario this rework exists for):
    /// a 2-step `Seq` flow where `step2` blocks on a gate until the test
    /// releases it. By the time `step2` has started, `step1`'s
    /// `dispatch_attempt_with` — and therefore its `submit_output` (and
    /// this rework's dual-write into the Data-plane store), plus its
    /// `RunRecord.step_entries` append — has unconditionally already
    /// completed (flow.ir's `Seq` awaits each child before starting the
    /// next), while the overall Run is still `Running` (not yet
    /// finalized). `GET /v1/tasks/:id/runs/:run/steps/step1` must return
    /// `step1`'s OUTPUT during that window.
    #[tokio::test(flavor = "multi_thread")]
    async fn steps_list_returns_in_flight_step_output_before_run_completes() {
        use mlua_flow_ir::{Expr, Node as FlowNode};
        use mlua_swarm::worker::adapter::WorkerResult;
        use mlua_swarm::{RustFnInProcessSpawnerFactory, SpawnerRegistry};

        let started = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(tokio::sync::Notify::new());
        let started_bg = started.clone();
        let gate_bg = gate.clone();

        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("step1", |inv| async move {
                Ok(WorkerResult {
                    value: json!({ "step1_out": inv.prompt }),
                    ok: true,
                })
            })
            .register_fn("step2", move |_inv| {
                let started = started_bg.clone();
                let gate = gate_bg.clone();
                async move {
                    started.notify_one();
                    gate.notified().await;
                    Ok(WorkerResult {
                        value: json!("step2 done"),
                        ok: true,
                    })
                }
            });
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));

        let engine = Engine::new_with_layers(EngineCfg::default(), crate::default_layer_registry());
        let data_store: Arc<dyn mlua_swarm::store::output::OutputStore> =
            Arc::new(InMemoryOutputStore::new());
        engine.set_output_store(data_store.clone());
        let compiler = mlua_swarm::Compiler::new(reg);
        let launch = Arc::new(mlua_swarm::TaskLaunchService::new(engine.clone(), compiler));
        let state = AppState {
            engine,
            sessions: Arc::new(Mutex::new(crate::SessionStore::default())),
            task_app: Arc::new(mlua_swarm::TaskApplication::new_inline_only(launch)),
            ws_operator_factory: None,
            data_store,
            operator_sessions: Arc::new(Mutex::new(HashMap::new())),
            roles_to_sid: Arc::new(Mutex::new(HashMap::new())),
            task_store: Arc::new(InMemoryTaskStore::new()),
            run_store: Arc::new(InMemoryRunStore::new()),
            base_url: None,
        };

        let flow = FlowNode::Seq {
            children: vec![
                FlowNode::Step {
                    ref_: "step1".to_string(),
                    in_: Expr::Path {
                        at: "$.greeting".to_string(),
                    },
                    out: Expr::Path {
                        at: "$.step1".to_string(),
                    },
                },
                FlowNode::Step {
                    ref_: "step2".to_string(),
                    in_: Expr::Path {
                        at: "$.step1".to_string(),
                    },
                    out: Expr::Path {
                        at: "$.step2".to_string(),
                    },
                },
            ],
        };
        let blueprint = Blueprint {
            schema_version: current_schema_version(),
            id: "projection-test-in-flight-bp".into(),
            flow,
            agents: vec![
                AgentDef {
                    name: "step1".into(),
                    kind: AgentKind::RustFn,
                    spec: json!({"fn_id": "step1"}),
                    profile: None,
                    meta: None,
                },
                AgentDef {
                    name: "step2".into(),
                    kind: AgentKind::RustFn,
                    spec: json!({"fn_id": "step2"}),
                    profile: None,
                    meta: None,
                },
            ],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
        };

        let req = TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(blueprint),
            },
            init_ctx: json!({ "greeting": "hi" }),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            goal: None,
        };

        let state_bg = state.clone();
        let launch_handle =
            tokio::spawn(async move { crate::tasks_start(State(state_bg), Json(req)).await });

        // step2 signals `started` only after step1's dispatch (and its
        // submit_output / Data-plane dual-write, and its step_entries
        // append) has fully returned — see the doc above.
        started.notified().await;

        let in_flight_tasks = state.task_store.list().await.expect("task_store list");
        assert_eq!(in_flight_tasks.len(), 1, "exactly one Task minted");
        let task_id = in_flight_tasks[0].id.clone();

        let resp = steps_list(
            State(state.clone()),
            Path((task_id.to_string(), "latest".to_string())),
        )
        .await
        .expect("steps_list while step2 is still in flight");
        let step1_entry = resp
            .steps
            .iter()
            .find(|s| s.name == "step1")
            .expect("step1 must already be visible");
        assert_eq!(step1_entry.source, ProjectionSource::DataPlane);

        // Release step2 so the background `tasks_start` can complete and
        // the test can join it cleanly.
        gate.notify_one();
        let posted = launch_handle.await.expect("join").expect("tasks_start").0;
        assert_eq!(posted.final_ctx["step2"], json!("step2 done"));
    }
}
