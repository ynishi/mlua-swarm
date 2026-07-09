//! `ProjectionAdapter` — pull-based supply of step OUTPUT data to
//! downstream Agent steps, materialized from run [`Ctx`](crate::core::ctx::Ctx)
//! state.
//!
//! # Architecture (ST5: role separation, MCP tool retired)
//!
//! Worker MCP dispatch stores each step's OUTPUT in the run ctx
//! (`Ctx.data` while a run is live, `RunRecord.result_ref` once a Run has
//! finalized on the server). `projection-adapter` ST5 settles this
//! module's role split around two axes, replacing the ST2-ST4 single
//! `mse_ctx_get` MCP tool:
//!
//! - **Worker axis (primary supply)** — a worker's `GET
//!   /v1/worker/prompt` fetch payload carries
//!   [`crate::core::agent_context::AgentContextView::steps`]:
//!   `Vec<`[`crate::core::agent_context::StepPointer`]`>`, a
//!   `ContextPolicy.steps`-filtered pointer list assembled automatically
//!   at fetch time by `crates/mlua-swarm-server/src/worker.rs`. No
//!   separate MCP tool call needed — a fetch already has every visible
//!   prior step's pointer, pre-filtered by the Blueprint-declared
//!   [`mlua_swarm_schema::ContextPolicy::steps`] / `steps_exclude`.
//! - **HTTP debug plane (metadata + content)** — `crates/mlua-swarm-server/src/projection.rs`'s
//!   `GET /v1/tasks/:id/runs/:run/steps*` REST hierarchy is the content
//!   the pointers' `content_url` addresses, plus an unfiltered
//!   metadata/content view for operators / humans debugging a run. The
//!   old `mse_ctx_get` MCP tool (a manual pull wrapper over the ST2/ST4
//!   `GET /v1/tasks/:id/ctx` single-value endpoint) is retired: its
//!   entire reason for being — a way to pull a *prior* step's OUTPUT on
//!   demand — is now automatic on the Worker axis.
//!
//! [`ProjectionAdapter::project`] turns a [`ProjectionKey`] (identifying a
//! slice of run ctx by task / run / step / field path) plus the
//! policy-filtered ctx data into a [`ProjectionRef`] locator; a caller
//! later calls [`ProjectionAdapter::fetch`] to retrieve the actual value.
//! The directive header only ever carries
//! [`ProjectionAdapter::pointer_line`]'s 1-3 line pointer — never the
//! projected value itself (no inline full-embed) — the SAME pointer-only
//! discipline `AgentContextView.steps` follows on the Worker axis.
//!
//! The run ctx / `RunRecord.result_ref` stays the single source of truth;
//! an adapter only decides *how the pull is served* — as a materialized
//! file ([`FileProjectionAdapter`], this module, still used for the
//! spawning agent's own `AgentContextView` — see
//! `crates/mlua-swarm-server/src/operator_ws/session.rs`'s
//! `append_projection_pointer`) or as an MCP query endpoint
//! (`McpQueryAdapter`, server-side, see `ProjectionRef::Query`). Both
//! adapters share the one [`ProjectionKey`] addressing type so callers
//! never need to branch on which adapter backs a given pointer.
//!
//! # Submit-path projection (subtask-4 / ST2 rework, carried into ST5)
//!
//! The subtask-4 rework adds a *third*, submit-triggered supply path,
//! sitting beside the two above rather than replacing either: the moment a
//! worker's `Final` output lands in [`crate::core::engine::Engine::submit_output`]
//! / `submit_worker_result_trusted` (the canonical worker-submit path, `POST
//! /v1/worker/submit` and `/v1/worker/result`), the engine materializes that
//! step's OUTPUT to the [`crate::core::projection_placement::ProjectionPlacement`]
//! resolver's target (`<root>/<dir_template>/<canonical_agent>.md`; the
//! byte-compat default layout is `workspace/tasks/<task_id>/ctx/`) via
//! [`FileProjectionAdapter::materialize_submission`] — this is the file a
//! Worker-axis `StepPointer.file_path` (when `Some`) and the HTTP debug
//! plane's `StepSummary.file_path` both address, so a *later* Agent step
//! (or an operator, over HTTP) can read a *prior* step's OUTPUT while the
//! overall run is still in flight, without waiting for
//! `RunRecord.result_ref` to be set at finalization. `<root>` is resolved
//! from the [`crate::core::agent_context::AgentContextView`]
//! `AgentContextMiddleware` snapshotted at spawn time, via
//! [`ProjectionPlacement::resolve_root`] (`work_dir` falling back to
//! `project_root` for the byte-compat default preference); this sink is
//! best-effort (fail-open — see the Invariants on `Engine::submit_output`'s
//! doc): an unresolved root, or a `Final` from a spawn that never ran
//! through that middleware, is a silent no-op, never a submit failure.
//!
//! `<canonical_agent>` (GH #23) is `producer_agent` (`TaskState.spec.agent`)
//! resolved through `Engine::step_naming_for(task_id)`'s
//! `crate::core::step_naming::StepNaming::canonical_of_producer` when a
//! table was snapshotted for this task's dispatch — `producer_agent`
//! unchanged (byte-identical file stem to pre-GH-#23 behavior) for any
//! step whose `AgentMeta.projection_name` is undeclared, or when no table
//! exists for this `task_id` at all.
//!
//! # Design: addressing
//!
//! [`ProjectionKey`] combines three independent axes — ID (`task_id` /
//! `run_id`), Name (`step`), and Path (`path`, a `$.a.b`-style dot path
//! into the step's OUTPUT value) — so a caller can address anything from
//! "the whole run ctx" down to "one field of one step's OUTPUT" with the
//! same type. [`ProjectionKey::resolve`] is the pure, adapter-independent
//! implementation of that narrowing, shared by every adapter.

use crate::core::projection_placement::ProjectionPlacement;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use thiserror::Error;

/// Addressing for one projection target. ID axis (`task_id` / `run_id`) +
/// Name axis (`step`) + Path axis (`path`) — the one addressing type both
/// [`FileProjectionAdapter`] and the future `McpQueryAdapter` (ST2) accept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProjectionKey {
    /// Task identity (`StepId`'s `Display` string form, the same shape
    /// `AgentContextView.task_id` uses).
    pub task_id: String,
    /// Run identity. `None` means "the latest run".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Step / agent name — the key directly under `ctx.data` this
    /// projection narrows to. `None` means "the whole ctx".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    /// Field path within the step's value, `$.a.b` dot-path form (the
    /// leading `$.` is optional). `None` means "the whole step value".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

impl ProjectionKey {
    /// Pure narrowing helper: resolves `self` against `ctx_data`, first by
    /// `step` (a direct key lookup) then by `path` (a `.`-separated walk of
    /// nested object fields). Returns `None` if any segment is absent —
    /// this is a pure lookup, not a fallible parse (an unparseable `path`
    /// simply yields no match rather than an error, matching the
    /// `Option`-returning contract callers expect from a resolve step).
    pub fn resolve<'a>(&self, ctx_data: &'a Value) -> Option<&'a Value> {
        let mut current = match &self.step {
            Some(step) => ctx_data.get(step)?,
            None => ctx_data,
        };
        if let Some(path) = &self.path {
            let path = path.strip_prefix("$.").unwrap_or(path.as_str());
            for segment in path.split('.').filter(|s| !s.is_empty()) {
                current = current.get(segment)?;
            }
        }
        Some(current)
    }

    /// Filename-safe step slug for [`FileProjectionAdapter`]'s materialize
    /// target — `step` verbatim, or `"_ctx"` when addressing the whole
    /// ctx (`step` is `None`).
    fn step_slug(&self) -> &str {
        self.step.as_deref().unwrap_or("_ctx")
    }
}

/// [`ProjectionAdapter::project`]'s return value — the locator a worker
/// uses to pull the projected value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub enum ProjectionRef {
    /// [`FileProjectionAdapter`]: absolute path to the materialized file.
    File {
        /// Absolute filesystem path of the materialized projection file.
        path: String,
    },
    /// `McpQueryAdapter` (server-side): a fetch endpoint plus the key to
    /// query it with.
    Query {
        /// Fetch endpoint (e.g. `GET
        /// /v1/tasks/:id/runs/:run/steps/:step/content`, ST5) the adapter
        /// serves this projection through.
        endpoint: String,
        /// Key identifying which projection to fetch at `endpoint`.
        key: ProjectionKey,
    },
}

/// All ways a [`ProjectionAdapter`] operation can fail.
#[derive(Debug, Error)]
pub enum ProjectionError {
    /// No value exists for the given key — either `project()` could not
    /// resolve `key` against the supplied ctx data, or `fetch()` found no
    /// materialized projection for it.
    #[error("projection not found for key {0:?}")]
    NotFound(ProjectionKey),
    /// A filesystem operation (materialize write / read-back) failed.
    #[error("projection io error: {0}")]
    Io(#[from] std::io::Error),
    /// `key` is structurally invalid for this adapter (e.g. an empty
    /// `task_id`).
    #[error("invalid projection key: {0}")]
    InvalidKey(String),
    /// Serializing the projected value to its on-disk/on-wire form, or
    /// deserializing it back, failed.
    #[error("projection serialize error: {0}")]
    Serialize(String),
}

/// Context projection supply abstraction. The single source of truth is
/// the run ctx passed into [`Self::project`]; an adapter only decides
/// *how the pull is served* (module doc has the full narrative).
pub trait ProjectionAdapter: Send + Sync {
    /// Stable adapter name (`"file"` / `"mcp-query"`), used in log lines
    /// and diagnostics.
    fn name(&self) -> &'static str;

    /// Projects the slice of `ctx_data` addressed by `key` and returns a
    /// locator a worker can later [`Self::fetch`]. `ctx_data` is the
    /// policy-filtered ctx data slice this projection is drawn from — the
    /// adapter never mutates it.
    fn project(
        &self,
        key: &ProjectionKey,
        ctx_data: &Value,
    ) -> Result<ProjectionRef, ProjectionError>;

    /// Pulls the value addressed by `key` directly, without going through
    /// a previously returned [`ProjectionRef`] locator.
    fn fetch(&self, key: &ProjectionKey) -> Result<Value, ProjectionError>;

    /// Renders the 1-3 line pointer this adapter's [`ProjectionRef`]
    /// contributes to the directive header. Must never embed the
    /// projected value itself (inline full-embed is the exact problem
    /// projection exists to avoid — see the module doc).
    fn pointer_line(&self, r: &ProjectionRef) -> String;
}

/// File-backed [`ProjectionAdapter`]: materializes the projected value
/// under `root`, at the target [`ProjectionPlacement::target_path`]
/// resolves for `key`, and reads it back on [`Self::fetch`]. [`Self::new`]
/// resolves through [`ProjectionPlacement::default`] (the pre-GH-#27
/// hardcoded `<root>/workspace/tasks/<task_id>/ctx/<step-or-_ctx>.md`
/// layout, unchanged); [`Self::with_placement`] resolves through a
/// caller-supplied resolver instead — see
/// `crate::core::projection_placement`'s module doc for the "3 path"
/// convergence this collapses.
pub struct FileProjectionAdapter {
    root: PathBuf,
    placement: ProjectionPlacement,
}

impl FileProjectionAdapter {
    /// Builds an adapter rooted at `root` (typically the resolved
    /// `work_dir` / `project_root`) using the byte-compat
    /// [`ProjectionPlacement::default`] resolver.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            placement: ProjectionPlacement::default(),
        }
    }

    /// Builds an adapter rooted at `root`, resolving materialize targets
    /// through the given `placement` instead of the byte-compat default —
    /// the constructor every one of the "3 path" call sites
    /// (`crate::core::projection_placement`'s module doc) uses once they
    /// hold a Blueprint-resolved [`ProjectionPlacement`].
    pub fn with_placement(root: impl Into<PathBuf>, placement: ProjectionPlacement) -> Self {
        Self {
            root: root.into(),
            placement,
        }
    }

    /// The materialize target for `key`, resolved via
    /// [`ProjectionPlacement::target_path`].
    fn target_path(&self, key: &ProjectionKey) -> PathBuf {
        self.placement
            .target_path(&self.root, &key.task_id, key.step_slug())
    }

    /// Submit-path materialize (subtask-4 / ST2 rework — see the module
    /// doc's "Submit-path projection" section). Unlike [`Self::project`],
    /// `value` is not narrowed out of a larger `ctx_data` via
    /// [`ProjectionKey::resolve`] — it is the exact submitted content, so
    /// this writes it directly. Reuses [`Self::target_path`] (the same
    /// [`ProjectionPlacement`]-resolved target — byte-compat default
    /// `<root>/workspace/tasks/<task_id>/ctx/<step>.md` — as
    /// [`Self::project`]), so a later [`Self::fetch`] against the same
    /// `key` reads it back unchanged (`fetch` only parses the fenced
    /// ```` ```json ```` block, so the extra `attempt` / `ok` front-matter
    /// fields this writes are inert to it). A full replace, never append —
    /// re-submitting the same `(task_id, producer_agent)` overwrites
    /// (idempotent, latest wins — Subtask 4 Invariant 2 / Test 4).
    ///
    /// `key.step` must be `Some(producer_agent)` — this is a submission
    /// slot, never "the whole ctx" (`step: None` still resolves to a valid
    /// path via [`ProjectionKey::step_slug`]'s `"_ctx"` fallback, but a
    /// caller addressing an actual submission should always name the
    /// producing agent).
    pub fn materialize_submission(
        &self,
        key: &ProjectionKey,
        value: &Value,
        attempt: u32,
        ok: bool,
    ) -> Result<ProjectionRef, ProjectionError> {
        if key.task_id.is_empty() {
            return Err(ProjectionError::InvalidKey(
                "task_id must not be empty".to_string(),
            ));
        }
        let target = self.target_path(key);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&target, render_submission_file(key, value, attempt, ok)?)?;
        Ok(ProjectionRef::File {
            path: target.to_string_lossy().into_owned(),
        })
    }
}

/// Front matter for a submit-time materialized projection file
/// ([`FileProjectionAdapter::materialize_submission`]) — the same
/// addressing fields as [`ProjectionKey`], flattened, plus the
/// submission's own `attempt` / `ok`. No `out_id`: `Engine::submit_output`
/// / `submit_worker_result_trusted` do not allocate one (that only happens
/// on the separate `POST /v1/data/emit` Data-plane path — see
/// `crate::store::output`'s module doc) — so there is nothing to carry
/// here.
#[derive(Serialize)]
struct SubmissionFrontMatter<'a> {
    #[serde(flatten)]
    key: &'a ProjectionKey,
    /// The submitting attempt number.
    attempt: u32,
    /// The submission's transport-level success flag (`OutputEvent::Final.ok`).
    ok: bool,
}

/// Renders a submit-time materialized projection file — same fenced-JSON
/// body convention as [`render_projection_file`], with a front matter that
/// additionally carries `attempt` / `ok` (see [`SubmissionFrontMatter`]).
fn render_submission_file(
    key: &ProjectionKey,
    value: &Value,
    attempt: u32,
    ok: bool,
) -> Result<String, ProjectionError> {
    let front = SubmissionFrontMatter { key, attempt, ok };
    let front_matter =
        serde_yaml::to_string(&front).map_err(|err| ProjectionError::Serialize(err.to_string()))?;
    let body = serde_json::to_string_pretty(value)
        .map_err(|err| ProjectionError::Serialize(err.to_string()))?;
    Ok(format!("---\n{front_matter}---\n\n```json\n{body}\n```\n"))
}

impl ProjectionAdapter for FileProjectionAdapter {
    fn name(&self) -> &'static str {
        "file"
    }

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
        let value = key
            .resolve(ctx_data)
            .ok_or_else(|| ProjectionError::NotFound(key.clone()))?;
        let target = self.target_path(key);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        // Full replace, never append — re-projecting the same key is
        // idempotent (invariant 3).
        fs::write(&target, render_projection_file(key, value)?)?;
        Ok(ProjectionRef::File {
            path: target.to_string_lossy().into_owned(),
        })
    }

    fn fetch(&self, key: &ProjectionKey) -> Result<Value, ProjectionError> {
        let target = self.target_path(key);
        let body = fs::read_to_string(&target).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                ProjectionError::NotFound(key.clone())
            } else {
                ProjectionError::Io(err)
            }
        })?;
        parse_projection_file(&body)
    }

    fn pointer_line(&self, r: &ProjectionRef) -> String {
        match r {
            ProjectionRef::File { path } => format!("projection(file): {path}"),
            ProjectionRef::Query { endpoint, key } => {
                format!("projection(mcp-query): {endpoint} task_id={}", key.task_id)
            }
        }
    }
}

/// Renders a materialized projection file: a `key`-describing YAML
/// front-matter header followed by the projected `value` as a fenced JSON
/// block ([`parse_projection_file`] reads the fence back out).
fn render_projection_file(key: &ProjectionKey, value: &Value) -> Result<String, ProjectionError> {
    let front_matter =
        serde_yaml::to_string(key).map_err(|err| ProjectionError::Serialize(err.to_string()))?;
    let body = serde_json::to_string_pretty(value)
        .map_err(|err| ProjectionError::Serialize(err.to_string()))?;
    Ok(format!("---\n{front_matter}---\n\n```json\n{body}\n```\n"))
}

/// Reads a materialized projection file back into its JSON value, the
/// inverse of [`render_projection_file`]. Only the fenced ```` ```json ````
/// block is parsed; the front-matter header is documentation for a human
/// reader and is not required for the round trip.
fn parse_projection_file(text: &str) -> Result<Value, ProjectionError> {
    const FENCE_OPEN: &str = "```json";
    const FENCE_CLOSE: &str = "```";
    let after_open = text.find(FENCE_OPEN).ok_or_else(|| {
        ProjectionError::Serialize("materialized projection file missing ```json block".into())
    })?;
    let body_start = after_open + FENCE_OPEN.len();
    let close_offset = text[body_start..].find(FENCE_CLOSE).ok_or_else(|| {
        ProjectionError::Serialize("materialized projection file missing closing ``` fence".into())
    })?;
    let json_body = text[body_start..body_start + close_offset].trim();
    serde_json::from_str(json_body).map_err(|err| ProjectionError::Serialize(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::projection_placement::RootPreference;
    use serde_json::json;
    use std::path::Path;
    use tempfile::TempDir;

    fn sample_ctx_data() -> Value {
        json!({
            "planner": {
                "plan": "do the thing",
                "nested": { "field": 42 }
            },
            "other_step": { "value": "x" }
        })
    }

    fn key(step: Option<&str>, path: Option<&str>) -> ProjectionKey {
        ProjectionKey {
            task_id: "T-1".to_string(),
            run_id: None,
            step: step.map(str::to_string),
            path: path.map(str::to_string),
        }
    }

    #[test]
    fn resolve_step_only_returns_step_value() {
        let ctx_data = sample_ctx_data();
        let resolved = key(Some("planner"), None).resolve(&ctx_data).unwrap();
        assert_eq!(
            resolved,
            &json!({"plan": "do the thing", "nested": {"field": 42}})
        );
    }

    #[test]
    fn resolve_step_and_path_narrows_to_field() {
        let ctx_data = sample_ctx_data();
        let resolved = key(Some("planner"), Some("$.nested.field"))
            .resolve(&ctx_data)
            .unwrap();
        assert_eq!(resolved, &json!(42));
    }

    #[test]
    fn resolve_missing_step_returns_none() {
        assert!(key(Some("does-not-exist"), None)
            .resolve(&sample_ctx_data())
            .is_none());
    }

    #[test]
    fn resolve_missing_path_returns_none() {
        assert!(key(Some("planner"), Some("$.nested.missing"))
            .resolve(&sample_ctx_data())
            .is_none());
    }

    #[test]
    fn file_adapter_project_then_fetch_round_trips() {
        let dir = TempDir::new().unwrap();
        let adapter = FileProjectionAdapter::new(dir.path());
        let k = key(Some("planner"), None);
        let ctx_data = sample_ctx_data();

        let reference = adapter.project(&k, &ctx_data).unwrap();
        let path = match &reference {
            ProjectionRef::File { path } => path.clone(),
            other => panic!("expected File ref, got {other:?}"),
        };
        assert!(Path::new(&path).exists());

        let fetched = adapter.fetch(&k).unwrap();
        assert_eq!(&fetched, k.resolve(&ctx_data).unwrap());
    }

    /// GH #27 (follow-up to #23): `with_placement` resolves the target
    /// through the given `ProjectionPlacement` instead of the byte-compat
    /// default — the file lands at the custom `dir_template`, and a
    /// `fetch` against the SAME key/adapter still round-trips (write and
    /// read agree by construction, since both go through the same
    /// resolver instance).
    #[test]
    fn file_adapter_with_placement_uses_custom_dir_template() {
        let dir = TempDir::new().unwrap();
        let placement = ProjectionPlacement {
            root_preference: RootPreference::WorkDir,
            dir_template: "custom/{task_id}/out".to_string(),
        };
        let adapter = FileProjectionAdapter::with_placement(dir.path(), placement);
        let k = key(Some("planner"), None);
        let ctx_data = sample_ctx_data();

        let reference = adapter.project(&k, &ctx_data).unwrap();
        let path = match &reference {
            ProjectionRef::File { path } => path.clone(),
            other => panic!("expected File ref, got {other:?}"),
        };
        assert!(
            path.ends_with("custom/T-1/out/planner.md"),
            "path must follow the custom dir_template: {path}"
        );
        assert!(Path::new(&path).exists());

        let fetched = adapter.fetch(&k).unwrap();
        assert_eq!(&fetched, k.resolve(&ctx_data).unwrap());
    }

    #[test]
    fn file_adapter_reproject_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let adapter = FileProjectionAdapter::new(dir.path());
        let k = key(Some("planner"), None);
        let ctx_data = sample_ctx_data();

        let first = adapter.project(&k, &ctx_data).unwrap();
        let second = adapter.project(&k, &ctx_data).unwrap();
        assert_eq!(first, second);
        assert_eq!(adapter.fetch(&k).unwrap(), adapter.fetch(&k).unwrap());
    }

    #[test]
    fn pointer_line_carries_path_not_value() {
        let reference = ProjectionRef::File {
            path: "/tmp/some/materialized.md".to_string(),
        };
        let adapter = FileProjectionAdapter::new("/unused");
        let line = adapter.pointer_line(&reference);
        assert!(line.contains("/tmp/some/materialized.md"));
        assert!(!line.contains('{'));
    }

    #[test]
    fn fetch_missing_projection_returns_not_found() {
        let dir = TempDir::new().unwrap();
        let adapter = FileProjectionAdapter::new(dir.path());
        let k = key(Some("nope"), None);

        let err = adapter.fetch(&k).unwrap_err();
        assert!(matches!(err, ProjectionError::NotFound(_)));
    }

    #[test]
    fn project_rejects_empty_task_id() {
        let dir = TempDir::new().unwrap();
        let adapter = FileProjectionAdapter::new(dir.path());
        let mut k = key(Some("planner"), None);
        k.task_id = String::new();

        let err = adapter.project(&k, &sample_ctx_data()).unwrap_err();
        assert!(matches!(err, ProjectionError::InvalidKey(_)));
    }

    // ─── subtask-4 / ST2 rework: FileProjectionAdapter::materialize_submission ───

    #[test]
    fn materialize_submission_writes_value_directly_and_fetch_reads_it_back() {
        let dir = TempDir::new().unwrap();
        let adapter = FileProjectionAdapter::new(dir.path());
        let k = key(Some("planner"), None);
        let value = json!({"plan": "do the thing"});

        let reference = adapter.materialize_submission(&k, &value, 1, true).unwrap();
        let path = match &reference {
            ProjectionRef::File { path } => path.clone(),
            other => panic!("expected File ref, got {other:?}"),
        };
        assert!(Path::new(&path).exists());

        // fetch() only parses the fenced ```json block, so the submission
        // front matter (attempt / ok) is inert to it — same round trip
        // contract as `project`.
        let fetched = adapter.fetch(&k).unwrap();
        assert_eq!(fetched, value);
    }

    #[test]
    fn materialize_submission_front_matter_carries_attempt_and_ok() {
        let dir = TempDir::new().unwrap();
        let adapter = FileProjectionAdapter::new(dir.path());
        let k = key(Some("reviewer"), None);

        let reference = adapter
            .materialize_submission(&k, &json!("hi"), 2, false)
            .unwrap();
        let path = match &reference {
            ProjectionRef::File { path } => path.clone(),
            other => panic!("expected File ref, got {other:?}"),
        };
        let body = std::fs::read_to_string(path).unwrap();
        assert!(body.contains("attempt: 2"), "front matter: {body}");
        assert!(body.contains("ok: false"), "front matter: {body}");
        assert!(body.contains("step: reviewer"), "front matter: {body}");
    }

    #[test]
    fn materialize_submission_resubmit_overwrites_with_latest() {
        let dir = TempDir::new().unwrap();
        let adapter = FileProjectionAdapter::new(dir.path());
        let k = key(Some("planner"), None);

        adapter
            .materialize_submission(&k, &json!("first"), 1, true)
            .unwrap();
        adapter
            .materialize_submission(&k, &json!("second"), 1, true)
            .unwrap();

        assert_eq!(adapter.fetch(&k).unwrap(), json!("second"));
    }

    #[test]
    fn materialize_submission_rejects_empty_task_id() {
        let dir = TempDir::new().unwrap();
        let adapter = FileProjectionAdapter::new(dir.path());
        let mut k = key(Some("planner"), None);
        k.task_id = String::new();

        let err = adapter
            .materialize_submission(&k, &json!("x"), 1, true)
            .unwrap_err();
        assert!(matches!(err, ProjectionError::InvalidKey(_)));
    }
}
