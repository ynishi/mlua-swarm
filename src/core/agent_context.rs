//! `AgentContextView` — Contract C: the task-level context that must reach
//! the LLM/Agent boundary, materialized once and consumed by every axis.
//!
//! # The gap this closes (GH #20)
//!
//! Task-level context lives in `Ctx.meta.runtime` (`project_root` /
//! `work_dir` / `task_metadata` / `run_id` / `project_name_alias`, …) and
//! has to reach the executing Agent through two independent renderers:
//!
//! - **Spawner axis** — the WS thin-path
//!   (`crates/mlua-swarm-server/src/operator_ws/session.rs`) splices
//!   individual `Ctx.meta.runtime` keys into the `Spawn.directive` text a
//!   MainAI reads.
//! - **Worker axis** — `Engine::fetch_worker_payload{,_trusted}`
//!   (`crate::core::engine`) builds a [`crate::types::WorkerPayload`] a
//!   SubAgent pulls over `GET /v1/worker/prompt`; it never carried
//!   task-level meta at all (only `task_id` / `attempt` / `agent` /
//!   `system` / `prompt`).
//!
//! Before this module, each axis field-by-field-pulled the keys it needed
//! straight out of `ctx.meta.runtime`, so a new field (e.g.
//! `task_metadata`) had to be wired into every consumer by hand — and
//! `task_metadata` never made it into the directive text at all (the F2
//! gap tracked in the `operator-execution-model` guide).
//!
//! # The fix: materialize once, consume everywhere
//!
//! [`AgentContextView`] formalizes Contract C as one view struct. A new
//! innermost `SpawnerLayer` (`crate::middleware::agent_context`) builds it
//! from `Ctx` exactly once per spawn — after the outer
//! `TaskInputMiddleware` / `ProjectNameAliasMiddleware` /
//! `WorkerBindingMiddleware` layers have inserted their runtime keys, and
//! before the base spawner stack (WS session / in-process AgentBlock) runs
//! — and fans the materialized view out on two independent rails:
//!
//! ```text
//!  TaskInput/Alias/Binding middlewares (outer, existing)  — insert runtime keys
//!         │ ctx (keys present)
//!         ▼
//!  AgentContextMiddleware (innermost layer)
//!    view = AgentContextView::from_ctx(ctx).apply_policy(&policy)
//!    (a) EngineState.agent_ctx[(task_id, attempt)].view = view     ← Worker axis source
//!    (b) ctx.meta.runtime[AGENT_CONTEXT_KEY] = json(view)          ← Spawner axis source
//!         │ inner.spawn(new_ctx)
//!         ▼
//!  base stack (OperatorDelegate → WS session.rs | in-proc AgentBlock runtime.rs)
//! ```
//!
//! (a) is read back by `Engine::fetch_worker_payload{,_trusted}` (keyed by
//! `(task_id, attempt)`, mirroring `EngineState.prompts` / `.systems` —
//! `Ctx` itself is not stored, so the view has to be snapshotted at
//! dispatch time to still be servable when the Worker axis fetches it
//! later). (b) is read back via [`AgentContextView::materialized_or_from_ctx`]
//! by both the Spawner axis (WS `session.rs`) and the in-process
//! AgentBlock axis (`crate::worker::agent_block::runtime`) — falling back
//! to [`AgentContextView::from_ctx`] when the middleware was never layered
//! (backward compat).
//!
//! A field added to [`AgentContextView`] (either a named field, or an
//! `extra` entry) reaches both axes automatically — no per-consumer wiring
//! required. [`ContextPolicy`] filters the materialized view before it is
//! snapshotted / stashed; GH #21 Phase 1 wires it to Blueprint schema
//! fields (`Blueprint.default_agent_ctx` / `default_context_policy`,
//! `AgentMeta.ctx` / `context_policy`, resolved by
//! `crate::service::task_launch::derive_agent_ctx` /
//! `derive_context_policies` and consumed by
//! `crate::middleware::agent_context::AgentContextMiddleware`).

use crate::core::ctx::Ctx;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `ctx.meta.runtime` key under which the materialized
/// [`AgentContextView`] (JSON-serialized) is stashed by
/// `crate::middleware::agent_context::AgentContextMiddleware` — the
/// Spawner axis's read-back source (see the module doc).
pub const AGENT_CONTEXT_KEY: &str = "agent_context";

/// `ctx.meta.runtime` key that carries the issue #13 run-id propagation
/// value. Canonical home as of GH #20 (previously a bare literal at
/// `Engine::dispatch_attempt_with`).
pub const RUN_ID_KEY: &str = "run_id";

/// `ctx.meta.runtime` key that carries the GH #21 Phase 2 Step tier's
/// resolved context bundle (`TaskSpec.step_ctx`, threaded through by
/// `Engine::dispatch_attempt_with` on every attempt — same insertion
/// site as [`RUN_ID_KEY`]). Consumed by
/// `crate::middleware::agent_context::AgentContextMiddleware`, which
/// unpacks the bundle's keys and applies them with the same
/// only-if-absent mechanics as the Agent / BP-global tiers, ordered
/// FIRST (Step outranks Agent and BP-global — see that middleware's
/// module doc for the full precedence narrative). The bundle itself
/// (this key's raw value) stays in the runtime bag verbatim — only its
/// individual keys are folded into [`AgentContextView::extra`].
pub const STEP_CTX_KEY: &str = "step_ctx";

/// `ctx.meta.runtime` key that carries `Blueprint.metadata.project_name_alias`.
/// Canonical home as of GH #20; re-exported from
/// [`crate::middleware::project_name_alias`] for API compatibility (same
/// string value the existing `ctx.meta.runtime.get("project_name_alias")`
/// consumers keyed off).
pub const PROJECT_NAME_ALIAS_KEY: &str = "project_name_alias";

/// `ctx.meta.runtime` key that carries the Task-level project root path.
/// Canonical home as of GH #20; re-exported from
/// [`crate::middleware::task_input`] for API compatibility.
pub const TASK_PROJECT_ROOT_KEY: &str = "project_root";

/// `ctx.meta.runtime` key that carries the Task-level work dir path.
/// Canonical home as of GH #20; re-exported from
/// [`crate::middleware::task_input`] for API compatibility.
pub const TASK_WORK_DIR_KEY: &str = "work_dir";

/// `ctx.meta.runtime` key that carries the Task-level free-form metadata
/// object. Canonical home as of GH #20; re-exported from
/// [`crate::middleware::task_input`] for API compatibility.
pub const TASK_METADATA_KEY: &str = "task_metadata";

/// A pointer to one preceding step's OUTPUT, embedded into
/// [`AgentContextView::steps`] by `crates/mlua-swarm-server/src/worker.rs`'s
/// `GET /v1/worker/prompt` handler (the `projection-adapter` ST5 Worker
/// axis) — never the OUTPUT content itself (pointer-only invariant: no
/// preview, no content bytes, matching the "worker payload never
/// inline-embeds a projected value" contract `crate::core::projection`'s
/// module doc establishes for the directive header).
///
/// A worker resolves the pointed-at content either by `Read`ing
/// [`Self::file_path`] (when `Some`, the step's OUTPUT was materialized to
/// disk — see `crate::core::projection::FileProjectionAdapter`) or by
/// fetching [`Self::content_url`] (always present; the HTTP debug-plane
/// route `GET /v1/tasks/:id/runs/:run/steps/:step/content` this URL
/// addresses serves the same content regardless of whether a materialized
/// file exists).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StepPointer {
    /// The producing step's name (the Blueprint `AgentDef.name` /
    /// flow.ir `Step.ref` that emitted this OUTPUT).
    pub name: String,
    /// Byte length of the OUTPUT body as served by [`Self::content_url`]
    /// (the exact bytes an HTTP `GET` of that URL returns).
    pub size_bytes: u64,
    /// Absolute filesystem path to the materialized projection file
    /// (`crate::core::projection::FileProjectionAdapter`'s
    /// `<root>/workspace/tasks/<step_id>/ctx/<name>.md` target), when the
    /// step's submission was materialized to disk. `None` when the OUTPUT
    /// is only resolvable via [`Self::content_url`] (in-memory Data-plane
    /// record, or a `RunRecord.result_ref` fallback — see
    /// `crates/mlua-swarm-server/src/projection.rs`'s module doc).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    /// Fetch URL for the step's OUTPUT content — absolute
    /// (`AppState.base_url`-prefixed) when the server has a configured
    /// base URL, relative otherwise. Always present, regardless of
    /// [`Self::file_path`].
    pub content_url: String,
    /// SHA-256 hex digest of the OUTPUT body — change detection, matching
    /// the HTTP debug-plane route's `ETag` header value (`sha256:<hex>`,
    /// minus the `sha256:` prefix).
    pub sha256: String,
}

/// Contract C's view struct — the task-level context that must reach the
/// LLM/Agent boundary, materialized once per spawn (see the module doc).
///
/// `task_id` / `agent` / `attempt` are Ctx-immediate identity fields,
/// always present. Every other named field is independently optional,
/// mirroring the "absent key = no value, not an empty placeholder"
/// contract the individual `ctx.meta.runtime` injectors already follow
/// (`TaskInputMiddleware`, `ProjectNameAliasMiddleware`, …). `extra` is
/// the injectable surface for future supply-axis fields (FlowIr ctx /
/// StepMeta) — empty in [`Self::from_ctx`] today, but a value dropped into
/// it reaches every consumer of this view (directive header text, the
/// Worker axis's [`crate::types::WorkerPayload::context`], and the
/// serialized JSON) with no further wiring.
///
/// `deny_unknown_fields` is deliberately NOT set: this view is meant to
/// grow additively (new named fields, new `extra` entries) without
/// breaking deserialization of a view a slightly older engine build
/// produced — forward tolerance for additive fields.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentContextView {
    /// The task this view was materialized for (`Ctx.task_id`, rendered
    /// via its `Display` — the same string form `StepId` serializes as
    /// elsewhere).
    pub task_id: String,
    /// The agent this dispatch is targeting (`Ctx.agent`).
    pub agent: String,
    /// 1-based attempt counter for `task_id` (`Ctx.attempt`).
    pub attempt: u32,
    /// Task-level project root path, from
    /// `ctx.meta.runtime[TASK_PROJECT_ROOT_KEY]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_root: Option<String>,
    /// Task-level working directory, from
    /// `ctx.meta.runtime[TASK_WORK_DIR_KEY]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_dir: Option<String>,
    /// Task-level free-form metadata bag, from
    /// `ctx.meta.runtime[TASK_METADATA_KEY]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<serde_json::Value>")]
    pub task_metadata: Option<Value>,
    /// Issue #13 run-id propagation value, from
    /// `ctx.meta.runtime[RUN_ID_KEY]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// `Blueprint.metadata.project_name_alias`, from
    /// `ctx.meta.runtime[PROJECT_NAME_ALIAS_KEY]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_name_alias: Option<String>,
    /// Injectable surface for future supply-axis fields not yet promoted
    /// to a named field (e.g. FlowIr ctx / StepMeta). Empty in
    /// [`Self::from_ctx`] today — a future `ContextPolicy`-aware
    /// materializer may populate it without a breaking change to this
    /// struct.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    #[schemars(with = "serde_json::Value")]
    pub extra: serde_json::Map<String, Value>,
    /// Pointers to preceding steps' OUTPUT, `ContextPolicy.steps`-filtered
    /// (`projection-adapter` ST5 Worker axis). Empty in [`Self::from_ctx`]
    /// / on every snapshot stashed into `EngineState.agent_ctx` —
    /// this field is populated only on the `WorkerPayload` clone
    /// `crates/mlua-swarm-server/src/worker.rs`'s `GET /v1/worker/prompt`
    /// handler returns (assembled at fetch time, so in-flight submissions
    /// after spawn are still visible — see that module's doc).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<StepPointer>,
}

impl AgentContextView {
    /// Builds a view directly off `ctx` — the Ctx-immediate identity
    /// fields plus whatever the canonical keys above currently hold in
    /// `ctx.meta.runtime`. `extra` is always empty here (see the field
    /// doc); a caller that wants to inject additional fields should build
    /// on top of the result.
    pub fn from_ctx(ctx: &Ctx) -> Self {
        let runtime = &ctx.meta.runtime;
        Self {
            task_id: ctx.task_id.to_string(),
            agent: ctx.agent.clone(),
            attempt: ctx.attempt,
            project_root: runtime
                .get(TASK_PROJECT_ROOT_KEY)
                .and_then(Value::as_str)
                .map(String::from),
            work_dir: runtime
                .get(TASK_WORK_DIR_KEY)
                .and_then(Value::as_str)
                .map(String::from),
            task_metadata: runtime.get(TASK_METADATA_KEY).cloned(),
            run_id: runtime
                .get(RUN_ID_KEY)
                .and_then(Value::as_str)
                .map(String::from),
            project_name_alias: runtime
                .get(PROJECT_NAME_ALIAS_KEY)
                .and_then(Value::as_str)
                .map(String::from),
            extra: serde_json::Map::new(),
            steps: Vec::new(),
        }
    }

    /// Reads the canonical, policy-applied view back out of
    /// `ctx.meta.runtime[AGENT_CONTEXT_KEY]` (materialized by
    /// `AgentContextMiddleware`); falls back to [`Self::from_ctx`] when
    /// the key is absent or fails to deserialize (middleware not yet
    /// layered onto this spawner stack — backward compat).
    pub fn materialized_or_from_ctx(ctx: &Ctx) -> Self {
        ctx.meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .and_then(|v| serde_json::from_value::<Self>(v.clone()).ok())
            .unwrap_or_else(|| Self::from_ctx(ctx))
    }

    /// Renders ONLY the task-level context lines for the Spawn directive
    /// header — `project_name_alias:` / `project_root:` / `work_dir:` in
    /// that literal order (matching the pre-existing splice order in
    /// `crates/mlua-swarm-server/src/operator_ws/session.rs`), then a new
    /// `task_metadata: {compact-json}` line when `Some`, then one
    /// `{key}: {compact-json}` line per `extra` entry. Absent fields
    /// render as nothing (no empty-string placeholder) — same contract as
    /// the individual `ctx.meta.runtime` injectors. Does NOT render
    /// `task_id` / `agent` / `attempt` / `run_id` — those already appear
    /// elsewhere in the directive template.
    pub fn to_directive_header(&self) -> String {
        let mut out = String::new();
        if let Some(alias) = &self.project_name_alias {
            out.push_str(&format!("project_name_alias: {alias}\n"));
        }
        if let Some(root) = &self.project_root {
            out.push_str(&format!("project_root: {root}\n"));
        }
        if let Some(dir) = &self.work_dir {
            out.push_str(&format!("work_dir: {dir}\n"));
        }
        if let Some(meta) = &self.task_metadata {
            out.push_str(&format!("task_metadata: {meta}\n"));
        }
        for (key, value) in &self.extra {
            out.push_str(&format!("{key}: {value}\n"));
        }
        out
    }

    /// Applies `policy` to `self`, returning a filtered copy. Filtered
    /// `Option` fields become `None`; filtered `extra` keys are removed.
    /// Identity fields (`task_id` / `agent` / `attempt`) are never
    /// filtered. A pass-all policy (the `ContextPolicy` default) returns
    /// `self` unchanged. Moved here from the former
    /// `ContextPolicy::apply(&self, view)` (GH #21 Phase 1 — the filter
    /// data shape now lives in `mlua_swarm_schema::ContextPolicy`; the
    /// application logic stays on the view it filters).
    pub fn apply_policy(mut self, policy: &ContextPolicy) -> Self {
        if !policy.allows("project_root") {
            self.project_root = None;
        }
        if !policy.allows("work_dir") {
            self.work_dir = None;
        }
        if !policy.allows("task_metadata") {
            self.task_metadata = None;
        }
        if !policy.allows("run_id") {
            self.run_id = None;
        }
        if !policy.allows("project_name_alias") {
            self.project_name_alias = None;
        }
        self.extra.retain(|key, _| policy.allows(key));

        self
    }
}

/// Filter over [`AgentContextView`] fields (GH #20/#21). Relocated to the
/// schema crate in GH #21 Phase 1 so a Blueprint author can declare one via
/// `Blueprint.default_context_policy` / `AgentMeta.context_policy` — this
/// re-export keeps `mlua_swarm::core::agent_context::ContextPolicy`
/// resolving unchanged for every existing caller (path-compat; see the
/// `context_policy_path_compat_reexport` test below). The filter
/// *application* logic lives on the view side, at
/// [`AgentContextView::apply_policy`].
pub use mlua_swarm_schema::ContextPolicy;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StepId;

    fn ctx_with_runtime(pairs: &[(&str, Value)]) -> Ctx {
        let mut ctx = Ctx::new(StepId::parse("ST-1").unwrap(), 3, "planner");
        for (k, v) in pairs {
            ctx.meta.runtime.insert((*k).to_string(), v.clone());
        }
        ctx
    }

    #[test]
    fn from_ctx_extracts_identity_and_all_runtime_keys() {
        let ctx = ctx_with_runtime(&[
            (TASK_PROJECT_ROOT_KEY, Value::String("/repo".into())),
            (TASK_WORK_DIR_KEY, Value::String("/repo/work".into())),
            (TASK_METADATA_KEY, serde_json::json!({"issue": 20})),
            (RUN_ID_KEY, Value::String("R-abc".into())),
            (PROJECT_NAME_ALIAS_KEY, Value::String("alias-x".into())),
        ]);
        let view = AgentContextView::from_ctx(&ctx);
        assert_eq!(view.task_id, "ST-1");
        assert_eq!(view.agent, "planner");
        assert_eq!(view.attempt, 3);
        assert_eq!(view.project_root.as_deref(), Some("/repo"));
        assert_eq!(view.work_dir.as_deref(), Some("/repo/work"));
        assert_eq!(view.task_metadata, Some(serde_json::json!({"issue": 20})));
        assert_eq!(view.run_id.as_deref(), Some("R-abc"));
        assert_eq!(view.project_name_alias.as_deref(), Some("alias-x"));
        assert!(view.extra.is_empty());
    }

    #[test]
    fn from_ctx_absent_runtime_keys_stay_none() {
        let ctx = ctx_with_runtime(&[]);
        let view = AgentContextView::from_ctx(&ctx);
        assert!(view.project_root.is_none());
        assert!(view.work_dir.is_none());
        assert!(view.task_metadata.is_none());
        assert!(view.run_id.is_none());
        assert!(view.project_name_alias.is_none());
    }

    #[test]
    fn materialized_or_from_ctx_prefers_materialized_view() {
        let mut view = AgentContextView::from_ctx(&ctx_with_runtime(&[]));
        view.task_id = "ST-materialized".into();
        view.extra
            .insert("custom".into(), Value::String("value".into()));
        let mut ctx = ctx_with_runtime(&[]);
        ctx.meta.runtime.insert(
            AGENT_CONTEXT_KEY.to_string(),
            serde_json::to_value(&view).unwrap(),
        );

        let resolved = AgentContextView::materialized_or_from_ctx(&ctx);
        assert_eq!(resolved.task_id, "ST-materialized");
        assert_eq!(
            resolved.extra.get("custom"),
            Some(&Value::String("value".into()))
        );
    }

    #[test]
    fn materialized_or_from_ctx_falls_back_when_key_absent() {
        let ctx = ctx_with_runtime(&[(TASK_PROJECT_ROOT_KEY, Value::String("/repo".into()))]);
        let resolved = AgentContextView::materialized_or_from_ctx(&ctx);
        assert_eq!(resolved.project_root.as_deref(), Some("/repo"));
    }

    #[test]
    fn materialized_or_from_ctx_falls_back_when_value_malformed() {
        let mut ctx = ctx_with_runtime(&[]);
        ctx.meta.runtime.insert(
            AGENT_CONTEXT_KEY.to_string(),
            Value::String("not an object".into()),
        );
        // Malformed AGENT_CONTEXT_KEY value must not panic or propagate an
        // error — fall back to from_ctx (identity fields still resolve).
        let resolved = AgentContextView::materialized_or_from_ctx(&ctx);
        assert_eq!(resolved.task_id, "ST-1");
    }

    #[test]
    fn serde_round_trip_skips_absent_optionals_and_empty_extra() {
        let view = AgentContextView {
            task_id: "ST-1".into(),
            agent: "planner".into(),
            attempt: 1,
            ..Default::default()
        };
        let json = serde_json::to_value(&view).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(
            obj.len(),
            3,
            "only identity fields should serialize: {obj:?}"
        );
        assert!(obj.contains_key("task_id"));
        assert!(obj.contains_key("agent"));
        assert!(obj.contains_key("attempt"));

        let round_tripped: AgentContextView = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, view);
    }

    #[test]
    fn serde_round_trip_full_view() {
        let mut view = AgentContextView::from_ctx(&ctx_with_runtime(&[
            (TASK_PROJECT_ROOT_KEY, Value::String("/repo".into())),
            (RUN_ID_KEY, Value::String("R-1".into())),
        ]));
        view.extra
            .insert("flow_ir_ctx".into(), serde_json::json!({"k": "v"}));
        let json = serde_json::to_value(&view).unwrap();
        let round_tripped: AgentContextView = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, view);
    }

    #[test]
    fn json_schema_export_contains_all_fields() {
        let schema = schemars::schema_for!(AgentContextView);
        let json = serde_json::to_value(&schema).unwrap();
        let props = json["properties"].as_object().expect("properties object");
        for field in [
            "task_id",
            "agent",
            "attempt",
            "project_root",
            "work_dir",
            "task_metadata",
            "run_id",
            "project_name_alias",
            "extra",
            "steps",
        ] {
            assert!(props.contains_key(field), "schema missing field {field}");
        }
    }

    #[test]
    fn context_policy_default_is_pass_all() {
        let view = AgentContextView::from_ctx(&ctx_with_runtime(&[
            (TASK_PROJECT_ROOT_KEY, Value::String("/repo".into())),
            (RUN_ID_KEY, Value::String("R-1".into())),
        ]));
        let filtered = view.clone().apply_policy(&ContextPolicy::default());
        assert_eq!(filtered, view);
    }

    #[test]
    fn context_policy_include_only_keeps_listed_fields() {
        let view = AgentContextView::from_ctx(&ctx_with_runtime(&[
            (TASK_PROJECT_ROOT_KEY, Value::String("/repo".into())),
            (TASK_WORK_DIR_KEY, Value::String("/repo/work".into())),
            (RUN_ID_KEY, Value::String("R-1".into())),
        ]));
        let policy = ContextPolicy {
            include: Some(vec!["project_root".to_string()]),
            exclude: Vec::new(),
            ..Default::default()
        };
        let filtered = view.apply_policy(&policy);
        assert_eq!(filtered.project_root.as_deref(), Some("/repo"));
        assert!(filtered.work_dir.is_none());
        assert!(filtered.run_id.is_none());
    }

    #[test]
    fn context_policy_exclude_drops_listed_fields() {
        let view = AgentContextView::from_ctx(&ctx_with_runtime(&[
            (TASK_PROJECT_ROOT_KEY, Value::String("/repo".into())),
            (TASK_WORK_DIR_KEY, Value::String("/repo/work".into())),
        ]));
        let policy = ContextPolicy {
            include: None,
            exclude: vec!["work_dir".to_string()],
            ..Default::default()
        };
        let filtered = view.apply_policy(&policy);
        assert_eq!(filtered.project_root.as_deref(), Some("/repo"));
        assert!(filtered.work_dir.is_none());
    }

    #[test]
    fn context_policy_exclude_beats_include() {
        let view = AgentContextView::from_ctx(&ctx_with_runtime(&[(
            TASK_PROJECT_ROOT_KEY,
            Value::String("/repo".into()),
        )]));
        let policy = ContextPolicy {
            include: Some(vec!["project_root".to_string()]),
            exclude: vec!["project_root".to_string()],
            ..Default::default()
        };
        let filtered = view.apply_policy(&policy);
        assert!(filtered.project_root.is_none());
    }

    #[test]
    fn context_policy_never_filters_identity_fields() {
        let view = AgentContextView::from_ctx(&ctx_with_runtime(&[]));
        let policy = ContextPolicy {
            include: Some(vec![]),
            exclude: vec![
                "task_id".to_string(),
                "agent".to_string(),
                "attempt".to_string(),
            ],
            ..Default::default()
        };
        let filtered = view.clone().apply_policy(&policy);
        assert_eq!(filtered.task_id, view.task_id);
        assert_eq!(filtered.agent, view.agent);
        assert_eq!(filtered.attempt, view.attempt);
    }

    #[test]
    fn context_policy_exclude_removes_extra_key() {
        let mut view = AgentContextView::from_ctx(&ctx_with_runtime(&[]));
        view.extra
            .insert("secret".into(), Value::String("x".into()));
        view.extra.insert("kept".into(), Value::String("y".into()));
        let policy = ContextPolicy {
            include: None,
            exclude: vec!["secret".to_string()],
            ..Default::default()
        };
        let filtered = view.apply_policy(&policy);
        assert!(!filtered.extra.contains_key("secret"));
        assert!(filtered.extra.contains_key("kept"));
    }

    #[test]
    fn to_directive_header_renders_task_metadata_and_omits_absent_fields() {
        let view = AgentContextView {
            task_id: "ST-1".into(),
            agent: "planner".into(),
            attempt: 1,
            project_root: Some("/repo".into()),
            work_dir: None,
            task_metadata: Some(serde_json::json!({"issue": 20})),
            run_id: None,
            project_name_alias: None,
            extra: serde_json::Map::new(),
            steps: Vec::new(),
        };
        let header = view.to_directive_header();
        assert_eq!(
            header,
            "project_root: /repo\ntask_metadata: {\"issue\":20}\n"
        );
        assert!(!header.contains("work_dir:"));
        assert!(!header.contains("project_name_alias:"));
    }

    #[test]
    fn to_directive_header_renders_alias_root_work_dir_in_existing_order() {
        let view = AgentContextView {
            task_id: "ST-1".into(),
            agent: "planner".into(),
            attempt: 1,
            project_root: Some("/repo".into()),
            work_dir: Some("/repo/work".into()),
            task_metadata: None,
            run_id: None,
            project_name_alias: Some("alias-x".into()),
            extra: serde_json::Map::new(),
            steps: Vec::new(),
        };
        let header = view.to_directive_header();
        assert_eq!(
            header,
            "project_name_alias: alias-x\nproject_root: /repo\nwork_dir: /repo/work\n"
        );
    }

    #[test]
    fn to_directive_header_empty_view_renders_empty_string() {
        let view = AgentContextView {
            task_id: "ST-1".into(),
            agent: "planner".into(),
            attempt: 1,
            ..Default::default()
        };
        assert_eq!(view.to_directive_header(), "");
    }

    #[test]
    fn extra_field_propagates_to_directive_header_and_serialized_json() {
        let mut view = AgentContextView {
            task_id: "ST-1".into(),
            agent: "planner".into(),
            attempt: 1,
            ..Default::default()
        };
        view.extra
            .insert("step_meta".into(), serde_json::json!({"loop_idx": 2}));

        let header = view.to_directive_header();
        assert_eq!(header, "step_meta: {\"loop_idx\":2}\n");

        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(
            json.get("extra").and_then(|e| e.get("step_meta")),
            Some(&serde_json::json!({"loop_idx": 2}))
        );
    }

    /// GH #21 Phase 1 path-compat guard: `ContextPolicy` moved to the
    /// schema crate, but every caller importing it as
    /// `crate::core::agent_context::ContextPolicy` (this module's `pub
    /// use` re-export) must keep resolving unchanged.
    #[test]
    fn context_policy_path_compat_reexport() {
        let policy: crate::core::agent_context::ContextPolicy =
            crate::core::agent_context::ContextPolicy::default();
        assert_eq!(policy, mlua_swarm_schema::ContextPolicy::default());
    }
}
