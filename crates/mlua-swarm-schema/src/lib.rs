//! Blueprint schema — Swarm IF SoT (= the core type set that defines "how a Blueprint object is written").
//!
//! This crate provides **schema types + serde derives only** as a pure IF crate. Execution
//! layers (SpawnerFactory / EngineDispatcher / Compiler) are not included here; consumers
//! (the `mlua-swarm` crate) own them. External consumers, sibling worktrees, and
//! future bundles can read/write Blueprints by depending on this single crate.
//!
//! # Versioning contract
//!
//! `Blueprint.schema_version` is tied to this crate's semver. It is fixed at 0.1.0 for now;
//! during 0.x breaking changes are free, and 1.0 will freeze the schema.
//!
//! # IN-immutability (extension discipline)
//!
//! This crate is the IN side of the swarm layering and stays **plain serde
//! data**: no compile pass, no field the engine macro-expands, no DSL
//! dialect. Flow conds are written literally against the Flow.ir Expr set
//! (`Eq($.<step>.verdict, Lit("blocked"))` — domain verdicts are plain
//! strings in step output). Authoring sugar (builders) lives OUT on the
//! consumer side; runtime behavior extension lives in the engine's
//! `SpawnerLayer` middleware.
//!
//! # AgentKind handling (= internal SoT)
//!
//! [`AgentKind`] is the SoT for the SpawnerAdapter offering axis. It is a closed enum managed
//! inside Swarm, extended by variant addition through **explicit maintenance**. String lookup
//! or a `Custom` escape hatch is deliberately avoided (= structurally eliminates the "silly
//! runtime typos" class of failures).
//!
//! # Examples
//!
//! Build a minimal [`Blueprint`] with a single [`AgentDef`] via struct literal:
//!
//! ```
//! use mlua_swarm_schema::{
//!     AgentDef, AgentKind, Blueprint, current_schema_version,
//! };
//! use mlua_flow_ir::{Expr, Node};
//! use serde_json::json;
//!
//! let bp = Blueprint {
//!     schema_version: current_schema_version(),
//!     id: "hello".into(),
//!     flow: Node::Step {
//!         ref_: "greeter".into(),
//!         in_: Expr::Lit { value: json!({"name": "world"}) },
//!         out: Expr::Path { at: "$.greeting".parse().unwrap() },
//!     },
//!     agents: vec![AgentDef {
//!         name: "greeter".into(),
//!         kind: AgentKind::RustFn,
//!         spec: json!({"fn_id": "hello_world"}),
//!         profile: None,
//!         meta: None,
//!         runner: None,
//!         runner_ref: None,
//!         verdict: None,
//!     }],
//!     operators: vec![],
//!     metas: vec![],
//!     hints: Default::default(),
//!     strategy: Default::default(),
//!     metadata: Default::default(),
//!     spawner_hints: Default::default(),
//!     default_agent_kind: AgentKind::Operator,
//!     default_operator_kind: None,
//!     default_init_ctx: None,
//!     default_agent_ctx: None,
//!     default_context_policy: None,
//!     projection_placement: None,
//!     audits: vec![],
//!     degradation_policy: None,
//!     runners: vec![],
//!     default_runner: None,
//!     check_policy: None,
//!     blueprint_ref_includes: vec![],
//! };
//!
//! assert_eq!(bp.id.as_str(), "hello");
//! assert_eq!(bp.agents.len(), 1);
//! assert_eq!(bp.strategy.strict_refs, true);
//! ```
//!
//! Round-trip a [`Blueprint`] through JSON (= confirms `serde` derives and the
//! `deny_unknown_fields` contract):
//!
//! ```
//! use mlua_swarm_schema::{AgentKind, Blueprint, BlueprintMetadata};
//! use mlua_flow_ir::{Expr, Node};
//! use serde_json::json;
//!
//! let bp = Blueprint {
//!     schema_version: mlua_swarm_schema::current_schema_version(),
//!     id: "roundtrip".into(),
//!     flow: Node::Seq { children: vec![] },
//!     agents: vec![],
//!     operators: vec![],
//!     metas: vec![],
//!     hints: Default::default(),
//!     strategy: Default::default(),
//!     metadata: BlueprintMetadata {
//!         description: Some("roundtrip smoke".into()),
//!         default_run_ttl_secs: Some(1800),
//!         ..Default::default()
//!     },
//!     spawner_hints: Default::default(),
//!     default_agent_kind: AgentKind::Operator,
//!     default_operator_kind: None,
//!     default_init_ctx: None,
//!     default_agent_ctx: None,
//!     default_context_policy: None,
//!     projection_placement: None,
//!     audits: vec![],
//!     degradation_policy: None,
//!     runners: vec![],
//!     default_runner: None,
//!     check_policy: None,
//!     blueprint_ref_includes: vec![],
//! };
//!
//! let json = serde_json::to_string(&bp).unwrap();
//! let back: Blueprint = serde_json::from_str(&json).unwrap();
//! assert_eq!(bp, back);
//! assert_eq!(back.metadata.default_run_ttl_secs, Some(1800));
//! ```

#![warn(missing_docs)]

use mlua_flow_ir::Node as FlowNode;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

// ──────────────────────────────────────────────────────────────────────────
// Versioning
// ──────────────────────────────────────────────────────────────────────────

/// Current Blueprint schema version. Tied to this crate's semver.
pub const CURRENT_SCHEMA_VERSION: &str = "0.1.0";

fn default_schema_version() -> semver::Version {
    current_schema_version()
}

/// Blueprint construction helper: returns the semver of the current schema version.
/// Callers can write `schema_version: current_schema_version(),`.
pub fn current_schema_version() -> semver::Version {
    semver::Version::parse(CURRENT_SCHEMA_VERSION)
        .expect("CURRENT_SCHEMA_VERSION must be valid semver")
}

// ──────────────────────────────────────────────────────────────────────────
// BlueprintId (human-facing ID newtype)
// ──────────────────────────────────────────────────────────────────────────

/// Identifier for a Blueprint series — the domain name (`coding`,
/// `design`, `testing`, etc.). Default: [`BlueprintId::main`].
///
/// One representation across the workspace (issue #14): this type is
/// shared by the schema's [`Blueprint::id`] and the engine's store-layer
/// keys (`mlua-swarm` re-exports it at the old
/// `blueprint::store::types::BlueprintId` path). The value is
/// user-supplied — there is no prefix convention to validate, unlike the
/// engine's minted `T-` / `R-` / `ST-` ids — so construction is
/// infallible; the inner string is private so call sites go through
/// [`BlueprintId::new`] and the accessors. `#[serde(transparent)]` keeps
/// both the JSON wire shape and the generated JSON Schema a plain string.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct BlueprintId(String);

impl BlueprintId {
    /// The default series name used when a caller doesn't pick one.
    pub const MAIN: &'static str = "main";

    /// Shorthand for `BlueprintId::new(BlueprintId::MAIN)`.
    pub fn main() -> Self {
        Self(Self::MAIN.to_string())
    }

    /// Wrap any string-like value as a `BlueprintId` (user-supplied key;
    /// nothing to validate).
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the inner series name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the id and return the inner series name.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for BlueprintId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for BlueprintId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for BlueprintId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

#[cfg(test)]
mod blueprint_id_tests {
    use super::*;

    /// issue #14 convergence guard: `Blueprint.id` becoming a newtype must
    /// not change the generated JSON Schema — the property stays an inline
    /// plain string (no `$ref`), byte-compatible with the `String` era.
    #[test]
    fn blueprint_id_field_schema_stays_a_plain_inline_string() {
        let schema = schemars::schema_for!(Blueprint);
        let v = serde_json::to_value(&schema).expect("schema serializes");
        let id = &v["properties"]["id"];
        assert_eq!(id["type"], "string", "id must stay a plain string: {id}");
        assert!(id.get("$ref").is_none(), "id must not become a $ref: {id}");
    }

    /// The JSON wire shape of the newtype is the bare string.
    #[test]
    fn blueprint_id_serde_is_transparent() {
        let id = BlueprintId::new("coding");
        assert_eq!(
            serde_json::to_value(&id).unwrap(),
            serde_json::json!("coding")
        );
        let back: BlueprintId = serde_json::from_value(serde_json::json!("coding")).unwrap();
        assert_eq!(back, id);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Blueprint (top-level package)
// ──────────────────────────────────────────────────────────────────────────

/// Unified package of flow.ir + Swarm extension layers. The entry-point type of Swarm.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Blueprint {
    /// Schema version (= tied to this crate's semver). Default = `CURRENT_SCHEMA_VERSION`.
    /// Serialized as a semver string (e.g. `"0.1.0"`).
    #[serde(default = "default_schema_version")]
    #[schemars(with = "String")]
    pub schema_version: semver::Version,
    /// Blueprint identifier (= unique key within the caller's namespace).
    #[schemars(with = "String")]
    pub id: BlueprintId,
    /// Embeds the flow.ir Node verbatim (= keeps flow.ir side unpolluted).
    /// Opaque in the JSON Schema (the Node shape is owned by the `mlua-flow-ir`
    /// crate, a separate repo; see its docs for the Node / Expr grammar).
    #[schemars(with = "Value")]
    pub flow: FlowNode,
    /// Swarm extension layer: agent → backend mapping.
    #[serde(default)]
    pub agents: Vec<AgentDef>,
    /// Swarm extension layer: **design-time definition** of Operator roles (first-class).
    ///
    /// `AgentDef.spec.operator_ref` references an `OperatorDef.name` (logical role name) in
    /// this vec. Embedding runtime-generated IDs such as sid into the BP is forbidden
    /// (= collapses the design-time vs runtime boundary). Runtime backend bindings are
    /// established via the attach / register path; the BP side holds only logical names.
    ///
    /// Every `kind = Operator` agent must have its `spec.operator_ref` present in this
    /// list — the compiler validates it at `compile()` time. May be `[]` only when the
    /// Blueprint declares no Operator agents.
    #[serde(default)]
    pub operators: Vec<OperatorDef>,
    /// GH #21 Phase 2 — named, BP-scoped pool of [`MetaDef`] entries. Two
    /// independent consumers resolve names against this pool: a
    /// `$step_meta.ref` envelope embedded in a Step's evaluated `in`
    /// value (the Step tier — resolved by `EngineDispatcher` in the
    /// `mlua-swarm` core crate at dispatch time), and
    /// [`AgentMeta::meta_ref`] (the Agent tier — resolved at launch
    /// time). The pool lets multiple Steps and/or Agents share one
    /// declarative context object by name instead of repeating it
    /// inline. `[]` = no named `MetaDef`s declared (pre-#21-Phase-2
    /// Blueprints unaffected).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metas: Vec<MetaDef>,
    /// Swarm extension layer: per-agent hints (interpreted by the Compiler).
    #[serde(default)]
    pub hints: CompilerHints,
    /// Swarm extension layer: Compiler behavior strategy (strict / lenient).
    #[serde(default)]
    pub strategy: CompilerStrategy,
    /// Blueprint metadata (description / origin / tags / ttl / version label / alias).
    #[serde(default)]
    pub metadata: BlueprintMetadata,
    /// Swarm extension layer: hint keys of the layers to wrap around the SpawnerStack.
    /// Resolved by the LayerRegistry at engine bind time (= unregistered keys are silently
    /// skipped). Flow / Blueprint do not hold middleware implementations (e.g. MainAIMiddleware)
    /// directly; they only declare required capabilities as string keys (= implementations
    /// live in the engine-side LayerRegistry).
    #[serde(default)]
    pub spawner_hints: SpawnerHints,
    /// BP-wide default `AgentKind` (= fallback when `AgentDef.kind` is omitted).
    /// Four-layer cascade: (1) Schema impl Default = Operator, (2) CLI
    /// `--default-agent-kind`, (3) this field (BP JSON literal), (4) `AgentDef.kind`
    /// (per-agent literal). (5) `CompilerHints.kind_override` allows runtime override.
    /// All default resolution flows through this path.
    #[serde(default = "default_global_agent_kind")]
    pub default_agent_kind: AgentKind,
    /// BP-wide default `OperatorKind` (= the "BP Global" tier of the 4-tier
    /// `OperatorKind` cascade). `None` when the Blueprint author does not
    /// declare a default; the caller-side resolver then falls through to
    /// the hardcoded `OperatorKind::default()` (Automate).
    ///
    /// # 4-tier cascade (highest to lowest priority)
    ///
    /// 1. Runtime Agent-level (per-agent override supplied at task-launch time)
    /// 2. Runtime Global (the launch-time `operator_kind` request)
    /// 3. BP Agent-level (`OperatorDef.kind`, resolved via `AgentDef.spec.operator_ref`)
    /// 4. BP Global (this field)
    /// 5. Default Fallback (`OperatorKind::default()` = Automate)
    ///
    /// The collapse itself is implemented once on the engine side and consumed
    /// per-agent when resolving operator info.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_operator_kind: Option<OperatorKind>,
    /// Blueprint-level default initial `ctx` for flow-ir eval.
    /// `TaskLaunchService::launch` shallow-merges this with the
    /// Task-level `init_ctx` (Task wins on key collision when both
    /// are `Object`; if Task's `init_ctx` is not an `Object`, it
    /// full-replaces the default). `None` — no default is merged;
    /// backward-compat with pre-#19 Blueprints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Value>")]
    pub default_init_ctx: Option<Value>,
    /// GH #21 Phase 1 — "BP Global" tier of the agent-context supply axis:
    /// a declarative object merged into `ctx.meta.runtime` (and, for
    /// unnamed keys, `AgentContextView.extra`) targeting every agent's
    /// runtime materialization. Contrast with [`Self::default_init_ctx`]:
    /// that field seeds the flow-ir eval `ctx` once at flow start, while
    /// this one is consumed per-spawn by
    /// `AgentContextMiddleware`/`AgentContextView` (Contract C, GH #20) —
    /// a pure flow-ir eval seed vs. an Agent/LLM-boundary runtime default.
    /// `None` = no BP-global default (pre-#21 Blueprints unaffected).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Value>")]
    pub default_agent_ctx: Option<Value>,
    /// GH #21 Phase 1 — "BP Global" tier of the [`ContextPolicy`] cascade:
    /// the default filter applied to the materialized `AgentContextView`
    /// when the targeted agent declares no `AgentMeta.context_policy` of
    /// its own. `None` = pass-all (the pre-#21 behavior).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_context_policy: Option<ContextPolicy>,
    /// GH #27 (follow-up to #23) — Blueprint-declared override of the
    /// `mlua-swarm` core crate's projection placement resolver (root
    /// preference + target directory template for materialized step
    /// OUTPUT files). `None` = the resolver's byte-compat default (root =
    /// `work_dir` falling back to `project_root`; dir_template =
    /// `"workspace/tasks/{task_id}/ctx"`) — every pre-#27 Blueprint is
    /// unaffected. See [`ProjectionPlacementSpec`]'s doc for field detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_placement: Option<ProjectionPlacementSpec>,
    /// GH #34 — Blueprint-declared after-run audit hooks: the engine
    /// auto-kicks each listed [`AuditDef`]'s agent once a matching Step
    /// settles, and persists its findings as an `OutputEvent::Artifact`
    /// named `"audit:<step_ref>"` on the AUDITED step's own output tail
    /// (see `mlua-swarm` core's `AfterRunAuditMiddleware` for the
    /// dispatch mechanics). `audits[].agent` is validated at
    /// `Compiler::compile` time against `Blueprint.agents[].name`
    /// (mirrors the `operator_ref` validation). `[]` (the default) = no
    /// audit hooks declared — every pre-#34 Blueprint is unaffected,
    /// byte-for-byte.
    ///
    /// **Binding invariant**: an audit's verdict, findings, or even its
    /// own failure NEVER change the audited step's outcome or gate the
    /// flow — audits are purely observational.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audits: Vec<AuditDef>,
    /// GH #32 — Blueprint-declared policy for worker-reported degradations
    /// (see `mlua-swarm` core's `RunRecord.degradations` /
    /// `DegradationEntry`). `None` (the default) is schema-only for now:
    /// [`DegradationPolicy::Warn`] and [`DegradationPolicy::Fail`] carry the
    /// same observational behavior at this point — degradations are always
    /// persisted, never gate the flow. Engine enforcement of `Fail`
    /// (terminating a Run on any reported degradation) is a follow-up; this
    /// field only declares author intent today. Every pre-#32 Blueprint is
    /// unaffected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degradation_policy: Option<DegradationPolicy>,
    /// GH #46 M2 — named registry of [`RunnerDef`] entries (Tier 1 of the
    /// 3-tier Worker model: Runner / Agent / Context). Referenced by
    /// `AgentDef.runner_ref` and [`Self::default_runner`] by name.
    /// Same registry shape as [`Self::metas`] (GH #21 Phase 2). `[]` (the
    /// default) = no Runner registry declared — every pre-#46 Blueprint
    /// is unaffected, byte-for-byte.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runners: Vec<RunnerDef>,
    /// GH #46 M2 — the "BP Global" tier of the [`resolve_runner`] cascade:
    /// a [`RunnerDef::name`] reference into [`Self::runners`] (inline
    /// `Runner` values are not accepted here — registry names only,
    /// mirroring [`Self::default_agent_ctx`]'s design). Ranks BELOW an
    /// agent's own inline `runner` / `runner_ref` / legacy
    /// `profile.worker_binding` declaration (see [`resolve_runner`]'s
    /// cascade doc for the full precedence). `None` = no BP-wide default
    /// declared — every pre-#46 Blueprint is unaffected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_runner: Option<String>,
    /// "Blueprint" tier (tier 2) of the `check_policy`
    /// cascade: `launch request > blueprint > server config` (highest to
    /// lowest priority). The launch entry point resolves
    /// `launch.check_policy.or(blueprint.check_policy)` exactly once and
    /// threads the result into every spawned step's `TaskSpec.check_policy`;
    /// `None` here (the default) is a no declaration — resolution falls
    /// through to the launch-request tier and, absent that, to the
    /// server-wide `EngineCfg.check_policy` default. Every pre-cascade
    /// Blueprint is unaffected, byte-for-byte. See [`CheckPolicy`] for the
    /// three fail-open reaction modes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_policy: Option<CheckPolicy>,
    /// Authoring-time include list consumed by the compile-side linker
    /// (tier 2 of the include cascade — see `mlua-swarm-compile`'s
    /// `ResolveConfig`). Each entry is a directory path resolved
    /// relative to the bp.lua parent that `$agent_md` / `$file` refs
    /// will search after the parent dir itself. Bare list; the schema
    /// carries the field only so `deny_unknown_fields` won't reject a
    /// bp.lua that declares it. `[]` (the default) — no in-bp includes;
    /// every pre-cascade Blueprint is unaffected.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(with = "Vec<String>")]
    pub blueprint_ref_includes: Vec<std::path::PathBuf>,
}

/// How a submit-time projection sink reacts when a fail-open condition
/// is encountered.
///
/// This is the Swarm IF SoT type for the `check_policy` axis; the
/// `mlua-swarm` core crate re-exports it as `crate::core::config::CheckPolicy`
/// so every existing path (`EngineCfg.check_policy`, `TaskSpec.check_policy`,
/// `apply_check_policy`) keeps its old type path unchanged.
///
/// Fail-open conditions include: `work_dir` / `project_root` unresolved,
/// `OutputStore` write error, `FileProjectionAdapter::materialize_submission`
/// error, and state lookup error. Each call site inside the engine's
/// `materialize_final_submission` / `materialize_artifact_submission`
/// currently logs a `tracing::warn!` and returns without materializing the
/// file / dual-write; `CheckPolicy` is the first-class knob that lets a
/// caller opt into a different reaction without changing that behaviour by
/// default.
///
/// The three modes are (a) [`CheckPolicy::Silent`] — no log, no error,
/// operation continues; (b) [`CheckPolicy::Warn`] — log warn (existing
/// message literal preserved), no error, operation continues (the
/// default = pre-existing behaviour); (c) [`CheckPolicy::Strict`] — log
/// the same warn AND return `EngineError::CheckPolicyStrict` (in the core
/// crate) so the caller can fail the step / launch fast. When Strict
/// returns an error, the underlying `OutputStore` may already have
/// appended (dual-write side-effect is not rolled back) — this "state
/// dirty on fail" semantics is intentional: the append happens **before**
/// the fail-open branch runs, so Strict surfaces the mismatch instead of
/// hiding it.
///
/// The wire form is snake_case (`"silent"` / `"warn"` / `"strict"`); the
/// default is [`CheckPolicy::Warn`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CheckPolicy {
    /// Skip both the log warn and the error path — completely silent.
    /// The operation continues (fail-open is still in effect).
    Silent,
    /// Log a `tracing::warn!` with the call site's existing message and
    /// continue (fail-open). Default — byte-identical to the
    /// pre-`CheckPolicy` behaviour of every submit-time projection sink
    /// code path.
    #[default]
    Warn,
    /// Log the same warn AND return `EngineError::CheckPolicyStrict` (the
    /// core crate's error variant). A caller that has opted in can fail the
    /// step / launch fast instead of proceeding with a partially-realized
    /// submission. This mode also drives a launch-time pre-dispatch
    /// validation in `TaskLaunchService::launch` (the `mlua-swarm` core
    /// crate): a launch whose effective policy resolves to `Strict` and
    /// that supplies neither `project_root` nor `work_dir` is rejected
    /// with `TaskLaunchError::PreDispatch` before any step is dispatched,
    /// rather than dispatching a step that would deterministically hit
    /// this same error at its first submit-time file materialize.
    Strict,
}

/// GH #32 — Blueprint-declared policy for worker-reported degradations. See
/// [`Blueprint::degradation_policy`] for the (currently schema-only)
/// enforcement contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DegradationPolicy {
    /// Observational only (today's only enforced behavior, regardless of
    /// which variant is declared): degradations are persisted to
    /// `RunRecord.degradations` and surfaced via `mse_doctor` /
    /// `GET /v1/runs/:id`, but never change the Run's outcome.
    Warn,
    /// Declares intent to terminate the Run on any reported degradation.
    /// Not yet enforced by the engine — schema-only until the follow-up
    /// lands.
    Fail,
}

/// GH #34 — one Blueprint-declared after-run audit hook. See
/// [`Blueprint::audits`] for the persistence / invariant contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditDef {
    /// Name of the audit agent (must match a [`Blueprint::agents`] entry's
    /// `name`) the engine dispatches after a matched step settles.
    /// Validated at `Compiler::compile` time (mirrors
    /// `AgentDef.spec.operator_ref`'s `operator_ref` validation) — an
    /// unresolved name rejects compilation.
    pub agent: String,
    /// Step names this audit applies to, matched against the step's agent
    /// ref name. `None`, or a list containing the literal `"*"`, means
    /// "every step". `Some(vec![])` (an explicit empty list) audits no
    /// step. `None` is the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steps: Option<Vec<String>>,
    /// Dispatch timing for this audit's agent (see [`AuditMode`]).
    /// Defaults to [`AuditMode::Async`].
    #[serde(default)]
    pub mode: AuditMode,
}

/// GH #34 — dispatch timing for an [`AuditDef`]'s audit agent. Neither
/// variant ever changes the audited step's outcome (see
/// [`Blueprint::audits`]'s binding invariant).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditMode {
    /// Fire-and-forget: the audit runs in the background after the
    /// audited step settles; the audited step's own spawn signal returns
    /// immediately, without waiting for the audit to finish.
    #[default]
    Async,
    /// Awaited before the audited step's spawn signal is returned to the
    /// engine — still never alters that signal or the step's recorded
    /// outcome.
    Sync,
}

/// Receptacle for a Blueprint-driven filter over the materialized
/// `AgentContextView` (GH #20/#21). Declared BP-side via
/// [`Blueprint::default_context_policy`] (BP-global) or
/// `AgentMeta::context_policy` (per-agent, outranks the BP-global tier) —
/// resolved and applied by `AgentContextMiddleware` in the `mlua-swarm`
/// core crate (this crate stays execution-free; see the crate doc).
/// Default (`include: None, exclude: vec![]`) is pass-all — [`Self::allows`]
/// returns `true` for every field name.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContextPolicy {
    /// Field names to keep. `None` means "keep everything" (pass-all).
    /// Matched against the `AgentContextView` named-field strings
    /// (`"project_root"` / `"work_dir"` / `"task_metadata"` / `"run_id"` /
    /// `"project_name_alias"`) and `extra` keys by their own key string.
    /// Identity fields (`task_id` / `agent` / `attempt`) are never
    /// filtered regardless of `include`.
    #[serde(default)]
    pub include: Option<Vec<String>>,
    /// Field names to drop, applied AFTER `include` (exclude wins when a
    /// name appears in both). Same name-matching rule as `include`.
    #[serde(default)]
    pub exclude: Vec<String>,
    /// Which preceding steps' OUTPUT pointers a worker's fetch payload may
    /// see (`WorkerPayload.context.steps`, ST5 of the `projection-adapter`
    /// design). `None` = pass-all (every submitted step, the pre-ST5
    /// `ctx_step_dir` behavior); `Some(list)` = only the named steps;
    /// `Some(vec![])` = none. Evaluated by [`Self::allows_step`], a sibling
    /// of [`Self::allows`] with the same include/exclude precedence rule
    /// but a separate namespace (step names vs. `AgentContextView` field /
    /// `extra` key names never collide).
    #[serde(default)]
    pub steps: Option<Vec<String>>,
    /// Step names to drop, applied AFTER `steps` (exclude wins when a name
    /// appears in both). Same name-matching rule as `steps`.
    #[serde(default)]
    pub steps_exclude: Vec<String>,
}

impl ContextPolicy {
    /// Whether `name` survives this policy: `false` if `exclude` lists it;
    /// otherwise `true` when `include` is `None` (pass-all) or lists
    /// `name`. Shared by both the schema crate (tests) and the `mlua-swarm`
    /// core crate's `AgentContextView::apply_policy`, so the include/exclude
    /// evaluation rule has exactly one implementation.
    pub fn allows(&self, name: &str) -> bool {
        if self.exclude.iter().any(|excluded| excluded == name) {
            return false;
        }
        match &self.include {
            Some(list) => list.iter().any(|included| included == name),
            None => true,
        }
    }

    /// Whether the preceding step named `name` survives this policy for the
    /// worker fetch payload's `context.steps` pointer list: `false` if
    /// `steps_exclude` lists it; otherwise `true` when `steps` is `None`
    /// (pass-all) or lists `name`. Same precedence rule as [`Self::allows`],
    /// evaluated against the separate `steps` / `steps_exclude` fields.
    pub fn allows_step(&self, name: &str) -> bool {
        if self.steps_exclude.iter().any(|excluded| excluded == name) {
            return false;
        }
        match &self.steps {
            Some(list) => list.iter().any(|included| included == name),
            None => true,
        }
    }
}

/// Global default `AgentKind` at the Schema impl Default layer. Bottom of the 4-layer cascade.
pub fn default_global_agent_kind() -> AgentKind {
    AgentKind::Operator
}

/// Set of **capability hint keys** for the SpawnerLayer required by a Blueprint.
///
/// # Design rationale (= for the person who will reconstruct this later)
///
/// A Blueprint is a pure layer of flow.ir + agent name binding and holds no middleware
/// **implementation**. Nevertheless there are cases where the caller must be told the BP
/// needs certain **capabilities** — e.g. "MainAI hook required", "Operator delegate path
/// required", operator role mode switching, presence/absence of senior escalation, and
/// so on.
///
/// `spawner_hints.layers` is the place where those capabilities are declared as **string
/// keys**. The engine-side `LayerRegistry` (= consumer crate) resolves key → factory and
/// wraps the compiled routes with a `SpawnerStack`. The Blueprint does not import the
/// concrete `MainAIMiddleware` type; it exposes intent through strings such as `"main_ai"`
/// (= separates the pure Flow layer from implementation details).
///
/// # Canonical hint keys
///
/// - `"main_ai"` → `MainAIMiddleware` (= fires SpawnHook before/after when kind is MainAi/Composite)
/// - `"senior_escalation"` → `SeniorEscalationMiddleware` (= fires SeniorBridge.ask on worker ok=false)
/// - `"operator_delegate"` → `OperatorDelegateMiddleware` (= delegates the entire spawn to an external Operator.execute)
///
/// # Behavior of unregistered keys
///
/// If the engine-side LayerRegistry has no matching factory, the key is **silently skipped**
/// (= lenient default). This preserves Blueprint portability (= an unsupported capability in
/// another deployment falls back gracefully).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpawnerHints {
    /// Ordered list of layer hint keys to wrap around the SpawnerStack.
    #[serde(default)]
    pub layers: Vec<String>,
}

// ──────────────────────────────────────────────────────────────────────────
// AgentDef / AgentKind / AgentProfile / AgentMeta
// ──────────────────────────────────────────────────────────────────────────

/// Maps an agent name to a Worker IMPL kind and its configuration. Referenced from flow.ir
/// `Step.ref` by name.
///
/// # Design
///
/// `AgentDef.kind` directly expresses the **Worker IMPL axis** (= not the old Spawner axis).
/// Dispatching to a host Spawner adapter (`InProcSpawner` / `ProcessSpawner` /
/// `OperatorSpawner`) is done by an internal Resolver on the compiler side. The design goal
/// is "do not make the caller aware of which Spawner hosts the Worker IMPL"; the caller
/// (Blueprint author) sees only the WorkerIMPL viewpoint.
///
/// A Spawner-axis hint (= "which adapter would you prefer running this Worker on", as a
/// priority list) will be added via a future `spawner_hint: Vec<Spawner>` field as a carry.
/// The current internal Resolver is a fixed 1:1 mapping, so the field is unnecessary today.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentDef {
    /// Agent name (= referenced from flow.ir `Step.ref`).
    pub name: String,
    /// Worker IMPL kind (= see [`AgentKind`]).
    pub kind: AgentKind,
    /// Free-form schema per kind. Interpreted by the SpawnerFactory.
    #[serde(default)]
    pub spec: Value,
    /// Agent persona information (system_prompt / model / tools, etc.). Orthogonal to the
    /// backend kind and is a first-class field. Expected to be populated by
    /// `agent_md_loader` from the frontmatter + body of an `agent.md`. `None` = an agent
    /// without a profile (= backend built solely from `spec`).
    #[serde(default)]
    pub profile: Option<AgentProfile>,
    /// Agent-level metadata (description / version / tags).
    #[serde(default)]
    pub meta: Option<AgentMeta>,
    /// GH #46 M2 — inline [`Runner`] declaration: the highest-priority
    /// tier of the [`resolve_runner`] cascade. `None` = this agent
    /// declares no inline Runner (falls through to [`Self::runner_ref`],
    /// then the legacy `profile.worker_binding` fallback, then
    /// `Blueprint.default_runner`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner: Option<Runner>,
    /// GH #46 M2 — a [`RunnerDef::name`] reference into
    /// `Blueprint.runners` (second-priority tier of [`resolve_runner`]).
    /// `None` = this agent declares no Runner registry reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_ref: Option<String>,
    /// GH #50 — opt-in declaration of which OUTPUT channel this agent's
    /// verdict token lives on, and the closed set of tokens it may emit
    /// through that channel (see [`VerdictContract`]). Consumed by the
    /// `mlua-swarm` core crate's `Compiler::compile` to lint
    /// `Branch`/`Loop` `Eq`/`Ne`/`In` conds against this agent's output at
    /// register time; a follow-up submit-time producer gate is a separate
    /// enforcement point. `None` (the default) — this agent declares no
    /// contract; a cond comparing its output to a literal is unchanged (at
    /// most a `tracing::warn!`, never rejected) — every pre-GH-#50
    /// Blueprint is unaffected, byte-for-byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<VerdictContract>,
}

/// Agent persona information. Orthogonal to the backend kind (Shell / InProc / Operator).
///
/// Populated by `agent_md_loader::load_dir` from the frontmatter and Markdown body of
/// `agents/*.md` in agent-profiles. The backend (e.g. AgentBlockOperator) receives this
/// struct at construction / dispatch time and consumes `system_prompt` as the LLM API
/// system message and `model` / `tools` as configuration.
///
/// C-C-specific fields (`permissionMode` / `memory` / `abtest`, etc.) are dumped into
/// `extras: Value`, and consumers that need them read them out. This is the escape hatch
/// that keeps the schema future-proof rather than making it strict.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentProfile {
    /// Markdown body (= system prompt content).
    #[serde(default)]
    pub system_prompt: String,
    /// LLM model identifier (e.g. `"sonnet"` / `"haiku"` / `"opus"`).
    #[serde(default)]
    pub model: Option<String>,
    /// Reasoning effort (e.g. `"low"` / `"medium"` / `"high"`).
    #[serde(default)]
    pub effort: Option<String>,
    /// List of available tool names (normalized from the CSV form in frontmatter).
    #[serde(default)]
    pub tools: Vec<String>,
    /// Frontmatter `description`. A short one-line description.
    #[serde(default)]
    pub description: Option<String>,
    /// C-C-specific / future-proof fields (permissionMode / memory / abtest / ...).
    /// Shape is the leftover keys of the agent.md frontmatter dumped as a JSON object.
    #[serde(default)]
    pub extras: Value,
    /// Content hash (blake3 32-byte hex) of the agent body (= `system_prompt`).
    ///
    /// # Purpose
    ///
    /// When the Enhance loop receives a Patch that replaces
    /// `/agents/N/profile/system_prompt`, the post-hook in `patch_applier.lua`
    /// recomputes this field (= new blake3 of the body) and updates it automatically.
    /// This is the field that structurally prevents a Blueprint carrying a stale hash
    /// from being committed.
    ///
    /// - `None` = hash not computed (= manually built agent, or a Blueprint predating this field)
    /// - `Some(hex)` = latest hash at agent-profiles seed time or after PatchApplier
    ///
    /// Planned to be used as the cache-index key in `AgentStore`.
    #[serde(default)]
    pub version_hash: Option<String>,
    /// Claude Code SubAgent definition name this agent binds to at spawn
    /// time (e.g. "mse-worker-coder"). Why: the Blueprint is the single
    /// source of truth for the declaration↔executor binding — an external
    /// registry would duplicate what `tools` already declares and drift.
    /// `None` is valid for agents whose operator backend never dispatches
    /// a SubAgent (direct-LLM operators); WS thin-path operators require
    /// it at compile time (see `Operator::requires_worker_binding`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_binding: Option<String>,
}

/// SoT of the **Worker IMPL axis**. A closed enum managed inside Swarm and extended by
/// variant addition through **explicit maintenance**. String lookup / escape hatches are
/// deliberately not adopted.
///
/// This enum **expresses Worker IMPL directly**; dispatching to a host Spawner adapter is
/// resolved by an internal Resolver on the compiler side (= callers see only the Worker
/// IMPL viewpoint).
///
/// # Internal Resolver mapping (= currently a fixed 1:1, carry: priority list form)
///
/// | AgentKind | Host Spawner adapter |
/// |---|---|
/// | `Lua` | `InProcSpawner` (mlua VM eval) |
/// | `RustFn` | `InProcSpawner` (Rust closure) |
/// | `AgentBlock` | `InProcSpawner` (agent-block-core SDK in-process) |
/// | `Subprocess` | `ProcessSpawner` (child process launch) |
/// | `Operator` | `OperatorSpawner` (interactive role / Human-MainAI delegation) |
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentKind {
    /// Lua script eval through the mlua VM (= factory-side registry looked up by `spec.fn_id`).
    Lua,
    /// Rust closure (= factory-side registry looked up by `spec.fn_id`).
    RustFn,
    /// Headless LLM agent via the agent-block-core SDK (in-process).
    AgentBlock,
    /// Child-process launch (= `spec.program` + `args`, via the ProcessSpawner path).
    Subprocess,
    /// Interactive Operator role (= MainAI / Human delegation, `spec.operator_ref`).
    Operator,
}

// ──────────────────────────────────────────────────────────────────────────
// VerdictContract / VerdictChannel (GH #50 — opt-in cond↔output-shape lint)
// ──────────────────────────────────────────────────────────────────────────

/// Opt-in per-agent declaration of the step OUTPUT shape a downstream
/// `Branch`/`Loop` `cond` is allowed to structurally compare against — see
/// the `blueprint-authoring.md` guide's "Returning verdicts to drive BP
/// flow" section for the Pattern A/B shapes this mirrors. Consumed by the
/// `mlua-swarm` core crate's `Compiler::compile` (a register-time,
/// read-only lint over `Branch`/`Loop` `Eq`/`Ne`/`In` conds — no `flow`
/// rewriting, no new `Expr` forms) and, as a follow-up, by the server's
/// submit-time producer gate. `None` on [`AgentDef::verdict`] (the
/// default) means neither enforcement point runs for that agent — the
/// pre-GH-#50 behavior, byte-for-byte.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VerdictContract {
    /// Which OUTPUT channel carries the verdict token — see
    /// [`VerdictChannel`].
    pub channel: VerdictChannel,
    /// Closed set of the verdict tokens this agent may emit through the
    /// declared `channel` (e.g. `["PASS", "BLOCKED"]`). A `Branch`/`Loop`
    /// cond's `Lit` operand(s) compared against this agent's declared
    /// channel must be members of this set.
    pub values: Vec<String>,
}

/// Which step OUTPUT channel a [`VerdictContract`] addresses — the two
/// canonical submit shapes documented in the `blueprint-authoring.md`
/// guide's "Returning verdicts to drive BP flow" section.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum VerdictChannel {
    /// Pattern A — the plain step OUTPUT body IS the verdict scalar; a cond
    /// addresses it as the bare step output (`$.<step>`).
    Body,
    /// Pattern B — the verdict is staged as the named part `"verdict"`
    /// alongside a separate plain-body report; a cond addresses it as
    /// `$.<step>.parts.verdict` (equivalently `$.<step>.parts["verdict"]`
    /// — both forms normalize to the same canonical [`Path`](mlua_flow_ir::Path) `Display`).
    Part,
}

// ──────────────────────────────────────────────────────────────────────────
// Runner / RunnerDef / WorkerModel / resolve_runner (GH #46 Milestone 2)
// ──────────────────────────────────────────────────────────────────────────

/// The execution shell an agent's Worker IMPL runs inside — holding tool
/// grant, model selection, and runtime capabilities. Tier 1 of the GH #46
/// 3-tier Worker model (Runner / Agent / Context).
///
/// Runner here is broader than the ADK / OpenAI Agents SDK Runner (a loop
/// driver): it is the execution shell holding tool grant, model
/// selection, and runtime capabilities. Loop driving itself is the
/// backend's job (Claude Code harness / AgentBlock runtime).
///
/// Resolved per-agent by [`resolve_runner`]'s 5-step cascade; wiring the
/// resolved value into the launch path is Milestone 3 — this Milestone
/// only declares the shape and the pure resolver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum Runner {
    /// WS backend: Claude Code subagent wrapper. `variant` is the
    /// wrapper's subagent_type; `tools` mirrors the wrapper frontmatter =
    /// enforced grant.
    WsClaudeCode {
        /// The wrapper's `subagent_type` (= `WorkerBinding.variant` in the
        /// `mlua-swarm` core crate).
        variant: String,
        /// Declared (informational) tool list — mirrors the wrapper
        /// frontmatter; the actual grant is enforced by the wrapper file
        /// itself, not by this list.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tools: Vec<String>,
    },
    /// In-process backend: agent-block runtime. `tools` is the effective
    /// (enforced) tool set for the in-process registry.
    AgentBlockInProcess {
        /// Effective (enforced) tool set passed to the agent-block
        /// runtime's registry — unlike `WsClaudeCode::tools`, this list is
        /// not merely informational.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tools: Vec<String>,
    },
}

/// One [`Blueprint::runners`] registry entry — a named [`Runner`]
/// declaration referenced by `AgentDef.runner_ref` /
/// [`Blueprint::default_runner`]. Same registry shape as [`MetaDef`] (GH
/// #21 Phase 2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunnerDef {
    /// Registry key, referenced by `AgentDef.runner_ref` /
    /// `Blueprint.default_runner`.
    pub name: String,
    /// The declared Runner.
    pub runner: Runner,
}

/// Canonical GH #46 Worker unit: a resolved [`Runner`] paired with the
/// [`AgentDef`] it backs. The Milestone 4 adapter is the consumer that
/// turns this into a runtime spawn; this crate only declares the shape
/// (no execution logic lives here — see the crate doc's IN-immutability
/// discipline).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerModel {
    /// The resolved Runner.
    pub runner: Runner,
    /// The agent this Runner backs.
    pub agent: AgentDef,
}

/// Everything [`resolve_runner`] can fail with: an `AgentDef.runner_ref`
/// / `Blueprint.default_runner` reference that names no entry in
/// `Blueprint.runners`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RunnerResolveError {
    /// `AgentDef.runner_ref` names a [`RunnerDef::name`] absent from
    /// `Blueprint.runners`.
    #[error(
        "agent '{agent}' runner_ref '{ref_name}' does not match any RunnerDef.name in \
         Blueprint.runners (defined: {available:?})"
    )]
    UnknownRunnerRef {
        /// The agent whose `runner_ref` didn't resolve.
        agent: String,
        /// The `runner_ref` value that was looked up.
        ref_name: String,
        /// The `RunnerDef.name`s that *are* declared, for the error message.
        available: Vec<String>,
    },
    /// `Blueprint.default_runner` names a [`RunnerDef::name`] absent from
    /// `Blueprint.runners`.
    #[error(
        "default_runner '{ref_name}' does not match any RunnerDef.name in Blueprint.runners \
         (defined: {available:?})"
    )]
    UnknownDefaultRunner {
        /// The `default_runner` value that was looked up.
        ref_name: String,
        /// The `RunnerDef.name`s that *are* declared, for the error message.
        available: Vec<String>,
    },
}

/// Resolve `agent`'s effective [`Runner`] against `bp`, in cascade order
/// (highest priority first):
///
/// 1. `agent.runner` (inline declaration) — wins unconditionally.
/// 2. `agent.runner_ref`, resolved against `bp.runners` (an unresolved
///    name is [`RunnerResolveError::UnknownRunnerRef`]).
/// 3. Legacy fallback (agent-level): `agent.profile.worker_binding =
///    Some(variant)` becomes `Runner::WsClaudeCode { variant,
///    tools: profile.tools.clone() }` — the same synthesis
///    `crate::service::task_launch::derive_worker_bindings` (in the
///    `mlua-swarm` core crate) performs at launch time today.
/// 4. `bp.default_runner`, resolved against `bp.runners` (an unresolved
///    name is [`RunnerResolveError::UnknownDefaultRunner`]).
/// 5. `Ok(None)` — no Runner declared through any tier.
///
/// **Legacy (agent-level) beats `default_runner` (BP-global)**: tier 3
/// outranks tier 4, the same "agent-level wins over BP-global" rule the
/// ctx cascade (`AgentInline > MetaRef > BpGlobal`, see
/// `mlua-swarm`'s `core::explain::CtxTier`) already follows.
///
/// Pure and read-only: this Milestone does not wire the result into the
/// launch / compile path (Milestone 3 scope) — it only declares the
/// resolver.
pub fn resolve_runner(
    bp: &Blueprint,
    agent: &AgentDef,
) -> Result<Option<Runner>, RunnerResolveError> {
    // 1. inline — wins unconditionally.
    if let Some(runner) = &agent.runner {
        return Ok(Some(runner.clone()));
    }

    // 2. runner_ref → bp.runners lookup.
    if let Some(ref_name) = &agent.runner_ref {
        return match bp.runners.iter().find(|def| &def.name == ref_name) {
            Some(def) => Ok(Some(def.runner.clone())),
            None => Err(RunnerResolveError::UnknownRunnerRef {
                agent: agent.name.clone(),
                ref_name: ref_name.clone(),
                available: bp.runners.iter().map(|d| d.name.clone()).collect(),
            }),
        };
    }

    // 3. legacy fallback (agent-level `profile.worker_binding`) — outranks
    // `bp.default_runner` (tier 4).
    if let Some(variant) = agent
        .profile
        .as_ref()
        .and_then(|p| p.worker_binding.as_ref())
    {
        let tools = agent
            .profile
            .as_ref()
            .map(|p| p.tools.clone())
            .unwrap_or_default();
        return Ok(Some(Runner::WsClaudeCode {
            variant: variant.clone(),
            tools,
        }));
    }

    // 4. bp.default_runner → bp.runners lookup.
    if let Some(ref_name) = &bp.default_runner {
        return match bp.runners.iter().find(|def| &def.name == ref_name) {
            Some(def) => Ok(Some(def.runner.clone())),
            None => Err(RunnerResolveError::UnknownDefaultRunner {
                ref_name: ref_name.clone(),
                available: bp.runners.iter().map(|d| d.name.clone()).collect(),
            }),
        };
    }

    // 5. nothing declared through any tier.
    Ok(None)
}

/// Which declaration tier supplied a [`BoundAgent`]'s resolved Runner.
/// Kept in the immutable snapshot so explain surfaces can distinguish a
/// first-class binding from the Claude Code compatibility fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunnerResolutionSource {
    /// `AgentDef.runner`.
    AgentInline,
    /// `AgentDef.runner_ref` resolved through `Blueprint.runners`.
    AgentRef,
    /// Deprecated `AgentProfile.worker_binding` compatibility path.
    LegacyWorkerBinding,
    /// `Blueprint.default_runner` resolved through `Blueprint.runners`.
    BlueprintDefault,
    /// No Runner applies to this in-process or otherwise unbound agent.
    None,
}

/// Strongly typed identity of one immutable [`BoundAgent`] snapshot.
/// Transparent serde keeps the public JSON wire form a plain string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct BindingDigest(String);

impl BindingDigest {
    /// Compute the canonical `sha256:<lowercase-hex>` digest of `bytes`.
    pub fn sha256(bytes: impl AsRef<[u8]>) -> Self {
        use sha2::Digest as _;
        Self(format!(
            "sha256:{}",
            hex::encode(sha2::Sha256::digest(bytes.as_ref()))
        ))
    }

    /// Borrow the stable wire representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BindingDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for BindingDigest {
    type Err = BindingDigestParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some(hex_part) = value.strip_prefix("sha256:") else {
            return Err(BindingDigestParseError::InvalidFormat(value.to_string()));
        };
        let canonical = hex_part.len() == 64
            && hex_part
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if !canonical {
            return Err(BindingDigestParseError::InvalidFormat(value.to_string()));
        }
        Ok(Self(value.to_string()))
    }
}

impl<'de> Deserialize<'de> for BindingDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use std::str::FromStr as _;
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

/// Rejection returned when an external binding digest is not in canonical
/// `sha256:<64 lowercase hex>` form.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BindingDigestParseError {
    /// Unsupported algorithm prefix, wrong length, uppercase, or non-hex.
    #[error("invalid binding digest '{0}'; expected sha256:<64 lowercase hex>")]
    InvalidFormat(String),
}

/// Platform-neutral request sent to an [`AgentBindingProvider`](https://docs.rs/mlua-swarm)
/// before a Run is dispatched.
///
/// The request contains only Swarm declarations. A provider may resolve
/// platform aliases or inspect its own execution environment, but Swarm
/// validates the returned [`BindReceipt`] before accepting it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BindRequest {
    /// Logical agent name; the receipt correlation key.
    pub agent: String,
    /// Digest of the declaration-only [`BoundAgent`] snapshot.
    pub request_digest: BindingDigest,
    /// Requested model name or tier from [`AgentProfile::model`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<String>,
    /// Minimum tool grant declared by the resolved [`Runner`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requested_tools: Vec<String>,
    /// Platform launch variant requested by the resolved [`Runner`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_variant: Option<String>,
}

/// Provider report describing the effective runtime binding for one agent.
/// This value is untrusted until Swarm validates it against [`BindRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BindReceipt {
    /// Logical agent name copied from the request.
    pub agent: String,
    /// Declaration digest copied from the request. Core rejects stale or
    /// cross-request receipts even when the logical agent name matches.
    pub request_digest: BindingDigest,
    /// Stable provider implementation identifier.
    pub provider_id: String,
    /// Provider or adapter revision used to resolve the binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_revision: Option<String>,
    /// Effective model after platform alias/tier resolution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_model: Option<String>,
    /// Effective tool grant enforced by the execution environment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effective_tools: Vec<String>,
    /// Effective platform launch variant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_variant: Option<String>,
    /// Optional digest of provider-owned evidence such as a capability
    /// manifest. The evidence body itself need not enter the Run snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_digest: Option<BindingDigest>,
}

/// Core-validated capability statement pinned into a [`BoundAgent`].
///
/// It deliberately omits the logical agent name because the containing
/// snapshot already supplies that identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BindingAttestation {
    /// Declaration-only digest the provider attested.
    pub request_digest: BindingDigest,
    /// Stable provider implementation identifier.
    pub provider_id: String,
    /// Provider or adapter revision used to resolve the binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_revision: Option<String>,
    /// Effective model after platform alias/tier resolution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_model: Option<String>,
    /// Effective tool grant, canonicalized by Swarm.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effective_tools: Vec<String>,
    /// Effective platform launch variant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_variant: Option<String>,
    /// Optional digest of provider-owned capability evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_digest: Option<BindingDigest>,
}

/// Immutable, Run-scoped result of binding the Runner / Agent / Context
/// layers for one logical agent.
///
/// This is derived state, not a fourth authoring source of truth. The full
/// [`AgentDef`] is retained deliberately: resume/replay must not re-read a
/// changed role prompt or result contract from a mutable Blueprint registry.
/// Capability attestation is adapter-owned and is therefore not guessed here;
/// the resolved [`Runner`] remains a declaration until an adapter records its
/// requested/effective comparison.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BoundAgent {
    /// Logical agent definition pinned for the Run.
    pub agent: AgentDef,
    /// Runner selected by [`resolve_runner`], if this agent needs one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner: Option<Runner>,
    /// Effective static Context policy (`AgentMeta.context_policy` wins over
    /// `Blueprint.default_context_policy`). Runtime context values are not
    /// embedded here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<ContextPolicy>,
    /// Declaration tier that supplied `runner`.
    pub runner_source: RunnerResolutionSource,
    /// Effective capability statement accepted from the injected binding
    /// provider. `None` preserves the declaration-only compatibility path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation: Option<BindingAttestation>,
    /// SHA-256 over the other fields of this snapshot, prefixed with
    /// `sha256:`. This is replay identity and an observability correlation
    /// key, not a signature.
    pub binding_digest: BindingDigest,
}

/// Failure while constructing immutable [`BoundAgent`] snapshots.
#[derive(Debug, thiserror::Error)]
pub enum BoundAgentResolveError {
    /// A Runner reference did not resolve.
    #[error(transparent)]
    Runner(#[from] RunnerResolveError),
    /// The snapshot input could not be serialized for deterministic hashing.
    #[error("bound agent '{agent}' could not be serialized for digest: {source}")]
    Digest {
        /// Logical agent name.
        agent: String,
        /// Serialization failure.
        source: serde_json::Error,
    },
    /// Strict binding rejected the deprecated Claude Code compatibility
    /// declaration instead of silently accepting it.
    #[error(
        "agent '{agent}' uses deprecated profile.worker_binding; strict binding requires runner or runner_ref"
    )]
    LegacyWorkerBindingDisabled {
        /// Logical agent that must be migrated.
        agent: String,
    },
}

#[derive(Serialize)]
struct BoundAgentDigestInput<'a> {
    agent: &'a AgentDef,
    runner: &'a Option<Runner>,
    context_policy: &'a Option<ContextPolicy>,
    runner_source: RunnerResolutionSource,
    attestation: &'a Option<BindingAttestation>,
}

impl BoundAgent {
    /// Replace the effective capability attestation and recompute replay
    /// identity over the complete immutable snapshot.
    pub fn set_attestation(
        &mut self,
        attestation: BindingAttestation,
    ) -> Result<(), BoundAgentResolveError> {
        self.attestation = Some(attestation);
        self.recompute_binding_digest()
    }

    /// Recompute `binding_digest` after a trusted snapshot mutation.
    pub fn recompute_binding_digest(&mut self) -> Result<(), BoundAgentResolveError> {
        let digest_input = BoundAgentDigestInput {
            agent: &self.agent,
            runner: &self.runner,
            context_policy: &self.context_policy,
            runner_source: self.runner_source,
            attestation: &self.attestation,
        };
        let bytes =
            serde_json::to_vec(&digest_input).map_err(|source| BoundAgentResolveError::Digest {
                agent: self.agent.name.clone(),
                source,
            })?;
        self.binding_digest = BindingDigest::sha256(bytes);
        Ok(())
    }
}

/// Resolve every `Blueprint.agents` entry into an immutable Run snapshot.
/// Output order follows `Blueprint.agents`, making persistence and explain
/// responses stable without a second sort.
pub fn resolve_bound_agents(bp: &Blueprint) -> Result<Vec<BoundAgent>, BoundAgentResolveError> {
    resolve_bound_agents_with_legacy(bp, true)
}

/// Strict counterpart to [`resolve_bound_agents`]: rejects the deprecated
/// `profile.worker_binding` fallback. This is the migration gate for callers
/// that require every binding to use the platform-neutral Runner contract.
pub fn resolve_bound_agents_strict(
    bp: &Blueprint,
) -> Result<Vec<BoundAgent>, BoundAgentResolveError> {
    resolve_bound_agents_with_legacy(bp, false)
}

fn resolve_bound_agents_with_legacy(
    bp: &Blueprint,
    allow_legacy: bool,
) -> Result<Vec<BoundAgent>, BoundAgentResolveError> {
    bp.agents
        .iter()
        .map(|agent| {
            let runner = resolve_runner(bp, agent)?;
            let runner_source = if agent.runner.is_some() {
                RunnerResolutionSource::AgentInline
            } else if agent.runner_ref.is_some() {
                RunnerResolutionSource::AgentRef
            } else if agent
                .profile
                .as_ref()
                .and_then(|p| p.worker_binding.as_ref())
                .is_some()
            {
                RunnerResolutionSource::LegacyWorkerBinding
            } else if bp.default_runner.is_some() {
                RunnerResolutionSource::BlueprintDefault
            } else {
                RunnerResolutionSource::None
            };
            if !allow_legacy && runner_source == RunnerResolutionSource::LegacyWorkerBinding {
                return Err(BoundAgentResolveError::LegacyWorkerBindingDisabled {
                    agent: agent.name.clone(),
                });
            }
            let context_policy = agent
                .meta
                .as_ref()
                .and_then(|m| m.context_policy.clone())
                .or_else(|| bp.default_context_policy.clone());
            let digest_input = BoundAgentDigestInput {
                agent,
                runner: &runner,
                context_policy: &context_policy,
                runner_source,
                attestation: &None,
            };
            let bytes = serde_json::to_vec(&digest_input).map_err(|source| {
                BoundAgentResolveError::Digest {
                    agent: agent.name.clone(),
                    source,
                }
            })?;
            let binding_digest = BindingDigest::sha256(bytes);
            Ok(BoundAgent {
                agent: agent.clone(),
                runner,
                context_policy,
                runner_source,
                attestation: None,
                binding_digest,
            })
        })
        .collect()
}

// ──────────────────────────────────────────────────────────────────────────
// OperatorDef / OperatorKind
// ──────────────────────────────────────────────────────────────────────────

/// Kind axis of an Operator role (= "in which mode does this Operator run").
/// Corresponds 1:1 with the engine's runtime `OperatorKind`. Kept as a schema
/// duplicate so that BPs can be authored while depending only on this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperatorKind {
    /// MainAI (= interactive AI Operator via WS client or SDK).
    MainAi,
    /// Automate (= normal spawn path, without human interception).
    #[default]
    Automate,
    /// Composite (= MainAi + Automate running side by side).
    Composite,
}

/// Design-time definition of an Operator role (first-class).
///
/// `AgentDef.spec.operator_ref` references this struct's `name` as a logical role name.
/// Binding to a runtime backend (WS session / SDK / pool, etc.) is established via the
/// attach path; the BP side only declares "under this logical name we expect an Operator
/// of this Kind".
///
/// `spec` is an escape hatch for kind-specific config (WS endpoint / SDK profile / pool
/// binding, etc.). Even when empty, declaring `name` + `kind` alone is enough for
/// compile-time validation to succeed (= it guarantees that agent `operator_ref` values
/// reference an existing definition).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OperatorDef {
    /// Logical role name (= design-time symbol referenced from `AgentDef.spec.operator_ref`).
    pub name: String,
    /// Display name for UI / docs (optional).
    #[serde(default)]
    pub display_name: Option<String>,
    /// Kind axis of the Operator (MainAi / Automate / Composite) — the "BP
    /// Agent-level" tier of the 4-tier `OperatorKind` cascade (see
    /// `Blueprint.default_operator_kind` for the full tier list). `None`
    /// when this `OperatorDef` does not declare a kind; the resolver then
    /// falls through to BP Global / Default Fallback for agents referencing
    /// this role via `AgentDef.spec.operator_ref`.
    #[serde(default)]
    pub kind: Option<OperatorKind>,
    /// Kind-specific config (WS endpoint / SDK profile / pool binding, etc.). Interpreted
    /// by the factory.
    #[serde(default)]
    pub spec: Value,
    /// Operator persona information (e.g. system_prompt template). Same shape as
    /// `AgentDef.profile`. Used as a template when the Operator itself plays a "role".
    /// If `None`, the agent-side profile is used instead.
    #[serde(default)]
    pub profile: Option<AgentProfile>,
    /// Operator-level metadata (description / version / tags).
    #[serde(default)]
    pub meta: Option<AgentMeta>,
}

/// Named, multi-step-shared declarative context payload (GH #21 Phase 2).
///
/// Lives in the [`Blueprint::metas`] pool and is referenced by name from
/// two independent consumers: a `$step_meta.ref` envelope embedded in a
/// Step's evaluated `in` value (the Step tier, resolved by
/// `EngineDispatcher::dispatch` in the `mlua-swarm` core crate at
/// dispatch time — see `EngineDispatcher::with_step_metas`), and
/// [`AgentMeta::meta_ref`] (the Agent tier, resolved at launch time and
/// merged UNDER the agent's inline `AgentMeta::ctx`). The pool lets
/// multiple Steps and/or Agents share one declarative context object by
/// name instead of repeating it inline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MetaDef {
    /// Logical name (= referenced by `$step_meta.ref` and
    /// `AgentMeta.meta_ref`; unique within [`Blueprint::metas`]).
    pub name: String,
    /// Declarative context payload. Consumers expect a JSON `Object` so
    /// it can be shallow-merged with an `inline` override / an agent's
    /// own `ctx` (a non-`Object` value is rejected — loudly at dispatch
    /// time for the Step tier, defensively (warn + skip) at launch time
    /// for the Agent tier); the shape is otherwise free-form.
    pub ctx: Value,
}

/// GH #27 (follow-up to #23) — Blueprint-declared override of the
/// `mlua-swarm` core crate's placement resolver
/// (`mlua_swarm::core::projection_placement::ProjectionPlacement`), which
/// decides where a Step's materialized OUTPUT file (submit-time sink,
/// server read-back, and spawn-time `ctx_projection` pointer — the "3
/// path" convergence point) is written on disk. Both fields are
/// independently optional and validated (`dir_template`) at
/// `Compiler::compile` time — see that resolver's `from_spec` doc for the
/// full rejection rules.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectionPlacementSpec {
    /// Which of the spawn-time `work_dir` / `project_root` to prefer as
    /// the materialize root, falling back to the other when the
    /// preferred one is absent. `"work_dir"` (default, current
    /// byte-compat behavior) | `"project_root"`. `None` = the default
    /// (`"work_dir"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    /// Target directory template, relative to the resolved root, with a
    /// `{task_id}` placeholder substituted at materialize time. `None` =
    /// the default (`"workspace/tasks/{task_id}/ctx"`, current byte-compat
    /// behavior). Must be non-empty, contain the `{task_id}` placeholder,
    /// stay relative, and not contain any `..` path segment — rejected at
    /// `Compiler::compile` time otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir_template: Option<String>,
}

/// Agent / Operator level metadata (description / version / tags).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentMeta {
    /// Short human-readable description.
    #[serde(default)]
    pub description: Option<String>,
    /// Free-form version label.
    #[serde(default)]
    pub version: Option<String>,
    /// Tag list for classification / routing.
    #[serde(default)]
    pub tags: Vec<String>,
    /// GH #21 Phase 1 — "BP Agent-level" tier of the agent-context supply
    /// axis: a declarative object merged into `ctx.meta.runtime` for this
    /// agent's spawns, on top of (and winning over)
    /// [`Blueprint::default_agent_ctx`]. See that field's doc for the
    /// contrast with `default_init_ctx`. `None` = this agent declares no
    /// per-agent context (the BP-global tier alone applies, if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Value>")]
    pub ctx: Option<Value>,
    /// GH #21 Phase 1 — "BP Agent-level" tier of the [`ContextPolicy`]
    /// cascade: outranks [`Blueprint::default_context_policy`] for this
    /// agent. `None` = fall through to the BP-global policy (or pass-all
    /// if that is also `None`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<ContextPolicy>,
    /// GH #21 Phase 2 — "BP Agent-level" tier of the [`MetaDef`] pool:
    /// resolves against [`Blueprint::metas`] by name. The resolved
    /// `ctx` sits UNDER this agent's inline [`Self::ctx`] (inline wins
    /// on key collision). `None` = this agent declares no shared
    /// `MetaDef` reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta_ref: Option<String>,
    /// GH #23 — the step-projection canonical name this agent's dispatched
    /// Steps should be addressed by (data-plane submit / `ContextPolicy`
    /// filter / `StepPointer`/`StepSummary` `name` / REST `:step` path /
    /// materialized file stem — see `mlua-swarm` core's
    /// `core::step_naming::StepNaming` for the table this field feeds).
    /// `None` = this agent declares no projection name; the canonical
    /// name falls back to the Step's `ref` (the flow.ir data-plane
    /// producer name), matching pre-GH-#23 behavior byte-for-byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_name: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────
// Compiler hints / strategy
// ──────────────────────────────────────────────────────────────────────────

/// Per-agent overrides / hints. Interpreted by the Compiler / SpawnerFactory; not required.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CompilerHints {
    /// Agent name → per-agent hint (= passed to `SpawnerFactory.build`).
    #[serde(default)]
    pub per_agent: HashMap<String, Value>,
    /// Global hints (= e.g. parallel limit, default timeout, ...).
    #[serde(default)]
    pub global: Value,
}

/// Compiler behavior rules. Controls strict / lenient handling and default fallback.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CompilerStrategy {
    /// If `true` (default), an unresolved `Step.ref` is an error; if `false`, it falls
    /// through to the default Spawner.
    #[serde(default = "default_true")]
    pub strict_refs: bool,
    /// If `true` (default), an `AgentKind` missing from the registry is an error; if
    /// `false`, it is skipped.
    #[serde(default = "default_true")]
    pub strict_kind: bool,
}

fn default_true() -> bool {
    true
}

impl Default for CompilerStrategy {
    fn default() -> Self {
        Self {
            strict_refs: true,
            strict_kind: true,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Blueprint metadata / origin
// ──────────────────────────────────────────────────────────────────────────

/// Blueprint-level metadata (description / origin / tags / ttl / version label / alias).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BlueprintMetadata {
    /// Short human-readable description of the Blueprint.
    #[serde(default)]
    pub description: Option<String>,
    /// Provenance record (inline / file / algocline).
    #[serde(default)]
    pub origin: BlueprintOrigin,
    /// Tag list for classification / routing.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Optional SemVer label (= match target for `TaskPipeline VersionSelector::SemVerReq`).
    /// Example: `"1.2.3"`. Rewritten by `EnhanceAdapter` on PATCH/MINOR/MAJOR bumps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_label: Option<String>,
    /// Optional LDS session alias label. The Swarm engine itself does not apply this
    /// (= it is free-form content); the value is expanded into the Spawn directive and
    /// reaches the MainAI. The MainAI is expected to establish a task session via
    /// `mcp__lds__session_create(root=..., alias=<this>)`, and to inject
    /// `LDS Session Alias: <this>` verbatim into the SubAgent dispatch prompt body.
    /// The SubAgent body then calls `mcp__lds__session_start(alias=<this>)` with the
    /// received alias. Worktree ownership is thereby unified under a single session, and
    /// cross-SubAgent / cross-worktree ownership blocks (= `not owned by this session`)
    /// cannot fire structurally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_name_alias: Option<String>,
    /// Optional default TTL (seconds) for tasks dispatched via this BP. Estimated by the
    /// Blueprint author from the flow shape (agent count × expected duration per agent).
    /// If `POST /v1/tasks` supplies `ttl_secs` explicitly, the body value wins; otherwise
    /// this metadata field is used as the default; if both are absent, the server global
    /// default (`default_run_ttl()` = 1800s) applies. Not needed for short chains (~5 min);
    /// recommended for long chains (14 agents × several minutes = 30-60 min).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_run_ttl_secs: Option<u64>,
    /// GH #50 follow-up (issue `33bc825b`): promote `VerdictValueUnhandled`
    /// compile-time lint to a hard error. When `false` (or absent), a
    /// declared `AgentDef.verdict.values` entry that no downstream cond
    /// references is only surfaced via `tracing::warn!` (informational);
    /// when `true`, `Compiler::compile` rejects the Blueprint with
    /// `CompileError::VerdictValueUnhandled`. Opt-in so existing Blueprints
    /// that intentionally leave some verdict values as silent-pass
    /// informational tokens keep compiling unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strict_verdict_handling: Option<bool>,
}

/// Provenance record of a Blueprint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BlueprintOrigin {
    /// Inline construction, e.g. via a Rust struct literal or test code.
    #[default]
    Inline,
    /// Loaded from a file.
    File {
        /// Source file path.
        path: String,
    },
    /// Emitted by an algocline strategy (traced by `session_id`).
    Algo {
        /// Algocline session identifier.
        session_id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_default_parses() {
        let v = default_schema_version();
        assert_eq!(v.to_string(), "0.1.0");
    }

    #[test]
    fn current_schema_version_const_matches() {
        assert_eq!(CURRENT_SCHEMA_VERSION, "0.1.0");
    }

    #[test]
    fn blueprint_json_schema_exports_key_properties() {
        let schema = schemars::schema_for!(Blueprint);
        let v = serde_json::to_value(&schema).expect("schema serializes");
        let props = v["properties"].as_object().expect("object schema");
        for key in [
            "schema_version",
            "id",
            "flow",
            "agents",
            "operators",
            "metas",
            "hints",
            "strategy",
            "metadata",
            "spawner_hints",
            "default_agent_kind",
            "default_operator_kind",
            "default_init_ctx",
            "default_agent_ctx",
            "default_context_policy",
            "projection_placement",
            "audits",
            "runners",
            "default_runner",
            "check_policy",
        ] {
            assert!(props.contains_key(key), "missing property: {key}");
        }
        // semver override lands as a plain string
        assert_eq!(v["properties"]["schema_version"]["type"], "string");
        // enum variants (snake_case) survive into the schema (LLM author axis)
        let dump = v.to_string();
        assert!(dump.contains("agent_block"), "AgentKind variants in schema");
        assert!(dump.contains("main_ai"), "OperatorKind variants in schema");
        // nested defs are referenced (AgentDef reachable from agents[])
        assert!(dump.contains("AgentDef"), "AgentDef definition in schema");
    }

    #[test]
    fn agent_profile_worker_binding_roundtrips_when_some() {
        let profile = AgentProfile {
            worker_binding: Some("mse-worker-coder".to_string()),
            ..Default::default()
        };
        let json = serde_json::to_value(&profile).expect("serializes");
        assert_eq!(json["worker_binding"], "mse-worker-coder");
        let back: AgentProfile = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back.worker_binding.as_deref(), Some("mse-worker-coder"));
    }

    #[test]
    fn agent_profile_worker_binding_omitted_when_none() {
        let profile = AgentProfile::default();
        let json = serde_json::to_value(&profile).expect("serializes");
        // `skip_serializing_if = "Option::is_none"` — the key must not appear at all.
        assert!(
            json.as_object().unwrap().get("worker_binding").is_none(),
            "worker_binding key must be absent when None: {json}"
        );
        let back: AgentProfile = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back.worker_binding, None);
    }

    // ──────────────────────────────────────────────────────────────
    // issue #19 ST3: `Blueprint.default_init_ctx`
    // ──────────────────────────────────────────────────────────────

    fn minimal_bp(default_init_ctx: Option<Value>) -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "bp-init-ctx-ut".into(),
            flow: FlowNode::Seq { children: vec![] },
            agents: vec![],
            operators: vec![],
            metas: vec![],
            hints: Default::default(),
            strategy: Default::default(),
            metadata: Default::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        }
    }

    #[test]
    fn blueprint_default_init_ctx_roundtrips_when_some() {
        let bp = minimal_bp(Some(serde_json::json!({ "seeded": true })));
        let json = serde_json::to_string(&bp).expect("serializes");
        let back: Blueprint = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(
            back.default_init_ctx,
            Some(serde_json::json!({ "seeded": true }))
        );
        assert_eq!(bp, back);
    }

    #[test]
    fn blueprint_default_init_ctx_omitted_when_none() {
        let bp = minimal_bp(None);
        let json = serde_json::to_value(&bp).expect("serializes");
        // `skip_serializing_if = "Option::is_none"` — the key must not appear at all
        // (pre-#19 Blueprints round-trip byte-identical through this path).
        assert!(
            json.as_object().unwrap().get("default_init_ctx").is_none(),
            "default_init_ctx key must be absent when None: {json}"
        );
        let back: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back.default_init_ctx, None);
        assert_eq!(bp, back);
    }

    #[test]
    fn blueprint_json_schema_exports_default_init_ctx_as_nullable_value() {
        let schema = schemars::schema_for!(Blueprint);
        let v = serde_json::to_value(&schema).expect("schema serializes");
        assert!(
            v["properties"]["default_init_ctx"].is_object(),
            "default_init_ctx must appear in the exported schema: {v}"
        );
    }

    // ──────────────────────────────────────────────────────────────
    // issue #21 Phase 1: `Blueprint.default_agent_ctx` /
    // `default_context_policy`, `AgentMeta.ctx` / `context_policy`,
    // `ContextPolicy`
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn blueprint_default_agent_ctx_and_context_policy_roundtrip_when_some() {
        let mut bp = minimal_bp(None);
        bp.default_agent_ctx = Some(serde_json::json!({ "org_conventions": "x" }));
        bp.default_context_policy = Some(ContextPolicy {
            include: Some(vec!["project_root".to_string()]),
            exclude: vec!["work_dir".to_string()],
            ..Default::default()
        });
        let json = serde_json::to_string(&bp).expect("serializes");
        let back: Blueprint = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(bp, back);
        assert_eq!(
            back.default_agent_ctx,
            Some(serde_json::json!({ "org_conventions": "x" }))
        );
        assert_eq!(
            back.default_context_policy,
            Some(ContextPolicy {
                include: Some(vec!["project_root".to_string()]),
                exclude: vec!["work_dir".to_string()],
                ..Default::default()
            })
        );
    }

    #[test]
    fn blueprint_default_agent_ctx_and_context_policy_omitted_when_none() {
        let bp = minimal_bp(None);
        let json = serde_json::to_value(&bp).expect("serializes");
        let obj = json.as_object().unwrap();
        assert!(
            obj.get("default_agent_ctx").is_none(),
            "default_agent_ctx key must be absent when None: {json}"
        );
        assert!(
            obj.get("default_context_policy").is_none(),
            "default_context_policy key must be absent when None: {json}"
        );
        let back: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back.default_agent_ctx, None);
        assert_eq!(back.default_context_policy, None);
        assert_eq!(bp, back);
    }

    #[test]
    fn blueprint_json_schema_exports_agent_ctx_and_context_policy() {
        let schema = schemars::schema_for!(Blueprint);
        let v = serde_json::to_value(&schema).expect("schema serializes");
        assert!(
            v["properties"]["default_agent_ctx"].is_object(),
            "default_agent_ctx must appear in the exported schema: {v}"
        );
        assert!(
            v["properties"]["default_context_policy"].is_object(),
            "default_context_policy must appear in the exported schema: {v}"
        );
    }

    // ──────────────────────────────────────────────────────────────
    // GH #27 (follow-up to #23): `Blueprint.projection_placement` /
    // `ProjectionPlacementSpec`
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn blueprint_projection_placement_roundtrips_when_some() {
        let mut bp = minimal_bp(None);
        bp.projection_placement = Some(ProjectionPlacementSpec {
            root: Some("project_root".to_string()),
            dir_template: Some("custom/{task_id}/out".to_string()),
        });
        let json = serde_json::to_string(&bp).expect("serializes");
        let back: Blueprint = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(bp, back);
        assert_eq!(
            back.projection_placement,
            Some(ProjectionPlacementSpec {
                root: Some("project_root".to_string()),
                dir_template: Some("custom/{task_id}/out".to_string()),
            })
        );
    }

    #[test]
    fn blueprint_projection_placement_omitted_when_none() {
        let bp = minimal_bp(None);
        let json = serde_json::to_value(&bp).expect("serializes");
        assert!(
            json.as_object()
                .unwrap()
                .get("projection_placement")
                .is_none(),
            "projection_placement key must be absent when None: {json}"
        );
        let back: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back.projection_placement, None);
        assert_eq!(bp, back);
    }

    #[test]
    fn blueprint_json_schema_exports_projection_placement() {
        let schema = schemars::schema_for!(Blueprint);
        let v = serde_json::to_value(&schema).expect("schema serializes");
        assert!(
            v["properties"]["projection_placement"].is_object(),
            "projection_placement must appear in the exported schema: {v}"
        );
    }

    #[test]
    fn agent_meta_ctx_and_context_policy_roundtrip_when_some() {
        let meta = AgentMeta {
            ctx: Some(serde_json::json!({ "k": "v" })),
            context_policy: Some(ContextPolicy {
                include: None,
                exclude: vec!["run_id".to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };
        let json = serde_json::to_value(&meta).expect("serializes");
        let back: AgentMeta = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, meta);
    }

    #[test]
    fn agent_meta_ctx_and_context_policy_omitted_when_none() {
        let meta = AgentMeta::default();
        let json = serde_json::to_value(&meta).expect("serializes");
        let obj = json.as_object().unwrap();
        assert!(
            obj.get("ctx").is_none(),
            "ctx key must be absent when None: {json}"
        );
        assert!(
            obj.get("context_policy").is_none(),
            "context_policy key must be absent when None: {json}"
        );
    }

    #[test]
    fn agent_meta_json_schema_exports_ctx_context_policy_and_meta_ref() {
        let schema = schemars::schema_for!(AgentMeta);
        let v = serde_json::to_value(&schema).expect("schema serializes");
        let props = v["properties"].as_object().expect("object schema");
        for key in [
            "description",
            "version",
            "tags",
            "ctx",
            "context_policy",
            "meta_ref",
            "projection_name",
        ] {
            assert!(props.contains_key(key), "missing property: {key}");
        }
    }

    // ──────────────────────────────────────────────────────────────
    // issue #21 Phase 2: `MetaDef`, `Blueprint.metas`, `AgentMeta.meta_ref`
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn meta_def_roundtrips_through_json() {
        let def = MetaDef {
            name: "heavy-scan".to_string(),
            ctx: serde_json::json!({ "work_dir": "/x" }),
        };
        let json = serde_json::to_value(&def).expect("serializes");
        let back: MetaDef = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, def);
    }

    #[test]
    fn blueprint_metas_omitted_when_empty() {
        let bp = minimal_bp(None);
        let json = serde_json::to_value(&bp).expect("serializes");
        assert!(
            json.as_object().unwrap().get("metas").is_none(),
            "metas key must be absent when empty: {json}"
        );
        let back: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert!(back.metas.is_empty());
        assert_eq!(bp, back);
    }

    #[test]
    fn blueprint_metas_roundtrips_when_non_empty() {
        let mut bp = minimal_bp(None);
        bp.metas = vec![MetaDef {
            name: "heavy-scan".to_string(),
            ctx: serde_json::json!({ "work_dir": "/x" }),
        }];
        let json = serde_json::to_string(&bp).expect("serializes");
        let back: Blueprint = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(bp, back);
        assert_eq!(back.metas.len(), 1);
        assert_eq!(back.metas[0].name, "heavy-scan");
    }

    #[test]
    fn blueprint_json_schema_exports_metas() {
        let schema = schemars::schema_for!(Blueprint);
        let v = serde_json::to_value(&schema).expect("schema serializes");
        assert!(
            v["properties"]["metas"].is_object(),
            "metas must appear in the exported schema: {v}"
        );
        let dump = v.to_string();
        assert!(dump.contains("MetaDef"), "MetaDef definition in schema");
    }

    #[test]
    fn agent_meta_meta_ref_roundtrips_when_some() {
        let meta = AgentMeta {
            meta_ref: Some("heavy-scan".to_string()),
            ..Default::default()
        };
        let json = serde_json::to_value(&meta).expect("serializes");
        assert_eq!(json["meta_ref"], "heavy-scan");
        let back: AgentMeta = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, meta);
    }

    #[test]
    fn agent_meta_meta_ref_omitted_when_none() {
        let meta = AgentMeta::default();
        let json = serde_json::to_value(&meta).expect("serializes");
        assert!(
            json.as_object().unwrap().get("meta_ref").is_none(),
            "meta_ref key must be absent when None: {json}"
        );
        let back: AgentMeta = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back.meta_ref, None);
    }

    // ──────────────────────────────────────────────────────────────
    // GH #23: `AgentMeta.projection_name`
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn agent_meta_projection_name_roundtrips_when_some() {
        let meta = AgentMeta {
            projection_name: Some("plan".to_string()),
            ..Default::default()
        };
        let json = serde_json::to_value(&meta).expect("serializes");
        assert_eq!(json["projection_name"], "plan");
        let back: AgentMeta = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, meta);
    }

    #[test]
    fn agent_meta_projection_name_omitted_when_none() {
        let meta = AgentMeta::default();
        let json = serde_json::to_value(&meta).expect("serializes");
        assert!(
            json.as_object().unwrap().get("projection_name").is_none(),
            "projection_name key must be absent when None: {json}"
        );
        let back: AgentMeta = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back.projection_name, None);
        assert_eq!(back, meta);
    }

    #[test]
    fn agent_meta_rejects_unknown_field_with_projection_name_present() {
        // `deny_unknown_fields` must still reject an unrelated stray key
        // even when `projection_name` is present alongside it (regression
        // guard: adding the field must not accidentally loosen the
        // contract for the rest of the struct).
        let json = serde_json::json!({
            "projection_name": "plan",
            "not_a_real_field": true
        });
        let err = serde_json::from_value::<AgentMeta>(json).unwrap_err();
        assert!(
            err.to_string().contains("not_a_real_field")
                || err.to_string().contains("unknown field"),
            "expected an unknown-field rejection, got: {err}"
        );
    }

    #[test]
    fn context_policy_default_allows_everything() {
        let policy = ContextPolicy::default();
        assert!(policy.allows("project_root"));
        assert!(policy.allows("anything"));
    }

    #[test]
    fn context_policy_include_only_allows_listed_names() {
        let policy = ContextPolicy {
            include: Some(vec!["project_root".to_string()]),
            exclude: vec![],
            ..Default::default()
        };
        assert!(policy.allows("project_root"));
        assert!(!policy.allows("work_dir"));
    }

    #[test]
    fn context_policy_exclude_wins_over_include() {
        let policy = ContextPolicy {
            include: Some(vec!["project_root".to_string()]),
            exclude: vec!["project_root".to_string()],
            ..Default::default()
        };
        assert!(!policy.allows("project_root"));
    }

    #[test]
    fn context_policy_roundtrips_through_json() {
        let policy = ContextPolicy {
            include: Some(vec!["a".to_string(), "b".to_string()]),
            exclude: vec!["c".to_string()],
            ..Default::default()
        };
        let json = serde_json::to_value(&policy).expect("serializes");
        let back: ContextPolicy = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, policy);
    }

    #[test]
    fn context_policy_default_roundtrips_as_empty_object() {
        let policy = ContextPolicy::default();
        let json = serde_json::to_value(&policy).expect("serializes");
        assert_eq!(
            json,
            serde_json::json!({
                "include": null,
                "exclude": [],
                "steps": null,
                "steps_exclude": [],
            })
        );
        let back: ContextPolicy = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, policy);
    }

    // ──────────────────────────────────────────────────────────────
    // ST5 (`projection-adapter`): `ContextPolicy.steps` / `steps_exclude`
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn context_policy_steps_default_allows_every_step() {
        let policy = ContextPolicy::default();
        assert!(policy.allows_step("planner"));
        assert!(policy.allows_step("anything"));
    }

    #[test]
    fn context_policy_steps_include_only_allows_listed_names() {
        let policy = ContextPolicy {
            steps: Some(vec!["planner".to_string()]),
            ..Default::default()
        };
        assert!(policy.allows_step("planner"));
        assert!(!policy.allows_step("coder"));
    }

    #[test]
    fn context_policy_steps_empty_list_allows_none() {
        let policy = ContextPolicy {
            steps: Some(vec![]),
            ..Default::default()
        };
        assert!(!policy.allows_step("planner"));
    }

    #[test]
    fn context_policy_steps_exclude_wins_over_steps() {
        let policy = ContextPolicy {
            steps: Some(vec!["planner".to_string()]),
            steps_exclude: vec!["planner".to_string()],
            ..Default::default()
        };
        assert!(!policy.allows_step("planner"));
    }

    #[test]
    fn context_policy_steps_roundtrips_through_json() {
        let policy = ContextPolicy {
            steps: Some(vec!["planner".to_string(), "coder".to_string()]),
            steps_exclude: vec!["reviewer".to_string()],
            ..Default::default()
        };
        let json = serde_json::to_value(&policy).expect("serializes");
        let back: ContextPolicy = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, policy);
    }

    // ──────────────────────────────────────────────────────────────
    // GH #34: `AuditDef`, `AuditMode`, `Blueprint.audits`
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn blueprint_audits_omitted_when_empty() {
        let bp = minimal_bp(None);
        let json = serde_json::to_value(&bp).expect("serializes");
        assert!(
            json.as_object().unwrap().get("audits").is_none(),
            "audits key must be absent when empty: {json}"
        );
        let back: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert!(back.audits.is_empty());
        assert_eq!(bp, back);
    }

    #[test]
    fn blueprint_audits_roundtrips_when_non_empty() {
        let mut bp = minimal_bp(None);
        bp.audits = vec![AuditDef {
            agent: "auditor".to_string(),
            steps: Some(vec!["worker".to_string()]),
            mode: AuditMode::Sync,
        }];
        let json = serde_json::to_string(&bp).expect("serializes");
        let back: Blueprint = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(bp, back);
        assert_eq!(back.audits.len(), 1);
        assert_eq!(back.audits[0].agent, "auditor");
        assert_eq!(back.audits[0].mode, AuditMode::Sync);
    }

    #[test]
    fn audit_def_steps_none_and_mode_default_when_omitted() {
        let json = serde_json::json!({ "agent": "auditor" });
        let def: AuditDef = serde_json::from_value(json).expect("deserializes");
        assert_eq!(def.steps, None);
        assert_eq!(def.mode, AuditMode::Async);
    }

    #[test]
    fn audit_def_rejects_unknown_field() {
        let json = serde_json::json!({ "agent": "auditor", "not_a_real_field": true });
        let err = serde_json::from_value::<AuditDef>(json).unwrap_err();
        assert!(
            err.to_string().contains("not_a_real_field")
                || err.to_string().contains("unknown field"),
            "expected an unknown-field rejection, got: {err}"
        );
    }

    #[test]
    fn audit_mode_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(AuditMode::Async).unwrap(),
            serde_json::json!("async")
        );
        assert_eq!(
            serde_json::to_value(AuditMode::Sync).unwrap(),
            serde_json::json!("sync")
        );
    }

    #[test]
    fn blueprint_json_schema_exports_audits_and_audit_def() {
        let schema = schemars::schema_for!(Blueprint);
        let v = serde_json::to_value(&schema).expect("schema serializes");
        assert!(
            v["properties"]["audits"].is_object(),
            "audits must appear in the exported schema: {v}"
        );
        let dump = v.to_string();
        assert!(dump.contains("AuditDef"), "AuditDef definition in schema");
    }

    // ──────────────────────────────────────────────────────────────
    // GH #32: `Blueprint.degradation_policy`, `DegradationPolicy`
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn blueprint_without_degradation_policy_deserializes_to_none() {
        let json = serde_json::json!({
            "schema_version": current_schema_version(),
            "id": "no-degradation-policy-ut",
            "flow": { "kind": "seq", "children": [] },
        });
        let bp: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert_eq!(bp.degradation_policy, None);
    }

    #[test]
    fn blueprint_degradation_policy_omitted_when_none() {
        let bp = minimal_bp(None);
        let json = serde_json::to_value(&bp).expect("serializes");
        assert!(
            json.as_object()
                .unwrap()
                .get("degradation_policy")
                .is_none(),
            "degradation_policy key must be absent when None: {json}"
        );
    }

    #[test]
    fn blueprint_degradation_policy_warn_and_fail_roundtrip() {
        for (label, expected) in [
            ("warn", DegradationPolicy::Warn),
            ("fail", DegradationPolicy::Fail),
        ] {
            let mut bp = minimal_bp(None);
            bp.degradation_policy = Some(expected);
            let json = serde_json::to_string(&bp).expect("serializes");
            assert!(json.contains(&format!("\"degradation_policy\":\"{label}\"")));
            let back: Blueprint = serde_json::from_str(&json).expect("deserializes");
            assert_eq!(back.degradation_policy, Some(expected));
        }
    }

    #[test]
    fn degradation_policy_rejects_unknown_variant() {
        let json = serde_json::json!({
            "schema_version": current_schema_version(),
            "id": "degradation-policy-unknown-variant-ut",
            "flow": { "kind": "seq", "children": [] },
            "degradation_policy": "ignore",
        });
        let err = serde_json::from_value::<Blueprint>(json).unwrap_err();
        assert!(
            err.to_string().contains("unknown variant"),
            "expected an unknown-variant rejection, got: {err}"
        );
    }

    // ──────────────────────────────────────────────────────────────
    // GH #46 Milestone 2: `Runner`, `RunnerDef`, `WorkerModel`,
    // `Blueprint.runners` / `default_runner`, `AgentDef.runner` /
    // `runner_ref`, `resolve_runner`
    // ──────────────────────────────────────────────────────────────

    fn agent_with_runner(
        name: &str,
        profile: Option<AgentProfile>,
        runner: Option<Runner>,
        runner_ref: Option<String>,
    ) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::RustFn,
            spec: serde_json::json!({ "fn_id": name }),
            profile,
            meta: None,
            runner,
            runner_ref,
            verdict: None,
        }
    }

    fn ws_runner(variant: &str, tools: Vec<&str>) -> Runner {
        Runner::WsClaudeCode {
            variant: variant.to_string(),
            tools: tools.into_iter().map(str::to_string).collect(),
        }
    }

    fn agent_block_runner(tools: Vec<&str>) -> Runner {
        Runner::AgentBlockInProcess {
            tools: tools.into_iter().map(str::to_string).collect(),
        }
    }

    // ─── round-trip byte-compat ─────────────────────────────────────

    #[test]
    fn blueprint_without_runners_or_default_runner_deserializes_to_defaults() {
        let json = serde_json::json!({
            "schema_version": current_schema_version(),
            "id": "no-runners-ut",
            "flow": { "kind": "seq", "children": [] },
        });
        let bp: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert!(bp.runners.is_empty());
        assert_eq!(bp.default_runner, None);
    }

    #[test]
    fn blueprint_runners_omitted_when_empty() {
        let bp = minimal_bp(None);
        let json = serde_json::to_value(&bp).expect("serializes");
        assert!(
            json.as_object().unwrap().get("runners").is_none(),
            "runners key must be absent when empty: {json}"
        );
        let back: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert!(back.runners.is_empty());
        assert_eq!(bp, back);
    }

    #[test]
    fn blueprint_runners_roundtrips_when_non_empty() {
        let mut bp = minimal_bp(None);
        bp.runners = vec![RunnerDef {
            name: "claude-worker".to_string(),
            runner: ws_runner("mse-worker-coder", vec!["Read", "Grep"]),
        }];
        let json = serde_json::to_string(&bp).expect("serializes");
        let back: Blueprint = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(bp, back);
        assert_eq!(back.runners.len(), 1);
        assert_eq!(back.runners[0].name, "claude-worker");
    }

    #[test]
    fn blueprint_default_runner_roundtrips_when_some() {
        let mut bp = minimal_bp(None);
        bp.default_runner = Some("claude-worker".to_string());
        let json = serde_json::to_value(&bp).expect("serializes");
        assert_eq!(json["default_runner"], "claude-worker");
        let back: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, bp);
    }

    #[test]
    fn blueprint_default_runner_omitted_when_none() {
        let bp = minimal_bp(None);
        let json = serde_json::to_value(&bp).expect("serializes");
        assert!(
            json.as_object().unwrap().get("default_runner").is_none(),
            "default_runner key must be absent when None: {json}"
        );
        let back: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, bp);
    }

    #[test]
    fn blueprint_json_schema_exports_runners_and_default_runner() {
        let schema = schemars::schema_for!(Blueprint);
        let v = serde_json::to_value(&schema).expect("schema serializes");
        assert!(
            v["properties"]["runners"].is_object(),
            "runners must appear in the exported schema: {v}"
        );
        assert!(
            v["properties"]["default_runner"].is_object(),
            "default_runner must appear in the exported schema: {v}"
        );
        let dump = v.to_string();
        assert!(dump.contains("RunnerDef"), "RunnerDef definition in schema");
        assert!(dump.contains("Runner"), "Runner definition in schema");
    }

    #[test]
    fn agent_def_runner_and_runner_ref_omitted_when_none() {
        let agent = agent_with_runner("scout", None, None, None);
        let json = serde_json::to_value(&agent).expect("serializes");
        let obj = json.as_object().unwrap();
        assert!(
            obj.get("runner").is_none(),
            "runner key must be absent when None: {json}"
        );
        assert!(
            obj.get("runner_ref").is_none(),
            "runner_ref key must be absent when None: {json}"
        );
        let back: AgentDef = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, agent);
    }

    #[test]
    fn agent_def_runner_inline_roundtrips_when_some() {
        let agent = agent_with_runner("coder", None, Some(agent_block_runner(vec!["Bash"])), None);
        let json = serde_json::to_string(&agent).expect("serializes");
        let back: AgentDef = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, agent);
    }

    #[test]
    fn agent_def_runner_ref_roundtrips_when_some() {
        let agent = agent_with_runner("coder", None, None, Some("claude-worker".to_string()));
        let json = serde_json::to_value(&agent).expect("serializes");
        assert_eq!(json["runner_ref"], "claude-worker");
        let back: AgentDef = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, agent);
    }

    #[test]
    fn agent_def_json_schema_exports_runner_and_runner_ref() {
        let schema = schemars::schema_for!(AgentDef);
        let v = serde_json::to_value(&schema).expect("schema serializes");
        let props = v["properties"].as_object().expect("object schema");
        for key in ["runner", "runner_ref"] {
            assert!(props.contains_key(key), "missing property: {key}");
        }
    }

    #[test]
    fn runner_ws_claude_code_roundtrips_through_json_and_tags_backend() {
        let runner = ws_runner("mse-worker-coder", vec!["Read", "Grep"]);
        let json = serde_json::to_value(&runner).expect("serializes");
        assert_eq!(json["backend"], "ws_claude_code");
        assert_eq!(json["variant"], "mse-worker-coder");
        assert_eq!(json["tools"], serde_json::json!(["Read", "Grep"]));
        let back: Runner = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, runner);
    }

    #[test]
    fn runner_agent_block_in_process_roundtrips_through_json_and_tags_backend() {
        let runner = agent_block_runner(vec!["Bash"]);
        let json = serde_json::to_value(&runner).expect("serializes");
        assert_eq!(json["backend"], "agent_block_in_process");
        assert_eq!(json["tools"], serde_json::json!(["Bash"]));
        let back: Runner = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, runner);
    }

    #[test]
    fn runner_tools_omitted_when_empty() {
        let runner = ws_runner("mse-worker-coder", vec![]);
        let json = serde_json::to_value(&runner).expect("serializes");
        assert!(
            json.as_object().unwrap().get("tools").is_none(),
            "tools key must be absent when empty: {json}"
        );
        let back: Runner = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, runner);
    }

    #[test]
    fn runner_rejects_unknown_field() {
        let json = serde_json::json!({
            "backend": "ws_claude_code",
            "variant": "x",
            "not_a_real_field": true,
        });
        let err = serde_json::from_value::<Runner>(json).unwrap_err();
        assert!(
            err.to_string().contains("not_a_real_field")
                || err.to_string().contains("unknown field"),
            "expected an unknown-field rejection, got: {err}"
        );
    }

    #[test]
    fn runner_def_roundtrips_through_json() {
        let def = RunnerDef {
            name: "claude-worker".to_string(),
            runner: ws_runner("mse-worker-coder", vec!["Read"]),
        };
        let json = serde_json::to_value(&def).expect("serializes");
        let back: RunnerDef = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, def);
    }

    #[test]
    fn worker_model_roundtrips_through_json() {
        let model = WorkerModel {
            runner: agent_block_runner(vec!["Bash"]),
            agent: agent_with_runner("coder", None, None, None),
        };
        let json = serde_json::to_value(&model).expect("serializes");
        let back: WorkerModel = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, model);
    }

    // ─── resolve_runner cascade precedence ─────────────────────────

    #[test]
    fn resolve_runner_inline_wins_over_everything() {
        let inline = agent_block_runner(vec!["Bash"]);
        let profile = AgentProfile {
            worker_binding: Some("legacy-variant".to_string()),
            tools: vec!["Read".to_string()],
            ..Default::default()
        };
        let agent = agent_with_runner(
            "coder",
            Some(profile),
            Some(inline.clone()),
            Some("registry-entry".to_string()),
        );
        let mut bp = minimal_bp(None);
        bp.default_runner = Some("registry-entry".to_string());
        bp.runners = vec![RunnerDef {
            name: "registry-entry".to_string(),
            runner: ws_runner("other-variant", vec![]),
        }];
        bp.agents = vec![agent.clone()];

        let resolved = resolve_runner(&bp, &agent).expect("resolves");
        assert_eq!(resolved, Some(inline));
    }

    #[test]
    fn resolve_runner_runner_ref_wins_over_legacy_fallback() {
        let profile = AgentProfile {
            worker_binding: Some("legacy-variant".to_string()),
            tools: vec!["Read".to_string()],
            ..Default::default()
        };
        let registry_runner = ws_runner("registry-variant", vec!["Grep"]);
        let agent = agent_with_runner(
            "coder",
            Some(profile),
            None,
            Some("registry-entry".to_string()),
        );
        let mut bp = minimal_bp(None);
        bp.runners = vec![RunnerDef {
            name: "registry-entry".to_string(),
            runner: registry_runner.clone(),
        }];
        bp.agents = vec![agent.clone()];

        let resolved = resolve_runner(&bp, &agent).expect("resolves");
        assert_eq!(resolved, Some(registry_runner));
    }

    #[test]
    fn resolve_runner_legacy_fallback_wins_over_default_runner() {
        let profile = AgentProfile {
            worker_binding: Some("legacy-variant".to_string()),
            tools: vec!["Read".to_string(), "Grep".to_string()],
            ..Default::default()
        };
        let agent = agent_with_runner("coder", Some(profile), None, None);
        let mut bp = minimal_bp(None);
        bp.default_runner = Some("registry-entry".to_string());
        bp.runners = vec![RunnerDef {
            name: "registry-entry".to_string(),
            runner: agent_block_runner(vec!["Bash"]),
        }];
        bp.agents = vec![agent.clone()];

        let resolved = resolve_runner(&bp, &agent).expect("resolves");
        assert_eq!(
            resolved,
            Some(ws_runner("legacy-variant", vec!["Read", "Grep"]))
        );
    }

    #[test]
    fn resolve_runner_default_runner_alone_when_no_agent_level_declaration() {
        let agent = agent_with_runner("coder", None, None, None);
        let mut bp = minimal_bp(None);
        bp.default_runner = Some("registry-entry".to_string());
        bp.runners = vec![RunnerDef {
            name: "registry-entry".to_string(),
            runner: agent_block_runner(vec!["Bash"]),
        }];
        bp.agents = vec![agent.clone()];

        let resolved = resolve_runner(&bp, &agent).expect("resolves");
        assert_eq!(resolved, Some(agent_block_runner(vec!["Bash"])));
    }

    #[test]
    fn resolve_runner_none_when_nothing_declared_through_any_tier() {
        let agent = agent_with_runner("coder", None, None, None);
        let bp = minimal_bp(None);

        let resolved = resolve_runner(&bp, &agent).expect("resolves");
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_runner_unknown_runner_ref_errs() {
        let agent = agent_with_runner("coder", None, None, Some("no-such-entry".to_string()));
        let mut bp = minimal_bp(None);
        bp.runners = vec![RunnerDef {
            name: "registry-entry".to_string(),
            runner: agent_block_runner(vec![]),
        }];
        bp.agents = vec![agent.clone()];

        let err = resolve_runner(&bp, &agent).expect_err("unresolved runner_ref");
        assert_eq!(
            err,
            RunnerResolveError::UnknownRunnerRef {
                agent: "coder".to_string(),
                ref_name: "no-such-entry".to_string(),
                available: vec!["registry-entry".to_string()],
            }
        );
    }

    #[test]
    fn resolve_runner_unknown_default_runner_errs() {
        let agent = agent_with_runner("coder", None, None, None);
        let mut bp = minimal_bp(None);
        bp.default_runner = Some("no-such-entry".to_string());
        bp.runners = vec![RunnerDef {
            name: "registry-entry".to_string(),
            runner: agent_block_runner(vec![]),
        }];
        bp.agents = vec![agent.clone()];

        let err = resolve_runner(&bp, &agent).expect_err("unresolved default_runner");
        assert_eq!(
            err,
            RunnerResolveError::UnknownDefaultRunner {
                ref_name: "no-such-entry".to_string(),
                available: vec!["registry-entry".to_string()],
            }
        );
    }

    #[test]
    fn bound_agent_digest_is_stable_and_tracks_runner_changes() {
        let agent = agent_with_runner(
            "coder",
            None,
            Some(ws_runner("worker-a", vec!["Read"])),
            None,
        );
        let mut bp = minimal_bp(None);
        bp.agents = vec![agent];

        let first = resolve_bound_agents(&bp).expect("binds");
        let second = resolve_bound_agents(&bp).expect("binds again");
        assert_eq!(first[0].binding_digest, second[0].binding_digest);
        assert!(first[0].binding_digest.as_str().starts_with("sha256:"));
        assert_eq!(first[0].binding_digest.as_str().len(), 71);
        assert_eq!(first[0].runner_source, RunnerResolutionSource::AgentInline);

        bp.agents[0].runner = Some(ws_runner("worker-b", vec!["Read"]));
        let changed = resolve_bound_agents(&bp).expect("binds changed runner");
        assert_ne!(first[0].binding_digest, changed[0].binding_digest);
    }

    #[test]
    fn bound_agent_pins_effective_context_policy_and_full_agent() {
        let mut agent = agent_with_runner("scout", None, None, None);
        agent.profile = Some(AgentProfile {
            system_prompt: "inspect carefully".to_string(),
            ..Default::default()
        });
        let mut bp = minimal_bp(None);
        bp.default_context_policy = Some(ContextPolicy {
            include: Some(vec!["task".to_string()]),
            ..Default::default()
        });
        bp.agents = vec![agent];

        let bound = resolve_bound_agents(&bp).expect("binds").remove(0);
        assert_eq!(
            bound.agent.profile.unwrap().system_prompt,
            "inspect carefully"
        );
        assert_eq!(
            bound.context_policy.unwrap().include,
            Some(vec!["task".to_string()])
        );
        assert_eq!(bound.runner_source, RunnerResolutionSource::None);
    }

    #[test]
    fn strict_bound_agent_resolution_rejects_legacy_worker_binding() {
        let profile = AgentProfile {
            worker_binding: Some("legacy-worker".to_string()),
            ..Default::default()
        };
        let mut bp = minimal_bp(None);
        bp.agents = vec![agent_with_runner("coder", Some(profile), None, None)];

        let err = resolve_bound_agents_strict(&bp).expect_err("legacy must fail closed");
        assert!(matches!(
            err,
            BoundAgentResolveError::LegacyWorkerBindingDisabled { agent } if agent == "coder"
        ));
    }

    #[test]
    fn binding_digest_is_a_validated_transparent_string() {
        use std::str::FromStr as _;

        let digest = BindingDigest::sha256(b"same snapshot");
        let json = serde_json::to_value(&digest).expect("serializes");
        assert_eq!(json, serde_json::Value::String(digest.to_string()));
        assert_eq!(
            serde_json::from_value::<BindingDigest>(json).expect("deserializes"),
            digest
        );
        for invalid in [
            "deadbeef",
            "sha256:abc",
            "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ] {
            assert!(
                BindingDigest::from_str(invalid).is_err(),
                "accepted {invalid}"
            );
        }
    }

    // ──────────────────────────────────────────────────────────────
    // GH #50: `AgentDef.verdict` / `VerdictContract` / `VerdictChannel`
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn verdict_contract_roundtrips_body_channel() {
        let json = serde_json::json!({"channel": "body", "values": ["PASS", "BLOCKED"]});
        let contract: VerdictContract = serde_json::from_value(json.clone()).expect("deserializes");
        assert_eq!(contract.channel, VerdictChannel::Body);
        assert_eq!(
            contract.values,
            vec!["PASS".to_string(), "BLOCKED".to_string()]
        );
        assert_eq!(serde_json::to_value(&contract).expect("serializes"), json);
    }

    #[test]
    fn verdict_contract_roundtrips_part_channel() {
        let json = serde_json::json!({"channel": "part", "values": ["ALLOW"]});
        let contract: VerdictContract = serde_json::from_value(json.clone()).expect("deserializes");
        assert_eq!(contract.channel, VerdictChannel::Part);
        assert_eq!(serde_json::to_value(&contract).expect("serializes"), json);
    }

    #[test]
    fn agent_def_verdict_omitted_when_none() {
        let agent = agent_with_runner("gate", None, None, None);
        let json = serde_json::to_value(&agent).expect("serializes");
        assert!(
            json.as_object().unwrap().get("verdict").is_none(),
            "verdict key must be absent when None: {json}"
        );
        let back: AgentDef = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back.verdict, None);
    }

    #[test]
    fn agent_def_verdict_roundtrips_when_some() {
        let mut agent = agent_with_runner("gate", None, None, None);
        agent.verdict = Some(VerdictContract {
            channel: VerdictChannel::Body,
            values: vec!["PASS".to_string(), "BLOCKED".to_string()],
        });
        let json = serde_json::to_value(&agent).expect("serializes");
        let back: AgentDef = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back.verdict, agent.verdict);
    }

    /// Acceptance criterion #2: the `02-verdict-loop.json` sample (no
    /// `verdict` field on any of its agents) must still deserialize
    /// unchanged under the new `#[serde(deny_unknown_fields)]`-constrained
    /// `AgentDef` — `verdict` is `#[serde(default)]`, so its absence is not
    /// an error.
    #[test]
    fn existing_verdict_loop_sample_deserializes_with_verdict_omitted() {
        const SAMPLE: &str =
            include_str!("../../mlua-swarm-cli/src/mcp/resources/samples/02-verdict-loop.json");
        let bp: Blueprint = serde_json::from_str(SAMPLE).expect("sample deserializes");
        assert_eq!(bp.agents.len(), 6);
        assert!(
            bp.agents.iter().all(|a| a.verdict.is_none()),
            "no agent in the sample declares a verdict contract"
        );
    }

    // ──────────────────────────────────────────────────────────────
    // CheckPolicy enum relocation + Blueprint.check_policy
    // (T1: schema round-trip / omit→None / invalid→error)
    // ──────────────────────────────────────────────────────────────

    /// The wire form is snake_case and byte-identical to the pre-relocation
    /// enum (`"silent"` / `"warn"` / `"strict"`), round-tripping in both
    /// directions — the relocation must not change the serde surface.
    #[test]
    fn check_policy_wire_form_round_trips() {
        for (variant, wire) in [
            (CheckPolicy::Silent, "silent"),
            (CheckPolicy::Warn, "warn"),
            (CheckPolicy::Strict, "strict"),
        ] {
            let json = serde_json::to_value(variant).expect("serializes");
            assert_eq!(json, serde_json::json!(wire), "wire form for {variant:?}");
            let back: CheckPolicy = serde_json::from_value(json).expect("deserializes");
            assert_eq!(back, variant, "round-trip for {variant:?}");
        }
    }

    /// The default is `Warn` (preserves the pre-CheckPolicy fail-open
    /// behaviour of every submit-time projection sink).
    #[test]
    fn check_policy_default_is_warn() {
        assert_eq!(CheckPolicy::default(), CheckPolicy::Warn);
    }

    /// A Blueprint that declares `check_policy: "strict"` parses to
    /// `Some(Strict)` and re-serializes with the same snake_case literal.
    #[test]
    fn blueprint_check_policy_strict_round_trips() {
        let json = serde_json::json!({
            "schema_version": current_schema_version(),
            "id": "check-policy-strict-ut",
            "flow": { "kind": "seq", "children": [] },
            "check_policy": "strict",
        });
        let bp: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert_eq!(bp.check_policy, Some(CheckPolicy::Strict));
        let re = serde_json::to_string(&bp).expect("serializes");
        assert!(
            re.contains("\"check_policy\":\"strict\""),
            "re-serialized BP must preserve the snake_case wire literal: {re}"
        );
    }

    /// An omitted `check_policy` parses to `None` and is skipped on
    /// serialize (backward-compat with every pre-cascade Blueprint).
    #[test]
    fn blueprint_check_policy_omitted_is_none() {
        let json = serde_json::json!({
            "schema_version": current_schema_version(),
            "id": "check-policy-omitted-ut",
            "flow": { "kind": "seq", "children": [] },
        });
        let bp: Blueprint = serde_json::from_value(json).expect("deserializes");
        assert_eq!(bp.check_policy, None);

        let out = serde_json::to_value(&bp).expect("serializes");
        assert!(
            out.as_object().unwrap().get("check_policy").is_none(),
            "check_policy key must be absent when None: {out}"
        );
    }

    /// An invalid `check_policy` value is a hard parse error (not silently
    /// dropped) — the enum is closed to the three snake_case variants. This
    /// also confirms `deny_unknown_fields` is not the gate here: the field
    /// IS known, only its value is invalid.
    #[test]
    fn blueprint_check_policy_invalid_value_errors() {
        let json = serde_json::json!({
            "schema_version": current_schema_version(),
            "id": "check-policy-invalid-ut",
            "flow": { "kind": "seq", "children": [] },
            "check_policy": "loud",
        });
        let err = serde_json::from_value::<Blueprint>(json)
            .expect_err("an unknown check_policy value must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("check_policy") || msg.contains("loud") || msg.contains("variant"),
            "error should point at the bad check_policy value: {msg}"
        );
    }
}
