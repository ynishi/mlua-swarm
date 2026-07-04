//! `Ctx` and `OperatorInfo` — cross-cutting context threaded through the
//! engine.
//!
//! The main pipeline (Engine → `SpawnerAdapter` → `WorkerAdapter`) does not
//! know about Operators. Middleware watches `Ctx.operator` and branches on
//! it.

use crate::types::TaskId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-attempt context threaded through the engine and into worker/spawner
/// code. Carries identity (`task_id` / `attempt` / `agent`), free-form
/// metadata (`meta`), and the resolved `Operator` faces (`operator`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ctx {
    /// The task this attempt belongs to.
    pub task_id: TaskId,
    /// 1-based attempt counter for `task_id` (bumped by
    /// `Engine::dispatch_attempt_with` on every dispatch).
    pub attempt: u32,
    /// Name of the agent being dispatched (`TaskSpec.agent`).
    pub agent: String,
    /// Free-form namespaced metadata (runtime / authz / observer / loop).
    pub meta: CtxMeta,
    /// The Operator faces resolved for this attempt. Not serialized —
    /// `Arc<dyn ...>` trait objects have no stable on-wire form; only the
    /// IDs (persisted on `OperatorSession`) survive a restart.
    #[serde(skip)]
    pub operator: OperatorInfo,
}

impl Ctx {
    /// Build a fresh `Ctx` with default `meta` and `operator`
    /// (`OperatorInfo::default()`, i.e. `Automate` / no bridges).
    pub fn new(task_id: TaskId, attempt: u32, agent: impl Into<String>) -> Self {
        Self {
            task_id,
            attempt,
            agent: agent.into(),
            meta: CtxMeta::default(),
            operator: OperatorInfo::default(),
        }
    }
}

/// Namespaced free-form key/value bags attached to a `Ctx`. Each namespace
/// is a convention, not an enforced schema — e.g. `runtime` carries
/// per-dispatch values like `worker_handle`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CtxMeta {
    /// Values set by the engine/spawner at dispatch time (e.g.
    /// `worker_handle`, `spawn_depth`).
    #[serde(default)]
    pub runtime: HashMap<String, Value>,
    /// Values relevant to authorization/role decisions.
    #[serde(default)]
    pub authz: HashMap<String, Value>,
    /// Values relevant to observers/tracing.
    #[serde(default)]
    pub observer: HashMap<String, Value>,
    /// Values relevant to loop/iteration bookkeeping.
    #[serde(default)]
    pub loop_ns: HashMap<String, Value>,
}

/// Who/what is driving a spawn: a plain automated worker, an interactive
/// MainAI operator, or a composite of both. Gates `MainAIMiddleware` /
/// `OperatorDelegateMiddleware` (see `OperatorInfo` doc below) and feeds
/// the 4-tier cascade resolved by `collapse_operator_kind`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorKind {
    /// An interactive, single-Operator-driven session (spawn hooks and
    /// full-spawn delegation are enabled).
    MainAi,
    /// A plain automated worker; middleware passes through into a normal
    /// spawn (the default).
    #[default]
    Automate,
    /// A mixed mode combining automated and MainAi-driven behavior (same
    /// gating as `MainAi` for middleware purposes).
    Composite,
}

impl From<mlua_swarm_schema::OperatorKind> for OperatorKind {
    fn from(k: mlua_swarm_schema::OperatorKind) -> Self {
        match k {
            mlua_swarm_schema::OperatorKind::MainAi => OperatorKind::MainAi,
            mlua_swarm_schema::OperatorKind::Automate => OperatorKind::Automate,
            mlua_swarm_schema::OperatorKind::Composite => OperatorKind::Composite,
        }
    }
}

/// The single canonical implementation of the 4-tier `OperatorKind` cascade
/// (schema doc: `mlua_swarm_schema::Blueprint::default_operator_kind`).
///
/// Each tier is optional; the first `Some` wins, top to bottom. All four
/// absent falls back to `OperatorKind::default()` (Automate).
///
/// | tier | meaning |
/// |---|---|
/// | `runtime_agent` | per-agent override supplied at task-launch time (narrowest, most direct) |
/// | `runtime_global` | the launch-time `operator_kind` request (session-wide) |
/// | `bp_agent` | `OperatorDef.kind`, resolved per-agent via `AgentDef.spec.operator_ref` |
/// | `bp_global` | `Blueprint.default_operator_kind` |
///
/// Consumed by `Engine::resolve_operator_info` (`crate::core::engine`), which
/// supplies `runtime_agent` / `bp_agent` from per-agent `HashMap` lookups on
/// `OperatorSession`, and `runtime_global` / `bp_global` from session-level
/// fields — `runtime_global` is `OperatorSession.operator_kind` verbatim
/// (an `Option<OperatorKind>`; `Some(_)` is always an explicit request,
/// including `Some(Automate)`, and `None` means unspecified).
pub fn collapse_operator_kind(
    runtime_agent: Option<OperatorKind>,
    runtime_global: Option<OperatorKind>,
    bp_agent: Option<OperatorKind>,
    bp_global: Option<OperatorKind>,
) -> OperatorKind {
    runtime_agent
        .or(runtime_global)
        .or(bp_agent)
        .or(bp_global)
        .unwrap_or_default()
}

#[cfg(test)]
mod collapse_operator_kind_tests {
    use super::*;

    // (i) All tiers None → Default Fallback (Automate).
    #[test]
    fn all_none_falls_back_to_automate() {
        assert_eq!(
            collapse_operator_kind(None, None, None, None),
            OperatorKind::Automate
        );
    }

    // (ii) BP Global alone → BP Global value.
    #[test]
    fn bp_global_only_wins() {
        assert_eq!(
            collapse_operator_kind(None, None, None, Some(OperatorKind::MainAi)),
            OperatorKind::MainAi
        );
    }

    // (iii) BP Agent alone → BP Agent value.
    #[test]
    fn bp_agent_only_wins() {
        assert_eq!(
            collapse_operator_kind(None, None, Some(OperatorKind::MainAi), None),
            OperatorKind::MainAi
        );
    }

    // (iv) Runtime Global alone → Runtime Global value.
    #[test]
    fn runtime_global_only_wins() {
        assert_eq!(
            collapse_operator_kind(None, Some(OperatorKind::MainAi), None, None),
            OperatorKind::MainAi
        );
    }

    // (v) Runtime Agent alone → Runtime Agent value.
    #[test]
    fn runtime_agent_only_wins() {
        assert_eq!(
            collapse_operator_kind(Some(OperatorKind::MainAi), None, None, None),
            OperatorKind::MainAi
        );
    }

    // (vi) All tiers set → Runtime Agent value (narrow-wins check).
    #[test]
    fn all_tiers_set_runtime_agent_wins() {
        assert_eq!(
            collapse_operator_kind(
                Some(OperatorKind::MainAi),
                Some(OperatorKind::Composite),
                Some(OperatorKind::Automate),
                Some(OperatorKind::Composite),
            ),
            OperatorKind::MainAi
        );
    }

    // (vii) BP Agent + Runtime Global together → Runtime Global (later-wins check).
    #[test]
    fn runtime_global_beats_bp_agent() {
        assert_eq!(
            collapse_operator_kind(
                None,
                Some(OperatorKind::Composite),
                Some(OperatorKind::MainAi),
                None,
            ),
            OperatorKind::Composite
        );
    }

    // null merge: Runtime Agent-level unset for this agent but BP Agent set,
    // BP Global also set → BP Agent (narrower) wins over BP Global.
    #[test]
    fn bp_agent_beats_bp_global_when_runtime_tiers_absent() {
        assert_eq!(
            collapse_operator_kind(
                None,
                None,
                Some(OperatorKind::MainAi),
                Some(OperatorKind::Composite),
            ),
            OperatorKind::MainAi
        );
    }

    #[test]
    fn schema_operator_kind_converts_into_ctx_operator_kind() {
        assert_eq!(
            OperatorKind::from(mlua_swarm_schema::OperatorKind::MainAi),
            OperatorKind::MainAi
        );
        assert_eq!(
            OperatorKind::from(mlua_swarm_schema::OperatorKind::Automate),
            OperatorKind::Automate
        );
        assert_eq!(
            OperatorKind::from(mlua_swarm_schema::OperatorKind::Composite),
            OperatorKind::Composite
        );
    }
}

/// The bundle of Operator faces the engine injects into `Ctx` at dispatch.
///
/// # The three `Arc<dyn ...>` fields — the three Operator faces
///
/// Conceptually the Operator is one role, but inside the engine it fans out
/// into three interception axes that fire independently. The canonical use
/// is one external Operator (say, a WebSocket client) that implements all
/// three traits and answers every axis from a single session (see
/// a WebSocket-backed operator session in the server crate).
///
/// | field | trait | firing layer | purpose |
/// |---|---|---|---|
/// | `senior_bridge` | [`SeniorBridge`] | `SeniorEscalationMiddleware` | When a worker returns `ok = false`, query a judgment source and upgrade the outcome to Pass. |
/// | `spawn_hook` | [`SpawnHook`] | `MainAIMiddleware` | Pre- and post-spawn observation and approve/reject gating (`kind = MainAi` / `Composite` only). |
/// | `operator` | [`crate::operator::Operator`] | `OperatorDelegateMiddleware` | Delegate the entire spawn to an external Operator (bypass `inner.spawn` and call `execute`; `kind = MainAi` / `Composite` only). |
///
/// # The role of `kind`
///
/// Middleware uses `OperatorKind` (`Automate` / `MainAi` / `Composite`) as a
/// gating signal: `MainAi` / `Composite` enable `spawn_hook` and `operator`;
/// `Automate` lets middleware pass through into a normal spawn.
/// `senior_bridge` is kind-agnostic and fires whenever `ok = false`.
///
/// # Default
///
/// `OperatorKind::Automate` with all three `Arc<dyn ...>` fields set to
/// `None`. Middleware passes through; execution stays inline as usual.
///
/// # Persistence boundary
///
/// `OperatorInfo` is transient inside `Ctx` (`#[serde(skip)]`). The
/// persisted `OperatorSession` only holds IDs (`bridge_id` / `hook_id` /
/// `operator_backend_id`). At dispatch time the engine resolves each `Arc`
/// by looking those IDs up in its `senior_bridges` / `spawn_hooks` /
/// `operators` `HashMap`s via `resolve_operator_info(session) -> OperatorInfo`.
#[derive(Clone)]
pub struct OperatorInfo {
    /// Gating signal consumed by middleware; see the "role of `kind`"
    /// section above.
    pub kind: OperatorKind,
    /// Identifier of the attached Operator/session (`OperatorSession.operator_id`).
    pub id: String,
    /// See the `senior_bridge` row in the table above.
    pub senior_bridge: Option<Arc<dyn SeniorBridge>>,
    /// See the `spawn_hook` row in the table above.
    pub spawn_hook: Option<Arc<dyn SpawnHook>>,
    /// See the `operator` row in the table above.
    pub operator: Option<Arc<dyn crate::operator::Operator>>,
}

impl std::fmt::Debug for OperatorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperatorInfo")
            .field("kind", &self.kind)
            .field("id", &self.id)
            .field("senior_bridge", &self.senior_bridge.is_some())
            .field("spawn_hook", &self.spawn_hook.is_some())
            .field("operator", &self.operator.is_some())
            .finish()
    }
}

impl Default for OperatorInfo {
    fn default() -> Self {
        Self {
            kind: OperatorKind::Automate,
            id: "default-automate".into(),
            senior_bridge: None,
            spawn_hook: None,
            operator: None,
        }
    }
}

/// Escalation channel fired by `SeniorEscalationMiddleware` whenever a
/// worker returns `ok = false`: a chance for a "senior" judgment source to
/// review and potentially upgrade the outcome to Pass.
#[async_trait]
pub trait SeniorBridge: Send + Sync {
    /// Ask the Senior a question and wait for the answer (`Value`). The
    /// implementation is free — a CLI prompt, an MCP modal, another
    /// process, whatever.
    async fn ask(&self, task_id: &TaskId, question: Value) -> Result<Value, String>;
}

/// Pre-/post-spawn observation and gating hook fired by
/// `MainAIMiddleware` (only when `OperatorKind` is `MainAi` / `Composite`).
#[async_trait]
pub trait SpawnHook: Send + Sync {
    /// Hook fired **before** the spawn. Returning `Err` aborts the spawn.
    async fn before(&self, ctx: &Ctx) -> Result<(), String>;
    /// Hook fired **after** the spawn (once the worker has finished).
    async fn after(&self, ctx: &Ctx, result: &Value) -> Result<(), String>;
}
