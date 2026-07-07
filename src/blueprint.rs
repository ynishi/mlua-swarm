//! Blueprint runner — glue that executes a flow.ir AST
//! (`mlua_flow_ir::Node`) through the engine. Each `Step.ref` is run as a
//! single task via `start_task` + `dispatch_attempt_with`, and the
//! resulting `Pass` `Value` is written back to `Step.out`.
//!
//! **Fully-async chain.** Uses `mlua_flow_ir::eval_async` and
//! `AsyncDispatcher`; `block_on` and `spawn_blocking` are never mixed in,
//! so the whole stack stays consistent with the engine's tokio async
//! world.
//!
//! # Usage
//!
//! ```ignore
//! let dispatcher = EngineDispatcher::with_spawner(engine.clone(), op_token, spawner);
//! let bp: mlua_flow_ir::Node = serde_json::from_str(BP_JSON)?;
//! let final_ctx = mlua_flow_ir::eval_async(&bp, init_ctx, &dispatcher).await?;
//! ```
//!
//! # Schema types (the IF crate)
//!
//! `Blueprint` / `AgentDef` / `AgentKind` and friends live in the
//! `mlua_swarm_schema` crate and are re-exported from here.
//! The `struct`/`enum` set that used to live directly in `src/blueprint.rs`
//! has been moved into the IF crate to support extension discipline,
//! versioning, and external consumers.

use crate::core::engine::Engine;
use crate::core::state::{DispatchOutcome, TaskSpec};
use crate::store::run::{RunContext, StepEntry};
use crate::types::{now_unix, CapToken};
use crate::worker::adapter::SpawnerAdapter;
use async_trait::async_trait;
pub mod compiler;
pub mod loader;
pub mod store;

use mlua_flow_ir::{AsyncDispatcher, EvalError};
use serde_json::Value;
use std::sync::Arc;

// The schema types are owned by the IF crate (mlua-swarm-schema); we re-export them here.
/// The schema-side `OperatorKind` (see `crate::core::ctx::OperatorKind` for the
/// runtime duplicate consumed by `Engine`). Re-exported under an explicit
/// alias so callers reading `Blueprint.operators[].kind` /
/// `Blueprint.default_operator_kind` do not have to reach into
/// `mlua_swarm_schema` directly.
pub use mlua_swarm_schema::OperatorKind as SchemaOperatorKind;
pub use mlua_swarm_schema::{
    current_schema_version, default_global_agent_kind, AgentDef, AgentKind, AgentMeta,
    AgentProfile, Blueprint, BlueprintMetadata, BlueprintOrigin, CompilerHints, CompilerStrategy,
    OperatorDef, SpawnerHints, CURRENT_SCHEMA_VERSION,
};

/// Bridges `mlua_flow_ir::AsyncDispatcher` to the engine's
/// `start_task` + `dispatch_attempt_with` pair. Holds one Operator session
/// token and one `spawner`, and spins up a fresh task per `Step.ref`, using
/// it as the agent name.
///
/// Constructed via `with_spawner`; each dispatch goes through
/// `engine.dispatch_attempt_with(token, tid, spawner, run_id)`, carrying the
/// spawner per request. Nothing is stashed on engine-global state, so
/// multiple dispatchers can drive different Blueprints against the same
/// `Engine` in parallel without racing.
///
/// Optionally carries a [`RunContext`] (via [`Self::with_run`], issue #13
/// run_id propagation): when present, every dispatched step's `run_id` is
/// exposed to the worker through `Ctx.meta.runtime["run_id"]`, and a
/// [`StepEntry`] is appended to `RunRecord.step_entries` once the step's
/// outcome is known (dispatch is synchronous end-to-end here, so there is
/// no need for a separate event/notification mechanism — the entry is
/// written with its final status in one call).
pub struct EngineDispatcher {
    engine: Engine,
    op_token: CapToken,
    spawner: Arc<dyn SpawnerAdapter>,
    run_ctx: Option<RunContext>,
}

impl EngineDispatcher {
    /// Build a dispatcher with no run-level tracing (`run_ctx = None`) —
    /// the pre-existing behavior. Use [`Self::with_run`] to opt into
    /// `RunRecord.step_entries` tracing / `ctx.meta.runtime["run_id"]`.
    pub fn with_spawner(
        engine: Engine,
        op_token: CapToken,
        spawner: Arc<dyn SpawnerAdapter>,
    ) -> Self {
        Self {
            engine,
            op_token,
            spawner,
            run_ctx: None,
        }
    }

    /// Attach a [`RunContext`] (builder style) so every dispatched step is
    /// traced into `RunRecord.step_entries` and exposes its `run_id` via
    /// `Ctx.meta.runtime`.
    pub fn with_run(mut self, run_ctx: RunContext) -> Self {
        self.run_ctx = Some(run_ctx);
        self
    }
}

#[async_trait]
impl AsyncDispatcher for EngineDispatcher {
    async fn dispatch(&self, ref_: &str, input: Value) -> Result<Value, EvalError> {
        // issue #18: the evaluated Step.in value passes straight through
        // as `TaskSpec.initial_directive` — no premature `Value → String`
        // coercion here. Consumers that need a rendered `String` do so at
        // their own late boundary: `Engine::start_task` /
        // `Engine::dispatch_attempt_with` render it into the
        // `EngineState.prompts` table for the Worker HTTP path
        // (`/v1/worker/prompt`), and
        // `operator_ws::session::default_spawn_directive_with_task_directive`
        // renders it into the WS `Spawn.directive` reminder text.
        let tid = self
            .engine
            .start_task(
                &self.op_token,
                TaskSpec {
                    agent: ref_.to_string(),
                    initial_directive: input,
                },
            )
            .await
            .map_err(|e| EvalError::DispatcherError {
                ref_: ref_.to_string(),
                msg: format!("start_task: {e}"),
            })?;

        let run_id_for_ctx = self.run_ctx.as_ref().map(|rc| rc.run_id.clone());
        let outcome = self
            .engine
            .dispatch_attempt_with(&self.op_token, &tid, &self.spawner, run_id_for_ctx.as_ref())
            .await;

        // issue #13 run_id propagation: append one step_entry per dispatched
        // step (`RunStore.append_step_entry` is append-only — there is no
        // in-place update — so the entry is written once here, after the
        // outcome is known, carrying its final status). Secondary
        // persistence failures are logged and swallowed, matching
        // `mse-server`'s `finalize_run` convention: they must not mask the
        // primary dispatch outcome the flow eval already has in hand.
        if let Some(rc) = &self.run_ctx {
            let status = match &outcome {
                Ok(DispatchOutcome::Pass(_)) => "passed",
                Ok(DispatchOutcome::Blocked(_)) => "blocked",
                Ok(DispatchOutcome::Suspended(_)) => "suspended",
                Ok(DispatchOutcome::Cancelled) => "cancelled",
                Ok(DispatchOutcome::Timeout) => "timeout",
                Err(_) => "failed",
            };
            let entry = StepEntry {
                step_id: tid.clone(),
                step_ref: Some(ref_.to_string()),
                status: Some(status.to_string()),
                at: now_unix(),
            };
            if let Err(e) = rc.run_store.append_step_entry(&rc.run_id, entry).await {
                tracing::warn!(
                    run_id = %rc.run_id,
                    step_id = %tid,
                    error = %e,
                    "EngineDispatcher::dispatch: append_step_entry failed"
                );
            }
        }

        match outcome {
            Ok(DispatchOutcome::Pass(v)) => Ok(v),
            Ok(DispatchOutcome::Blocked(v)) => Err(EvalError::DispatcherError {
                ref_: ref_.to_string(),
                msg: format!("blocked: {v}"),
            }),
            Ok(other) => Err(EvalError::DispatcherError {
                ref_: ref_.to_string(),
                msg: format!("non-terminal outcome: {:?}", other),
            }),
            Err(e) => Err(EvalError::DispatcherError {
                ref_: ref_.to_string(),
                msg: format!("dispatch_attempt: {e}"),
            }),
        }
    }
}
