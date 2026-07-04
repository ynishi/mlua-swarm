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
use crate::types::CapToken;
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
/// Constructed exclusively via `with_spawner`: each dispatch goes through
/// `engine.dispatch_attempt_with(token, tid, spawner)`, carrying the
/// spawner per request. Nothing is stashed on engine-global state, so
/// multiple dispatchers can drive different Blueprints against the same
/// `Engine` in parallel without racing.
pub struct EngineDispatcher {
    engine: Engine,
    op_token: CapToken,
    spawner: Arc<dyn SpawnerAdapter>,
}

impl EngineDispatcher {
    /// The sole constructor: the spawner is carried per-dispatcher.
    pub fn with_spawner(
        engine: Engine,
        op_token: CapToken,
        spawner: Arc<dyn SpawnerAdapter>,
    ) -> Self {
        Self {
            engine,
            op_token,
            spawner,
        }
    }
}

#[async_trait]
impl AsyncDispatcher for EngineDispatcher {
    async fn dispatch(&self, ref_: &str, input: Value) -> Result<Value, EvalError> {
        // Turn the evaluated Step.in value into a directive. Strings pass
        // through verbatim; anything else is serde-stringified (the worker
        // is expected to re-parse it).
        let directive = match &input {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };

        let tid = self
            .engine
            .start_task(
                &self.op_token,
                TaskSpec {
                    agent: ref_.to_string(),
                    initial_directive: directive,
                },
            )
            .await
            .map_err(|e| EvalError::DispatcherError {
                ref_: ref_.to_string(),
                msg: format!("start_task: {e}"),
            })?;

        let outcome = self
            .engine
            .dispatch_attempt_with(&self.op_token, &tid, &self.spawner)
            .await;
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
