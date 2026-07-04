//! `TaskLaunchService` — the domain service that runs a Blueprint flow
//! to completion through the engine.
//!
//! Responsibilities:
//! 1. Compile the Blueprint and link it into a `SpawnerAdapter` (via
//!    `service::linker::link`, wrapped by `EngineDispatcher::with_spawner`).
//! 2. Acquire an Operator session (via `engine.attach`).
//! 3. Run flow.ir's `eval_async` through an `EngineDispatcher` and
//!    return the final `ctx`.
//! 4. If any step fails (dispatcher error), `eval_async` errors and
//!    the failure propagates as-is.
//!
//! Callers on the Application layer never touch the engine directly —
//! `bind`, `start_task`, and `eval_async` all stay inside the Service.
//!
//! A single-task-spawn API (calling `start_task` directly) is
//! deliberately absent here: a single spawn can be modeled as a
//! one-Step flow, and we do not want two interfaces for the same
//! shape.

use crate::blueprint::compiler::{CompileError, Compiler};
use crate::blueprint::{Blueprint, EngineDispatcher};
use crate::core::ctx::OperatorKind;
use crate::core::engine::Engine;
use crate::core::errors::EngineError;
use crate::middleware::project_name_alias::ProjectNameAliasMiddleware;
use crate::middleware::SpawnerStack;
use crate::service::linker;
use crate::types::{CapToken, Role};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;

/// Derive the "BP Agent-level" tier of the `OperatorKind` cascade from a
/// Blueprint: for every `AgentDef` whose `spec.operator_ref` resolves to an
/// `OperatorDef` with a `Some` `kind`, map `AgentDef.name -> OperatorKind`.
///
/// Deliberately **not** filtered by `AgentDef.kind == AgentKind::Operator`:
/// the `OperatorKind` cascade is a middleware-level cross-cutting concern
/// (spawn_hook / senior_bridge / operator-delegate gating via `Ctx.operator`),
/// orthogonal to the Worker IMPL axis that `AgentKind` expresses (see the
/// crate root doc, "Operator is delivered as a cross-cutting overlay through
/// `Ctx` plus middleware"). A `RustFn` / `Lua` / `Subprocess` agent can
/// equally declare `spec.operator_ref` to opt into a BP-declared
/// `OperatorKind` without changing its Worker IMPL. Agents without an
/// `operator_ref`, an unresolved `operator_ref`, or an `OperatorDef.kind =
/// None` are simply absent from the map (= that tier falls through for
/// them). This is a separate, independent consumer of `Blueprint.operators`
/// from the design-time `operator_ref` validation in
/// `blueprint::compiler::Compiler::compile` (issue: `OperatorDef`
/// first-class treatment), which only checks the reference resolves for
/// `AgentKind::Operator` agents and is unaffected by this function.
fn derive_bp_agent_kinds(blueprint: &Blueprint) -> HashMap<String, OperatorKind> {
    let mut out = HashMap::new();
    if blueprint.operators.is_empty() {
        return out;
    }
    for agent in &blueprint.agents {
        let Some(op_ref) = agent.spec.get("operator_ref").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(op_def) = blueprint.operators.iter().find(|o| o.name == op_ref) else {
            continue;
        };
        if let Some(kind) = op_def.kind {
            out.insert(agent.name.clone(), OperatorKind::from(kind));
        }
    }
    out
}

/// Failure modes of [`TaskLaunchService::launch`].
#[derive(Debug, Error)]
pub enum TaskLaunchError {
    /// `Compiler::compile` rejected the Blueprint.
    #[error("compile: {0}")]
    Compile(#[from] CompileError),
    /// `Engine::attach_with_ids` failed.
    #[error("engine: {0}")]
    Engine(#[from] EngineError),
    /// A `Step` inside `flow.ir`'s `eval_async` produced a dispatcher
    /// error, or a sub-flow raised.
    #[error("flow eval: {0}")]
    FlowEval(String),
}

/// Input to [`TaskLaunchService::launch`].
#[derive(Debug, Clone)]
pub struct TaskLaunchInput {
    /// The Blueprint to compile, link, and run.
    pub blueprint: Blueprint,
    /// Caller-supplied id for the Operator that owns this run.
    pub operator_id: String,
    /// The Operator's role for this run.
    pub role: Role,
    /// How long the attached session is allowed to live.
    pub ttl: Duration,
    /// "Runtime Global" tier of the `OperatorKind` cascade. `Some(_)` is
    /// always an explicit request — including `Some(OperatorKind::Automate)`
    /// — that outranks the BP-level tiers (`OperatorDef.kind` /
    /// `Blueprint.default_operator_kind`); `None` leaves it unspecified so
    /// those tiers / the final default decide. Under `MainAi` or
    /// `Composite`, `MainAIMiddleware`'s `spawn_hook` before/after
    /// callbacks become effective. See
    /// `crate::core::ctx::collapse_operator_kind`.
    pub operator_kind: Option<OperatorKind>,
    /// `SeniorBridge` registry ID. `None` — no bridge; `Some(id)` —
    /// attach a bridge previously registered via
    /// `engine.register_senior_bridge`.
    pub bridge_id: Option<String>,
    /// `SpawnHook` registry ID. Same shape as above, via
    /// `engine.register_spawn_hook`.
    pub hook_id: Option<String>,
    /// Operator registry ID — used on the path that hands the whole
    /// spawn off to an external Operator. Name previously registered
    /// with `engine.register_operator`; resolved by
    /// `OperatorDelegateMiddleware`, which — for `kind = MainAi` or
    /// `Composite` — bypasses `inner.spawn` and calls
    /// `operator.execute`.
    pub operator_backend_id: Option<String>,
    /// "Runtime Agent-level" tier (highest priority) of the `OperatorKind`
    /// cascade — per-agent override, keyed by `AgentDef.name`. Empty by
    /// default (no override for any agent). See
    /// `crate::core::ctx::collapse_operator_kind` for the full tier list.
    pub operator_kind_overrides: HashMap<String, OperatorKind>,
    /// The initial `ctx` (JSON `Value`) that flow.ir's `eval_async`
    /// starts from. Every `Step.in` `$.<path>` reference reads from
    /// here.
    pub init_ctx: Value,
}

impl TaskLaunchInput {
    /// Helper for existing callers on the default path — no hooks and no
    /// per-agent `OperatorKind` overrides. Leaves the "Runtime Global" tier
    /// unspecified (`None`), so the BP-level tiers / final default
    /// (`OperatorKind::Automate`) decide — this preserves today's
    /// behaviour for every existing caller without silently forcing
    /// `Automate` as an explicit override that would outrank a BP-declared
    /// `MainAi`/`Composite` kind.
    pub fn automate(
        blueprint: Blueprint,
        operator_id: impl Into<String>,
        role: Role,
        ttl: Duration,
        init_ctx: Value,
    ) -> Self {
        Self {
            blueprint,
            operator_id: operator_id.into(),
            role,
            ttl,
            operator_kind: None,
            bridge_id: None,
            hook_id: None,
            operator_backend_id: None,
            operator_kind_overrides: HashMap::new(),
            init_ctx,
        }
    }
}

/// Result of a successful [`TaskLaunchService::launch`] call.
#[derive(Debug, Clone)]
pub struct TaskLaunchOutput {
    /// The capability token for the attached session.
    pub token: CapToken,
    /// The final `ctx` after the flow ran — every `Step.out` has
    /// been written. Application-layer callers pull the outcome out
    /// of this `Value` and fold it into a domain status.
    pub final_ctx: Value,
}

/// Domain service that compiles, links, and runs a Blueprint's flow to
/// completion through the [`Engine`]. See the module doc for the full
/// responsibility list.
pub struct TaskLaunchService {
    engine: Engine,
    compiler: Compiler,
}

impl TaskLaunchService {
    /// Build a service bound to one `Engine` and one `Compiler`.
    pub fn new(engine: Engine, compiler: Compiler) -> Self {
        Self { engine, compiler }
    }

    /// The bound `Engine`.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The bound `Compiler`.
    pub fn compiler(&self) -> &Compiler {
        &self.compiler
    }

    /// Run the Blueprint's flow to completion and return the final
    /// `ctx`.
    ///
    /// Failure paths:
    ///
    /// - `compiler.compile` failure → `TaskLaunchError::Compile`.
    /// - `engine.attach` failure → `TaskLaunchError::Engine`.
    /// - A `Step` inside `flow eval` producing a dispatcher error, or
    ///   a sub-flow raising, → `TaskLaunchError::FlowEval`. There is
    ///   no silent partial-success completion; failures always
    ///   propagate.
    pub async fn launch(
        &self,
        input: TaskLaunchInput,
    ) -> Result<TaskLaunchOutput, TaskLaunchError> {
        // After the stateless-executor refactor, the
        // caller (Service) does compile + link +
        // `EngineDispatcher::with_spawner` itself; the engine no longer
        // holds any global spawner state to touch. The link path (base
        // `SpawnerAdapter` +
        // `LayerRegistry` resolution + `SpawnerStack` wrapping) is
        // concentrated inside `service::linker::link` — Service
        // scatter is intentionally prevented.
        let compiled = self.compiler.compile(&input.blueprint)?;
        let spawner = linker::link(
            compiled.router.clone(),
            &input.blueprint.spawner_hints.layers,
            &self.engine,
        );
        // When `Blueprint.metadata.project_name_alias` is Some, layer a
        // `ProjectNameAliasMiddleware` on top of the stack that injects the
        // alias into `Ctx.meta.runtime.project_name_alias` just before spawn.
        // Downstream operators (for example, the server crate's
        // `Operator.execute`) read `ctx.meta.runtime.get("project_name_alias")`
        // and expand it into the Spawn directive prompt body.
        let spawner = if let Some(alias) = input.blueprint.metadata.project_name_alias.as_deref() {
            SpawnerStack::new(spawner)
                .layer(ProjectNameAliasMiddleware::new(alias))
                .build()
        } else {
            spawner
        };

        // "BP Agent-level" (`OperatorDef.kind` via `operator_ref`) + "BP
        // Global" (`Blueprint.default_operator_kind`) tiers of the
        // `OperatorKind` cascade, baked here (the only point that has both
        // the resolved Blueprint and the launch-time overrides in scope).
        let bp_agent_kinds = derive_bp_agent_kinds(&input.blueprint);
        let bp_global_kind = input
            .blueprint
            .default_operator_kind
            .map(OperatorKind::from);

        let token = self
            .engine
            .attach_with_ids(
                input.operator_id,
                input.role,
                input.ttl,
                input.operator_kind,
                input.bridge_id,
                input.hook_id,
                input.operator_backend_id,
                input.operator_kind_overrides,
                bp_agent_kinds,
                bp_global_kind,
            )
            .await?;
        let dispatcher =
            EngineDispatcher::with_spawner(self.engine.clone(), token.clone(), spawner);
        let final_ctx =
            mlua_flow_ir::eval_async(&input.blueprint.flow, input.init_ctx, &dispatcher)
                .await
                .map_err(|e| TaskLaunchError::FlowEval(e.to_string()))?;
        Ok(TaskLaunchOutput { token, final_ctx })
    }
}

// ──────────────────────────────────────────────────────────────────────────
// UT
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blueprint::compiler::{RustFnInProcessSpawnerFactory, SpawnerRegistry};
    use crate::blueprint::{
        current_schema_version, AgentDef, AgentKind, AgentMeta, BlueprintMetadata, CompilerHints,
        CompilerStrategy,
    };
    use crate::core::config::EngineCfg;
    use crate::worker::adapter::{WorkerError, WorkerResult};
    use mlua_flow_ir::{Expr, JoinMode, Node as FlowNode};
    use serde_json::json;
    use std::sync::Arc;

    fn path(s: &str) -> Expr {
        Expr::Path { at: s.to_string() }
    }
    fn step(ref_: &str, in_: Expr, out: Expr) -> FlowNode {
        FlowNode::Step {
            ref_: ref_.to_string(),
            in_,
            out,
        }
    }

    fn agent(name: &str, fn_id: &str) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::RustFn,
            spec: json!({ "fn_id": fn_id }),
            profile: None,
            meta: Some(AgentMeta::default()),
        }
    }

    fn build_service(factory: RustFnInProcessSpawnerFactory) -> TaskLaunchService {
        let engine = Engine::new(EngineCfg::default());
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));
        let compiler = Compiler::new(reg);
        TaskLaunchService::new(engine, compiler)
    }

    fn bp(flow: FlowNode, agents: Vec<AgentDef>) -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "ut".into(),
            flow,
            agents,
            operators: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
        }
    }

    fn launch_input(blueprint: Blueprint, init_ctx: Value) -> TaskLaunchInput {
        TaskLaunchInput::automate(
            blueprint,
            "ut-op",
            Role::Operator,
            Duration::from_secs(30),
            init_ctx,
        )
    }

    #[tokio::test]
    async fn launch_single_step_writes_out_path() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!({ "echoed": inv.prompt }),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        let out = svc
            .launch(launch_input(blueprint, json!({ "input": "hi" })))
            .await
            .expect("launch ok");
        assert_eq!(out.final_ctx["out"]["echoed"], "hi");
    }

    #[tokio::test]
    async fn launch_three_step_seq_threads_ctx_forward() {
        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("upper", |inv| async move {
                let s = serde_json::from_str::<String>(&inv.prompt).unwrap_or(inv.prompt);
                Ok(WorkerResult {
                    value: json!(s.to_uppercase()),
                    ok: true,
                })
            })
            .register_fn("suffix", |inv| async move {
                let s = serde_json::from_str::<String>(&inv.prompt).unwrap_or(inv.prompt);
                Ok(WorkerResult {
                    value: json!(format!("{s}!")),
                    ok: true,
                })
            })
            .register_fn("wrap", |inv| async move {
                let s = serde_json::from_str::<String>(&inv.prompt).unwrap_or(inv.prompt);
                Ok(WorkerResult {
                    value: json!(format!("[{s}]")),
                    ok: true,
                })
            });
        let svc = build_service(factory);
        let flow = FlowNode::Seq {
            children: vec![
                step("upper", path("$.in"), path("$.s1")),
                step("suffix", path("$.s1"), path("$.s2")),
                step("wrap", path("$.s2"), path("$.s3")),
            ],
        };
        let blueprint = bp(
            flow,
            vec![
                agent("upper", "upper"),
                agent("suffix", "suffix"),
                agent("wrap", "wrap"),
            ],
        );
        let out = svc
            .launch(launch_input(blueprint, json!({ "in": "hello" })))
            .await
            .expect("launch ok");
        assert_eq!(out.final_ctx["s1"], "HELLO");
        assert_eq!(out.final_ctx["s2"], "HELLO!");
        assert_eq!(out.final_ctx["s3"], "[HELLO!]");
    }

    #[tokio::test]
    async fn launch_fanout_join_all_parallel_completes() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let counter = Arc::new(AtomicU32::new(0));
        let max_seen = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();
        let max_clone = max_seen.clone();

        // Each worker bumps the inflight counter up, sleeps 50ms, then bumps it down.
        // When parallel execution is working, max inflight exceeds 1.
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("para", move |inv| {
            let counter = counter_clone.clone();
            let max_seen = max_clone.clone();
            async move {
                let now = counter.fetch_add(1, Ordering::SeqCst) + 1;
                let mut prev = max_seen.load(Ordering::SeqCst);
                while now > prev {
                    match max_seen.compare_exchange(prev, now, Ordering::SeqCst, Ordering::SeqCst) {
                        Ok(_) => break,
                        Err(p) => prev = p,
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                counter.fetch_sub(1, Ordering::SeqCst);
                let s = serde_json::from_str::<String>(&inv.prompt).unwrap_or(inv.prompt);
                Ok(WorkerResult {
                    value: json!(format!("did:{s}")),
                    ok: true,
                })
            }
        });
        let svc = build_service(factory);
        let flow = FlowNode::Fanout {
            items: path("$.items"),
            bind: path("$.item"),
            body: Box::new(step("para", path("$.item"), path("$.r"))),
            join: JoinMode::All,
            out: path("$.results"),
        };
        let blueprint = bp(flow, vec![agent("para", "para")]);
        let out = svc
            .launch(launch_input(
                blueprint,
                json!({ "items": ["a", "b", "c", "d"] }),
            ))
            .await
            .expect("launch ok");
        let results = out.final_ctx["results"].as_array().expect("array");
        assert_eq!(results.len(), 4);
        for (i, expected) in ["a", "b", "c", "d"].iter().enumerate() {
            assert_eq!(results[i]["r"], json!(format!("did:{expected}")));
        }
        let max = max_seen.load(Ordering::SeqCst);
        assert!(
            max >= 2,
            "expected parallel execution (max inflight >= 2), got {max}"
        );
    }

    #[tokio::test]
    async fn launch_propagates_worker_error_as_flow_eval_err() {
        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("ok", |inv| async move {
                Ok(WorkerResult {
                    value: json!(inv.prompt),
                    ok: true,
                })
            })
            .register_fn("boom", |_inv| async move {
                Err(WorkerError::Failed("intentional boom".into()))
            });
        let svc = build_service(factory);
        let flow = FlowNode::Seq {
            children: vec![
                step("ok", path("$.input"), path("$.s1")),
                step("boom", path("$.s1"), path("$.s2")),
                step("ok", path("$.s2"), path("$.s3")),
            ],
        };
        let blueprint = bp(flow, vec![agent("ok", "ok"), agent("boom", "boom")]);
        let err = svc
            .launch(launch_input(blueprint, json!({ "input": "x" })))
            .await
            .expect_err("expected fail");
        match err {
            TaskLaunchError::FlowEval(msg) => {
                assert!(
                    msg.contains("boom") || msg.contains("intentional"),
                    "expected error to mention worker failure, got: {msg}"
                );
            }
            other => panic!("expected FlowEval error, got {other:?}"),
        }
    }
}
