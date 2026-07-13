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
use crate::core::projection_placement::ProjectionPlacement;
use crate::core::state::{DispatchOutcome, TaskSpec};
use crate::core::step_naming::StepNaming;
use crate::store::run::{RunContext, StepEntry};
use crate::types::{now_unix, CapToken};
use crate::worker::adapter::SpawnerAdapter;
use async_trait::async_trait;
pub mod compiler;
pub mod loader;
pub mod store;

use mlua_flow_ir::{AsyncDispatcher, EvalError};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

// The schema types are owned by the IF crate (mlua-swarm-schema); we re-export them here.
/// The schema-side `OperatorKind` (see `crate::core::ctx::OperatorKind` for the
/// runtime duplicate consumed by `Engine`). Re-exported under an explicit
/// alias so callers reading `Blueprint.operators[].kind` /
/// `Blueprint.default_operator_kind` do not have to reach into
/// `mlua_swarm_schema` directly.
pub use mlua_swarm_schema::OperatorKind as SchemaOperatorKind;
pub use mlua_swarm_schema::{
    current_schema_version, default_global_agent_kind, resolve_runner, AgentDef, AgentKind,
    AgentMeta, AgentProfile, AuditDef, AuditMode, Blueprint, BlueprintMetadata, BlueprintOrigin,
    CompilerHints, CompilerStrategy, MetaDef, OperatorDef, ProjectionPlacementSpec, Runner,
    RunnerDef, RunnerResolveError, SpawnerHints, WorkerModel, CURRENT_SCHEMA_VERSION,
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
///
/// Also carries the GH #21 Phase 2 named `MetaDef` pool (via
/// [`Self::with_step_metas`]) — the Step tier's dispatch-time resolver;
/// see [`Self::dispatch`]'s doc for the full envelope contract.
///
/// GH #23: optionally carries the Blueprint's [`StepNaming`] table (via
/// [`Self::with_step_naming`], built once by
/// `blueprint::compiler::Compiler::compile` — see that type's doc for the
/// full addressing-space narrative). When present, [`Self::dispatch`]
/// snapshots the same `Arc` into `EngineState.step_namings` for every
/// dispatched task, keyed by its freshly-minted `StepId` — the storage
/// half of the "construct once, read many" contract; `Engine::step_naming_for`
/// is the read-back accessor later consumers (GH #23 subtask-2/3) pull
/// from.
///
/// GH #27 (follow-up to #23): optionally also carries the Blueprint's
/// [`ProjectionPlacement`] resolver (via [`Self::with_projection_placement`],
/// built once by `Compiler::compile`) — the SAME snapshot-then-read-back
/// contract as [`StepNaming`] above, this time read back via
/// `Engine::projection_placement_for`.
pub struct EngineDispatcher {
    engine: Engine,
    op_token: CapToken,
    spawner: Arc<dyn SpawnerAdapter>,
    run_ctx: Option<RunContext>,
    step_metas: HashMap<String, Value>,
    step_naming: Option<Arc<StepNaming>>,
    projection_placement: Option<Arc<ProjectionPlacement>>,
}

impl EngineDispatcher {
    /// Build a dispatcher with no run-level tracing (`run_ctx = None`),
    /// no named `MetaDef`s (`step_metas` empty), and no [`StepNaming`]
    /// table — the pre-existing behavior. Use [`Self::with_run`] /
    /// [`Self::with_step_metas`] / [`Self::with_step_naming`] to opt into
    /// any of them.
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
            step_metas: HashMap::new(),
            step_naming: None,
            projection_placement: None,
        }
    }

    /// Attach a [`RunContext`] (builder style) so every dispatched step is
    /// traced into `RunRecord.step_entries` and exposes its `run_id` via
    /// `Ctx.meta.runtime`.
    pub fn with_run(mut self, run_ctx: RunContext) -> Self {
        self.run_ctx = Some(run_ctx);
        self
    }

    /// GH #21 Phase 2: attach the named `MetaDef` pool (`Blueprint.metas`,
    /// resolved by `service::task_launch::derive_step_metas` into a
    /// `name -> ctx` map) that [`Self::dispatch`] resolves `$step_meta.ref`
    /// envelopes against. Unconditional to call — an empty map (the
    /// pre-#21-Phase-2 default) makes every `$step_meta.ref` lookup miss
    /// loudly, same as a Blueprint that never declares `Blueprint.metas`.
    pub fn with_step_metas(mut self, step_metas: HashMap<String, Value>) -> Self {
        self.step_metas = step_metas;
        self
    }

    /// GH #23: attach the Blueprint's [`StepNaming`] table (built once by
    /// `blueprint::compiler::Compiler::compile`). `None` (the default via
    /// [`Self::with_spawner`]) preserves pre-GH-#23 behavior byte-for-byte
    /// — [`Self::dispatch`] simply skips the `EngineState.step_namings`
    /// snapshot for every caller that never opts in (e.g. tests that build
    /// an `EngineDispatcher` directly instead of going through
    /// `service::task_launch::TaskLaunchService::launch`).
    pub fn with_step_naming(mut self, step_naming: Arc<StepNaming>) -> Self {
        self.step_naming = Some(step_naming);
        self
    }

    /// GH #27 (follow-up to #23): attach the Blueprint's
    /// [`ProjectionPlacement`] resolver (built once by
    /// `blueprint::compiler::Compiler::compile`). `None` (the default via
    /// [`Self::with_spawner`]) preserves pre-GH-#27 behavior byte-for-byte
    /// — [`Self::dispatch`] simply skips the
    /// `EngineState.projection_placements` snapshot for every caller that
    /// never opts in, mirroring [`Self::with_step_naming`]'s contract.
    pub fn with_projection_placement(
        mut self,
        projection_placement: Arc<ProjectionPlacement>,
    ) -> Self {
        self.projection_placement = Some(projection_placement);
        self
    }
}

/// GH #21 Phase 2: resolve a `$step_meta` envelope embedded in a Step's
/// evaluated `in` value into `(initial_directive, step_ctx)` — the Step
/// tier's dispatch-time entry point, called from [`EngineDispatcher::dispatch`]
/// BEFORE `Engine::start_task` (critical: `start_task` seeds
/// `EngineState.prompts[(tid, 1)]` from `TaskSpec.initial_directive`, so
/// stripping the envelope any later would leak `$step_meta` into the
/// worker prompt AND the WS `Spawn.directive` text).
///
/// Contract:
///
/// - `input` is not a JSON `Object`, or is an `Object` with no
///   `"$step_meta"` key → passthrough unchanged, `step_ctx = None`
///   (pre-#21-Phase-2 Blueprints are byte-identical through this path).
/// - `input` IS an `Object` with a `"$step_meta"` key: the key is always
///   stripped (never reaches the returned directive). Everything past
///   this point is loud — an error names the offending step (`ref_`) and,
///   for an unresolved `ref`, the defined `step_metas` names:
///   - the envelope itself must be an `Object` shaped
///     `{"ref": Option<String>, "inline": Option<Object>}`; any other
///     shape is a malformed-envelope error;
///   - `ref` (when present and non-null) is looked up in `step_metas`; an
///     unknown name is an error (no silent skip). The resolved `MetaDef`
///     ctx must itself be an `Object` (or the lookup is treated as
///     malformed);
///   - `inline` (when present and non-null) must be an `Object`;
///   - the resolved Step-tier ctx = the `ref`-resolved ctx shallow-merged
///     with `inline`, **`inline` wins** key collisions.
/// - Directive rule (applied to the remaining `Object`, after
///   `"$step_meta"` is stripped): if it still contains an `"$in"` key,
///   that value becomes the returned directive (other sibling keys are
///   ignored for the directive — envelope-only input, e.g. one final
///   `$step_meta` key, therefore never becomes an empty directive by
///   accident just because more keys existed alongside it). Otherwise
///   the whole remainder becomes the directive; an empty remainder
///   becomes `Value::String(String::new())`.
fn resolve_step_envelope(
    step_metas: &HashMap<String, Value>,
    ref_: &str,
    input: Value,
) -> Result<(Value, Option<Value>), EvalError> {
    let mut obj = match input {
        Value::Object(obj) => obj,
        other => return Ok((other, None)),
    };
    let Some(envelope) = obj.remove("$step_meta") else {
        return Ok((Value::Object(obj), None));
    };
    let envelope = match envelope {
        Value::Object(map) => map,
        other => {
            return Err(EvalError::DispatcherError {
                ref_: ref_.to_string(),
                msg: format!(
                    "malformed $step_meta envelope for step '{ref_}': expected an object, got {other}"
                ),
            });
        }
    };

    let ref_ctx: Option<Map<String, Value>> = match envelope.get("ref") {
        None | Some(Value::Null) => None,
        Some(Value::String(name)) => {
            let resolved = step_metas.get(name).cloned().ok_or_else(|| {
                EvalError::DispatcherError {
                    ref_: ref_.to_string(),
                    msg: format!(
                        "$step_meta.ref '{name}' (step '{ref_}') is not a defined Blueprint.metas entry (defined: {:?})",
                        step_metas.keys().collect::<Vec<_>>()
                    ),
                }
            })?;
            match resolved {
                Value::Object(map) => Some(map),
                other => {
                    return Err(EvalError::DispatcherError {
                        ref_: ref_.to_string(),
                        msg: format!(
                            "malformed $step_meta: MetaDef '{name}'.ctx must be an object, got {other}"
                        ),
                    });
                }
            }
        }
        Some(other) => {
            return Err(EvalError::DispatcherError {
                ref_: ref_.to_string(),
                msg: format!(
                    "malformed $step_meta.ref (step '{ref_}'): expected a string, got {other}"
                ),
            });
        }
    };

    let inline: Option<Map<String, Value>> = match envelope.get("inline") {
        None | Some(Value::Null) => None,
        Some(Value::Object(map)) => Some(map.clone()),
        Some(other) => {
            return Err(EvalError::DispatcherError {
                ref_: ref_.to_string(),
                msg: format!(
                    "malformed $step_meta.inline (step '{ref_}'): expected an object, got {other}"
                ),
            });
        }
    };

    let step_ctx = match (ref_ctx, inline) {
        (None, None) => None,
        (Some(base), None) => Some(Value::Object(base)),
        (None, Some(inline)) => Some(Value::Object(inline)),
        (Some(mut base), Some(inline)) => {
            for (k, v) in inline {
                base.insert(k, v);
            }
            Some(Value::Object(base))
        }
    };

    // Directive rule — only reached once a `$step_meta` envelope was
    // present in `input`.
    let initial_directive = if let Some(in_value) = obj.remove("$in") {
        in_value
    } else if obj.is_empty() {
        Value::String(String::new())
    } else {
        Value::Object(obj)
    };

    Ok((initial_directive, step_ctx))
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
        //
        // GH #21 Phase 2: BEFORE that pass-through, resolve_step_envelope
        // strips + resolves any `$step_meta` envelope — see its doc for
        // the full contract. Inputs without one flow through unchanged.
        let (initial_directive, step_ctx) = resolve_step_envelope(&self.step_metas, ref_, input)?;
        let tid = self
            .engine
            .start_task(
                &self.op_token,
                TaskSpec {
                    agent: ref_.to_string(),
                    initial_directive,
                    step_ctx,
                },
            )
            .await
            .map_err(|e| EvalError::DispatcherError {
                ref_: ref_.to_string(),
                msg: format!("start_task: {e}"),
            })?;

        // GH #23: snapshot the (already-built, Blueprint-wide) StepNaming
        // table into `EngineState.step_namings` keyed by this dispatch's
        // freshly-minted `tid` — the storage half of the "construct once
        // (`Compiler::compile`), read many (`Engine::step_naming_for`)"
        // contract. `None` (no `with_step_naming` call) is a no-op, same
        // fail-open convention as the `run_ctx` step_entry append below:
        // a secondary-persistence failure here must never mask the
        // primary dispatch outcome.
        if let Some(step_naming) = self.step_naming.clone() {
            let tid_for_naming = tid.clone();
            if let Err(e) = self
                .engine
                .with_state("EngineDispatcher::dispatch.step_naming", move |s| {
                    s.step_namings.insert(tid_for_naming, step_naming);
                })
                .await
            {
                tracing::warn!(
                    task_id = %tid,
                    error = %e,
                    "EngineDispatcher::dispatch: failed to snapshot StepNaming into EngineState"
                );
            }
        }

        // GH #27 (follow-up to #23): same snapshot pattern as StepNaming
        // above — stash the (already-built, Blueprint-wide)
        // ProjectionPlacement resolver into `EngineState.projection_placements`
        // keyed by this dispatch's `tid`. `None` (no
        // `with_projection_placement` call) is a no-op, same fail-open
        // convention as the `step_naming` snapshot: a secondary-persistence
        // failure here must never mask the primary dispatch outcome.
        if let Some(projection_placement) = self.projection_placement.clone() {
            let tid_for_placement = tid.clone();
            if let Err(e) = self
                .engine
                .with_state(
                    "EngineDispatcher::dispatch.projection_placement",
                    move |s| {
                        s.projection_placements
                            .insert(tid_for_placement, projection_placement);
                    },
                )
                .await
            {
                tracing::warn!(
                    task_id = %tid,
                    error = %e,
                    "EngineDispatcher::dispatch: failed to snapshot ProjectionPlacement into EngineState"
                );
            }
        }

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

// ──────────────────────────────────────────────────────────────────────────
// issue #21 Phase 2: `resolve_step_envelope` unit tests + a dispatch-level
// end-to-end leak-proof test
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn metas(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn no_envelope_string_input_passes_through_unchanged() {
        let (directive, step_ctx) =
            resolve_step_envelope(&HashMap::new(), "scout", json!("plain string")).unwrap();
        assert_eq!(directive, json!("plain string"));
        assert_eq!(step_ctx, None);
    }

    #[test]
    fn no_envelope_plain_object_input_passes_through_unchanged() {
        let input = json!({ "foo": "bar" });
        let (directive, step_ctx) =
            resolve_step_envelope(&HashMap::new(), "scout", input.clone()).unwrap();
        assert_eq!(directive, input);
        assert_eq!(step_ctx, None);
    }

    #[test]
    fn envelope_with_only_ref_resolves_that_metadef_ctx() {
        let step_metas = metas(&[("heavy-scan", json!({ "work_dir": "/x" }))]);
        let input = json!({ "$step_meta": { "ref": "heavy-scan" }, "$in": "go" });
        let (directive, step_ctx) = resolve_step_envelope(&step_metas, "scout", input).unwrap();
        assert_eq!(directive, json!("go"));
        assert_eq!(step_ctx, Some(json!({ "work_dir": "/x" })));
    }

    #[test]
    fn envelope_with_only_inline_uses_inline_verbatim() {
        let input = json!({
            "$step_meta": { "inline": { "work_dir": "/inline-only" } },
            "$in": "go"
        });
        let (directive, step_ctx) = resolve_step_envelope(&HashMap::new(), "scout", input).unwrap();
        assert_eq!(directive, json!("go"));
        assert_eq!(step_ctx, Some(json!({ "work_dir": "/inline-only" })));
    }

    #[test]
    fn inline_wins_over_ref_on_key_collision() {
        let step_metas = metas(&[(
            "heavy-scan",
            json!({ "work_dir": "/ref", "extra": "from-ref" }),
        )]);
        let input = json!({
            "$step_meta": {
                "ref": "heavy-scan",
                "inline": { "work_dir": "/inline-wins" }
            },
            "$in": "go"
        });
        let (_, step_ctx) = resolve_step_envelope(&step_metas, "scout", input).unwrap();
        assert_eq!(
            step_ctx,
            Some(json!({ "work_dir": "/inline-wins", "extra": "from-ref" })),
            "inline must win the collided key while ref-only keys survive the merge"
        );
    }

    #[test]
    fn dollar_in_rule_extracts_directive_and_ignores_other_sibling_keys() {
        let input = json!({
            "$step_meta": { "inline": { "k": "v" } },
            "$in": "the real directive",
            "unrelated_sibling": "ignored"
        });
        let (directive, step_ctx) = resolve_step_envelope(&HashMap::new(), "scout", input).unwrap();
        assert_eq!(directive, json!("the real directive"));
        assert_eq!(step_ctx, Some(json!({ "k": "v" })));
    }

    #[test]
    fn no_dollar_in_remainder_becomes_the_directive() {
        let input = json!({
            "$step_meta": { "inline": { "k": "v" } },
            "other_key": "other_value"
        });
        let (directive, _) = resolve_step_envelope(&HashMap::new(), "scout", input).unwrap();
        assert_eq!(directive, json!({ "other_key": "other_value" }));
    }

    #[test]
    fn empty_remainder_becomes_empty_string_directive() {
        let input = json!({ "$step_meta": { "ref": "heavy-scan" } });
        let step_metas = metas(&[("heavy-scan", json!({ "work_dir": "/x" }))]);
        let (directive, step_ctx) = resolve_step_envelope(&step_metas, "scout", input).unwrap();
        assert_eq!(directive, Value::String(String::new()));
        assert_eq!(step_ctx, Some(json!({ "work_dir": "/x" })));
    }

    #[test]
    fn unresolved_ref_is_a_loud_dispatcher_error_naming_ref_and_defined() {
        let step_metas = metas(&[("known", json!({}))]);
        let input = json!({ "$step_meta": { "ref": "unknown" }, "$in": "go" });
        let err = resolve_step_envelope(&step_metas, "scout", input).unwrap_err();
        match err {
            EvalError::DispatcherError { ref_, msg } => {
                assert_eq!(ref_, "scout");
                assert!(
                    msg.contains("unknown"),
                    "message must name the unresolved ref: {msg}"
                );
                assert!(
                    msg.contains("known"),
                    "message must list defined names: {msg}"
                );
            }
            other => panic!("expected DispatcherError, got {other:?}"),
        }
    }

    #[test]
    fn malformed_step_meta_not_an_object_is_a_loud_error() {
        let input = json!({ "$step_meta": "not-an-object" });
        let err = resolve_step_envelope(&HashMap::new(), "scout", input).unwrap_err();
        assert!(matches!(err, EvalError::DispatcherError { .. }));
    }

    #[test]
    fn malformed_ref_non_string_is_a_loud_error() {
        let input = json!({ "$step_meta": { "ref": 42 } });
        let err = resolve_step_envelope(&HashMap::new(), "scout", input).unwrap_err();
        assert!(matches!(err, EvalError::DispatcherError { .. }));
    }

    #[test]
    fn malformed_inline_non_object_is_a_loud_error() {
        let input = json!({ "$step_meta": { "inline": "not-an-object" } });
        let err = resolve_step_envelope(&HashMap::new(), "scout", input).unwrap_err();
        assert!(matches!(err, EvalError::DispatcherError { .. }));
    }

    #[test]
    fn ref_resolved_metadef_ctx_non_object_is_a_loud_error() {
        let step_metas = metas(&[("bad", json!("not-an-object"))]);
        let input = json!({ "$step_meta": { "ref": "bad" } });
        let err = resolve_step_envelope(&step_metas, "scout", input).unwrap_err();
        assert!(matches!(err, EvalError::DispatcherError { .. }));
    }

    /// End-to-end proof (issue #21 Phase 2 Done Criteria #5): a `$step_meta`
    /// envelope must never reach `EngineState.prompts[(tid, 1)]` — the
    /// resolve step runs BEFORE `start_task` seeds that table.
    #[tokio::test]
    async fn dispatch_step_meta_envelope_never_leaks_into_stored_prompt() {
        use crate::blueprint::compiler::{RustFnInProcessSpawnerFactory, SpawnerFactory};
        use crate::core::config::EngineCfg;
        use crate::types::{Role, StepId};
        use crate::worker::adapter::WorkerResult;
        use std::sync::Mutex as StdMutex;
        use std::time::Duration;

        let captured_tid: Arc<StdMutex<Option<StepId>>> = Arc::new(StdMutex::new(None));
        let captured_tid_for_fn = captured_tid.clone();
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", move |inv| {
            let captured_tid = captured_tid_for_fn.clone();
            async move {
                *captured_tid.lock().unwrap() = Some(inv.task_id.clone());
                Ok(WorkerResult {
                    value: json!({ "ok": true }),
                    ok: true,
                })
            }
        });
        let def = AgentDef {
            name: "scout".into(),
            kind: AgentKind::RustFn,
            spec: json!({ "fn_id": "echo" }),
            profile: None,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
        };
        let spawner = factory.build(&def, None).expect("build");

        let engine = Engine::new(EngineCfg::default());
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let step_metas = metas(&[("heavy-scan", json!({ "work_dir": "/x" }))]);
        let dispatcher = EngineDispatcher::with_spawner(engine.clone(), token, spawner)
            .with_step_metas(step_metas);

        let input = json!({
            "$step_meta": { "ref": "heavy-scan" },
            "$in": "do the thing"
        });
        let out = dispatcher
            .dispatch("scout", input)
            .await
            .expect("dispatch ok");
        assert_eq!(out, json!({ "ok": true }));

        let tid = captured_tid
            .lock()
            .unwrap()
            .clone()
            .expect("task_id captured");
        let stored_prompt = engine
            .with_state("test.read_prompt", move |s| {
                s.prompts.get(&(tid, 1)).cloned()
            })
            .await
            .expect("with_state")
            .expect("prompt recorded for attempt 1");
        assert_eq!(
            stored_prompt,
            json!("do the thing"),
            "the stored prompt must be the post-envelope directive, with no $step_meta leakage"
        );
    }
}
