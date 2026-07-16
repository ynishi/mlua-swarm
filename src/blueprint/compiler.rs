//! Blueprint `Compiler`, `CompiledAgentTable`, and the three default
//! `SpawnerFactory` implementations.
//!
//! ## Pipeline
//!
//! ```text
//! Blueprint (= flow + agents + hints + strategy + spawner_hints)
//!     │
//!     │ Compiler.compile(&bp)          ← this module (AgentDef → SpawnerAdapter table)
//!     ▼
//! CompiledBlueprint {
//!     router: Arc<CompiledAgentTable>, // ctx.agent → SpawnerAdapter lookup
//!     flow:   FlowNode,                // the flow.ir source (evaluated via EngineDispatcher)
//!     metadata: BlueprintMetadata,
//! }
//!     │
//!     │ service::linker::link(router, blueprint.spawner_hints.layers, &engine)
//!     ▼                                   ↑ Layer wrapping is done separately (src/service/linker.rs)
//! `Arc<dyn SpawnerAdapter>`            (already wrapped with base + hint SpawnerLayers)
//!     │
//!     ▼ EngineDispatcher::with_spawner → engine.dispatch_attempt_with
//! ```
//!
//! `CompiledAgentTable` is a thin table: it looks up `routes[name]` by
//! `ctx.agent` and hands the spawn off to the matching `SpawnerAdapter`.
//! The `routes` map is built at compile time through `SpawnerFactory`
//! implementations. Layer wrapping is not part of this module — it lives
//! in `service::linker::link`.

use crate::blueprint::{AgentDef, AgentKind, Blueprint, BlueprintMetadata};
use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::core::projection_placement::{ProjectionPlacement, ProjectionPlacementError};
use crate::core::step_naming::{StepNaming, StepNamingError};
use crate::operator::{Operator, OperatorSpawner, WorkerBinding};
use crate::types::{CapToken, StepId};
use crate::worker::adapter::{InProcSpawner, SpawnError, SpawnerAdapter, WorkerFn};
use crate::worker::process_spawner::{ProcessSpawner, StreamMode};
use crate::worker::Worker;
use async_trait::async_trait;
use mlua_flow_ir::{Expr, Node as FlowNode, Path};
use mlua_swarm_schema::{VerdictChannel, VerdictContract};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

// ─── error ───────────────────────────────────────────────────────────────

/// Everything that can go wrong while `Compiler::compile` turns a
/// `Blueprint` into a `CompiledBlueprint`.
#[derive(Debug, Error)]
pub enum CompileError {
    /// An `AgentDef.kind` has no matching entry in the `SpawnerRegistry`
    /// and `Blueprint.strategy.strict_kind` is set.
    #[error("unknown agent kind in SpawnerRegistry: {0:?}")]
    UnknownKind(AgentKind),
    /// The `AgentDef.spec` shape did not match what the factory for its
    /// kind requires (missing/mistyped field, etc.).
    #[error("agent '{name}' spec invalid: {msg}")]
    InvalidSpec {
        /// The offending agent's name.
        name: String,
        /// Human-readable description of what was wrong with the spec.
        msg: String,
    },
    /// The flow references an agent name that has no corresponding
    /// `AgentDef` (and no default spawner is configured).
    #[error("flow references agent '{0}' but no AgentDef matches")]
    UnresolvedRef(String),
    /// Two `AgentDef`s in the same `Blueprint` share a name.
    #[error("duplicate AgentDef name: {0}")]
    DuplicateAgent(String),
    /// A `kind = Operator` agent's `spec.operator_ref` does not match
    /// any `OperatorDef.name` declared in `Blueprint.operators`.
    #[error("agent '{agent}' operator_ref '{op_ref}' does not match any OperatorDef.name in Blueprint.operators (defined: {defined:?})")]
    UnresolvedOperatorRef {
        /// The agent whose `operator_ref` didn't resolve.
        agent: String,
        /// The `operator_ref` value that was looked up.
        op_ref: String,
        /// The `OperatorDef.name`s that *are* declared, for the error
        /// message.
        defined: Vec<String>,
    },
    /// GH #21 Phase 2: an `AgentMeta.meta_ref` or a statically-visible
    /// `$step_meta.ref` (inside a `Step.in` **Lit** expr) does not match
    /// any `MetaDef.name` declared in `Blueprint.metas`.
    #[error("{where_} names an undefined MetaDef: '{meta_ref}' (defined: {defined:?})")]
    UnresolvedMetaRef {
        /// Human-readable description of where the reference was found
        /// (e.g. `"AgentMeta.meta_ref of agent 'planner'"` or `"Step
        /// 'scout' $step_meta.ref"`).
        where_: String,
        /// The `meta_ref` value that was looked up.
        meta_ref: String,
        /// The `MetaDef.name`s that *are* declared, for the error
        /// message.
        defined: Vec<String>,
    },
    /// GH #23: two Steps' canonical/alias projection names collide and at
    /// least one side declared `AgentMeta.projection_name` — see
    /// [`crate::core::step_naming::StepNaming::from_blueprint`]'s doc for
    /// the full resolution rule (an undeclared/undeclared clash is a soft
    /// warning instead, logged but not rejected).
    #[error("StepNaming collision: {0}")]
    StepNamingCollision(#[from] StepNamingError),
    /// GH #27 (follow-up to #23): `Blueprint.projection_placement` failed
    /// validation — see
    /// [`crate::core::projection_placement::ProjectionPlacement::from_spec`]'s
    /// doc for the rejection rules (`dir_template` empty / missing the
    /// `{task_id}` placeholder / absolute / containing a `..` segment, or
    /// `root` not `"work_dir"`/`"project_root"`).
    #[error("invalid projection_placement: {0}")]
    InvalidProjectionPlacement(#[from] ProjectionPlacementError),
    /// GH #34: an `audits[].agent` name does not match any `AgentDef.name`
    /// declared in `Blueprint.agents` — mirrors the `operator_ref`
    /// validation above (same "design-time reference must resolve"
    /// discipline).
    #[error("audits[].agent '{agent}' does not match any AgentDef.name in Blueprint.agents (defined: {defined:?})")]
    UnresolvedAuditAgent {
        /// The `audits[].agent` value that was looked up.
        agent: String,
        /// The `AgentDef.name`s that *are* declared, for the error
        /// message.
        defined: Vec<String>,
    },
    /// GH #50: a `Branch`/`Loop` `cond` compares a contract-bearing
    /// agent's output using the wrong OUTPUT channel — e.g. the agent
    /// declares `channel: "part"` (verdict staged as the named part
    /// `"verdict"`, addressed `$.<step>.parts.verdict`) but the cond
    /// addresses the bare step output (`$.<step>`) instead, or vice
    /// versa. See the `blueprint-authoring.md` guide's "Returning
    /// verdicts to drive BP flow" section for Pattern A (`channel:
    /// "body"`) vs Pattern B (`channel: "part"`).
    #[error(
        "agent '{agent}' declares verdict channel '{expected_channel}' but {where_} \
         addresses it as '{actual_shape}' output — see the \"Returning verdicts to drive \
         BP flow\" guide's Pattern A (channel: \"body\") / Pattern B (channel: \"part\")"
    )]
    VerdictChannelMismatch {
        /// Human-readable description of where the offending cond was
        /// found (e.g. `"Branch cond"` / `"Loop cond"`).
        where_: String,
        /// The agent whose declared `verdict.channel` didn't match.
        agent: String,
        /// The agent's declared channel (`"body"` or `"part"`).
        expected_channel: String,
        /// The channel shape the cond's `Path` actually addressed
        /// (`"body"` or `"part"`).
        actual_shape: String,
    },
    /// GH #50: a `Branch`/`Loop` `cond`'s `Lit` operand (or, for `In`, one
    /// of the `Lit` haystack's array elements) is not a member of a
    /// contract-bearing agent's declared `verdict.values` closed token
    /// set.
    #[error(
        "agent '{agent}' verdict Lit '{value}' at {where_} is not a member of the declared \
         values {values:?}"
    )]
    VerdictValueNotInContract {
        /// Human-readable description of where the offending cond was
        /// found (e.g. `"Branch cond"` / `"Loop cond"`).
        where_: String,
        /// The agent whose declared `verdict.values` didn't contain
        /// `value`.
        agent: String,
        /// The offending `Lit` value, rendered as a string (the raw JSON
        /// representation when it is not itself a JSON string — a
        /// non-string `Lit` can never be a member of `values: Vec<String>`
        /// either way).
        value: String,
        /// The agent's declared `verdict.values` closed token set, for the
        /// error message.
        values: Vec<String>,
    },
    /// GH #50 follow-up (issue `33bc825b`): a contract-bearing agent
    /// declares `verdict.values = [...]` but at least one member of that
    /// closed token set is never referenced by any downstream
    /// `Branch`/`Loop` `cond` `Lit` — the flow author declared a verdict
    /// value they never wrote a handler for. Emitted only when the
    /// Blueprint opts in via
    /// [`BlueprintMetadata::strict_verdict_handling`]`= Some(true)`; under
    /// the default (`None`/`Some(false)`) unhandled values surface as
    /// `tracing::warn!` only and compilation succeeds (back-compat with
    /// Blueprints that intentionally leave some verdict values as
    /// silent-pass informational tokens).
    #[error(
        "agent '{agent}' declares verdict value '{value}' but no downstream Branch/Loop \
         cond references it (declared: {declared_values:?}, at step '{step_ref}') — either \
         handle the value downstream or drop it from `verdict.values`"
    )]
    VerdictValueUnhandled {
        /// The agent whose declared `verdict.values` entry lacks a
        /// downstream handler.
        agent: String,
        /// The declared value that has no downstream `cond` reference.
        value: String,
        /// The agent's full declared `verdict.values` closed token set,
        /// for the error message.
        declared_values: Vec<String>,
        /// The `Step.ref_` where this agent is invoked. When the same
        /// agent is invoked at multiple sites, the first one encountered
        /// during flow walk is reported (best-effort — the diagnostic
        /// still identifies the offending agent uniquely).
        step_ref: String,
    },
}

// ─── SpawnerFactory + Registry ───────────────────────────────────────────

/// Factory trait that interprets an `AgentDef` and builds the concrete
/// `SpawnerAdapter`. Register one per kind. Parsing the spec,
/// validating it, and baking the profile are the implementation's job.
///
/// The signature was widened in v9 from `(name, spec, hint)` to
/// `(&AgentDef, hint)` so the profile can be passed through. Most
/// implementations still just pull `&agent_def.name` and
/// `&agent_def.spec`, but Operator-backend factories consume
/// `agent_def.profile` to bake the persona in.
pub trait SpawnerFactory: Send + Sync {
    /// Build the concrete `SpawnerAdapter` for one `AgentDef`. `hint` is
    /// the matching entry (if any) from `Blueprint.hints.per_agent`.
    fn build(
        &self,
        agent_def: &AgentDef,
        hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError>;
}

/// Companion trait that carries the **type-side source of truth** for
/// the Adapter ↔ `AgentKind` correspondence.
///
/// The base [`SpawnerFactory`] trait deliberately does not carry an
/// associated const so it stays dyn-compatible — that is, so it can be
/// stored and dispatched as `Arc<dyn SpawnerFactory>`. This companion
/// trait splits `const KIND: AgentKind` out, and
/// [`SpawnerRegistry::register`] uses `F::KIND` as the `HashMap` key.
/// That physically removes the string-lookup failure mode at the type
/// layer.
///
/// The three built-in factories (`Shell` / `InProc` / `Operator`)
/// implement this. Extension backends (say, `AgentBlockSpawnerFactory`)
/// follow the same explicit two-step recipe: add a new `AgentKind`
/// variant and implement this trait.
pub trait SpawnerFactoryKind: SpawnerFactory {
    /// The `AgentKind` this factory handles — used as the `HashMap` key
    /// by `SpawnerRegistry::register`.
    const KIND: AgentKind;
    /// The concrete Worker type produced by this `AgentKind` — this
    /// binds the type chain all the way from `AgentKind` down to `Worker`.
    /// Every factory declares it so the `AgentKind → Worker` mapping is
    /// explicit across all four layers. It is the source of truth for
    /// preserving the concrete type right up until `SpawnerAdapter::spawn`
    /// erases it into `Box<dyn Worker>`.
    type Worker: crate::worker::Worker;
}

/// `AgentKind → SpawnerFactory` mapping. The compiler looks entries up
/// during `compile()`.
#[derive(Clone)]
pub struct SpawnerRegistry {
    factories: HashMap<AgentKind, Arc<dyn SpawnerFactory>>,
}

impl SpawnerRegistry {
    /// Start with an empty `AgentKind → SpawnerFactory` mapping.
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }
    /// **Type-driven registration** — takes `F::KIND` and uses it as the
    /// `HashMap` key.
    ///
    /// Callers use the form
    /// `reg.register::<SubprocessProcessSpawnerFactory>(Arc::new(...))`
    /// and never have to pass an `AgentKind` literal. The Adapter ↔ Kind
    /// correspondence is enforced at the type layer, physically removing
    /// the string / enum-literal lookup failure mode.
    pub fn register<F: SpawnerFactoryKind + 'static>(&mut self, factory: Arc<F>) -> &mut Self {
        let f: Arc<dyn SpawnerFactory> = factory;
        self.factories.insert(F::KIND, f);
        self
    }
}

impl Default for SpawnerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Compiler ────────────────────────────────────────────────────────────

/// Turns a `Blueprint` into a `CompiledBlueprint` by resolving every
/// `AgentDef` against a `SpawnerRegistry`. One-shot: build a fresh
/// `Compiler` per `compile()` call (or reuse it — it holds no
/// per-compile state).
pub struct Compiler {
    registry: SpawnerRegistry,
    default_spawner: Option<Arc<dyn SpawnerAdapter>>,
}

/// The result of `Compiler::compile` — a routing table plus the
/// unmodified flow and metadata, ready to hand to
/// `EngineDispatcher::with_spawner` / `mlua_flow_ir::eval_async`.
pub struct CompiledBlueprint {
    /// `ctx.agent → SpawnerAdapter` lookup table.
    pub router: Arc<CompiledAgentTable>,
    /// The flow.ir source, copied verbatim from `Blueprint.flow`.
    pub flow: FlowNode,
    /// Copied verbatim from `Blueprint.metadata`.
    pub metadata: BlueprintMetadata,
    /// GH #23: the Blueprint's [`StepNaming`] addressing-space table,
    /// built once here (the sole construction site — see
    /// [`StepNaming::from_blueprint`]'s doc) and threaded through
    /// `EngineDispatcher::with_step_naming` for `EngineState` storage.
    pub step_naming: Arc<StepNaming>,
    /// GH #27 (follow-up to #23): the Blueprint's [`ProjectionPlacement`]
    /// resolver, built once here (the sole construction site — see
    /// [`ProjectionPlacement::from_spec`]'s doc) and threaded through
    /// `EngineDispatcher::with_projection_placement` for `EngineState`
    /// storage.
    pub projection_placement: Arc<ProjectionPlacement>,
}

impl Compiler {
    /// Build a `Compiler` around the given `SpawnerRegistry`, with no
    /// default spawner (unresolved flow refs are an error unless
    /// `with_default` is chained on).
    pub fn new(registry: SpawnerRegistry) -> Self {
        Self {
            registry,
            default_spawner: None,
        }
    }

    /// Set a default spawner — used for flow refs (and unregistered
    /// `AgentKind`s under non-strict strategy) that don't resolve
    /// against any `AgentDef`/`SpawnerRegistry` entry.
    pub fn with_default(mut self, sp: Arc<dyn SpawnerAdapter>) -> Self {
        self.default_spawner = Some(sp);
        self
    }

    /// Resolve every `Blueprint.agents` entry through the registry,
    /// validate `operator_ref`s and flow refs per `Blueprint.strategy`,
    /// and return the routing table alongside the untouched flow and
    /// metadata.
    pub fn compile(&self, bp: &Blueprint) -> Result<CompiledBlueprint, CompileError> {
        let mut routes: HashMap<String, Arc<dyn SpawnerAdapter>> = HashMap::new();
        let mut seen: HashMap<String, ()> = HashMap::new();
        // GH #50: `AgentDef.name` → declared `VerdictContract`, collected
        // alongside `routes` below (every `verdict: Some(...)` agent, kind
        // resolution notwithstanding). Consumed by the cond↔output-shape
        // lint right after the loop, and carried into
        // `CompiledAgentTable.verdict_contracts`.
        let mut verdict_contracts: HashMap<String, VerdictContract> = HashMap::new();

        // Design-time validation (OperatorDef as a first-class value):
        // every `kind = Operator` agent's `spec.operator_ref` must point at
        // one of `bp.operators[].name`. A Blueprint with any Operator agent
        // must therefore declare its operators up front; the empty-operators
        // backward-compat bypass is retired.
        let defined: Vec<String> = bp.operators.iter().map(|o| o.name.clone()).collect();
        for ad in &bp.agents {
            if !matches!(ad.kind, AgentKind::Operator) {
                continue;
            }
            let op_ref = ad.spec.get("operator_ref").and_then(|v| v.as_str());
            if let Some(op_ref) = op_ref {
                if !defined.iter().any(|n| n == op_ref) {
                    return Err(CompileError::UnresolvedOperatorRef {
                        agent: ad.name.clone(),
                        op_ref: op_ref.to_string(),
                        defined: defined.clone(),
                    });
                }
            }
            // A missing `op_ref` is reported through OperatorSpawnerFactory.build under a different error.
        }

        // GH #21 Phase 2: named `MetaDef` pool (`Blueprint.metas`) —
        // validate every reference against it, mirroring the
        // `operator_ref` validation above.
        let metas_defined: Vec<String> = bp.metas.iter().map(|m| m.name.clone()).collect();
        for ad in &bp.agents {
            let meta_ref = ad.meta.as_ref().and_then(|m| m.meta_ref.as_ref());
            if let Some(meta_ref) = meta_ref {
                if !metas_defined.iter().any(|n| n == meta_ref) {
                    return Err(CompileError::UnresolvedMetaRef {
                        where_: format!("AgentMeta.meta_ref of agent '{}'", ad.name),
                        meta_ref: meta_ref.clone(),
                        defined: metas_defined.clone(),
                    });
                }
            }
        }
        // Best-effort static walk of the flow for `$step_meta.ref`
        // envelopes embedded in a Step's **Lit** `in` expr — this is a
        // design-time hint only: a non-`Lit` `Step.in` (e.g. `Path`) is
        // invisible here and skipped silently; `EngineDispatcher::dispatch`
        // is the authoritative, loud validation line for those.
        let mut static_step_meta_refs: Vec<(String, String)> = Vec::new();
        collect_step_meta_refs(&bp.flow, &mut static_step_meta_refs);
        for (where_, meta_ref) in static_step_meta_refs {
            if !metas_defined.iter().any(|n| n == &meta_ref) {
                return Err(CompileError::UnresolvedMetaRef {
                    where_,
                    meta_ref,
                    defined: metas_defined.clone(),
                });
            }
        }

        // GH #34: `audits[].agent` must name an entry in `Blueprint.agents`
        // — mirrors the `operator_ref` validation above (design-time
        // reference must resolve at compile time, before any spawner is
        // built).
        let agents_defined: Vec<String> = bp.agents.iter().map(|a| a.name.clone()).collect();
        for audit in &bp.audits {
            if !agents_defined.iter().any(|n| n == &audit.agent) {
                return Err(CompileError::UnresolvedAuditAgent {
                    agent: audit.agent.clone(),
                    defined: agents_defined.clone(),
                });
            }
        }

        for ad in &bp.agents {
            if seen.contains_key(&ad.name) {
                return Err(CompileError::DuplicateAgent(ad.name.clone()));
            }
            seen.insert(ad.name.clone(), ());

            // GH #50: contract registration is orthogonal to spawner
            // resolution (an agent may declare `verdict` regardless of
            // whether its `kind` resolves), so it happens unconditionally
            // here, before the kind-resolution branch below that may
            // `continue`.
            if let Some(contract) = &ad.verdict {
                verdict_contracts.insert(ad.name.clone(), contract.clone());
            }

            let factory = match self.registry.factories.get(&ad.kind) {
                Some(f) => f.clone(),
                None => {
                    if bp.strategy.strict_kind {
                        return Err(CompileError::UnknownKind(ad.kind.clone()));
                    } else {
                        tracing::warn!(
                            agent = %ad.name,
                            kind = ?ad.kind,
                            "no spawner factory registered for agent kind; \
                             dropping agent from routing table (strict_kind=false)"
                        );
                        continue;
                    }
                }
            };
            let hint = bp.hints.per_agent.get(&ad.name);
            let spawner = factory.build(ad, hint)?;
            routes.insert(ad.name.clone(), spawner);
        }

        // GH #50: `Branch`/`Loop` cond↔output-shape lint. A contract-
        // bearing agent's output must be compared the way its declared
        // `verdict.channel` requires and its `Lit` value(s) must be
        // members of its declared `verdict.values`; an agent referenced by
        // a cond but declaring no contract only gets a `tracing::warn!`
        // (opt-in, back-compat — see `AgentDef::verdict`'s doc). Read-only
        // inspection of `bp.flow` — no rewriting, no new `Expr` forms.
        //
        // GH #50 follow-up (issue `33bc825b`): the reverse-direction lint
        // — declared `verdict.values` entries that no downstream cond
        // references — runs in the same walk. Under
        // `BlueprintMetadata.strict_verdict_handling = Some(true)` it
        // rejects the compile; otherwise it only surfaces
        // `tracing::warn!` so existing Blueprints that intentionally leave
        // some verdict values as silent-pass informational tokens keep
        // compiling unchanged.
        let strict_verdict_handling = bp.metadata.strict_verdict_handling.unwrap_or(false);
        verify_verdict_conds(&bp.flow, &verdict_contracts, strict_verdict_handling)?;

        if bp.strategy.strict_refs {
            verify_refs(&bp.flow, &routes, self.default_spawner.is_some())?;
        }

        // GH #23: build the StepNaming addressing-space table once, here
        // (the sole construction site). A hard collision (either side
        // declares `AgentMeta.projection_name`) rejects the compile via
        // `?` (`StepNamingError` → `CompileError::StepNamingCollision`,
        // same family as the other Blueprint validation checks above); a
        // soft undeclared/undeclared collision is logged and compilation
        // proceeds (pre-GH-#23 union-rule behavior preserved).
        let (step_naming, step_naming_warnings) = StepNaming::from_blueprint(bp)?;
        for warning in &step_naming_warnings {
            tracing::warn!(
                name = %warning.name,
                first_step_ref = %warning.first_step_ref,
                second_step_ref = %warning.second_step_ref,
                "StepNaming: undeclared steps' canonical/alias names collide; \
                 the step whose own ref matches the name keeps it (data-plane priority)"
            );
        }

        // GH #27 (follow-up to #23): build the ProjectionPlacement resolver
        // once, here (the sole construction site) — an invalid
        // `dir_template` / `root` literal rejects the compile via `?`
        // (`ProjectionPlacementError` → `CompileError::InvalidProjectionPlacement`,
        // same family as the other Blueprint validation checks above). No
        // declared `projection_placement` (the pre-#27 default) resolves
        // to `ProjectionPlacement::default()` unchanged.
        let projection_placement =
            ProjectionPlacement::from_spec(bp.projection_placement.as_ref())?;

        let router = Arc::new(CompiledAgentTable {
            routes,
            default: self.default_spawner.clone(),
            verdict_contracts,
        });
        Ok(CompiledBlueprint {
            router,
            flow: bp.flow.clone(),
            metadata: bp.metadata.clone(),
            step_naming: Arc::new(step_naming),
            projection_placement: Arc::new(projection_placement),
        })
    }
}

/// Walk the flow `Node`, collect every `Step.ref`, and check that no ref
/// is unresolved against `routes` (or the default, when one exists).
fn verify_refs(
    node: &FlowNode,
    routes: &HashMap<String, Arc<dyn SpawnerAdapter>>,
    has_default: bool,
) -> Result<(), CompileError> {
    let mut refs: Vec<String> = Vec::new();
    collect_refs(node, &mut refs);
    for r in refs {
        if !routes.contains_key(&r) && !has_default {
            return Err(CompileError::UnresolvedRef(r));
        }
    }
    Ok(())
}

fn collect_refs(node: &FlowNode, out: &mut Vec<String>) {
    match node {
        FlowNode::Step { ref_, .. } => out.push(ref_.clone()),
        FlowNode::Seq { children } => {
            for c in children {
                collect_refs(c, out);
            }
        }
        FlowNode::Branch { then_, else_, .. } => {
            collect_refs(then_, out);
            collect_refs(else_, out);
        }
        FlowNode::Fanout { body, .. } => collect_refs(body, out),
        FlowNode::Loop { body, .. } => collect_refs(body, out),
        FlowNode::Try { body, catch, .. } => {
            collect_refs(body, out);
            collect_refs(catch, out);
        }
        FlowNode::Assign { .. } => {} // The Assign node carries no ref.
    }
}

/// GH #21 Phase 2: walk the flow `Node` (same recursion shape as
/// [`collect_refs`]) and collect every statically-visible `$step_meta.ref`
/// found inside a Step's `in` **Lit** expr, as `(where_, meta_ref)` pairs
/// for [`CompileError::UnresolvedMetaRef`] reporting. Non-`Lit` `in`
/// exprs (e.g. `Expr::Path`) cannot be inspected statically and are
/// silently skipped — `EngineDispatcher::dispatch` (the `mlua-swarm` core
/// crate) is the authoritative, loud validation line for those.
fn collect_step_meta_refs(node: &FlowNode, out: &mut Vec<(String, String)>) {
    match node {
        FlowNode::Step { ref_, in_, .. } => {
            if let Expr::Lit { value } = in_ {
                if let Some(meta_ref) = static_step_meta_ref(value) {
                    out.push((format!("Step '{ref_}' $step_meta.ref"), meta_ref));
                }
            }
        }
        FlowNode::Seq { children } => {
            for c in children {
                collect_step_meta_refs(c, out);
            }
        }
        FlowNode::Branch { then_, else_, .. } => {
            collect_step_meta_refs(then_, out);
            collect_step_meta_refs(else_, out);
        }
        FlowNode::Fanout { body, .. } => collect_step_meta_refs(body, out),
        FlowNode::Loop { body, .. } => collect_step_meta_refs(body, out),
        FlowNode::Try { body, catch, .. } => {
            collect_step_meta_refs(body, out);
            collect_step_meta_refs(catch, out);
        }
        FlowNode::Assign { .. } => {} // The Assign node carries no `in`.
    }
}

/// Extract the `$step_meta.ref` string out of a literal `Step.in` value,
/// if present and well-formed: `{"$step_meta": {"ref": "<name>", ...},
/// ...}`. Any other shape (no `$step_meta` key, `ref` absent/null, `ref`
/// not a string) yields `None` — this is a best-effort static hint only;
/// a malformed envelope is caught loudly at dispatch time instead (see
/// `EngineDispatcher::dispatch`'s doc in the `mlua-swarm` core crate).
fn static_step_meta_ref(value: &Value) -> Option<String> {
    value
        .as_object()?
        .get("$step_meta")?
        .as_object()?
        .get("ref")?
        .as_str()
        .map(str::to_string)
}

// ─── GH #50: verdict contract cond↔output-shape lint ───────────────────────

/// GH #50: `Blueprint.agents[].verdict` cond↔output-shape lint, run from
/// `Compiler::compile` after the routing table is built. Two-pass, same
/// shape as [`collect_step_meta_refs`]'s best-effort static walk: Pass 1
/// ([`collect_step_outputs`]) builds `Step.out` `Path` string → producing
/// `Step.ref_`; Pass 2 ([`collect_verdict_conds`]) walks every
/// `Branch`/`Loop` `cond` and resolves each `Eq`/`Ne`/`In` `Path`+`Lit`
/// comparison back through the Pass 1 map. Collects every violation before
/// returning, then surfaces the first one (mirrors the other
/// `Compiler::compile` validation blocks' `Result::Err`-via-`?` pattern).
fn verify_verdict_conds(
    flow: &FlowNode,
    verdict_contracts: &HashMap<String, VerdictContract>,
    strict_verdict_handling: bool,
) -> Result<(), CompileError> {
    let mut step_outputs: HashMap<String, String> = HashMap::new();
    let mut step_agents: HashMap<String, String> = HashMap::new();
    collect_step_outputs_and_agents(flow, &mut step_outputs, &mut step_agents);

    let mut errors: Vec<CompileError> = Vec::new();
    let mut referenced_values: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
    collect_verdict_conds(
        flow,
        &step_outputs,
        verdict_contracts,
        &mut referenced_values,
        &mut errors,
    );
    check_unhandled_verdict_values(
        verdict_contracts,
        &referenced_values,
        &step_agents,
        strict_verdict_handling,
        &mut errors,
    );
    match errors.into_iter().next() {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Pass 1 of [`verify_verdict_conds`]: `Step.out` `Path` (rendered via its
/// canonical `Display` string) → the producing `Step.ref_` — mirrors
/// [`collect_refs`]'s `Step.ref_` ↔ `AgentDef.name` correspondence (a
/// `Step.ref_` directly indexes `Blueprint.agents[].name`, per
/// `verify_refs`). Only `Step` nodes produce agent output; `Fanout`'s
/// joined-array `out` and `Assign`'s computed `at` are not attributed to
/// any single agent and are not inserted here.
///
/// GH #50 follow-up (issue `33bc825b`): `step_agents` additionally maps
/// each `Step.ref_` (= agent name) to the first-seen `Step.ref_` literal,
/// so [`check_unhandled_verdict_values`] can attribute a diagnostic to a
/// concrete step site. When the same agent is invoked at multiple sites,
/// the first-encountered site is retained (best-effort — the diagnostic
/// still identifies the offending agent uniquely).
fn collect_step_outputs_and_agents(
    node: &FlowNode,
    out: &mut HashMap<String, String>,
    step_agents: &mut HashMap<String, String>,
) {
    match node {
        FlowNode::Step {
            ref_,
            out: out_expr,
            ..
        } => {
            if let Expr::Path { at } = out_expr {
                out.insert(at.to_string(), ref_.clone());
            }
            step_agents
                .entry(ref_.clone())
                .or_insert_with(|| ref_.clone());
        }
        FlowNode::Seq { children } => {
            for c in children {
                collect_step_outputs_and_agents(c, out, step_agents);
            }
        }
        FlowNode::Branch { then_, else_, .. } => {
            collect_step_outputs_and_agents(then_, out, step_agents);
            collect_step_outputs_and_agents(else_, out, step_agents);
        }
        FlowNode::Fanout { body, .. } => collect_step_outputs_and_agents(body, out, step_agents),
        FlowNode::Loop { body, .. } => collect_step_outputs_and_agents(body, out, step_agents),
        FlowNode::Try { body, catch, .. } => {
            collect_step_outputs_and_agents(body, out, step_agents);
            collect_step_outputs_and_agents(catch, out, step_agents);
        }
        FlowNode::Assign { .. } => {} // The Assign node produces no agent output.
    }
}

/// Pass 2 of [`verify_verdict_conds`]: recurse through the flow the same
/// way [`collect_refs`] does, and for every `Branch`/`Loop` node lint its
/// own `cond` field via [`lint_cond_expr`] (in addition to recursing into
/// `then_`/`else_`/`body`).
fn collect_verdict_conds(
    node: &FlowNode,
    step_outputs: &HashMap<String, String>,
    verdict_contracts: &HashMap<String, VerdictContract>,
    referenced_values: &mut HashMap<String, std::collections::HashSet<String>>,
    errors: &mut Vec<CompileError>,
) {
    match node {
        FlowNode::Branch { cond, then_, else_ } => {
            lint_cond_expr(
                cond,
                "Branch cond",
                step_outputs,
                verdict_contracts,
                referenced_values,
                errors,
            );
            collect_verdict_conds(
                then_,
                step_outputs,
                verdict_contracts,
                referenced_values,
                errors,
            );
            collect_verdict_conds(
                else_,
                step_outputs,
                verdict_contracts,
                referenced_values,
                errors,
            );
        }
        FlowNode::Loop { cond, body, .. } => {
            lint_cond_expr(
                cond,
                "Loop cond",
                step_outputs,
                verdict_contracts,
                referenced_values,
                errors,
            );
            collect_verdict_conds(
                body,
                step_outputs,
                verdict_contracts,
                referenced_values,
                errors,
            );
        }
        FlowNode::Seq { children } => {
            for c in children {
                collect_verdict_conds(
                    c,
                    step_outputs,
                    verdict_contracts,
                    referenced_values,
                    errors,
                );
            }
        }
        FlowNode::Fanout { body, .. } => collect_verdict_conds(
            body,
            step_outputs,
            verdict_contracts,
            referenced_values,
            errors,
        ),
        FlowNode::Try { body, catch, .. } => {
            collect_verdict_conds(
                body,
                step_outputs,
                verdict_contracts,
                referenced_values,
                errors,
            );
            collect_verdict_conds(
                catch,
                step_outputs,
                verdict_contracts,
                referenced_values,
                errors,
            );
        }
        FlowNode::Step { .. } | FlowNode::Assign { .. } => {}
    }
}

/// Lint one `cond` `Expr` tree for [`collect_verdict_conds`]: recurses into
/// `And`/`Or`/`Not` (the only boolean combinators a verdict comparison can
/// be nested under) and, for every `Eq`/`Ne` leaf whose operands are a
/// `Path` + `Lit` pair (either order — see [`path_lit_operands`]), or every
/// `In` leaf whose `needle` is a `Path` and `haystack` is a `Lit` JSON
/// array, resolves + validates via [`resolve_and_check`]. Any other `Expr`
/// shape (arithmetic, `Exists`, `CallExtern`, a non-`Path`/`Lit` `Eq`/`Ne`
/// pair, ...) is not a verdict comparison and is skipped.
fn lint_cond_expr(
    expr: &Expr,
    where_: &str,
    step_outputs: &HashMap<String, String>,
    verdict_contracts: &HashMap<String, VerdictContract>,
    referenced_values: &mut HashMap<String, std::collections::HashSet<String>>,
    errors: &mut Vec<CompileError>,
) {
    match expr {
        Expr::Eq { lhs, rhs } | Expr::Ne { lhs, rhs } => {
            if let Some((path, lit)) = path_lit_operands(lhs, rhs) {
                resolve_and_check(
                    path,
                    &[lit],
                    where_,
                    step_outputs,
                    verdict_contracts,
                    referenced_values,
                    errors,
                );
            }
        }
        Expr::In { needle, haystack } => {
            if let (
                Expr::Path { at },
                Expr::Lit {
                    value: Value::Array(items),
                },
            ) = (needle.as_ref(), haystack.as_ref())
            {
                let lits: Vec<&Value> = items.iter().collect();
                resolve_and_check(
                    at,
                    &lits,
                    where_,
                    step_outputs,
                    verdict_contracts,
                    referenced_values,
                    errors,
                );
            }
        }
        Expr::And { args } | Expr::Or { args } => {
            for a in args {
                lint_cond_expr(
                    a,
                    where_,
                    step_outputs,
                    verdict_contracts,
                    referenced_values,
                    errors,
                );
            }
        }
        Expr::Not { arg } => lint_cond_expr(
            arg,
            where_,
            step_outputs,
            verdict_contracts,
            referenced_values,
            errors,
        ),
        _ => {}
    }
}

/// Extract a `(Path, Lit value)` pair out of an `Eq`/`Ne`'s two operands,
/// regardless of which side the `Path` is on. `None` when the pairing is
/// not exactly one `Path` + one `Lit` (e.g. both are `Path`, or either is a
/// compound expr) — those are not statically resolvable to a single
/// literal token and are left for `EngineDispatcher`'s runtime eval.
fn path_lit_operands<'a>(lhs: &'a Expr, rhs: &'a Expr) -> Option<(&'a Path, &'a Value)> {
    match (lhs, rhs) {
        (Expr::Path { at }, Expr::Lit { value }) => Some((at, value)),
        (Expr::Lit { value }, Expr::Path { at }) => Some((at, value)),
        _ => None,
    }
}

/// Resolve `path` back to a producing step — either as the bare step
/// output (`channel: Body`) or, via the literal `.parts.verdict` suffix
/// (`channel: Part` — the "verdict" part name is a literal, per the
/// "Returning verdicts to drive BP flow" guide's Pattern B), as that
/// step's staged verdict part. A `path` that resolves to neither shape
/// against any known step output is skipped silently (best-effort static
/// lint only, same posture as [`collect_step_meta_refs`]).
///
/// When the resolved agent declares a [`VerdictContract`], validates the
/// resolved channel against it first (a mismatch short-circuits — the
/// value comparison is moot once the channel itself is wrong) and then
/// every entry of `lits` against `contract.values`, pushing at most one
/// `CompileError` per violation. When the resolved agent declares no
/// contract, emits a `tracing::warn!` only (GH #50's opt-in requirement).
fn resolve_and_check(
    path: &Path,
    lits: &[&Value],
    where_: &str,
    step_outputs: &HashMap<String, String>,
    verdict_contracts: &HashMap<String, VerdictContract>,
    referenced_values: &mut HashMap<String, std::collections::HashSet<String>>,
    errors: &mut Vec<CompileError>,
) {
    let path_str = path.to_string();
    let (agent, actual_shape) = if let Some(agent) = step_outputs.get(&path_str) {
        (agent, "body")
    } else if let Some(stripped) = path_str.strip_suffix(".parts.verdict") {
        match step_outputs.get(stripped) {
            Some(agent) => (agent, "part"),
            None => return,
        }
    } else {
        return;
    };

    let Some(contract) = verdict_contracts.get(agent) else {
        tracing::warn!(
            agent = %agent,
            where_ = %where_,
            "cond references agent output but no verdict contract declared"
        );
        return;
    };

    let expected_channel = match contract.channel {
        VerdictChannel::Body => "body",
        VerdictChannel::Part => "part",
    };
    if expected_channel != actual_shape {
        errors.push(CompileError::VerdictChannelMismatch {
            where_: where_.to_string(),
            agent: agent.clone(),
            expected_channel: expected_channel.to_string(),
            actual_shape: actual_shape.to_string(),
        });
        return;
    }

    for lit in lits {
        let value_str = lit
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| lit.to_string());
        if !contract.values.iter().any(|v| v == &value_str) {
            errors.push(CompileError::VerdictValueNotInContract {
                where_: where_.to_string(),
                agent: agent.clone(),
                value: value_str.clone(),
                values: contract.values.clone(),
            });
        }
        // GH #50 follow-up (issue `33bc825b`): record the referenced value
        // regardless of contract membership. `VerdictValueNotInContract`
        // already caught the out-of-set case above; recording here still
        // helps future variants that widen the set later. The value string
        // is normalized identically to the membership check for symmetric
        // comparison in `check_unhandled_verdict_values`.
        referenced_values
            .entry(agent.clone())
            .or_default()
            .insert(value_str);
    }
}

/// GH #50 follow-up (issue `33bc825b`): reverse-direction lint.
///
/// For every agent that declares a [`VerdictContract`], check that every
/// entry of `contract.values` was referenced by at least one downstream
/// `Branch`/`Loop` `cond` `Lit` (as collected into `referenced_values` by
/// [`resolve_and_check`] during the forward pass). Any declared value
/// that no cond references is a `verdict_value` the flow author declared
/// but forgot to write a handler for.
///
/// When `strict_verdict_handling` is `true` (opt-in via
/// [`BlueprintMetadata::strict_verdict_handling`]), every unhandled value
/// pushes a [`CompileError::VerdictValueUnhandled`] onto `errors` and
/// [`verify_verdict_conds`] surfaces the first one, rejecting the compile.
/// Under the default (`false`), unhandled values only surface via
/// `tracing::warn!` — existing Blueprints that intentionally leave some
/// verdict values as silent-pass informational tokens keep compiling
/// unchanged (back-compat with GH #50's opt-in posture).
fn check_unhandled_verdict_values(
    verdict_contracts: &HashMap<String, VerdictContract>,
    referenced_values: &HashMap<String, std::collections::HashSet<String>>,
    step_agents: &HashMap<String, String>,
    strict_verdict_handling: bool,
    errors: &mut Vec<CompileError>,
) {
    // Iterate in a stable order (BTreeMap-style sort by agent name, then
    // by declared value) so the first `VerdictValueUnhandled` error
    // surfaced under strict mode is deterministic across HashMap hash
    // seeds. This mirrors GH #50's other lint diagnostics, which are
    // stable because they walk the flow tree in source order.
    let mut agents: Vec<&String> = verdict_contracts.keys().collect();
    agents.sort();
    for agent in agents {
        let contract = &verdict_contracts[agent];
        let referenced = referenced_values.get(agent);
        let step_ref = step_agents
            .get(agent)
            .cloned()
            .unwrap_or_else(|| agent.clone());
        for value in &contract.values {
            let handled = referenced.map(|set| set.contains(value)).unwrap_or(false);
            if handled {
                continue;
            }
            if strict_verdict_handling {
                errors.push(CompileError::VerdictValueUnhandled {
                    agent: agent.clone(),
                    value: value.clone(),
                    declared_values: contract.values.clone(),
                    step_ref: step_ref.clone(),
                });
            } else {
                tracing::warn!(
                    agent = %agent,
                    value = %value,
                    step_ref = %step_ref,
                    "declared verdict value has no downstream cond handler; \
                     opt in to `metadata.strict_verdict_handling` to reject at compile"
                );
            }
        }
    }
}

// ─── CompiledAgentTable ───────────────────────────────────────────────────────

/// The compile result: an `agent name → SpawnerAdapter` lookup table.
///
/// Looks `routes` up by `ctx.agent` (the flow.ir `Step.ref`) and hands
/// the spawn to the matching `SpawnerAdapter`. If the name is not
/// registered and a `default` is configured, the default is used; if
/// there is no default, `SpawnError::NotRegistered` is returned.
///
/// Layer wrapping (`AuditMiddleware` / `MainAIMiddleware` and friends) is
/// not this type's concern — that is done separately in
/// `service::linker::link`.
pub struct CompiledAgentTable {
    pub(crate) routes: HashMap<String, Arc<dyn SpawnerAdapter>>,
    pub(crate) default: Option<Arc<dyn SpawnerAdapter>>,
    /// GH #50: `AgentDef.name` → declared `VerdictContract`, for every
    /// agent that declared one (built by `Compiler::compile`, alongside
    /// `routes`). Backs the submit-time enforcement point (a follow-up).
    pub(crate) verdict_contracts: HashMap<String, VerdictContract>,
}

impl CompiledAgentTable {
    /// Whether the given agent name is registered in the table — i.e.,
    /// whether its spawner has been resolved.
    pub fn has_route(&self, agent: &str) -> bool {
        self.routes.contains_key(agent)
    }
    /// List every resolved agent name.
    pub fn routed_agents(&self) -> Vec<String> {
        self.routes.keys().cloned().collect()
    }
    /// GH #50: the declared [`VerdictContract`] for `agent`, if any —
    /// `None` both when `agent` is unresolved and when it resolved but
    /// declared no contract (opt-in; see `AgentDef::verdict`'s doc).
    pub fn verdict_contract_for(&self, agent: &str) -> Option<&VerdictContract> {
        self.verdict_contracts.get(agent)
    }
}

#[async_trait]
impl SpawnerAdapter for CompiledAgentTable {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let sp = self
            .routes
            .get(&ctx.agent)
            .cloned()
            .or_else(|| self.default.clone())
            .ok_or_else(|| SpawnError::NotRegistered(ctx.agent.clone()))?;
        sp.spawn(engine, ctx, task_id, attempt, token).await
    }
}

// ─── default factories (three variants) ───────────────────────────────────

/// Factory for `AgentKind::Subprocess`. Turns the spec into a
/// [`ProcessSpawner`].
///
/// Naming convention: `<WorkerIMPL><AdapterType>SpawnerFactory`. Factory
/// names carry both the worker implementation and the host adapter so
/// they are not confused with each other; the old
/// `ShellSpawnerFactory` was renamed to this.
///
/// Spec shape:
/// ```jsonc
/// { "program": "agent-block", "args": ["-s","s.lua"],
///   "use_stdin": true,                       // optional, default = true
///   "stream_mode": "ndjson_lines" | "sse_events" | "length_prefixed" | null  // optional, default = null (plain)
/// }
/// ```
pub struct SubprocessProcessSpawnerFactory;

impl SpawnerFactoryKind for SubprocessProcessSpawnerFactory {
    const KIND: AgentKind = AgentKind::Subprocess;
    type Worker = crate::worker::process_spawner::ProcessWorker;
}

impl SpawnerFactory for SubprocessProcessSpawnerFactory {
    fn build(
        &self,
        agent_def: &AgentDef,
        _hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
        let agent_name = &agent_def.name;
        let spec = &agent_def.spec;
        let invalid = |msg: String| CompileError::InvalidSpec {
            name: agent_name.to_string(),
            msg,
        };
        let program = spec
            .get("program")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid("shell spec: 'program' (string) required".into()))?
            .to_string();
        let args: Vec<String> = spec
            .get("args")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let use_stdin = spec
            .get("use_stdin")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let stream_mode = match spec.get("stream_mode").and_then(|v| v.as_str()) {
            Some("ndjson_lines") => Some(StreamMode::NdjsonLines),
            Some("sse_events") => Some(StreamMode::SseEvents),
            Some("length_prefixed") => Some(StreamMode::LengthPrefixed),
            Some(other) => return Err(invalid(format!("unknown stream_mode: {other}"))),
            None => None,
        };

        let mut sp = ProcessSpawner {
            program,
            args,
            use_stdin,
            stream_mode,
        };
        if let Some(mode) = sp.stream_mode.clone() {
            sp = sp.stream_mode(mode);
        }
        Ok(Arc::new(sp))
    }
}

/// Factory for `AgentKind::Lua`. At `build` time it inspects the
/// `AgentDef.spec` and returns an [`InProcSpawner`] with the Lua-eval
/// `WorkerFn` registered under `agent_name` — one `InProcSpawner`
/// instance per agent.
///
/// Naming convention: `<WorkerIMPL><AdapterType>SpawnerFactory` (Lua
/// worker on InProcess adapter). One half of the old
/// `InProcSpawnerFactory`, split into Lua and RustFn variants.
///
/// Spec shape (choose one; `source` wins when both are present):
///
/// ```jsonc
/// // (a) Registry lookup — Lua source id pre-registered with the
/// //     factory via `register_lua` (used by the enhance flow's built-in
/// //     workers). Requires the factory to know the id at construction
/// //     time.
/// { "fn_id": "patch-spawner" }
///
/// // (b) Inline source — a Lua chunk carried by the Blueprint itself,
/// //     wrapped on the fly at `build` time. Combined with the loader's
/// //     `$file` ref expansion (`"source": {"$file": "gates/foo.lua"}`)
/// //     this lets a BP ship deterministic Lua gates without any
/// //     pre-registration. `label` is optional and defaults to
/// //     `"<agent_name>.lua"` for error messages.
/// { "source": "return { value = 42, ok = true }",
///   "label": "psim-gate.lua" }
/// ```
///
/// Host bridges registered on the factory (see [`Self::with_bridge`])
/// apply to both spec shapes.
pub struct LuaInProcessSpawnerFactory {
    registry: HashMap<String, WorkerFn>,
    bridges: HashMap<String, HostBridge>,
}

/// Rust-side bridge function callable from Lua.
///
/// Inputs and outputs are both `serde_json::Value` (i.e. JSON). Lua
/// invokes it as `host.<name>(arg_table)`. If the implementation needs
/// to call async Rust, the caller does the sync-ification (typically
/// `tokio::runtime::Handle::current().block_on(...)`).
///
/// Design intent: keep Lua scripts focused on flow control and `ctx`
/// walking, while the heavy lifting (LLM calls, RFC 6902 apply,
/// verifiers, and so on) stays on the Rust side. Going "pure Lua" —
/// removing the bridge — is a carry.
#[derive(Clone)]
pub struct HostBridge(
    Arc<dyn Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync>,
);

impl HostBridge {
    /// Wrap a Rust closure as a bridge callable from Lua.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    /// Invoke the bridge directly — a thin trampoline over the inner
    /// `Fn`. The production path goes through the Lua runtime, but this
    /// stays `pub` so unit tests can exercise the primitive directly.
    pub fn call(&self, arg: serde_json::Value) -> Result<serde_json::Value, String> {
        (self.0)(arg)
    }
}

/// Carrier type for Lua script sources. Paths are not required — a
/// source string plus an identifying label is all it holds.
///
/// Callers bring in the source (via `include_str!` or similar) and
/// register it with the factory through
/// [`LuaInProcessSpawnerFactory::register_lua`].
#[derive(Clone)]
pub struct LuaScriptSource {
    /// The Lua chunk source.
    pub source: String,
    /// Label used in error messages — typically the script's logical id
    /// (for example `"patch_spawner.lua"`).
    pub label: String,
}

impl LuaScriptSource {
    /// Wrap a Lua chunk source and its error-message label.
    pub fn new(source: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            label: label.into(),
        }
    }
}

impl LuaInProcessSpawnerFactory {
    /// Start with no registered scripts and no host bridges.
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
            bridges: HashMap::new(),
        }
    }

    /// Register a host bridge. Subsequent `register_lua` calls snapshot
    /// the current bridge set.
    ///
    /// Ordering rule: register bridges first, then call `register_lua`;
    /// bridges added after `register_lua` will not be visible to that
    /// script.
    pub fn with_bridge(mut self, name: impl Into<String>, bridge: HostBridge) -> Self {
        self.bridges.insert(name.into(), bridge);
        self
    }

    /// Register a **Lua-eval Worker** under `fn_id`.
    ///
    /// Each dispatch spins up a fresh `mlua::Lua` VM, injects globals
    /// (`_PROMPT` / `_AGENT` / `_TASK_ID` / `_ATTEMPT` / `_CTX` — the last
    /// is `_PROMPT` parsed as JSON, or `nil` if that fails), evaluates
    /// the script, and marshals the returned table into a `WorkerResult`.
    ///
    /// Marshalling rules for the return value:
    /// - `{ value = ..., ok = bool }` → `WorkerResult.value` /
    ///   `WorkerResult.ok` verbatim.
    /// - Anything else → `value = <returned value>`, `ok = true`.
    ///
    /// Execution runs on `tokio::task::spawn_blocking` because `mlua::Lua`
    /// is `!Send` and needs to stay away from the tokio async context.
    /// Host bridges (the Lua-to-Rust callback path) previously registered
    /// with [`Self::with_bridge`] are snapshotted at call time and
    /// injected into every dispatch inside `run_lua_worker`.
    pub fn register_lua(mut self, fn_id: impl Into<String>, source: LuaScriptSource) -> Self {
        let source = Arc::new(source);
        let bridges = Arc::new(self.bridges.clone());
        let wrapped: WorkerFn = Arc::new(move |inv| {
            let source = source.clone();
            let bridges = bridges.clone();
            Box::pin(run_lua_worker(source, bridges, inv))
        });
        self.registry.insert(fn_id.into(), wrapped);
        self
    }
}

/// Body of a single Lua-eval invocation (called from `register_lua`).
async fn run_lua_worker(
    source: Arc<LuaScriptSource>,
    bridges: Arc<HashMap<String, HostBridge>>,
    inv: crate::worker::adapter::WorkerInvocation,
) -> Result<crate::worker::adapter::WorkerResult, crate::worker::adapter::WorkerError> {
    use crate::worker::adapter::WorkerError;
    use mlua::LuaSerdeExt;

    let label = source.label.clone();
    let outcome =
        tokio::task::spawn_blocking(move || -> Result<(serde_json::Value, bool), String> {
            let lua = mlua::Lua::new();
            let g = lua.globals();

            // 1. Base globals.
            g.set("_PROMPT", inv.prompt.clone())
                .map_err(|e| format!("set _PROMPT: {e}"))?;
            g.set("_AGENT", inv.agent.clone())
                .map_err(|e| format!("set _AGENT: {e}"))?;
            g.set("_TASK_ID", inv.task_id.to_string())
                .map_err(|e| format!("set _TASK_ID: {e}"))?;
            g.set("_ATTEMPT", inv.attempt as i64)
                .map_err(|e| format!("set _ATTEMPT: {e}"))?;

            // 2. _CTX = JSON parse(_PROMPT); nil on parse failure (co-exists with the plain-string prompt path).
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&inv.prompt) {
                let lua_val = lua
                    .to_value(&json_val)
                    .map_err(|e| format!("_CTX to_value: {e}"))?;
                g.set("_CTX", lua_val)
                    .map_err(|e| format!("set _CTX: {e}"))?;
            }

            // 3. Inject the host bridge (Lua can call `host.<name>(arg)`).
            if !bridges.is_empty() {
                let host = lua
                    .create_table()
                    .map_err(|e| format!("create host table: {e}"))?;
                for (name, bridge) in bridges.iter() {
                    let bridge = bridge.clone();
                    let bname = name.clone();
                    let f = lua
                        .create_function(move |lua, arg: mlua::Value| {
                            let json_arg: serde_json::Value = lua.from_value(arg).map_err(|e| {
                                mlua::Error::external(format!("bridge {bname} arg → json: {e}"))
                            })?;
                            let result_json =
                                bridge.call(json_arg).map_err(mlua::Error::external)?;
                            lua.to_value(&result_json).map_err(|e| {
                                mlua::Error::external(format!("bridge {bname} ret → lua: {e}"))
                            })
                        })
                        .map_err(|e| format!("create_function {name}: {e}"))?;
                    host.set(name.as_str(), f)
                        .map_err(|e| format!("host.{name} set: {e}"))?;
                }
                g.set("host", host).map_err(|e| format!("set host: {e}"))?;
            }

            // 4. eval
            let result: mlua::Value = lua
                .load(&source.source)
                .set_name(&source.label)
                .eval()
                .map_err(|e| format!("lua eval [{}]: {e}", source.label))?;

            // 5. Marshal: shape `{ value=..., ok=true }` or raw value.
            let json_result: serde_json::Value = lua
                .from_value(result)
                .map_err(|e| format!("lua → json [{}]: {e}", source.label))?;

            let (value, ok) = match &json_result {
                serde_json::Value::Object(map)
                    if map.contains_key("value") || map.contains_key("ok") =>
                {
                    let ok = map.get("ok").and_then(|v| v.as_bool()).unwrap_or(true);
                    let value = map.get("value").cloned().unwrap_or(json_result.clone());
                    (value, ok)
                }
                _ => (json_result, true),
            };
            Ok((value, ok))
        })
        .await
        .map_err(|e| WorkerError::Failed(format!("spawn_blocking join [{label}]: {e}")))?
        .map_err(WorkerError::Failed)?;

    Ok(crate::worker::adapter::WorkerResult {
        value: outcome.0,
        ok: outcome.1,
    })
}

impl Default for LuaInProcessSpawnerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SpawnerFactoryKind for LuaInProcessSpawnerFactory {
    const KIND: AgentKind = AgentKind::Lua;
    type Worker = LuaWorker;
}

impl SpawnerFactory for LuaInProcessSpawnerFactory {
    fn build(
        &self,
        agent_def: &AgentDef,
        _hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
        // Inline `spec.source` (a Lua chunk carried by the BP itself) takes
        // precedence over `spec.fn_id`. This is the path a BP author uses to
        // ship a deterministic Lua gate without pre-registering it with the
        // factory — the plumbing (`run_lua_worker` / `LuaScriptSource`) is
        // the same, only the entry point differs.
        if let Some(source) = agent_def.spec.get("source").and_then(|v| v.as_str()) {
            let label = agent_def
                .spec
                .get("label")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| format!("{}.lua", agent_def.name));
            let script = Arc::new(LuaScriptSource::new(source.to_string(), label));
            let bridges = Arc::new(self.bridges.clone());
            let wrapped: WorkerFn = Arc::new(move |inv| {
                let source = script.clone();
                let bridges = bridges.clone();
                Box::pin(run_lua_worker(source, bridges, inv))
            });
            let mut sp: InProcSpawner<LuaWorker> = InProcSpawner::<LuaWorker>::typed();
            sp.registry.insert(agent_def.name.to_string(), wrapped);
            return Ok(Arc::new(sp));
        }
        build_inproc_from_registry::<LuaWorker>(&self.registry, agent_def, "lua")
    }
}

/// Factory for `AgentKind::RustFn`. At `build` time it looks the `fn_id`
/// up in its internal registry and returns an [`InProcSpawner`] with the
/// Rust closure `WorkerFn` registered under `agent_name`.
///
/// Naming convention: `<WorkerIMPL><AdapterType>SpawnerFactory` (RustFn
/// worker on InProcess adapter). Sibling to
/// [`LuaInProcessSpawnerFactory`] — the Lua-worker half of the same
/// split.
///
/// Spec shape:
/// ```jsonc
/// { "fn_id": "echo" }     // Rust closure id pre-registered with the factory
/// ```
pub struct RustFnInProcessSpawnerFactory {
    registry: HashMap<String, WorkerFn>,
}

impl RustFnInProcessSpawnerFactory {
    /// Start with no registered closures.
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
        }
    }

    /// Register a Rust closure `WorkerFn` under `fn_id`, wrapping it so
    /// it matches the `WorkerFn` signature (boxed, pinned future).
    pub fn register_fn<F, Fut>(mut self, fn_id: impl Into<String>, f: F) -> Self
    where
        F: Fn(crate::worker::adapter::WorkerInvocation) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<
                Output = Result<
                    crate::worker::adapter::WorkerResult,
                    crate::worker::adapter::WorkerError,
                >,
            > + Send
            + 'static,
    {
        let f = Arc::new(f);
        let wrapped: WorkerFn = Arc::new(move |inv| {
            let f = f.clone();
            Box::pin(f(inv))
        });
        self.registry.insert(fn_id.into(), wrapped);
        self
    }
}

impl Default for RustFnInProcessSpawnerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SpawnerFactoryKind for RustFnInProcessSpawnerFactory {
    const KIND: AgentKind = AgentKind::RustFn;
    type Worker = RustFnWorker;
}

impl SpawnerFactory for RustFnInProcessSpawnerFactory {
    fn build(
        &self,
        agent_def: &AgentDef,
        _hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
        build_inproc_from_registry::<RustFnWorker>(&self.registry, agent_def, "rust_fn")
    }
}

/// Shared build helper used by both the Lua and the RustFn factories —
/// look `spec.fn_id` up in the registry and return an `InProcSpawner`.
/// The generic type parameter `W` fixes the per-kind Worker concrete
/// type at the type level (the build-site half of the trait's
/// associated-type binding across the four-layer cascade).
fn build_inproc_from_registry<W>(
    registry: &HashMap<String, WorkerFn>,
    agent_def: &AgentDef,
    kind_label: &str,
) -> Result<Arc<dyn SpawnerAdapter>, CompileError>
where
    W: crate::worker::Worker + From<crate::worker::WorkerJoinHandler> + Send + Sync + 'static,
{
    let agent_name = &agent_def.name;
    let spec = &agent_def.spec;
    let invalid = |msg: String| CompileError::InvalidSpec {
        name: agent_name.to_string(),
        msg,
    };
    let fn_id = spec
        .get("fn_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| invalid(format!("{kind_label} spec: 'fn_id' (string) required")))?;
    let f = registry
        .get(fn_id)
        .cloned()
        .ok_or_else(|| invalid(format!("fn_id '{fn_id}' not registered in factory")))?;
    let mut sp: InProcSpawner<W> = InProcSpawner::<W>::typed();
    // Register under `agent_name` (the flow's `Step.ref`). Both
    // `CompiledAgentTable` and the `InProcSpawner` look the function up
    // by name, so the same key is needed at both layers.
    sp.registry.insert(agent_name.to_string(), f);
    Ok(Arc::new(sp))
}

/// Concrete Worker type for the Lua kind — a handle to a Lua-eval task
/// inside an mlua VM. Embeds a `WorkerJoinHandler`. Reserved as the home
/// for future Lua-specific extensions (an mlua VM cancellation
/// mechanism, Lua-side error type retention, and so on).
pub struct LuaWorker {
    /// The join handle / cancellation token for the underlying task.
    pub handler: crate::worker::WorkerJoinHandler,
}

impl From<crate::worker::WorkerJoinHandler> for LuaWorker {
    fn from(handler: crate::worker::WorkerJoinHandler) -> Self {
        Self { handler }
    }
}

#[async_trait::async_trait]
impl crate::worker::Worker for LuaWorker {
    fn id(&self) -> &crate::types::WorkerId {
        &self.handler.worker_id
    }
    fn cancel_token(&self) -> tokio_util::sync::CancellationToken {
        self.handler.cancel.clone()
    }
    async fn join(self: Box<Self>) -> Result<(), crate::worker::adapter::WorkerError> {
        self.handler.await_completion().await
    }
}

/// Concrete Worker type for the RustFn kind — a handle to a task that
/// directly calls a Rust closure. Embeds a `WorkerJoinHandler`. Being a
/// pure function, there is minimal kind-specific extension surface here;
/// the primary purpose is to nail down the type binding.
pub struct RustFnWorker {
    /// The join handle / cancellation token for the underlying task.
    pub handler: crate::worker::WorkerJoinHandler,
}

impl From<crate::worker::WorkerJoinHandler> for RustFnWorker {
    fn from(handler: crate::worker::WorkerJoinHandler) -> Self {
        Self { handler }
    }
}

#[async_trait::async_trait]
impl crate::worker::Worker for RustFnWorker {
    fn id(&self) -> &crate::types::WorkerId {
        &self.handler.worker_id
    }
    fn cancel_token(&self) -> tokio_util::sync::CancellationToken {
        self.handler.cancel.clone()
    }
    async fn join(self: Box<Self>) -> Result<(), crate::worker::adapter::WorkerError> {
        self.handler.await_completion().await
    }
}

/// Factory for `AgentKind::Operator`. Looks up the `Arc<dyn Operator>`
/// pre-registered under `spec.operator_ref` and wraps it in an
/// `OperatorSpawner`. Also resolves `AgentDef.profile.worker_binding` into
/// a `WorkerBinding` at compile time and fails loud (`CompileError::InvalidSpec`)
/// when the resolved operator's `Operator::requires_worker_binding` is `true`
/// and no binding was declared.
///
/// Spec shape:
/// ```jsonc
/// { "operator_ref": "main_ai" }     // Operator id pre-registered with the factory
/// ```
///
/// # Split of responsibilities with `OperatorDelegateMiddleware`
///
/// The two axes exist for different reasons:
///
/// - **This factory (`OperatorSpawnerFactory` → `OperatorSpawner`) — the
///   AgentSpec axis.** Bakes a separate Operator backend into each
///   `AgentDef`. A `kind = Operator` `AgentDef` names its backend through
///   `spec.operator_ref`; at `compile()` time the `Arc<dyn Operator>` is
///   baked into `routes[agent_name]`. Because the `agent.md` loader
///   (`agent_md_loader`) defaults `kind` to `Operator`, agents that flow
///   in through agent-profiles land here.
///
/// - **`OperatorDelegateMiddleware` — the Blueprint-global (session)
///   axis.** Delegates every agent to the same Operator backend. At
///   session-attach time you call `engine.register_operator(id, op)`
///   plus `attach_with_ids(.., operator_backend_id = Some(id))` to bind
///   it session-wide, and declare
///   `spawner_hints.layers = ["operator_delegate"]` to opt in. `ctx.agent`
///   is ignored; the operator handles every spawn in that session (a
///   MainAI-wide driver, a human-wide console, that sort of thing).
///
/// # Exclusivity (a double fire is structurally impossible)
///
/// When both are effective — the hint is declared, the session has an
/// operator backend, **and** the Blueprint has a `kind = Operator`
/// `AgentDef` — `OperatorDelegateMiddleware` sits at the outer end of
/// the stack and **completely bypasses** `inner.spawn`. The
/// `OperatorSpawner` is never reached, so under those conditions this
/// factory's routes entry is inert. This is not a double fire — the
/// session axis is overriding the agent axis. Consistent usage means
/// picking one axis per use case.
///
/// Interior mutability is provided by an `Arc<RwLock>`. Even after the
/// factory has been stored as `Arc<dyn SpawnerFactory>` in
/// `SpawnerRegistry`, a caller holding an `Arc` clone can still add
/// Operator backends dynamically via `register_operator(&self, id, op)`.
/// Typical uses: registering a `WSOperatorSession` under the session id
/// on WebSocket connect, binding agents that arrive via the `agent.md`
/// loader to arbitrary backends, and so on. `build()` performs a
/// `read()` lookup each time.
pub struct OperatorSpawnerFactory {
    operators: Arc<std::sync::RwLock<HashMap<String, Arc<dyn Operator>>>>,
}

impl OperatorSpawnerFactory {
    /// Start with no registered Operator backends.
    pub fn new() -> Self {
        Self {
            operators: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Register an Operator backend dynamically through `&self`.
    /// Overwrites are allowed — later wins. Callers can still reach this
    /// after the factory has been stored as `Arc<dyn SpawnerFactory>` in
    /// `SpawnerRegistry`, as long as they hold an `Arc` clone; interior
    /// mutability is provided by the inner `RwLock`.
    pub fn register_operator(&self, id: impl Into<String>, op: Arc<dyn Operator>) -> &Self {
        self.operators
            .write()
            .expect("OperatorSpawnerFactory.operators RwLock poisoned")
            .insert(id.into(), op);
        self
    }

    /// Dynamically unregister an id (used to clean up when a WebSocket
    /// disconnects, for example). A missing id is a no-op.
    pub fn unregister_operator(&self, id: &str) -> &Self {
        self.operators
            .write()
            .expect("OperatorSpawnerFactory.operators RwLock poisoned")
            .remove(id);
        self
    }
}

impl Default for OperatorSpawnerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SpawnerFactoryKind for OperatorSpawnerFactory {
    const KIND: AgentKind = AgentKind::Operator;
    type Worker = crate::operator::OperatorWorker;
}

impl SpawnerFactory for OperatorSpawnerFactory {
    fn build(
        &self,
        agent_def: &AgentDef,
        _hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
        let agent_name = &agent_def.name;
        let spec = &agent_def.spec;
        // Bake AgentDef.profile.system_prompt into the OperatorSpawner at compile time.
        // `Some` → adopted first at spawn time; `None` → falls back to fetch_prompt (initial_directive).
        // Fallback path. Sibling: AgentBlockInProcessSpawnerFactory
        // (agent_block/runtime.rs) does the same compile-time bake by stuffing
        // the profile into BlockConfig.context.
        let system_prompt = agent_def.profile.as_ref().map(|p| p.system_prompt.clone());
        let invalid = |msg: String| CompileError::InvalidSpec {
            name: agent_name.to_string(),
            msg,
        };
        let op_ref = spec
            .get("operator_ref")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid("operator spec: 'operator_ref' (string) required".into()))?;
        let operators = self
            .operators
            .read()
            .expect("OperatorSpawnerFactory.operators RwLock poisoned");
        let op = operators.get(op_ref).cloned().ok_or_else(|| {
            let mut names: Vec<String> = operators.keys().cloned().collect();
            names.sort();
            let names_list = if names.is_empty() {
                "<none>".to_string()
            } else {
                names.join(", ")
            };
            invalid(format!(
                "operator_ref '{op_ref}' not registered in factory. \
                 Registered sids: [{names_list}]. \
                 Hint: call mse_operator_join(roles=[...]) to mint the sid first."
            ))
        })?;
        drop(operators);

        // Resolve the Blueprint-baked worker binding from
        // `AgentDef.profile.worker_binding` — the SoT for the
        // declaration↔executor binding (see `WorkerBinding` doc). Fail
        // loud at compile time when the operator backend requires one
        // and the Blueprint didn't declare it; this is a compile-time
        // gate, not a runtime guess.
        let worker_binding = agent_def
            .profile
            .as_ref()
            .and_then(|p| p.worker_binding.as_ref())
            .map(|variant| WorkerBinding {
                variant: variant.clone(),
                tools: agent_def
                    .profile
                    .as_ref()
                    .map(|p| p.tools.clone())
                    .unwrap_or_default(),
            });
        if op.requires_worker_binding() && worker_binding.is_none() {
            // Issue #9: the two Blueprint authoring paths (direct JSON
            // and `$agent_md` file ref) both land here. Old message
            // pointed only at the `.md` frontmatter, which was
            // confusing for authors on the JSON-direct path.
            return Err(invalid(
                "profile.worker_binding is required for this operator backend. \
                 Fix by either: \
                 (a) if authoring the Blueprint JSON directly, add \
                 `agents[N].profile.worker_binding: \"<subagent-type>\"` \
                 to the JSON literal; or \
                 (b) if using an $agent_md file ref, add \
                 `worker_binding: <subagent-type>` to the agent .md frontmatter."
                    .into(),
            ));
        }
        Ok(Arc::new(OperatorSpawner::new(
            op,
            system_prompt,
            worker_binding,
        )))
    }
}

#[cfg(test)]
mod operator_spawner_factory_worker_binding_tests {
    use super::*;
    use crate::blueprint::AgentProfile;
    use crate::core::ctx::Ctx;
    use crate::types::CapToken;
    use crate::worker::adapter::{WorkerError, WorkerResult};

    /// Minimal `Operator` stub whose `requires_worker_binding` is
    /// configurable — enough to exercise the compile-time fail-loud gate
    /// without standing up a real backend (e.g. `WSOperatorSession`,
    /// which lives in a downstream crate).
    struct StubOperator {
        requires_binding: bool,
    }

    #[async_trait]
    impl Operator for StubOperator {
        async fn execute(
            &self,
            _ctx: &Ctx,
            _system: Option<String>,
            _prompt: Value,
            _worker: Option<WorkerBinding>,
            _worker_token: CapToken,
        ) -> Result<WorkerResult, WorkerError> {
            Ok(WorkerResult {
                value: Value::Null,
                ok: true,
            })
        }

        fn requires_worker_binding(&self) -> bool {
            self.requires_binding
        }
    }

    fn agent_def_with(profile: Option<AgentProfile>) -> AgentDef {
        AgentDef {
            name: "test-agent".to_string(),
            kind: AgentKind::Operator,
            spec: serde_json::json!({ "operator_ref": "op1" }),
            profile,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
        }
    }

    #[test]
    fn build_fails_loud_when_binding_required_but_absent() {
        let factory = OperatorSpawnerFactory::new();
        factory.register_operator(
            "op1",
            Arc::new(StubOperator {
                requires_binding: true,
            }) as Arc<dyn Operator>,
        );
        let def = agent_def_with(Some(AgentProfile::default()));
        match factory.build(&def, None) {
            Err(CompileError::InvalidSpec { name, msg }) => {
                assert_eq!(name, "test-agent");
                assert!(
                    msg.contains("worker_binding is required"),
                    "unexpected message: {msg}"
                );
                // Issue #9: the message must be actionable for both
                // authoring paths — the JSON-direct hint and the
                // $agent_md hint both surface.
                assert!(
                    msg.contains("agents[N].profile.worker_binding"),
                    "message missing JSON-direct hint (issue #9): {msg}"
                );
                assert!(
                    msg.contains("agent .md frontmatter"),
                    "message missing $agent_md hint: {msg}"
                );
            }
            Err(other) => panic!("expected InvalidSpec, got: {other:?}"),
            Ok(_) => panic!("expected compile-time failure, got Ok"),
        }
    }

    #[test]
    fn build_succeeds_when_binding_required_and_present() {
        let factory = OperatorSpawnerFactory::new();
        factory.register_operator(
            "op1",
            Arc::new(StubOperator {
                requires_binding: true,
            }) as Arc<dyn Operator>,
        );
        let profile = AgentProfile {
            worker_binding: Some("mse-worker-coder".to_string()),
            tools: vec!["Read".to_string(), "Edit".to_string()],
            ..Default::default()
        };
        let def = agent_def_with(Some(profile));
        assert!(
            factory.build(&def, None).is_ok(),
            "expected Ok when worker_binding is declared"
        );
    }

    #[test]
    fn build_succeeds_when_binding_not_required_and_absent() {
        let factory = OperatorSpawnerFactory::new();
        factory.register_operator(
            "op1",
            Arc::new(StubOperator {
                requires_binding: false,
            }) as Arc<dyn Operator>,
        );
        let def = agent_def_with(Some(AgentProfile::default()));
        assert!(
            factory.build(&def, None).is_ok(),
            "backends that don't require a binding must not be gated by its absence"
        );
    }
}

// ─── LuaInProcessSpawnerFactory: inline `spec.source` support ─────────────
//
// Issue `ab3d1145`: BPs served by `mse serve` couldn't declare `kind: lua`
// without pre-registering a `fn_id` on the factory. These tests cover the
// new inline path — `spec.source = "<lua chunk>"` (optionally with `label`)
// wraps a fresh `LuaScriptSource` at `build` time and runs it through the
// same `run_lua_worker` plumbing as the registry path.
#[cfg(test)]
mod lua_inline_source_tests {
    use super::*;
    use crate::types::{CapToken, Role, StepId};

    fn agent(name: &str, spec: Value) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::Lua,
            spec,
            profile: None,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
        }
    }

    fn test_invocation(prompt: &str) -> crate::worker::adapter::WorkerInvocation {
        crate::worker::adapter::WorkerInvocation {
            token: CapToken {
                agent_id: "a".into(),
                role: Role::Worker,
                scopes: vec!["*".into()],
                issued_at: 0,
                expire_at: u64::MAX / 2,
                max_uses: None,
                nonce: "test-nonce".into(),
                sig_hex: "".into(),
            },
            task_id: StepId::parse("ST-test").expect("StepId parse"),
            attempt: 1,
            agent: "g".into(),
            prompt: prompt.into(),
            sink: None,
            cancel_token: None,
        }
    }

    #[test]
    fn build_accepts_inline_source_without_pre_registration() {
        let factory = LuaInProcessSpawnerFactory::new();
        let def = agent(
            "g",
            serde_json::json!({ "source": "return { value = 42, ok = true }" }),
        );
        assert!(
            factory.build(&def, None).is_ok(),
            "inline spec.source must build without a pre-registered fn_id"
        );
    }

    #[test]
    fn build_rejects_when_neither_source_nor_fn_id_is_present() {
        let factory = LuaInProcessSpawnerFactory::new();
        let def = agent("g", serde_json::json!({}));
        match factory.build(&def, None) {
            Err(CompileError::InvalidSpec { msg, .. }) => {
                assert!(
                    msg.contains("fn_id"),
                    "empty spec must still surface the fn_id-required message: {msg}"
                );
            }
            Err(other) => panic!("expected InvalidSpec, got a different CompileError: {other}"),
            // `SpawnerAdapter` is not Debug, so we can't `unwrap_err()` /
            // pattern-print the Ok arm — describe the mismatch directly.
            Ok(_) => panic!("expected InvalidSpec, got Ok(SpawnerAdapter)"),
        }
    }

    /// The inline path shares `run_lua_worker` with the registry path, so
    /// exercising the marshaller once through it is enough to prove the
    /// wrap is faithful.
    #[tokio::test]
    async fn inline_source_evaluates_and_marshals_result() {
        let source =
            LuaScriptSource::new("return { value = _PROMPT .. '!', ok = true }", "smoke.lua");
        let out = run_lua_worker(
            std::sync::Arc::new(source),
            std::sync::Arc::new(HashMap::new()),
            test_invocation("hello"),
        )
        .await
        .expect("lua worker ok");
        assert_eq!(out.value, serde_json::json!("hello!"));
        assert!(out.ok);
    }

    #[tokio::test]
    async fn inline_source_can_signal_agent_level_failure() {
        // Deterministic gate pattern: return `ok = false` to flip the
        // dispatch outcome to `Blocked` (the flow.ir Try catch path).
        let source = LuaScriptSource::new("return { value = 'nope', ok = false }", "gate.lua");
        let out = run_lua_worker(
            std::sync::Arc::new(source),
            std::sync::Arc::new(HashMap::new()),
            test_invocation("input"),
        )
        .await
        .expect("lua worker ok");
        assert_eq!(out.value, serde_json::json!("nope"));
        assert!(!out.ok);
    }
}

// ─── GH #21 Phase 2: `Blueprint.metas` / `AgentMeta.meta_ref` / static
// `$step_meta.ref` compile-time validation ─────────────────────────────────
#[cfg(test)]
mod meta_ref_validation_tests {
    use super::*;
    use crate::blueprint::{AgentMeta, MetaDef};
    use crate::worker::adapter::WorkerResult;

    fn registry_with_echo() -> SpawnerRegistry {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: Value::String(inv.prompt),
                ok: true,
            })
        });
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));
        reg
    }

    fn rustfn_agent(name: &str) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::RustFn,
            spec: serde_json::json!({ "fn_id": "echo" }),
            profile: None,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
        }
    }

    fn simple_flow(agent_ref: &str, in_: Expr) -> FlowNode {
        FlowNode::Step {
            ref_: agent_ref.to_string(),
            in_,
            out: Expr::Path {
                at: "$.output".parse().expect("literal test path: $.output"),
            },
        }
    }

    fn minimal_bp(agents: Vec<AgentDef>, metas: Vec<MetaDef>, flow: FlowNode) -> Blueprint {
        Blueprint {
            schema_version: crate::blueprint::current_schema_version(),
            id: "meta-ref-ut".into(),
            flow,
            agents,
            operators: vec![],
            metas,
            hints: Default::default(),
            strategy: Default::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            check_policy: None,
        }
    }

    #[test]
    fn valid_meta_ref_compiles() {
        let mut agent = rustfn_agent("worker");
        agent.meta = Some(AgentMeta {
            meta_ref: Some("shared".to_string()),
            ..Default::default()
        });
        let bp = minimal_bp(
            vec![agent],
            vec![MetaDef {
                name: "shared".into(),
                ctx: serde_json::json!({ "k": "v" }),
            }],
            simple_flow(
                "worker",
                Expr::Path {
                    at: "$.input".parse().expect("literal test path: $.input"),
                },
            ),
        );
        let compiler = Compiler::new(registry_with_echo());
        assert!(
            compiler.compile(&bp).is_ok(),
            "a resolvable AgentMeta.meta_ref must compile"
        );
    }

    #[test]
    fn unknown_agent_meta_ref_is_unresolved_meta_ref() {
        let mut agent = rustfn_agent("worker");
        agent.meta = Some(AgentMeta {
            meta_ref: Some("missing".to_string()),
            ..Default::default()
        });
        let bp = minimal_bp(
            vec![agent],
            vec![],
            simple_flow(
                "worker",
                Expr::Path {
                    at: "$.input".parse().expect("literal test path: $.input"),
                },
            ),
        );
        let compiler = Compiler::new(registry_with_echo());
        match compiler.compile(&bp) {
            Err(CompileError::UnresolvedMetaRef {
                where_,
                meta_ref,
                defined,
            }) => {
                assert!(
                    where_.contains("worker"),
                    "where_ must name the agent: {where_}"
                );
                assert_eq!(meta_ref, "missing");
                assert!(defined.is_empty());
            }
            Err(other) => {
                panic!("expected UnresolvedMetaRef, got a different CompileError: {other}")
            }
            Ok(_) => panic!("expected compile-time failure, got Ok"),
        }
    }

    #[test]
    fn unknown_static_step_meta_ref_in_lit_is_unresolved_meta_ref() {
        let agent = rustfn_agent("worker");
        let in_ = Expr::Lit {
            value: serde_json::json!({ "$step_meta": { "ref": "missing" }, "$in": "go" }),
        };
        let bp = minimal_bp(vec![agent], vec![], simple_flow("worker", in_));
        let compiler = Compiler::new(registry_with_echo());
        match compiler.compile(&bp) {
            Err(CompileError::UnresolvedMetaRef {
                where_, meta_ref, ..
            }) => {
                assert!(
                    where_.contains("worker"),
                    "where_ must name the offending step: {where_}"
                );
                assert_eq!(meta_ref, "missing");
            }
            Err(other) => {
                panic!("expected UnresolvedMetaRef, got a different CompileError: {other}")
            }
            Ok(_) => panic!("expected compile-time failure, got Ok"),
        }
    }

    #[test]
    fn path_op_input_with_no_static_envelope_compiles_fine() {
        let agent = rustfn_agent("worker");
        let bp = minimal_bp(
            vec![agent],
            vec![],
            simple_flow(
                "worker",
                Expr::Path {
                    at: "$.input".parse().expect("literal test path: $.input"),
                },
            ),
        );
        let compiler = Compiler::new(registry_with_echo());
        assert!(
            compiler.compile(&bp).is_ok(),
            "a non-Lit Step.in must not trigger the best-effort static $step_meta check"
        );
    }
}

// ─── GH #34: `Blueprint.audits[].agent` compile-time validation ────────────
#[cfg(test)]
mod audit_agent_validation_tests {
    use super::*;
    use crate::worker::adapter::WorkerResult;
    use mlua_swarm_schema::{AuditDef, AuditMode};

    fn registry_with_echo() -> SpawnerRegistry {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: Value::String(inv.prompt),
                ok: true,
            })
        });
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));
        reg
    }

    fn rustfn_agent(name: &str) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::RustFn,
            spec: serde_json::json!({ "fn_id": "echo" }),
            profile: None,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
        }
    }

    fn minimal_bp(agents: Vec<AgentDef>, audits: Vec<AuditDef>) -> Blueprint {
        Blueprint {
            schema_version: crate::blueprint::current_schema_version(),
            id: "audit-ref-ut".into(),
            flow: FlowNode::Step {
                ref_: "worker".to_string(),
                in_: Expr::Path {
                    at: "$.input".parse().expect("literal test path: $.input"),
                },
                out: Expr::Path {
                    at: "$.output".parse().expect("literal test path: $.output"),
                },
            },
            agents,
            operators: vec![],
            metas: vec![],
            hints: Default::default(),
            strategy: Default::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits,
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            check_policy: None,
        }
    }

    #[test]
    fn unresolved_audit_agent_is_a_loud_compile_error() {
        let bp = minimal_bp(
            vec![rustfn_agent("worker")],
            vec![AuditDef {
                agent: "missing-auditor".to_string(),
                steps: None,
                mode: AuditMode::default(),
            }],
        );
        let compiler = Compiler::new(registry_with_echo());
        match compiler.compile(&bp) {
            Err(CompileError::UnresolvedAuditAgent { agent, defined }) => {
                assert_eq!(agent, "missing-auditor");
                assert_eq!(defined, vec!["worker".to_string()]);
            }
            Err(other) => {
                panic!("expected UnresolvedAuditAgent, got a different CompileError: {other}")
            }
            Ok(_) => panic!("expected compile-time failure, got Ok"),
        }
    }

    #[test]
    fn resolved_audit_agent_compiles_fine() {
        let bp = minimal_bp(
            vec![rustfn_agent("worker"), rustfn_agent("auditor")],
            vec![AuditDef {
                agent: "auditor".to_string(),
                steps: None,
                mode: AuditMode::default(),
            }],
        );
        let compiler = Compiler::new(registry_with_echo());
        assert!(
            compiler.compile(&bp).is_ok(),
            "an audits[].agent that names a declared AgentDef must compile"
        );
    }
}

// ─── GH #27 (follow-up to #23): `Blueprint.projection_placement` compile-time
// validation + `CompiledBlueprint.projection_placement` construction ────────
#[cfg(test)]
mod projection_placement_compile_tests {
    use super::*;
    use crate::core::projection_placement::{ProjectionPlacement, RootPreference};
    use crate::worker::adapter::WorkerResult;
    use mlua_swarm_schema::ProjectionPlacementSpec;

    fn registry_with_echo() -> SpawnerRegistry {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: Value::String(inv.prompt),
                ok: true,
            })
        });
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));
        reg
    }

    fn minimal_bp(projection_placement: Option<ProjectionPlacementSpec>) -> Blueprint {
        Blueprint {
            schema_version: crate::blueprint::current_schema_version(),
            id: "projection-placement-ut".into(),
            flow: FlowNode::Step {
                ref_: "worker".to_string(),
                in_: Expr::Path {
                    at: "$.input".parse().expect("literal test path: $.input"),
                },
                out: Expr::Path {
                    at: "$.output".parse().expect("literal test path: $.output"),
                },
            },
            agents: vec![AgentDef {
                name: "worker".to_string(),
                kind: AgentKind::RustFn,
                spec: serde_json::json!({ "fn_id": "echo" }),
                profile: None,
                meta: None,
                runner: None,
                runner_ref: None,
                verdict: None,
            }],
            operators: vec![],
            metas: vec![],
            hints: Default::default(),
            strategy: Default::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            check_policy: None,
        }
    }

    #[test]
    fn undeclared_projection_placement_compiles_to_byte_compat_default() {
        let bp = minimal_bp(None);
        let compiled = Compiler::new(registry_with_echo())
            .compile(&bp)
            .expect("undeclared projection_placement compiles");
        assert_eq!(
            *compiled.projection_placement,
            ProjectionPlacement::default()
        );
    }

    #[test]
    fn declared_valid_projection_placement_compiles_to_matching_resolver() {
        let bp = minimal_bp(Some(ProjectionPlacementSpec {
            root: Some("project_root".to_string()),
            dir_template: Some("custom/{task_id}/out".to_string()),
        }));
        let compiled = Compiler::new(registry_with_echo())
            .compile(&bp)
            .expect("valid projection_placement compiles");
        assert_eq!(
            compiled.projection_placement.root_preference,
            RootPreference::ProjectRoot
        );
        assert_eq!(
            compiled.projection_placement.dir_template,
            "custom/{task_id}/out"
        );
    }

    #[test]
    fn declared_invalid_dir_template_rejects_compile() {
        let bp = minimal_bp(Some(ProjectionPlacementSpec {
            root: None,
            dir_template: Some("workspace/tasks/ctx".to_string()), // missing {task_id}
        }));
        match Compiler::new(registry_with_echo()).compile(&bp) {
            Err(CompileError::InvalidProjectionPlacement(_)) => {}
            Err(other) => {
                panic!("expected InvalidProjectionPlacement, got a different CompileError: {other}")
            }
            Ok(_) => {
                panic!("expected compile-time rejection for a missing {{task_id}} placeholder")
            }
        }
    }

    #[test]
    fn declared_invalid_root_literal_rejects_compile() {
        let bp = minimal_bp(Some(ProjectionPlacementSpec {
            root: Some("nope".to_string()),
            dir_template: None,
        }));
        match Compiler::new(registry_with_echo()).compile(&bp) {
            Err(CompileError::InvalidProjectionPlacement(_)) => {}
            Err(other) => {
                panic!("expected InvalidProjectionPlacement, got a different CompileError: {other}")
            }
            Ok(_) => panic!("expected compile-time rejection for an invalid root literal"),
        }
    }
}

// ─── GH #50: `Blueprint.agents[].verdict` cond↔output-shape lint ──────────
#[cfg(test)]
mod verdict_contract_lint_tests {
    use super::*;
    use crate::worker::adapter::WorkerResult;

    fn registry_with_echo() -> SpawnerRegistry {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: Value::String(inv.prompt),
                ok: true,
            })
        });
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));
        reg
    }

    fn gate_agent(verdict: Option<VerdictContract>) -> AgentDef {
        AgentDef {
            name: "gate".to_string(),
            kind: AgentKind::RustFn,
            spec: serde_json::json!({ "fn_id": "echo" }),
            profile: None,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict,
        }
    }

    fn minimal_bp(agent: AgentDef, flow: FlowNode) -> Blueprint {
        Blueprint {
            schema_version: crate::blueprint::current_schema_version(),
            id: "verdict-contract-ut".into(),
            flow,
            agents: vec![agent],
            operators: vec![],
            metas: vec![],
            hints: Default::default(),
            strategy: Default::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            check_policy: None,
        }
    }

    fn step(ref_: &str, out_path: &str) -> FlowNode {
        FlowNode::Step {
            ref_: ref_.to_string(),
            in_: Expr::Lit { value: Value::Null },
            out: Expr::Path {
                at: out_path.parse().expect("literal test path"),
            },
        }
    }

    fn noop() -> FlowNode {
        FlowNode::Seq { children: vec![] }
    }

    fn eq_cond(path: &str, lit: &str) -> Expr {
        Expr::Eq {
            lhs: Box::new(Expr::Path {
                at: path.parse().expect("literal test path"),
            }),
            rhs: Box::new(Expr::Lit {
                value: Value::String(lit.to_string()),
            }),
        }
    }

    fn branch(cond: Expr, then_: FlowNode, else_: FlowNode) -> FlowNode {
        FlowNode::Branch {
            cond,
            then_: Box::new(then_),
            else_: Box::new(else_),
        }
    }

    fn body_contract(values: &[&str]) -> VerdictContract {
        VerdictContract {
            channel: VerdictChannel::Body,
            values: values.iter().map(|v| v.to_string()).collect(),
        }
    }

    fn part_contract(values: &[&str]) -> VerdictContract {
        VerdictContract {
            channel: VerdictChannel::Part,
            values: values.iter().map(|v| v.to_string()).collect(),
        }
    }

    #[test]
    fn contract_with_correct_body_channel_and_value_compiles() {
        let agent = gate_agent(Some(body_contract(&["PASS", "BLOCKED"])));
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.verdict"),
                branch(eq_cond("$.verdict", "BLOCKED"), noop(), noop()),
            ],
        };
        let bp = minimal_bp(agent, flow);
        assert!(
            Compiler::new(registry_with_echo()).compile(&bp).is_ok(),
            "a cond addressing the bare step output must match a channel: \"body\" contract"
        );
    }

    #[test]
    fn contract_with_correct_part_channel_and_value_compiles() {
        let agent = gate_agent(Some(part_contract(&["PASS", "BLOCKED"])));
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.gate"),
                branch(eq_cond("$.gate.parts.verdict", "BLOCKED"), noop(), noop()),
            ],
        };
        let bp = minimal_bp(agent, flow);
        assert!(
            Compiler::new(registry_with_echo()).compile(&bp).is_ok(),
            "a cond addressing '<step>.parts.verdict' must match a channel: \"part\" contract"
        );
    }

    #[test]
    fn body_channel_contract_rejects_cond_addressing_parts_verdict() {
        // Pattern A declared (channel: "body") but the cond addresses the
        // Pattern B shape ('$.gate.parts.verdict') instead of the bare
        // step output — GH #50 register-time enforcement point 1.
        let agent = gate_agent(Some(body_contract(&["PASS", "BLOCKED"])));
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.gate"),
                branch(eq_cond("$.gate.parts.verdict", "BLOCKED"), noop(), noop()),
            ],
        };
        let bp = minimal_bp(agent, flow);
        match Compiler::new(registry_with_echo()).compile(&bp) {
            Err(CompileError::VerdictChannelMismatch {
                where_,
                agent,
                expected_channel,
                actual_shape,
            }) => {
                assert_eq!(agent, "gate");
                assert_eq!(expected_channel, "body");
                assert_eq!(actual_shape, "part");
                assert!(where_.contains("Branch cond"), "where_: {where_}");
            }
            Err(other) => {
                panic!("expected VerdictChannelMismatch, got a different CompileError: {other}")
            }
            Ok(_) => panic!("expected compile-time rejection for the wrong channel shape"),
        }
    }

    #[test]
    fn part_channel_contract_rejects_cond_addressing_bare_output() {
        // Inverse of the previous case: channel: "part" declared, but the
        // cond addresses the bare step output.
        let agent = gate_agent(Some(part_contract(&["PASS", "BLOCKED"])));
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.verdict"),
                branch(eq_cond("$.verdict", "BLOCKED"), noop(), noop()),
            ],
        };
        let bp = minimal_bp(agent, flow);
        match Compiler::new(registry_with_echo()).compile(&bp) {
            Err(CompileError::VerdictChannelMismatch {
                agent,
                expected_channel,
                actual_shape,
                ..
            }) => {
                assert_eq!(agent, "gate");
                assert_eq!(expected_channel, "part");
                assert_eq!(actual_shape, "body");
            }
            Err(other) => {
                panic!("expected VerdictChannelMismatch, got a different CompileError: {other}")
            }
            Ok(_) => panic!("expected compile-time rejection for the wrong channel shape"),
        }
    }

    #[test]
    fn contract_rejects_lit_outside_declared_values() {
        let agent = gate_agent(Some(body_contract(&["PASS", "BLOCKED"])));
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.verdict"),
                branch(eq_cond("$.verdict", "UNKNOWN"), noop(), noop()),
            ],
        };
        let bp = minimal_bp(agent, flow);
        match Compiler::new(registry_with_echo()).compile(&bp) {
            Err(CompileError::VerdictValueNotInContract {
                agent,
                value,
                values,
                ..
            }) => {
                assert_eq!(agent, "gate");
                assert_eq!(value, "UNKNOWN");
                assert_eq!(values, vec!["PASS".to_string(), "BLOCKED".to_string()]);
            }
            Err(other) => {
                panic!("expected VerdictValueNotInContract, got a different CompileError: {other}")
            }
            Ok(_) => panic!("expected compile-time rejection for a Lit outside declared values"),
        }
    }

    #[test]
    fn undeclared_agent_referenced_by_cond_compiles_with_warning_only() {
        let agent = gate_agent(None);
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.verdict"),
                branch(eq_cond("$.verdict", "BLOCKED"), noop(), noop()),
            ],
        };
        let bp = minimal_bp(agent, flow);
        assert!(
            Compiler::new(registry_with_echo()).compile(&bp).is_ok(),
            "an undeclared verdict contract must never reject compile (opt-in, back-compat)"
        );
    }

    #[test]
    fn in_expr_with_lit_haystack_members_compiles() {
        let agent = gate_agent(Some(body_contract(&["PASS", "BLOCKED"])));
        let cond = Expr::In {
            needle: Box::new(Expr::Path {
                at: "$.verdict".parse().expect("literal test path"),
            }),
            haystack: Box::new(Expr::Lit {
                value: serde_json::json!(["PASS", "BLOCKED"]),
            }),
        };
        let flow = FlowNode::Seq {
            children: vec![step("gate", "$.verdict"), branch(cond, noop(), noop())],
        };
        let bp = minimal_bp(agent, flow);
        assert!(
            Compiler::new(registry_with_echo()).compile(&bp).is_ok(),
            "an `In` haystack whose every Lit is a declared value must compile"
        );
    }

    /// GH #50 follow-up (issue `33bc825b`): opt-in strict mode rejects a
    /// Blueprint whose declared `verdict.values` set includes at least one
    /// entry that no downstream `Branch`/`Loop` `cond` references. The
    /// contract declares `["PASS", "BLOCKED"]` but only "BLOCKED" is
    /// referenced by the cond → "PASS" is unhandled → `CompileError::
    /// VerdictValueUnhandled` under `strict_verdict_handling: Some(true)`.
    #[test]
    fn strict_mode_rejects_unhandled_declared_value() {
        let agent = gate_agent(Some(body_contract(&["PASS", "BLOCKED"])));
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.verdict"),
                branch(eq_cond("$.verdict", "BLOCKED"), noop(), noop()),
            ],
        };
        let mut bp = minimal_bp(agent, flow);
        bp.metadata.strict_verdict_handling = Some(true);
        match Compiler::new(registry_with_echo()).compile(&bp) {
            Err(CompileError::VerdictValueUnhandled {
                agent,
                value,
                declared_values,
                step_ref,
            }) => {
                assert_eq!(agent, "gate");
                assert_eq!(value, "PASS");
                assert_eq!(
                    declared_values,
                    vec!["PASS".to_string(), "BLOCKED".to_string()]
                );
                assert_eq!(step_ref, "gate");
            }
            Err(other) => {
                panic!("expected VerdictValueUnhandled, got a different CompileError: {other}")
            }
            Ok(_) => panic!(
                "expected compile-time rejection for a declared verdict value with no \
                 downstream handler under strict_verdict_handling=Some(true)"
            ),
        }
    }

    /// GH #50 follow-up (issue `33bc825b`): default mode (i.e.
    /// `strict_verdict_handling` absent or `Some(false)`) surfaces
    /// unhandled declared values via `tracing::warn!` only — the compile
    /// still succeeds. This preserves back-compat with GH #50's original
    /// test cases (many of which declare `values = ["PASS", "BLOCKED"]`
    /// and cond-reference only one).
    #[test]
    fn default_mode_permits_unhandled_declared_value() {
        let agent = gate_agent(Some(body_contract(&["PASS", "BLOCKED"])));
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.verdict"),
                branch(eq_cond("$.verdict", "BLOCKED"), noop(), noop()),
            ],
        };
        let bp = minimal_bp(agent, flow);
        // `strict_verdict_handling` left as `None` (default)
        assert!(
            Compiler::new(registry_with_echo()).compile(&bp).is_ok(),
            "default mode must never reject a Blueprint for unhandled declared values \
             (opt-in, back-compat with GH #50)"
        );
    }

    /// GH #50 follow-up (issue `33bc825b`): under strict mode, when every
    /// declared value is referenced by at least one downstream cond, the
    /// compile succeeds. This tests the positive path of the reverse-
    /// direction lint.
    #[test]
    fn strict_mode_accepts_all_declared_values_handled() {
        let agent = gate_agent(Some(body_contract(&["PASS", "BLOCKED"])));
        // Two branches, each cond referencing one declared value —
        // together they cover the full `values` set.
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.verdict"),
                branch(eq_cond("$.verdict", "BLOCKED"), noop(), noop()),
                branch(eq_cond("$.verdict", "PASS"), noop(), noop()),
            ],
        };
        let mut bp = minimal_bp(agent, flow);
        bp.metadata.strict_verdict_handling = Some(true);
        assert!(
            Compiler::new(registry_with_echo()).compile(&bp).is_ok(),
            "strict mode must accept a Blueprint that handles every declared value"
        );
    }

    /// GH #50 follow-up (issue `33bc825b`): under strict mode, an `In`
    /// cond whose `Lit` haystack lists every declared value satisfies
    /// the handler-coverage check in one go.
    #[test]
    fn strict_mode_accepts_declared_values_covered_by_in_expr() {
        let agent = gate_agent(Some(body_contract(&["PASS", "BLOCKED"])));
        let cond = Expr::In {
            needle: Box::new(Expr::Path {
                at: "$.verdict".parse().expect("literal test path"),
            }),
            haystack: Box::new(Expr::Lit {
                value: serde_json::json!(["PASS", "BLOCKED"]),
            }),
        };
        let flow = FlowNode::Seq {
            children: vec![step("gate", "$.verdict"), branch(cond, noop(), noop())],
        };
        let mut bp = minimal_bp(agent, flow);
        bp.metadata.strict_verdict_handling = Some(true);
        assert!(
            Compiler::new(registry_with_echo()).compile(&bp).is_ok(),
            "strict mode must accept an `In` haystack that covers every declared value"
        );
    }

    /// GH #50 follow-up (issue `33bc825b`): under strict mode, a `part`
    /// channel contract with unhandled declared value is rejected the same
    /// way as the `body` channel case. Confirms channel-agnostic coverage.
    #[test]
    fn strict_mode_rejects_unhandled_part_channel_value() {
        let agent = gate_agent(Some(part_contract(&["PASS", "BLOCKED"])));
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.gate"),
                branch(eq_cond("$.gate.parts.verdict", "BLOCKED"), noop(), noop()),
            ],
        };
        let mut bp = minimal_bp(agent, flow);
        bp.metadata.strict_verdict_handling = Some(true);
        match Compiler::new(registry_with_echo()).compile(&bp) {
            Err(CompileError::VerdictValueUnhandled {
                agent,
                value,
                step_ref,
                ..
            }) => {
                assert_eq!(agent, "gate");
                assert_eq!(value, "PASS");
                assert_eq!(step_ref, "gate");
            }
            Err(other) => {
                panic!("expected VerdictValueUnhandled, got a different CompileError: {other}")
            }
            Ok(_) => panic!(
                "expected compile-time rejection for a declared verdict value with no \
                 downstream handler (part channel) under strict_verdict_handling=Some(true)"
            ),
        }
    }

    /// Acceptance criterion #7 (5th case): a Blueprint shaped like the
    /// existing `02-verdict-loop.json` sample — a `Loop` retrying while
    /// `$.verdict == "BLOCKED"` plus a `Branch` on `$.verdict == "PASS"` —
    /// but with `verdict` omitted on every agent must compile unchanged
    /// (at most `tracing::warn!`) and leave `CompiledAgentTable.
    /// verdict_contracts` empty.
    #[test]
    fn verdict_omitted_blueprint_compiles_unchanged_with_empty_contracts() {
        let agent = gate_agent(None);
        let flow = FlowNode::Seq {
            children: vec![
                step("gate", "$.verdict"),
                FlowNode::Loop {
                    counter: Expr::Path {
                        at: "$.n".parse().expect("literal test path"),
                    },
                    cond: eq_cond("$.verdict", "BLOCKED"),
                    body: Box::new(step("gate", "$.verdict")),
                    max: 3,
                },
                branch(eq_cond("$.verdict", "PASS"), noop(), noop()),
            ],
        };
        let bp = minimal_bp(agent, flow);
        let compiled = Compiler::new(registry_with_echo())
            .compile(&bp)
            .expect("a verdict-omitted Blueprint must compile unchanged");
        assert!(
            compiled.router.verdict_contracts.is_empty(),
            "no agent declared a verdict contract"
        );
    }
}
