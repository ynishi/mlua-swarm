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

use crate::blueprint::{
    resolve_bound_agents, AgentDef, AgentKind, AgentProfile, Blueprint, BlueprintMetadata,
    BoundAgent, BoundAgentResolveError, Runner,
};
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
    /// Runner / Agent / Context binding failed before any spawner was built.
    #[error("bound agent resolution: {0}")]
    BoundAgent(#[from] BoundAgentResolveError),
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

/// Stable prefix of the `InvalidSpec` message the operator factory emits
/// when a WS-thin-path operator agent lacks its worker binding. Shared
/// by the message construction site
/// ([`OperatorSpawnerFactory::build`]) and the
/// [`From<&CompileError>`] specialization below, so the two can never
/// drift apart (GH #79 — the CLI used to re-detect this case by
/// substring-matching the *formatted* error, which broke silently on
/// any wording change).
pub const WORKER_BINDING_REQUIRED_MSG_PREFIX: &str =
    "profile.worker_binding is required for this operator backend";

/// GH #79 Phase 2: project every [`CompileError`] variant into the
/// unified [`Diagnostic`] shape (`mlua-swarm-diag`), preserving the
/// variant's typed fields into `span` / `notes` / `help` directly — no
/// substring re-parse of the `#[error(...)]` strings.
///
/// Every diagnostic is `stage: CompileLint` / `level: Error` (a
/// `CompileError` always aborts the compile). The `kind` keys match
/// [`mlua_swarm_diag::LINT_DECLS`] entries one-to-one — asserted by
/// this module's `every_compile_error_variant_maps_to_a_declared_lint`
/// test.
///
/// One specialization: an [`CompileError::InvalidSpec`] whose message
/// carries [`WORKER_BINDING_REQUIRED_MSG_PREFIX`] maps to the
/// dual-stage kind `worker-binding-missing` (the same lint `bp_doctor`
/// reports as `Warn` post-register) instead of the generic
/// `invalid-agent-spec` — one lint kind, one docs anchor, one
/// downstream switch key across both stages.
impl From<&CompileError> for mlua_swarm_diag::Diagnostic {
    fn from(err: &CompileError) -> Self {
        use mlua_swarm_diag::{
            Applicability, DiagElement, DiagLevel, DiagSpan, DiagStage, Diagnostic, DocsRef,
            Suggestion,
        };
        let base = |kind: &'static str| {
            Diagnostic::new(
                kind,
                DiagStage::CompileLint,
                DiagLevel::Error,
                err.to_string(),
            )
        };
        let agent_span = |name: &str| DiagSpan {
            element: DiagElement::Agent {
                name: name.to_string(),
            },
            json_path: Some(format!("$.agents[?(@.name=='{name}')]")),
        };
        match err {
            CompileError::BoundAgent(_) => base("bound-agent-resolution"),
            CompileError::UnknownKind(_) => base("unknown-agent-kind").with_help(
                "register a SpawnerFactory for this kind, or disable strategy.strict_kind",
            ),
            CompileError::InvalidSpec { name, msg }
                if msg.starts_with(WORKER_BINDING_REQUIRED_MSG_PREFIX) =>
            {
                Diagnostic::new(
                    "worker-binding-missing",
                    DiagStage::CompileLint,
                    DiagLevel::Error,
                    format!(
                        "operator agent '{name}' has no explicit Runner or legacy \
                         `profile.worker_binding`"
                    ),
                )
                .with_note(msg.clone())
                .with_suggestion(Suggestion {
                    msg: "add an explicit Runner (or legacy profile.worker_binding)".into(),
                    patch: "runner = { backend = \"ws_operator\", variant = \"claude\", \
                            tools = {} }"
                        .into(),
                    applicability: Applicability::HasPlaceholders,
                })
                .with_docs_ref(DocsRef {
                    uri: "mse://guides/bp-dsl-templates",
                    anchor: None,
                })
                .with_span(agent_span(name))
            }
            CompileError::InvalidSpec { name, .. } => {
                base("invalid-agent-spec").with_span(agent_span(name))
            }
            CompileError::UnresolvedRef(ref_) => base("unresolved-agent-ref").with_span(DiagSpan {
                element: DiagElement::Step { ref_: ref_.clone() },
                json_path: None,
            }),
            CompileError::DuplicateAgent(name) => {
                base("duplicate-agent-name").with_span(agent_span(name))
            }
            CompileError::UnresolvedOperatorRef { agent, defined, .. } => {
                base("unresolved-operator-ref")
                    .with_note(format!("declared OperatorDef names: {defined:?}"))
                    .with_span(agent_span(agent))
            }
            CompileError::UnresolvedMetaRef { defined, .. } => base("unresolved-meta-ref")
                .with_note(format!("declared MetaDef names: {defined:?}")),
            CompileError::StepNamingCollision(_) => base("step-naming-collision"),
            CompileError::InvalidProjectionPlacement(_) => base("invalid-projection-placement")
                .with_span(DiagSpan {
                    element: DiagElement::BlueprintRoot,
                    json_path: Some("$.projection_placement".into()),
                }),
            CompileError::UnresolvedAuditAgent { defined, .. } => base("unresolved-audit-agent")
                .with_note(format!("declared AgentDef names: {defined:?}"))
                .with_span(DiagSpan {
                    element: DiagElement::BlueprintRoot,
                    json_path: Some("$.audits".into()),
                }),
            CompileError::VerdictChannelMismatch { agent, .. } => base("verdict-channel-mismatch")
                .with_help(
                    "see the \"Returning verdicts to drive BP flow\" guide's Pattern A \
                         (channel: \"body\") / Pattern B (channel: \"part\")",
                )
                .with_docs_ref(DocsRef {
                    uri: "mse://guides/blueprint-authoring",
                    anchor: None,
                })
                .with_span(agent_span(agent)),
            CompileError::VerdictValueNotInContract { agent, .. } => {
                base("verdict-value-not-in-contract")
                    // The patch is deliberately the same prose recipe the
                    // legacy FixHint carried (GH #62) — CLI stderr and the
                    // bp_build response render it verbatim, and the
                    // `bp_build_cli` smoke test asserts on the
                    // `agents[N].verdict.values` pointer inside it.
                    .with_suggestion(Suggestion {
                        msg: "align the cond literal with the agent's declared verdict \
                              contract"
                            .into(),
                        patch: "either add the cond's literal to `agents[N].verdict.values`, \
                                or change the cond to a value that is already declared"
                            .into(),
                        applicability: Applicability::MaybeIncorrect,
                    })
                    .with_docs_ref(DocsRef {
                        uri: "mse://guides/blueprint-authoring",
                        anchor: None,
                    })
                    .with_span(agent_span(agent))
            }
            CompileError::VerdictValueUnhandled {
                agent,
                declared_values,
                ..
            } => base("verdict-value-unhandled")
                .with_note(format!("declared verdict.values: {declared_values:?}"))
                .with_help(
                    "either handle the value in a downstream Branch/Loop cond, or drop it \
                     from verdict.values",
                )
                .with_span(agent_span(agent)),
        }
    }
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

fn project_bound_agent_for_legacy_factories(bound: &BoundAgent) -> AgentDef {
    let mut agent = bound.agent.clone();
    match &bound.runner {
        Some(Runner::WsOperator { variant, tools })
        | Some(Runner::WsClaudeCode { variant, tools }) => {
            let profile = agent.profile.get_or_insert_with(AgentProfile::default);
            profile.worker_binding = Some(variant.clone());
            profile.tools = tools.clone();
        }
        Some(Runner::AgentBlockInProcess { tools }) => {
            let profile = agent.profile.get_or_insert_with(AgentProfile::default);
            profile.worker_binding = None;
            profile.tools = tools.clone();
        }
        // GH #83: the Subprocess EmbedAgent backend has no legacy profile
        // projection — the resolved SubprocessDef template reaches
        // `SubprocessProcessSpawnerFactory` through the build hint, and
        // profile.model/tools are consumed by the factory directly.
        Some(Runner::Subprocess { .. }) => {}
        None => {}
    }
    let meta = agent.meta.get_or_insert_with(Default::default);
    meta.context_policy = bound.context_policy.clone();
    agent
}

/// Rebuild a Blueprint's Agent/Context layers from an immutable binding
/// snapshot while leaving its flow and non-binding metadata untouched.
pub(crate) fn materialize_bound_blueprint(
    bp: &Blueprint,
    bound_agents: &[BoundAgent],
) -> Blueprint {
    let mut effective = bp.clone();
    effective.agents = bound_agents
        .iter()
        .map(project_bound_agent_for_legacy_factories)
        .collect();
    // Each effective policy is now pinned on its AgentDef; retaining a
    // mutable BP-global default would reintroduce registry drift on resume.
    effective.default_context_policy = None;
    effective
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
        let bound_agents = resolve_bound_agents(bp)?;
        self.compile_bound_pinned(bp, &bound_agents, None)
    }

    /// Compile with an already-resolved immutable binding snapshot. Resume
    /// paths use this entry point so a mutable Blueprint registry cannot
    /// silently change the Runner, prompt, contract, or static context policy
    /// between the original Run and its continuation.
    pub fn compile_bound(
        &self,
        bp: &Blueprint,
        bound_agents: &[BoundAgent],
    ) -> Result<CompiledBlueprint, CompileError> {
        self.compile_bound_pinned(bp, bound_agents, None)
    }

    /// [`Self::compile_bound`] plus a launch-scoped Operator session pin.
    ///
    /// `operator_pin` is the session id (`S-<hex>`) this launch is bound to.
    /// When `Some`, every `kind = Operator` agent is compiled against that
    /// session instead of whichever session currently holds the agent's
    /// `spec.operator_ref` role — the Blueprint keeps naming the logical
    /// role, and which session it means becomes a launch-time fact. The pin
    /// travels as a compile-synthesized build hint (same mechanism as the
    /// Subprocess template hint, see [`resolve_subprocess_template_hint`]);
    /// `spec.operator_ref` itself is never rewritten, so design-time
    /// validation and the `OperatorDef.kind` cascade keep reading the
    /// declared role.
    ///
    /// `None` reproduces [`Self::compile_bound`] byte-for-byte.
    pub fn compile_bound_pinned(
        &self,
        bp: &Blueprint,
        bound_agents: &[BoundAgent],
        operator_pin: Option<&str>,
    ) -> Result<CompiledBlueprint, CompileError> {
        let effective = materialize_bound_blueprint(bp, bound_agents);
        self.compile_resolved(&effective, operator_pin)
    }

    fn compile_resolved(
        &self,
        bp: &Blueprint,
        operator_pin: Option<&str>,
    ) -> Result<CompiledBlueprint, CompileError> {
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
            // GH #83: a Subprocess agent resolving to `Runner::Subprocess`
            // gets a compile-synthesized hint carrying its resolved
            // `SubprocessDef` template + overrides (EmbedAgent mode). Any
            // other resolution keeps the historical spec-based hint — an
            // existing Subprocess BP (program/args in spec) is untouched.
            //
            // No sibling arm exists for `AgentKind::AgentBlock`: its Runner
            // input (`tools`) already arrives as `profile.tools` off the
            // pinned `BoundAgent` snapshot — see the note on
            // `project_bound_agent_for_legacy_factories` / the
            // `SUBPROCESS_*_HINT_KEY` consts.
            let subprocess_hint = if ad.kind == AgentKind::Subprocess {
                resolve_subprocess_template_hint(bp, ad)?
            } else {
                None
            };
            // Run-scoped Operator pin: an `Operator` agent compiled under a
            // launch-time session pin carries it in as a synthesized hint
            // (sibling of the Subprocess template hint above). The pin is
            // merged INTO the author-declared `hints.per_agent` entry rather
            // than replacing it, so a Blueprint that already hints this agent
            // keeps every key it declared. Unpinned launches synthesize
            // nothing and the historical hint is passed through untouched.
            let operator_pin_hint = match operator_pin {
                Some(pin) if ad.kind == AgentKind::Operator => {
                    Some(merge_operator_pin_hint(hint, pin, &ad.name)?)
                }
                _ => None,
            };
            let spawner = factory.build(
                ad,
                operator_pin_hint
                    .as_ref()
                    .or(subprocess_hint.as_ref())
                    .or(hint),
            )?;
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
        //
        // Only STRONG claims (a `Step.ref`, a declared `projection_name`,
        // or an `out` that is exactly `$.T`) reach either path. Steps
        // sharing a nesting root (`$.r.a` / `$.r.b`) claim it weakly, and
        // a contested weak claim is dropped inside `from_blueprint` at
        // `debug!` level — so the ordinary "several lanes under one root"
        // Blueprint no longer warns on every compile. See
        // `StepNaming`'s struct doc for the full ladder + boundary table.
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
    for finding in fold_unhandled_verdict_values(verdict_contracts, referenced_values, step_agents)
    {
        if strict_verdict_handling {
            errors.push(CompileError::VerdictValueUnhandled {
                agent: finding.agent,
                value: finding.value,
                declared_values: finding.declared_values,
                step_ref: finding.step_ref,
            });
        } else {
            tracing::warn!(
                agent = %finding.agent,
                value = %finding.value,
                step_ref = %finding.step_ref,
                "declared verdict value has no downstream cond handler; \
                 opt in to `metadata.strict_verdict_handling` to reject at compile"
            );
        }
    }
}

/// One declared `verdict.values` entry that no downstream `Branch`/`Loop`
/// `cond` ever compares against — the reverse-direction lint's finding,
/// as data.
///
/// Exists so the same check can drive two very different surfaces without
/// a second implementation: the compile gate
/// ([`check_unhandled_verdict_values`], which turns a finding into a
/// `CompileError` under `strict_verdict_handling` and a `tracing::warn!`
/// otherwise) and the report-only `bp_doctor` `verdict_contract_lint`
/// family (via [`unhandled_verdict_values`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnhandledVerdictValue {
    /// The contract-bearing agent (= `AgentDef.name` = `Step.ref_`).
    pub agent: String,
    /// The declared value nothing handles.
    pub value: String,
    /// The agent's full declared token set, for the diagnostic's context.
    pub declared_values: Vec<String>,
    /// The first flow site that invokes `agent`, for attribution.
    pub step_ref: String,
}

/// Report-only projection of the reverse-direction verdict lint: run both
/// passes [`verify_verdict_conds`] runs and return the unhandled declared
/// values as data instead of turning the first one into a `CompileError`.
///
/// Callable on an already-registered Blueprint with no `SpawnerRegistry`
/// and no compile — the `bp_doctor` `verdict_contract_lint` family's
/// producer. Forward-direction violations (`VerdictChannelMismatch` /
/// `VerdictValueNotInContract`) are the compile gate's business and are
/// deliberately dropped here: they already hard-fail `bp_build`, so
/// re-reporting them as advisory findings would double-count.
///
/// A Blueprint whose flow declares contracts but has no `Branch`/`Loop`
/// at all yields one finding per declared value — the shape that reads as
/// "this contract is decorative", and the earliest signal that a
/// `channel` was declared without anything downstream actually reading
/// it.
pub fn unhandled_verdict_values(
    flow: &FlowNode,
    verdict_contracts: &HashMap<String, VerdictContract>,
) -> Vec<UnhandledVerdictValue> {
    let mut step_outputs: HashMap<String, String> = HashMap::new();
    let mut step_agents: HashMap<String, String> = HashMap::new();
    collect_step_outputs_and_agents(flow, &mut step_outputs, &mut step_agents);

    let mut referenced_values: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
    let mut discarded_errors: Vec<CompileError> = Vec::new();
    collect_verdict_conds(
        flow,
        &step_outputs,
        verdict_contracts,
        &mut referenced_values,
        &mut discarded_errors,
    );
    fold_unhandled_verdict_values(verdict_contracts, &referenced_values, &step_agents)
}

/// One agent whose entire declared `verdict.values` set went unread — the
/// per-agent aggregate of [`UnhandledVerdictValue`]. Signals that the
/// contract is decorative: the step declares a verdict, but every declared
/// token is unhandled downstream, so the gate cannot halt the flow.
///
/// Separate from [`UnhandledVerdictValue`] because a normal Blueprint
/// always leaks one per-value finding per agent (the halt gate only reads
/// the halt token, so PASS is structurally unhandled). That baseline noise
/// hides the actual defect this variant catches — the whole gate being
/// dropped (e.g. `2db863e` opt-OUT authoring surviving the `bafe47d4`
/// opt-in flip). Consumers surface both: per-value stays for parity with
/// `strict_verdict_handling`, per-agent adds a WARN whose count equals the
/// number of agents whose gate is fully dead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentContractUnread {
    /// The contract-bearing agent (= `AgentDef.name` = `Step.ref_`).
    pub agent: String,
    /// The full declared token set — every one of these is unread.
    pub declared_values: Vec<String>,
    /// The first flow site that invokes `agent`, for attribution.
    pub step_ref: String,
}

/// Per-agent aggregate of [`unhandled_verdict_values`]: return one entry
/// per agent whose entire declared `verdict.values` set went unhandled.
///
/// Called by the `bp_doctor` `verdict_contract_lint` family alongside the
/// per-value producer; the two views coexist. Agents with a partially
/// handled contract (any single value read by a cond) contribute nothing
/// here — the per-value findings already point at the specific gap.
///
/// Stable order (agent name sort) mirrors [`fold_unhandled_verdict_values`]
/// so the `bp_doctor` findings array is reproducible between calls.
pub fn agents_with_all_verdict_values_unread(
    flow: &FlowNode,
    verdict_contracts: &HashMap<String, VerdictContract>,
) -> Vec<AgentContractUnread> {
    let per_value = unhandled_verdict_values(flow, verdict_contracts);
    let mut unread_counts: HashMap<String, usize> = HashMap::new();
    for finding in &per_value {
        *unread_counts.entry(finding.agent.clone()).or_default() += 1;
    }
    let mut agents: Vec<&String> = verdict_contracts.keys().collect();
    agents.sort();
    let mut out = Vec::new();
    for agent in agents {
        let contract = &verdict_contracts[agent];
        let declared = contract.values.len();
        if declared == 0 {
            continue;
        }
        let unread = unread_counts.get(agent).copied().unwrap_or(0);
        if unread != declared {
            continue;
        }
        // Attribute to the first step that invokes this agent, matching the
        // per-value producer's `step_ref` field so downstream renderers can
        // cross-reference the two finding sets by agent + step.
        let step_ref = per_value
            .iter()
            .find(|f| &f.agent == agent)
            .map(|f| f.step_ref.clone())
            .unwrap_or_else(|| agent.clone());
        out.push(AgentContractUnread {
            agent: agent.clone(),
            declared_values: contract.values.clone(),
            step_ref,
        });
    }
    out
}

/// The shared core of [`check_unhandled_verdict_values`] and
/// [`unhandled_verdict_values`]: given the two passes' output, fold out
/// the declared values nothing references.
///
/// Iterates in a stable order (sorted by agent name, then declared-value
/// order) so the first `VerdictValueUnhandled` error surfaced under
/// strict mode is deterministic across HashMap hash seeds, and so the
/// `bp_doctor` family's findings array is reproducible between calls.
/// This mirrors GH #50's other lint diagnostics, which are stable because
/// they walk the flow tree in source order.
fn fold_unhandled_verdict_values(
    verdict_contracts: &HashMap<String, VerdictContract>,
    referenced_values: &HashMap<String, std::collections::HashSet<String>>,
    step_agents: &HashMap<String, String>,
) -> Vec<UnhandledVerdictValue> {
    let mut agents: Vec<&String> = verdict_contracts.keys().collect();
    agents.sort();
    let mut findings = Vec::new();
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
            findings.push(UnhandledVerdictValue {
                agent: agent.clone(),
                value: value.clone(),
                declared_values: contract.values.clone(),
                step_ref: step_ref.clone(),
            });
        }
    }
    findings
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
///
/// # GH #83 — EmbedAgent template mode
///
/// When the build `hint` carries a `subprocess_template` key (synthesized
/// by `Compiler::compile` from a resolved `Runner::Subprocess` — see
/// [`resolve_subprocess_template_hint`]), the factory switches to the
/// EmbedAgent path instead: it bakes `agent_def.profile`
/// (system_prompt / model / tools, same compile-time bake shape as
/// `OperatorSpawnerFactory`), validates the template's placeholder tokens
/// against the closed set, and returns a `ProcessSpawner` whose `embed`
/// field drives the render → exec → normalize spawn. The spec-based
/// shape above stays byte-for-byte untouched when no such hint is
/// present.
pub struct SubprocessProcessSpawnerFactory;

impl SpawnerFactoryKind for SubprocessProcessSpawnerFactory {
    const KIND: AgentKind = AgentKind::Subprocess;
    type Worker = crate::worker::process_spawner::ProcessWorker;
}

/// GH #83 — hint key carrying the resolved [`SubprocessDef`] template
/// (synthesized at compile time, see [`resolve_subprocess_template_hint`]).
pub const SUBPROCESS_TEMPLATE_HINT_KEY: &str = "subprocess_template";
/// GH #83 — hint key carrying the `Runner::Subprocess` overrides.
pub const SUBPROCESS_OVERRIDES_HINT_KEY: &str = "subprocess_overrides";

// GH #86 note — no `agent_block_tools` build hint exists, deliberately.
// `Runner::AgentBlockInProcess.tools` already reaches the AgentBlock
// factory as `profile.tools`, projected from the immutable `BoundAgent`
// snapshot by `project_bound_agent_for_legacy_factories` above. Re-deriving
// it here (the shape GH #83's Subprocess sibling uses, which has no such
// projection) would re-run `resolve_runner` against the LIVE Blueprint and
// so let a `Blueprint.runners` edit change a pinned Run's enforced grant on
// resume — exactly the drift `compile_bound` exists to prevent.

/// GH #83 — reject any `{ident}` token outside the closed placeholder
/// set. Only lowercase-identifier tokens (`[a-z_]+`) are placeholder
/// candidates; other brace contents (e.g. JSON literals like
/// `{"result": 1}` inside a `sh -c` one-liner) are legal template text.
fn validate_embed_placeholders(s: &str, where_: &str) -> Result<(), String> {
    let mut rest = s;
    while let Some(start) = rest.find('{') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('}') else {
            break;
        };
        let token = &after[..end];
        let is_candidate =
            !token.is_empty() && token.chars().all(|c| c.is_ascii_lowercase() || c == '_');
        if is_candidate {
            if !crate::worker::process_spawner::EMBED_PLACEHOLDERS.contains(&token) {
                return Err(format!(
                    "unknown placeholder '{{{token}}}' in {where_}; closed set is \
                     {{system, system_file, prompt, model, tools_csv, work_dir, task_id, attempt}}"
                ));
            }
            rest = &after[end + 1..];
        } else {
            // Literal brace text — keep scanning right after the '{' so a
            // placeholder nested inside (e.g. a JSON-wrapped stdin like
            // `{"task": "{prompt}"}`) is still validated. Mirrors the
            // spawn-time render scan in `EmbedVars::render`.
            rest = after;
        }
    }
    Ok(())
}

/// GH #83 — compile-time resolution of an agent's `Runner::Subprocess`
/// declaration into the synthesized build hint the
/// `SubprocessProcessSpawnerFactory` consumes. Returns `Ok(None)` when
/// the agent resolves to no Runner or to a non-Subprocess backend — the
/// caller then keeps the historical spec-based hint untouched.
fn resolve_subprocess_template_hint(
    bp: &Blueprint,
    ad: &AgentDef,
) -> Result<Option<Value>, CompileError> {
    let invalid = |msg: String| CompileError::InvalidSpec {
        name: ad.name.clone(),
        msg,
    };
    let runner = mlua_swarm_schema::resolve_runner(bp, ad).map_err(|e| invalid(e.to_string()))?;
    let Some(Runner::Subprocess {
        template,
        overrides,
    }) = runner
    else {
        return Ok(None);
    };
    let def = bp
        .subprocesses
        .iter()
        .find(|d| d.name == template)
        .ok_or_else(|| {
            let mut names: Vec<&str> = bp.subprocesses.iter().map(|d| d.name.as_str()).collect();
            names.sort_unstable();
            invalid(format!(
                "Runner::Subprocess template '{template}' not found in \
                 Blueprint.subprocesses (defined: [{}])",
                names.join(", ")
            ))
        })?;
    Ok(Some(serde_json::json!({
        SUBPROCESS_TEMPLATE_HINT_KEY: def,
        SUBPROCESS_OVERRIDES_HINT_KEY: overrides,
    })))
}

/// Hint key carrying the launch-scoped Operator session pin (`S-<hex>`),
/// synthesized by [`Compiler::compile_bound_pinned`] and consumed by
/// [`OperatorSpawnerFactory::build`].
pub const OPERATOR_SID_PIN_HINT_KEY: &str = "operator_sid_pin";

/// Merge the run-scoped Operator session pin into an agent's declared build
/// hint. An absent hint becomes a fresh one-key object; a declared object
/// hint is cloned and gains the pin key (declared keys survive). A declared
/// non-object hint is a Blueprint authoring error here — merging would have
/// to drop it silently, and a dropped hint is exactly the kind of quiet
/// substitution the pin exists to remove.
fn merge_operator_pin_hint(
    hint: Option<&Value>,
    pin: &str,
    agent: &str,
) -> Result<Value, CompileError> {
    let mut object = match hint {
        None => serde_json::Map::new(),
        Some(Value::Object(map)) => map.clone(),
        Some(other) => {
            return Err(CompileError::InvalidSpec {
                name: agent.to_string(),
                msg: format!(
                    "hints.per_agent['{agent}'] must be a JSON object to carry the \
                     run-scoped operator pin (got {other})"
                ),
            });
        }
    };
    object.insert(
        OPERATOR_SID_PIN_HINT_KEY.to_string(),
        Value::String(pin.to_string()),
    );
    Ok(Value::Object(object))
}

impl SubprocessProcessSpawnerFactory {
    /// GH #83 — the EmbedAgent template build path (see the struct doc).
    /// Returns the concrete [`ProcessSpawner`] so unit tests can inspect
    /// the baked [`EmbedTemplate`]; `SpawnerFactory::build` wraps it in
    /// the trait `Arc`.
    fn build_embed(
        agent_def: &AgentDef,
        template: &Value,
        overrides: Option<&Value>,
    ) -> Result<ProcessSpawner, CompileError> {
        use crate::worker::process_spawner::EmbedTemplate;
        use mlua_swarm_schema::{SubprocessDef, SubprocessOverrides};

        let agent_name = &agent_def.name;
        let invalid = |msg: String| CompileError::InvalidSpec {
            name: agent_name.to_string(),
            msg,
        };
        let def: SubprocessDef = serde_json::from_value(template.clone())
            .map_err(|e| invalid(format!("subprocess_template hint: {e}")))?;
        let overrides: SubprocessOverrides = match overrides {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|e| invalid(format!("subprocess_overrides hint: {e}")))?,
            None => SubprocessOverrides::default(),
        };

        if def.argv.is_empty() {
            return Err(invalid(format!(
                "SubprocessDef '{}': argv must not be empty",
                def.name
            )));
        }
        // Closed-set placeholder validation across every template string.
        for (i, a) in def.argv.iter().enumerate() {
            validate_embed_placeholders(a, &format!("argv[{i}]")).map_err(&invalid)?;
        }
        if let Some(stdin) = &def.stdin {
            validate_embed_placeholders(stdin, "stdin").map_err(&invalid)?;
        }
        for (k, v) in &def.env {
            validate_embed_placeholders(v, &format!("env['{k}']")).map_err(&invalid)?;
        }
        if let Some(cwd) = &def.cwd {
            validate_embed_placeholders(cwd, "cwd").map_err(&invalid)?;
        }
        let stream_mode = match def.stream_mode.as_deref() {
            Some("ndjson_lines") => Some(StreamMode::NdjsonLines),
            Some("sse_events") => Some(StreamMode::SseEvents),
            Some("length_prefixed") => Some(StreamMode::LengthPrefixed),
            Some(other) => return Err(invalid(format!("unknown stream_mode: {other}"))),
            None => None,
        };
        if let Some(output) = &def.output {
            if stream_mode.is_some() {
                return Err(invalid(format!(
                    "SubprocessDef '{}': output normalization is a plain-mode \
                     declaration; remove either `output` or `stream_mode`",
                    def.name
                )));
            }
            if let Some(format) = output.format.as_deref() {
                if format != "json" {
                    return Err(invalid(format!(
                        "SubprocessDef '{}': unknown output.format '{format}' \
                         (supported: \"json\")",
                        def.name
                    )));
                }
            }
            if let Some(ptr) = output.result_ptr.as_deref() {
                if !ptr.starts_with('/') {
                    return Err(invalid(format!(
                        "SubprocessDef '{}': output.result_ptr '{ptr}' is not a \
                         JSON Pointer (RFC 6901 — must start with '/')",
                        def.name
                    )));
                }
            }
            if let Some(ok_from) = output.ok_from.as_deref() {
                if ok_from != "exit_code" && !ok_from.starts_with('/') {
                    return Err(invalid(format!(
                        "SubprocessDef '{}': output.ok_from '{ok_from}' must be \
                         \"exit_code\" or a JSON Pointer (starting with '/')",
                        def.name
                    )));
                }
            }
        }

        // Compile-time profile bake — same shape as OperatorSpawnerFactory,
        // with Runner::Subprocess overrides winning over the profile.
        let profile = agent_def.profile.as_ref();
        let system_prompt = profile
            .map(|p| p.system_prompt.clone())
            .filter(|s| !s.is_empty());
        let model = overrides
            .model
            .clone()
            .or_else(|| profile.and_then(|p| p.model.clone()));
        let tools: Vec<String> = if overrides.tools.is_empty() {
            profile.map(|p| p.tools.clone()).unwrap_or_default()
        } else {
            overrides.tools.clone()
        };
        // overrides.cwd wins over the template's own cwd.
        let cwd = overrides.cwd.clone().or_else(|| def.cwd.clone());
        if let Some(c) = &cwd {
            validate_embed_placeholders(c, "overrides.cwd").map_err(&invalid)?;
        }

        let program = def.argv[0].clone();
        let sp = ProcessSpawner {
            program,
            args: Vec::new(),
            use_stdin: def.stdin.is_some(),
            stream_mode,
            embed: Some(EmbedTemplate {
                argv: def.argv,
                stdin: def.stdin,
                env: def.env,
                cwd,
                output: def.output,
                system_prompt,
                model,
                tools_csv: tools.join(","),
            }),
        };
        Ok(sp)
    }
}

impl SpawnerFactory for SubprocessProcessSpawnerFactory {
    fn build(
        &self,
        agent_def: &AgentDef,
        hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
        // GH #83: EmbedAgent template mode when the compile-synthesized
        // hint is present; the spec-based path below is byte-for-byte
        // unchanged otherwise.
        if let Some(template) = hint.and_then(|h| h.get(SUBPROCESS_TEMPLATE_HINT_KEY)) {
            let overrides = hint.and_then(|h| h.get(SUBPROCESS_OVERRIDES_HINT_KEY));
            return Self::build_embed(agent_def, template, overrides).map(|sp| {
                let arc: Arc<dyn SpawnerAdapter> = Arc::new(sp);
                arc
            });
        }
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
            embed: None,
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

            // 1b. GH #86: the task-context tier, off the same
            //     `WorkerInvocation.context` seam the AgentBlock backend
            //     reads, rendered through the same shared mapping
            //     (`context_globals`) so a Lua gate sees identical globals
            //     on either in-process backend and stays portable between
            //     them. An absent field contributes no entry, so the
            //     global is simply nil — the "insert nothing when absent"
            //     contract the rest of this axis follows.
            for (name, value) in
                crate::worker::agent_block::runtime::context_globals(inv.context.as_ref())
            {
                let lua_val = lua
                    .to_value(&value)
                    .map_err(|e| format!("{name} to_value: {e}"))?;
                g.set(name.as_str(), lua_val)
                    .map_err(|e| format!("set {name}: {e}"))?;
            }

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
        stats: None,
    }
    .ensure_worker_kind("lua"))
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
///   in through external agent.md files land here.
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
/// # Run-scoped session pin
///
/// `spec.operator_ref` names a logical role, and a role's holder is
/// process-global state that another driver's session can own. When the
/// launch pins a session ([`Compiler::compile_bound_pinned`]), the pin
/// arrives as the [`OPERATOR_SID_PIN_HINT_KEY`] build hint and becomes the
/// lookup key for this factory — the Blueprint still declares the role, the
/// launch decides which session it means for this run. A pin that resolves
/// to no registered backend fails the compile; there is deliberately no
/// fallback to the role's current holder.
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
        hint: Option<&Value>,
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
        // Run-scoped pin (see `Compiler::compile_bound_pinned`): the launch
        // named the session this run routes to, so the lookup key is that
        // sid, not the role's current global holder. A pin that resolves to
        // nothing fails the launch loudly — falling back to the role here
        // would silently hand the run to the very session the pin exists to
        // avoid.
        let pin = hint
            .and_then(|h| h.get(OPERATOR_SID_PIN_HINT_KEY))
            .and_then(|v| v.as_str());
        let lookup_key = pin.unwrap_or(op_ref);
        let operators = self
            .operators
            .read()
            .expect("OperatorSpawnerFactory.operators RwLock poisoned");
        let op = operators.get(lookup_key).cloned().ok_or_else(|| {
            let mut names: Vec<String> = operators.keys().cloned().collect();
            names.sort();
            let names_list = if names.is_empty() {
                "<none>".to_string()
            } else {
                names.join(", ")
            };
            match pin {
                Some(pin) => invalid(format!(
                    "run-scoped operator pin '{pin}' (declared operator_ref '{op_ref}') \
                     is not registered in factory. \
                     Registered ids: [{names_list}]. \
                     The launch pinned this run to that session; resolving \
                     '{op_ref}' through whichever session currently holds the \
                     role would send this run's Spawn frames somewhere the \
                     caller did not ask for, so the launch fails instead. \
                     Hint: the pinned session must be joined (and still live) \
                     before launching."
                )),
                None => invalid(format!(
                    "operator_ref '{op_ref}' not registered in factory. \
                     Registered sids: [{names_list}]. \
                     Hint: call mse_operator_join(roles=[...]) to mint the sid first."
                )),
            }
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
                // Compile-time path: no immutable BoundAgent snapshot exists
                // here (the launch path resolves the digest). Self-check
                // inputs are supplied on the launch axis only.
                request_digest: None,
                requested_model: None,
            });
        if op.requires_worker_binding() && worker_binding.is_none() {
            // Issue #9: the two Blueprint authoring paths (direct JSON
            // and `$agent_md` file ref) both land here. Old message
            // pointed only at the `.md` frontmatter, which was
            // confusing for authors on the JSON-direct path. The prefix
            // const keeps this message and the GH #79 Diagnostic
            // specialization in lockstep.
            return Err(invalid(format!(
                "{WORKER_BINDING_REQUIRED_MSG_PREFIX}. \
                 Fix by either: \
                 (a) if authoring the Blueprint JSON directly, add \
                 `agents[N].profile.worker_binding: \"<subagent-type>\"` \
                 to the JSON literal; or \
                 (b) if using an $agent_md file ref, add \
                 `worker_binding: <subagent-type>` to the agent .md frontmatter."
            )));
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
                stats: None,
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
            lints: None,
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

    /// GH #79 regression lock: the factory error the compile-time gate
    /// emits must keep starting with the shared
    /// `WORKER_BINDING_REQUIRED_MSG_PREFIX` — otherwise the
    /// `From<&CompileError>` Diagnostic specialization (and `bp_doctor`'s
    /// dual-stage `worker-binding-missing` story) silently degrades to
    /// the generic `invalid-agent-spec` kind.
    #[test]
    fn factory_error_message_carries_the_shared_prefix_and_specializes_the_diagnostic() {
        let factory = OperatorSpawnerFactory::new();
        factory.register_operator(
            "op1",
            Arc::new(StubOperator {
                requires_binding: true,
            }) as Arc<dyn Operator>,
        );
        let def = agent_def_with(Some(AgentProfile::default()));
        let err = match factory.build(&def, None) {
            Err(err) => err,
            Ok(_) => panic!("expected compile-time failure, got Ok"),
        };
        match &err {
            CompileError::InvalidSpec { msg, .. } => {
                assert!(
                    msg.starts_with(WORKER_BINDING_REQUIRED_MSG_PREFIX),
                    "factory message must start with the shared prefix, got: {msg}"
                );
            }
            other => panic!("expected InvalidSpec, got: {other:?}"),
        }
        let d = mlua_swarm_diag::Diagnostic::from(&err);
        assert_eq!(d.kind, "worker-binding-missing");
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
            worker_binding: Some("code-worker".to_string()),
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
            lints: None,
        }
    }

    fn test_invocation(prompt: &str) -> crate::worker::adapter::WorkerInvocation {
        crate::worker::adapter::WorkerInvocation::new(
            CapToken {
                agent_id: "a".into(),
                role: Role::Worker,
                scopes: vec!["*".into()],
                issued_at: 0,
                expire_at: u64::MAX / 2,
                max_uses: None,
                nonce: "test-nonce".into(),
                sig_hex: "".into(),
            },
            StepId::parse("ST-test").expect("StepId parse"),
            1,
            "g",
            prompt,
        )
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
                stats: None,
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
            lints: None,
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
            subprocesses: vec![],
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
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
                stats: None,
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
            lints: None,
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
            subprocesses: vec![],
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
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

// ─── run-scoped Operator session pin ──────────────────────────────────────
//
// `spec.operator_ref` names a logical role whose holder is process-global
// state; a launch may pin the session it actually belongs to. These tests
// cover both halves: the compiler synthesizing the pin hint for exactly the
// `kind = Operator` agents, and the factory resolving (or loudly failing)
// against the pinned id instead of the role.
#[cfg(test)]
mod operator_run_pin_tests {
    use super::*;
    use crate::core::ctx::Ctx;
    use crate::types::CapToken;
    use crate::worker::adapter::{WorkerError, WorkerResult};
    use std::sync::Mutex;

    /// Shared `(agent, hint)` log the recording factories append to.
    type Seen = Arc<Mutex<Vec<(String, Option<Value>)>>>;

    /// Records every `(agent, hint)` pair the compiler hands it, so a test
    /// can assert on the hint an agent was built with — the pin's whole
    /// effect at this layer.
    struct RecordingOperatorFactory {
        seen: Seen,
    }

    impl SpawnerFactory for RecordingOperatorFactory {
        fn build(
            &self,
            agent_def: &AgentDef,
            hint: Option<&Value>,
        ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
            self.seen
                .lock()
                .expect("RecordingOperatorFactory.seen poisoned")
                .push((agent_def.name.clone(), hint.cloned()));
            let mut spawner: InProcSpawner<LuaWorker> = InProcSpawner::<LuaWorker>::typed();
            let worker: WorkerFn = Arc::new(|_inv| {
                Box::pin(async move {
                    Ok(WorkerResult {
                        value: Value::Null,
                        ok: true,
                        stats: None,
                    })
                })
            });
            spawner.registry.insert(agent_def.name.clone(), worker);
            Ok(Arc::new(spawner))
        }
    }

    impl SpawnerFactoryKind for RecordingOperatorFactory {
        const KIND: AgentKind = AgentKind::Operator;
        type Worker = crate::operator::OperatorWorker;
    }

    /// Same recorder on a non-Operator kind, to prove the pin does not
    /// leak onto agents it has no business touching.
    struct RecordingLuaFactory {
        seen: Seen,
    }

    impl SpawnerFactory for RecordingLuaFactory {
        fn build(
            &self,
            agent_def: &AgentDef,
            hint: Option<&Value>,
        ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
            self.seen
                .lock()
                .expect("RecordingLuaFactory.seen poisoned")
                .push((agent_def.name.clone(), hint.cloned()));
            let mut spawner: InProcSpawner<LuaWorker> = InProcSpawner::<LuaWorker>::typed();
            let worker: WorkerFn = Arc::new(|_inv| {
                Box::pin(async move {
                    Ok(WorkerResult {
                        value: Value::Null,
                        ok: true,
                        stats: None,
                    })
                })
            });
            spawner.registry.insert(agent_def.name.clone(), worker);
            Ok(Arc::new(spawner))
        }
    }

    impl SpawnerFactoryKind for RecordingLuaFactory {
        const KIND: AgentKind = AgentKind::Lua;
        type Worker = LuaWorker;
    }

    fn recording_compiler() -> (Compiler, Seen, Seen) {
        let operator_seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let lua_seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let mut registry = SpawnerRegistry::new();
        registry.register::<RecordingOperatorFactory>(Arc::new(RecordingOperatorFactory {
            seen: operator_seen.clone(),
        }));
        registry.register::<RecordingLuaFactory>(Arc::new(RecordingLuaFactory {
            seen: lua_seen.clone(),
        }));
        (Compiler::new(registry), operator_seen, lua_seen)
    }

    /// Two agents (one Operator on role `main-ai`, one Lua) plus an
    /// author-declared `hints.per_agent` entry on the Operator one, so the
    /// merge behaviour is observable.
    fn bp_with_operator_and_lua_agents() -> Blueprint {
        serde_json::from_value(serde_json::json!({
            "schema_version": crate::blueprint::current_schema_version(),
            "id": "operator-pin-ut",
            "flow": {
                "kind": "step",
                "ref": "planner",
                "in": { "op": "path", "at": "$.input" },
                "out": { "op": "path", "at": "$.output" }
            },
            "agents": [
                {
                    "name": "planner",
                    "kind": "operator",
                    "spec": { "operator_ref": "main-ai" }
                },
                {
                    "name": "scorer",
                    "kind": "lua",
                    "spec": { "source": "return { value = 1, ok = true }" }
                }
            ],
            "operators": [{ "name": "main-ai" }],
            "hints": { "per_agent": { "planner": { "authored": "keep-me" } } },
            "strategy": { "strict_refs": false }
        }))
        .expect("test Blueprint literal")
    }

    fn hint_for(seen: &Seen, agent: &str) -> Option<Value> {
        seen.lock()
            .expect("seen poisoned")
            .iter()
            .find(|(name, _)| name == agent)
            .map(|(_, hint)| hint.clone())
            .expect("agent was never built")
    }

    /// Test 1: a pinned compile reaches the Operator factory with the sid,
    /// merged into (not over) the author's declared hint.
    #[test]
    fn pinned_compile_hands_the_operator_factory_the_pinned_sid() {
        let (compiler, operator_seen, _lua_seen) = recording_compiler();
        let bp = bp_with_operator_and_lua_agents();
        let bound = resolve_bound_agents(&bp).expect("resolve bound agents");
        compiler
            .compile_bound_pinned(&bp, &bound, Some("S-pinned"))
            .expect("pinned compile");

        let hint = hint_for(&operator_seen, "planner").expect("planner hint");
        assert_eq!(
            hint.get(OPERATOR_SID_PIN_HINT_KEY),
            Some(&Value::String("S-pinned".to_string())),
            "the pin must reach the factory as a build hint: {hint}"
        );
        assert_eq!(
            hint.get("authored"),
            Some(&Value::String("keep-me".to_string())),
            "merging the pin must not drop the author's declared hint: {hint}"
        );
    }

    /// Test 3 (regression lock): an unpinned compile synthesizes nothing —
    /// the factory sees exactly the authored hint, and an agent with no
    /// authored hint still sees `None`.
    #[test]
    fn unpinned_compile_passes_the_authored_hint_through_untouched() {
        let (compiler, operator_seen, lua_seen) = recording_compiler();
        let bp = bp_with_operator_and_lua_agents();
        let bound = resolve_bound_agents(&bp).expect("resolve bound agents");
        compiler
            .compile_bound(&bp, &bound)
            .expect("unpinned compile");

        assert_eq!(
            hint_for(&operator_seen, "planner"),
            Some(serde_json::json!({ "authored": "keep-me" })),
            "an unpinned compile must hand over the authored hint verbatim"
        );
        assert_eq!(
            hint_for(&lua_seen, "scorer"),
            None,
            "an agent with no authored hint must still be built with None"
        );
    }

    /// The pin is an Operator-axis fact: a Lua (or any non-Operator) agent
    /// in the same pinned Blueprint is built exactly as it would be
    /// unpinned.
    #[test]
    fn pin_does_not_leak_onto_non_operator_agents() {
        let (compiler, _operator_seen, lua_seen) = recording_compiler();
        let bp = bp_with_operator_and_lua_agents();
        let bound = resolve_bound_agents(&bp).expect("resolve bound agents");
        compiler
            .compile_bound_pinned(&bp, &bound, Some("S-pinned"))
            .expect("pinned compile");

        assert_eq!(
            hint_for(&lua_seen, "scorer"),
            None,
            "a non-Operator agent must not receive the operator pin hint"
        );
    }

    /// A declared hint that is not a JSON object cannot carry the pin.
    /// Dropping it silently is the kind of quiet substitution this feature
    /// exists to remove, so the compile fails instead.
    #[test]
    fn non_object_authored_hint_fails_the_pinned_compile() {
        let (compiler, _operator_seen, _lua_seen) = recording_compiler();
        let mut bp = bp_with_operator_and_lua_agents();
        bp.hints
            .per_agent
            .insert("planner".to_string(), Value::String("not-an-object".into()));
        let bound = resolve_bound_agents(&bp).expect("resolve bound agents");
        match compiler.compile_bound_pinned(&bp, &bound, Some("S-pinned")) {
            Err(CompileError::InvalidSpec { name, msg }) => {
                assert_eq!(name, "planner");
                assert!(
                    msg.contains("run-scoped operator pin"),
                    "message must explain why the hint blocks the pin: {msg}"
                );
            }
            Err(other) => panic!("expected InvalidSpec, got a different CompileError: {other}"),
            Ok(_) => panic!("expected the non-object hint to fail the pinned compile"),
        }
        // Unpinned, the same Blueprint is none of the compiler's business.
        assert!(
            compiler.compile_bound(&bp, &bound).is_ok(),
            "an unpinned compile must keep accepting whatever hint shape the author declared"
        );
    }

    // ── factory-level resolution ─────────────────────────────────────────

    /// Backend stub whose `requires_worker_binding` doubles as an identity
    /// marker: the two registrations below disagree on it, so which one the
    /// factory picked is visible in `build`'s outcome alone.
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
                stats: None,
            })
        }

        fn requires_worker_binding(&self) -> bool {
            self.requires_binding
        }
    }

    fn operator_agent() -> AgentDef {
        AgentDef {
            name: "planner".to_string(),
            kind: AgentKind::Operator,
            spec: serde_json::json!({ "operator_ref": "main-ai" }),
            profile: None,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
            lints: None,
        }
    }

    fn pin_hint(sid: &str) -> Value {
        serde_json::json!({ OPERATOR_SID_PIN_HINT_KEY: sid })
    }

    /// Test 1 (factory half): with the role held by one backend and the pin
    /// naming another, the pinned one is what gets baked — even though the
    /// agent keeps declaring the role.
    #[test]
    fn pin_resolves_the_pinned_session_not_the_role_holder() {
        let factory = OperatorSpawnerFactory::new();
        // The role's current holder would reject this agent (it requires a
        // worker_binding the agent does not declare); the pinned session
        // would accept it. Building successfully therefore proves the
        // pinned backend answered.
        factory.register_operator(
            "main-ai",
            Arc::new(StubOperator {
                requires_binding: true,
            }) as Arc<dyn Operator>,
        );
        factory.register_operator(
            "S-pinned",
            Arc::new(StubOperator {
                requires_binding: false,
            }) as Arc<dyn Operator>,
        );
        assert!(
            factory
                .build(&operator_agent(), Some(&pin_hint("S-pinned")))
                .is_ok(),
            "the pinned session must resolve the spawner, not the role's holder"
        );
        // And the mirror image: unpinned, the role's holder answers and
        // rejects it.
        assert!(
            factory.build(&operator_agent(), None).is_err(),
            "without a pin the role's holder must still be the resolution"
        );
    }

    /// Test 2: a pin naming no registered session fails the build loudly,
    /// naming both the pin and the role it did NOT fall back to.
    #[test]
    fn pin_miss_fails_loud_and_never_falls_back_to_the_role() {
        let factory = OperatorSpawnerFactory::new();
        factory.register_operator(
            "main-ai",
            Arc::new(StubOperator {
                requires_binding: false,
            }) as Arc<dyn Operator>,
        );
        match factory.build(&operator_agent(), Some(&pin_hint("S-gone"))) {
            Err(CompileError::InvalidSpec { name, msg }) => {
                assert_eq!(name, "planner");
                assert!(msg.contains("S-gone"), "message must name the pin: {msg}");
                assert!(
                    msg.contains("main-ai"),
                    "message must name the declared role it refused to fall back to: {msg}"
                );
            }
            Err(other) => panic!("expected InvalidSpec, got a different CompileError: {other}"),
            Ok(_) => panic!(
                "a pin naming no live session must fail the launch, not silently resolve the role"
            ),
        }
    }

    /// Test 3 (factory half): with no pin hint the lookup and its error
    /// message are the pre-pin ones.
    #[test]
    fn unpinned_build_keeps_the_historical_role_lookup_and_message() {
        let factory = OperatorSpawnerFactory::new();
        match factory.build(&operator_agent(), None) {
            Err(CompileError::InvalidSpec { msg, .. }) => {
                assert!(
                    msg.contains("operator_ref 'main-ai' not registered in factory"),
                    "unpinned message must stay the historical one: {msg}"
                );
                assert!(
                    !msg.contains("run-scoped operator pin"),
                    "an unpinned failure must not mention the pin: {msg}"
                );
            }
            Err(other) => panic!("expected InvalidSpec, got a different CompileError: {other}"),
            Ok(_) => panic!("an unregistered role must still fail"),
        }
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
                stats: None,
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
                lints: None,
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
            subprocesses: vec![],
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
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
                stats: None,
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
            lints: None,
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
            subprocesses: vec![],
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
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

    // ─── GH #79 Phase 2: CompileError → Diagnostic projection ────────

    /// Every `kind` key the `From<&CompileError>` impl can emit must be
    /// declared in `mlua_swarm_diag::LINT_DECLS` (the exhaustiveness of
    /// the variant mapping itself is enforced by the compiler — the
    /// `match` in the impl has no wildcard arm).
    #[test]
    fn every_compile_error_diagnostic_kind_is_a_declared_lint() {
        let kinds = [
            "bound-agent-resolution",
            "unknown-agent-kind",
            "invalid-agent-spec",
            "worker-binding-missing",
            "unresolved-agent-ref",
            "duplicate-agent-name",
            "unresolved-operator-ref",
            "unresolved-meta-ref",
            "step-naming-collision",
            "invalid-projection-placement",
            "unresolved-audit-agent",
            "verdict-channel-mismatch",
            "verdict-value-not-in-contract",
            "verdict-value-unhandled",
        ];
        for kind in kinds {
            assert!(
                mlua_swarm_diag::lint_decl(kind).is_some(),
                "kind '{kind}' emitted by From<&CompileError> has no LINT_DECLS entry"
            );
        }
    }

    #[test]
    fn invalid_spec_with_worker_binding_prefix_specializes_the_diagnostic_kind() {
        // The factory's message construction and the From matcher share
        // WORKER_BINDING_REQUIRED_MSG_PREFIX, so building the error the
        // way the factory does must hit the specialized arm.
        let err = CompileError::InvalidSpec {
            name: "greeter".into(),
            msg: format!("{WORKER_BINDING_REQUIRED_MSG_PREFIX}. Fix by either: (a) ..."),
        };
        let d = mlua_swarm_diag::Diagnostic::from(&err);
        assert_eq!(d.kind, "worker-binding-missing");
        assert_eq!(d.level, mlua_swarm_diag::DiagLevel::Error);
        assert!(matches!(d.stage, mlua_swarm_diag::DiagStage::CompileLint));
        assert!(d.message.contains("greeter"));
        let suggestion = d
            .suggestion
            .expect("specialized arm must carry a suggestion");
        assert!(suggestion.patch.contains("backend = \"ws_operator\""));
        assert_eq!(
            suggestion.applicability,
            mlua_swarm_diag::Applicability::HasPlaceholders
        );
        assert_eq!(
            d.docs_ref.expect("docs_ref must be set").uri,
            "mse://guides/bp-dsl-templates"
        );
        match d.span.expect("span must be set").element {
            mlua_swarm_diag::DiagElement::Agent { name } => assert_eq!(name, "greeter"),
            other => panic!("expected Agent span, got {other:?}"),
        }
    }

    #[test]
    fn generic_invalid_spec_maps_to_the_generic_kind() {
        let err = CompileError::InvalidSpec {
            name: "solo".into(),
            msg: "operator spec: 'operator_ref' (string) required".into(),
        };
        let d = mlua_swarm_diag::Diagnostic::from(&err);
        assert_eq!(d.kind, "invalid-agent-spec");
        assert!(
            d.suggestion.is_none(),
            "generic arm carries no canned patch"
        );
    }

    #[test]
    fn verdict_value_not_in_contract_diagnostic_carries_suggestion_and_span() {
        let err = CompileError::VerdictValueNotInContract {
            where_: "Branch cond".into(),
            agent: "review".into(),
            value: "NOT_DECLARED".into(),
            values: vec!["PASS".into(), "BLOCKED".into()],
        };
        let d = mlua_swarm_diag::Diagnostic::from(&err);
        assert_eq!(d.kind, "verdict-value-not-in-contract");
        assert!(d.message.contains("NOT_DECLARED"));
        assert!(d.suggestion.is_some());
        match d.span.expect("span must be set").element {
            mlua_swarm_diag::DiagElement::Agent { name } => assert_eq!(name, "review"),
            other => panic!("expected Agent span, got {other:?}"),
        }
    }
}

// ─── GH #83: SubprocessDef template hint + placeholder validation ─────────
#[cfg(test)]
mod subprocess_embed_compile_tests {
    use super::*;
    use mlua_swarm_schema::{current_schema_version, SubprocessDef, SubprocessOverrides};

    fn subprocess_agent(name: &str, runner: Option<Runner>) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::Subprocess,
            spec: serde_json::json!({}),
            profile: Some(AgentProfile {
                system_prompt: "you are a headless worker".to_string(),
                model: Some("profile-model".to_string()),
                tools: vec!["Read".to_string()],
                ..Default::default()
            }),
            meta: None,
            runner,
            runner_ref: None,
            verdict: None,
            lints: None,
        }
    }

    fn echo_def(name: &str) -> SubprocessDef {
        SubprocessDef {
            name: name.to_string(),
            argv: vec!["sh".to_string(), "-c".to_string(), "cat".to_string()],
            stdin: Some("{prompt}".to_string()),
            env: Default::default(),
            cwd: None,
            output: None,
            stream_mode: None,
        }
    }

    fn bp_with(agents: Vec<AgentDef>, subprocesses: Vec<SubprocessDef>) -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "gh83-ut".into(),
            flow: FlowNode::Seq { children: vec![] },
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
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            subprocesses,
            check_policy: None,
            blueprint_ref_includes: vec![],
        }
    }

    fn subprocess_runner(template: &str) -> Runner {
        Runner::Subprocess {
            template: template.to_string(),
            overrides: SubprocessOverrides::default(),
        }
    }

    #[test]
    fn validate_placeholders_accepts_closed_set_and_json_braces() {
        for ok in [
            "{system} {system_file} {prompt} {model} {tools_csv} {work_dir} {task_id} {attempt}",
            r#"echo '{"result": "ok", "nested": {"a": 1}}'"#,
            "no placeholders at all",
            "unmatched { brace",
        ] {
            validate_embed_placeholders(ok, "ut").expect("must be accepted");
        }
    }

    #[test]
    fn validate_placeholders_rejects_unknown_token() {
        let err = validate_embed_placeholders("--flag {evil}", "argv[1]").unwrap_err();
        assert!(err.contains("'{evil}'"), "token named: {err}");
        assert!(err.contains("closed set"), "closed set listed: {err}");
    }

    /// The scan descends into literal braces — a token nested inside a
    /// JSON-wrapped template string is still validated (mirrors the
    /// spawn-time render scan).
    #[test]
    fn validate_placeholders_descends_into_literal_braces() {
        validate_embed_placeholders(r#"{"task": "{prompt}"}"#, "stdin")
            .expect("nested closed-set token must be accepted");
        let err = validate_embed_placeholders(r#"{"task": "{evil}"}"#, "stdin").unwrap_err();
        assert!(
            err.contains("'{evil}'"),
            "nested unknown token caught: {err}"
        );
    }

    #[test]
    fn hint_resolution_finds_declared_template() {
        let agent = subprocess_agent("headless", Some(subprocess_runner("echo")));
        let bp = bp_with(vec![agent.clone()], vec![echo_def("echo")]);
        let hint = resolve_subprocess_template_hint(&bp, &agent)
            .expect("resolves")
            .expect("Runner::Subprocess must synthesize a hint");
        assert_eq!(hint[SUBPROCESS_TEMPLATE_HINT_KEY]["name"], "echo");
        assert!(hint.get(SUBPROCESS_OVERRIDES_HINT_KEY).is_some());
    }

    #[test]
    fn hint_resolution_unknown_template_is_invalid_spec() {
        let agent = subprocess_agent("headless", Some(subprocess_runner("nope")));
        let bp = bp_with(vec![agent.clone()], vec![echo_def("echo")]);
        let err = resolve_subprocess_template_hint(&bp, &agent).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("'nope'"), "missing template named: {msg}");
        assert!(msg.contains("echo"), "defined templates listed: {msg}");
    }

    #[test]
    fn hint_resolution_none_without_subprocess_runner() {
        let agent = subprocess_agent("headless", None);
        let bp = bp_with(vec![agent.clone()], vec![echo_def("echo")]);
        let hint = resolve_subprocess_template_hint(&bp, &agent).expect("resolves");
        assert!(hint.is_none(), "spec-based agents keep the historical path");
    }

    // ─── GH #86: AgentBlock tool-grant hint ───────────────────────────────
    //
    // Sibling of the `resolve_subprocess_template_hint` cases above; the
    // shared `bp_with` / `Blueprint` fixture is why these live in the same
    // module rather than a third one.

    fn agent_block_agent(name: &str, runner: Option<Runner>, profile_tools: &[&str]) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::AgentBlock,
            spec: serde_json::json!({}),
            profile: Some(AgentProfile {
                system_prompt: "you are an in-process auditor".to_string(),
                tools: profile_tools.iter().map(|t| t.to_string()).collect(),
                ..Default::default()
            }),
            meta: None,
            runner,
            runner_ref: None,
            verdict: None,
            lints: None,
        }
    }

    fn agent_block_runner(tools: &[&str]) -> Runner {
        Runner::AgentBlockInProcess {
            tools: tools.iter().map(|t| t.to_string()).collect(),
        }
    }

    /// The AgentBlock tool grant reaches the factory through the
    /// `BoundAgent` projection, NOT through a build hint: a declared
    /// `Runner::AgentBlockInProcess` overwrites `profile.tools` with its own
    /// list. Asserting on the projection is what pins the contract, since a
    /// hint for this axis would bypass the pinned snapshot on resume.
    #[test]
    fn agent_block_runner_tools_are_projected_over_profile_tools() {
        let agent = agent_block_agent(
            "auditor",
            Some(agent_block_runner(&["mcp__outline__list_docs"])),
            &["Read"],
        );
        let bp = bp_with(vec![agent], vec![]);
        let bound = resolve_bound_agents(&bp).expect("binds");
        let effective = materialize_bound_blueprint(&bp, &bound);
        assert_eq!(
            effective.agents[0].profile.as_ref().unwrap().tools,
            vec!["mcp__outline__list_docs".to_string()],
            "the declared Runner tools replace profile.tools (['Read'])"
        );
    }

    /// A declared-but-empty `tools` list is an enforced-empty grant: the
    /// projection must still overwrite, or an agent.md's inherited `tools:`
    /// line would silently survive a Blueprint that meant to revoke it.
    #[test]
    fn agent_block_projection_distinguishes_declared_empty_from_absent() {
        let declared = agent_block_agent("auditor", Some(agent_block_runner(&[])), &["Read"]);
        let bp = bp_with(vec![declared], vec![]);
        let bound = resolve_bound_agents(&bp).expect("binds");
        let effective = materialize_bound_blueprint(&bp, &bound);
        assert!(
            effective.agents[0]
                .profile
                .as_ref()
                .unwrap()
                .tools
                .is_empty(),
            "empty means enforced-empty, not 'unset'"
        );

        let absent = agent_block_agent("auditor", None, &["Read"]);
        let bp = bp_with(vec![absent], vec![]);
        let bound = resolve_bound_agents(&bp).expect("binds");
        let effective = materialize_bound_blueprint(&bp, &bound);
        assert_eq!(
            effective.agents[0].profile.as_ref().unwrap().tools,
            vec!["Read".to_string()],
            "no Runner declared → the agent.md tools line stands"
        );
    }

    /// End-to-end through `Compiler::compile`: the projected grant reaches
    /// `AgentBlockInProcessSpawnerFactory::build`, whose ScriptBasedAgent
    /// guard rejects an unenforceable MCP grant. A successful build returns
    /// an opaque `Arc<dyn SpawnerAdapter>`, so this negative path is the
    /// compile-level assertion available; the positive paths are covered in
    /// `worker::agent_block::runtime`'s tests.
    #[test]
    fn compile_rejects_script_mode_with_a_declared_mcp_grant() {
        let mut agent = agent_block_agent(
            "auditor",
            Some(agent_block_runner(&["mcp__outline__list_docs"])),
            &[],
        );
        agent.spec = serde_json::json!({ "script_path": "gate.lua" });
        let mut bp = bp_with(vec![agent], vec![]);
        bp.strategy.strict_refs = false;

        let mut registry = SpawnerRegistry::new();
        registry.register::<crate::worker::agent_block::AgentBlockInProcessSpawnerFactory>(
            Arc::new(crate::worker::agent_block::AgentBlockInProcessSpawnerFactory::new()),
        );
        // `CompiledBlueprint` is not `Debug`, so `expect_err` is unavailable.
        let err = match Compiler::new(registry).compile(&bp) {
            Err(e) => e,
            Ok(_) => panic!("script mode + declared MCP grant must not compile"),
        };
        let msg = format!("{err}");
        assert!(msg.contains("script_path"), "names the trigger: {msg}");
        assert!(
            msg.contains("mcp__outline__list_docs"),
            "names the unenforceable tools: {msg}"
        );
    }

    /// The guard must not catch a script-mode agent whose tools are all
    /// inert (non-`mcp__`) — that shape compiled before the guard existed
    /// and grants nothing this backend can enforce either way.
    #[test]
    fn compile_accepts_script_mode_with_only_inert_tools() {
        let mut agent = agent_block_agent("auditor", None, &["Read", "WebSearch"]);
        agent.spec = serde_json::json!({ "script_path": "gate.lua" });
        let mut bp = bp_with(vec![agent], vec![]);
        bp.strategy.strict_refs = false;

        let mut registry = SpawnerRegistry::new();
        registry.register::<crate::worker::agent_block::AgentBlockInProcessSpawnerFactory>(
            Arc::new(crate::worker::agent_block::AgentBlockInProcessSpawnerFactory::new()),
        );
        if let Err(e) = Compiler::new(registry).compile(&bp) {
            panic!("inert tools must not trip the MCP-grant guard: {e}");
        }
    }

    #[test]
    fn build_embed_rejects_unknown_placeholder() {
        let agent = subprocess_agent("headless", None);
        let mut def = echo_def("echo");
        def.argv.push("--x={evil}".to_string());
        let err = SubprocessProcessSpawnerFactory::build_embed(
            &agent,
            &serde_json::to_value(&def).unwrap(),
            None,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("'{evil}'"));
    }

    #[test]
    fn build_embed_rejects_output_with_stream_mode() {
        let agent = subprocess_agent("headless", None);
        let mut def = echo_def("echo");
        def.stream_mode = Some("ndjson_lines".to_string());
        def.output = Some(mlua_swarm_schema::SubprocessOutput {
            format: Some("json".to_string()),
            result_ptr: None,
            ok_from: None,
            stats: None,
        });
        let err = SubprocessProcessSpawnerFactory::build_embed(
            &agent,
            &serde_json::to_value(&def).unwrap(),
            None,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("plain-mode"));
    }

    #[test]
    fn build_embed_rejects_malformed_result_ptr_and_ok_from() {
        let agent = subprocess_agent("headless", None);
        let mut def = echo_def("echo");
        def.output = Some(mlua_swarm_schema::SubprocessOutput {
            format: None,
            result_ptr: Some("result".to_string()),
            ok_from: None,
            stats: None,
        });
        let err = SubprocessProcessSpawnerFactory::build_embed(
            &agent,
            &serde_json::to_value(&def).unwrap(),
            None,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("JSON Pointer"));

        let mut def = echo_def("echo");
        def.output = Some(mlua_swarm_schema::SubprocessOutput {
            format: None,
            result_ptr: None,
            ok_from: Some("status".to_string()),
            stats: None,
        });
        let err = SubprocessProcessSpawnerFactory::build_embed(
            &agent,
            &serde_json::to_value(&def).unwrap(),
            None,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("exit_code"));
    }

    #[test]
    fn build_embed_bakes_profile_with_override_precedence() {
        let agent = subprocess_agent("headless", None);
        let def = echo_def("echo");
        let overrides = SubprocessOverrides {
            model: Some("override-model".to_string()),
            tools: vec!["Bash".to_string(), "Write".to_string()],
            cwd: Some("/tmp/override-wd".to_string()),
        };
        let sp = SubprocessProcessSpawnerFactory::build_embed(
            &agent,
            &serde_json::to_value(&def).unwrap(),
            Some(&serde_json::to_value(&overrides).unwrap()),
        )
        .expect("builds");
        let embed = sp.embed.as_ref().expect("embed template baked");
        assert_eq!(embed.model.as_deref(), Some("override-model"));
        assert_eq!(embed.tools_csv, "Bash,Write");
        assert_eq!(embed.cwd.as_deref(), Some("/tmp/override-wd"));
        assert_eq!(
            embed.system_prompt.as_deref(),
            Some("you are a headless worker")
        );
    }
}
