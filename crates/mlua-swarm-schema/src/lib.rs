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
//!         out: Expr::Path { at: "$.greeting".into() },
//!     },
//!     agents: vec![AgentDef {
//!         name: "greeter".into(),
//!         kind: AgentKind::RustFn,
//!         spec: json!({"fn_id": "hello_world"}),
//!         profile: None,
//!         meta: None,
//!     }],
//!     operators: vec![],
//!     hints: Default::default(),
//!     strategy: Default::default(),
//!     metadata: Default::default(),
//!     spawner_hints: Default::default(),
//!     default_agent_kind: AgentKind::Operator,
//!     default_operator_kind: None,
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
            "hints",
            "strategy",
            "metadata",
            "spawner_hints",
            "default_agent_kind",
            "default_operator_kind",
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
}
