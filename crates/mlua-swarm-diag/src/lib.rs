//! Unified Clippy-style diagnostic model for the mlua-swarm
//! authoring-time feedback surface (GH #79).
//!
//! ## Why this crate exists
//!
//! MSE checks the same class of Blueprint invariants at several stages —
//! compile-lint (`mse bp build` / `bp_build`), post-register lint
//! (`bp_doctor`), launch pre-flight, runtime, `$file`/`$agent_md` ref
//! resolution — and each stage historically produced its own ad-hoc
//! shape (a typed `CompileError` enum, a substring-matched `FixHint`,
//! per-family `serde_json::Value` blobs). This crate is the shared
//! vocabulary those stages converge on, mirroring Clippy's design:
//!
//! | Clippy          | here                          |
//! |-----------------|-------------------------------|
//! | `Level`         | [`DiagLevel`]                 |
//! | `Diagnostic`    | [`Diagnostic`]                |
//! | `Suggestion`    | [`Suggestion`]                |
//! | `Applicability` | [`Applicability`]             |
//! | `Lint` registry | [`LintDecl`] / [`LINT_DECLS`] |
//! | `#[allow]` / `#[warn]` / `#[deny]` | [`LintSetting`] / [`LintConfig`] / [`resolve_level`] |
//!
//! ## Boundary discipline
//!
//! This crate depends on **no other mlua-swarm crate** (serde only), so
//! it can sit between producers (`mlua-swarm`'s
//! `impl From<&CompileError> for Diagnostic`) and consumers
//! (`mlua-swarm-cli`'s `bp_doctor` response, renderers, future
//! `mse bp fix` tooling) without either side coupling to the other's
//! internals. Historically the CLI re-parsed the compiler's
//! pre-formatted error strings via `contains(...)` precisely to avoid
//! that coupling — this crate is the typed replacement for that
//! substring seam.
//!
//! ## Adding a new lint
//!
//! The 4-step recipe (declare in [`LINT_DECLS`] → document → produce →
//! test) is documented in the bundled MCP resource
//! `mse://guides/lint-diagnostic-model`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Severity of one [`Diagnostic`]. Mirrors Clippy's `Level`, collapsed
/// to the three bands the existing MSE surfaces actually use
/// (`bp_doctor`'s `INFO`/`WARN`/`BLOCK` strings, compile-lint's hard
/// errors).
///
/// The same lint kind may fire at different levels per stage: e.g.
/// `worker-binding-missing` is [`DiagLevel::Error`] at
/// [`DiagStage::CompileLint`] (blocks register) but
/// [`DiagLevel::Warn`] at `bp_doctor` (report-only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagLevel {
    /// Informational — not a defect (e.g. a capability-manifest
    /// requirement listing). Never escalates an aggregate verdict.
    Info,
    /// Advisory defect — report-only, nothing is blocked.
    Warn,
    /// Hard failure — the producing stage refuses to proceed
    /// (compile-lint fail, register refusal).
    Error,
}

/// Confidence grade of a [`Suggestion`]'s `patch` — the gate a future
/// auto-apply surface (`mse bp fix`) switches on. Mirrors rustc /
/// Clippy's `Applicability` verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Applicability {
    /// The patch can be applied mechanically with no human review.
    MachineApplicable,
    /// The patch is plausible but may not be what the author meant —
    /// apply only with review.
    MaybeIncorrect,
    /// The patch contains placeholder tokens (e.g. `<subagent-type>`)
    /// the author must fill in before it is valid.
    HasPlaceholders,
    /// No confidence judgment was made.
    Unspecified,
}

/// Stable pointer into the bundled `mse://` docs surface. `uri` is
/// `&'static str` on purpose: every docs target is a bundled MCP
/// resource compiled into the binary, so a dynamic string here would
/// only enable dangling references. The `mse://` scheme invariant is
/// asserted by this crate's tests over [`LINT_DECLS`]. Serialize-only
/// (like [`Diagnostic`]): the `&'static str` fields make this a
/// produce-side type — wire consumers read the JSON generically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DocsRef {
    /// Bundled resource URI, e.g. `"mse://guides/bp-dsl-templates"`.
    pub uri: &'static str,
    /// Optional section anchor within the guide (e.g. `"#worker-binding"`).
    pub anchor: Option<&'static str>,
}

/// Which Blueprint element a [`DiagSpan`] points at. Serialized
/// internally tagged (`{"type": "Agent", "name": "greeter"}`) so
/// downstream renderers can switch on `type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DiagElement {
    /// An `agents[]` entry, addressed by `AgentDef.name`.
    Agent {
        /// The agent's declared name.
        name: String,
    },
    /// A flow `Step` node, addressed by its `ref` (agent name).
    Step {
        /// The step's `ref` value.
        #[serde(rename = "ref")]
        ref_: String,
    },
    /// An `operators[]` entry, addressed by `OperatorDef.name`.
    Operator {
        /// The operator role's declared name.
        name: String,
    },
    /// A `metas[]` entry, addressed by `MetaDef.name`.
    Meta {
        /// The meta pool entry's declared name.
        name: String,
    },
    /// The Blueprint document itself (Blueprint-scoped finding).
    BlueprintRoot,
}

/// Location of a [`Diagnostic`] inside the Blueprint document — MSE's
/// analogue of a source span. Blueprints are JSON documents, not source
/// text, so the "span" is an element identity plus an optional JSONPath
/// refinement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagSpan {
    /// The Blueprint element this diagnostic is about.
    pub element: DiagElement,
    /// Optional JSONPath into the Blueprint document, e.g.
    /// `"$.agents[?(@.name=='greeter')].profile.worker_binding"`.
    pub json_path: Option<String>,
}

/// A concrete recovery suggestion attached to a [`Diagnostic`].
/// Successor of the CLI's `FixHint` (`reason` / `patch_suggestion`)
/// with an explicit [`Applicability`] confidence grade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Suggestion {
    /// One-line imperative statement of what to add or change.
    pub msg: String,
    /// The patch text the author would paste in place. Format-agnostic —
    /// single-line snippet or multi-line block; renderers indent it as a
    /// code block.
    pub patch: String,
    /// Confidence grade — the gate an auto-apply surface switches on.
    pub applicability: Applicability,
}

/// The `bp_doctor` lint family a [`DiagStage::BpDoctor`] diagnostic
/// belongs to. One variant per family the tool reports today; new
/// families (e.g. a `ContextPolicyLint` for GH #78) land here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BpDoctorFamily {
    /// agent.md size check (bytes / lines vs. authoring-guide targets).
    AgentMdSize,
    /// GH #45: phantom MCP tool references in `profile.tools`.
    ToolLint,
    /// GH #45: missing / malformed `profile.extras.expected_output`.
    OutputContractLint,
    /// GH #61: operator-kind agent without the compile-required
    /// `profile.worker_binding`.
    WorkerBindingLint,
    /// C4: Blueprint-scoped operator-binding advisories.
    BindingLint,
    /// GH #76: Skip-tier / `skip_on` DSL cross-checks.
    SkipOnLint,
    /// Wrapper-side tool drift (`bp_explain_agent`'s `tool_drift`
    /// wrapper-only classification).
    WrapperOnly,
    /// GH #78: `context_policy` / projection-root vs launch-seed
    /// cross-checks (silent `file_path: null` prevention).
    ContextPolicyLint,
    /// Findings about the `lints` declaration itself rather than about
    /// the Blueprint's behavior — the meta-lints `unknown-lint-kind` and
    /// `non-suppressible-lint` (rustc's `unknown_lints` analogue).
    LintControl,
    /// Declared `agents[].verdict` contracts nothing downstream reads —
    /// the reverse-direction verdict lint, surfaced report-only on an
    /// already-registered Blueprint (the compile stage only fires it under
    /// `metadata.strict_verdict_handling`).
    VerdictContractLint,
}

/// Which producing stage emitted a [`Diagnostic`]. Serialized
/// internally tagged (`{"type": "BpDoctor", "family": "WorkerBindingLint"}`)
/// so downstream renderers can switch on `type` without knowing every
/// family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DiagStage {
    /// Compile lint (`mse bp build` / `bp_build` / `mse bp lint`) —
    /// the `Compiler::compile` gate.
    CompileLint,
    /// Post-register lint (`bp_doctor`), qualified by lint family.
    BpDoctor {
        /// The `bp_doctor` family this diagnostic belongs to.
        family: BpDoctorFamily,
    },
    /// Task-launch pre-flight validation (producers land with GH #78).
    LaunchPreflight,
    /// Runtime diagnostics (declared for completeness; converting the
    /// existing runtime error paths is out of GH #79's scope).
    Runtime,
    /// `$file` / `$agent_md` ref resolution (the include cascade).
    RefResolve,
}

/// One diagnostic finding — the unified shape every MSE feedback stage
/// produces. Field-by-field mirror of Clippy's `Diagnostic`, adapted to
/// a JSON-document world (see [`DiagSpan`]). Serialize-only: `kind`
/// (and any [`DocsRef`]) are `&'static str` registry keys, so this
/// type is constructed by producers, never parsed back — wire
/// consumers read the JSON generically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Diagnostic {
    /// Machine-readable stable lint key (e.g. `"worker-binding-missing"`).
    /// Every kind has a matching [`LintDecl`] in [`LINT_DECLS`];
    /// downstream tooling switches on this, never on message text.
    pub kind: &'static str,
    /// The producing stage.
    pub stage: DiagStage,
    /// Severity at this stage (may differ from the kind's
    /// [`LintDecl::default_level`] — see [`DiagLevel`]).
    pub level: DiagLevel,
    /// Primary human-readable statement of the finding.
    pub message: String,
    /// Additional context lines (rendered after the message).
    pub notes: Vec<String>,
    /// Optional "how to think about fixing this" line — prose guidance,
    /// distinct from the concrete [`Suggestion`] patch.
    pub help: Option<String>,
    /// Optional concrete recovery patch with confidence grade.
    pub suggestion: Option<Suggestion>,
    /// Optional pointer to the bundled guide covering this lint.
    pub docs_ref: Option<DocsRef>,
    /// Optional location inside the Blueprint document.
    pub span: Option<DiagSpan>,
}

impl Diagnostic {
    /// Minimal constructor; use the `with_*` builders for the optional
    /// fields.
    pub fn new(
        kind: &'static str,
        stage: DiagStage,
        level: DiagLevel,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            stage,
            level,
            message: message.into(),
            notes: Vec::new(),
            help: None,
            suggestion: None,
            docs_ref: None,
            span: None,
        }
    }

    /// Append one note line.
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }

    /// Set the help line.
    pub fn with_help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    /// Attach a concrete recovery suggestion.
    pub fn with_suggestion(mut self, suggestion: Suggestion) -> Self {
        self.suggestion = Some(suggestion);
        self
    }

    /// Attach a docs pointer.
    pub fn with_docs_ref(mut self, docs_ref: DocsRef) -> Self {
        self.docs_ref = Some(docs_ref);
        self
    }

    /// Attach a Blueprint-document span.
    pub fn with_span(mut self, span: DiagSpan) -> Self {
        self.span = Some(span);
        self
    }
}

/// Coarse lint grouping — Clippy-style categories, collapsed to the
/// five MSE needs as a baseline (finer user-facing `#[allow]`-style
/// control is a declared non-goal of GH #79).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LintCategory {
    /// The Blueprint is wrong — it will fail to compile, register, or
    /// dispatch (dangling refs, duplicate names, type mismatches).
    Correctness,
    /// The Blueprint is probably wrong — the shape is legal but the
    /// declared intent and the flow disagree (dead guards, unhandled
    /// verdict values).
    Suspicious,
    /// Authoring-quality concerns that never affect execution
    /// (agent.md size targets).
    Style,
    /// A declared machine-readable contract is missing or violated
    /// (verdict contracts, output contracts, worker bindings, tool
    /// registries).
    Contract,
    /// A deprecated surface is still in use; a replacement exists
    /// (legacy `profile.worker_binding` fallback).
    Migration,
}

impl LintCategory {
    /// The lowercase name this category is addressed by in a
    /// [`LintConfig`] group key (`"category:<name>"`).
    pub fn as_key(&self) -> &'static str {
        match self {
            Self::Correctness => "correctness",
            Self::Suspicious => "suspicious",
            Self::Style => "style",
            Self::Contract => "contract",
            Self::Migration => "migration",
        }
    }
}

/// Static declaration of one lint kind — the registry row. One
/// [`LintDecl`] per kind, whatever stages that kind fires at (the
/// per-stage level lives on the emitted [`Diagnostic`]; the decl
/// carries the default).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LintDecl {
    /// Machine-readable stable lint key ([`Diagnostic::kind`] values
    /// match exactly one entry).
    pub kind: &'static str,
    /// The level this lint fires at when the producing stage does not
    /// override it.
    pub default_level: DiagLevel,
    /// Coarse grouping.
    pub category: LintCategory,
    /// One-line description for docs generation / `--help lints`-style
    /// listings.
    pub desc: &'static str,
    /// The bundled guide covering this lint.
    pub docs_ref: DocsRef,
}

/// Shorthand for the [`LINT_DECLS`] table below.
const fn decl(
    kind: &'static str,
    default_level: DiagLevel,
    category: LintCategory,
    desc: &'static str,
    docs_uri: &'static str,
) -> LintDecl {
    LintDecl {
        kind,
        default_level,
        category,
        desc,
        docs_ref: DocsRef {
            uri: docs_uri,
            anchor: None,
        },
    }
}

/// The static lint registry: one entry per lint kind across every
/// stage. This is the single source that enumerates all lints for docs
/// generation and future opt-out surfaces; [`Diagnostic::kind`] values
/// are drawn from here (asserted by the producing crates' tests).
///
/// Ordering: compile-lint kinds first (one per `CompileError` variant,
/// plus the two kinds shared with other stages), then the `bp_doctor`
/// family kinds.
pub const LINT_DECLS: &[LintDecl] = &[
    // ─── CompileLint stage (one per CompileError variant) ────────────
    decl(
        "bound-agent-resolution",
        DiagLevel::Error,
        LintCategory::Correctness,
        "Runner / Agent / Context binding failed before any spawner was built.",
        "mse://guides/blueprint-authoring",
    ),
    decl(
        "unknown-agent-kind",
        DiagLevel::Error,
        LintCategory::Correctness,
        "An AgentDef.kind has no matching SpawnerRegistry entry under strategy.strict_kind.",
        "mse://guides/blueprint-authoring",
    ),
    decl(
        "invalid-agent-spec",
        DiagLevel::Error,
        LintCategory::Correctness,
        "An AgentDef.spec shape does not match what the factory for its kind requires.",
        "mse://guides/blueprint-authoring",
    ),
    decl(
        "worker-binding-missing",
        DiagLevel::Error,
        LintCategory::Contract,
        "An operator-kind agent lacks the worker binding its backend requires \
         (Error at compile-lint; Warn at bp_doctor).",
        "mse://guides/bp-dsl-templates",
    ),
    decl(
        "unresolved-agent-ref",
        DiagLevel::Error,
        LintCategory::Correctness,
        "The flow references an agent name with no corresponding AgentDef.",
        "mse://guides/blueprint-authoring",
    ),
    decl(
        "duplicate-agent-name",
        DiagLevel::Error,
        LintCategory::Correctness,
        "Two AgentDefs in the same Blueprint share a name.",
        "mse://guides/blueprint-authoring",
    ),
    decl(
        "unresolved-operator-ref",
        DiagLevel::Error,
        LintCategory::Correctness,
        "An agent's spec.operator_ref matches no OperatorDef.name in Blueprint.operators.",
        "mse://guides/operator-execution-model",
    ),
    decl(
        "unresolved-meta-ref",
        DiagLevel::Error,
        LintCategory::Correctness,
        "An AgentMeta.meta_ref or $step_meta.ref names an undefined MetaDef.",
        "mse://guides/operator-execution-model",
    ),
    decl(
        "step-naming-collision",
        DiagLevel::Error,
        LintCategory::Correctness,
        "Two Steps' canonical/alias projection names collide with a declared projection_name.",
        "mse://guides/operator-execution-model",
    ),
    decl(
        "invalid-projection-placement",
        DiagLevel::Error,
        LintCategory::Correctness,
        "Blueprint.projection_placement failed validation.",
        "mse://guides/operator-execution-model",
    ),
    decl(
        "unresolved-audit-agent",
        DiagLevel::Error,
        LintCategory::Correctness,
        "An audits[].agent name matches no AgentDef.name in Blueprint.agents.",
        "mse://guides/operator-execution-model",
    ),
    decl(
        "verdict-channel-mismatch",
        DiagLevel::Error,
        LintCategory::Contract,
        "A Branch/Loop cond addresses a contract-bearing agent's verdict on the wrong \
         OUTPUT channel (body vs part).",
        "mse://guides/blueprint-authoring",
    ),
    decl(
        "verdict-value-not-in-contract",
        DiagLevel::Error,
        LintCategory::Contract,
        "A Branch/Loop cond Lit is not a member of the agent's declared verdict.values.",
        "mse://guides/blueprint-authoring",
    ),
    decl(
        "verdict-value-unhandled",
        DiagLevel::Error,
        LintCategory::Suspicious,
        "A declared verdict.values member is never referenced by any downstream cond \
         (compile stage: Error, only under metadata.strict_verdict_handling; bp_doctor \
         verdict_contract_lint family: Warn, always).",
        "mse://guides/blueprint-authoring",
    ),
    decl(
        "verdict-contract-never-read",
        DiagLevel::Warn,
        LintCategory::Suspicious,
        "Every declared verdict.values entry on an agent is unhandled — no downstream \
         cond reads any of them, so the contract is decorative and the step cannot halt \
         the flow. Per-agent aggregate of `verdict-value-unhandled`; separates a full \
         gate loss from the per-value baseline that fires on the always-unread PASS token.",
        "mse://guides/blueprint-authoring",
    ),
    decl(
        "halted-at-missing",
        DiagLevel::Error,
        LintCategory::Correctness,
        "The flow declares a halt-on rule but no halted_at sink.",
        "mse://guides/bp-dsl-templates",
    ),
    // ─── BpDoctor stage (one per family / finding check) ─────────────
    decl(
        "agent-md-size",
        DiagLevel::Warn,
        LintCategory::Style,
        "An agent's profile.system_prompt exceeds the authoring-guide size targets.",
        "mse://guides/agent-md-authoring",
    ),
    decl(
        "tool-unknown-mcp-ref",
        DiagLevel::Warn,
        LintCategory::Contract,
        "profile.tools references an mcp__mse__ tool absent from the live registry.",
        "mse://guides/mcp-tool-reference",
    ),
    decl(
        "output-contract-missing",
        DiagLevel::Warn,
        LintCategory::Contract,
        "profile.extras.expected_output is absent or malformed (GH #44 convention).",
        "mse://guides/agent-md-authoring",
    ),
    decl(
        "binding-requirements-info",
        DiagLevel::Info,
        LintCategory::Contract,
        "A Runner-backed agent's capability_manifest requirements (informational).",
        "mse://guides/operator-execution-model",
    ),
    decl(
        "strict-binding-without-runners",
        DiagLevel::Warn,
        LintCategory::Suspicious,
        "strategy.strict_binding is true but no agent resolves to a Runner (no-op).",
        "mse://guides/operator-execution-model",
    ),
    decl(
        "legacy-worker-binding",
        DiagLevel::Warn,
        LintCategory::Migration,
        "An agent's Runner resolves from the deprecated profile.worker_binding fallback.",
        "mse://guides/operator-execution-model",
    ),
    decl(
        "binding-resolution-error",
        DiagLevel::Warn,
        LintCategory::Correctness,
        "Runner bindings could not be resolved for the bp_doctor advisory pass.",
        "mse://guides/operator-execution-model",
    ),
    decl(
        "skip-on-missing-for-skip-like-verdict-value",
        DiagLevel::Warn,
        LintCategory::Suspicious,
        "A skip-like verdict value (SKIP / NOT_APPLICABLE / N/A) has no skip_on guard.",
        "mse://guides/skip-tier-and-skip-on",
    ),
    decl(
        "skip-on-declared-but-no-matching-verdict-value",
        DiagLevel::Warn,
        LintCategory::Suspicious,
        "A skip_on list captures a value no agent's verdict.values declares (dead guard).",
        "mse://guides/skip-tier-and-skip-on",
    ),
    decl(
        "skip-on-pattern-conflicts-with-halt-on",
        DiagLevel::Warn,
        LintCategory::Suspicious,
        "The same value appears in both a skip_on list and a halt_on gate cond.",
        "mse://guides/skip-tier-and-skip-on",
    ),
    decl(
        "wrapper-only-drift",
        DiagLevel::Info,
        LintCategory::Contract,
        "The worker wrapper grants tools the Blueprint never declares (informational).",
        "mse://guides/agent-md-authoring",
    ),
    decl(
        "context-policy-strips-projection-roots",
        DiagLevel::Warn,
        LintCategory::Suspicious,
        "An agent's effective context_policy filters out both 'work_dir' and \
         'project_root', so the projection placement root can never resolve \
         (step artifacts materialize with file_path: null).",
        "mse://guides/operator-execution-model",
    ),
    decl(
        "projection-root-seed-missing",
        DiagLevel::Warn,
        LintCategory::Contract,
        "Under the simulated launch payload, no policy-surviving root field \
         (work_dir / project_root) is seeded, so materialized step artifacts \
         will carry file_path: null (content_url-only).",
        "mse://guides/operator-execution-model",
    ),
    // ─── Meta-lints (findings about the lints config itself) ─────────
    decl(
        "unknown-lint-kind",
        DiagLevel::Warn,
        LintCategory::Suspicious,
        "A lints map key matches no declared lint kind, category, or 'all'.",
        "mse://guides/lint-diagnostic-model",
    ),
    decl(
        "non-suppressible-lint",
        DiagLevel::Warn,
        LintCategory::Suspicious,
        "A lints map entry targets a compile-hard-error kind with allow; ignored.",
        "mse://guides/lint-diagnostic-model",
    ),
];

/// Look up the [`LintDecl`] for a kind key. `None` for unknown kinds —
/// producers should treat that as a bug (every emitted
/// [`Diagnostic::kind`] must be declared).
pub fn lint_decl(kind: &str) -> Option<&'static LintDecl> {
    LINT_DECLS.iter().find(|d| d.kind == kind)
}

/// Author-declared level for one lint kind or group — the diag-side twin
/// of `mlua_swarm_schema::LintSetting` (same three lowercase wire
/// literals). The twin exists because this crate depends on no other
/// mlua-swarm crate: the schema owns the Blueprint-side vocabulary, this
/// crate owns resolution, and the consumer (`bp_doctor`) maps one onto
/// the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LintSetting {
    /// Suppress the lint — resolves to [`ResolvedLint::Suppressed`].
    Allow,
    /// Report as advisory — resolves to [`DiagLevel::Warn`].
    Warn,
    /// Escalate to an error — resolves to [`DiagLevel::Error`].
    Deny,
}

impl LintSetting {
    /// Parse the wire spelling (`"allow"` / `"warn"` / `"deny"`, ASCII
    /// case-insensitive). `None` for anything else — the call-site layer
    /// arrives as untyped strings and a typo there must surface as a
    /// meta-lint, never as a rejected request (see
    /// [`LintConfig::from_str_map`]).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "allow" => Some(Self::Allow),
            "warn" => Some(Self::Warn),
            "deny" => Some(Self::Deny),
            _ => None,
        }
    }
}

/// One layer of author-declared lint levels, with the entries that could
/// not be honored kept aside instead of dropped.
///
/// A key selects what its setting applies to: an exact lint kind literal
/// (`"agent-md-size"`), a category group (`"category:style"`, see
/// [`LintCategory::as_key`]), or `"all"`. Keys are validated here
/// against [`LINT_DECLS`]; anything unrecognized — and any entry whose
/// *value* failed to parse — lands in [`LintConfig::unknown_keys`] for
/// the `unknown-lint-kind` meta-lint rather than failing construction.
/// Construction is total: no input shape errors or panics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LintConfig {
    entries: BTreeMap<String, LintSetting>,
    unknown_keys: Vec<String>,
}

impl LintConfig {
    /// Build from already-typed pairs — the Blueprint path, where serde
    /// has already rejected invalid values (the caller maps
    /// `mlua_swarm_schema::LintSetting` onto [`LintSetting`]). Keys are
    /// still validated, so an unknown kind literal in a Blueprint is a
    /// meta-lint, not a register failure.
    pub fn from_pairs(pairs: impl IntoIterator<Item = (String, LintSetting)>) -> Self {
        let mut cfg = Self::default();
        for (key, setting) in pairs {
            cfg.insert(key, Some(setting));
        }
        cfg
    }

    /// Build from the untyped call-site form (`bp_doctor`'s
    /// `lints: {kind: level}` request field). Lenient on both axes: an
    /// unknown key *and* an unparseable value are collected the same way,
    /// so a typo degrades to a meta-lint instead of rejecting the request.
    pub fn from_str_map(map: &BTreeMap<String, String>) -> Self {
        let mut cfg = Self::default();
        for (key, raw) in map {
            cfg.insert(key.clone(), LintSetting::parse(raw));
        }
        cfg
    }

    /// The honored entries (registry-known key, parsed value).
    pub fn entries(&self) -> &BTreeMap<String, LintSetting> {
        &self.entries
    }

    /// Keys this layer declared that matched no lint kind, no
    /// `category:<cat>` group and not `all`, plus keys whose value did
    /// not parse. One `unknown-lint-kind` diagnostic per entry; the
    /// emitting layer is the caller's to name.
    pub fn unknown_keys(&self) -> &[String] {
        &self.unknown_keys
    }

    /// `true` when this layer honors no entry at all — it can never win a
    /// [`resolve_level`] lookup and callers may skip it.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The setting this layer declares for `decl`, applying within-layer
    /// specificity: an exact kind key beats `category:<cat>`, which beats
    /// `all`. `None` = this layer says nothing about `decl`. Kind keys
    /// match the registry literal exactly; the category name after
    /// `category:` is matched ASCII case-insensitively (keys are stored
    /// as the author spelled them, so the meta-lint can quote them back).
    pub fn setting_for(&self, decl: &LintDecl) -> Option<LintSetting> {
        if let Some(s) = self.entries.get(decl.kind) {
            return Some(*s);
        }
        let category = decl.category.as_key();
        let group = self.entries.iter().find(|(key, _)| {
            key.strip_prefix("category:")
                .is_some_and(|cat| cat.eq_ignore_ascii_case(category))
        });
        if let Some((_, s)) = group {
            return Some(*s);
        }
        self.entries.get("all").copied()
    }

    /// Record one entry, routing it to `entries` or `unknown_keys`.
    /// `setting == None` means the value did not parse.
    fn insert(&mut self, key: String, setting: Option<LintSetting>) {
        match setting {
            Some(s) if key_is_known(&key) => {
                self.entries.insert(key, s);
            }
            _ => self.unknown_keys.push(key),
        }
    }
}

/// Whether a [`LintConfig`] key addresses something that exists: a
/// declared kind literal, a `category:<cat>` group with a known category,
/// or `all`. Category names are compared ASCII case-insensitively against
/// [`LintCategory::as_key`].
fn key_is_known(key: &str) -> bool {
    if key == "all" || lint_decl(key).is_some() {
        return true;
    }
    match key.strip_prefix("category:") {
        Some(cat) => LINT_DECLS
            .iter()
            .any(|d| d.category.as_key().eq_ignore_ascii_case(cat)),
        None => false,
    }
}

/// Outcome of resolving one lint kind against the declared layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedLint {
    /// The lint is allowed away: the finding must not fold into a verdict
    /// and must not appear among the reported diagnostics. Consumers move
    /// it to a visible `suppressed[]` array — omitted ≠ passed.
    Suppressed,
    /// The lint fires at this level (the declared override, or the kind's
    /// [`LintDecl::default_level`] when nothing declared it).
    Level(DiagLevel),
}

/// Resolve the effective level of `decl` against the declared layers.
///
/// `layers` is **most-specific first** — the consumer passes
/// `[call_site, agent, blueprint]`, omitting the layers that do not apply
/// to the diagnostic at hand (an agent layer only applies to diagnostics
/// spanning that agent or a step referencing it). The first layer with
/// *any* matching key wins outright, with no merging across layers: this
/// mirrors rustc's attribute-proximity model, where the innermost
/// `#[allow]` decides. Within one layer, specificity orders the keys
/// (exact kind > `category:<cat>` > `all`, see
/// [`LintConfig::setting_for`]). With no match anywhere the kind's
/// registry default level applies.
///
/// This function is deliberately **stage-agnostic**: it maps settings to
/// levels and nothing else. The non-suppressible boundary is the
/// caller's contract and is *stage-scoped*, not kind-scoped — a kind
/// that is a compile-stage hard error ([`is_compile_hard_error`])
/// ignores `allow` / `warn` at [`DiagStage::CompileLint`], but the same
/// kind stays suppressible at `bp_doctor` when that stage emits it at
/// `Warn` (`worker-binding-missing`, `verdict-value-unhandled`). At
/// `bp_doctor`, an exact-kind entry targeting a kind the stage never
/// emits raises the `non-suppressible-lint` meta-lint; `category:` /
/// `all` keys never do. Encoding that here would require a stage
/// argument the resolver has no other use for.
pub fn resolve_level(decl: &LintDecl, layers: &[&LintConfig]) -> ResolvedLint {
    for layer in layers {
        match layer.setting_for(decl) {
            Some(LintSetting::Allow) => return ResolvedLint::Suppressed,
            Some(LintSetting::Warn) => return ResolvedLint::Level(DiagLevel::Warn),
            Some(LintSetting::Deny) => return ResolvedLint::Level(DiagLevel::Error),
            None => continue,
        }
    }
    ResolvedLint::Level(decl.default_level)
}

/// Whether `decl` is a compile-stage hard error rather than a lint —
/// i.e. its [`LintDecl::default_level`] is [`DiagLevel::Error`], so
/// `Compiler::compile` refuses the Blueprint and no author setting can
/// change that *at compile*.
///
/// Callers use this as the guard around [`resolve_level`]'s result,
/// stage-scoped: at [`DiagStage::CompileLint`] an `allow` / `warn` on
/// such a kind is ignored outright. At `bp_doctor` the kind stays
/// suppressible when that stage emits it (at `Warn`); the
/// `non-suppressible-lint` meta-lint fires only for an exact-kind entry
/// on a kind `bp_doctor` never emits, and a `category:` / `all` key
/// skips those without noise.
pub fn is_compile_hard_error(decl: &LintDecl) -> bool {
    matches!(decl.default_level, DiagLevel::Error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lint_decl_kinds_are_unique() {
        let mut seen = std::collections::BTreeSet::new();
        for d in LINT_DECLS {
            assert!(seen.insert(d.kind), "duplicate LintDecl kind: {}", d.kind);
        }
    }

    #[test]
    fn lint_decl_docs_refs_use_the_mse_scheme() {
        for d in LINT_DECLS {
            assert!(
                d.docs_ref.uri.starts_with("mse://"),
                "LintDecl {} docs_ref must use the mse:// scheme, got: {}",
                d.kind,
                d.docs_ref.uri
            );
            assert!(!d.desc.is_empty(), "LintDecl {} desc must be set", d.kind);
        }
    }

    #[test]
    fn lint_decl_lookup_round_trips_every_entry() {
        for d in LINT_DECLS {
            let found = lint_decl(d.kind).expect("every declared kind must resolve");
            assert_eq!(found.kind, d.kind);
        }
        assert!(lint_decl("no-such-lint").is_none());
    }

    #[test]
    fn diagnostic_serializes_to_the_documented_wire_shape() {
        // The GH #79 sample response shape: stage internally tagged with
        // family, unit enums as bare variant-name strings.
        let d = Diagnostic::new(
            "worker-binding-missing",
            DiagStage::BpDoctor {
                family: BpDoctorFamily::WorkerBindingLint,
            },
            DiagLevel::Warn,
            "operator agent 'greeter' lacks worker_binding",
        )
        .with_note("profile.worker_binding is required for the WS thin-path backend")
        .with_help("The WS thin-path operator backend requires worker_binding at dispatch.")
        .with_suggestion(Suggestion {
            msg: "add explicit Runner or profile.worker_binding".into(),
            patch: "runner = { backend = \"ws_operator\", variant = \"claude\", tools = {} }"
                .into(),
            applicability: Applicability::HasPlaceholders,
        })
        .with_docs_ref(DocsRef {
            uri: "mse://guides/bp-dsl-templates",
            anchor: Some("#worker-binding"),
        })
        .with_span(DiagSpan {
            element: DiagElement::Agent {
                name: "greeter".into(),
            },
            json_path: Some("$.agents[?(@.name=='greeter')].profile.worker_binding".into()),
        });

        let v = serde_json::to_value(&d).expect("Diagnostic must serialize");
        assert_eq!(v["kind"], "worker-binding-missing");
        assert_eq!(v["stage"]["type"], "BpDoctor");
        assert_eq!(v["stage"]["family"], "WorkerBindingLint");
        assert_eq!(v["level"], "Warn");
        assert_eq!(v["suggestion"]["applicability"], "HasPlaceholders");
        assert_eq!(v["docs_ref"]["uri"], "mse://guides/bp-dsl-templates");
        assert_eq!(v["span"]["element"]["type"], "Agent");
        assert_eq!(v["span"]["element"]["name"], "greeter");
    }

    #[test]
    fn unit_stage_variants_serialize_internally_tagged() {
        let v = serde_json::to_value(DiagStage::CompileLint).expect("stage must serialize");
        assert_eq!(v["type"], "CompileLint");
        let step = serde_json::to_value(DiagElement::Step {
            ref_: "scout".into(),
        })
        .expect("element must serialize");
        assert_eq!(step["type"], "Step");
        assert_eq!(step["ref"], "scout");
    }

    #[test]
    fn minimal_diagnostic_serializes_optional_fields_as_null_or_empty() {
        // A minimal Diagnostic (no notes / help / suggestion / docs_ref /
        // span) still serializes every field, so wire consumers can rely
        // on key presence rather than probing.
        let d = Diagnostic::new(
            "agent-md-size",
            DiagStage::BpDoctor {
                family: BpDoctorFamily::AgentMdSize,
            },
            DiagLevel::Warn,
            "agent 'planner' system_prompt is 30 KB",
        );
        let v = serde_json::to_value(&d).expect("serialize");
        assert_eq!(v["notes"], serde_json::json!([]));
        assert_eq!(v["help"], serde_json::Value::Null);
        assert_eq!(v["suggestion"], serde_json::Value::Null);
        assert_eq!(v["docs_ref"], serde_json::Value::Null);
        assert_eq!(v["span"], serde_json::Value::Null);
    }

    // ─── lint level control (LintConfig / resolve_level) ─────────────

    /// `agent-md-size` — Warn / Style, the canonical suppressible lint.
    fn style_decl() -> &'static LintDecl {
        lint_decl("agent-md-size").expect("declared")
    }

    /// `worker-binding-missing` — Error / Contract, a compile-stage hard
    /// error.
    fn hard_error_decl() -> &'static LintDecl {
        lint_decl("worker-binding-missing").expect("declared")
    }

    fn cfg(pairs: &[(&str, LintSetting)]) -> LintConfig {
        LintConfig::from_pairs(pairs.iter().map(|(k, v)| (k.to_string(), *v)))
    }

    #[test]
    fn resolve_level_falls_through_to_the_registry_default() {
        assert_eq!(
            resolve_level(style_decl(), &[]),
            ResolvedLint::Level(DiagLevel::Warn)
        );
        let empty = LintConfig::default();
        assert_eq!(
            resolve_level(style_decl(), &[&empty, &empty]),
            ResolvedLint::Level(DiagLevel::Warn)
        );
    }

    #[test]
    fn resolve_level_maps_allow_warn_deny() {
        for (setting, expected) in [
            (LintSetting::Allow, ResolvedLint::Suppressed),
            (LintSetting::Warn, ResolvedLint::Level(DiagLevel::Warn)),
            (LintSetting::Deny, ResolvedLint::Level(DiagLevel::Error)),
        ] {
            let layer = cfg(&[("agent-md-size", setting)]);
            assert_eq!(
                resolve_level(style_decl(), &[&layer]),
                expected,
                "mapping for {setting:?}"
            );
        }
    }

    /// The first layer with any matching key wins outright — an outer
    /// layer never overrides or merges into an inner one.
    #[test]
    fn resolve_level_first_matching_layer_wins() {
        let call_site = cfg(&[("agent-md-size", LintSetting::Allow)]);
        let agent = cfg(&[("agent-md-size", LintSetting::Deny)]);
        let blueprint = cfg(&[("agent-md-size", LintSetting::Warn)]);

        assert_eq!(
            resolve_level(style_decl(), &[&call_site, &agent, &blueprint]),
            ResolvedLint::Suppressed,
            "call-site beats agent"
        );
        assert_eq!(
            resolve_level(style_decl(), &[&agent, &blueprint]),
            ResolvedLint::Level(DiagLevel::Error),
            "agent beats blueprint"
        );
        assert_eq!(
            resolve_level(style_decl(), &[&blueprint]),
            ResolvedLint::Level(DiagLevel::Warn)
        );
    }

    /// A less specific key in a nearer layer still wins over an exact
    /// key further out (proximity is decided before specificity).
    #[test]
    fn resolve_level_layer_proximity_beats_key_specificity() {
        let agent = cfg(&[("all", LintSetting::Allow)]);
        let blueprint = cfg(&[("agent-md-size", LintSetting::Deny)]);
        assert_eq!(
            resolve_level(style_decl(), &[&agent, &blueprint]),
            ResolvedLint::Suppressed
        );
    }

    #[test]
    fn resolve_level_within_a_layer_kind_beats_category_beats_all() {
        let all_three = cfg(&[
            ("agent-md-size", LintSetting::Deny),
            ("category:style", LintSetting::Warn),
            ("all", LintSetting::Allow),
        ]);
        assert_eq!(
            resolve_level(style_decl(), &[&all_three]),
            ResolvedLint::Level(DiagLevel::Error),
            "exact kind wins"
        );

        let category_and_all = cfg(&[
            ("category:style", LintSetting::Warn),
            ("all", LintSetting::Allow),
        ]);
        assert_eq!(
            resolve_level(style_decl(), &[&category_and_all]),
            ResolvedLint::Level(DiagLevel::Warn),
            "category beats all"
        );

        let all_only = cfg(&[("all", LintSetting::Allow)]);
        assert_eq!(
            resolve_level(style_decl(), &[&all_only]),
            ResolvedLint::Suppressed,
            "all is the fallback within the layer"
        );
    }

    /// A `category:` key only matches its own category, and the category
    /// name is compared case-insensitively.
    #[test]
    fn category_keys_match_only_their_own_category() {
        let style_group = cfg(&[("category:style", LintSetting::Allow)]);
        assert_eq!(
            resolve_level(style_decl(), &[&style_group]),
            ResolvedLint::Suppressed
        );
        assert_eq!(
            resolve_level(hard_error_decl(), &[&style_group]),
            ResolvedLint::Level(DiagLevel::Error),
            "a Contract kind is untouched by a Style group key"
        );

        let upper = cfg(&[("category:STYLE", LintSetting::Allow)]);
        assert_eq!(
            resolve_level(style_decl(), &[&upper]),
            ResolvedLint::Suppressed,
            "category names are ASCII case-insensitive"
        );
        assert!(upper.unknown_keys().is_empty());
    }

    #[test]
    fn every_declared_category_is_addressable_as_a_group_key() {
        for d in LINT_DECLS {
            let key = format!("category:{}", d.category.as_key());
            let layer = cfg(&[(key.as_str(), LintSetting::Allow)]);
            assert!(
                layer.unknown_keys().is_empty(),
                "{key} must be a known group key"
            );
            assert_eq!(
                resolve_level(d, &[&layer]),
                ResolvedLint::Suppressed,
                "{} must be reachable through {key}",
                d.kind
            );
        }
    }

    #[test]
    fn unknown_keys_are_collected_not_honored() {
        let layer = cfg(&[
            ("agent-md-size", LintSetting::Allow),
            ("agent-md-sizes", LintSetting::Deny),
            ("category:nonsense", LintSetting::Deny),
            ("everything", LintSetting::Deny),
        ]);
        assert_eq!(
            layer.unknown_keys(),
            ["agent-md-sizes", "category:nonsense", "everything"]
        );
        assert_eq!(layer.entries().len(), 1);
        assert_eq!(
            resolve_level(style_decl(), &[&layer]),
            ResolvedLint::Suppressed,
            "the one honored entry still resolves"
        );
    }

    /// The untyped call-site form never errors: an unparseable value is
    /// collected exactly like an unknown key.
    #[test]
    fn from_str_map_collects_invalid_values_alongside_unknown_keys() {
        let map = BTreeMap::from([
            ("agent-md-size".to_string(), "ALLOW".to_string()),
            ("category:style".to_string(), "forbid".to_string()),
            ("no-such-lint".to_string(), "deny".to_string()),
            ("all".to_string(), "warn".to_string()),
        ]);
        let layer = LintConfig::from_str_map(&map);

        assert_eq!(layer.unknown_keys(), ["category:style", "no-such-lint"]);
        assert_eq!(
            layer.entries().get("agent-md-size"),
            Some(&LintSetting::Allow),
            "values parse ASCII case-insensitively"
        );
        assert_eq!(layer.entries().get("all"), Some(&LintSetting::Warn));
        assert_eq!(
            resolve_level(hard_error_decl(), &[&layer]),
            ResolvedLint::Level(DiagLevel::Warn),
            "the surviving 'all' entry still applies"
        );
    }

    #[test]
    fn empty_config_is_reported_empty_and_never_matches() {
        let empty = LintConfig::from_str_map(&BTreeMap::new());
        assert!(empty.is_empty());
        assert!(empty.unknown_keys().is_empty());
        assert_eq!(empty.setting_for(style_decl()), None);

        let only_unknown =
            LintConfig::from_str_map(&BTreeMap::from([("typo".to_string(), "allow".to_string())]));
        assert!(only_unknown.is_empty(), "unknown keys honor nothing");
        assert_eq!(only_unknown.unknown_keys(), ["typo"]);
    }

    /// The boundary helper is a pure `default_level` probe — the stage
    /// rules on top of it belong to the consumer.
    #[test]
    fn is_compile_hard_error_tracks_the_registry_default_level() {
        assert!(is_compile_hard_error(hard_error_decl()));
        assert!(!is_compile_hard_error(style_decl()));
        for d in LINT_DECLS {
            assert_eq!(
                is_compile_hard_error(d),
                d.default_level == DiagLevel::Error,
                "{}",
                d.kind
            );
        }
    }

    /// The two meta-lints are declared like any other kind, so the
    /// consumer emits them through the same registry lookup.
    #[test]
    fn meta_lints_are_declared_in_the_registry() {
        for kind in ["unknown-lint-kind", "non-suppressible-lint"] {
            let d = lint_decl(kind).expect("meta-lint must be declared");
            assert_eq!(d.default_level, DiagLevel::Warn);
            assert_eq!(d.category, LintCategory::Suspicious);
            assert_eq!(d.docs_ref.uri, "mse://guides/lint-diagnostic-model");
            assert!(!is_compile_hard_error(d));
        }
    }

    #[test]
    fn lint_setting_parses_and_serializes_the_lowercase_wire_form() {
        assert_eq!(LintSetting::parse("allow"), Some(LintSetting::Allow));
        assert_eq!(LintSetting::parse("Warn"), Some(LintSetting::Warn));
        assert_eq!(LintSetting::parse("DENY"), Some(LintSetting::Deny));
        assert_eq!(LintSetting::parse("forbid"), None);
        assert_eq!(LintSetting::parse(""), None);
        assert_eq!(
            serde_json::to_value(LintSetting::Allow).expect("serializes"),
            serde_json::json!("allow")
        );
        assert_eq!(
            serde_json::from_value::<LintSetting>(serde_json::json!("deny")).expect("deserializes"),
            LintSetting::Deny
        );
    }
}
