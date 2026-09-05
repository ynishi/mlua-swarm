//! MCP Resource surface for `mse mcp` — read-only guides + Blueprint
//! samples + the live Blueprint JSON Schema, addressable by URI.
//!
//! Guide and sample bodies are baked via `include_str!` at compile time
//! (no runtime file I/O), and the source `.md` / `.json` files live
//! **inside the crate directory** (`src/mcp/resources/guides/` and
//! `src/mcp/resources/samples/`) so `cargo publish` packages them
//! automatically. The one exception is `mse://api/blueprint-schema`,
//! whose body is generated at `read_resource` time from the same
//! `schemars`-derived [`Blueprint`] schema the `bp_schema` tool returns
//! (see [`blueprint_schema_value`]). One more exception: the `.bp.lua`
//! DSL sample `mse://blueprints/samples/06-dsl-verdict-loop`
//! `include_str!`s directly from `tests/fixtures/` rather than
//! duplicating into `src/mcp/resources/samples/` — it is the exact same
//! file `dsl_json_equivalence_verdict_loop.rs` already build-tests via
//! `dsl::build_bp_from_script`, so a single source of truth guarantees
//! the bundled sample can never silently diverge from what CI proves
//! compiles. (`07-dsl-pipeline` lives in `src/mcp/resources/samples/`
//! like the JSON samples and is build-tested by
//! `bp_lua_sample_bodies_build_via_dsl`.)
//!
//! ## URI scheme
//!
//! ```text
//! mse://guides/<slug>
//! mse://blueprints/samples/<slug>
//! mse://api/blueprint-schema
//! mse://api/http-endpoints
//! mse://api/mcp-tools
//! ```
//!
//! ## Current resources
//!
//! | uri                                       | role                                              |
//! |--------------------------------------------|---------------------------------------------------|
//! | `mse://guides/getting-started`              | Entry points, quickstart, MCP client wiring.       |
//! | `mse://guides/blueprint-authoring`           | Flow node kinds, expr ops, agents, versioning.     |
//! | `mse://guides/mcp-tool-reference`            | All `mse mcp` tools grouped by family.             |
//! | `mse://guides/id-lifecycle`                  | Canonical ID inventory + lifecycle (issue #11).     |
//! | `mse://guides/operator-execution-model`      | 3-hop execution model for `AgentKind::Operator` (WS thin-path). |
//! | `mse://guides/agent-md-authoring`            | SubAgent (agent.md) canonical shape, size targets, fetch-vs-embed policy. |
//! | `mse://guides/dsl-authoring`                 | flow_dsl/bp_dsl authoring DSL: Expr/Node builders, pipeline conventions, JSON→DSL migration SOP. |
//! | `mse://guides/worker-io-contract`            | Worker I/O contract: fetch-based IN, path-free tool-call OUT, server-side file materialization, and the in-process twin of the same contract. |
//! | `mse://guides/bp-dsl-templates`              | `mse bp new` / `bp_new` template inventory + flag surface (GH #62 Axis A). |
//! | `mse://guides/server-management`             | `mse server` subcommand reference, MCP-tool ↔ subcmd mapping, and recovery SOPs (GH #69). |
//! | `mse://guides/blueprint-ref-paths`           | `$file` / `$agent_md` refs, the 6-tier include cascade, and the `--strict-embed` opt-in. |
//! | `mse://guides/bp-lifecycle`                  | Workflow-oriented lifecycle map (develop → trial-run → operate): per-stage feature routing + the `MainAi` vs `Automate` operator contract (GH #80). |
//! | `mse://guides/lint-diagnostic-model`         | Unified Clippy-style `Diagnostic` model + `LINT_DECLS` registry + the 4-step add-a-lint recipe (GH #79). |
//! | `mse://guides/strict-embed-modes`            | The two `strict-embed` layers side by side: client build-time pre-embed vs server register-time raw-ref reject (GH #78 P1b). |
//! | `mse://guides/enhance-flow`                  | Enhance flow (issue → JSON Patch → verify → commit): prerequisites, HTTP surface, the spawner's output contract, and the `EnhanceSetting.spawner` swap. |
//! | `mse://guides/auth-token-model`              | The three credential layers (L0 access token / L1 identity / L2 capability), fail-closed remote binds, and `MSE_ACCESS_TOKEN` client pass-through (GH #101). |
//! | `mse://guides/agent-block-runner`            | Blocks on the caller's host: `ws_operator` + `variant = "agent-block"`, resolved under `MSE_BLOCKS_DIR` by agent name and run by `mse mcp` inside `mse_pending_wait` — no SubAgent, no server-side script path. |
//! | `mse://blueprints/samples/01-pure-ctx-eval`  | Zero-spawn ctx-only Blueprint sample.               |
//! | `mse://blueprints/samples/02-verdict-loop`   | Verdict retry-loop Blueprint sample.                |
//! | `mse://blueprints/samples/03-fn-override`    | Verdict fn-override Blueprint sample.               |
//! | `mse://blueprints/samples/04-after-run-audit-operator` | GH #34 operator-backed after-run audit sample. |
//! | `mse://blueprints/samples/05-after-run-audit-agent-block` | GH #34 agent-block-backed after-run audit sample. |
//! | `mse://blueprints/samples/06-dsl-verdict-loop` | Sample `.bp.lua` — flow_dsl verdict-loop reproduction. |
//! | `mse://blueprints/samples/07-dsl-pipeline`   | Sample `.bp.lua` — bp_dsl verdict-gated pipeline.   |
//! | `mse://blueprints/samples/08-bundled-refs`   | Sample `.bp.lua` — `$agent_md` refs into the bundled `samples/agents/` dir (include cascade). |
//! | `mse://blueprints/samples/09-skip-on-example`| Sample `.bp.lua` — GH #76 DSL sugar `skip_on` DSL sugar (Skip tier). |
//! | `mse://blueprints/samples/10-fanout`         | Sample `.bp.lua` — GH #82 `F.fanout`: one agent fanned out over an item array (one dispatch per item) + aggregate. |
//! | `mse://guides/subprocess-backends`           | GH #83 SubprocessDef CLI invocation templates (EmbedAgent): closed placeholder set, output normalization, Runner::Subprocess binding. |
//! | `mse://blueprints/samples/11-subprocess-embed` | GH #83 sample — two headless workers on different SubprocessDef templates, neutral binaries only. |
//! | `mse://guides/skip-tier-and-skip-on`         | Skip tier semantics + `skip_on = { ... }` DSL surface + `bp_doctor` `skip_on_lint` family + error surface (GH #76). |
//! | `mse://api/blueprint-schema`                 | Live Blueprint JSON Schema (generated per read).    |
//! | `mse://api/http-endpoints`                   | Live HTTP wire-body JSON Schemas, keyed by endpoint (issue #19). |
//! | `mse://api/mcp-tools`                        | Live schemars-generated MCP tool inputSchemas keyed by tool name (GH #24 sibling). |
//!
//! `mse://api/http-endpoints` is deliberately a separate resource from
//! `mse://api/blueprint-schema` — the two schemas serve different
//! readers (HTTP wire body vs. the Blueprint document format) and mixing
//! them into one JSON document would blur that boundary. Fields whose
//! type is the Blueprint document itself (`TaskLaunchRequest.blueprint`)
//! stay opaque here; see [`http_endpoints_schema_value`].

use mlua_swarm::blueprint::Blueprint;

/// How a [`ResourceEntry`] produces its body when read.
pub enum ResourceBody {
    /// Body is baked in at compile time via `include_str!`.
    Static(&'static str),
    /// Body is generated at `read_resource` time (the Blueprint JSON Schema).
    BlueprintSchema,
    /// Body is generated at `read_resource` time (the HTTP wire-body JSON
    /// Schemas, keyed by endpoint; see [`http_endpoints_schema_value`]).
    HttpEndpoints,
    /// Body is generated at `read_resource` time (the MCP tool inputSchemas,
    /// keyed by tool name; see [`mcp_tools_schema_value`]).
    McpTools,
}

/// One MCP Resource entry exposed under the `mse://` scheme.
pub struct ResourceEntry {
    /// Full resource URI, e.g. `"mse://guides/getting-started"`.
    pub uri: &'static str,
    /// Human-readable title (used as the `resources/list` `name`).
    pub title: &'static str,
    /// One-line description shown in `resources/list`.
    pub description: &'static str,
    /// MIME type reported in `resources/list` and `resources/read`.
    pub mime_type: &'static str,
    /// Body source (static or dynamically generated).
    pub body: ResourceBody,
}

const GETTING_STARTED_BODY: &str = include_str!("./resources/guides/getting-started.md");
const BLUEPRINT_AUTHORING_BODY: &str = include_str!("./resources/guides/blueprint-authoring.md");
const MCP_TOOL_REFERENCE_BODY: &str = include_str!("./resources/guides/mcp-tool-reference.md");
const ID_LIFECYCLE_BODY: &str = include_str!("./resources/guides/id-lifecycle.md");
const OPERATOR_EXECUTION_MODEL_BODY: &str =
    include_str!("./resources/guides/operator-execution-model.md");
const AGENT_MD_AUTHORING_BODY: &str = include_str!("./resources/guides/agent-md-authoring.md");
const DSL_AUTHORING_GUIDE_BODY: &str = include_str!("./resources/guides/dsl-authoring.md");
const WORKER_IO_CONTRACT_BODY: &str = include_str!("./resources/guides/worker-io-contract.md");
const REPLAY_AND_RESUME_BODY: &str = include_str!("./resources/guides/replay-and-resume.md");
const BP_DSL_TEMPLATES_BODY: &str = include_str!("./resources/guides/bp-dsl-templates.md");
const SERVER_MANAGEMENT_BODY: &str = include_str!("./resources/guides/server-management.md");
const BLUEPRINT_REF_PATHS_BODY: &str = include_str!("./resources/guides/blueprint-ref-paths.md");
const SKIP_TIER_AND_SKIP_ON_BODY: &str =
    include_str!("./resources/guides/skip-tier-and-skip-on.md");
const BP_LIFECYCLE_BODY: &str = include_str!("./resources/guides/bp-lifecycle.md");
const LINT_DIAGNOSTIC_MODEL_BODY: &str =
    include_str!("./resources/guides/lint-diagnostic-model.md");
const STRICT_EMBED_MODES_BODY: &str = include_str!("./resources/guides/strict-embed-modes.md");
const SUBPROCESS_BACKENDS_BODY: &str = include_str!("./resources/guides/subprocess-backends.md");
const ENHANCE_FLOW_BODY: &str = include_str!("./resources/guides/enhance-flow.md");
const AUTH_TOKEN_MODEL_BODY: &str = include_str!("./resources/guides/auth-token-model.md");
const AGENT_BLOCK_RUNNER_BODY: &str = include_str!("./resources/guides/agent-block-runner.md");

const SAMPLE_01_PURE_CTX_EVAL_BODY: &str =
    include_str!("./resources/samples/01-pure-ctx-eval.json");
const SAMPLE_02_VERDICT_LOOP_BODY: &str = include_str!("./resources/samples/02-verdict-loop.json");
const SAMPLE_03_FN_OVERRIDE_BODY: &str = include_str!("./resources/samples/03-fn-override.json");
const SAMPLE_04_AFTER_RUN_AUDIT_OPERATOR_BODY: &str =
    include_str!("./resources/samples/04-after-run-audit-operator.json");
const SAMPLE_05_AFTER_RUN_AUDIT_AGENT_BLOCK_BODY: &str =
    include_str!("./resources/samples/05-after-run-audit-agent-block.json");
const SAMPLE_06_DSL_VERDICT_LOOP_BODY: &str =
    include_str!("../../tests/fixtures/verdict_loop.bp.lua");
const SAMPLE_07_DSL_PIPELINE_BODY: &str =
    include_str!("./resources/samples/07-dsl-pipeline.bp.lua");
const SAMPLE_08_BUNDLED_REFS_BODY: &str =
    include_str!("./resources/samples/08-bundled-refs.bp.lua");
const SAMPLE_09_SKIP_ON_EXAMPLE_BODY: &str =
    include_str!("./resources/samples/bp/skip-on-example.bp.lua");
const SAMPLE_10_FANOUT_BODY: &str = include_str!("./resources/samples/10-fanout.bp.lua");
const SAMPLE_11_SUBPROCESS_EMBED_BODY: &str =
    include_str!("./resources/samples/11-subprocess-embed.json");

/// Static resource catalogue. Order is the order `list_resources` reports.
pub const RESOURCES: &[ResourceEntry] = &[
    ResourceEntry {
        uri: "mse://guides/getting-started",
        title: "mse — Getting started",
        description: "What mse is, the three entry points (serve / mcp / run), and quickstart snippets.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(GETTING_STARTED_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/blueprint-authoring",
        title: "mse — Blueprint authoring guide",
        description: "Blueprint shape, flow node kinds, expr ops, agents, $agent_md refs, and versioning.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(BLUEPRINT_AUTHORING_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/mcp-tool-reference",
        title: "mse — MCP tool reference",
        description: "All mse mcp tools grouped by family, with side-effect notes.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(MCP_TOOL_REFERENCE_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/id-lifecycle",
        title: "mse — ID lifecycle",
        description: "Canonical inventory of every run-pipeline identifier (Blueprint/Task/Run/Step/Attempt, sid, worker_handle, req_id, capability_token) with mint sites and lifecycle scopes.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(ID_LIFECYCLE_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/operator-execution-model",
        title: "mse — Operator execution model",
        description: "The three-hop execution model for AgentKind::Operator (WS thin-path): Task IF → mse-server splice → MainAI → SubAgent. Explains the responsibility boundary at each hop.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(OPERATOR_EXECUTION_MODEL_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/agent-md-authoring",
        title: "mse — Agent (agent.md) authoring guide",
        description: "SubAgent prompt canonical shape (Role / When invoked / Tool guidance / Output format), Output contract (inline body vs @file: sentinel, opt-in per step), size targets (≤ 200 lines / 25 KB), fetch-vs-embed policy, and anti-patterns.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(AGENT_MD_AUTHORING_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/dsl-authoring",
        title: "mse — Blueprint DSL (flow_dsl / bp_dsl) authoring guide",
        description: "flow_dsl Expr/Node builders, bp_dsl pipeline conventions (default in/out, verdict gate, retry expansion), and a JSON→DSL migration SOP.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(DSL_AUTHORING_GUIDE_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/worker-io-contract",
        title: "mse — Worker I/O contract",
        description: "Why worker IN is one authenticated prompt fetch and OUT is path-free tool calls (submit / artifact?name=), with the server-side projection sink materializing the next step's IN files. Includes the in-process twin of that contract (WorkerInvocation instead of the HTTP fetch, `bus.emit` instead of submit/artifact) and the one deliberate asymmetry: prior-step pointers are out-of-process only. Design rationale + authoring checklist.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(WORKER_IO_CONTRACT_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/replay-and-resume",
        title: "mse — Replay & Resume",
        description: "Ctx-snapshot replay log, SqliteReplayStore config + schema versioning (PRAGMA user_version state machine), POST /v1/runs/:id/resume state-driven endpoint (404/409/422/202), boot recovery sweep + resumable-log hint, and deferred pieces (boot auto-respawn / subprocess-mode E2E).",
        mime_type: "text/markdown",
        body: ResourceBody::Static(REPLAY_AND_RESUME_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/bp-dsl-templates",
        title: "mse — bp_dsl authoring templates (mse bp new)",
        description: "GH #62 Axis A: `mse bp new` / `bp_new` MCP scaffolding — three templates (pipeline / single / verdict) that emit a compile-lint-legal `.bp.lua` with every currently-mandatory field pre-filled (halted_at, explicit ws_operator Runner, strict_refs/strict_kind). Prevention layer for the trap surface that GH #60 / GH #61 sibling fixes tightened.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(BP_DSL_TEMPLATES_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/server-management",
        title: "mse — Server lifecycle management",
        description: "GH #69: `mse server <subcmd>` reference for the full 9-subcommand launchd lifecycle family (install / uninstall / bootstrap / bootout / start / stop / restart / status / logs), the MCP-tool ↔ `mse server` mapping (7 `mlua_swarm_server_*` tools), and recovery SOPs for throttle-backoff / booted-out / uninstalled states. Recovery is closed under the MCP tool surface — no shell access required.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(SERVER_MANAGEMENT_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/blueprint-ref-paths",
        title: "mse — Blueprint ref paths ($file / $agent_md, include cascade)",
        description: "How the linker resolves `$file` and `$agent_md` refs: the 6-tier include cascade (bp.lua parent → in-bp includes → env → CLI `--include` → server config → bundled default), Warn-default behavior on unresolved refs, and the `--strict-embed` / `blueprint_strict_embed` opt-ins at each layer.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(BLUEPRINT_REF_PATHS_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/bp-lifecycle",
        title: "mse — Blueprint lifecycle (develop → trial-run → operate)",
        description: "GH #80: workflow-oriented map of the Blueprint authoring loop. Stage 1 Develop (bp_new templates, `mse bp lint`, `bp_build register=false` fix hints, `bp_doctor` six lint families), Stage 2 Trial-run (`OperatorKind::Automate` is the default; `swarm_run` blocking / `detach: true` + `swarm_status`; rekick vs `resume` vs `rerun-from`), Stage 3 Operate (the `MainAi` contract: the attached operator owns the `mse_pending_wait` → dispatch → `mse_ack` loop by design). Routes each stage to the feature-level reference guides.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(BP_LIFECYCLE_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/lint-diagnostic-model",
        title: "mse — Lint & Diagnostic model (Diagnostic / LINT_DECLS / Applicability)",
        description: "GH #79: the unified Clippy-style diagnostic model every feedback stage produces (compile-lint / bp_doctor / launch pre-flight). Wire shape (stable kind, internally-tagged stage, per-stage level, suggestion with Applicability auto-apply gate, mse:// docs_ref, Blueprint-document span), the mlua-swarm-diag LINT_DECLS registry, where diagnostics surface today (bp_build `diagnostic`, bp_doctor `diagnostics`), allow/warn/deny lint control (3 layers, key grammar, suppressed[], the stage-scoped non-suppressible boundary, disable_*_lint aliases), and the 4-step add-a-lint recipe (declare → document → produce → test).",
        mime_type: "text/markdown",
        body: ResourceBody::Static(LINT_DIAGNOSTIC_MODEL_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/strict-embed-modes",
        title: "mse — The two strict-embed layers (build-time vs register-time)",
        description: "GH #78 P1b: side-by-side comparison of the two switches sharing the `strict-embed` token — `mse bp build --strict-embed` (client, build-time: unresolved refs hard-fail the build) vs `mse serve --blueprint-strict-embed` / `blueprint_strict_embed` config (server, register-time: raw-ref bodies get 400). Covers flag surface, layer, trigger point, effect, failure mode, and the four composition postures.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(STRICT_EMBED_MODES_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/enhance-flow",
        title: "mse — Enhance flow (issue → JSON Patch → verify → commit)",
        description: "The Blueprint self-improvement loop: the four-step enhance-default flow (patch-spawner / patch-applier / fanout verifier-router / committer), the two prerequisites (`mse serve --enable-enhance-flow`, an EnhanceSetting under id `default`), the HTTP surface (/v1/enhance-settings, /v1/issues, /v1/enhance/log), the spawner's output contract (`ops` / `bump` / `rationale`, RFC 6901 pointer rules, fenced-reply folding), and swapping the spawner's execution backend via `EnhanceSetting.spawner` without rewriting the Blueprint.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(ENHANCE_FLOW_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/auth-token-model",
        title: "mse — Auth & token model (three credential layers)",
        description: "The layered credential vocabulary (GH #101): L0 perimeter access token (X-MSE-Access-Token, fail-closed on non-loopback binds), L1 identity (operator session token / worker CapToken / wh- handle on Authorization: Bearer), L2 capability (role × verbs + scopes + seat, server-side), and token_secret as the CapToken signing key — plus the remote-hosting posture (TLS at the edge, MSE_ACCESS_TOKEN client pass-through).",
        mime_type: "text/markdown",
        body: ResourceBody::Static(AUTH_TOKEN_MODEL_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/agent-block-runner",
        title: "mse — Running blocks on the caller's host (`agent-block` launch variant)",
        description: "How a deterministic Lua block step runs against a hosted `mse serve` without a SubAgent: bind the agent to `ws_operator` + `variant = \"agent-block\"`, name it after its block, and `mse mcp` runs `$MSE_BLOCKS_DIR/<name>/init.lua` itself when `mse_pending_wait` pops the spawn — worker fetch, run with the launch's work_dir, artifact / submit POSTs, spawn_ack — with no turn for the MainAI. Script contract (same globals and `bus.emit` rules as the in-process runtime), the join-manifest capability, and when to pick in-process vs caller-side.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(AGENT_BLOCK_RUNNER_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/01-pure-ctx-eval",
        title: "Sample Blueprint — pure ctx eval",
        description: "Zero-spawn pure ctx evaluation using Assign + And + Gt + Lt + Lit primitives.",
        mime_type: "application/json",
        body: ResourceBody::Static(SAMPLE_01_PURE_CTX_EVAL_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/02-verdict-loop",
        title: "Sample Blueprint — verdict loop",
        description: "Verdict retry loop with a self-managed counter (Loop + Branch + Operator agents).",
        mime_type: "application/json",
        body: ResourceBody::Static(SAMPLE_02_VERDICT_LOOP_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/03-fn-override",
        title: "Sample Blueprint — fn override",
        description: "A BLOCKED verdict overridden to ALLOW by an approver step, gating a commit branch.",
        mime_type: "application/json",
        body: ResourceBody::Static(SAMPLE_03_FN_OVERRIDE_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/04-after-run-audit-operator",
        title: "Sample Blueprint — after-run audit (operator)",
        description: "GH #34: an operator-kind `auditor` declared in `audits` is auto-kicked after the `worker` step settles, receiving an ordinary Spawn frame whose directive asks it to audit that step.",
        mime_type: "application/json",
        body: ResourceBody::Static(SAMPLE_04_AFTER_RUN_AUDIT_OPERATOR_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/05-after-run-audit-agent-block",
        title: "Sample Blueprint — after-run audit (agent_block)",
        description: "GH #34: an agent_block-kind `auditor` declared in `audits` runs in-process after the `worker` step settles, with no operator round-trip.",
        mime_type: "application/json",
        body: ResourceBody::Static(SAMPLE_05_AFTER_RUN_AUDIT_AGENT_BLOCK_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/06-dsl-verdict-loop",
        title: "Sample .bp.lua — verdict loop (flow_dsl)",
        description: "Hand-written flow_dsl reproduction of mse://blueprints/samples/02-verdict-loop (loop/branch shape not expressible via bp_dsl's B.pipeline sugar).",
        mime_type: "text/x-lua",
        body: ResourceBody::Static(SAMPLE_06_DSL_VERDICT_LOOP_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/07-dsl-pipeline",
        title: "Sample .bp.lua — verdict-gated pipeline (bp_dsl)",
        description: "B.pipeline{}-built three-stage pipeline: default in/out wiring derived from stage ids, automatic verdict gates, and a bounded fix-and-regate retry loop.",
        mime_type: "text/x-lua",
        body: ResourceBody::Static(SAMPLE_07_DSL_PIPELINE_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/08-bundled-refs",
        title: "Sample .bp.lua — bundled `$agent_md` refs (include cascade)",
        description: "Two-stage research→review pipeline whose agents are supplied by `$agent_md` refs against the bundled `samples/agents/*.md` files. Demonstrates tier 1 (bp.lua parent) and tier 6 (bundled default) of the Blueprint include cascade; see mse://guides/blueprint-ref-paths.",
        mime_type: "text/x-lua",
        body: ResourceBody::Static(SAMPLE_08_BUNDLED_REFS_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/09-skip-on-example",
        title: "Sample .bp.lua — Skip tier + `skip_on` DSL sugar (GH #76 DSL sugar)",
        description: "Three-stage analyst chain (triage -> analyze -> summarize) whose middle stage uses `skip_on = { \"NOT_APPLICABLE\" }` to pre-emptively elide its body when triage's staged verdict part reads NOT_APPLICABLE, letting the pipeline continue to summarize. Runnable via `mse bp build` (embed the agents inline). Full semantics: mse://guides/skip-tier-and-skip-on.",
        mime_type: "text/x-lua",
        body: ResourceBody::Static(SAMPLE_09_SKIP_ON_EXAMPLE_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/10-fanout",
        title: "Sample .bp.lua — fanout over an item array + aggregate (F.fanout, GH #82)",
        description: "GH #82: one `check` agent fanned out over the `$.d.targets` array via `F.fanout` (join = \"all\") — the body holds exactly one step, because it runs once per item — with an aggregate stage consuming the collected `$.results`. Demonstrates the `F.fanout` builder (flow.ir's 7th Node kind, previously reachable only via `F.raw()`) and the bound `$.item`. Heterogeneous lanes (one agent per lane, selected by branching on `$.item`) are what `mse bp new fanout` scaffolds: `mse://guides/bp-dsl-templates`. Lane semantics and how to gate on `$.results`: `mse://guides/blueprint-authoring`.",
        mime_type: "text/x-lua",
        body: ResourceBody::Static(SAMPLE_10_FANOUT_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/subprocess-backends",
        title: "mse — Subprocess backends (SubprocessDef CLI invocation templates)",
        description: "GH #83: declarative per-worker CLI backend descriptors for headless Subprocess workers (EmbedAgent). SubprocessDef fields (argv/stdin/env/cwd/output/stream_mode), the closed logic-free placeholder set ({system}/{system_file}/{prompt}/{model}/{tools_csv}/{work_dir}/{task_id}/{attempt}), output normalization (format/result_ptr/ok_from), Runner::Subprocess binding + overrides, failure semantics, and a runnable neutral-binary example. Adding a new CLI backend = one more Blueprint.subprocesses entry, no spawner code.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(SUBPROCESS_BACKENDS_BODY),
    },
    ResourceEntry {
        uri: "mse://blueprints/samples/11-subprocess-embed",
        title: "Sample Blueprint — Subprocess EmbedAgent templates (GH #83)",
        description: "GH #83: two headless workers backed by different SubprocessDef templates in one Blueprint — one pipes {prompt} to stdin and reads {system_file} back via env, one is a bare echo; both extract their worker result via output.result_ptr. Neutral binaries only (sh) — runnable with no vendor CLI installed. Authoring guide: mse://guides/subprocess-backends.",
        mime_type: "application/json",
        body: ResourceBody::Static(SAMPLE_11_SUBPROCESS_EMBED_BODY),
    },
    ResourceEntry {
        uri: "mse://guides/skip-tier-and-skip-on",
        title: "mse — Skip tier & `skip_on` DSL sugar (GH #76)",
        description: "Skip tier semantics (engine + wire), the two entry paths (runtime `mse_worker_submit --verdict=skip` vs. pre-emptive `skip_on = { ... }` on a `B.stage`), the `bp_doctor` `skip_on_lint` family (BLOCK-disabled by default), and the structured `TaskLaunchError::FlowEval` error surface.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(SKIP_TIER_AND_SKIP_ON_BODY),
    },
    ResourceEntry {
        uri: "mse://api/blueprint-schema",
        title: "Blueprint JSON Schema",
        description: "Live schemars-generated JSON Schema for the Blueprint type. flow is opaque (owned by mlua-flow-ir).",
        mime_type: "application/json",
        body: ResourceBody::BlueprintSchema,
    },
    ResourceEntry {
        uri: "mse://api/http-endpoints",
        title: "HTTP endpoint wire-body JSON Schemas",
        description: "Live schemars-generated request/response JSON Schemas for the /v1/blueprints, /v1/tasks, /v1/runs (including the per-step stats trace), and /v1/worker families, keyed by endpoint. A separate resource from mse://api/blueprint-schema (issue #19).",
        mime_type: "application/json",
        body: ResourceBody::HttpEndpoints,
    },
    ResourceEntry {
        uri: "mse://api/mcp-tools",
        title: "MCP tool inputSchemas",
        description: "Live schemars-generated inputSchema for every tool `mse mcp` exposes, keyed by tool name. External callers get the same wire contract this MCP client validates against — makes schemars any-schema drops (see GH #24) inspectable ahead of time. Full-fat OpenAPI 3.1 for the HTTP surface is a companion follow-up.",
        mime_type: "application/json",
        body: ResourceBody::McpTools,
    },
];

/// Look up a resource entry by its full URI. Returns `None` for unknown URIs.
pub fn find_by_uri(uri: &str) -> Option<&'static ResourceEntry> {
    RESOURCES.iter().find(|r| r.uri == uri)
}

/// Generate the Blueprint JSON Schema (schemars-derived) as a
/// `serde_json::Value`. Shared by the `bp_schema` tool and the
/// `mse://api/blueprint-schema` dynamic resource so both surfaces stay
/// byte-for-byte identical.
pub fn blueprint_schema_value() -> Result<serde_json::Value, serde_json::Error> {
    let schema = schemars::schema_for!(Blueprint);
    serde_json::to_value(&schema)
}

/// Generate the HTTP endpoint wire-body JSON Schemas (issue #19) as a
/// `serde_json::Value`, keyed by endpoint. Shared by the
/// `mse://api/http-endpoints` dynamic resource; regenerated on every call
/// so it never drifts from the wire structs' current shape.
///
/// Form (endpoint-unit map, easy to extend — `must_not_simplify #5`):
/// `{"endpoints": {"<METHOD PATH>": {"request"?, "response"?}}}`. Endpoints
/// whose request body *is* the Blueprint document (`POST
/// /v1/blueprints/:id`) point at `mse://api/blueprint-schema` by URI
/// instead of duplicating that schema here — the two resources stay
/// separate documents (see the module doc / `must_not_simplify #1` /
/// `#6`). `GET /v1/runs` uses the same idiom in prose: its body is an
/// array of the `GET /v1/runs/:id` response, so it carries a `$comment`
/// naming its query params rather than a second copy of `RunRecord`'s
/// `$defs`. Thin endpoints (`doctor` / `healthz`) are out of scope here;
/// adding one later is one more map entry.
pub fn http_endpoints_schema_value() -> Result<serde_json::Value, serde_json::Error> {
    let task_launch_request_schema = schemars::schema_for!(mlua_swarm_server::TaskLaunchRequest);
    let task_launch_request = serde_json::to_value(&task_launch_request_schema)?;
    let task_launch_response_schema = schemars::schema_for!(mlua_swarm_server::TaskLaunchResponse);
    let task_launch_response = serde_json::to_value(&task_launch_response_schema)?;
    let task_detail_response_schema = schemars::schema_for!(mlua_swarm_server::TaskDetailResponse);
    let task_detail_response = serde_json::to_value(&task_detail_response_schema)?;
    let run_kick_request_schema = schemars::schema_for!(mlua_swarm_server::RunKickRequest);
    let run_kick_request = serde_json::to_value(&run_kick_request_schema)?;
    let run_kick_response_schema = schemars::schema_for!(mlua_swarm_server::RunKickResponse);
    let run_kick_response = serde_json::to_value(&run_kick_response_schema)?;
    let run_bindings_response_schema =
        schemars::schema_for!(mlua_swarm_server::RunBindingsExplainResponse);
    let run_bindings_response = serde_json::to_value(&run_bindings_response_schema)?;
    let worker_payload_schema = schemars::schema_for!(mlua_swarm::WorkerPayload);
    let worker_payload = serde_json::to_value(&worker_payload_schema)?;
    let run_record_schema = schemars::schema_for!(mlua_swarm::store::run::RunRecord);
    let run_record = serde_json::to_value(&run_record_schema)?;
    let run_steps_response_schema = schemars::schema_for!(mlua_swarm_server::RunStepsResponse);
    let run_steps_response = serde_json::to_value(&run_steps_response_schema)?;
    let stats_body_schema = schemars::schema_for!(mlua_swarm_server::StatsBody);
    let stats_body = serde_json::to_value(&stats_body_schema)?;
    let degradation_body_schema = schemars::schema_for!(mlua_swarm_server::DegradationBody);
    let degradation_body = serde_json::to_value(&degradation_body_schema)?;
    let run_assignees_response_schema =
        schemars::schema_for!(mlua_swarm_server::handover::RunAssigneesResp);
    let run_assignees_response = serde_json::to_value(&run_assignees_response_schema)?;
    let run_handover_response_schema =
        schemars::schema_for!(mlua_swarm_server::handover::RunHandoverResp);
    let run_handover_response = serde_json::to_value(&run_handover_response_schema)?;
    let run_material_response_schema =
        schemars::schema_for!(mlua_swarm_server::handover::StepMaterialResp);
    let run_material_response = serde_json::to_value(&run_material_response_schema)?;
    let run_acquire_request_schema = schemars::schema_for!(mlua_swarm_server::RunAcquireRequest);
    let run_acquire_request = serde_json::to_value(&run_acquire_request_schema)?;
    let run_acquire_response_schema = schemars::schema_for!(mlua_swarm_server::RunAcquireResponse);
    let run_acquire_response = serde_json::to_value(&run_acquire_response_schema)?;

    Ok(serde_json::json!({
        "endpoints": {
            "POST /v1/blueprints/:id": {
                "request": {
                    "$comment": "Body is a Blueprint document verbatim; see mse://api/blueprint-schema for its schema.",
                    "schema_ref": "mse://api/blueprint-schema",
                },
                "response": {
                    "$comment": "Ad-hoc JSON {id, version, seeded} (201/200); not yet a typed schemars struct.",
                },
            },
            "POST /v1/tasks": {
                "request": task_launch_request,
                "response": task_launch_response,
            },
            "GET /v1/tasks/:id": {
                "response": task_detail_response,
            },
            "POST /v1/tasks/:id/runs": {
                "request": run_kick_request,
                "response": run_kick_response,
            },
            "GET /v1/runs/:id/bindings": {
                "$comment": "Run-scoped AgentProvider explain. Reads only the immutable bound_agents launch snapshot; never re-resolves the current Blueprint. Returns 422 for legacy Runs without that snapshot.",
                "response": run_bindings_response,
            },
            "GET /v1/worker/prompt": {
                "$comment": "Worker self-fetch. Query: `task_id=<StepId>`. Auth: `Authorization: Bearer <worker_handle>` (short handle from the Spawn frame, or full capability_token). Response body = WorkerPayload; `context` carries the AgentContextView (GH #20 Contract C) when AgentContextMiddleware was layered; exactly one of `system` / `system_ref` is populated when a system_prompt was baked (GH #31).",
                "response": worker_payload,
            },
            "GET /v1/runs/:id": {
                "$comment": "One persisted Run. `step_entries[]` is the per-step stats trace, appended once per dispatched step at outcome time and write-once thereafter (in-flight visibility belongs to `GET /v1/runs/:id/trace`). `input_json` is the opaque launch snapshot — its shape is not part of this contract, do not read fields out of it.",
                "response": run_record,
            },
            "GET /v1/runs/:id/steps": {
                "$comment": "The same StepEntry rows `GET /v1/runs/:id` embeds, split out as a sub-resource so a stats poller does not drag the full RunRecord (launch snapshot included) on every poll.",
                "response": run_steps_response,
            },
            "GET /v1/runs": {
                "$comment": "Filtered Run collection, newest-first. Query: `task_id` / `status` (pending|running|done|failed|interrupted) / `limit` / `offset`, all optional. Response body is {\"runs\": [<the GET /v1/runs/:id response>, ...]} — the element schema is not restated here.",
            },
            "GET /v1/runs/:id/assignees": {
                "$comment": "Who holds each of the Run's Operator seats, and which seats nobody holds. Auth: `Authorization: Bearer <token>` of ANY live Operator session (mint one with POST /v1/operators). A seat with no holder is present with `vacant: true` and `holder: null` — it is never omitted, so \"nobody is on this Run\" and \"this response did not report holders\" are different bytes. `seats_source: \"run_current_only\"` means the Blueprint could not be resolved, so declared-but-vacant seats are missing from the list; `note` says why. Read it together with GET /v1/operators before acquiring a seat.",
                "response": run_assignees_response,
            },
            "GET /v1/runs/:id/handover": {
                "$comment": "The four things an Assignee needs to read to decide what to do next, in one call — whether or not it is taking over from anybody. Auth: Bearer of ANY live Operator session, same rule as /assignees. `trace` is a REFERENCE ({route, latest_seq}), not the events: `latest_seq` is the watermark separating what is in this snapshot from what happened after it. `seats` / `seats_source` / `note` are exactly the /assignees body, taken from the same RunRecord read. `unanswered[]` is every request a current holder still owes this Run, each listed ONCE: `slot` / `op` / `generation` name the Operator seat the request was dispatched through and whoever holds it now, and all three are `null` for a request that belongs to no seat (a `hook_before` never passes through a seat, so naming one would be a guess). Each entry carries `final_present` / `final_ok` — whether that (step_id, attempt) ALREADY produced a Final, which is the difference between re-running a step and doubling its side effect — plus a `material_route` pointing at the route below. `unread_seats[]` names a held seat whose holder could not be asked; an empty `unanswered` means every holder was asked and owed nothing, never that nobody was asked. This is a read: no resume, no skip, no retry, and nothing here empties a seat.",
                "response": run_handover_response,
            },
            "GET /v1/runs/:id/material": {
                "$comment": "What one step of a Run needs in order to be run — the second half of \"what do I do next\", pointed at by each `unanswered[].material_route`. Query: `step_id=<StepId>` (required). Auth: Bearer of ANY live Operator session; note that this is a weaker credential than the per-task worker CapToken the same payload is normally fetched with, and minting an Operator session needs no credential at all, so treat the gate as a shape check rather than as confidentiality. Body: `payload` is the same WorkerPayload GET /v1/worker/prompt serves; `run_link` is `confirmed` when the payload's own context names the Run in the path and `unconfirmed` when the payload carries no Run identity to check against (`note` says why); `final_present` / `final_ok` repeat axis 4's first half so this route answers \"what do I do next\" on its own. The Final's VALUE is deliberately not here — presence and the ok flag are what the decision needs, and the value is unbounded. 404 when the step is unknown to the engine or belongs to a different Run.",
                "response": run_material_response,
            },
            "POST /v1/runs/:id/acquire": {
                "$comment": "Take one Operator seat of a Run. NO auth: this route is deliberately ungated, because a bearer must not decide who holds a seat and a handover must never be lockable-out. It also never refuses and never enquires — a held seat is taken from its holder, last writer wins — so nothing on this route prevents a takeover of the wrong Run. What prevents it is reading GET /v1/operators and GET /v1/runs/:id/assignees FIRST; note the asymmetry that those two reads are the Bearer-gated ones. Body: `op` (the OperatorId that becomes the holder, stored verbatim and not checked against the operator registry) / `desc` (mandatory, non-empty — the line a later reader tells two concurrent takeovers apart by) / `slot` (optional: omit when the Blueprint declares exactly one Operator, name it when it declares several, otherwise 400 listing the candidates). Response: `gen` is the generation the new holder is stamped at and the number every later reply for the seat is accepted under; `previous` is the displaced holder, serialized as null rather than skipped when the seat was vacant; `t_discard` reports what happened to that holder's in-flight requests for THIS seat (`discarded: null` = the discard could not be addressed at all, which is not the same as nothing to drop), and is absent exactly when `previous` is null.",
                "request": run_acquire_request,
                "response": run_acquire_response,
            },
            "GET /v1/operators": {
                "$comment": "Every live Operator session with its 記名: `sid` / `joined_at_secs` / `connected`, the join-time `desc` the session wrote about itself (null when it wrote none — the key is always present), and `observed[]`, one entry per Operator seat it has been assigned ({run_id, slot, goal, project_root, work_dir, task_metadata, task_metadata_omitted, text_truncated, at_secs}). Every entry field is bounded so the ring has a stated size: `task_metadata` over 4 KiB is dropped with `task_metadata_omitted: true`, and `goal` / `project_root` / `work_dir` over 1 KiB are cut to fit, ending in `…` with `text_truncated: true`. Auth: Bearer of any live session — this route was unauthenticated before and is now gated. Ordered by `last_activity_secs` descending, then by sid. Query: `limit` (default 50, clamped to 200). Response body is {\"operators\": [...], \"total\": N, \"limit\": N}; `total` is the count before the page cut, and `observed_total > observed.length` means older entries have aged out of the per-session ring. No token or capability manifest is ever on this surface.",
            },
            "POST /v1/worker/stats": {
                "$comment": "Worker self-reported per-attempt stats. Auth: `Authorization: Bearer <worker_handle>` (short handle from the Spawn frame, or full capability_token). `worker_kind` defaults to \"operator\". Every field is optional and an all-empty body is accepted and dropped. Call it BEFORE the attempt's final `POST /v1/worker/submit`: the dispatcher folds the recorded stats into the step's StepEntry at outcome time, so stats arriving after that fold never reach the Run record. 204 on success; 410 once the addressed Run is terminal. Also reachable via `mse_worker_submit`'s `stats` object, which POSTs here before its own submit; aggregate a run's reports with the `swarm_run_stats` tool.",
                "request": stats_body,
            },
            "POST /v1/worker/degradation": {
                "$comment": "Worker-reported tool degradation (GH #32). Same Bearer forms as POST /v1/worker/stats. The server injects `step_ref` / `attempt` / `at`; the persisted shape is a DegradationEntry on `RunRecord.degradations`, readable via GET /v1/runs/:id. 204 on success; 410 once the addressed Run is terminal. Also reachable via `mse_worker_submit`'s `degradations` array.",
                "request": degradation_body,
            },
        },
    }))
}

/// Resolve a resource entry's body as a `String`. Static entries return
/// instantly; the schema entries generate fresh JSON on every call so
/// they never drift from the underlying Rust types.
pub fn body_for(entry: &ResourceEntry) -> Result<String, String> {
    match entry.body {
        ResourceBody::Static(s) => Ok(s.to_string()),
        ResourceBody::BlueprintSchema => {
            let value = blueprint_schema_value().map_err(|e| format!("schema serialize: {e}"))?;
            serde_json::to_string_pretty(&value).map_err(|e| format!("schema stringify: {e}"))
        }
        ResourceBody::HttpEndpoints => {
            let value =
                http_endpoints_schema_value().map_err(|e| format!("schema serialize: {e}"))?;
            serde_json::to_string_pretty(&value).map_err(|e| format!("schema stringify: {e}"))
        }
        ResourceBody::McpTools => {
            let value = mcp_tools_schema_value().map_err(|e| format!("schema serialize: {e}"))?;
            serde_json::to_string_pretty(&value).map_err(|e| format!("schema stringify: {e}"))
        }
    }
}

/// Generate the MCP tool inputSchemas (schemars-derived from every
/// `#[tool]` on `MseServer`) as a `serde_json::Value`, keyed by tool
/// name. Shared by the `mse://api/mcp-tools` dynamic resource;
/// regenerated on every call so it never drifts from the current
/// `tool_router()` output.
///
/// Form (name-unit map, symmetric with [`http_endpoints_schema_value`]):
/// `{"tools": {"<name>": {"description", "input_schema"}}}`. The
/// `input_schema` value is the exact JSON Schema this MCP server
/// validates arguments against — so any schemars any-schema regression
/// (see GH #24) is inspectable to external readers ahead of time.
pub fn mcp_tools_schema_value() -> Result<serde_json::Value, serde_json::Error> {
    let tools = crate::mcp::MseServer::tool_router().list_all();
    let mut entries = serde_json::Map::with_capacity(tools.len());
    for tool in &tools {
        let name = tool.name.to_string();
        entries.insert(
            name,
            serde_json::json!({
                "description": tool.description,
                "input_schema": &tool.input_schema,
            }),
        );
    }
    Ok(serde_json::json!({ "tools": entries }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resources_have_non_empty_uri_and_body() {
        for r in RESOURCES {
            assert!(!r.uri.is_empty(), "uri empty for {}", r.title);
            let body = body_for(r).expect("body must generate");
            assert!(!body.is_empty(), "body empty for {}", r.title);
        }
    }

    #[test]
    fn find_by_uri_round_trip() {
        for r in RESOURCES {
            let found = find_by_uri(r.uri).expect("resource must be found by its own uri");
            assert_eq!(found.uri, r.uri);
        }
    }

    #[test]
    fn find_by_uri_rejects_unknown_uri() {
        assert!(find_by_uri("mse://guides/nonexistent").is_none());
        assert!(find_by_uri("mse://other/getting-started").is_none());
        assert!(find_by_uri("https://example.com").is_none());
    }

    /// GH #83: the bundled subprocess-embed sample must stay a valid
    /// Blueprint document — schema drift in `SubprocessDef` /
    /// `Runner::Subprocess` breaks this test before it breaks a reader.
    #[test]
    fn bundled_sample_subprocess_embed_deserializes_as_blueprint() {
        let bp: Blueprint = serde_json::from_str(SAMPLE_11_SUBPROCESS_EMBED_BODY)
            .expect("bundled 11-subprocess-embed sample must deserialize as a Blueprint");
        assert_eq!(bp.subprocesses.len(), 2, "two templates declared");
        assert_eq!(bp.agents.len(), 2, "two workers declared");
        for agent in &bp.agents {
            let runner = mlua_swarm::blueprint::resolve_runner(&bp, agent)
                .expect("resolve_runner")
                .expect("every sample agent declares a Runner");
            let mlua_swarm::blueprint::Runner::Subprocess { template, .. } = runner else {
                panic!("sample agents must resolve to Runner::Subprocess");
            };
            assert!(
                bp.subprocesses.iter().any(|d| d.name == template),
                "template '{template}' must be declared in Blueprint.subprocesses"
            );
        }
    }

    #[test]
    fn blueprint_schema_resource_generates_valid_json() {
        let entry = find_by_uri("mse://api/blueprint-schema").expect("schema resource must exist");
        let body = body_for(entry).expect("schema resource body generation must succeed");
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("body must be valid JSON");
        assert!(
            parsed.get("properties").is_some(),
            "schema must expose properties"
        );
    }

    #[test]
    fn http_endpoints_resource_generates_valid_json_with_expected_endpoints() {
        let entry = find_by_uri("mse://api/http-endpoints").expect("resource must exist");
        let body = body_for(entry).expect("http-endpoints resource body generation must succeed");
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("body must be valid JSON");
        let endpoints = parsed
            .get("endpoints")
            .expect("body must expose an endpoints map")
            .as_object()
            .expect("endpoints must be a JSON object");
        for key in [
            "POST /v1/blueprints/:id",
            "POST /v1/tasks",
            "GET /v1/tasks/:id",
            "POST /v1/tasks/:id/runs",
            "GET /v1/runs/:id/bindings",
            "GET /v1/worker/prompt",
            "GET /v1/runs/:id",
            "GET /v1/runs/:id/steps",
            "GET /v1/runs",
            "GET /v1/runs/:id/assignees",
            "GET /v1/runs/:id/handover",
            "GET /v1/runs/:id/material",
            "POST /v1/runs/:id/acquire",
            "GET /v1/operators",
            "POST /v1/worker/stats",
            "POST /v1/worker/degradation",
        ] {
            assert!(
                endpoints.contains_key(key),
                "endpoints map must include {key}, got keys: {:?}",
                endpoints.keys().collect::<Vec<_>>()
            );
        }
        // GET /v1/worker/prompt response is the WorkerPayload schema —
        // GH #20 Contract C means the `context` field (AgentContextView)
        // must surface here so authoring readers can discover it without
        // reading the wire struct source directly.
        let worker_prompt_response = &endpoints["GET /v1/worker/prompt"]["response"];
        let worker_props = worker_prompt_response
            .get("properties")
            .expect("GET /v1/worker/prompt response must expose properties");
        for field in ["task_id", "attempt", "agent", "prompt", "context"] {
            assert!(
                worker_props.get(field).is_some(),
                "WorkerPayload schema must expose {field}: {worker_prompt_response}"
            );
        }
        // POST /v1/tasks request schema must expose the TaskLaunchRequest
        // properties, and must NOT inline the Blueprint schema (must_not_simplify #1/#6).
        let tasks_request = &endpoints["POST /v1/tasks"]["request"];
        let props = tasks_request
            .get("properties")
            .expect("POST /v1/tasks request must expose properties");
        assert!(
            props.get("init_ctx").is_some(),
            "TaskLaunchRequest schema must expose init_ctx: {tasks_request}"
        );
        assert!(
            props.get("blueprint").is_some(),
            "TaskLaunchRequest schema must expose blueprint (opaque): {tasks_request}"
        );
        // must_not_simplify #6: the blueprint field stays opaque here — no
        // nested Blueprint-schema properties (e.g. `flow`/`agents`) leak in.
        assert!(
            tasks_request.get("flow").is_none(),
            "Blueprint schema must not be inlined into the http-endpoints resource"
        );
        // POST /v1/blueprints/:id cross-refs the existing blueprint-schema
        // resource instead of duplicating it.
        assert_eq!(
            endpoints["POST /v1/blueprints/:id"]["request"]["schema_ref"],
            serde_json::json!("mse://api/blueprint-schema")
        );
    }

    /// The run-read family and the two worker report routes are the only
    /// published description of the per-step stats WIRE surface: `POST
    /// /v1/worker/stats` is reachable from a tool (`mse_worker_submit`'s
    /// `stats` object) but its body shape and the fold-ordering rule are
    /// documented here, so a worker harness author calling the route
    /// directly discovers them here or not at all.
    #[test]
    fn http_endpoints_resource_publishes_the_run_stats_surface() {
        let entry = find_by_uri("mse://api/http-endpoints").expect("resource must exist");
        let body = body_for(entry).expect("http-endpoints resource body generation must succeed");
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("body must be valid JSON");
        let endpoints = &parsed["endpoints"];

        // GET /v1/runs/:id carries the whole RunRecord ...
        let run_response = &endpoints["GET /v1/runs/:id"]["response"];
        let run_props = run_response
            .get("properties")
            .expect("GET /v1/runs/:id response must expose properties");
        for field in ["step_entries", "degradations", "result_ref", "status"] {
            assert!(
                run_props.get(field).is_some(),
                "RunRecord schema must expose {field}: {run_response}"
            );
        }
        // ... including StepEntry's stats block under `$defs` (schemars 1
        // emits `$defs`, not the draft-07 `definitions` — same convention
        // the mcp-tools resource asserts against).
        let step_entry_props = run_response
            .get("$defs")
            .and_then(|d| d.get("StepEntry"))
            .and_then(|s| s.get("properties"))
            .expect("RunRecord schema must define StepEntry under $defs");
        for field in [
            "usage",
            "duration_ms",
            "worker_kind",
            "model",
            "num_turns",
            "adapter_data",
        ] {
            assert!(
                step_entry_props.get(field).is_some(),
                "StepEntry schema must expose the stats field {field}: {step_entry_props}"
            );
        }

        // GET /v1/runs/:id/steps publishes the same rows standalone.
        assert!(
            endpoints["GET /v1/runs/:id/steps"]["response"]["properties"]
                .get("steps")
                .is_some(),
            "RunStepsResponse schema must expose steps"
        );
        // GET /v1/runs is a cross-ref entry: prose only, no duplicated
        // RunRecord $defs.
        assert!(
            endpoints["GET /v1/runs"].get("response").is_none(),
            "GET /v1/runs must stay a cross-ref entry"
        );

        // POST /v1/worker/stats is a typed request body.
        let stats_props = endpoints["POST /v1/worker/stats"]["request"]
            .get("properties")
            .expect("POST /v1/worker/stats request must expose properties");
        for field in ["usage", "num_turns", "worker_kind", "model"] {
            assert!(
                stats_props.get(field).is_some(),
                "StatsBody schema must expose {field}: {stats_props}"
            );
        }
        let degradation_props = endpoints["POST /v1/worker/degradation"]["request"]
            .get("properties")
            .expect("POST /v1/worker/degradation request must expose properties");
        for field in ["tool", "error", "fallback"] {
            assert!(
                degradation_props.get(field).is_some(),
                "DegradationBody schema must expose {field}: {degradation_props}"
            );
        }
    }

    #[test]
    fn mcp_tools_resource_covers_every_registered_tool_with_a_typed_input_schema() {
        let entry = find_by_uri("mse://api/mcp-tools").expect("resource must exist");
        let body = body_for(entry).expect("mcp-tools resource body generation must succeed");
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("body must be valid JSON");
        let tools = parsed
            .get("tools")
            .expect("body must expose a tools map")
            .as_object()
            .expect("tools must be a JSON object");

        // Coverage: the resource must list exactly the tools the router
        // exposes — no missing entries, no phantom entries. If a future
        // change adds/removes a tool, this asserts the resource stays in
        // sync (drift detector, symmetric with `mcp_tool_reference` guide).
        let registered: std::collections::BTreeSet<String> = crate::mcp::MseServer::tool_router()
            .list_all()
            .iter()
            .map(|t| t.name.to_string())
            .collect();
        let published: std::collections::BTreeSet<String> = tools.keys().cloned().collect();
        assert_eq!(
            registered, published,
            "mse://api/mcp-tools must list exactly the tools the router exposes"
        );

        // GH #24 sibling: every tool entry must carry an `input_schema`
        // whose top-level is a JSON object with a `type` key. The tightest
        // signal that schemars any-schema drops (which render as bare
        // booleans / missing types) are being kept out of the wire
        // contract external readers see.
        for (name, entry) in tools {
            let input_schema = entry
                .get("input_schema")
                .unwrap_or_else(|| panic!("tool {name}: missing input_schema"));
            let obj = input_schema
                .as_object()
                .unwrap_or_else(|| panic!("tool {name}: input_schema must be a JSON object"));
            assert!(
                obj.contains_key("type"),
                "tool {name}: input_schema must declare a top-level `type` key (schemars any-schema regression): {input_schema}"
            );
        }
    }

    #[test]
    fn sample_bodies_deserialize_into_blueprint() {
        // Guards the shipped samples against Blueprint schema drift: every
        // sample must parse as the typed Blueprint, not merely as JSON.
        for uri in [
            "mse://blueprints/samples/01-pure-ctx-eval",
            "mse://blueprints/samples/02-verdict-loop",
            "mse://blueprints/samples/03-fn-override",
            "mse://blueprints/samples/04-after-run-audit-operator",
            "mse://blueprints/samples/05-after-run-audit-agent-block",
        ] {
            let entry = find_by_uri(uri).unwrap_or_else(|| panic!("sample must exist: {uri}"));
            let body = body_for(entry).expect("sample body must generate");
            let bp: Blueprint = serde_json::from_str(&body)
                .unwrap_or_else(|e| panic!("{uri}: not a valid Blueprint: {e}"));
            assert!(
                !bp.id.as_str().is_empty(),
                "{uri}: sample Blueprint must carry a non-empty id"
            );
            mlua_swarm::blueprint::resolve_bound_agents_strict(&bp).unwrap_or_else(|error| {
                panic!("{uri}: bundled sample must not require legacy binding fallback: {error}")
            });
        }
    }

    /// GH #86: the JSON samples must survive the real `Compiler` — not just
    /// `serde` — under the registry `mse bp build` lints with. The
    /// `agent_block` sample is the reason this test exists: `AgentKind::
    /// AgentBlock` had no factory in `lint_registry`, so
    /// `05-after-run-audit-agent-block` could never compile even though the
    /// schema-level test above passed. Any future kind added to a sample but
    /// not to the lint registry fails here instead of at an author's prompt.
    #[test]
    fn json_sample_bodies_compile_under_the_lint_registry() {
        use mlua_swarm::Compiler;

        for uri in [
            "mse://blueprints/samples/01-pure-ctx-eval",
            "mse://blueprints/samples/02-verdict-loop",
            "mse://blueprints/samples/03-fn-override",
            "mse://blueprints/samples/04-after-run-audit-operator",
            "mse://blueprints/samples/05-after-run-audit-agent-block",
        ] {
            let entry = find_by_uri(uri).unwrap_or_else(|| panic!("sample must exist: {uri}"));
            let body = body_for(entry).expect("sample body must generate");
            let bp: Blueprint = serde_json::from_str(&body)
                .unwrap_or_else(|e| panic!("{uri}: not a valid Blueprint: {e}"));
            if let Err(e) = Compiler::new(crate::bp::lint_registry(&bp)).compile(&bp) {
                panic!("{uri}: bundled sample must pass compile lint: {e}");
            }
        }
    }

    /// Every bundled `samples/agents/*.md` must parse via
    /// `mlua_swarm_compile::agent_md::load_file` — they are the tier-6
    /// (`bundled_default`) fallback the CLI linker walks when resolving
    /// `$agent_md` refs. A frontmatter regression here would silently
    /// break every `.bp.lua` that relies on the bundled tier (including
    /// `mse://blueprints/samples/08-bundled-refs`).
    #[test]
    fn bundled_agents_parse_via_agent_md_loader() {
        use mlua_swarm_compile::agent_md;
        use std::path::PathBuf;

        let dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/mcp/resources/samples/agents");
        // README.md is documentation, not an agent — skip it explicitly so
        // this test asserts on the `*.md` entries the loader is meant to
        // consume rather than on `dir_stream::load_dir`'s implicit skip
        // behavior.
        let agent_files = ["bp-review.md", "researcher.md", "reviewer.md"];
        for name in agent_files {
            let path = dir.join(name);
            assert!(path.is_file(), "bundled agent missing: {}", path.display());
            let def = agent_md::load_file(&path, mlua_swarm::blueprint::AgentKind::Operator)
                .unwrap_or_else(|e| panic!("{}: agent_md parse failed: {e}", path.display()));
            assert!(
                !def.name.is_empty(),
                "{}: parsed AgentDef must carry a name",
                path.display()
            );
            let profile = def.profile.as_ref().unwrap_or_else(|| {
                panic!("{}: parsed AgentDef must carry a profile", path.display())
            });
            assert!(
                !profile.system_prompt.is_empty(),
                "{}: profile.system_prompt must be non-empty",
                path.display()
            );
        }
    }

    #[test]
    fn bp_lua_sample_bodies_build_via_dsl() {
        // Guards the shipped `.bp.lua` samples against DSL-surface drift:
        // every sample must actually compile via `dsl::build_bp_from_script`,
        // not merely exist as static text (crux: "bundled samples build
        // drift" — samples that only exist as uncompiled text are not
        // acceptable).
        for uri in [
            "mse://blueprints/samples/06-dsl-verdict-loop",
            "mse://blueprints/samples/07-dsl-pipeline",
            "mse://blueprints/samples/08-bundled-refs",
            "mse://blueprints/samples/09-skip-on-example",
            "mse://blueprints/samples/10-fanout",
        ] {
            let entry = find_by_uri(uri).unwrap_or_else(|| panic!("sample must exist: {uri}"));
            let body = body_for(entry).expect("sample body must generate");
            mlua_swarm_cli::dsl::build_bp_from_script(&body).unwrap_or_else(|e| {
                panic!("{uri}: does not build via dsl::build_bp_from_script: {e}")
            });
        }
    }

    /// GH #82: the bundled fanout sample must dispatch exactly one step
    /// per item. The `body` of a `fanout` runs once per element, so a
    /// `seq` of N steps there is N x N dispatches — a defect invisible in
    /// the sample's prose and in every schema-level check, which is why
    /// the built node shape is asserted directly. `bind` being read by
    /// the step's `in` is the other half: a sample that binds `$.item`
    /// and never reads it teaches the wrong shape.
    #[test]
    fn bundled_sample_fanout_dispatches_one_step_per_lane() {
        let uri = "mse://blueprints/samples/10-fanout";
        let entry = find_by_uri(uri).unwrap_or_else(|| panic!("sample must exist: {uri}"));
        let body = body_for(entry).expect("sample body must generate");
        let value = mlua_swarm_cli::dsl::build_bp_from_script(&body)
            .unwrap_or_else(|e| panic!("{uri}: does not build via dsl::build_bp_from_script: {e}"));

        let fanout = value["flow"]["children"]
            .as_array()
            .expect("flow must be a seq")
            .iter()
            .find(|node| node["kind"] == "fanout")
            .unwrap_or_else(|| panic!("{uri}: sample must contain a fanout node"));
        assert_eq!(
            fanout["body"]["kind"], "step",
            "{uri}: the fanout body must be a single step (it runs once per item): {}",
            fanout["body"]
        );
        assert_eq!(
            fanout["body"]["in"],
            serde_json::json!({"op": "path", "at": "$.item"}),
            "{uri}: the lane step must read the bound item"
        );
        assert_eq!(
            fanout["bind"],
            serde_json::json!({"op": "path", "at": "$.item"})
        );
    }

    /// GH #76 DSL sugar: the bundled skip_on sample must additionally parse
    /// as a valid Blueprint (once through the DSL builder) — guards
    /// against a well-formed `.bp.lua` that emits a shape rejected by
    /// the Blueprint schema (e.g. a missing halted_at, an agent
    /// without a runner). Subtask verify: `bundled_sample_skip_on_example_parses_via_bp_build`.
    #[test]
    fn bundled_sample_skip_on_example_parses_via_bp_build() {
        let uri = "mse://blueprints/samples/09-skip-on-example";
        let entry = find_by_uri(uri).unwrap_or_else(|| panic!("sample must exist: {uri}"));
        let body = body_for(entry).expect("sample body must generate");
        let value = mlua_swarm_cli::dsl::build_bp_from_script(&body)
            .unwrap_or_else(|e| panic!("{uri}: does not build via dsl::build_bp_from_script: {e}"));
        let bp: Blueprint = serde_json::from_value(value)
            .unwrap_or_else(|e| panic!("{uri}: DSL output is not a valid Blueprint: {e}"));
        assert_eq!(bp.id.as_str(), "sample-skip-on-example");
        assert_eq!(
            bp.agents.len(),
            3,
            "sample must declare triager / analyzer / summarizer"
        );
    }

    /// GH #34: the two reference auditor samples must actually declare
    /// `audits`, and each declared `agent` must resolve to an `AgentDef` of
    /// the backend the sample's title promises — guards the guide/sample
    /// pairing against silent drift (e.g. someone flattening `audits` back
    /// to `[]` while editing the flow).
    #[test]
    fn after_run_audit_samples_declare_audits_on_the_expected_backend() {
        use mlua_swarm::blueprint::AgentKind;

        let cases: &[(&str, AgentKind)] = &[
            (
                "mse://blueprints/samples/04-after-run-audit-operator",
                AgentKind::Operator,
            ),
            (
                "mse://blueprints/samples/05-after-run-audit-agent-block",
                AgentKind::AgentBlock,
            ),
        ];
        for case in cases {
            let uri: &str = case.0;
            let expected_auditor_kind: &AgentKind = &case.1;
            let entry = find_by_uri(uri).unwrap_or_else(|| panic!("sample must exist: {uri}"));
            let body = body_for(entry).expect("sample body must generate");
            let bp: Blueprint = serde_json::from_str(&body)
                .unwrap_or_else(|e| panic!("{uri}: not a valid Blueprint: {e}"));
            assert!(!bp.audits.is_empty(), "{uri}: must declare audits");
            for audit in &bp.audits {
                let auditor = bp
                    .agents
                    .iter()
                    .find(|a| a.name == audit.agent)
                    .unwrap_or_else(|| {
                        panic!(
                            "{uri}: audits[].agent {:?} has no matching AgentDef",
                            audit.agent
                        )
                    });
                assert_eq!(
                    &auditor.kind, expected_auditor_kind,
                    "{uri}: auditor agent {:?} kind mismatch",
                    audit.agent
                );
            }
        }
    }

    /// **Every bundled sample stays launchable by `swarm_run` alone.**
    ///
    /// A sample with `kind = operator` agents only reaches an operator if
    /// the launch pins one — an unpinned `POST /v1/tasks` seats nobody, so
    /// its first Operator dispatch fails naming a `Vacant` seat. `swarm_run`
    /// supplies that pin from this process's sole live session, and it
    /// deliberately does **not** supply `operator_slot`: which lane a pin
    /// fills is the Blueprint's business, and the server refuses to guess
    /// when several are declared.
    ///
    /// So the property a bundled sample has to keep is that it declares at
    /// most **one** Operator seat. Declare a second and the sample stops
    /// being launchable by the tool the guides tell readers to launch it
    /// with — it starts requiring a hand-rolled HTTP call naming a lane.
    #[test]
    fn bundled_operator_samples_declare_at_most_one_seat() {
        for entry in RESOURCES.iter().filter(|e| e.uri.contains("/samples/")) {
            let body = body_for(entry).expect("sample body must generate");
            // The `.bp.lua` samples go through the DSL builder first; the
            // JSON ones are already Blueprint-shaped.
            let value: serde_json::Value = match serde_json::from_str(&body) {
                Ok(value) => value,
                Err(_) => match mlua_swarm_cli::dsl::build_bp_from_script(&body) {
                    Ok(value) => value,
                    // Not a Blueprint at all (a guide, a doc): nothing to check.
                    Err(_) => continue,
                },
            };
            let Ok(bp) = serde_json::from_value::<Blueprint>(value) else {
                continue;
            };
            let uses_operator_agents = bp
                .agents
                .iter()
                .any(|agent| agent.kind == mlua_swarm::blueprint::AgentKind::Operator);
            if !uses_operator_agents {
                continue;
            }
            let seats: Vec<&str> = bp.operators.iter().map(|o| o.name.as_str()).collect();
            assert!(
                seats.len() <= 1,
                "{}: an Operator-backed sample must declare at most one seat so a \
                 `swarm_run` auto-pin can fill it without naming a lane; declares {seats:?}",
                entry.uri
            );
        }
    }

    /// Guide ↔ schema drift guard (issue #6, layer 2 AC #4).
    ///
    /// The `blueprint-authoring` guide lists every Expr op / Node kind
    /// with the field names an author writes verbatim. If the upstream
    /// `flow-ir-core` schema renames or removes any of those fields, this
    /// test fails and prompts a guide update — so the guide stays a
    /// trustworthy reference instead of silently drifting.
    ///
    /// The fixture cases live in `ops_fixture_cases` /
    /// `node_kinds_fixture_cases` below and are shared with the layer-3
    /// markdown-parity tests (issue #38) — a single source of truth for
    /// both drift guards.
    #[test]
    fn guide_expr_ops_match_schema_field_names() {
        use mlua_flow_ir::Expr;

        for (op, v) in ops_fixture_cases() {
            serde_json::from_value::<Expr>(v).unwrap_or_else(|e| {
                panic!(
                    "guide Expr op `{op}` does not deserialize with the documented field names: {e} \
                     (fix the blueprint-authoring guide or the guide↔schema mapping)"
                )
            });
        }
    }

    #[test]
    fn guide_flow_node_kinds_match_schema_field_names() {
        use mlua_flow_ir::Node;

        for (kind, v) in node_kinds_fixture_cases() {
            serde_json::from_value::<Node>(v).unwrap_or_else(|e| {
                panic!(
                    "guide Node kind `{kind}` does not deserialize with the documented field names: {e} \
                     (fix the blueprint-authoring guide or the guide↔schema mapping)"
                )
            });
        }
    }

    // ============================================================
    // Shared fixture cases for guide↔schema drift guards.
    //
    // These are the single source of truth for every op / kind the
    // guide documents. They drive:
    //   * layer 2 (issue #6): serde deserialize check — schema drift
    //   * layer 3 (issue #38): guide markdown table field-name parity
    // ============================================================

    fn ops_fixture_cases() -> Vec<(&'static str, serde_json::Value)> {
        vec![
            ("path", serde_json::json!({"op":"path","at":"$.x"})),
            ("lit", serde_json::json!({"op":"lit","value":42})),
            (
                "eq",
                serde_json::json!({"op":"eq","lhs":{"op":"lit","value":1},"rhs":{"op":"lit","value":1}}),
            ),
            (
                "ne",
                serde_json::json!({"op":"ne","lhs":{"op":"lit","value":1},"rhs":{"op":"lit","value":2}}),
            ),
            (
                "lt",
                serde_json::json!({"op":"lt","lhs":{"op":"lit","value":1},"rhs":{"op":"lit","value":2}}),
            ),
            (
                "lte",
                serde_json::json!({"op":"lte","lhs":{"op":"lit","value":1},"rhs":{"op":"lit","value":2}}),
            ),
            (
                "gt",
                serde_json::json!({"op":"gt","lhs":{"op":"lit","value":2},"rhs":{"op":"lit","value":1}}),
            ),
            (
                "gte",
                serde_json::json!({"op":"gte","lhs":{"op":"lit","value":2},"rhs":{"op":"lit","value":1}}),
            ),
            (
                "not",
                serde_json::json!({"op":"not","arg":{"op":"lit","value":true}}),
            ),
            (
                "and",
                serde_json::json!({"op":"and","args":[{"op":"lit","value":true}]}),
            ),
            (
                "or",
                serde_json::json!({"op":"or","args":[{"op":"lit","value":true}]}),
            ),
            (
                "exists",
                serde_json::json!({"op":"exists","arg":{"op":"path","at":"$.x"}}),
            ),
            (
                "add",
                serde_json::json!({"op":"add","lhs":{"op":"lit","value":1},"rhs":{"op":"lit","value":2}}),
            ),
            (
                "sub",
                serde_json::json!({"op":"sub","lhs":{"op":"lit","value":3},"rhs":{"op":"lit","value":1}}),
            ),
            (
                "mul",
                serde_json::json!({"op":"mul","lhs":{"op":"lit","value":2},"rhs":{"op":"lit","value":3}}),
            ),
            (
                "div",
                serde_json::json!({"op":"div","lhs":{"op":"lit","value":6},"rhs":{"op":"lit","value":2}}),
            ),
            (
                "mod",
                serde_json::json!({"op":"mod","lhs":{"op":"lit","value":5},"rhs":{"op":"lit","value":2}}),
            ),
            (
                "len",
                serde_json::json!({"op":"len","arg":{"op":"lit","value":"hi"}}),
            ),
            (
                "in",
                serde_json::json!({"op":"in","needle":{"op":"lit","value":1},"haystack":{"op":"lit","value":[1,2,3]}}),
            ),
            (
                "call_extern",
                serde_json::json!({"op":"call_extern","ref":"math.sqrt","args":[{"op":"lit","value":9}]}),
            ),
        ]
    }

    fn node_kinds_fixture_cases() -> Vec<(&'static str, serde_json::Value)> {
        vec![
            (
                "step",
                serde_json::json!({
                    "kind":"step","ref":"a","in":{"op":"path","at":"$.in"},"out":{"op":"path","at":"$.out"}
                }),
            ),
            ("seq", serde_json::json!({"kind":"seq","children":[]})),
            (
                "branch",
                serde_json::json!({
                    "kind":"branch",
                    "cond":{"op":"lit","value":true},
                    "then":{"kind":"seq","children":[]},
                    "else":{"kind":"seq","children":[]}
                }),
            ),
            (
                "loop",
                serde_json::json!({
                    "kind":"loop",
                    "counter":{"op":"path","at":"$.i"},
                    "cond":{"op":"lit","value":true},
                    "body":{"kind":"seq","children":[]},
                    "max":3
                }),
            ),
            (
                "fanout",
                serde_json::json!({
                    "kind":"fanout",
                    "items":{"op":"lit","value":[1,2]},
                    "bind":{"op":"path","at":"$.item"},
                    "body":{"kind":"seq","children":[]},
                    "join":"all",
                    "out":{"op":"path","at":"$.results"}
                }),
            ),
            (
                "try",
                serde_json::json!({
                    "kind":"try",
                    "body":{"kind":"seq","children":[]},
                    "catch":{"kind":"seq","children":[]},
                    "err_at":{"op":"path","at":"$.err"}
                }),
            ),
            (
                "assign",
                serde_json::json!({
                    "kind":"assign","at":{"op":"path","at":"$.x"},"value":{"op":"lit","value":1}
                }),
            ),
        ]
    }

    // ---- issue #38 layer 3: markdown table parity ----
    //
    // Parse the two field-name tables in the `blueprint-authoring` guide
    // and assert their field-name sets match the fixture cases above
    // (bijection on kind/op names + equal field sets per row).
    //
    // Layer 2 (above) catches upstream schema drift; layer 3 catches the
    // remaining hole where the guide markdown edit and the fixture edit
    // are not made together — the hand-maintained link the fixture-in-Rust
    // approach left open.

    fn extract_first_backticked(s: &str) -> Option<String> {
        let start = s.find('`')?;
        let rest = &s[start + 1..];
        let end = rest.find('`')?;
        Some(rest[..end].to_string())
    }

    fn extract_all_backticked(s: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = s;
        while let Some(start) = rest.find('`') {
            let after_open = &rest[start + 1..];
            let Some(end) = after_open.find('`') else {
                break;
            };
            out.push(after_open[..end].to_string());
            rest = &after_open[end + 1..];
        }
        out
    }

    fn split_top_level_commas(s: &str) -> Vec<&str> {
        let mut out = Vec::new();
        let mut depth: i32 = 0;
        let mut start = 0;
        for (i, c) in s.char_indices() {
            match c {
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                ',' if depth == 0 => {
                    out.push(&s[start..i]);
                    start = i + c.len_utf8();
                }
                _ => {}
            }
        }
        out.push(&s[start..]);
        out
    }

    /// Parse a markdown table in `md` whose header row's first column
    /// equals `header_col1` and whose second column is `fields`. Returns
    /// a map `{ name -> {field-name-set} }`. Multi-op rows (e.g. `` `lt`
    /// / `lte` / `gt` / `gte` ``) expand into one entry per name, all
    /// sharing the row's field set. Trailing `?` on a field marks
    /// optional in the guide — stripped before insertion. Parenthesized
    /// type annotations (e.g. `` `children` (`Node[]`) ``) after a field
    /// name are ignored.
    fn parse_guide_field_sets(
        md: &str,
        header_col1: &str,
    ) -> std::collections::BTreeMap<String, std::collections::BTreeSet<String>> {
        use std::collections::{BTreeMap, BTreeSet};
        let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

        let split_cells = |line: &str| -> Vec<String> {
            line.trim_start()
                .trim_start_matches('|')
                .split('|')
                .map(|c| c.trim().to_string())
                .collect()
        };

        let mut lines = md.lines().peekable();
        while let Some(line) = lines.next() {
            let trimmed = line.trim_start();
            if !trimmed.starts_with('|') {
                continue;
            }
            let cols = split_cells(line);
            if cols.len() < 3 {
                continue;
            }
            if cols[0] != header_col1 || cols[1] != "fields" {
                continue;
            }
            // Header matched. Skip the separator line.
            let Some(sep) = lines.next() else {
                break;
            };
            if !sep.trim_start().starts_with('|') || !sep.contains("---") {
                continue;
            }
            // Collect data rows until the first non-`|` line.
            for row in lines.by_ref() {
                let trimmed = row.trim_start();
                if !trimmed.starts_with('|') {
                    break;
                }
                let cols = split_cells(row);
                if cols.len() < 3 {
                    continue;
                }
                let names = extract_all_backticked(&cols[0]);
                let mut fields: BTreeSet<String> = BTreeSet::new();
                for seg in split_top_level_commas(&cols[1]) {
                    let Some(name) = extract_first_backticked(seg) else {
                        continue;
                    };
                    let name = name.trim_end_matches('?').to_string();
                    if name.is_empty() {
                        continue;
                    }
                    fields.insert(name);
                }
                for name in names {
                    out.insert(name, fields.clone());
                }
            }
            return out;
        }
        out
    }

    fn fixture_field_sets(
        cases: &[(&str, serde_json::Value)],
        discriminator: &str,
    ) -> std::collections::BTreeMap<String, std::collections::BTreeSet<String>> {
        use std::collections::{BTreeMap, BTreeSet};
        let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (name, value) in cases {
            let obj = value
                .as_object()
                .unwrap_or_else(|| panic!("fixture `{name}` must be a JSON object"));
            let fields: BTreeSet<String> = obj
                .keys()
                .filter(|k| k.as_str() != discriminator)
                .cloned()
                .collect();
            out.insert((*name).to_string(), fields);
        }
        out
    }

    #[test]
    fn guide_expr_ops_table_field_names_match_fixtures() {
        let guide = parse_guide_field_sets(BLUEPRINT_AUTHORING_BODY, "op");
        assert!(
            !guide.is_empty(),
            "expected to parse the `op | fields | result` table from mse://guides/blueprint-authoring \
             (did the table header format change?)"
        );
        let cases = ops_fixture_cases();
        let fixture = fixture_field_sets(&cases, "op");

        assert_eq!(
            guide.keys().cloned().collect::<Vec<_>>(),
            fixture.keys().cloned().collect::<Vec<_>>(),
            "Expr op coverage drift between the blueprint-authoring guide table and the fixture cases. \
             Guide ops: {guide:?}. Fixture ops: {fixture:?}."
        );
        for (op, guide_fields) in &guide {
            let fixture_fields = fixture
                .get(op)
                .unwrap_or_else(|| panic!("op `{op}` present in guide but missing from fixtures"));
            assert_eq!(
                guide_fields, fixture_fields,
                "Expr op `{op}`: field-name drift between the guide table and the fixture. \
                 Guide fields: {guide_fields:?}. Fixture fields: {fixture_fields:?}."
            );
        }
    }

    #[test]
    fn guide_flow_node_kinds_table_field_names_match_fixtures() {
        let guide = parse_guide_field_sets(BLUEPRINT_AUTHORING_BODY, "kind");
        assert!(
            !guide.is_empty(),
            "expected to parse the `kind | fields | behavior` table from mse://guides/blueprint-authoring \
             (did the table header format change?)"
        );
        let cases = node_kinds_fixture_cases();
        let fixture = fixture_field_sets(&cases, "kind");

        assert_eq!(
            guide.keys().cloned().collect::<Vec<_>>(),
            fixture.keys().cloned().collect::<Vec<_>>(),
            "Flow node kind coverage drift between the blueprint-authoring guide table and the fixture cases. \
             Guide kinds: {guide:?}. Fixture kinds: {fixture:?}."
        );
        for (kind, guide_fields) in &guide {
            let fixture_fields = fixture.get(kind).unwrap_or_else(|| {
                panic!("kind `{kind}` present in guide but missing from fixtures")
            });
            assert_eq!(
                guide_fields, fixture_fields,
                "Flow node kind `{kind}`: field-name drift between the guide table and the fixture. \
                 Guide fields: {guide_fields:?}. Fixture fields: {fixture_fields:?}."
            );
        }
    }
}
