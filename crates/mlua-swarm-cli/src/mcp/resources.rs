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
//! | `mse://guides/worker-io-contract`            | Worker I/O contract: fetch-based IN, path-free tool-call OUT, server-side file materialization. |
//! | `mse://guides/bp-dsl-templates`              | `mse bp new` / `bp_new` template inventory + flag surface (GH #62 Axis A). |
//! | `mse://blueprints/samples/01-pure-ctx-eval`  | Zero-spawn ctx-only Blueprint sample.               |
//! | `mse://blueprints/samples/02-verdict-loop`   | Verdict retry-loop Blueprint sample.                |
//! | `mse://blueprints/samples/03-fn-override`    | Verdict fn-override Blueprint sample.               |
//! | `mse://blueprints/samples/04-after-run-audit-operator` | GH #34 operator-backed after-run audit sample. |
//! | `mse://blueprints/samples/05-after-run-audit-agent-block` | GH #34 agent-block-backed after-run audit sample. |
//! | `mse://blueprints/samples/06-dsl-verdict-loop` | Sample `.bp.lua` — flow_dsl verdict-loop reproduction. |
//! | `mse://blueprints/samples/07-dsl-pipeline`   | Sample `.bp.lua` — bp_dsl verdict-gated pipeline.   |
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
        description: "Why worker IN is one authenticated prompt fetch and OUT is path-free tool calls (submit / artifact?name=), with the server-side projection sink materializing the next step's IN files. Design rationale + authoring checklist.",
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
        description: "GH #62 Axis A: `mse bp new` / `bp_new` MCP scaffolding — three templates (pipeline / single / verdict) that emit a compile-lint-legal `.bp.lua` with every currently-mandatory field pre-filled (halted_at, worker_binding, strict_refs/strict_kind). Prevention layer for the trap surface that GH #60 / GH #61 sibling fixes tightened.",
        mime_type: "text/markdown",
        body: ResourceBody::Static(BP_DSL_TEMPLATES_BODY),
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
        uri: "mse://api/blueprint-schema",
        title: "Blueprint JSON Schema",
        description: "Live schemars-generated JSON Schema for the Blueprint type. flow is opaque (owned by mlua-flow-ir).",
        mime_type: "application/json",
        body: ResourceBody::BlueprintSchema,
    },
    ResourceEntry {
        uri: "mse://api/http-endpoints",
        title: "HTTP endpoint wire-body JSON Schemas",
        description: "Live schemars-generated request/response JSON Schemas for /v1/blueprints, /v1/tasks, and /v1/tasks/:id/runs, keyed by endpoint. A separate resource from mse://api/blueprint-schema (issue #19).",
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
/// `#6`). Thin endpoints (`doctor` / `healthz`) are out of this
/// subtask's scope; adding one later is one more map entry.
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
    let worker_payload_schema = schemars::schema_for!(mlua_swarm::WorkerPayload);
    let worker_payload = serde_json::to_value(&worker_payload_schema)?;

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
            "GET /v1/worker/prompt": {
                "$comment": "Worker self-fetch. Query: `task_id=<StepId>`. Auth: `Authorization: Bearer <worker_handle>` (short handle from the Spawn frame, or full capability_token). Response body = WorkerPayload; `context` carries the AgentContextView (GH #20 Contract C) when AgentContextMiddleware was layered; exactly one of `system` / `system_ref` is populated when a system_prompt was baked (GH #31).",
                "response": worker_payload,
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
            "GET /v1/worker/prompt",
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
        ] {
            let entry = find_by_uri(uri).unwrap_or_else(|| panic!("sample must exist: {uri}"));
            let body = body_for(entry).expect("sample body must generate");
            mlua_swarm_cli::dsl::build_bp_from_script(&body).unwrap_or_else(|e| {
                panic!("{uri}: does not build via dsl::build_bp_from_script: {e}")
            });
        }
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

    /// Guide ↔ schema drift guard (issue #6, layer 2 AC #4).
    ///
    /// The `blueprint-authoring` guide lists every Expr op / Node kind
    /// with the field names an author writes verbatim. If the upstream
    /// `flow-ir-core` schema renames or removes any of those fields, this
    /// test fails and prompts a guide update — so the guide stays a
    /// trustworthy reference instead of silently drifting.
    ///
    /// Each row is a `(kind_or_op, minimal_json_snippet)` pair. The
    /// snippets use the exact field names documented in the guide.
    #[test]
    fn guide_expr_ops_match_schema_field_names() {
        use mlua_flow_ir::Expr;

        let cases: &[(&str, serde_json::Value)] = &[
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
        ];
        for (op, v) in cases {
            serde_json::from_value::<Expr>(v.clone()).unwrap_or_else(|e| {
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

        let step = serde_json::json!({
            "kind":"step","ref":"a","in":{"op":"path","at":"$.in"},"out":{"op":"path","at":"$.out"}
        });
        let seq = serde_json::json!({"kind":"seq","children":[]});
        let branch = serde_json::json!({
            "kind":"branch",
            "cond":{"op":"lit","value":true},
            "then":{"kind":"seq","children":[]},
            "else":{"kind":"seq","children":[]}
        });
        let loop_n = serde_json::json!({
            "kind":"loop",
            "counter":{"op":"path","at":"$.i"},
            "cond":{"op":"lit","value":true},
            "body":{"kind":"seq","children":[]},
            "max":3
        });
        let fanout = serde_json::json!({
            "kind":"fanout",
            "items":{"op":"lit","value":[1,2]},
            "bind":{"op":"path","at":"$.item"},
            "body":{"kind":"seq","children":[]},
            "join":"all",
            "out":{"op":"path","at":"$.results"}
        });
        let try_n = serde_json::json!({
            "kind":"try",
            "body":{"kind":"seq","children":[]},
            "catch":{"kind":"seq","children":[]},
            "err_at":{"op":"path","at":"$.err"}
        });
        let assign = serde_json::json!({
            "kind":"assign","at":{"op":"path","at":"$.x"},"value":{"op":"lit","value":1}
        });

        for (kind, v) in [
            ("step", step),
            ("seq", seq),
            ("branch", branch),
            ("loop", loop_n),
            ("fanout", fanout),
            ("try", try_n),
            ("assign", assign),
        ] {
            serde_json::from_value::<Node>(v).unwrap_or_else(|e| {
                panic!(
                    "guide Node kind `{kind}` does not deserialize with the documented field names: {e} \
                     (fix the blueprint-authoring guide or the guide↔schema mapping)"
                )
            });
        }
    }
}
