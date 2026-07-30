//! GH #86 end-to-end: `AgentKind::AgentBlock` in **ScriptBasedAgent**
//! mode through the full compile → launch → in-process dispatch →
//! result-fold path.
//!
//! This is the no-LLM lane the issue names as its motivation ("running
//! deterministic Lua validation gates (no LLM involved) as in-process
//! fanout lanes of a Blueprint"): `spec.script_path` runs a caller Lua
//! script inside the server process, so nothing here needs an API key or
//! a network. The PromptBasedAgent lane (`spec.script_path` absent) does
//! call a model and is therefore NOT covered by these tests — its
//! compile-side behavior is unit-tested in
//! `src/worker/agent_block/runtime.rs`.
//!
//! Sibling of `subprocess_embed_e2e.rs` (GH #83), same harness shape.

use mlua_swarm::{
    AgentBlockInProcessSpawnerFactory, Compiler, Engine, EngineCfg, Role, SpawnerRegistry,
    TaskInputSpec, TaskLaunchInput, TaskLaunchService,
};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Route `agent-block-core`'s three SQLite backends (`std.kv` / `std.sql`
/// / `std.ts`, each defaulting to a file under `AGENT_BLOCK_HOME`
/// (`$HOME/.agent-block/`)) to the `:memory:` sentinel so file locks
/// disappear entirely. Two axes of contention drove the CI red on
/// 4f5fbfb: (a) cross-test-binary — `cargo test --workspace` opens the
/// same default `$HOME/.agent-block/{kv,db,ts}.sqlite` from several
/// binaries in parallel processes → WAL journal lock; (b) intra-file —
/// this file's 15 `#[tokio::test]`s run parallel on the shared runtime
/// and would still contend inside one tempdir. Per-process tempdir
/// isolation only addressed (a); `:memory:` removes the file layer
/// altogether so (b) is a no-op too. Each SQLite connection open in
/// `:memory:` mode gets its own private in-memory DB — none of these
/// tests need agent-block state to survive across launches, so that
/// per-launch isolation is a feature here.
///
/// `OnceLock` gates the `set_var` calls to exactly once per process,
/// before any AgentBlock `Runtime` reads the env — the config surface
/// re-reads at each open, so setting before the first launch is
/// sufficient.
static AGENT_BLOCK_STATE_ISOLATED: OnceLock<()> = OnceLock::new();

fn isolate_agent_block_state() {
    AGENT_BLOCK_STATE_ISOLATED.get_or_init(|| {
        std::env::set_var("AGENT_BLOCK_KV_PATH", ":memory:");
        std::env::set_var("AGENT_BLOCK_SQL_PATH", ":memory:");
        std::env::set_var("AGENT_BLOCK_TS_PATH", ":memory:");
    });
}

fn service() -> TaskLaunchService {
    isolate_agent_block_state();
    let mut reg = SpawnerRegistry::new();
    reg.register::<AgentBlockInProcessSpawnerFactory>(Arc::new(
        AgentBlockInProcessSpawnerFactory::new(),
    ));
    TaskLaunchService::new(Engine::new(EngineCfg::default()), Compiler::new(reg))
}

fn launch_input(bp: Value, init_ctx: Value) -> TaskLaunchInput {
    isolate_agent_block_state();
    TaskLaunchInput::automate(
        serde_json::from_value(bp).expect("blueprint deserializes"),
        "gh86-e2e-op",
        Role::Operator,
        Duration::from_secs(30),
        init_ctx,
    )
}

/// Materialize a Lua gate script under a unique temp dir and return
/// `(script_path, dir)`. The dir doubles as the agent's `project_root`.
fn write_script(tag: &str, source: &str) -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "gh86-agent-block-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create script dir");
    let path = dir.join("gate.lua");
    std::fs::write(&path, source).expect("write gate script");
    (path, dir)
}

fn script_agent_bp(id: &str, script: &Path, project_root: &Path, verdict: Value) -> Value {
    let mut agent = json!({
        "name": "gate-danger",
        "kind": "agent_block",
        "spec": {
            "script_path": script.display().to_string(),
            "project_root": project_root.display().to_string()
        },
        // Enforced-empty grant: the script owns any connection it opens,
        // which is the only legal script-mode shape (GH #86).
        "runner": {"backend": "agent_block_in_process", "tools": []}
    });
    if !verdict.is_null() {
        agent["verdict"] = verdict;
    }
    json!({
        "schema_version": "0.1.0",
        "id": id,
        "flow": {
            "kind": "step",
            "ref": "gate-danger",
            "in": {"op": "lit", "value": "audit-this-payload"},
            "out": {"op": "path", "at": "$.danger_result"}
        },
        "agents": [agent]
    })
}

/// The core end-to-end claim: a Pure-Lua gate dispatches in-process, the
/// step's evaluated `in` reaches it as the `_PROMPT` global (not via the
/// server process env), and the value it hands back through
/// `bus.emit` lands in the flow ctx at the step's `out` path.
#[tokio::test]
async fn script_mode_gate_dispatches_in_process_and_folds_its_result() {
    let (script, dir) = write_script(
        "roundtrip",
        r#"
bus.emit("worker_result", { ok = true, response = "seen:" .. tostring(_PROMPT) })
"#,
    );
    let bp = script_agent_bp("gh86-script-roundtrip", &script, &dir, Value::Null);
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(
        out.final_ctx["danger_result"],
        json!("seen:audit-this-payload"),
        "the step's `in` must reach the script as _PROMPT and its emit must fold into ctx"
    );
}

/// The scalar-body verdict shape: a gate whose `bus.emit` payload carries
/// a bare token satisfies a `channel: "body"` `VerdictContract`, so the
/// completion-time contract check accepts it and the token is what a
/// downstream cond would compare.
#[tokio::test]
async fn script_mode_gate_satisfies_a_body_channel_verdict_contract() {
    let (script, dir) = write_script(
        "verdict",
        r#"
bus.emit("worker_result", { ok = true, response = "PASS" })
"#,
    );
    let bp = script_agent_bp(
        "gh86-script-verdict",
        &script,
        &dir,
        json!({"channel": "body", "values": ["PASS", "BLOCKED"]}),
    );
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(out.final_ctx["danger_result"], json!("PASS"));
}

/// GH #86: `VerdictChannel::Part` (Pattern B) is reachable from a script.
/// The gate stages a named `verdict` part via the reserved `artifact`
/// emit kind, then finishes with a separate plain-body report — and the
/// completion-time contract check reads the STAGED part, not the body.
/// Before the sink bridge, staging had no route out of this backend and a
/// `channel: "part"` contract always failed with `VerdictPartMissing`.
///
/// The staged part must ALSO reach the flow ctx as `parts.verdict`. The
/// contract check reads the output tail directly, so it was satisfied
/// even while the part was missing from the `{out, parts}` fold — the
/// gate passed and `$.<step>.parts["verdict"]` still read `null`, because
/// only the HTTP staging route registered the name in the fold's
/// worker-owned allowlist. The in-process sink registers it too now.
#[tokio::test]
async fn script_mode_gate_satisfies_a_part_channel_verdict_contract() {
    let (script, dir) = write_script(
        "verdict-part",
        r#"
bus.emit("artifact", { name = "verdict", content = "PASS" })
bus.emit("worker_result", { ok = true, response = "the full prose report" })
"#,
    );
    let bp = script_agent_bp(
        "gh86-script-verdict-part",
        &script,
        &dir,
        json!({"channel": "part", "values": ["PASS", "BLOCKED"]}),
    );
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(
        out.final_ctx["danger_result"],
        json!({
            "out": "the full prose report",
            "parts": {"verdict": "PASS"},
        }),
        "the body stays the report under `out`, and the staged verdict is \
         addressable at `parts.verdict` — the shape a downstream \
         `$.<step>.parts[\"verdict\"]` cond reads"
    );
}

/// Multiple staged parts all fold, keyed by name, and a part staged twice
/// is last-write-wins — the same semantics the HTTP staging route has.
#[tokio::test]
async fn script_mode_multiple_staged_parts_fold_by_name() {
    let (script, dir) = write_script(
        "verdict-parts-many",
        r#"
bus.emit("artifact", { name = "verdict", content = "BLOCKED" })
bus.emit("artifact", { name = "evidence", content = { count = 2 } })
bus.emit("artifact", { name = "verdict", content = "PASS" })
bus.emit("worker_result", { ok = true, response = "report" })
"#,
    );
    let bp = script_agent_bp(
        "gh86-script-verdict-parts-many",
        &script,
        &dir,
        json!({"channel": "part", "values": ["PASS", "BLOCKED"]}),
    );
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(
        out.final_ctx["danger_result"],
        json!({
            "out": "report",
            "parts": {"verdict": "PASS", "evidence": {"count": 2}},
        }),
        "every staged name folds; a name staged twice keeps the last value"
    );
}

/// Lenient fold, end-to-end through the real dispatch: a Lua STRING body
/// / part whose bytes are a JSON container folds structured into the flow
/// ctx with NO declaration — `$.<step>.out.lanes` and
/// `$.<step>.parts["plan-meta.json"].lanes` become addressable. Scalar
/// content (the bare verdict token) stays a string: the containers-only
/// rule (`FoldParse::Lenient` in `mlua_swarm::core::engine`).
#[tokio::test]
async fn script_mode_json_container_string_bodies_fold_structured() {
    let (script, dir) = write_script(
        "lenient-fold",
        r#"
bus.emit("artifact", { name = "plan-meta.json", content = '{"lanes":[{"id":1},{"id":2}]}' })
bus.emit("artifact", { name = "verdict", content = "PASS" })
bus.emit("worker_result", { ok = true, response = '{"lanes":["a","b"]}' })
"#,
    );
    let bp = script_agent_bp("gh-lenient-fold", &script, &dir, Value::Null);
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(
        out.final_ctx["danger_result"],
        json!({
            "out": {"lanes": ["a", "b"]},
            "parts": {
                "plan-meta.json": {"lanes": [{"id": 1}, {"id": 2}]},
                "verdict": "PASS",
            },
        }),
        "JSON-container strings fold structured with no declaration; the \
         scalar verdict token stays a string"
    );
}

/// `submit_format: "text"` declared on the agent's meta channel
/// (`AgentMeta.ctx`) opts the step's fold out of the lenient parse: the
/// same JSON-container strings reach the flow ctx as raw text.
#[tokio::test]
async fn script_mode_text_declared_step_keeps_container_strings_raw() {
    let (script, dir) = write_script(
        "text-optout",
        r#"
bus.emit("artifact", { name = "data.json", content = '{"k":1}' })
bus.emit("worker_result", { ok = true, response = '{"lanes":["a","b"]}' })
"#,
    );
    let mut bp = script_agent_bp("gh-text-optout", &script, &dir, Value::Null);
    bp["agents"][0]["meta"] = json!({"ctx": {"submit_format": "text"}});
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(
        out.final_ctx["danger_result"],
        json!({
            "out": r#"{"lanes":["a","b"]}"#,
            "parts": {"data.json": r#"{"k":1}"#},
        }),
        "a text-declared step folds every string as itself"
    );
}

/// A step that stages nothing keeps the plain body — the pre-GH-#36
/// shape. Registering in-process part names must not start wrapping
/// every in-process step in `{out, parts}`.
#[tokio::test]
async fn script_mode_without_staged_parts_keeps_the_plain_body() {
    let (script, dir) = write_script(
        "no-parts",
        r#"
bus.emit("worker_result", { ok = true, response = "just the body" })
"#,
    );
    let bp = script_agent_bp("gh86-script-no-parts", &script, &dir, Value::Null);
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(out.final_ctx["danger_result"], json!("just the body"));
}

/// The staged part is what the `part` contract validates: a gate staging
/// a token outside its declared `values` fails even though its plain body
/// is unremarkable.
#[tokio::test]
async fn script_mode_gate_staging_an_undeclared_part_verdict_fails_the_step() {
    let (script, dir) = write_script(
        "verdict-part-bad",
        r#"
bus.emit("artifact", { name = "verdict", content = "MAYBE" })
bus.emit("worker_result", { ok = true, response = "looks fine to me" })
"#,
    );
    let bp = script_agent_bp(
        "gh86-script-verdict-part-bad",
        &script,
        &dir,
        json!({"channel": "part", "values": ["PASS", "BLOCKED"]}),
    );
    let result = service().launch(launch_input(bp, json!({}))).await;
    assert!(
        result.is_err(),
        "an undeclared staged verdict must fail the step: {result:?}"
    );
}

/// A `part` contract with nothing staged fails — the gate must not pass
/// just because its body happened to look acceptable.
#[tokio::test]
async fn script_mode_part_contract_without_a_staged_verdict_fails_the_step() {
    let (script, dir) = write_script(
        "verdict-part-missing",
        r#"
bus.emit("worker_result", { ok = true, response = "PASS" })
"#,
    );
    let bp = script_agent_bp(
        "gh86-script-verdict-part-missing",
        &script,
        &dir,
        json!({"channel": "part", "values": ["PASS", "BLOCKED"]}),
    );
    let result = service().launch(launch_input(bp, json!({}))).await;
    assert!(
        result.is_err(),
        "a part contract with no staged verdict must fail: {result:?}"
    );
}

/// A gate emitting a token OUTSIDE its declared `values` is rejected by
/// the completion-time contract check — the enforcement is live on the
/// in-process AgentBlock path, not just on the HTTP submit routes.
///
/// The failure must also NAME the contract it violated. The rejection
/// happens inside `submit_output`, whose `Result` this lane used to
/// discard: the attempt then failed with the bare `no Final in
/// output_tail` symptom and no cause anywhere, not even a log line. See
/// `InProcSpawner::spawn`'s emit block.
#[tokio::test]
async fn script_mode_gate_emitting_an_undeclared_verdict_fails_the_step() {
    let (script, dir) = write_script(
        "verdict-bad",
        r#"
bus.emit("worker_result", { ok = true, response = "MAYBE" })
"#,
    );
    let bp = script_agent_bp(
        "gh86-script-verdict-bad",
        &script,
        &dir,
        json!({"channel": "body", "values": ["PASS", "BLOCKED"]}),
    );
    let result = service().launch(launch_input(bp, json!({}))).await;
    let err = result.expect_err("an undeclared verdict token must fail the step");
    assert_verdict_rejection_is_diagnosable(&err.to_string(), "MAYBE");
}

/// The realistic shape of the same mistake, and the one that motivated
/// making the rejection diagnosable: an in-process gate declares
/// `channel: "body"` but emits its whole JSON report as the body, because
/// the report is what downstream wants to read. Every payload key the
/// captor understands (`response` / `content`) fails identically — the
/// body channel compares the terminal VALUE, so only a bare declared
/// token can satisfy it. `channel: "part"` is the shape that keeps the
/// body free for a report (see the sibling test above).
#[tokio::test]
async fn script_mode_report_body_under_a_body_channel_contract_names_the_contract() {
    for payload_key in ["response", "content"] {
        let (script, dir) = write_script(
            &format!("verdict-report-body-{payload_key}"),
            &format!(
                r#"
bus.emit("worker_result", {{ ok = true, {payload_key} = std.json.encode({{
    verdict = "PASS",
    summary = "0 findings",
}}) }})
"#
            ),
        );
        let bp = script_agent_bp(
            &format!("gh86-script-verdict-report-body-{payload_key}"),
            &script,
            &dir,
            json!({"channel": "body", "values": ["PASS", "BLOCKED"]}),
        );
        let result = service().launch(launch_input(bp, json!({}))).await;
        let err = result.expect_err(&format!(
            "a report body must fail a body contract ({payload_key})"
        ));
        assert_verdict_rejection_is_diagnosable(&err.to_string(), "summary");
    }
}

/// The diagnosability contract shared by every completion-time verdict
/// rejection on this lane: the surfaced error must carry the rejected
/// value and the declared token set, and must NOT degrade to the bare
/// missing-Final symptom.
fn assert_verdict_rejection_is_diagnosable(msg: &str, rejected_fragment: &str) {
    assert!(
        msg.contains("verdict contract violation"),
        "the failure must name the violated contract, got: {msg}"
    );
    assert!(
        msg.contains(rejected_fragment),
        "the failure must echo the rejected value (looking for {rejected_fragment:?}): {msg}"
    );
    assert!(
        msg.contains("PASS") && msg.contains("BLOCKED"),
        "the failure must echo the declared values: {msg}"
    );
    assert!(
        !msg.contains("no Final in output_tail"),
        "the cause must replace the missing-Final symptom, not hide behind it: {msg}"
    );
}

/// `init_ctx.work_dir` outranks `spec.project_root` for the running
/// script.
///
/// What that override actually controls is the SDK's `project_root`, NOT
/// the host process's cwd — a server hosting many agents cannot `chdir`
/// per worker. So a bare Lua `io.open("marker.txt")` still resolves
/// against the server's own cwd; the per-task directory reaches the
/// script through `std.env.project_root()` and, derived from it, the
/// default cwd of `sh.exec` and of MCP servers spawned by `mcp.connect`.
/// This test pins that contract from the script's side: the marker is
/// read via `sh.exec`, whose cwd defaults to the resolved `project_root`,
/// and the marker exists ONLY under `work_dir`.
#[tokio::test]
async fn task_work_dir_overrides_spec_project_root_for_the_script() {
    let (script, spec_root) = write_script(
        "work-dir",
        r#"
local r = sh.exec("cat marker.txt")
bus.emit("worker_result", {
    ok = true,
    response = { via_sh = (r.stdout or ""), root = std.env.project_root() },
})
"#,
    );
    // The marker lives ONLY in work_dir, never in spec.project_root, so a
    // successful read proves work_dir won.
    let work_dir = std::env::temp_dir().join(format!(
        "gh86-agent-block-workdir-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&work_dir).expect("create work_dir");
    std::fs::write(work_dir.join("marker.txt"), "from-work-dir").expect("write marker");

    let bp = script_agent_bp("gh86-script-work-dir", &script, &spec_root, Value::Null);
    let mut input = launch_input(bp, json!({}));
    input.task_input = Some(TaskInputSpec {
        project_root: None,
        work_dir: Some(work_dir.display().to_string()),
        task_metadata: None,
    });
    let out = service().launch(input).await.expect("launch must complete");
    let got = &out.final_ctx["danger_result"];
    assert_eq!(
        got["via_sh"],
        json!("from-work-dir"),
        "sh.exec's default cwd must be the task work_dir, not spec.project_root: {got}"
    );
    assert_ne!(
        got["root"],
        json!(spec_root.display().to_string()),
        "work_dir must have outranked spec.project_root: {got}"
    );
    assert!(
        got["root"].as_str().expect("root is a string").ends_with(
            work_dir
                .file_name()
                .and_then(|n| n.to_str())
                .expect("work_dir basename")
        ),
        "std.env.project_root() must report the task work_dir: {got}"
    );
}

/// GH #86: the launch's `init_ctx.task_metadata` bag reaches the script
/// as the `_TASK_METADATA` Lua global, set through the SDK's
/// `extra_globals` (so the JSON is converted natively — nesting intact).
/// Before this, the in-process lane resolved the value and then dropped
/// it, while the WS Operator lane had always received it as a
/// `task_metadata:` line of the Spawn directive header.
#[tokio::test]
async fn task_metadata_reaches_the_script_as_a_lua_global() {
    let (script, dir) = write_script(
        "task-metadata",
        r#"
bus.emit("worker_result", {
    ok = true,
    response = {
        issue = _TASK_METADATA and _TASK_METADATA.issue or "no-metadata",
        nested = _TASK_METADATA and _TASK_METADATA.nested and _TASK_METADATA.nested[2] or "none",
    },
})
"#,
    );
    let bp = script_agent_bp("gh86-script-task-metadata", &script, &dir, Value::Null);
    let mut input = launch_input(bp, json!({}));
    input.task_input = Some(TaskInputSpec {
        project_root: None,
        work_dir: None,
        task_metadata: Some(json!({"issue": 86, "nested": ["a", "b"]})),
    });
    let out = service().launch(input).await.expect("launch must complete");
    assert_eq!(
        out.final_ctx["danger_result"],
        json!({"issue": 86, "nested": "b"}),
        "task_metadata must arrive as a real Lua table, nesting intact"
    );
}

/// GH #86 / GH #21: Blueprint-declared agent context
/// (`default_agent_ctx` / `AgentMeta.ctx`, `ContextPolicy`-filtered)
/// reaches the script as the `_AGENT_CTX` Lua table. The WS Operator lane
/// has always received these as directive-header lines; before this the
/// in-process lane resolved them into `AgentContextView.extra` and then
/// rendered nothing.
#[tokio::test]
async fn blueprint_declared_agent_ctx_reaches_the_script() {
    let (script, dir) = write_script(
        "agent-ctx",
        r#"
bus.emit("worker_result", {
    ok = true,
    response = {
        conventions = _AGENT_CTX and _AGENT_CTX.org_conventions or "none",
        depth = _AGENT_CTX and _AGENT_CTX.nested and _AGENT_CTX.nested.level or "none",
    },
})
"#,
    );
    let mut bp = script_agent_bp("gh86-script-agent-ctx", &script, &dir, Value::Null);
    bp["default_agent_ctx"] = json!({
        "org_conventions": "two-space indent",
        "nested": { "level": 2 },
    });
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(
        out.final_ctx["danger_result"],
        json!({"conventions": "two-space indent", "depth": 2}),
        "BP-declared agent ctx must arrive as a real Lua table, nesting intact"
    );
}

/// Delivering `task_metadata` must not disturb the chunk itself.
///
/// Because it rides on the SDK's `extra_globals` rather than on anything
/// spliced into the source, a caller script keeps `ScriptSource::Path` in
/// both cases — so its own directory stays at the front of `package.path`
/// and a sibling `require` resolves whether or not metadata was supplied.
/// (An earlier iteration injected a generated prelude, which forced the
/// inline route and would have moved `script_dir` to `project_root`; this
/// test is the regression guard for that whole class.) With no metadata,
/// `_TASK_METADATA` is simply absent rather than an empty table.
#[tokio::test]
async fn sibling_require_resolves_with_and_without_task_metadata() {
    let (script, dir) = write_script(
        "sibling-require",
        r#"
local helper = require("helper")
bus.emit("worker_result", {
    ok = true,
    response = { helped = helper.answer(), meta = _TASK_METADATA and "present" or "absent" },
})
"#,
    );
    std::fs::write(
        dir.join("helper.lua"),
        "return { answer = function() return \"from-sibling\" end }\n",
    )
    .expect("write sibling module");

    // (a) no task_metadata supplied.
    let bp = script_agent_bp("gh86-sibling-no-meta", &script, &dir, Value::Null);
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("no-metadata launch must complete");
    assert_eq!(
        out.final_ctx["danger_result"],
        json!({"helped": "from-sibling", "meta": "absent"})
    );

    // (b) task_metadata present — same chunk, same route, plus the global.
    let bp = script_agent_bp("gh86-sibling-with-meta", &script, &dir, Value::Null);
    let mut input = launch_input(bp, json!({}));
    input.task_input = Some(TaskInputSpec {
        project_root: None,
        work_dir: None,
        task_metadata: Some(json!({"issue": 86})),
    });
    let out = service()
        .launch(input)
        .await
        .expect("with-metadata launch must complete");
    assert_eq!(
        out.final_ctx["danger_result"],
        json!({"helped": "from-sibling", "meta": "present"}),
        "supplying task_metadata must not disturb the chunk or package.path"
    );
}

/// GH #86 AC "the bundled sample dispatches end to end": the shape of
/// `mse://blueprints/samples/05-after-run-audit-agent-block` — a `rust_fn`
/// worker running the flow's only step, with an `agent_block` auditor
/// auto-kicked in-process after that step settles via `audits` — actually
/// reaches the AgentBlock backend and runs.
///
/// The bundled sample's auditor is PromptBasedAgent, so dispatching *it*
/// verbatim would call a model: non-deterministic, billable, and not
/// runnable in CI. This runs the identical wiring with a ScriptBased
/// auditor instead, which exercises everything up to (and excluding) the
/// model call: audits resolution, the in-process kick, the AgentBlock
/// spawner, and the artifact fold onto the audited step's output tail.
/// The sample's own compile is covered by
/// `json_sample_bodies_compile_under_the_lint_registry`.
#[tokio::test]
async fn after_run_audit_kicks_an_agent_block_auditor_in_process() {
    use mlua_swarm::{AgentBlockInProcessSpawnerFactory, RustFnInProcessSpawnerFactory};

    // The auditor writes a marker so the assertion is positive evidence
    // that the script body ran, not merely that the launch survived — an
    // audit is observational and never gates the flow, so a green launch
    // alone would also be consistent with the auditor never dispatching.
    let marker = std::env::temp_dir().join(format!(
        "gh86-audit-marker-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let (script, dir) = write_script(
        "auditor",
        &format!(
            r#"
local f = assert(io.open({marker:?}, "w"))
f:write(tostring(_PROMPT))
f:close()
bus.emit("worker_result", {{ ok = true, response = "audited" }})
"#,
            marker = marker.display().to_string()
        ),
    );
    let bp = json!({
        "schema_version": "0.1.0",
        "id": "gh86-after-run-audit-agent-block",
        "flow": {
            "kind": "step",
            "ref": "worker",
            "in": {"op": "lit", "value": "do the thing"},
            "out": {"op": "path", "at": "$.result"}
        },
        "agents": [
            { "name": "worker", "kind": "rust_fn", "spec": {"fn_id": "identity"} },
            {
                "name": "auditor",
                "kind": "agent_block",
                "spec": {
                    "script_path": script.display().to_string(),
                    "project_root": dir.display().to_string()
                },
                "runner": {"backend": "agent_block_in_process", "tools": []}
            }
        ],
        "audits": [{ "agent": "auditor", "steps": ["worker"], "mode": "sync" }]
    });

    // The audit lane needs both backends registered, unlike the
    // single-agent cases above.
    let mut reg = SpawnerRegistry::new();
    reg.register::<AgentBlockInProcessSpawnerFactory>(Arc::new(
        AgentBlockInProcessSpawnerFactory::new(),
    ));
    reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(
        mlua_swarm::worker::baseline::extend_with_baseline(RustFnInProcessSpawnerFactory::new()),
    ));
    let svc = TaskLaunchService::new(Engine::new(EngineCfg::default()), Compiler::new(reg));

    let out = svc
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(
        out.final_ctx["result"]["echoed"],
        json!("do the thing"),
        "the audited step's own outcome is untouched by the audit"
    );
    // The exclusion invariant, asserted directly rather than implied by the
    // line above: the audit's `audit:<step_ref>` Artifact lands on the
    // AUDITED step's tail, but it is not a part the worker staged, so it
    // must stay out of the `{out, parts}` fold. `AfterRunAuditMiddleware`
    // calls `Engine::submit_output` directly for that reason — only the
    // worker's own sink registers names in the fold's allowlist.
    assert!(
        out.final_ctx["result"].get("parts").is_none(),
        "an audit sidecar must not wrap the audited step's BP-chain value \
         in a parts fold, got: {}",
        out.final_ctx["result"]
    );

    // `svc.launch(...).await` returning does not guarantee the AFTER_RUN
    // audit lane has flushed its side-effects to the FS — the in-process
    // auditor runs on a spawned task and, on macOS runners, the marker
    // write has occasionally not appeared by the time `read_to_string`
    // fires (the symptom that broke macOS CI on 4f5fbfb). Poll with a
    // bounded budget instead of a single-shot read; the auditor writes
    // synchronously once it starts, so any delay here is dispatch/schedule
    // latency, capped at a few hundred ms in practice.
    let seen = {
        let budget = Duration::from_secs(2);
        let start = std::time::Instant::now();
        loop {
            match std::fs::read_to_string(&marker) {
                Ok(s) => break s,
                Err(e) if start.elapsed() > budget => panic!(
                    "the auditor script must have run in-process and written its marker \
                     within {budget:?}: last error {e:?}, marker path {}",
                    marker.display()
                ),
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    };
    assert!(
        seen.contains("worker"),
        "the auditor's prompt should name the step it audits, got: {seen:?}"
    );
    let _ = std::fs::remove_file(&marker);
}

/// A gate that finishes without emitting anything is a failed step, not a
/// silently-empty success — the `bus.emit` contract is load-bearing and
/// must fail loud when a script forgets it (e.g. an author who returned a
/// value from the chunk instead).
#[tokio::test]
async fn script_that_never_emits_fails_the_step() {
    let (script, dir) = write_script(
        "no-emit",
        r#"
-- Returning a value is NOT the result contract; the host reads bus.emit.
return { ok = true, response = "ignored" }
"#,
    );
    let bp = script_agent_bp("gh86-script-no-emit", &script, &dir, Value::Null);
    let result = service().launch(launch_input(bp, json!({}))).await;
    assert!(
        result.is_err(),
        "a script that never emits must fail the step: {result:?}"
    );
}
