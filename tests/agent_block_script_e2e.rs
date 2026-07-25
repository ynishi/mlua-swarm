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
use std::sync::Arc;
use std::time::Duration;

fn service() -> TaskLaunchService {
    let mut reg = SpawnerRegistry::new();
    reg.register::<AgentBlockInProcessSpawnerFactory>(Arc::new(
        AgentBlockInProcessSpawnerFactory::new(),
    ));
    TaskLaunchService::new(Engine::new(EngineCfg::default()), Compiler::new(reg))
}

fn launch_input(bp: Value, init_ctx: Value) -> TaskLaunchInput {
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

/// A gate emitting a token OUTSIDE its declared `values` is rejected by
/// the completion-time contract check — the enforcement is live on the
/// in-process AgentBlock path, not just on the HTTP submit routes.
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
    assert!(
        result.is_err(),
        "an undeclared verdict token must fail the step: {result:?}"
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
