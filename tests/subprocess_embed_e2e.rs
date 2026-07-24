//! GH #83 end-to-end: `SubprocessDef` declarative CLI invocation
//! templates (EmbedAgent) through the full compile → launch → render →
//! exec → normalize path, using neutral binaries only (`sh` / `cat` —
//! no vendor CLI anywhere).
//!
//! Spawning tests are `#[cfg(unix)]`: the templates exercise `sh -c`,
//! which the Ubuntu / macOS CI test jobs cover; the Windows job only
//! `cargo check`s.

#![cfg(unix)]

use mlua_swarm::{
    Compiler, Engine, EngineCfg, Role, SpawnerRegistry, SubprocessProcessSpawnerFactory,
    TaskInputSpec, TaskLaunchInput, TaskLaunchService,
};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn service() -> TaskLaunchService {
    let mut reg = SpawnerRegistry::new();
    reg.register::<SubprocessProcessSpawnerFactory>(Arc::new(SubprocessProcessSpawnerFactory));
    TaskLaunchService::new(Engine::new(EngineCfg::default()), Compiler::new(reg))
}

fn launch_input(bp: Value, init_ctx: Value) -> TaskLaunchInput {
    TaskLaunchInput::automate(
        serde_json::from_value(bp).expect("blueprint deserializes"),
        "gh83-e2e-op",
        Role::Operator,
        Duration::from_secs(30),
        init_ctx,
    )
}

/// (a) Normal path: `{prompt}` reaches the child via stdin, the baked
/// `profile.system_prompt` reaches it via `{system_file}` (env), and the
/// declared `output.result_ptr` extracts the worker result out of the
/// child's JSON stdout.
#[tokio::test]
async fn embed_template_renders_prompt_and_system_file_and_extracts_result() {
    let bp = json!({
        "schema_version": "0.1.0",
        "id": "gh83-embed-normal",
        "flow": {
            "kind": "step",
            "ref": "headless",
            "in": {"op": "lit", "value": "hello-prompt"},
            "out": {"op": "path", "at": "$.result"}
        },
        "agents": [{
            "name": "headless",
            "kind": "subprocess",
            "spec": {},
            "profile": {"system_prompt": "the-system-prompt"},
            "runner": {"backend": "subprocess", "template": "probe"}
        }],
        "subprocesses": [{
            "name": "probe",
            "argv": [
                "sh", "-c",
                "printf '{\"payload\": {\"prompt_seen\": \"%s\", \"system_seen\": \"%s\"}}' \"$(cat)\" \"$(cat \"$SYS_FILE\")\""
            ],
            "stdin": "{prompt}",
            "env": {"SYS_FILE": "{system_file}"},
            "output": {"format": "json", "result_ptr": "/payload", "ok_from": "exit_code"}
        }]
    });
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(
        out.final_ctx["result"],
        json!({"prompt_seen": "hello-prompt", "system_seen": "the-system-prompt"})
    );
}

/// (b) Two different `SubprocessDef` templates coexist in ONE Blueprint,
/// each backing its own worker — the "workers backed by different CLI
/// backends can coexist" acceptance criterion.
#[tokio::test]
async fn two_templates_coexist_in_one_blueprint() {
    let bp = json!({
        "schema_version": "0.1.0",
        "id": "gh83-embed-coexist",
        "flow": {"kind": "seq", "children": [
            {
                "kind": "step",
                "ref": "worker_a",
                "in": {"op": "lit", "value": "ignored"},
                "out": {"op": "path", "at": "$.a"}
            },
            {
                "kind": "step",
                "ref": "worker_b",
                "in": {"op": "lit", "value": "ignored"},
                "out": {"op": "path", "at": "$.b"}
            }
        ]},
        "agents": [
            {
                "name": "worker_a",
                "kind": "subprocess",
                "spec": {},
                "runner": {"backend": "subprocess", "template": "echo-a"}
            },
            {
                "name": "worker_b",
                "kind": "subprocess",
                "spec": {},
                "runner": {"backend": "subprocess", "template": "echo-b"}
            }
        ],
        "subprocesses": [
            {
                "name": "echo-a",
                "argv": ["sh", "-c", "printf '{\"result\": \"from-A\"}'"],
                "output": {"format": "json", "result_ptr": "/result"}
            },
            {
                "name": "echo-b",
                "argv": ["sh", "-c", "printf '{\"result\": \"from-B\"}'"],
                "output": {"format": "json", "result_ptr": "/result"}
            }
        ]
    });
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    assert_eq!(out.final_ctx["a"], json!("from-A"));
    assert_eq!(out.final_ctx["b"], json!("from-B"));
}

/// (c) Failure path: a non-zero exit is a failed step (dispatch outcome
/// Blocked) — the launch surfaces an error instead of silently folding a
/// broken result into the ctx.
#[tokio::test]
async fn nonzero_exit_fails_the_step() {
    let bp = json!({
        "schema_version": "0.1.0",
        "id": "gh83-embed-exit-fail",
        "flow": {
            "kind": "step",
            "ref": "failing",
            "in": {"op": "lit", "value": "ignored"},
            "out": {"op": "path", "at": "$.never"}
        },
        "agents": [{
            "name": "failing",
            "kind": "subprocess",
            "spec": {},
            "runner": {"backend": "subprocess", "template": "boom"}
        }],
        "subprocesses": [{
            "name": "boom",
            "argv": ["sh", "-c", "echo 'went wrong' >&2; exit 7"]
        }]
    });
    let result = service().launch(launch_input(bp, json!({}))).await;
    assert!(
        result.is_err(),
        "non-zero exit must fail the step: {result:?}"
    );
}

/// (c') Failure path: declared-JSON stdout that does not parse fails the
/// step even though the child exited 0.
#[tokio::test]
async fn declared_json_unparsable_stdout_fails_the_step() {
    let bp = json!({
        "schema_version": "0.1.0",
        "id": "gh83-embed-unparse-fail",
        "flow": {
            "kind": "step",
            "ref": "chatty",
            "in": {"op": "lit", "value": "ignored"},
            "out": {"op": "path", "at": "$.never"}
        },
        "agents": [{
            "name": "chatty",
            "kind": "subprocess",
            "spec": {},
            "runner": {"backend": "subprocess", "template": "not-json"}
        }],
        "subprocesses": [{
            "name": "not-json",
            "argv": ["sh", "-c", "echo 'plain prose, not JSON'"],
            "output": {"format": "json"}
        }]
    });
    let result = service().launch(launch_input(bp, json!({}))).await;
    assert!(
        result.is_err(),
        "declared-JSON unparsable stdout must fail the step: {result:?}"
    );
}

/// cwd: a template `cwd: "{work_dir}"` runs the child inside the
/// task-level work_dir (proved by reading a marker file with a relative
/// path — no path-string comparison, so macOS `/private` symlinking
/// cannot flake this).
#[tokio::test]
async fn template_cwd_work_dir_places_the_child_in_the_task_work_dir() {
    let work_dir = std::env::temp_dir().join(format!(
        "gh83-embed-cwd-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&work_dir).expect("create work_dir");
    std::fs::write(work_dir.join("marker.txt"), "marker-content").expect("write marker");

    let bp = json!({
        "schema_version": "0.1.0",
        "id": "gh83-embed-cwd",
        "flow": {
            "kind": "step",
            "ref": "reader",
            "in": {"op": "lit", "value": "ignored"},
            "out": {"op": "path", "at": "$.result"}
        },
        "agents": [{
            "name": "reader",
            "kind": "subprocess",
            "spec": {},
            "runner": {"backend": "subprocess", "template": "read-marker"}
        }],
        "subprocesses": [{
            "name": "read-marker",
            "argv": ["sh", "-c", "printf '{\"result\": \"%s\"}' \"$(cat marker.txt)\""],
            "cwd": "{work_dir}",
            "output": {"format": "json", "result_ptr": "/result"}
        }]
    });
    let mut input = launch_input(bp, json!({}));
    input.task_input = Some(TaskInputSpec {
        project_root: None,
        work_dir: Some(work_dir.display().to_string()),
        task_metadata: None,
    });
    let out = service().launch(input).await.expect("launch must complete");
    assert_eq!(out.final_ctx["result"], json!("marker-content"));
}

/// Spec-based Subprocess agents (program/args in `spec`, no
/// `Runner::Subprocess`) keep the historical path — regression pin for
/// "existing Subprocess BPs are untouched".
#[tokio::test]
async fn spec_based_subprocess_path_is_unchanged() {
    let bp = json!({
        "schema_version": "0.1.0",
        "id": "gh83-spec-based-compat",
        "flow": {
            "kind": "step",
            "ref": "legacy",
            "in": {"op": "lit", "value": "{\"echoed\": true}"},
            "out": {"op": "path", "at": "$.result"}
        },
        "agents": [{
            "name": "legacy",
            "kind": "subprocess",
            "spec": {"program": "cat", "args": [], "use_stdin": true}
        }]
    });
    let out = service()
        .launch(launch_input(bp, json!({})))
        .await
        .expect("launch must complete");
    // `cat` echoes the directive JSON back; the historical lenient parse
    // folds it into a JSON value.
    assert_eq!(out.final_ctx["result"], json!({"echoed": true}));
}
