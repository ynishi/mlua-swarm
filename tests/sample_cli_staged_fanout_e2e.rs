//! The bundled `mse://blueprints/samples/12-cli-staged-fanout` sample runs
//! end to end as published: the `next-batch` CLI drives the loop through
//! its three stages, every unit is built, and the loop stops on the empty
//! batch.
//!
//! `#[cfg(unix)]`: the sample's templates are `sh -c` scripts.

#![cfg(unix)]

use mlua_swarm::{
    Compiler, Engine, EngineCfg, Role, SpawnerRegistry, SubprocessProcessSpawnerFactory,
    TaskLaunchInput, TaskLaunchService,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

const SAMPLE: &str =
    include_str!("../crates/mlua-swarm-cli/src/mcp/resources/samples/12-cli-staged-fanout.json");

#[tokio::test]
async fn sample_builds_every_stage_and_stops_on_the_empty_batch() {
    let mut reg = SpawnerRegistry::new();
    reg.register::<SubprocessProcessSpawnerFactory>(Arc::new(SubprocessProcessSpawnerFactory));
    let svc = TaskLaunchService::new(Engine::new(EngineCfg::default()), Compiler::new(reg));

    let out = svc
        .launch(TaskLaunchInput::automate(
            serde_json::from_str(SAMPLE).expect("sample deserializes as a Blueprint"),
            "sample-12-e2e-op",
            Role::Operator,
            Duration::from_secs(30),
            json!({}),
        ))
        .await
        .expect("the sample must run to completion");

    let ctx = &out.final_ctx;
    assert_eq!(ctx["finished"], json!(true), "{ctx}");
    // Stages 0, 1, 2 build; round 3 gets the empty batch and stops the
    // loop, so the counter ends one past it.
    assert_eq!(ctx["round"], json!(4), "{ctx}");
    assert_eq!(ctx["batch"], json!([]), "{ctx}");
    // The last stage that built anything was the single `docs` unit.
    let last = ctx["last_stage"].as_array().expect("last_stage array");
    assert_eq!(last.len(), 1, "{ctx}");
    assert_eq!(last[0]["built"], json!("docs"), "{ctx}");
}
