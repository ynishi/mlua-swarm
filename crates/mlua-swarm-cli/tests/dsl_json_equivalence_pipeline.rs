//! JSON-equivalence test: a `.bp.lua` DSL script
//! (`tests/fixtures/pipeline.bp.lua`) must build to exactly the same
//! Blueprint value as the hand-written JSON fixture
//! (`tests/fixtures/pipeline.json`). Equality is on `serde_json::Value`
//! (key order ignored) — same convention as
//! `dsl_json_equivalence_verdict_loop.rs`.

use mlua_swarm_cli::dsl;
use serde_json::Value;

const SAMPLE_JSON: &str = include_str!("fixtures/pipeline.json");
const FIXTURE_SCRIPT: &str = include_str!("fixtures/pipeline.bp.lua");

#[test]
fn dsl_builds_the_same_value_as_the_json_fixture() {
    let expected: Value =
        serde_json::from_str(SAMPLE_JSON).expect("sample file must be valid JSON");
    let actual = dsl::build_bp_from_script(FIXTURE_SCRIPT)
        .expect("fixture script must build a Blueprint value");

    assert_eq!(
        actual, expected,
        "DSL-built Blueprint diverges from the JSON fixture \
         (tests/fixtures/pipeline.json)"
    );
}

#[test]
fn dsl_output_still_carries_unexpanded_agent_md_refs() {
    // Unlike `dsl_json_equivalence_verdict_loop.rs` (which builds `AgentDef`
    // literals directly with `kind = "operator"`), this fixture's agents
    // go through `B.agent{md=...}` — the DSL author-time convenience
    // that mirrors the loader's `$agent_md` file-ref shape, resolved
    // separately at `mse bp build` compile-lint / server register time
    // (see `crates/mlua-swarm-cli/src/bp.rs`), never inside
    // `build_bp_from_script` itself. A direct
    // `serde_json::from_value::<Blueprint>` round-trip would fail here
    // (`AgentDef.kind` is required and `$agent_md` is not a recognized
    // field under `#[serde(deny_unknown_fields)]`) — this test instead
    // asserts the DSL output really does carry the unexpanded ref shape
    // the equivalence comparison above expects.
    let value = dsl::build_bp_from_script(FIXTURE_SCRIPT).expect("fixture script must build");
    let agents = value["agents"].as_array().expect("agents is an array");
    assert_eq!(agents.len(), 9, "the fixture declares 9 agents");
    for agent in agents {
        assert!(
            agent.get("$agent_md").and_then(Value::as_str).is_some(),
            "every fixture agent entry must carry an unexpanded $agent_md ref: {agent:?}"
        );
    }
}
