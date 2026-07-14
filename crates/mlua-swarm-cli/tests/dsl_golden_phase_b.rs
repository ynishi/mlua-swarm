//! Golden test: reproduce the bundled real-world phase-pipeline
//! Blueprint (`tests/fixtures/phase-b-real-agents.json`) from a
//! `.bp.lua` DSL script (`tests/fixtures/phase_b.bp.lua`) and
//! assert the two `serde_json::Value`s are equal (key order ignored, per
//! `serde_json::Value` equality — same convention as
//! `dsl_golden_verdict_loop.rs`).

use mlua_swarm_cli::dsl;
use serde_json::Value;

const SAMPLE_JSON: &str = include_str!("fixtures/phase-b-real-agents.json");
const FIXTURE_SCRIPT: &str = include_str!("fixtures/phase_b.bp.lua");

/// `dsl::build_bp_from_script` converts every Lua table with zero
/// entries to a JSON empty ARRAY (`encode_empty_tables_as_array(true)`
/// — see that function's doc comment: Lua's table type cannot itself
/// distinguish an empty array from an empty object). The real BP
/// fixture declares `operators[0].spec` as an empty JSON OBJECT (`{}`);
/// reproducing that exact shape from Lua is structurally impossible
/// with the current DSL bridge as written (see GH #52 for the planned
/// empty-object marker). This helper canonicalizes
/// every empty array/object leaf on BOTH sides to `[]` before
/// comparison, so this single, semantically-irrelevant quirk (there is
/// nothing inside either shape to lose) doesn't fail an otherwise
/// byte-exact golden comparison. Any *non-empty* mismatch anywhere else
/// in the tree is left untouched and still fails the assertion.
fn normalize_empty_collections(v: &mut Value) {
    match v {
        Value::Object(map) => {
            if map.is_empty() {
                *v = Value::Array(vec![]);
                return;
            }
            for val in map.values_mut() {
                normalize_empty_collections(val);
            }
        }
        Value::Array(arr) => {
            for val in arr.iter_mut() {
                normalize_empty_collections(val);
            }
        }
        _ => {}
    }
}

#[test]
fn dsl_reproduces_phase_b_real_agents_sample_verbatim() {
    let mut expected: Value =
        serde_json::from_str(SAMPLE_JSON).expect("sample file must be valid JSON");
    let mut actual = dsl::build_bp_from_script(FIXTURE_SCRIPT)
        .expect("fixture script must build a Blueprint value");

    normalize_empty_collections(&mut expected);
    normalize_empty_collections(&mut actual);

    assert_eq!(
        actual, expected,
        "DSL-built Blueprint diverges from the golden phase-b sample \
         (tests/fixtures/phase-b-real-agents.json)"
    );
}

#[test]
fn dsl_reproduced_phase_b_sample_still_carries_unexpanded_agent_md_refs() {
    // Unlike `dsl_golden_verdict_loop.rs` (which builds `AgentDef`
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
    // the golden comparison above expects.
    let value = dsl::build_bp_from_script(FIXTURE_SCRIPT).expect("fixture script must build");
    let agents = value["agents"].as_array().expect("agents is an array");
    assert_eq!(agents.len(), 9, "phase-b declares 9 agents");
    for agent in agents {
        assert!(
            agent.get("$agent_md").and_then(Value::as_str).is_some(),
            "every phase-b agent entry must carry an unexpanded $agent_md ref: {agent:?}"
        );
    }
}
