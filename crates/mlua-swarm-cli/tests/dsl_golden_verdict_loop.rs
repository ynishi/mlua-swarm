//! Golden test: reproduce
//! `mse://blueprints/samples/02-verdict-loop` verbatim from a `.bp.lua` DSL
//! script (`tests/fixtures/verdict_loop.bp.lua`) and assert the two
//! `serde_json::Value`s are equal. `serde_json::Value` equality ignores
//! object key order, so this is a structural AST-shape check, not a
//! byte-for-byte text diff.

use mlua_swarm_cli::dsl;

const SAMPLE_JSON: &str = include_str!("../src/mcp/resources/samples/02-verdict-loop.json");
const FIXTURE_SCRIPT: &str = include_str!("fixtures/verdict_loop.bp.lua");

#[test]
fn dsl_reproduces_verdict_loop_sample_verbatim() {
    let expected: serde_json::Value =
        serde_json::from_str(SAMPLE_JSON).expect("sample file must be valid JSON");
    let actual = dsl::build_bp_from_script(FIXTURE_SCRIPT)
        .expect("fixture script must build a Blueprint value");

    assert_eq!(
        actual, expected,
        "DSL-built Blueprint diverges from the golden sample \
         (mse://blueprints/samples/02-verdict-loop)"
    );
}

#[test]
fn dsl_reproduced_sample_is_a_valid_blueprint() {
    let value = dsl::build_bp_from_script(FIXTURE_SCRIPT).expect("fixture script must build");
    // Round-trips through the real Blueprint type (proves the DSL output
    // isn't just textually equal to the sample, but schema-valid too).
    serde_json::from_value::<mlua_swarm_schema::Blueprint>(value)
        .expect("DSL-built value must deserialize as a real Blueprint");
}
