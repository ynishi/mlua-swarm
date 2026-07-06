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
//! (see [`blueprint_schema_value`]).
//!
//! ## URI scheme
//!
//! ```text
//! mse://guides/<slug>
//! mse://blueprints/samples/<slug>
//! mse://api/blueprint-schema
//! ```
//!
//! ## Current resources
//!
//! | uri                                       | role                                              |
//! |--------------------------------------------|---------------------------------------------------|
//! | `mse://guides/getting-started`              | Entry points, quickstart, MCP client wiring.       |
//! | `mse://guides/blueprint-authoring`           | Flow node kinds, expr ops, agents, versioning.     |
//! | `mse://guides/mcp-tool-reference`            | All `mse mcp` tools grouped by family.             |
//! | `mse://blueprints/samples/01-pure-ctx-eval`  | Zero-spawn ctx-only Blueprint sample.               |
//! | `mse://blueprints/samples/02-verdict-loop`   | Verdict retry-loop Blueprint sample.                |
//! | `mse://blueprints/samples/03-fn-override`    | Verdict fn-override Blueprint sample.               |
//! | `mse://api/blueprint-schema`                 | Live Blueprint JSON Schema (generated per read).    |

use mlua_swarm::blueprint::Blueprint;

/// How a [`ResourceEntry`] produces its body when read.
pub enum ResourceBody {
    /// Body is baked in at compile time via `include_str!`.
    Static(&'static str),
    /// Body is generated at `read_resource` time (the Blueprint JSON Schema).
    BlueprintSchema,
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

const SAMPLE_01_PURE_CTX_EVAL_BODY: &str =
    include_str!("./resources/samples/01-pure-ctx-eval.json");
const SAMPLE_02_VERDICT_LOOP_BODY: &str = include_str!("./resources/samples/02-verdict-loop.json");
const SAMPLE_03_FN_OVERRIDE_BODY: &str = include_str!("./resources/samples/03-fn-override.json");

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
        uri: "mse://api/blueprint-schema",
        title: "Blueprint JSON Schema",
        description: "Live schemars-generated JSON Schema for the Blueprint type. flow is opaque (owned by mlua-flow-ir).",
        mime_type: "application/json",
        body: ResourceBody::BlueprintSchema,
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

/// Resolve a resource entry's body as a `String`. Static entries return
/// instantly; the schema entry generates fresh JSON on every call so it
/// never drifts from the `Blueprint` type.
pub fn body_for(entry: &ResourceEntry) -> Result<String, String> {
    match entry.body {
        ResourceBody::Static(s) => Ok(s.to_string()),
        ResourceBody::BlueprintSchema => {
            let value = blueprint_schema_value().map_err(|e| format!("schema serialize: {e}"))?;
            serde_json::to_string_pretty(&value).map_err(|e| format!("schema stringify: {e}"))
        }
    }
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
    fn sample_bodies_deserialize_into_blueprint() {
        // Guards the shipped samples against Blueprint schema drift: every
        // sample must parse as the typed Blueprint, not merely as JSON.
        for uri in [
            "mse://blueprints/samples/01-pure-ctx-eval",
            "mse://blueprints/samples/02-verdict-loop",
            "mse://blueprints/samples/03-fn-override",
        ] {
            let entry = find_by_uri(uri).unwrap_or_else(|| panic!("sample must exist: {uri}"));
            let body = body_for(entry).expect("sample body must generate");
            let bp: Blueprint = serde_json::from_str(&body)
                .unwrap_or_else(|e| panic!("{uri}: not a valid Blueprint: {e}"));
            assert!(
                !bp.id.is_empty(),
                "{uri}: sample Blueprint must carry a non-empty id"
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
