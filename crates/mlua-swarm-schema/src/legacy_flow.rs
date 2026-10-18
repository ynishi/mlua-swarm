//! Read-compat for flow nodes `mlua-flow-ir` renamed under us.
//!
//! `mlua-flow-ir` 0.3.0 renamed the pure-transform node `assign` to `let`
//! and changed its `at` field from a `path` Expr object
//! (`{"op": "path", "at": "$.x"}`) to a bare path string (`"$.x"`). A
//! Blueprint written before that — every one registered on a server,
//! every launch snapshot in a replay store, every hand-written JSON file —
//! still says `"kind": "assign"`. Refusing those documents would break
//! registered Blueprints and resume, so the flow is rewritten to the new
//! shape before it is deserialized, and every rewritten node is reported
//! (lint `flow-node-renamed`, see `mse://guides/blueprint-authoring`
//! § Legacy `assign` nodes) by the stages that still see the raw document.

use mlua_flow_ir::Node as FlowNode;
use serde::{Deserialize, Deserializer};
use serde_json::Value;

/// Wire tag of the node `mlua-flow-ir` 0.3.0 renamed to `let`.
pub const LEGACY_ASSIGN_KIND: &str = "assign";

/// JSONPaths into the Blueprint document (`$.flow...`) of every node in `flow`
/// that still uses the legacy `assign` kind, in document order.
///
/// Pure: reads `flow` and changes nothing. Stages that hold the raw
/// Blueprint document (registration, `mse bp lint` / `build`) call this on
/// `doc["flow"]` to produce `flow-node-renamed` findings; the rewrite
/// itself happens in deserialization ([`upgrade_legacy_flow`]).
pub fn legacy_assign_paths(flow: &Value) -> Vec<String> {
    let mut found = Vec::new();
    walk(flow, "$.flow".to_string(), &mut |node, path| {
        if node.get("kind").and_then(Value::as_str) == Some(LEGACY_ASSIGN_KIND) {
            found.push(path.to_string());
        }
    });
    found
}

/// Rewrite every legacy `assign` node in `flow` to the `let` shape, in
/// place: the tag becomes `"let"`, and an `at` that is a `path` Expr
/// object is replaced by its path string. An `at` already in string form
/// is kept. Returns the number of nodes rewritten.
pub fn upgrade_legacy_flow(flow: &mut Value) -> usize {
    let mut rewritten = 0;
    walk_mut(flow, &mut |node| {
        let Some(obj) = node.as_object_mut() else {
            return;
        };
        if obj.get("kind").and_then(Value::as_str) != Some(LEGACY_ASSIGN_KIND) {
            return;
        }
        obj.insert("kind".into(), Value::String("let".into()));
        if let Some(at) = obj.get_mut("at") {
            let path_str = at
                .as_object()
                .filter(|o| o.get("op").and_then(Value::as_str) == Some("path"))
                .and_then(|o| o.get("at"))
                .filter(|s| s.is_string())
                .cloned();
            if let Some(s) = path_str {
                *at = s;
            }
        }
        rewritten += 1;
    });
    rewritten
}

/// Deserialize a flow [`FlowNode`] from `value`, accepting the legacy
/// `assign` node (see the module docs).
pub fn flow_node_from_value(mut value: Value) -> Result<FlowNode, serde_json::Error> {
    upgrade_legacy_flow(&mut value);
    serde_json::from_value(value)
}

/// `#[serde(deserialize_with)]` hook for `Blueprint.flow`.
pub(crate) fn deserialize_flow<'de, D>(deserializer: D) -> Result<FlowNode, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    flow_node_from_value(value).map_err(serde::de::Error::custom)
}

/// The child-node slots of a flow node, by the node's `kind`. Only these
/// positions hold nodes; every other field holds an Expr or a scalar, so
/// the walk never mistakes an Expr object for a node.
fn child_slots(kind: &str) -> &'static [&'static str] {
    match kind {
        "seq" => &["children"],
        "branch" => &["then", "else"],
        "fanout" | "loop" => &["body"],
        "try" => &["body", "catch"],
        _ => &[],
    }
}

fn walk(node: &Value, path: String, visit: &mut impl FnMut(&Value, &str)) {
    visit(node, &path);
    let Some(kind) = node.get("kind").and_then(Value::as_str) else {
        return;
    };
    for slot in child_slots(kind) {
        match node.get(*slot) {
            Some(Value::Array(children)) => {
                for (i, child) in children.iter().enumerate() {
                    walk(child, format!("{path}.{slot}[{i}]"), visit);
                }
            }
            Some(child @ Value::Object(_)) => walk(child, format!("{path}.{slot}"), visit),
            _ => {}
        }
    }
}

fn walk_mut(node: &mut Value, visit: &mut impl FnMut(&mut Value)) {
    visit(node);
    let Some(kind) = node.get("kind").and_then(Value::as_str).map(str::to_owned) else {
        return;
    };
    for slot in child_slots(&kind) {
        match node.get_mut(*slot) {
            Some(Value::Array(children)) => {
                for child in children.iter_mut() {
                    walk_mut(child, visit);
                }
            }
            Some(child @ Value::Object(_)) => walk_mut(child, visit),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn legacy_doc() -> Value {
        json!({
            "kind": "seq",
            "children": [
                {"kind": "assign", "at": {"op": "path", "at": "$.a"},
                 "value": {"op": "lit", "value": 1}},
                {"kind": "branch",
                 "cond": {"op": "lit", "value": true},
                 "then": {"kind": "assign", "at": {"op": "path", "at": "$.b"},
                          "value": {"op": "lit", "value": 2}},
                 "else": {"kind": "seq", "children": []}}
            ]
        })
    }

    #[test]
    fn finds_every_legacy_node_with_its_path() {
        assert_eq!(
            legacy_assign_paths(&legacy_doc()),
            vec![
                "$.flow.children[0]".to_string(),
                "$.flow.children[1].then".to_string()
            ]
        );
    }

    #[test]
    fn new_shape_reports_nothing() {
        let doc = json!({"kind": "let", "at": "$.a", "value": {"op": "lit", "value": 1}});
        assert!(legacy_assign_paths(&doc).is_empty());
    }

    #[test]
    fn legacy_flow_deserializes_as_let() {
        let node = flow_node_from_value(legacy_doc()).expect("legacy flow parses");
        let FlowNode::Seq { children } = node else {
            panic!("expected seq");
        };
        let FlowNode::Let { at, .. } = &children[0] else {
            panic!("expected let, got {:?}", children[0]);
        };
        assert_eq!(at.to_string(), "$.a");
    }

    #[test]
    fn upgrade_keeps_a_string_at_and_counts_rewrites() {
        let mut doc = json!({"kind": "assign", "at": "$.a", "value": {"op": "lit", "value": 1}});
        assert_eq!(upgrade_legacy_flow(&mut doc), 1);
        assert_eq!(doc["kind"], "let");
        assert_eq!(doc["at"], "$.a");
    }

    #[test]
    fn expr_objects_are_not_visited_as_nodes() {
        // A `lit` Expr whose value happens to look like an assign node is
        // data, not a node — it must survive untouched.
        let mut doc = json!({
            "kind": "step", "ref": "x",
            "in": {"op": "lit", "value": {"kind": "assign", "at": "$.z"}},
            "out": {"op": "path", "at": "$.x"}
        });
        assert!(legacy_assign_paths(&doc).is_empty());
        assert_eq!(upgrade_legacy_flow(&mut doc), 0);
        assert_eq!(doc["in"]["value"]["kind"], "assign");
    }
}
