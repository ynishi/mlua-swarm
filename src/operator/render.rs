//! `system_prompt` template rendering for the Operator path.
//!
//! Lets the agent.md body (`AgentDef.profile.system_prompt`) carry
//! Jinja2-compatible syntax (`{{ directive }}` / `{% if intent %}` /
//! `{{ x | upper }}` and so on). The caller passes
//! `TaskSpec.initial_directive` as JSON and its fields expand into the
//! template slots.
//!
//! ## Engine choice
//!
//! minijinja (maintained by Armin Ronacher, the Jinja2 author — a light
//! dependency). Two defaults are forced up front to avoid classic traps:
//!
//! - `auto_escape = None`. HTML auto-escape is off; for LLM prompts,
//!   turning `<` / `>` into `&lt;` / `&gt;` corrupts the prompt.
//! - `UndefinedBehavior::Strict`. A typo'd variable would otherwise
//!   silently render as an empty string; in a production prompt
//!   template we want that to fail loud.
//!
//! ## Syntax available inside the agent.md body (Jinja2-compatible, per
//! minijinja v2)
//!
//! ```text
//! Variables:  {{ directive }} / {{ slot.nested }} / {{ items[0] }}
//! Filters:    {{ name | upper }} / {{ x | default("fallback") }} / {{ s | length }}
//! Branch:     {% if intent %}...{% elif other %}...{% else %}...{% endif %}
//! Loop:       {% for x in items %}{{ x }},{% endfor %}
//! Comment:    {# note #}
//! Raw:        {% raw %}{{ literal }}{% endraw %}
//! ```
//!
//! Macros, `include`, and inheritance are not available here — this
//! layer performs a flat render over one string handed in by the caller,
//! and does not support multi-template composition. If we ever need
//! that, adding a source loader to `Environment` is a carry.
//!
//! ## Slot names (the variables the caller can reference in the
//! template)
//!
//! `slots_from_prompt(prompt: &str)` builds the slot map:
//!
//! - When `prompt` is a **JSON object**, the object's top-level keys
//!   become the slot names — for example
//!   `r#"{"directive":"X","intent":"fix"}"#` exposes `{{ directive }}`
//!   and `{{ intent }}`.
//! - When `prompt` is **anything else** (a plain string, a number, an
//!   array, an already-stringified JSON), it is wrapped as
//!   `{"directive": <the prompt itself>}`; only `{{ directive }}` is
//!   available.
//!
//! To expose additional slots, the caller (whoever assembles
//! `TaskSpec.initial_directive`) passes a JSON object. Conventions:
//!
//! - `directive` (effectively required) — the main task instruction;
//!   the plain-prompt fallback also lives here.
//! - `intent` — task kind / classification hint (optional; used in
//!   `if` branches).
//! - `context` — additional context (optional).
//! - Beyond those, agent.md authors are free to add whatever slots the
//!   template needs.
//!
//! ## Errors
//!
//! - Undefined variable → `RenderError::Template` (strict mode).
//! - Syntax error → `RenderError::Template`.
//! - On the `OperatorSpawner` path this is wrapped in
//!   `SpawnError::Internal("render system_prompt: ...")` and propagated
//!   — no silent fallback, fail loud.

use minijinja::{Environment, UndefinedBehavior, Value};
use thiserror::Error;

/// Render errors. Anything from minijinja is wrapped as `Template`.
#[derive(Debug, Error)]
pub enum RenderError {
    /// minijinja syntax errors, undefined-variable errors, runtime
    /// errors, and the like.
    #[error("template render failed: {0}")]
    Template(String),
}

impl From<minijinja::Error> for RenderError {
    fn from(e: minijinja::Error) -> Self {
        RenderError::Template(format!("{e:#}"))
    }
}

/// Render a `system_prompt` template in strict mode with auto-escape
/// disabled.
///
/// `slots` is any JSON value. When it is an object, the top-level keys
/// are exposed as variables — `{{ directive }}` reads `slots.directive`.
/// When it is not an object, this function binds the value under a
/// single variable named `value`, reachable as `{{ value }}`.
pub fn render_system(template: &str, slots: &serde_json::Value) -> Result<String, RenderError> {
    let mut env = Environment::new();
    env.set_auto_escape_callback(|_| minijinja::AutoEscape::None);
    env.set_undefined_behavior(UndefinedBehavior::Strict);

    let tmpl = env.template_from_str(template)?;
    let value = Value::from_serialize(slots);
    let rendered = if let serde_json::Value::Object(_) = slots {
        tmpl.render(value)?
    } else {
        // A non-object is bound as the single variable `value`.
        tmpl.render(minijinja::context! { value => value })?
    };
    Ok(rendered)
}

/// If `prompt` is a JSON object, treat it as the slot map; otherwise
/// wrap it as `{"directive": prompt}`. Corresponds to the
/// `initial_directive` intake convention on the caller side
/// (`OperatorSpawner`). Takes `Value` directly (issue #18): `Value`
/// flows end-to-end through `EngineState.prompts` and
/// `Engine::fetch_prompt`; parsing a JSON literal `String` back into
/// an `Object` is no longer required at this boundary.
pub fn slots_from_prompt(prompt: &serde_json::Value) -> serde_json::Value {
    match prompt {
        v @ serde_json::Value::Object(_) => v.clone(),
        other => serde_json::json!({ "directive": other }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn expands_simple_variable() {
        let out = render_system("hello {{ directive }}", &json!({ "directive": "world" }))
            .expect("render ok");
        assert_eq!(out, "hello world");
    }

    #[test]
    fn supports_if_branch() {
        let tmpl = "{% if intent %}intent={{ intent }}{% else %}no-intent{% endif %}";
        let with = render_system(tmpl, &json!({ "intent": "fix-bug" })).unwrap();
        assert_eq!(with, "intent=fix-bug");
        let without = render_system(tmpl, &json!({ "intent": null })).unwrap();
        assert_eq!(without, "no-intent");
    }

    #[test]
    fn supports_filter() {
        let out = render_system("{{ name | upper }}", &json!({ "name": "shi" })).unwrap();
        assert_eq!(out, "SHI");
    }

    #[test]
    fn undefined_variable_errors_strict() {
        let err = render_system("hello {{ missing }}", &json!({ "directive": "x" }))
            .expect_err("strict undef must fail");
        let msg = format!("{err}");
        assert!(
            msg.contains("undefined") || msg.contains("missing"),
            "expected strict undef error, got: {msg}"
        );
    }

    #[test]
    fn syntax_error_returns_err() {
        let err = render_system("hello {{ unclosed", &json!({})).expect_err("syntax error");
        let msg = format!("{err}");
        assert!(
            msg.contains("syntax") || msg.contains("unexpected"),
            "got: {msg}"
        );
    }

    #[test]
    fn html_chars_not_escaped() {
        // For LLM prompt use, escaping `<` / `>` / `&` corrupts the prompt.
        let out = render_system("{{ snippet }}", &json!({ "snippet": "<tag>&amp;" })).unwrap();
        assert_eq!(out, "<tag>&amp;");
    }

    #[test]
    fn supports_for_loop() {
        let tmpl = "{% for x in xs %}{{ x }},{% endfor %}";
        let out = render_system(tmpl, &json!({ "xs": ["a", "b", "c"] })).unwrap();
        assert_eq!(out, "a,b,c,");
    }

    #[test]
    fn slots_from_prompt_object() {
        let v = slots_from_prompt(&json!({"directive":"do-X","intent":"fix"}));
        assert_eq!(v["directive"], "do-X");
        assert_eq!(v["intent"], "fix");
    }

    #[test]
    fn slots_from_prompt_plain_string() {
        let v = slots_from_prompt(&json!("just a plain instruction"));
        assert_eq!(v["directive"], "just a plain instruction");
    }

    #[test]
    fn slots_from_prompt_json_array_falls_back_to_directive() {
        // A top-level array is not an object, so fall back to wrapping in `directive`.
        let v = slots_from_prompt(&json!(["a", "b"]));
        assert_eq!(v["directive"], json!(["a", "b"]));
    }
}
