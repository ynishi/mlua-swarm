//! agent.md frontmatter + body loader — turns agent-profiles
//! `agents/*.md` files into `AgentDef`s.
//!
//! ## Input format
//!
//! ```text
//! ---
//! name: impl-lead
//! description: Implementation worker ...
//! model: sonnet
//! effort: high
//! tools: Read, Edit, Write, Grep, Glob
//! worker_binding: mse-worker-coder
//! permissionMode: bypassPermissions
//! memory: user
//! abtest: true
//! ---
//! <Markdown system prompt body>
//! ```
//!
//! ## Output
//!
//! A `Vec<AgentDef>` — each entry carries `profile: Some(AgentProfile
//! { ... })`, `kind` defaults to `AgentKind::Operator`, and `spec` is
//! `Value::Null`. The backend configuration (`spec`) is injected
//! separately by the caller — on the Operator-construction path.
//!
//! ## Scope
//!
//! - Only YAML frontmatter delimited by `---` is accepted. TOML and
//!   JSON are not supported.
//! - `tools` accepts both a CSV string (`"Read, Edit"`) and a YAML
//!   array (`["Read", "Edit"]`).
//! - `worker_binding` is the Claude Code SubAgent definition name this
//!   agent binds to at spawn time — first-class (not dumped into
//!   `extras`) because the compiler and the WS thin path read it
//!   directly (see `AgentProfile::worker_binding`).
//! - Any field beyond the known set (`name` / `description` / `model`
//!   / `effort` / `tools` / `worker_binding`) is dumped into an
//!   `extras` `Value` — a future-proof carry for C-C-specific fields.
//! - The body is kept verbatim, from just after the closing `---` to
//!   the end of the file. Body headings (e.g. `## Input`, `## When
//!   invoked:`, `## Output format`) are treated as opaque prompt text —
//!   no structural extraction. Any input contract stated under a body
//!   heading is a prose convention the worker follows because the body
//!   is verbatim in its system prompt (matches
//!   `mse://guides/agent-md-authoring` § "Input is not a section").

use crate::blueprint::{AgentDef, AgentKind, AgentProfile};
use serde_json::{Map, Value};
use std::fs;
use std::path::Path;

/// Errors specific to the agent.md loader.
#[derive(Debug)]
pub enum LoadError {
    /// Reading the file failed (not found, permissions, etc.).
    Io(std::io::Error),
    /// The `---` frontmatter delimiter was not found, or the body
    /// could not be separated.
    NoFrontmatter {
        /// Path (or source label) of the offending file.
        path: String,
    },
    /// Frontmatter YAML failed to parse.
    Yaml {
        /// Path (or source label) of the offending file.
        path: String,
        /// The underlying YAML parse error.
        source: serde_yaml::Error,
    },
    /// Frontmatter has no `name` field, so we cannot determine an
    /// agent identifier.
    MissingName {
        /// Path (or source label) of the offending file.
        path: String,
    },
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Io(e) => write!(f, "io error: {e}"),
            LoadError::NoFrontmatter { path } => {
                write!(f, "no frontmatter delimiter `---` in {path}")
            }
            LoadError::Yaml { path, source } => write!(f, "yaml parse error in {path}: {source}"),
            LoadError::MissingName { path } => {
                write!(f, "frontmatter missing required `name` field in {path}")
            }
        }
    }
}

impl std::error::Error for LoadError {}

impl From<std::io::Error> for LoadError {
    fn from(e: std::io::Error) -> Self {
        LoadError::Io(e)
    }
}

/// Turn a single `agent.md` file into an `AgentDef`.
///
/// **`kind` must be provided explicitly by the caller.** The old
/// hardcoded `Operator` default was structurally wrong: an agent.md
/// has no knowledge of deployment and should not decide `kind` in the
/// loader. The caller passes the kind after resolving the cascade —
/// `Blueprint.default_agent_kind` → the sibling `$agent_md` override
/// → `CompilerHints.kind_override`. `spec` is produced as
/// `Value::Null`; the caller overwrites it if needed.
pub fn load_file(path: impl AsRef<Path>, kind: AgentKind) -> Result<AgentDef, LoadError> {
    let path = path.as_ref();
    let text = fs::read_to_string(path)?;
    parse(&text, &path.display().to_string(), kind)
}

/// Load every `*.md` under `dir`. Sorted ascending by file name.
///
/// Files without frontmatter — explanatory docs that are not agents —
/// are **skipped**; `NoFrontmatter` is not turned into an error.
/// Files that have frontmatter but fail to parse or lack `name` do
/// propagate their errors.
///
/// `kind` applies uniformly to every file — the global default for
/// this directory scope. To differentiate per file, the caller calls
/// `load_file(path, per_file_kind)` directly.
pub fn load_dir(dir: impl AsRef<Path>, kind: AgentKind) -> Result<Vec<AgentDef>, LoadError> {
    let dir = dir.as_ref();
    let mut entries: Vec<_> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("md"))
        .collect();
    entries.sort();
    let mut out = Vec::new();
    for p in entries {
        match load_file(&p, kind.clone()) {
            Ok(def) => out.push(def),
            Err(LoadError::NoFrontmatter { .. }) => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// Turn the text of an agent.md into an `AgentDef`. `pub` so unit
/// tests can reach it. `kind` must be provided by the caller — same
/// contract as `load_file`.
pub fn parse(text: &str, source_label: &str, kind: AgentKind) -> Result<AgentDef, LoadError> {
    let (front, body) = split_frontmatter(text).ok_or_else(|| LoadError::NoFrontmatter {
        path: source_label.into(),
    })?;
    let yaml: Value = serde_yaml::from_str(front).map_err(|e| LoadError::Yaml {
        path: source_label.into(),
        source: e,
    })?;
    let obj = yaml.as_object().cloned().unwrap_or_default();

    let name = obj
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| LoadError::MissingName {
            path: source_label.into(),
        })?;

    let description = obj
        .get("description")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string());
    let model = obj
        .get("model")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let effort = obj
        .get("effort")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let tools = obj.get("tools").map(normalize_tools).unwrap_or_default();
    let worker_binding = obj
        .get("worker_binding")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Dump everything outside the known set into `extras` — a
    // future-proof carry for C-C-specific fields.
    let known = [
        "name",
        "description",
        "model",
        "effort",
        "tools",
        "worker_binding",
    ];
    let mut extras = Map::new();
    for (k, v) in &obj {
        if !known.contains(&k.as_str()) {
            extras.insert(k.clone(), v.clone());
        }
    }

    let version_hash = Some(compute_body_hash(body));

    let profile = AgentProfile {
        system_prompt: body.to_string(),
        model,
        effort,
        tools,
        description: description.clone(),
        extras: if extras.is_empty() {
            Value::Null
        } else {
            Value::Object(extras)
        },
        version_hash,
        worker_binding,
    };

    Ok(AgentDef {
        name,
        kind,
        spec: Value::Null,
        profile: Some(profile),
        meta: None,
        // GH #46 M2: `agent.md` frontmatter parsing for `runner` /
        // `runner_ref` is not part of this Milestone (schema + resolver
        // + validation only); the legacy `profile.worker_binding` path
        // above remains the sole source until a later Milestone wires
        // frontmatter authoring for the new tier.
        runner: None,
        runner_ref: None,
        // GH #50: `agent.md` frontmatter authoring for `verdict` is not
        // part of this scope either — Blueprint JSON authors declare it
        // directly (`agents[N].verdict`) until a later follow-up wires
        // frontmatter authoring for it too.
        verdict: None,
    })
}

/// Compute the content hash of an agent body (its `system_prompt`).
///
/// 32-byte blake3, hex-encoded. This is the same form that populates
/// `AgentProfile.version_hash`, and the same form recomputed by the
/// `patch_applier.lua` post-hook when it detects a
/// `/agents/N/profile/system_prompt` replacement — the
/// `host.content_hash` primitive is also blake3 — so the Phase 1
/// hash-consistency guarantee holds.
pub fn compute_body_hash(body: &str) -> String {
    blake3::hash(body.as_bytes()).to_hex().to_string()
}

/// Split `---\n...\n---\n<body>` into `(frontmatter, body)`. Returns
/// `None` when the delimiter is missing.
fn split_frontmatter(text: &str) -> Option<(&str, &str)> {
    let t = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))?;
    // Find the next `---` line.
    let mut search_from = 0;
    while let Some(idx) = t[search_from..].find("---") {
        let abs = search_from + idx;
        // Require line-start.
        if abs == 0 || t.as_bytes()[abs - 1] == b'\n' {
            let after = &t[abs + 3..];
            let body = after
                .strip_prefix("\r\n")
                .or_else(|| after.strip_prefix('\n'))
                .unwrap_or(after);
            return Some((&t[..abs], body));
        }
        search_from = abs + 3;
    }
    None
}

/// Normalise the frontmatter's `tools` field to a `Vec<String>`.
/// Accepted forms: CSV string (`"Read, Edit"`) or YAML array
/// (`["Read", "Edit"]`).
fn normalize_tools(v: &Value) -> Vec<String> {
    if let Some(arr) = v.as_array() {
        return arr
            .iter()
            .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Some(s) = v.as_str() {
        return s
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "---\nname: impl-lead\ndescription: Implementation worker\nmodel: sonnet\neffort: high\ntools: Read, Edit, Grep\npermissionMode: bypassPermissions\nmemory: user\nabtest: true\n---\nYou are the implementation lead.\n\nWork in the caller-provided task directory.\n";

    #[test]
    fn parses_full_frontmatter() {
        let def = parse(SAMPLE, "sample", AgentKind::Operator).expect("parse ok");
        assert_eq!(def.name, "impl-lead");
        assert!(matches!(def.kind, AgentKind::Operator));
        let p = def.profile.expect("profile present");
        assert_eq!(p.model.as_deref(), Some("sonnet"));
        assert_eq!(p.effort.as_deref(), Some("high"));
        assert_eq!(p.tools, vec!["Read", "Edit", "Grep"]);
        assert_eq!(p.description.as_deref(), Some("Implementation worker"));
        assert!(p
            .system_prompt
            .starts_with("You are the implementation lead."));
        // extras: permissionMode / memory / abtest
        let extras = p.extras.as_object().expect("extras object");
        assert_eq!(
            extras.get("permissionMode").and_then(|v| v.as_str()),
            Some("bypassPermissions")
        );
        assert_eq!(extras.get("memory").and_then(|v| v.as_str()), Some("user"));
        assert_eq!(extras.get("abtest").and_then(|v| v.as_bool()), Some(true));
        // no worker_binding in SAMPLE → None, and not dumped into extras.
        assert_eq!(p.worker_binding, None);
        assert!(extras.get("worker_binding").is_none());
    }

    #[test]
    fn worker_binding_extracted_as_first_class_field() {
        let t = "---\nname: x\nworker_binding: mse-worker-coder\n---\nbody\n";
        let def = parse(t, "x", AgentKind::Operator).unwrap();
        let p = def.profile.expect("profile present");
        assert_eq!(p.worker_binding.as_deref(), Some("mse-worker-coder"));
        // must not leak into extras alongside the first-class field.
        assert!(matches!(p.extras, Value::Null));
    }

    #[test]
    fn worker_binding_absent_is_none_not_extras() {
        let t = "---\nname: x\nmodel: sonnet\n---\nbody\n";
        let def = parse(t, "x", AgentKind::Operator).unwrap();
        let p = def.profile.expect("profile present");
        assert_eq!(p.worker_binding, None);
    }

    #[test]
    fn tools_accepts_yaml_array() {
        let t = "---\nname: x\ntools:\n  - Read\n  - Edit\n---\nbody\n";
        let def = parse(t, "x", AgentKind::Operator).unwrap();
        assert_eq!(def.profile.unwrap().tools, vec!["Read", "Edit"]);
    }

    #[test]
    fn missing_name_errors() {
        let t = "---\nmodel: sonnet\n---\nbody\n";
        assert!(matches!(
            parse(t, "x", AgentKind::Operator),
            Err(LoadError::MissingName { .. })
        ));
    }

    #[test]
    fn no_frontmatter_errors() {
        let t = "plain body without frontmatter";
        assert!(matches!(
            parse(t, "x", AgentKind::Operator),
            Err(LoadError::NoFrontmatter { .. })
        ));
    }

    #[test]
    fn body_preserves_markdown() {
        let t = "---\nname: x\n---\n# Heading\n\nparagraph with `code`.\n";
        let p = parse(t, "x", AgentKind::Operator).unwrap().profile.unwrap();
        assert_eq!(p.system_prompt, "# Heading\n\nparagraph with `code`.\n");
    }

    #[test]
    fn populates_version_hash_from_body() {
        let def = parse(SAMPLE, "sample", AgentKind::Operator).unwrap();
        let p = def.profile.unwrap();
        let expected = compute_body_hash(&p.system_prompt);
        assert_eq!(p.version_hash.as_deref(), Some(expected.as_str()));
        // blake3 hex = 64 chars
        assert_eq!(expected.len(), 64);
    }

    #[test]
    fn version_hash_changes_with_body() {
        let t1 = "---\nname: x\n---\nbody one\n";
        let t2 = "---\nname: x\n---\nbody two\n";
        let h1 = parse(t1, "x", AgentKind::Operator)
            .unwrap()
            .profile
            .unwrap()
            .version_hash;
        let h2 = parse(t2, "x", AgentKind::Operator)
            .unwrap()
            .profile
            .unwrap()
            .version_hash;
        assert!(h1.is_some() && h2.is_some());
        assert_ne!(h1, h2);
    }

    #[test]
    fn version_hash_stable_across_frontmatter_reorder() {
        // Reordering the frontmatter must not affect the body → hash stays the same.
        let t1 = "---\nname: x\nmodel: sonnet\n---\nsame body\n";
        let t2 = "---\nmodel: sonnet\nname: x\n---\nsame body\n";
        let h1 = parse(t1, "x", AgentKind::Operator)
            .unwrap()
            .profile
            .unwrap()
            .version_hash;
        let h2 = parse(t2, "x", AgentKind::Operator)
            .unwrap()
            .profile
            .unwrap()
            .version_hash;
        assert_eq!(h1, h2);
    }
}
