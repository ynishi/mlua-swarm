//! Blueprint loader (Phase B). Loads a Blueprint from a JSON / YAML file
//! and recursively expands the internal `{"$file": "..."}` refs.
//!
//! ## File-ref expansion
//!
//! Anywhere inside the JSON value, this form is replaced by the referenced
//! file's contents **as a raw string**. Paths are resolved **relative to
//! the Blueprint file's directory**:
//!
//! ```jsonc
//! { "$file": "prompts/system-writer.md" }
//! ```
//!
//! Typical uses:
//!
//! - Externalising a large prompt out of a flow `Step.in`:
//!   `{"op":"lit","value":{"$file":"prompts/x.md"}}`.
//! - Externalising any field inside `AgentDef.spec` (system_prompt, args,
//!   etc.).
//! - Externalising per-agent or global `hints`.
//!
//! ## Agent-md ref expansion (structured ref)
//!
//! Specialised ref that expands an `agent.md` (frontmatter + body) into
//! an **`AgentDef` object**:
//!
//! ```jsonc
//! {
//!   "agents": [
//!     { "$agent_md": "agents/domain-researcher.md" }
//!   ]
//! }
//! ```
//!
//! Where `$file` returns a raw string, `$agent_md` runs the file through
//! `agent_md_loader::parse` and returns a fully-populated `AgentDef` JSON
//! object with `profile.system_prompt`, `meta`, `spec`, and so on already
//! filled in. Path hygiene matches `$file`: absolute paths and `..` are
//! rejected.

use mlua_swarm_schema::{default_global_agent_kind, AgentKind, Blueprint};
use serde_json::Value;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Resolution config for `$agent_md` / `$file` refs. Ordered list of
/// directories the linker walks first-hit-wins across 6 tiers.
///
/// Tier order (highest priority first):
///
/// 1. `base` — bp.lua parent directory (always tier 1).
/// 2. `in_bp_includes` — in-bp declared `blueprint_ref_includes`
///    (relative to `base`).
/// 3. `env_includes` — env `MSE_BLUEPRINT_INCLUDES` (`:`- or `;`-
///    separated absolute paths).
/// 4. `cli_includes` — CLI `--include <path>` repeatable.
/// 5. `config_includes` — server / config-file `blueprint_ref_includes`.
/// 6. `bundled_default` — bundled fallback (typically
///    `crates/mlua-swarm-cli/src/mcp/resources/samples/agents/`).
///
/// Callers construct the config via [`ResolveConfig::new`] and layer
/// additional tiers through the builder methods, then pass the config
/// to [`expand_file_refs_with_config`].
#[derive(Debug, Clone, Default)]
pub struct ResolveConfig {
    /// bp.lua parent directory (always tier 1, always first).
    pub base: PathBuf,
    /// In-bp declared includes (tier 2, relative to `base`).
    pub in_bp_includes: Vec<PathBuf>,
    /// Env `MSE_BLUEPRINT_INCLUDES` (tier 3).
    pub env_includes: Vec<PathBuf>,
    /// CLI `--include <path>` repeatable (tier 4).
    pub cli_includes: Vec<PathBuf>,
    /// Server / config-file `blueprint_ref_includes` (tier 5).
    pub config_includes: Vec<PathBuf>,
    /// Bundled default (tier 6). `None` = no bundled fallback (typical
    /// for server-side use, where the server never ships authoring
    /// files).
    pub bundled_default: Option<PathBuf>,
}

impl ResolveConfig {
    /// Build a config with only the base tier set. Callers layer the
    /// remaining tiers via builder methods.
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self {
            base: base.into(),
            ..Default::default()
        }
    }

    /// Set the in-bp include list (tier 2). Paths are resolved relative
    /// to `base` at search time.
    pub fn with_in_bp_includes(mut self, v: Vec<PathBuf>) -> Self {
        self.in_bp_includes = v;
        self
    }

    /// Set the env include list (tier 3).
    pub fn with_env_includes(mut self, v: Vec<PathBuf>) -> Self {
        self.env_includes = v;
        self
    }

    /// Set the CLI include list (tier 4).
    pub fn with_cli_includes(mut self, v: Vec<PathBuf>) -> Self {
        self.cli_includes = v;
        self
    }

    /// Set the config-file include list (tier 5).
    pub fn with_config_includes(mut self, v: Vec<PathBuf>) -> Self {
        self.config_includes = v;
        self
    }

    /// Set the bundled-default fallback (tier 6). Pass `None` to
    /// disable the fallback (server-side default).
    pub fn with_bundled_default(mut self, p: Option<PathBuf>) -> Self {
        self.bundled_default = p;
        self
    }

    /// Iterate every configured directory in cascade order (tier 1 → 6).
    /// `in_bp_includes` entries are joined onto `base` at iteration
    /// time so callers can pass in relative paths as declared in the
    /// bp.lua source.
    pub fn search_paths(&self) -> impl Iterator<Item = PathBuf> + '_ {
        let base = self.base.clone();
        std::iter::once(self.base.clone())
            .chain(self.in_bp_includes.iter().map(move |p| base.join(p)))
            .chain(self.env_includes.iter().cloned())
            .chain(self.cli_includes.iter().cloned())
            .chain(self.config_includes.iter().cloned())
            .chain(self.bundled_default.iter().cloned())
    }
}

/// Read `MSE_BLUEPRINT_INCLUDES` and split it into a directory list
/// using the platform-native separator (`:` on Unix, `;` on Windows).
/// Returns an empty vec when the variable is unset.
pub fn env_blueprint_includes() -> Vec<PathBuf> {
    std::env::var_os("MSE_BLUEPRINT_INCLUDES")
        .map(|s| std::env::split_paths(&s).collect())
        .unwrap_or_default()
}

/// Pull the top-level `blueprint_ref_includes` list out of the raw BP
/// JSON. Returns an empty vec when the field is absent or malformed.
/// Consumed by the CLI / server as tier 2 of the cascade.
pub fn pre_read_in_bp_includes(val: &Value) -> Vec<PathBuf> {
    val.get("blueprint_ref_includes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_default()
}

/// Everything that can go wrong while loading and `$file`/`$agent_md`
/// expanding a Blueprint from disk.
#[derive(Debug, Error)]
pub enum LoadError {
    /// Reading the Blueprint file (or a referenced `$file`/`$agent_md`)
    /// failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The `.json` file did not parse as JSON.
    #[error("json parse: {0}")]
    Json(#[from] serde_json::Error),
    /// The `.yaml`/`.yml` file did not parse as YAML.
    #[error("yaml parse: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// The file extension is not one of `.json` / `.yaml` / `.yml`.
    #[error("unsupported extension: {0:?} (expected .json / .yaml / .yml)")]
    UnknownFormat(Option<String>),
    /// A `$file`/`$agent_md` ref failed path hygiene checks or the
    /// referenced file could not be read/parsed.
    #[error("$file ref expansion at {path:?}: {msg}")]
    FileRef {
        /// The resolved (or rejected) path of the ref.
        path: PathBuf,
        /// Human-readable description of what went wrong.
        msg: String,
    },
    /// The expanded JSON value did not deserialize into a `Blueprint`.
    #[error("blueprint shape invalid: {0}")]
    Shape(String),
}

/// Load a Blueprint from a file path. Detects JSON vs. YAML by
/// extension, recursively expands `$file` refs, and parses the result
/// into a typed `Blueprint`.
pub fn load_blueprint_from_path<P: AsRef<Path>>(path: P) -> Result<Blueprint, LoadError> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path)?;
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_lowercase());
    let value: Value = match ext.as_deref() {
        Some("json") => serde_json::from_str(&raw)?,
        Some("yaml") | Some("yml") => {
            let yv: serde_yaml::Value = serde_yaml::from_str(&raw)?;
            serde_json::to_value(yv)
                .map_err(|e| LoadError::Shape(format!("yaml→json convert: {e}")))?
        }
        other => return Err(LoadError::UnknownFormat(other.map(|s| s.to_string()))),
    };
    let base = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    // Steps (1) and (3) of the four-layer cascade: pre-read the BP JSON's
    // top-level `default_agent_kind`. If it is absent, fall back to the
    // schema's `Default` impl (`Operator`). The value is passed into
    // `expand_file_refs` and used as the loader-side kind default when a
    // `$agent_md` has no sibling override. Step (2), the caller-side
    // (CLI) override, is out of this function's scope — an upper layer
    // (the server seed handler) is responsible for overwriting the
    // pre-read value with the CLI value.
    let default_kind = pre_read_default_agent_kind(&value);
    let resolved = expand_file_refs(value, &base, default_kind)?;
    let bp: Blueprint = serde_json::from_value(resolved)
        .map_err(|e| LoadError::Shape(format!("typed parse: {e}")))?;
    Ok(bp)
}

/// Pull `default_agent_kind` out of the raw BP JSON top level. Falls
/// back to the schema's `Default` impl (`Operator`) if the key is
/// missing or its type does not match. This is the first stage of
/// resolving the default kind used inside `expand_file_refs` when a
/// `$agent_md` has no sibling `kind` override.
pub fn pre_read_default_agent_kind(val: &Value) -> AgentKind {
    val.get("default_agent_kind")
        .and_then(|v| serde_json::from_value::<AgentKind>(v.clone()).ok())
        .unwrap_or_else(default_global_agent_kind)
}

/// Takes a JSON value: an object whose only key is `"$file": "path"` is
/// replaced with the referenced file's contents; other objects / arrays
/// recurse; scalars pass through unchanged.
///
/// Path hygiene: absolute paths and `..` parent-directory escapes are
/// **rejected**, sandboxing all refs to the Blueprint's base-directory
/// subtree. That structurally prevents accidentally pulling in
/// `/etc/passwd` or `~/.ssh/id_rsa`. The trust boundary is spelled out
/// explicitly.
///
/// Shared path hygiene for `$file` and `$agent_md`: absolute paths and
/// `..` parent escapes are rejected; refs are searched across the
/// 6-tier cascade in `cfg` (first-hit-wins). On miss, the error names
/// every tier dir searched so authors can diagnose which include layer
/// to add.
fn resolve_ref_path(rel: &str, cfg: &ResolveConfig) -> Result<PathBuf, LoadError> {
    let rel_path = Path::new(rel);
    if rel_path.is_absolute() {
        return Err(LoadError::FileRef {
            path: rel_path.to_path_buf(),
            msg: "absolute path not allowed (must be relative to Blueprint dir)".into(),
        });
    }
    if rel_path
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(LoadError::FileRef {
            path: rel_path.to_path_buf(),
            msg: "'..' parent-dir escape not allowed".into(),
        });
    }
    let mut searched: Vec<PathBuf> = Vec::new();
    for dir in cfg.search_paths() {
        let candidate = dir.join(rel_path);
        if candidate.exists() {
            return Ok(candidate);
        }
        searched.push(dir);
    }
    let searched_str = searched
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(LoadError::FileRef {
        path: rel_path.to_path_buf(),
        msg: format!("not found in include cascade (searched: {searched_str})"),
    })
}

/// Primary entry — recursively expands `$file` / `$agent_md` refs
/// across the 6-tier include cascade defined in `cfg`. Prefer this
/// entry over [`expand_file_refs`] for any new caller that wants
/// cascade-aware resolution.
///
/// `default_kind` is the fallback used when a `$agent_md` has no
/// sibling `kind` — it should already be resolved by upper layers of
/// the four-layer kind cascade. Callers resolve the BP top-level
/// `default_agent_kind` and any CLI override before calling this
/// function and pass in the literal kind.
pub fn expand_file_refs_with_config(
    val: Value,
    cfg: &ResolveConfig,
    default_kind: AgentKind,
) -> Result<Value, LoadError> {
    match val {
        Value::Object(map) => {
            // `$file`: a single-key raw-string substitution.
            if map.len() == 1 {
                if let Some(Value::String(rel)) = map.get("$file") {
                    let full = resolve_ref_path(rel, cfg)?;
                    let content =
                        std::fs::read_to_string(&full).map_err(|e| LoadError::FileRef {
                            path: full.clone(),
                            msg: e.to_string(),
                        })?;
                    return Ok(Value::String(content));
                }
            }
            // `$agent_md` accepts either a single-key object or an object
            // with sibling keys. Sibling keys are shallow-merged onto the
            // expanded AgentDef object, so the caller's values override
            // whatever the AgentDef itself carried. Typical use: keep the
            // name and profile from the agent.md but override only
            // `spec.operator_ref` or `meta` at the call site.
            //
            // Kind resolution cascade: (a) if a sibling `"kind"` literal
            // is present, use it as-is; (b) otherwise, fall back to the
            // `default_kind` argument, which the caller already resolved
            // upstream from BP `default_agent_kind` or the CLI default.
            if let Some(Value::String(rel)) = map.get("$agent_md") {
                let full = resolve_ref_path(rel, cfg)?;
                // Peek at the sibling "kind"; fall back to `default_kind`
                // if absent.
                let resolved_kind = map
                    .get("kind")
                    .and_then(|v| serde_json::from_value::<AgentKind>(v.clone()).ok())
                    .unwrap_or_else(|| default_kind.clone());
                let def =
                    crate::agent_md::load_file(&full, resolved_kind).map_err(|e| {
                        LoadError::FileRef {
                            path: full.clone(),
                            msg: format!("agent_md parse: {e}"),
                        }
                    })?;
                let mut def_v = serde_json::to_value(&def).map_err(|e| LoadError::FileRef {
                    path: full.clone(),
                    msg: format!("agent_md serialize: {e}"),
                })?;
                if let Value::Object(def_map) = &mut def_v {
                    for (k, v) in map {
                        if k == "$agent_md" {
                            continue;
                        }
                        // Recursively expand the sibling before applying
                        // it as a shallow override.
                        let expanded = expand_file_refs_with_config(v, cfg, default_kind.clone())?;
                        def_map.insert(k, expanded);
                    }
                }
                return Ok(def_v);
            }
            let mut new_map = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                new_map.insert(
                    k,
                    expand_file_refs_with_config(v, cfg, default_kind.clone())?,
                );
            }
            Ok(Value::Object(new_map))
        }
        Value::Array(arr) => {
            let mut new_arr = Vec::with_capacity(arr.len());
            for v in arr {
                new_arr.push(expand_file_refs_with_config(v, cfg, default_kind.clone())?);
            }
            Ok(Value::Array(new_arr))
        }
        other => Ok(other),
    }
}

/// Backward-compat adapter — resolves refs against a single-tier
/// cascade (only `base`). New callers should use
/// [`expand_file_refs_with_config`] to opt into the full cascade.
pub fn expand_file_refs(
    val: Value,
    base: &Path,
    default_kind: AgentKind,
) -> Result<Value, LoadError> {
    let cfg = ResolveConfig::new(base.to_path_buf());
    expand_file_refs_with_config(val, &cfg, default_kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use tempfile::TempDir;

    fn write_md(dir: &Path, rel: &str, content: &str) -> PathBuf {
        let p = dir.join(rel);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&p, content).unwrap();
        p
    }

    const AGENT_MD: &str = "---\n\
name: researcher\n\
description: focus on XX/YY sites\n\
model: sonnet\n\
---\n\
You are a researcher. Focus on XX/YY sites.\n";

    #[test]
    fn agent_md_ref_expands_to_typed_agent_def_object() {
        let dir = TempDir::new().unwrap();
        write_md(dir.path(), "agents/r.md", AGENT_MD);

        let bp = json!({
            "agents": [ { "$agent_md": "agents/r.md" } ]
        });
        let resolved = expand_file_refs(bp, dir.path(), AgentKind::Operator).expect("expand ok");

        let agent = &resolved["agents"][0];
        assert!(agent.is_object(), "expanded value is JSON object");
        assert_eq!(agent["name"], "researcher");
        assert_eq!(agent["kind"], "operator", "default kind from loader");
        assert!(
            agent["profile"]["system_prompt"]
                .as_str()
                .unwrap()
                .contains("You are a researcher"),
            "profile.system_prompt baked from body, got: {:?}",
            agent["profile"]
        );
    }

    #[test]
    fn agent_md_ref_rejects_absolute_path() {
        let dir = TempDir::new().unwrap();
        let bp = json!({ "$agent_md": "/etc/passwd" });
        let err = expand_file_refs(bp, dir.path(), AgentKind::Operator).expect_err("abs rejected");
        assert!(format!("{err}").contains("absolute path"), "got: {err}");
    }

    #[test]
    fn agent_md_ref_rejects_parent_dir_escape() {
        let dir = TempDir::new().unwrap();
        let bp = json!({ "$agent_md": "../escape.md" });
        let err = expand_file_refs(bp, dir.path(), AgentKind::Operator).expect_err(".. rejected");
        assert!(format!("{err}").contains("parent-dir escape"), "got: {err}");
    }

    #[test]
    fn agent_md_ref_merges_sibling_keys_as_shallow_override() {
        let dir = TempDir::new().unwrap();
        write_md(dir.path(), "agents/r.md", AGENT_MD);
        let bp = json!({
            "$agent_md": "agents/r.md",
            "spec": { "operator_ref": "ws-sid-42" },
        });
        let resolved = expand_file_refs(bp, dir.path(), AgentKind::Operator).expect("expand ok");
        assert_eq!(resolved["name"], "researcher", "name from md preserved");
        assert_eq!(
            resolved["spec"]["operator_ref"], "ws-sid-42",
            "sibling spec overrides md default (= Null)"
        );
        assert!(
            resolved["profile"]["system_prompt"]
                .as_str()
                .unwrap()
                .contains("You are a researcher"),
            "profile from md preserved"
        );
    }

    #[test]
    fn file_ref_still_returns_raw_string_unchanged() {
        let dir = TempDir::new().unwrap();
        write_md(dir.path(), "prompts/raw.md", "raw body content");
        let bp = json!({ "$file": "prompts/raw.md" });
        let resolved = expand_file_refs(bp, dir.path(), AgentKind::Operator).expect("expand ok");
        assert_eq!(resolved, json!("raw body content"));
    }

    // ────────────────────────────────────────────────────────────────
    // Include-cascade tests (Phase 3 — GH issue 4c4e3eb8)
    // ────────────────────────────────────────────────────────────────

    #[test]
    fn cascade_falls_through_tiers() {
        // Same filename in three distinct dirs, each with a different body;
        // verify only the highest-priority tier hit is used.
        let base_dir = TempDir::new().unwrap();
        let cli_dir = TempDir::new().unwrap();
        let bundled_dir = TempDir::new().unwrap();

        // Tier 1 (base): the "correct" one.
        write_md(base_dir.path(), "prompts/x.md", "from-base");
        // Tier 4 (cli): shadowed by base.
        write_md(cli_dir.path(), "prompts/x.md", "from-cli");
        // Tier 6 (bundled): shadowed by base and cli.
        write_md(bundled_dir.path(), "prompts/x.md", "from-bundled");

        let cfg = ResolveConfig::new(base_dir.path().to_path_buf())
            .with_cli_includes(vec![cli_dir.path().to_path_buf()])
            .with_bundled_default(Some(bundled_dir.path().to_path_buf()));
        let bp = json!({ "$file": "prompts/x.md" });
        let resolved =
            expand_file_refs_with_config(bp, &cfg, AgentKind::Operator).expect("expand ok");
        assert_eq!(resolved, json!("from-base"));
    }

    #[test]
    fn cascade_reports_all_searched_paths_on_miss() {
        // No file exists in any tier — error message must name every
        // searched dir so the author can diagnose the miss.
        let base_dir = TempDir::new().unwrap();
        let cli_a = TempDir::new().unwrap();
        let cli_b = TempDir::new().unwrap();

        let cfg = ResolveConfig::new(base_dir.path().to_path_buf()).with_cli_includes(vec![
            cli_a.path().to_path_buf(),
            cli_b.path().to_path_buf(),
        ]);
        let bp = json!({ "$file": "prompts/missing.md" });
        let err = expand_file_refs_with_config(bp, &cfg, AgentKind::Operator)
            .expect_err("miss reports cascade");
        let msg = format!("{err}");
        assert!(
            msg.contains(base_dir.path().to_str().unwrap()),
            "base dir named: {msg}"
        );
        assert!(
            msg.contains(cli_a.path().to_str().unwrap()),
            "cli_a dir named: {msg}"
        );
        assert!(
            msg.contains(cli_b.path().to_str().unwrap()),
            "cli_b dir named: {msg}"
        );
        assert!(msg.contains("cascade"), "message flags cascade: {msg}");
    }

    #[test]
    fn env_includes_split_multi_paths() {
        // `env_blueprint_includes` splits `MSE_BLUEPRINT_INCLUDES` on the
        // platform separator. Set → read → unset to avoid poisoning
        // sibling tests.
        let old = std::env::var_os("MSE_BLUEPRINT_INCLUDES");
        let sep = if cfg!(windows) { ';' } else { ':' };
        std::env::set_var(
            "MSE_BLUEPRINT_INCLUDES",
            format!("/tmp/aaa{sep}/tmp/bbb{sep}/tmp/ccc"),
        );
        let got = env_blueprint_includes();
        // Restore before asserting so a failed assert still leaves the
        // env clean.
        match old {
            Some(v) => std::env::set_var("MSE_BLUEPRINT_INCLUDES", v),
            None => std::env::remove_var("MSE_BLUEPRINT_INCLUDES"),
        }
        assert_eq!(
            got,
            vec![
                PathBuf::from("/tmp/aaa"),
                PathBuf::from("/tmp/bbb"),
                PathBuf::from("/tmp/ccc"),
            ]
        );
    }

    #[test]
    fn in_bp_includes_reader_returns_empty_when_absent() {
        let bp = json!({ "id": "no-includes" });
        assert!(pre_read_in_bp_includes(&bp).is_empty());

        let bp2 = json!({
            "id": "with-includes",
            "blueprint_ref_includes": ["ext/agents", "vendor/samples"],
        });
        assert_eq!(
            pre_read_in_bp_includes(&bp2),
            vec![PathBuf::from("ext/agents"), PathBuf::from("vendor/samples")]
        );
    }

    #[test]
    fn absolute_and_parent_escape_still_rejected_across_cascade() {
        // Hygiene stays regardless of which cascade tiers are configured
        // — absolute paths and `..` are rejected before any tier is
        // walked.
        let base = TempDir::new().unwrap();
        let extra = TempDir::new().unwrap();
        let cfg = ResolveConfig::new(base.path().to_path_buf())
            .with_cli_includes(vec![extra.path().to_path_buf()]);

        let err_abs = expand_file_refs_with_config(
            json!({ "$file": "/etc/passwd" }),
            &cfg,
            AgentKind::Operator,
        )
        .expect_err("absolute rejected");
        assert!(
            format!("{err_abs}").contains("absolute path"),
            "got: {err_abs}"
        );

        let err_parent = expand_file_refs_with_config(
            json!({ "$file": "../escape.md" }),
            &cfg,
            AgentKind::Operator,
        )
        .expect_err(".. rejected");
        assert!(
            format!("{err_parent}").contains("parent-dir escape"),
            "got: {err_parent}"
        );
    }

    #[test]
    fn bundled_default_used_only_when_no_other_match() {
        // Bundled default (tier 6) is the last resort — used only when
        // none of tiers 1-5 match.
        let base_dir = TempDir::new().unwrap();
        let bundled_dir = TempDir::new().unwrap();
        write_md(bundled_dir.path(), "prompts/y.md", "from-bundled");

        let cfg = ResolveConfig::new(base_dir.path().to_path_buf())
            .with_bundled_default(Some(bundled_dir.path().to_path_buf()));
        let bp = json!({ "$file": "prompts/y.md" });
        let resolved =
            expand_file_refs_with_config(bp, &cfg, AgentKind::Operator).expect("expand ok");
        assert_eq!(resolved, json!("from-bundled"));
    }
}
