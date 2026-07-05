//! Server config file support (`~/.mse/config.toml` by default).
//!
//! Resolution precedence: **CLI flag > config file > built-in default**.
//! CLI flags are represented as `Option<T>` on the `main.rs` `Args` struct
//! (rather than relying on `clap`'s `default_value`) so "not passed" can be
//! distinguished from "matches the default value"; [`resolve`] performs the
//! actual 3-way merge.
//!
//! Design rationale: the config file becomes the lifecycle SoT; the launchd
//! plist's `ProgramArguments` stays fixed at `<server-bin> --config <path>`,
//! so changing settings = editing the file + restarting, not editing the plist.

use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Default config path, `~/.mse/config.toml`. Falls back to a relative path
/// literal when `$HOME` is unset (best-effort; dev-only edge case).
pub fn default_config_path() -> PathBuf {
    match std::env::var("HOME") {
        Ok(home) => PathBuf::from(home).join(".mse").join("config.toml"),
        Err(_) => PathBuf::from(".mse/config.toml"),
    }
}

/// Default `BlueprintStore` root, `~/.mse/store`. Same `$HOME` fallback
/// rule as [`default_config_path`]. The store is always git-backed;
/// config/CLI only override *where* the repos live, never whether they
/// persist.
pub fn default_store_path() -> PathBuf {
    match std::env::var("HOME") {
        Ok(home) => PathBuf::from(home).join(".mse").join("store"),
        Err(_) => PathBuf::from(".mse/store"),
    }
}

/// TOML config schema. All fields are optional — a missing field falls back
/// to the CLI-supplied value or the built-in default at [`resolve`] time.
/// Unknown fields are a hard error (`deny_unknown_fields`; typo guard).
#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    /// Listen address string (e.g. `"127.0.0.1:7777"`), parsed at [`resolve`] time.
    pub bind: Option<String>,
    /// Whether the enhance flow (Lua + AgentBlock factories) is baked into the registry.
    pub enable_enhance_flow: Option<bool>,
    /// Base dir for `$file` / `$agent_md` ref expansion in seeded Blueprints.
    pub blueprint_ref_base: Option<PathBuf>,
    /// Root path for the git-backed `BlueprintStore` (when using the git2 backend).
    pub git_store_path: Option<PathBuf>,
    /// Path to the SQLite database file backing the `IssueStore`. `None` = fall
    /// back to `InMemoryIssueStore` (process-volatile).
    pub issue_store_path: Option<PathBuf>,
    /// Path to the SQLite database file backing the `EnhanceSettingStore`.
    /// `None` = fall back to `InMemoryEnhanceSettingStore` (process-volatile).
    pub enhance_setting_store_path: Option<PathBuf>,
    /// Path to the SQLite database file backing the `EnhanceLogStore`.
    /// `None` = fall back to `InMemoryEnhanceLogStore` (process-volatile).
    pub enhance_log_store_path: Option<PathBuf>,
    /// Path to the SQLite database file backing the `OutputStore`.
    /// `None` = fall back to `InMemoryOutputStore` (process-volatile).
    pub output_store_path: Option<PathBuf>,
    /// Seed blueprint id used in combined-mode default routing.
    pub seed_blueprint_id: Option<String>,
    /// snake_case `AgentKind` literal (`operator` / `agent_block` / `rust_fn` /
    /// `lua` / `subprocess`). Validated by the caller after [`resolve`].
    pub default_agent_kind: Option<String>,
    /// Shared secret used to verify/sign `CapToken` HMAC signatures.
    pub token_secret: Option<String>,
}

/// CLI-side overrides. Mirrors [`FileConfig`] field-for-field. Kept as a
/// separate type (rather than reusing `clap::Args` directly) so this module
/// stays independent of the `clap` derive on `main.rs::Args`.
#[derive(Debug, Default, Clone)]
pub struct CliOverrides {
    /// `--bind` value, unparsed (mirrors [`FileConfig::bind`]).
    pub bind: Option<String>,
    /// `--enable-enhance-flow` flag.
    pub enable_enhance_flow: Option<bool>,
    /// `--blueprint-ref-base` value.
    pub blueprint_ref_base: Option<PathBuf>,
    /// `--git-store-path` value.
    pub git_store_path: Option<PathBuf>,
    /// `--issue-store-path` value (mirrors [`FileConfig::issue_store_path`]).
    pub issue_store_path: Option<PathBuf>,
    /// `--enhance-setting-store-path` value.
    pub enhance_setting_store_path: Option<PathBuf>,
    /// `--enhance-log-store-path` value.
    pub enhance_log_store_path: Option<PathBuf>,
    /// `--output-store-path` value.
    pub output_store_path: Option<PathBuf>,
    /// `--seed-blueprint-id` value.
    pub seed_blueprint_id: Option<String>,
    /// `--default-agent-kind` value (snake_case `AgentKind` literal, unvalidated).
    pub default_agent_kind: Option<String>,
    /// `--token-secret` value.
    pub token_secret: Option<String>,
}

/// Fully resolved config — every field has the built-in default applied.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedConfig {
    /// Parsed listen address for the server to bind to.
    pub bind: SocketAddr,
    /// Whether the enhance flow (Lua + AgentBlock factories) is baked into the registry.
    pub enable_enhance_flow: bool,
    /// Base dir for `$file` / `$agent_md` ref expansion in seeded Blueprints.
    pub blueprint_ref_base: Option<PathBuf>,
    /// Root path for the git-backed `BlueprintStore`. Always set — defaults
    /// to [`default_store_path`] (`~/.mse/store`) when neither CLI nor config
    /// file provides one.
    pub git_store_path: PathBuf,
    /// Path to the SQLite database file backing the `IssueStore`. `None` = fall
    /// back to `InMemoryIssueStore` (process-volatile).
    pub issue_store_path: Option<PathBuf>,
    /// Path to the SQLite database file backing the `EnhanceSettingStore`.
    /// `None` = `InMemoryEnhanceSettingStore`.
    pub enhance_setting_store_path: Option<PathBuf>,
    /// Path to the SQLite database file backing the `EnhanceLogStore`.
    /// `None` = `InMemoryEnhanceLogStore`.
    pub enhance_log_store_path: Option<PathBuf>,
    /// Path to the SQLite database file backing the `OutputStore`.
    /// `None` = `InMemoryOutputStore`.
    pub output_store_path: Option<PathBuf>,
    /// Seed blueprint id used in combined-mode default routing.
    pub seed_blueprint_id: String,
    /// snake_case `AgentKind` literal, unvalidated. `None` = caller applies
    /// the schema-impl `Default` (`Operator`).
    pub default_agent_kind: Option<String>,
    /// Shared secret used to verify/sign `CapToken` HMAC signatures.
    pub token_secret: Option<String>,
}

impl Default for ResolvedConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            enable_enhance_flow: false,
            blueprint_ref_base: None,
            git_store_path: default_store_path(),
            issue_store_path: None,
            enhance_setting_store_path: None,
            enhance_log_store_path: None,
            output_store_path: None,
            seed_blueprint_id: "main".into(),
            default_agent_kind: None,
            token_secret: None,
        }
    }
}

fn default_bind() -> SocketAddr {
    "127.0.0.1:7777"
        .parse()
        .expect("literal default bind must parse")
}

/// Load + parse a TOML config file. A missing file resolves to
/// `Ok(FileConfig::default())` (built-in default fallback, per module doc);
/// any other IO error or a parse error is `Err` — a malformed config file
/// must not be silently ignored (fail-loud).
pub fn load_file_config(path: &Path) -> Result<FileConfig, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => toml::from_str(&text)
            .map_err(|e| format!("config file {} parse error: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(FileConfig::default()),
        Err(e) => Err(format!("config file {} read error: {e}", path.display())),
    }
}

/// 3-way merge: CLI > file > built-in default. `bind` requires a parse step
/// (both CLI and file carry it as a string); a parse error surfaces as `Err`.
pub fn resolve(cli: CliOverrides, file: FileConfig) -> Result<ResolvedConfig, String> {
    let default = ResolvedConfig::default();

    let bind = match cli.bind.or(file.bind) {
        Some(s) => s
            .parse::<SocketAddr>()
            .map_err(|e| format!("bind {s:?}: {e}"))?,
        None => default.bind,
    };

    Ok(ResolvedConfig {
        bind,
        enable_enhance_flow: cli
            .enable_enhance_flow
            .or(file.enable_enhance_flow)
            .unwrap_or(default.enable_enhance_flow),
        blueprint_ref_base: cli.blueprint_ref_base.or(file.blueprint_ref_base),
        git_store_path: cli
            .git_store_path
            .or(file.git_store_path)
            .unwrap_or_else(default_store_path),
        issue_store_path: cli.issue_store_path.or(file.issue_store_path),
        enhance_setting_store_path: cli
            .enhance_setting_store_path
            .or(file.enhance_setting_store_path),
        enhance_log_store_path: cli.enhance_log_store_path.or(file.enhance_log_store_path),
        output_store_path: cli.output_store_path.or(file.output_store_path),
        seed_blueprint_id: cli
            .seed_blueprint_id
            .or(file.seed_blueprint_id)
            .unwrap_or(default.seed_blueprint_id),
        default_agent_kind: cli.default_agent_kind.or(file.default_agent_kind),
        token_secret: cli.token_secret.or(file.token_secret),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_cli_flag_wins_over_file_and_default() {
        let cli = CliOverrides {
            bind: Some("127.0.0.1:9999".into()),
            ..Default::default()
        };
        let file = FileConfig {
            bind: Some("127.0.0.1:8888".into()),
            ..Default::default()
        };
        let resolved = resolve(cli, file).expect("resolve");
        assert_eq!(
            resolved.bind,
            "127.0.0.1:9999".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn resolve_file_wins_over_built_in_default_when_cli_absent() {
        let cli = CliOverrides::default();
        let file = FileConfig {
            seed_blueprint_id: Some("from-file".into()),
            enable_enhance_flow: Some(true),
            ..Default::default()
        };
        let resolved = resolve(cli, file).expect("resolve");
        assert_eq!(resolved.seed_blueprint_id, "from-file");
        assert!(resolved.enable_enhance_flow);
    }

    #[test]
    fn resolve_built_in_default_when_cli_and_file_absent() {
        let resolved = resolve(CliOverrides::default(), FileConfig::default()).expect("resolve");
        assert_eq!(resolved.bind, default_bind());
        assert_eq!(resolved.seed_blueprint_id, "main");
        assert!(!resolved.enable_enhance_flow);
        assert_eq!(resolved.git_store_path, default_store_path());
    }

    #[test]
    fn resolve_git_store_path_file_overrides_default_location() {
        let file = FileConfig {
            git_store_path: Some(PathBuf::from("/tmp/custom-store")),
            ..Default::default()
        };
        let resolved = resolve(CliOverrides::default(), file).expect("resolve");
        assert_eq!(resolved.git_store_path, PathBuf::from("/tmp/custom-store"));
    }

    #[test]
    fn resolve_bind_parse_error_is_propagated() {
        let cli = CliOverrides {
            bind: Some("not-a-valid-addr".into()),
            ..Default::default()
        };
        let err = resolve(cli, FileConfig::default()).unwrap_err();
        assert!(err.contains("not-a-valid-addr"), "unexpected error: {err}");
    }

    #[test]
    fn load_file_config_rejects_unknown_fields() {
        let toml_text = "bind = \"127.0.0.1:1234\"\ntypo_field = true\n";
        let err = toml::from_str::<FileConfig>(toml_text).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("typo_field") || msg.contains("unknown field"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn load_file_config_missing_file_falls_back_to_default() {
        let path = std::path::Path::new("/nonexistent/mse-config-test-path/config.toml");
        let cfg = load_file_config(path).expect("missing file should not error");
        assert_eq!(cfg, FileConfig::default());
    }

    #[test]
    fn load_file_config_parses_valid_toml() {
        let dir = std::env::temp_dir().join(format!("server-config-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create tmp dir");
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            "bind = \"127.0.0.1:7000\"\nenable_enhance_flow = true\nseed_blueprint_id = \"main\"\n",
        )
        .expect("write tmp config");
        let cfg = load_file_config(&path).expect("parse tmp config");
        assert_eq!(cfg.bind.as_deref(), Some("127.0.0.1:7000"));
        assert_eq!(cfg.enable_enhance_flow, Some(true));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
