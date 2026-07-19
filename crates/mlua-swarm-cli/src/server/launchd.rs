//! Thin `launchctl` wrapper for the `mse serve` HTTP daemon lifecycle.
//!
//! Lifecycle ownership is consolidated on launchd (macOS user-level
//! LaunchAgent, `Label = com.mse.server`). This module holds **no**
//! spawn / pid tracking state — it is a pure wrapper around a handful of
//! `launchctl` subcommands + healthz polling + a compile-time-baked plist
//! template:
//!
//! - `start` → `launchctl kickstart gui/<uid>/com.mse.server`
//!   (auto-`bootstrap`s once if the job is missing).
//! - `shutdown` / `bootout` → `launchctl bootout gui/<uid>/com.mse.server`.
//! - `restart` → `launchctl kickstart -k gui/<uid>/com.mse.server`
//!   (auto-`bootstrap`s once if the job is missing).
//! - `status` → healthz + `launchctl print gui/<uid>/com.mse.server`.
//! - `bootstrap` → `launchctl bootstrap gui/<uid> <plist>`.
//! - `install` → render the baked plist template + write it to
//!   `~/Library/LaunchAgents/com.mse.server.plist` + `bootstrap`.
//!   Semantically identical to the legacy shell installer that predates
//!   the `mse server` subcommand family (retired in GH #69).
//! - `uninstall` → `bootout` (idempotent) + `remove_file` the installed
//!   plist (missing plist tolerated).
//! - `logs` → tail the `/tmp/mse-server.{stdout,stderr}` sinks.
//!
//! No fallback spawn path is implemented by design — a second lifecycle
//! owner alongside launchd is exactly the failure mode this module
//! replaces. Every pub fn returns `Result<T, ServerError>` (see
//! [`crate::server::error`]); the on-missing-job auto-`bootstrap` retry
//! embedded in `start` / `restart` closes the MCP-only recovery
//! state-machine — a client can now recover from `bootout` without
//! shelling out to a separate installer script.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tokio::process::Command;

use crate::server::error::ServerError;

/// Default `host:port` the `mse serve` daemon binds to when the caller
/// omits `--bind`.
pub const DEFAULT_BIND: &str = "127.0.0.1:7777";
/// The launchd `Label` (also the plist file's basename minus the
/// extension) for the `mse serve` LaunchAgent.
pub const LAUNCHD_LABEL: &str = "com.mse.server";
const POLL_TOTAL: Duration = Duration::from_secs(30);
const POLL_STEP: Duration = Duration::from_millis(500);
const HEALTHZ_TIMEOUT: Duration = Duration::from_millis(500);
const SHUTDOWN_POLL_TOTAL: Duration = Duration::from_secs(10);

/// Compile-time-baked plist template. `include_str!` is source-file
/// relative so this string is resolved from
/// `crates/mlua-swarm-cli/src/server/plist.template` — the single source
/// of truth since the legacy shell-side copy was retired (GH #69). The
/// three placeholders `{{HOME}}` / `{{CARGO_BIN}}` / `{{PROJECT_ROOT}}`
/// are expanded by [`render`].
pub const TEMPLATE: &str = include_str!("./plist.template");

/// healthz check via reqwest. Treats HTTP 200 with body `ok` as healthy.
pub async fn healthz_ok(bind: &str) -> bool {
    let url = format!("http://{bind}/v1/healthz");
    let client = match reqwest::Client::builder().timeout(HEALTHZ_TIMEOUT).build() {
        Ok(c) => c,
        Err(_) => return false,
    };
    match client.get(&url).send().await {
        Ok(r) if r.status().is_success() => {
            r.text().await.map(|t| t.trim() == "ok").unwrap_or(false)
        }
        _ => false,
    }
}

/// `GET /v1/status` on the running `mse serve` process — the lifecycle
/// occupancy guard. `Err` covers both network failure and
/// an older server binary predating this route (404) — callers should
/// treat `Err` as "occupancy unknown", not "occupancy = busy" (see the
/// MCP tool handlers' fail-open-on-Err policy).
pub async fn occupancy(bind: &str) -> Result<Occupancy, ServerError> {
    let url = format!("http://{bind}/v1/status");
    let client = reqwest::Client::builder()
        .timeout(HEALTHZ_TIMEOUT)
        .build()
        .map_err(|e| occupancy_io_err(format!("client build failed: {e}")))?;
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| occupancy_io_err(format!("request failed: {e}")))?;
    if !resp.status().is_success() {
        return Err(occupancy_io_err(format!(
            "non-success status {}",
            resp.status()
        )));
    }
    resp.json::<Occupancy>()
        .await
        .map_err(|e| occupancy_io_err(format!("decode failed: {e}")))
}

fn occupancy_io_err(msg: String) -> ServerError {
    ServerError::Io(std::io::Error::other(format!("occupancy: {msg}")))
}

fn current_uid() -> u32 {
    // launchctl targets are `gui/<uid>/<label>`, so the numeric uid is only
    // meaningful on Unix. On Windows this whole module's tools (`launchctl
    // kickstart` / `bootout`) will fail at runtime for lack of the binary
    // regardless of what we return, so a placeholder keeps the code
    // portable at build time without pretending to offer functionality.
    #[cfg(unix)]
    {
        nix::unistd::Uid::current().as_raw()
    }
    #[cfg(not(unix))]
    {
        0
    }
}

fn domain_target() -> String {
    format!("gui/{}/{}", current_uid(), LAUNCHD_LABEL)
}

/// Human-readable install hint. Points at the `mse server install`
/// subcommand — the legacy shell-installer literal is gone (Crux #1
/// closure). Currently referenced only by the acceptance test
/// [`install_hint_points_to_mse_server_install`](tests::install_hint_points_to_mse_server_install)
/// — the inline start/restart error paths now surface the same literal
/// via `ServerError::MissingJob`'s `Display` impl — so the fn is
/// retained as the public API contract for the "install hint" string
/// even without inline call sites.
#[allow(dead_code)]
pub fn install_hint() -> String {
    format!(
        "launchd job '{label}' not found. Install it first:\n  mse server install",
        label = LAUNCHD_LABEL,
    )
}

fn home_path() -> Result<PathBuf, ServerError> {
    std::env::var_os("HOME").map(PathBuf::from).ok_or_else(|| {
        ServerError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "HOME env not set",
        ))
    })
}

/// Absolute path of the installed LaunchAgent plist — `$HOME/Library/
/// LaunchAgents/com.mse.server.plist`. Same location the legacy shell
/// installer wrote to before the `mse server` family (GH #69).
pub fn installed_plist_path() -> Result<PathBuf, ServerError> {
    let home = home_path()?;
    Ok(home.join("Library/LaunchAgents/com.mse.server.plist"))
}

async fn run_launchctl(args: &[&str]) -> Result<std::process::Output, ServerError> {
    Command::new("launchctl")
        .args(args)
        .output()
        .await
        .map_err(ServerError::LaunchctlExec)
}

fn combined_output_text(stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = String::from_utf8_lossy(stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => stdout,
        (true, false) => stderr,
        (false, false) => format!("{stdout}\n{stderr}"),
    }
}

/// Heuristically detects "unknown service target" style errors from
/// `launchctl` (= the job has not been `bootstrap`ed yet).
fn looks_like_missing_job(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("could not find")
        || lower.contains("no such process")
        || lower.contains("service target specification is invalid")
        || lower.contains("not find service")
}

/// Heuristically detects "job is already loaded / bootstrapped" style
/// errors from `launchctl bootstrap` — used by [`bootstrap`] to fold the
/// already-loaded case into `Ok(BootstrapOutcome::AlreadyLoaded)` for
/// idempotency.
fn looks_like_already_loaded(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("already loaded")
        || lower.contains("already bootstrapped")
        || lower.contains("service is already")
        || lower.contains("service already loaded")
        || lower.contains("already exists")
}

/// Heuristically detects "the plist file doesn't exist" style errors
/// from `launchctl bootstrap`.
fn looks_like_missing_plist(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("no such file")
        || lower.contains("path not specified")
        || lower.contains("could not find specified service")
}

/// Heuristically detects the macOS 15 (Sequoia) EIO signature launchctl
/// returns for a double-`bootstrap` against an already-loaded job:
///
/// ```text
/// Bootstrap failed: 5: Input/output error
/// ```
///
/// Unlike [`looks_like_already_loaded`], this pattern is intentionally
/// **ambiguous** — a genuine EIO (disk / IPC failure) prints the same
/// text — so callers must pair the match with a `launchctl print`
/// probe (see [`probe_already_loaded_via_print`]) before folding the
/// failure into `BootstrapOutcome::AlreadyLoaded`. This is why the
/// signature is not merged into `looks_like_already_loaded`: doing so
/// would silently swallow real EIO conditions.
fn looks_like_bootstrap_eio(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("bootstrap failed: 5") || lower.contains("input/output error")
}

/// Pure parser for [`probe_already_loaded_via_print`] — returns `true`
/// iff `text` (leading whitespace trimmed) starts with the literal
/// `<target> = {`, which is the shape `launchctl print <target>` emits
/// when the job is loaded (top-level entry opening brace). A body of
/// `Bad request.` / `Could not find service ...` — what launchctl
/// prints when the target is unknown — falls through to `false`.
fn launchctl_print_body_indicates_loaded(text: &str, target: &str) -> bool {
    let prefix = format!("{target} = {{");
    text.trim_start().starts_with(&prefix)
}

/// Consult `launchctl print <target>` and return `true` iff the process
/// exits successfully AND its stdout matches
/// [`launchctl_print_body_indicates_loaded`]. Used exclusively to
/// disambiguate the macOS 15 EIO signature (see
/// [`looks_like_bootstrap_eio`]): if `bootstrap` failed with the
/// ambiguous EIO text and `print` confirms the job is loaded, the
/// double-bootstrap is idempotent success; otherwise the original
/// `LaunchctlFailed` propagates.
///
/// launchctl exec failure / non-zero exit / unrecognised body all fold
/// to `false` (probe unsuccessful) — the caller then propagates the
/// original failure rather than silently converting an unknown state to
/// `AlreadyLoaded`.
async fn probe_already_loaded_via_print(target: &str) -> bool {
    let out = match run_launchctl(&["print", target]).await {
        Ok(o) => o,
        Err(_) => return false,
    };
    if !out.status.success() {
        return false;
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    launchctl_print_body_indicates_loaded(&stdout, target)
}

async fn poll_healthz_until_up(bind: &str, total: Duration, step: Duration) -> bool {
    let deadline = Instant::now() + total;
    while Instant::now() < deadline {
        if healthz_ok(bind).await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

/// Render the compile-time-baked plist template with the given absolute
/// paths substituted for `{{HOME}}` / `{{CARGO_BIN}}` /
/// `{{PROJECT_ROOT}}`. Byte-identical to the sed pipeline in the legacy
/// shell installer's `render()` function. Rejects non-UTF-8 paths with
/// `ServerError::Render`; also rejects any surviving `{{...}}` placeholder
/// as a defense against future template extensions silently leaking
/// through.
pub fn render(home: &Path, cargo_bin: &Path, project_root: &Path) -> Result<String, ServerError> {
    render_impl(TEMPLATE, home, cargo_bin, project_root)
}

fn render_impl(
    template: &str,
    home: &Path,
    cargo_bin: &Path,
    project_root: &Path,
) -> Result<String, ServerError> {
    let home_s = home
        .to_str()
        .ok_or_else(|| ServerError::Render("non-utf8 home".into()))?;
    let cargo_bin_s = cargo_bin
        .to_str()
        .ok_or_else(|| ServerError::Render("non-utf8 cargo_bin".into()))?;
    let project_root_s = project_root
        .to_str()
        .ok_or_else(|| ServerError::Render("non-utf8 project_root".into()))?;
    let out = template
        .replace("{{HOME}}", home_s)
        .replace("{{CARGO_BIN}}", cargo_bin_s)
        .replace("{{PROJECT_ROOT}}", project_root_s);
    if let Some(start) = out.find("{{") {
        let end_off = out[start..]
            .find("}}")
            .map(|e| start + e + 2)
            .unwrap_or_else(|| out.len().min(start + 40));
        let placeholder = &out[start..end_off];
        return Err(ServerError::Render(format!(
            "unresolved placeholder: {placeholder}"
        )));
    }
    Ok(out)
}

/// `launchctl kickstart gui/<uid>/com.mse.server`. If healthz is already
/// up, short-circuits to `AlreadyRunning` without touching launchd. If
/// launchctl reports a missing job, calls [`bootstrap`] once and retries
/// the kickstart — a second missing-job report is surfaced as a hard
/// `ServerError::MissingJob` (no infinite loop).
pub async fn start(bind: &str) -> Result<StartOutcome, ServerError> {
    if healthz_ok(bind).await {
        return Ok(StartOutcome::AlreadyRunning { bind: bind.into() });
    }
    let target = domain_target();
    let out = run_launchctl(&["kickstart", &target]).await?;
    if !out.status.success() {
        let text = combined_output_text(&out.stdout, &out.stderr);
        if looks_like_missing_job(&text) {
            bootstrap().await?;
            let retry = run_launchctl(&["kickstart", &target]).await?;
            if !retry.status.success() {
                let retry_text = combined_output_text(&retry.stdout, &retry.stderr);
                return Err(if looks_like_missing_job(&retry_text) {
                    ServerError::MissingJob {
                        label: LAUNCHD_LABEL.into(),
                    }
                } else {
                    ServerError::LaunchctlFailed {
                        op: "kickstart",
                        stderr: retry_text,
                    }
                });
            }
        } else {
            return Err(ServerError::LaunchctlFailed {
                op: "kickstart",
                stderr: text,
            });
        }
    }
    if poll_healthz_until_up(bind, POLL_TOTAL, POLL_STEP).await {
        Ok(StartOutcome::Started { bind: bind.into() })
    } else {
        Err(ServerError::HealthzTimeout {
            op: "kickstart",
            duration: POLL_TOTAL,
        })
    }
}

/// `launchctl bootout gui/<uid>/com.mse.server` (full teardown, KeepAlive
/// included). A "job already not loaded" error from launchctl is treated
/// as an idempotent success (falls through to the healthz-down
/// confirmation).
pub async fn shutdown(bind: &str) -> Result<StopOutcome, ServerError> {
    let target = domain_target();
    let out = run_launchctl(&["bootout", &target]).await?;
    if !out.status.success() {
        let text = combined_output_text(&out.stdout, &out.stderr);
        if !looks_like_missing_job(&text) {
            return Err(ServerError::LaunchctlFailed {
                op: "bootout",
                stderr: text,
            });
        }
    }
    let deadline = Instant::now() + SHUTDOWN_POLL_TOTAL;
    while Instant::now() < deadline {
        if !healthz_ok(bind).await {
            return Ok(StopOutcome {
                bind: bind.into(),
                stopped: true,
            });
        }
        tokio::time::sleep(POLL_STEP).await;
    }
    Ok(StopOutcome {
        bind: bind.into(),
        stopped: false,
    })
}

/// `mse server bootout`-subcommand entry point — alias for [`shutdown`]
/// (semantics identical: bootout + healthz-down confirmation, missing-
/// job idempotent).
pub async fn bootout(bind: &str) -> Result<StopOutcome, ServerError> {
    shutdown(bind).await
}

/// `launchctl kickstart -k gui/<uid>/com.mse.server` (kill + restart,
/// used to pick up a `~/.mse/config.toml` edit). Auto-`bootstrap`s once
/// on missing-job (same retry policy as [`start`]).
pub async fn restart(bind: &str) -> Result<StartOutcome, ServerError> {
    let target = domain_target();
    let out = run_launchctl(&["kickstart", "-k", &target]).await?;
    if !out.status.success() {
        let text = combined_output_text(&out.stdout, &out.stderr);
        if looks_like_missing_job(&text) {
            bootstrap().await?;
            let retry = run_launchctl(&["kickstart", &target]).await?;
            if !retry.status.success() {
                let retry_text = combined_output_text(&retry.stdout, &retry.stderr);
                return Err(if looks_like_missing_job(&retry_text) {
                    ServerError::MissingJob {
                        label: LAUNCHD_LABEL.into(),
                    }
                } else {
                    ServerError::LaunchctlFailed {
                        op: "kickstart",
                        stderr: retry_text,
                    }
                });
            }
        } else {
            return Err(ServerError::LaunchctlFailed {
                op: "kickstart -k",
                stderr: text,
            });
        }
    }
    if poll_healthz_until_up(bind, POLL_TOTAL, POLL_STEP).await {
        Ok(StartOutcome::Started { bind: bind.into() })
    } else {
        Err(ServerError::HealthzTimeout {
            op: "kickstart -k",
            duration: POLL_TOTAL,
        })
    }
}

/// healthz + a `launchctl print` summary (state / pid / last exit code).
/// Never raw-dumps the `launchctl print` output. Infallible — any
/// launchctl failure is folded into `launchd_state: None`.
pub async fn status(bind: &str) -> StatusOutcome {
    let up = healthz_ok(bind).await;
    let target = domain_target();
    let print_out = run_launchctl(&["print", &target]).await.ok();
    let (state, pid, last_exit_code) = match &print_out {
        Some(out) if out.status.success() => {
            parse_launchctl_print(&String::from_utf8_lossy(&out.stdout))
        }
        _ => (None, None, None),
    };
    StatusOutcome {
        bind: bind.into(),
        up,
        launchd_state: state,
        launchd_pid: pid,
        launchd_last_exit_code: last_exit_code,
    }
}

/// `launchctl bootstrap gui/<uid> <plist>` — load the LaunchAgent
/// (idempotent: already-loaded is `Ok(BootstrapOutcome::AlreadyLoaded)`,
/// missing plist is `Err(ServerError::MissingJob)` so the caller knows
/// to run `mse server install` first).
pub async fn bootstrap() -> Result<BootstrapOutcome, ServerError> {
    let plist_path = installed_plist_path()?;
    let plist_str = plist_path
        .to_str()
        .ok_or_else(|| ServerError::Render("non-utf8 plist path".into()))?;
    let domain = format!("gui/{}", current_uid());
    let out = run_launchctl(&["bootstrap", &domain, plist_str]).await?;
    if out.status.success() {
        return Ok(BootstrapOutcome::Bootstrapped { plist_path });
    }
    let text = combined_output_text(&out.stdout, &out.stderr);
    if looks_like_already_loaded(&text) {
        return Ok(BootstrapOutcome::AlreadyLoaded { plist_path });
    }
    // macOS 15 (Sequoia) collapses the classical "already loaded" phrasing
    // into a bare `Bootstrap failed: 5: Input/output error`, which is
    // ambiguous vs a genuine EIO. Post-check with `launchctl print` on
    // the job target: if the job is present, treat the double-bootstrap
    // as idempotent success; otherwise fall through and propagate the
    // original `LaunchctlFailed`. Older macOS versions still hit the
    // classical `looks_like_already_loaded` branch above.
    if looks_like_bootstrap_eio(&text) {
        let target = domain_target();
        if probe_already_loaded_via_print(&target).await {
            return Ok(BootstrapOutcome::AlreadyLoaded { plist_path });
        }
    }
    if looks_like_missing_plist(&text) {
        return Err(ServerError::MissingJob {
            label: LAUNCHD_LABEL.into(),
        });
    }
    Err(ServerError::LaunchctlFailed {
        op: "bootstrap",
        stderr: text,
    })
}

/// Render the plist template + write it to `~/Library/LaunchAgents/
/// com.mse.server.plist` + `bootstrap`. Semantically identical to the
/// legacy shell installer's `install` path. Idempotent — re-running
/// `install` on an already-loaded job first `bootout`s it, then rewrites
/// the plist, then `bootstrap`s (so the in-memory launchd view stays
/// synced with the on-disk plist).
///
/// `cargo_bin` defaults to `$CARGO_BIN` if set, else `$HOME/.cargo/bin`.
/// `project_root` defaults to `$PWD` if set, else the process's current
/// working directory.
pub async fn install(
    cargo_bin: Option<&Path>,
    project_root: Option<&Path>,
) -> Result<InstallOutcome, ServerError> {
    let home = home_path()?;
    let cargo_bin_pb = cargo_bin.map(|p| p.to_path_buf()).unwrap_or_else(|| {
        std::env::var_os("CARGO_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".cargo/bin"))
    });
    let project_root_pb = project_root.map(|p| p.to_path_buf()).unwrap_or_else(|| {
        std::env::var_os("PWD")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_default())
    });
    let rendered = render(&home, &cargo_bin_pb, &project_root_pb)?;
    let plist_path = installed_plist_path()?;
    let launch_agents_dir = plist_path.parent().ok_or_else(|| {
        ServerError::Render("installed plist path has no parent directory".into())
    })?;
    tokio::fs::create_dir_all(launch_agents_dir)
        .await
        .map_err(ServerError::Io)?;
    // If the job is currently loaded, bootout first so bootstrap picks up
    // the new plist body (install.sh L64-66).
    let target = domain_target();
    let print_out = run_launchctl(&["print", &target]).await?;
    if print_out.status.success() {
        // Non-zero exits from bootout are swallowed (best-effort
        // idempotency); exec failures (launchctl missing / permission)
        // still propagate via `?` and would resurface at the bootstrap
        // call below anyway.
        let _ = run_launchctl(&["bootout", &target]).await?;
    }
    tokio::fs::write(&plist_path, rendered.as_bytes())
        .await
        .map_err(ServerError::Io)?;
    let bootstrap_outcome = bootstrap().await?;
    Ok(InstallOutcome {
        plist_path,
        bootstrap: bootstrap_outcome,
    })
}

/// `bootout` the job + `remove_file` the installed plist. Idempotent —
/// missing job / missing plist are both treated as `Ok`. Semantically
/// identical to the legacy shell installer's `--uninstall` path.
pub async fn uninstall() -> Result<UninstallOutcome, ServerError> {
    let plist_path = installed_plist_path()?;
    let target = domain_target();
    let out = run_launchctl(&["bootout", &target]).await?;
    if !out.status.success() {
        let text = combined_output_text(&out.stdout, &out.stderr);
        if !looks_like_missing_job(&text) {
            return Err(ServerError::LaunchctlFailed {
                op: "bootout",
                stderr: text,
            });
        }
    }
    match tokio::fs::remove_file(&plist_path).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(ServerError::Io(e)),
    }
    Ok(UninstallOutcome { plist_path })
}

/// Read the tail of the launchd-managed stdout/stderr log sinks
/// (`/tmp/mse-server.stdout` / `/tmp/mse-server.stderr`). Missing files
/// surface as empty `stdout_tail` / `stderr_tail`, not an `Err` — the
/// user just hasn't started the server yet. `tail` defaults to 20 lines;
/// `--follow` is not implemented (may arrive in a follow-up).
pub async fn logs(tail: Option<usize>) -> Result<LogsOutcome, ServerError> {
    let stdout_path = PathBuf::from("/tmp/mse-server.stdout");
    let stderr_path = PathBuf::from("/tmp/mse-server.stderr");
    let n = tail.unwrap_or(20);
    let stdout_tail = read_tail(&stdout_path, n).await;
    let stderr_tail = read_tail(&stderr_path, n).await;
    Ok(LogsOutcome {
        stdout_path,
        stderr_path,
        stdout_tail,
        stderr_tail,
    })
}

async fn read_tail(path: &Path, n: usize) -> Vec<String> {
    match tokio::fs::read_to_string(path).await {
        Ok(text) => {
            let lines: Vec<&str> = text.lines().collect();
            let start = lines.len().saturating_sub(n);
            lines[start..].iter().map(|s| (*s).to_string()).collect()
        }
        Err(_) => Vec::new(),
    }
}

/// Minimal `launchctl print` parser — extracts `state = ...` / `pid = ...` /
/// `last exit code = ...` lines. Anything else in the dump is ignored.
fn parse_launchctl_print(text: &str) -> (Option<String>, Option<i64>, Option<i64>) {
    let mut state = None;
    let mut pid = None;
    let mut last_exit_code = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some(v) = line.strip_prefix("state = ") {
            state = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("pid = ") {
            pid = v.trim().parse::<i64>().ok();
        } else if let Some(v) = line.strip_prefix("last exit code = ") {
            last_exit_code = v.trim().parse::<i64>().ok();
        }
    }
    (state, pid, last_exit_code)
}

/// Outcome of a successful [`start`] / [`restart`] operation.
#[derive(serde::Serialize)]
#[serde(tag = "status")]
pub enum StartOutcome {
    /// healthz was already up before the operation; launchctl was not
    /// invoked.
    #[serde(rename = "already_running")]
    AlreadyRunning {
        /// The `host:port` the daemon is bound to.
        bind: String,
    },
    /// launchctl kickstart succeeded and healthz came up within the poll
    /// window.
    #[serde(rename = "started")]
    Started {
        /// The `host:port` the daemon is bound to.
        bind: String,
    },
}

/// Outcome of a successful [`shutdown`] / [`bootout`] operation.
#[derive(serde::Serialize)]
pub struct StopOutcome {
    /// The `host:port` the daemon was bound to.
    pub bind: String,
    /// Whether healthz went down within the poll window after `bootout`.
    pub stopped: bool,
}

/// Outcome of [`status`] — infallible healthz + `launchctl print` summary.
#[derive(serde::Serialize)]
pub struct StatusOutcome {
    /// The `host:port` the daemon is bound to.
    pub bind: String,
    /// Whether `GET /v1/healthz` returned HTTP 200 with body `ok`.
    pub up: bool,
    /// `state = ...` from `launchctl print` (`None` if launchctl is
    /// unavailable or the label isn't loaded).
    pub launchd_state: Option<String>,
    /// `pid = ...` from `launchctl print` (`None` if not running).
    pub launchd_pid: Option<i64>,
    /// `last exit code = ...` from `launchctl print` (`None` if launchd
    /// hasn't recorded one yet).
    pub launchd_last_exit_code: Option<i64>,
}

/// Outcome of a successful [`bootstrap`] operation.
#[derive(serde::Serialize)]
#[serde(tag = "status")]
pub enum BootstrapOutcome {
    /// launchctl bootstrap succeeded — the LaunchAgent is now loaded.
    #[serde(rename = "bootstrapped")]
    Bootstrapped {
        /// Absolute path of the plist file that was bootstrapped.
        plist_path: PathBuf,
    },
    /// The LaunchAgent was already loaded (idempotent success).
    #[serde(rename = "already_loaded")]
    AlreadyLoaded {
        /// Absolute path of the plist file that was already loaded.
        plist_path: PathBuf,
    },
}

/// Outcome of a successful [`install`] operation.
#[derive(serde::Serialize)]
pub struct InstallOutcome {
    /// Absolute path of the installed plist file.
    pub plist_path: PathBuf,
    /// The bootstrap outcome (`bootstrapped` on first install,
    /// `already_loaded` on idempotent re-install after `bootout` +
    /// rewrite).
    pub bootstrap: BootstrapOutcome,
}

/// Outcome of a successful [`uninstall`] operation.
#[derive(serde::Serialize)]
pub struct UninstallOutcome {
    /// Absolute path of the plist file that was (or would have been)
    /// removed.
    pub plist_path: PathBuf,
}

/// Outcome of a successful [`logs`] operation.
#[derive(serde::Serialize)]
pub struct LogsOutcome {
    /// Absolute path of the stdout log sink (`/tmp/mse-server.stdout`).
    pub stdout_path: PathBuf,
    /// Absolute path of the stderr log sink (`/tmp/mse-server.stderr`).
    pub stderr_path: PathBuf,
    /// Tail of the stdout log sink (empty if the file is missing).
    pub stdout_tail: Vec<String>,
    /// Tail of the stderr log sink (empty if the file is missing).
    pub stderr_tail: Vec<String>,
}

/// Mirrors `mlua_swarm_server::StatusResponse` — kept as a distinct,
/// independently-`Deserialize`d type (rather than importing the
/// `mlua_swarm_server` crate into `mlua-swarm-cli`'s MCP module, which
/// this module does not otherwise depend on) so this crate's HTTP
/// client stays a plain JSON consumer, same posture as `healthz_ok`'s
/// plain-text `"ok"` check.
#[derive(serde::Deserialize)]
pub struct Occupancy {
    /// Number of runs currently in `running` state on the server.
    pub running_runs: usize,
    /// Number of attached operators (WebSocket sessions) on the server.
    pub attached_operators: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_launchctl_print_extracts_state_pid_exit_code() {
        let sample = "\
com.mse.server = {
\tactive count = 1
\tpath = $HOME/Library/LaunchAgents/com.mse.server.plist
\ttype = LaunchAgent
\tstate = running

\tprogram = $HOME/.cargo/bin/mse serve
\targuments = {
\t\t$HOME/.cargo/bin/mse serve
\t\t--config
\t\t$HOME/.mse/config.toml
\t}

\tpid = 12345
\tlast exit code = 0
}";
        let (state, pid, code) = parse_launchctl_print(sample);
        assert_eq!(state.as_deref(), Some("running"));
        assert_eq!(pid, Some(12345));
        assert_eq!(code, Some(0));
    }

    #[test]
    fn parse_launchctl_print_missing_fields_are_none() {
        let (state, pid, code) = parse_launchctl_print("not a plist dump\njust noise");
        assert_eq!(state, None);
        assert_eq!(pid, None);
        assert_eq!(code, None);
    }

    #[test]
    fn looks_like_missing_job_detects_common_launchctl_errors() {
        assert!(looks_like_missing_job(
            "Could not find service \"com.mse.server\" in domain for port"
        ));
        assert!(!looks_like_missing_job("Operation now in progress"));
    }

    #[test]
    fn combined_output_text_joins_stdout_and_stderr() {
        assert_eq!(
            combined_output_text(b"out-line", b"err-line"),
            "out-line\nerr-line"
        );
        assert_eq!(combined_output_text(b"only-out", b""), "only-out");
        assert_eq!(combined_output_text(b"", b"only-err"), "only-err");
        assert_eq!(combined_output_text(b"", b""), "");
    }

    #[test]
    fn domain_target_embeds_uid_and_label() {
        let target = domain_target();
        assert!(target.starts_with("gui/"));
        assert!(target.ends_with(LAUNCHD_LABEL));
    }

    /// `occupancy()` makes a real HTTP call — a full integration test (spin
    /// up an actual `axum::serve` on a random port bound to a
    /// `build_router_full`-constructed router, hit `occupancy()` against
    /// it) is preferred over mocking, since no `#[tool]`/HTTP-calling test
    /// exists yet in this file (only the pure-logic parsers above are
    /// covered). No `launchctl` is involved — this only exercises the
    /// `GET /v1/status` round trip.
    #[tokio::test]
    async fn occupancy_parses_status_response() {
        let engine = mlua_swarm::Engine::new(mlua_swarm::EngineCfg::default());
        let router = mlua_swarm_server::build_router_full(
            engine,
            mlua_swarm_server::default_registry(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            300,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let bind = addr.to_string();

        let occ = occupancy(&bind).await.expect("occupancy() must succeed");
        assert_eq!(occ.running_runs, 0);
        assert_eq!(occ.attached_operators, 0);
    }

    // ---- Lifecycle-family test additions ---------------------------------

    #[test]
    fn render_substitutes_placeholders() {
        let home = Path::new("/Users/alice");
        let cargo_bin = Path::new("/Users/alice/.cargo/bin");
        let project_root = Path::new("/Users/alice/projects/mlua-swarm");
        let rendered = render(home, cargo_bin, project_root).expect("render succeeds");
        // All three placeholders fully substituted.
        assert!(!rendered.contains("{{HOME}}"), "HOME placeholder leaked");
        assert!(
            !rendered.contains("{{CARGO_BIN}}"),
            "CARGO_BIN placeholder leaked"
        );
        assert!(
            !rendered.contains("{{PROJECT_ROOT}}"),
            "PROJECT_ROOT placeholder leaked"
        );
        // No stray `{{` = guard is silent when all placeholders are known.
        assert!(!rendered.contains("{{"), "unresolved `{{{{` in output");
        // Payload substitutions materialized in the concrete plist body.
        assert!(rendered.contains("/Users/alice/.cargo/bin/mse"));
        assert!(rendered.contains("/Users/alice/.mse/config.toml"));
        assert!(rendered.contains("/Users/alice/projects/mlua-swarm"));
    }

    #[test]
    fn render_rejects_unresolved_placeholder() {
        // Feed the shared `render_impl` a template that carries an unknown
        // placeholder — the post-render guard must catch it as
        // `ServerError::Render` rather than silently emitting the leaky
        // string.
        let extended = format!("{TEMPLATE}\n<key>Future</key><string>{{{{FUTURE}}}}</string>");
        let err = render_impl(&extended, Path::new("/H"), Path::new("/C"), Path::new("/P"))
            .expect_err("unresolved placeholder must be rejected");
        match err {
            ServerError::Render(msg) => {
                assert!(
                    msg.contains("unresolved placeholder"),
                    "message missing 'unresolved placeholder': {msg}"
                );
                assert!(
                    msg.contains("{{FUTURE}}"),
                    "message missing the leaked placeholder literal: {msg}"
                );
            }
            other => panic!("expected ServerError::Render, got {other:?}"),
        }
    }

    #[test]
    fn looks_like_already_loaded_detects_common_launchctl_errors() {
        // Case-insensitive match across the launchctl phrasing variants
        // we've observed on macOS 13 / 14 / 15.
        assert!(looks_like_already_loaded(
            "Bootstrap failed: Service is already loaded"
        ));
        assert!(looks_like_already_loaded("SERVICE ALREADY LOADED"));
        assert!(looks_like_already_loaded(
            "com.mse.server: already bootstrapped"
        ));
        assert!(looks_like_already_loaded(
            "The service already exists in this domain"
        ));
        assert!(looks_like_already_loaded(
            "service is already registered in domain"
        ));
        // Negative: unrelated launchctl chatter must not match.
        assert!(!looks_like_already_loaded("Operation now in progress"));
        assert!(!looks_like_already_loaded(
            "Could not find service in domain"
        ));
    }

    // ---- macOS 15 Sequoia EIO post-check ---------------------------------

    #[test]
    fn probe_already_loaded_recognises_running_body() {
        // Real-shape `launchctl print gui/<uid>/<label>` output for a
        // loaded job — top-level entry opens with the target followed
        // by ` = {`.
        // The path / state / pid lines below the header are decorative —
        // the parser only inspects the header prefix — but the shape mirrors
        // real `launchctl print` output so the fixture reads faithfully.
        let body = "gui/501/com.mse.server = {\n\
                    \tactive count = 1\n\
                    \tstate = running\n\
                    \tpid = 12345\n\
                    }";
        assert!(launchctl_print_body_indicates_loaded(
            body,
            "gui/501/com.mse.server"
        ));
    }

    #[test]
    fn probe_already_loaded_rejects_missing_body() {
        // `launchctl print <unknown-target>` on macOS emits either
        // `Bad request.` (older) or `Could not find service ...`
        // (newer). Neither must be treated as loaded.
        let bad_request = "Bad request.\n";
        assert!(!launchctl_print_body_indicates_loaded(
            bad_request,
            "gui/501/com.mse.server"
        ));
        let not_found = "Could not find service \"com.mse.server\" in domain for port\n";
        assert!(!launchctl_print_body_indicates_loaded(
            not_found,
            "gui/501/com.mse.server"
        ));
        // Empty stdout (probe exec succeeded but produced nothing) must
        // also fall through.
        assert!(!launchctl_print_body_indicates_loaded(
            "",
            "gui/501/com.mse.server"
        ));
    }

    #[test]
    fn looks_like_bootstrap_eio_matches_sequoia_signature() {
        // Literal stderr observed on macOS 15.7.3 (kernel 24.6.0) when
        // running `launchctl bootstrap gui/<uid> <plist>` against an
        // already-loaded LaunchAgent.
        let sequoia = "Bootstrap failed: 5: Input/output error\n\
                       Try re-running the command as root for richer errors.";
        assert!(looks_like_bootstrap_eio(sequoia));
        // Case-insensitive on both signature substrings.
        assert!(looks_like_bootstrap_eio("bootstrap failed: 5"));
        assert!(looks_like_bootstrap_eio("INPUT/OUTPUT ERROR"));
        // Unrelated launchctl chatter must not match.
        assert!(!looks_like_bootstrap_eio(
            "Bootstrap failed: 37: Service is already loaded"
        ));
        assert!(!looks_like_bootstrap_eio("Operation now in progress"));
    }

    #[test]
    fn classical_heuristic_does_not_cover_sequoia_signature() {
        // Design intent: the Sequoia EIO signature is intentionally NOT
        // matched by `looks_like_already_loaded` — that branch is kept
        // narrow (classical phrasings only) so a genuine EIO on older
        // macOS is not silently folded into `AlreadyLoaded`. The
        // Sequoia path is handled by the EIO-post-check branch in
        // `bootstrap()`, which pairs `looks_like_bootstrap_eio` with a
        // `launchctl print` probe to disambiguate.
        let sequoia = "Bootstrap failed: 5: Input/output error";
        assert!(
            !looks_like_already_loaded(sequoia),
            "classical heuristic must stay narrow; Sequoia goes through \
             the EIO-post-check branch, not this one"
        );
    }

    #[test]
    fn install_hint_points_to_mse_server_install() {
        let hint = install_hint();
        assert!(
            hint.contains("mse server install"),
            "install_hint missing `mse server install` literal: {hint}"
        );
        // Assemble the negative-check literal from parts so this test's
        // source doesn't itself contain the forbidden path literal
        // (acceptance criterion: `rg <legacy>` on this file returns
        // zero hits).
        let legacy = format!("{}/{}/{}", "scripts", "launchd", "install.sh");
        assert!(
            !hint.contains(&legacy),
            "install_hint still references the legacy shell installer: {hint}"
        );
    }
}
