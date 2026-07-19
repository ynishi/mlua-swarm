//! Thin `launchctl` wrapper for the `mse serve` HTTP daemon lifecycle.
//!
//! Lifecycle ownership is consolidated on launchd (macOS user-level
//! LaunchAgent, `Label = com.mse.server`, installed via
//! `scripts/launchd/install.sh`). This module holds **no** spawn / pid
//! tracking state — it is a pure wrapper around three `launchctl`
//! subcommands + healthz polling:
//!
//! - `start`   → `launchctl kickstart gui/<uid>/com.mse.server`
//! - `shutdown`→ `launchctl bootout gui/<uid>/com.mse.server`
//! - `restart` → `launchctl kickstart -k gui/<uid>/com.mse.server`
//! - `status`  → healthz + `launchctl print gui/<uid>/com.mse.server` summary
//!
//! No fallback spawn path is implemented by design — a second lifecycle
//! owner alongside launchd is exactly the failure mode this module replaces
//! (see the server-lifecycle design). When
//! the launchd job is not installed, `start` / `restart` return an `Err`
//! that includes the `launchctl bootstrap` install instructions.

use std::time::{Duration, Instant};

use tokio::process::Command;

pub const DEFAULT_BIND: &str = "127.0.0.1:7777";
pub const LAUNCHD_LABEL: &str = "com.mse.server";
const POLL_TOTAL: Duration = Duration::from_secs(30);
const POLL_STEP: Duration = Duration::from_millis(500);
const HEALTHZ_TIMEOUT: Duration = Duration::from_millis(500);
const SHUTDOWN_POLL_TOTAL: Duration = Duration::from_secs(10);

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

/// `GET /v1/status` on the running `mse serve` process (issue #35 ST4
/// — lifecycle occupancy guard). `Err` covers both network failure and
/// an older server binary predating this route (404) — callers should
/// treat `Err` as "occupancy unknown", not "occupancy = busy" (see the
/// MCP tool handlers' fail-open-on-Err policy).
pub async fn occupancy(bind: &str) -> Result<Occupancy, String> {
    let url = format!("http://{bind}/v1/status");
    let client = reqwest::Client::builder()
        .timeout(HEALTHZ_TIMEOUT)
        .build()
        .map_err(|e| format!("occupancy: client build failed: {e}"))?;
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("occupancy: request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("occupancy: non-success status {}", resp.status()));
    }
    resp.json::<Occupancy>()
        .await
        .map_err(|e| format!("occupancy: decode failed: {e}"))
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

fn install_hint() -> String {
    format!(
        "launchd job '{label}' not found. Install it first:\n  \
         scripts/launchd/install.sh",
        label = LAUNCHD_LABEL,
    )
}

async fn run_launchctl(args: &[&str]) -> Result<std::process::Output, String> {
    Command::new("launchctl")
        .args(args)
        .output()
        .await
        .map_err(|e| format!("launchctl exec failed: {e}"))
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

/// `launchctl kickstart gui/<uid>/com.mse.server`. If healthz is already up,
/// short-circuits to `AlreadyRunning` without touching launchd. On job-missing
/// errors, the returned `Err` includes the install instructions.
pub async fn start(bind: &str) -> Result<StartOutcome, String> {
    if healthz_ok(bind).await {
        return Ok(StartOutcome::AlreadyRunning { bind: bind.into() });
    }
    let target = domain_target();
    let out = run_launchctl(&["kickstart", &target]).await?;
    if !out.status.success() {
        let text = combined_output_text(&out.stdout, &out.stderr);
        return Err(if looks_like_missing_job(&text) {
            format!("{text}\n\n{}", install_hint())
        } else {
            format!("launchctl kickstart failed: {text}")
        });
    }
    if poll_healthz_until_up(bind, POLL_TOTAL, POLL_STEP).await {
        Ok(StartOutcome::Started { bind: bind.into() })
    } else {
        Err(format!(
            "launchctl kickstart succeeded but healthz did not respond within {POLL_TOTAL:?}"
        ))
    }
}

/// `launchctl bootout gui/<uid>/com.mse.server` (full teardown, KeepAlive included).
/// A "job already not loaded" error from launchctl is treated as an idempotent
/// success (falls through to the healthz-down confirmation).
pub async fn shutdown(bind: &str) -> Result<StopOutcome, String> {
    let target = domain_target();
    let out = run_launchctl(&["bootout", &target]).await?;
    if !out.status.success() {
        let text = combined_output_text(&out.stdout, &out.stderr);
        if !looks_like_missing_job(&text) {
            return Err(format!("launchctl bootout failed: {text}"));
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

/// `launchctl kickstart -k gui/<uid>/com.mse.server` (kill + restart, used to
/// pick up a `~/.mse/config.toml` edit).
pub async fn restart(bind: &str) -> Result<StartOutcome, String> {
    let target = domain_target();
    let out = run_launchctl(&["kickstart", "-k", &target]).await?;
    if !out.status.success() {
        let text = combined_output_text(&out.stdout, &out.stderr);
        return Err(if looks_like_missing_job(&text) {
            format!("{text}\n\n{}", install_hint())
        } else {
            format!("launchctl kickstart -k failed: {text}")
        });
    }
    if poll_healthz_until_up(bind, POLL_TOTAL, POLL_STEP).await {
        Ok(StartOutcome::Started { bind: bind.into() })
    } else {
        Err(format!(
            "launchctl kickstart -k succeeded but healthz did not respond within {POLL_TOTAL:?}"
        ))
    }
}

/// healthz + a `launchctl print` summary (state / pid / last exit code). Never
/// raw-dumps the `launchctl print` output.
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

#[derive(serde::Serialize)]
#[serde(tag = "status")]
pub enum StartOutcome {
    #[serde(rename = "already_running")]
    AlreadyRunning { bind: String },
    #[serde(rename = "started")]
    Started { bind: String },
}

#[derive(serde::Serialize)]
pub struct StopOutcome {
    pub bind: String,
    /// Whether healthz went down within the poll window after `bootout`.
    pub stopped: bool,
}

#[derive(serde::Serialize)]
pub struct StatusOutcome {
    pub bind: String,
    pub up: bool,
    pub launchd_state: Option<String>,
    pub launchd_pid: Option<i64>,
    pub launchd_last_exit_code: Option<i64>,
}

/// Mirrors `mlua_swarm_server::StatusResponse` — kept as a distinct,
/// independently-`Deserialize`d type (rather than importing the
/// `mlua_swarm_server` crate into `mlua-swarm-cli`'s MCP module, which
/// this module does not otherwise depend on) so this crate's HTTP
/// client stays a plain JSON consumer, same posture as `healthz_ok`'s
/// plain-text `"ok"` check.
#[derive(serde::Deserialize)]
pub struct Occupancy {
    pub running_runs: usize,
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
}
