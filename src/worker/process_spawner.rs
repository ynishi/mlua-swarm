//! `ProcessSpawner` — a general-purpose `SpawnerAdapter` implementation
//! that spawns an arbitrary binary (or a one-line shell command) and
//! runs it as a worker. The thin path for wrapping an agent-block CLI,
//! an LLM CLI, a random binary, or a shell script as a worker.
//!
//! Direct library integration with the `agent-block-core` SDK lives on
//! a separate axis, in
//! [`crate::worker::agent_block::AgentBlockInProcessSpawnerFactory`]: the SDK
//! is embedded in-process, and `bus.emit("worker_result", ...)` is
//! captured host-side. This spawner's selling point is "call anything
//! over a shell"; it is not agent-block-specific, and the two paths
//! have fully separated responsibilities.
//!
//! Naming convention: `ProcessSpawner` starts a shell process, and
//! `AgentBlockInProcessSpawnerFactory` provides direct integration
//! with the agent-block SDK. Older commits still reference an
//! "AgentBlockSpawner" — that was renamed to `ProcessSpawner` in the current design
//! (commit 8d1058f). See mini-app issue `96821965` for the full
//! rationale.
//!
//! # Modes (two flavours)
//!
//! **plain mode (default):**
//! 1. On `spawn`, launch a child process with
//!    `Command::new(self.program)` + `args`.
//! 2. Write the directive to the child's stdin (used as the prompt).
//! 3. Buffer the child's stdout in full.
//! 4. Try to parse stdout as JSON; on failure wrap it as
//!    `{"raw": "<text>"}`.
//! 5. `ok = true` on exit code 0, otherwise `ok = false`.
//! 6. Emit the `WorkerResult` in parallel via
//!    `engine.submit_output(Final)` (design intent).
//!
//! **streaming mode (`.stream_mode(StreamMode::...)`):**
//! 1-2. Same as plain mode.
//! 3. Read the child's stdout **line by line** through a `BufReader`
//!    for NDJSON — or via a different protocol later.
//! 4. Parse each chunk as an `OutputEvent`; skip failures.
//! 5. `engine.submit_output` each successfully-parsed event
//!    **incrementally**.
//! 6. When `OutputEvent::Final` arrives, fold its `{content, ok}`
//!    into the `WorkerResult`.
//! 7. If EOF is hit without a `Final`, mark the outcome `ok = false`
//!    (Blocked).
//!
//! Only `StreamMode::NdjsonLines` ships today; SSE, length-prefixed,
//! and friends are carries for future turns.
//!
//! Token metadata is also handed to the child as environment variables
//! so a worker can re-pull if it needs to. `sig_hex` is deliberately
//! not exported, to keep exposure minimal.

use crate::core::agent_context::AgentContextView;
use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::types::{CapToken, StepId, WorkerId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter, WorkerError, WorkerResult};
use crate::worker::output::{ContentRef, OutputEvent};
use crate::worker::{Worker, WorkerJoinHandler};
use async_trait::async_trait;
use mlua_swarm_schema::SubprocessOutput;
use serde_json::Value;
use std::collections::BTreeMap;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// GH #83 — the closed, logic-free placeholder vocabulary a
/// [`SubprocessDef`](mlua_swarm_schema::SubprocessDef) template may
/// reference. Rendering is pure string substitution over exactly this
/// set; the compile-time validator
/// (`SubprocessProcessSpawnerFactory::build`) rejects any other
/// `{...}` token with `CompileError::InvalidSpec`.
pub const EMBED_PLACEHOLDERS: [&str; 8] = [
    "system",
    "system_file",
    "prompt",
    "model",
    "tools_csv",
    "work_dir",
    "task_id",
    "attempt",
];

/// GH #83 — the compile-time-baked EmbedAgent invocation: a resolved
/// [`SubprocessDef`](mlua_swarm_schema::SubprocessDef) template merged
/// with the agent profile (system prompt / model / tools) and the
/// `Runner::Subprocess` overrides. When [`ProcessSpawner::embed`] carries
/// one of these, `spawn` takes the EmbedAgent path: materialize the
/// worker payload via `Engine::fetch_worker_payload` (never the
/// `fetch_prompt`-only single-directive path), render the closed
/// placeholder set into argv/stdin/env/cwd, and normalize stdout per
/// [`SubprocessOutput`]. When `None`, `spawn` keeps the historical
/// spec-based behavior byte-for-byte.
#[derive(Debug, Clone, Default)]
pub struct EmbedTemplate {
    /// Full argv (element 0 = program), unrendered — elements may carry
    /// placeholder tokens.
    pub argv: Vec<String>,
    /// stdin template. `Some` = render + pipe to the child's stdin;
    /// `None` = no stdin write (immediate EOF).
    pub stdin: Option<String>,
    /// Extra env vars (appended to the `MSE_*` token exports); values may
    /// carry placeholder tokens.
    pub env: BTreeMap<String, String>,
    /// Working-directory template (`Runner::Subprocess` `overrides.cwd`
    /// already merged in at compile time — it wins over the template's
    /// own `cwd`). `None` = fall back to the spawn-time `{work_dir}`
    /// source; if that is also absent the child inherits the server CWD
    /// (historical behavior).
    pub cwd: Option<String>,
    /// stdout normalization declaration. `None` = historical JSON-or-raw.
    pub output: Option<SubprocessOutput>,
    /// Compile-time-baked `profile.system_prompt` (pre-render template —
    /// minijinja slot expansion happens at spawn, mirroring
    /// `OperatorSpawner`).
    pub system_prompt: Option<String>,
    /// `{model}` placeholder value (overrides.model > profile.model).
    pub model: Option<String>,
    /// `{tools_csv}` placeholder value (overrides.tools > profile.tools,
    /// CSV-joined).
    pub tools_csv: String,
}

/// Wire protocol used to receive `OutputEvent`s from the child's
/// stdout. `None` means plain mode — the default — which buffers stdout
/// in full and folds it into a single `Final`.
#[derive(Debug, Clone)]
pub enum StreamMode {
    /// One line per `OutputEvent` JSON (newline-delimited JSON).
    NdjsonLines,
    /// The `text/event-stream` form. Each event is a `data: <json>`
    /// line terminated by a blank line. `event:` / `id:` / `retry:`
    /// lines are ignored (MVP: only `data` lines are picked up).
    /// Multiple `data` lines are concatenated into a single JSON
    /// payload.
    SseEvents,
    /// Binary form: repeated `[u32 BE length][N bytes JSON payload]`.
    /// Handy for LLM tools and high-frequency streams that want to
    /// avoid text-framing overhead.
    LengthPrefixed,
}

/// A `SpawnerAdapter` that runs a worker as an external OS process
/// (a binary or a `sh -c` one-liner). Configured with the builder
/// methods below, then registered like any other spawner.
#[derive(Debug)]
pub struct ProcessSpawner {
    /// Binary (or `sh`, when built via [`ProcessSpawner::run`]) to
    /// execute.
    pub program: String,
    /// Extra arguments passed to `program`, in order.
    pub args: Vec<String>,
    /// Whether to pipe the directive into the child's stdin — most LLM
    /// CLIs read prompts that way (`--prompt -` and friends). When
    /// `false`, the directive is appended to `args` instead.
    pub use_stdin: bool,
    /// `Some(mode)` — streaming mode. `None` — plain mode (the default).
    pub stream_mode: Option<StreamMode>,
    /// GH #83 — `Some` switches `spawn` to the EmbedAgent template path
    /// (see [`EmbedTemplate`]); `None` (the default) keeps the historical
    /// spec-based behavior byte-for-byte.
    pub embed: Option<EmbedTemplate>,
}

impl ProcessSpawner {
    /// Builder entry point: spawn `program` with no args, stdin piping
    /// on, and plain mode.
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            use_stdin: true,
            stream_mode: None,
            embed: None,
        }
    }

    /// Appends a single argument.
    pub fn arg(mut self, a: impl Into<String>) -> Self {
        self.args.push(a.into());
        self
    }

    /// Appends multiple arguments at once.
    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args.extend(args.into_iter().map(|a| a.into()));
        self
    }

    /// Sets whether the directive/prompt is piped to the child's stdin
    /// (`true`, the default) or appended as a trailing arg (`false`).
    pub fn use_stdin(mut self, v: bool) -> Self {
        self.use_stdin = v;
        self
    }

    /// Set the streaming mode. Default: `None` (plain mode).
    pub fn stream_mode(mut self, mode: StreamMode) -> Self {
        self.stream_mode = Some(mode);
        self
    }

    /// Reset to plain mode explicitly — sets `stream_mode` to `None`.
    pub fn plain(mut self) -> Self {
        self.stream_mode = None;
        self
    }

    /// Compatibility helper: `ndjson(true)` is equivalent to
    /// `.stream_mode(StreamMode::NdjsonLines)`, and `ndjson(false)` to
    /// `.plain()`. A deprecation candidate, kept around for now.
    pub fn ndjson(mut self, v: bool) -> Self {
        self.stream_mode = if v {
            Some(StreamMode::NdjsonLines)
        } else {
            None
        };
        self
    }

    /// Convenience builder that runs a one-liner via `sh -c '<cmd>'`.
    pub fn run(cmd: impl Into<String>) -> Self {
        Self {
            program: "sh".into(),
            args: vec!["-c".into(), cmd.into()],
            use_stdin: true,
            stream_mode: None,
            embed: None,
        }
    }

    /// Builder that spawns an arbitrary binary directly, without going
    /// through a shell.
    pub fn cmd(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            use_stdin: true,
            stream_mode: None,
            embed: None,
        }
    }

    /// GH #83: switch this spawner to the EmbedAgent template path.
    pub fn embed(mut self, template: EmbedTemplate) -> Self {
        self.embed = Some(template);
        self
    }
}

#[async_trait]
impl SpawnerAdapter for ProcessSpawner {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        // GH #83: the EmbedAgent template path materializes the full
        // worker payload (system + prompt + context) and renders the
        // declared template — never the fetch_prompt-only path below.
        if let Some(embed) = &self.embed {
            return self
                .spawn_embed(embed, engine, ctx, task_id, attempt, token)
                .await;
        }
        // design intent: `prompt` is obtained through
        // `engine.fetch_prompt`, replacing the removed `directive`
        // argument. `ProcessSpawner` snapshots it here and pushes it
        // either into the child's stdin or the tail of `args`. If a
        // child process wants to pull `fetch_prompt` itself, it can
        // rebuild the token from the `MSE_TOKEN_*` env vars and call
        // the engine — that lives in a separate spawner implementation.
        let directive = engine
            .fetch_prompt(&token, &task_id)
            .await
            .map_err(|e| SpawnError::Internal(format!("fetch_prompt: {e}")))?;
        // Subprocess spawner consumes `directive` as `String` (command arg /
        // stdin). Issue #18 boundary render — Value flows end-to-end upstream.
        let directive = crate::core::engine::render_directive_to_string(&directive);

        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args)
            .env("MSE_TOKEN_AGENT_ID", &token.agent_id)
            .env("MSE_TOKEN_NONCE", &token.nonce)
            .env("MSE_TASK_ID", task_id.as_str())
            .env("MSE_ATTEMPT", attempt.to_string())
            .env("MSE_CTX_AGENT", &ctx.agent)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if !self.use_stdin {
            cmd.arg(&directive);
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| SpawnError::Internal(format!("spawn failed: {e}")))?;

        if self.use_stdin {
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(directive.as_bytes())
                    .await
                    .map_err(|e| SpawnError::Internal(format!("stdin write: {e}")))?;
                drop(stdin); // EOF for child
            }
        }

        let cancel = CancellationToken::new();
        let cancel_inner = cancel.clone();
        let worker_id = WorkerId::new();
        // issue #11: surface the minted WorkerId in the trace log.
        tracing::debug!(worker_id = %worker_id, step_id = %task_id, "worker spawned (subprocess)");
        let (tx, rx) = oneshot::channel();
        // design intent: hand `engine` / `token` to the spawn task so it can emit
        // OutputEvent via submit_output (side-by-side with the WorkerResult
        // oneshot path).
        let engine_for_emit = engine.clone();
        let token_for_emit = token.clone();
        let task_id_for_emit = task_id.clone();
        let stream_mode = self.stream_mode.clone();

        tokio::spawn(async move {
            let result: Result<WorkerResult, WorkerError> = if let Some(mode) = stream_mode {
                // ── streaming mode: read stdout as a chunk stream per protocol,
                // pushing each chunk to submit_output as an OutputEvent. When we
                // see a Final, fold {value, ok} into WorkerResult.
                run_streaming_mode(
                    mode,
                    child,
                    &engine_for_emit,
                    &token_for_emit,
                    &task_id_for_emit,
                    attempt,
                    cancel_inner,
                )
                .await
            } else {
                // ── plain mode (default): buffer all stdout, JSON parse
                // once, fold a single Final, then emit engine.submit_output(Final) in parallel.
                let result = tokio::select! {
                    output = child.wait_with_output() => {
                        match output {
                            Ok(out) => {
                                let stdout = String::from_utf8_lossy(&out.stdout).to_string();
                                let value: Value = serde_json::from_str(stdout.trim())
                                    .unwrap_or_else(|_| serde_json::json!({
                                        "raw": stdout.trim_end(),
                                        "stderr": String::from_utf8_lossy(&out.stderr).to_string(),
                                    }));
                                Ok(WorkerResult {
                                    value,
                                    ok: out.status.success(),
                                    stats: Some(subprocess_base_stats(&out.status, None)),
                                })
                            }
                            Err(e) => Err(WorkerError::Failed(format!("wait_with_output: {e}"))),
                        }
                    }
                    _ = cancel_inner.cancelled() => Err(WorkerError::Cancelled),
                };
                if let Ok(wr) = &result {
                    if let Some(stats) = wr.stats.clone() {
                        engine_for_emit
                            .record_worker_stats(&task_id_for_emit, attempt, stats)
                            .await;
                    }
                    let ev = OutputEvent::Final {
                        content: ContentRef::Inline {
                            value: wr.value.clone(),
                        },
                        ok: wr.ok,
                    };
                    let _ = engine_for_emit
                        .submit_output(&token_for_emit, &task_id_for_emit, attempt, ev)
                        .await;
                }
                result
            };
            // signal-only: the value travels through output_tail.
            let signal: Result<(), WorkerError> = result.map(|_| ());
            let _ = tx.send(signal);
        });

        Ok(Box::new(ProcessWorker {
            handler: WorkerJoinHandler {
                worker_id,
                cancel,
                completion: rx,
            },
        }))
    }
}

/// GH #83 — spawn-time values behind the closed placeholder set. `None`
/// entries are "no source for this spawn": referencing their token in a
/// template is a fail-loud `SpawnError`, never a silent empty string
/// (`{system}` alone renders empty when no system prompt exists, because
/// an agent without a profile is a legal EmbedAgent).
struct EmbedVars {
    system: String,
    system_file: Option<String>,
    prompt: String,
    model: Option<String>,
    tools_csv: String,
    work_dir: Option<String>,
    task_id: String,
    attempt: String,
}

impl EmbedVars {
    /// Resolves one closed-set token to its spawn-time value.
    /// `Ok(None)` = not a placeholder (literal brace text — left alone);
    /// `Err` = closed-set token referenced but no value source exists for
    /// this spawn (fail-loud with the actionable cause).
    fn lookup(&self, token: &str) -> Result<Option<&str>, SpawnError> {
        let (value, cause): (Option<&str>, &str) = match token {
            "system" => (Some(self.system.as_str()), ""),
            "system_file" => (
                self.system_file.as_deref(),
                "no system prompt was baked for this attempt (agent has no profile.system_prompt?)",
            ),
            "prompt" => (Some(self.prompt.as_str()), ""),
            "model" => (
                self.model.as_deref(),
                "no model declared (set profile.model or Runner::Subprocess overrides.model)",
            ),
            "tools_csv" => (Some(self.tools_csv.as_str()), ""),
            "work_dir" => (
                self.work_dir.as_deref(),
                "no work_dir/project_root in the agent context view and no overrides.cwd",
            ),
            "task_id" => (Some(self.task_id.as_str()), ""),
            "attempt" => (Some(self.attempt.as_str()), ""),
            _ => return Ok(None),
        };
        match value {
            Some(v) => Ok(Some(v)),
            None => Err(SpawnError::Internal(format!(
                "placeholder {{{token}}}: {cause}"
            ))),
        }
    }

    /// Pure closed-set substitution — no conditionals, no loops, no
    /// expression evaluation. Single left-to-right pass over the
    /// TEMPLATE only: substituted values are copied to the output and
    /// never re-scanned, so runtime data (e.g. a prompt containing a
    /// literal `{model}`) can never trigger a second-order substitution
    /// or a spurious missing-source failure. Unknown `{...}` tokens in
    /// the template were already rejected at compile time; non-token
    /// brace text (JSON literals etc.) is copied through verbatim.
    fn render(&self, tmpl: &str) -> Result<String, SpawnError> {
        let mut out = String::with_capacity(tmpl.len());
        let mut rest = tmpl;
        while let Some(start) = rest.find('{') {
            out.push_str(&rest[..start]);
            let after = &rest[start + 1..];
            let Some(end) = after.find('}') else {
                // Unmatched '{' — literal tail.
                out.push_str(&rest[start..]);
                return Ok(out);
            };
            let token = &after[..end];
            match self.lookup(token)? {
                Some(value) => {
                    out.push_str(value);
                    rest = &after[end + 1..];
                }
                None => {
                    // Not a placeholder — emit the '{' and keep scanning
                    // right after it, so a placeholder nested inside
                    // literal braces (e.g. a JSON-wrapped stdin like
                    // `{"task": "{prompt}"}`) is still found.
                    out.push('{');
                    rest = after;
                }
            }
        }
        out.push_str(rest);
        Ok(out)
    }
}

/// GH #83 — does any template string of `embed` reference `token`?
fn embed_references(embed: &EmbedTemplate, token: &str) -> bool {
    embed.argv.iter().any(|a| a.contains(token))
        || embed.stdin.as_deref().is_some_and(|s| s.contains(token))
        || embed.env.values().any(|v| v.contains(token))
        || embed.cwd.as_deref().is_some_and(|c| c.contains(token))
}

/// Baseline per-attempt stats every subprocess result carries: the
/// worker kind, the template-declared model (when any), and the child's
/// exit code as adapter data. Declared-pointer enrichment
/// ([`enrich_declared_stats`]) layers usage/model/num_turns on top when
/// the stdout parses and the template opted in.
fn subprocess_base_stats(
    status: &std::process::ExitStatus,
    model: Option<&str>,
) -> crate::store::trace::WorkerStats {
    crate::store::trace::WorkerStats {
        worker_kind: Some("subprocess".to_string()),
        model: model.map(str::to_string),
        usage: None,
        num_turns: None,
        adapter_data: Some(serde_json::json!({ "exit_code": status.code() })),
    }
}

/// GH #83 stats declaration — apply `SubprocessOutput.stats` JSON
/// Pointers to the parsed stdout, enriching `stats` in place. Pointer
/// misses are silent (stats are observational; a CLI that omits usage
/// on some runs must not fail the step).
fn enrich_declared_stats(
    stats: &mut crate::store::trace::WorkerStats,
    parsed: &Value,
    decl: &SubprocessOutput,
) {
    let Some(sd) = &decl.stats else { return };
    if let Some(ptr) = sd.model_ptr.as_deref() {
        if let Some(m) = parsed.pointer(ptr).and_then(|v| v.as_str()) {
            stats.model = Some(m.to_string());
        }
    }
    if let Some(ptr) = sd.num_turns_ptr.as_deref() {
        if let Some(n) = parsed.pointer(ptr).and_then(|v| v.as_u64()) {
            stats.num_turns = Some(n as u32);
        }
    }
    if let Some(ptr) = sd.usage_ptr.as_deref() {
        if let Some(u) = parsed.pointer(ptr) {
            let input = u
                .get("input_tokens")
                .or_else(|| u.get("prompt_tokens"))
                .and_then(|v| v.as_u64());
            let output = u
                .get("output_tokens")
                .or_else(|| u.get("completion_tokens"))
                .and_then(|v| v.as_u64());
            let total = u.get("total_tokens").and_then(|v| v.as_u64());
            // Partial reports count: a CLI that prints only a total (or
            // only the splits) still lands a usage record — see
            // `TokenUsage::from_parts` for the normalization rule.
            if let Some(usage) = crate::store::trace::TokenUsage::from_parts(input, output, total) {
                stats.usage = Some(usage);
                // Keep the raw usage object too — cache-token detail and
                // provider-specific fields ride as adapter data.
                if let Some(Value::Object(ad)) = stats.adapter_data.as_mut() {
                    ad.insert("usage_raw".to_string(), u.clone());
                }
            }
        }
    }
}

/// GH #83 — plain-mode stdout normalization under a
/// [`SubprocessOutput`] declaration. `decl = None` reproduces the
/// historical JSON-or-raw wrap byte-for-byte (same expression as the
/// spec-based path). `model` is the template-declared `{model}` value,
/// recorded into the stats sidecar (a declared `stats.model_ptr`
/// overrides it with the model the CLI reports it actually used).
fn normalize_plain_output(
    out: &std::process::Output,
    decl: Option<&SubprocessOutput>,
    model: Option<&str>,
) -> WorkerResult {
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = || String::from_utf8_lossy(&out.stderr).to_string();
    let exit_ok = out.status.success();
    let mut stats = subprocess_base_stats(&out.status, model);

    let Some(decl) = decl else {
        let value: Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|_| {
            serde_json::json!({
                "raw": stdout.trim_end(),
                "stderr": stderr(),
            })
        });
        return WorkerResult {
            value,
            ok: exit_ok,
            stats: Some(stats),
        };
    };

    let parsed: Result<Value, _> = serde_json::from_str(stdout.trim());
    let parsed = match parsed {
        Ok(v) => v,
        Err(e) => {
            if decl.format.as_deref() == Some("json") {
                // Declared-JSON stdout that does not parse is a failed
                // step, regardless of the exit code.
                return WorkerResult {
                    value: serde_json::json!({
                        "raw": stdout.trim_end(),
                        "stderr": stderr(),
                        "parse_error": e.to_string(),
                    }),
                    ok: false,
                    stats: Some(stats),
                };
            }
            // Lenient (format undeclared): historical raw wrap; pointer
            // extraction cannot apply to a non-JSON stdout.
            return WorkerResult {
                value: serde_json::json!({
                    "raw": stdout.trim_end(),
                    "stderr": stderr(),
                }),
                ok: exit_ok,
                stats: Some(stats),
            };
        }
    };

    enrich_declared_stats(&mut stats, &parsed, decl);

    let value = match decl.result_ptr.as_deref() {
        Some(ptr) => match parsed.pointer(ptr) {
            Some(v) => v.clone(),
            None => {
                return WorkerResult {
                    value: serde_json::json!({
                        "error": format!("result_ptr '{ptr}' not found in stdout JSON"),
                        "raw": parsed,
                        "stderr": stderr(),
                    }),
                    ok: false,
                    stats: Some(stats),
                }
            }
        },
        None => parsed.clone(),
    };

    let ok = match decl.ok_from.as_deref() {
        None | Some("exit_code") => exit_ok,
        Some(ptr) => parsed.pointer(ptr) == Some(&Value::Bool(true)),
    };

    WorkerResult {
        value,
        ok,
        stats: Some(stats),
    }
}

impl ProcessSpawner {
    /// GH #83 — the EmbedAgent spawn path: materialize the same
    /// `WorkerPayload` the Operator/HTTP paths use (bake → fetch, never
    /// the `fetch_prompt`-only single-directive path), render the closed
    /// placeholder set into the declared template, exec, and normalize
    /// stdout per the template's `output` declaration.
    async fn spawn_embed(
        &self,
        embed: &EmbedTemplate,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        // 1. Render + bake the system prompt (same minijinja slot
        //    expansion as OperatorSpawner::spawn — bake BEFORE fetch so
        //    fetch_worker_payload reads it back from `s.systems`).
        let prompt_value = engine
            .fetch_prompt(&token, &task_id)
            .await
            .map_err(|e| SpawnError::Internal(format!("fetch_prompt: {e}")))?;
        let system = match embed.system_prompt.as_deref() {
            Some(tmpl) => {
                let slots = crate::operator::render::slots_from_prompt(&prompt_value);
                let rendered = crate::operator::render::render_system(tmpl, &slots)
                    .map_err(|e| SpawnError::Internal(format!("render system_prompt: {e}")))?;
                Some(rendered)
            }
            None => None,
        };
        engine
            .bake_worker_system_prompt(&task_id, attempt, system.clone())
            .await
            .map_err(|e| SpawnError::Internal(format!("bake system_prompt: {e}")))?;

        // 2. Materialize the canonical worker payload. `{prompt}` comes
        //    from here; `{system}` uses the locally rendered value so the
        //    GH #31 system_ref threshold (which may clear
        //    `payload.system` in favor of a by-reference pointer) cannot
        //    silently empty the placeholder.
        let payload = engine
            .fetch_worker_payload(&token, &task_id)
            .await
            .map_err(|e| SpawnError::Internal(format!("fetch_worker_payload: {e}")))?;

        // 3. `{system_file}` — unconditional materialization, only when
        //    the template actually references it.
        let system_file = if embed_references(embed, "{system_file}") {
            match engine
                .materialize_system_file(&task_id, attempt)
                .await
                .map_err(|e| SpawnError::Internal(format!("materialize system_file: {e}")))?
            {
                Some(path) => Some(path.display().to_string()),
                None => {
                    return Err(SpawnError::Internal(
                        "placeholder {system_file}: no system prompt was baked for this \
                         attempt (agent has no profile.system_prompt?)"
                            .into(),
                    ))
                }
            }
        } else {
            None
        };

        // 4. `{work_dir}` source — the GH #20 context view (work_dir,
        //    falling back to project_root).
        let view = AgentContextView::materialized_or_from_ctx(ctx);
        let work_dir = view.work_dir.clone().or_else(|| view.project_root.clone());

        let vars = EmbedVars {
            system: system.unwrap_or_default(),
            system_file,
            prompt: payload.prompt.clone(),
            model: embed.model.clone(),
            tools_csv: embed.tools_csv.clone(),
            work_dir,
            task_id: task_id.to_string(),
            attempt: attempt.to_string(),
        };

        // 5. Render argv / env / cwd / stdin.
        if embed.argv.is_empty() {
            return Err(SpawnError::Internal(
                "embed template: argv must not be empty".into(),
            ));
        }
        let mut rendered_argv = Vec::with_capacity(embed.argv.len());
        for a in &embed.argv {
            rendered_argv.push(vars.render(a)?);
        }
        let mut rendered_env = Vec::with_capacity(embed.env.len());
        for (k, v) in &embed.env {
            rendered_env.push((k.clone(), vars.render(v)?));
        }
        let rendered_cwd = match embed.cwd.as_deref() {
            Some(c) => Some(vars.render(c)?),
            None => None,
        };
        let rendered_stdin = match embed.stdin.as_deref() {
            Some(s) => Some(vars.render(s)?),
            None => None,
        };

        let mut cmd = Command::new(&rendered_argv[0]);
        cmd.args(&rendered_argv[1..])
            .env("MSE_TOKEN_AGENT_ID", &token.agent_id)
            .env("MSE_TOKEN_NONCE", &token.nonce)
            .env("MSE_TASK_ID", task_id.as_str())
            .env("MSE_ATTEMPT", attempt.to_string())
            .env("MSE_CTX_AGENT", &ctx.agent)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (k, v) in &rendered_env {
            cmd.env(k, v);
        }
        if let Some(cwd) = &rendered_cwd {
            cmd.current_dir(cwd);
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| SpawnError::Internal(format!("spawn failed: {e}")))?;

        if let Some(stdin_body) = rendered_stdin {
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(stdin_body.as_bytes())
                    .await
                    .map_err(|e| SpawnError::Internal(format!("stdin write: {e}")))?;
                drop(stdin); // EOF for child
            }
        } else {
            // No stdin declared: close the pipe so the child sees EOF.
            drop(child.stdin.take());
        }

        let cancel = CancellationToken::new();
        let cancel_inner = cancel.clone();
        let worker_id = WorkerId::new();
        tracing::debug!(worker_id = %worker_id, step_id = %task_id, "worker spawned (subprocess embed)");
        let (tx, rx) = oneshot::channel();
        let engine_for_emit = engine.clone();
        let token_for_emit = token.clone();
        let task_id_for_emit = task_id.clone();
        let stream_mode = self.stream_mode.clone();
        let output_decl = embed.output.clone();
        let model_for_stats = embed.model.clone();

        tokio::spawn(async move {
            let result: Result<WorkerResult, WorkerError> = if let Some(mode) = stream_mode {
                // Streaming keeps the existing event protocol untouched
                // (output normalization is a plain-mode declaration).
                run_streaming_mode(
                    mode,
                    child,
                    &engine_for_emit,
                    &token_for_emit,
                    &task_id_for_emit,
                    attempt,
                    cancel_inner,
                )
                .await
            } else {
                let result = tokio::select! {
                    output = child.wait_with_output() => {
                        match output {
                            Ok(out) => Ok(normalize_plain_output(
                                &out,
                                output_decl.as_ref(),
                                model_for_stats.as_deref(),
                            )),
                            Err(e) => Err(WorkerError::Failed(format!("wait_with_output: {e}"))),
                        }
                    }
                    _ = cancel_inner.cancelled() => Err(WorkerError::Cancelled),
                };
                if let Ok(wr) = &result {
                    if let Some(stats) = wr.stats.clone() {
                        engine_for_emit
                            .record_worker_stats(&task_id_for_emit, attempt, stats)
                            .await;
                    }
                    let ev = OutputEvent::Final {
                        content: ContentRef::Inline {
                            value: wr.value.clone(),
                        },
                        ok: wr.ok,
                    };
                    let _ = engine_for_emit
                        .submit_output(&token_for_emit, &task_id_for_emit, attempt, ev)
                        .await;
                }
                result
            };
            let signal: Result<(), WorkerError> = result.map(|_| ());
            let _ = tx.send(signal);
        });

        Ok(Box::new(ProcessWorker {
            handler: WorkerJoinHandler {
                worker_id,
                cancel,
                completion: rx,
            },
        }))
    }
}

/// Concrete Worker type for the Subprocess kind — the handle to a
/// child OS process's `wait_with_output` / stream wait. Embeds a
/// `WorkerJoinHandler` to carry the async signal.
pub struct ProcessWorker {
    /// The completion-signal handle for this child process's spawned
    /// wait task.
    pub handler: WorkerJoinHandler,
}

#[async_trait]
impl Worker for ProcessWorker {
    fn id(&self) -> &WorkerId {
        &self.handler.worker_id
    }
    fn cancel_token(&self) -> CancellationToken {
        self.handler.cancel.clone()
    }
    async fn join(self: Box<Self>) -> Result<(), WorkerError> {
        self.handler.await_completion().await
    }
}

/// Streaming-mode dispatcher. Picks one of the three reader functions
/// per protocol. Owns the shared boilerplate — final tracking, child
/// wait, synthetic-final emit, `WorkerResult` construction — so each
/// reader only has to worry about parsing its protocol and calling
/// `submit_output` per chunk.
async fn run_streaming_mode(
    mode: StreamMode,
    mut child: tokio::process::Child,
    engine: &Engine,
    token: &CapToken,
    task_id: &StepId,
    attempt: u32,
    cancel: CancellationToken,
) -> Result<WorkerResult, WorkerError> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| WorkerError::Failed("streaming: stdout pipe missing".into()))?;

    let last_final = match mode {
        StreamMode::NdjsonLines => {
            read_ndjson(stdout, engine, token, task_id, attempt, cancel.clone()).await?
        }
        StreamMode::SseEvents => {
            read_sse(stdout, engine, token, task_id, attempt, cancel.clone()).await?
        }
        StreamMode::LengthPrefixed => {
            read_length_prefixed(stdout, engine, token, task_id, attempt, cancel.clone()).await?
        }
    };

    let status = child
        .wait()
        .await
        .map_err(|e| WorkerError::Failed(format!("streaming wait: {e}")))?;

    match last_final {
        Some((value, ok)) => Ok(WorkerResult {
            value,
            ok: ok && status.success(),
            // Streaming keeps its event protocol untouched; only the
            // baseline exit-code stats ride along.
            stats: Some(subprocess_base_stats(&status, None)),
        }),
        None => {
            // No Final present: push a synthesized Final so dispatch can pull it from output_tail.
            let value = serde_json::json!({
                "raw": "",
                "note": "streaming mode: no Final event received",
                "exit_success": status.success(),
            });
            let _ = engine
                .submit_output(
                    token,
                    task_id,
                    attempt,
                    OutputEvent::Final {
                        content: ContentRef::Inline {
                            value: value.clone(),
                        },
                        ok: false,
                    },
                )
                .await;
            Ok(WorkerResult {
                value,
                ok: false,
                stats: Some(subprocess_base_stats(&status, None)),
            })
        }
    }
}

/// Shared per-chunk parse + emit path. Called by every reader once it
/// has recovered an `OutputEvent`.
async fn forward_event(
    engine: &Engine,
    token: &CapToken,
    task_id: &StepId,
    attempt: u32,
    ev: OutputEvent,
    last_final: &mut Option<(Value, bool)>,
) {
    if let OutputEvent::Final { content, ok } = &ev {
        let value = match content {
            ContentRef::Inline { value } => value.clone(),
            ContentRef::FileRef {
                path,
                mime,
                size_hint,
            } => serde_json::json!({
                "file_ref": path.to_string_lossy(),
                "mime": mime,
                "size_hint": size_hint,
            }),
        };
        *last_final = Some((value, *ok));
    }
    let _ = engine.submit_output(token, task_id, attempt, ev).await;
}

/// NDJSON: one line per JSON `OutputEvent`. Unparseable lines are
/// skipped.
async fn read_ndjson(
    stdout: tokio::process::ChildStdout,
    engine: &Engine,
    token: &CapToken,
    task_id: &StepId,
    attempt: u32,
    cancel: CancellationToken,
) -> Result<Option<(Value, bool)>, WorkerError> {
    let mut reader = BufReader::new(stdout).lines();
    let mut last_final = None;
    loop {
        tokio::select! {
            line_res = reader.next_line() => match line_res {
                Ok(Some(line)) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() { continue; }
                    if let Ok(ev) = serde_json::from_str::<OutputEvent>(trimmed) {
                        forward_event(engine, token, task_id, attempt, ev, &mut last_final).await;
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(WorkerError::Failed(format!("ndjson read: {e}"))),
            },
            _ = cancel.cancelled() => return Err(WorkerError::Cancelled),
        }
    }
    Ok(last_final)
}

/// SSE: one event per `data: <json>` line followed by a blank line.
/// `event:` / `id:` / `retry:` lines are ignored; multiple `data:`
/// lines are LF-joined into a single JSON payload (a W3C-SSE-spec MVP).
async fn read_sse(
    stdout: tokio::process::ChildStdout,
    engine: &Engine,
    token: &CapToken,
    task_id: &StepId,
    attempt: u32,
    cancel: CancellationToken,
) -> Result<Option<(Value, bool)>, WorkerError> {
    let mut reader = BufReader::new(stdout).lines();
    let mut last_final = None;
    let mut data_buf = String::new();
    loop {
        tokio::select! {
            line_res = reader.next_line() => match line_res {
                Ok(Some(line)) => {
                    if line.is_empty() {
                        // Empty line = event terminator, so flush.
                        if !data_buf.is_empty() {
                            if let Ok(ev) = serde_json::from_str::<OutputEvent>(data_buf.trim()) {
                                forward_event(engine, token, task_id, attempt, ev, &mut last_final).await;
                            }
                            data_buf.clear();
                        }
                    } else if let Some(rest) = line.strip_prefix("data:") {
                        // SSE spec: optional space after colon
                        let payload = rest.strip_prefix(' ').unwrap_or(rest);
                        if !data_buf.is_empty() {
                            data_buf.push('\n');
                        }
                        data_buf.push_str(payload);
                    }
                    // else: event: / id: / retry: / comment line → skip
                }
                Ok(None) => {
                    // EOF: flush any leftover data_buf as the final event.
                    if !data_buf.is_empty() {
                        if let Ok(ev) = serde_json::from_str::<OutputEvent>(data_buf.trim()) {
                            forward_event(engine, token, task_id, attempt, ev, &mut last_final).await;
                        }
                    }
                    break;
                }
                Err(e) => return Err(WorkerError::Failed(format!("sse read: {e}"))),
            },
            _ = cancel.cancelled() => return Err(WorkerError::Cancelled),
        }
    }
    Ok(last_final)
}

/// Length-prefixed: repeated `[u32 BE length][N bytes JSON payload]`
/// binary frames.
async fn read_length_prefixed(
    mut stdout: tokio::process::ChildStdout,
    engine: &Engine,
    token: &CapToken,
    task_id: &StepId,
    attempt: u32,
    cancel: CancellationToken,
) -> Result<Option<(Value, bool)>, WorkerError> {
    use tokio::io::AsyncReadExt;
    let mut last_final = None;
    loop {
        // Read the 4-byte length prefix (racing against cancel via select).
        let mut len_buf = [0u8; 4];
        let read_fut = stdout.read_exact(&mut len_buf);
        let read_res = tokio::select! {
            r = read_fut => r,
            _ = cancel.cancelled() => return Err(WorkerError::Cancelled),
        };
        match read_res {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // clean EOF
            Err(e) => return Err(WorkerError::Failed(format!("len read: {e}"))),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > 16 * 1024 * 1024 {
            // 0 or > 16 MiB is treated as a frame error; break out.
            break;
        }
        let mut payload = vec![0u8; len];
        let read_fut = stdout.read_exact(&mut payload);
        let read_res = tokio::select! {
            r = read_fut => r,
            _ = cancel.cancelled() => return Err(WorkerError::Cancelled),
        };
        if read_res.is_err() {
            break;
        }
        if let Ok(ev) = serde_json::from_slice::<OutputEvent>(&payload) {
            forward_event(engine, token, task_id, attempt, ev, &mut last_final).await;
        }
    }
    Ok(last_final)
}

#[cfg(test)]
mod embed_tests {
    use super::*;

    fn vars() -> EmbedVars {
        EmbedVars {
            system: "SYS".to_string(),
            system_file: Some("/tmp/sys.md".to_string()),
            prompt: "do the task".to_string(),
            model: Some("small".to_string()),
            tools_csv: "Read,Grep".to_string(),
            work_dir: Some("/tmp/wd".to_string()),
            task_id: "ST-1".to_string(),
            attempt: "1".to_string(),
        }
    }

    #[test]
    fn render_substitutes_every_closed_set_token() {
        let v = vars();
        let out = v
            .render("{system}|{system_file}|{prompt}|{model}|{tools_csv}|{work_dir}|{task_id}|{attempt}")
            .expect("render");
        assert_eq!(
            out,
            "SYS|/tmp/sys.md|do the task|small|Read,Grep|/tmp/wd|ST-1|1"
        );
    }

    #[test]
    fn render_leaves_non_placeholder_braces_alone() {
        let v = vars();
        let out = v
            .render(r#"echo '{"result": "{prompt}"}'"#)
            .expect("render");
        assert_eq!(out, r#"echo '{"result": "do the task"}'"#);
    }

    #[test]
    fn render_fails_loud_when_model_referenced_but_absent() {
        let mut v = vars();
        v.model = None;
        let err = v.render("--model {model}").unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("{model}"), "actionable token in error: {msg}");
        assert!(msg.contains("profile.model"), "actionable cause: {msg}");
    }

    #[test]
    fn render_fails_loud_when_work_dir_referenced_but_absent() {
        let mut v = vars();
        v.work_dir = None;
        let err = v.render("{work_dir}").unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("{work_dir}"), "actionable token: {msg}");
    }

    #[test]
    fn render_empty_system_is_legal() {
        let mut v = vars();
        v.system = String::new();
        assert_eq!(v.render("[{system}]").expect("render"), "[]");
    }

    /// Holistic-review fix: substituted VALUES are never re-scanned — a
    /// prompt containing a literal closed-set token must pass through
    /// verbatim, and must not trigger a missing-source failure for a
    /// token the template itself never references.
    #[test]
    fn render_never_substitutes_inside_substituted_values() {
        let mut v = vars();
        v.prompt = "please mention {model} and {work_dir} literally".to_string();
        v.model = None; // template does not reference {model} → no source needed
        v.work_dir = None;
        let out = v.render("task: {prompt}").expect("render");
        assert_eq!(out, "task: please mention {model} and {work_dir} literally");
    }

    /// Placeholders nested inside literal braces (JSON-wrapped stdin)
    /// are still substituted by the single-pass scan.
    #[test]
    fn render_substitutes_placeholder_nested_in_literal_braces() {
        let v = vars();
        let out = v.render(r#"{"task": "{prompt}", "n": 1}"#).expect("render");
        assert_eq!(out, r#"{"task": "do the task", "n": 1}"#);
    }

    #[test]
    fn embed_references_scans_argv_stdin_env_cwd() {
        let mut t = EmbedTemplate {
            argv: vec!["cat".to_string()],
            ..Default::default()
        };
        assert!(!embed_references(&t, "{system_file}"));
        t.stdin = Some("{system_file}".to_string());
        assert!(embed_references(&t, "{system_file}"));
        t.stdin = None;
        t.env.insert("X".to_string(), "{system_file}".to_string());
        assert!(embed_references(&t, "{system_file}"));
        t.env.clear();
        t.cwd = Some("{system_file}".to_string());
        assert!(embed_references(&t, "{system_file}"));
    }

    fn fake_output(stdout: &str, stderr: &str, code: i32) -> std::process::Output {
        #[cfg(unix)]
        use std::os::unix::process::ExitStatusExt;
        #[cfg(windows)]
        use std::os::windows::process::ExitStatusExt;
        use std::process::ExitStatus;
        #[cfg(unix)]
        let status = ExitStatus::from_raw(code << 8);
        #[cfg(windows)]
        let status = ExitStatus::from_raw(code as u32);
        std::process::Output {
            status,
            stdout: stdout.as_bytes().to_vec(),
            stderr: stderr.as_bytes().to_vec(),
        }
    }

    /// Historical byte-compat pin: with NO output declaration, the
    /// normalize result must equal the spec-based path's expression —
    /// lenient JSON parse, raw wrap on failure, `ok = exit success`.
    #[test]
    fn normalize_without_decl_is_historical_json_or_raw() {
        let out = fake_output(r#"{"a": 1}"#, "", 0);
        let wr = normalize_plain_output(&out, None, None);
        assert_eq!(wr.value, serde_json::json!({"a": 1}));
        assert!(wr.ok);

        let out = fake_output("plain text\n", "warned", 0);
        let wr = normalize_plain_output(&out, None, None);
        assert_eq!(
            wr.value,
            serde_json::json!({"raw": "plain text", "stderr": "warned"})
        );
        assert!(wr.ok);

        let out = fake_output("boom", "", 1);
        let wr = normalize_plain_output(&out, None, None);
        assert!(!wr.ok, "non-zero exit is a failed step");
    }

    #[test]
    fn normalize_result_ptr_extracts_declared_value() {
        let decl = SubprocessOutput {
            format: Some("json".to_string()),
            result_ptr: Some("/result".to_string()),
            ok_from: Some("exit_code".to_string()),
            stats: None,
        };
        let out = fake_output(r#"{"result": {"answer": 42}, "noise": true}"#, "", 0);
        let wr = normalize_plain_output(&out, Some(&decl), None);
        assert_eq!(wr.value, serde_json::json!({"answer": 42}));
        assert!(wr.ok);
    }

    #[test]
    fn normalize_declared_json_unparsable_stdout_fails_loud() {
        let decl = SubprocessOutput {
            format: Some("json".to_string()),
            result_ptr: None,
            ok_from: None,
            stats: None,
        };
        let out = fake_output("not json at all", "stderr text", 0);
        let wr = normalize_plain_output(&out, Some(&decl), None);
        assert!(!wr.ok, "declared-JSON unparsable stdout is a failed step");
        assert_eq!(wr.value["raw"], "not json at all");
        assert_eq!(wr.value["stderr"], "stderr text");
        assert!(wr.value["parse_error"].is_string());
    }

    #[test]
    fn normalize_missing_result_ptr_fails_loud_with_actionable_value() {
        let decl = SubprocessOutput {
            format: Some("json".to_string()),
            result_ptr: Some("/missing".to_string()),
            ok_from: None,
            stats: None,
        };
        let out = fake_output(r#"{"present": 1}"#, "", 0);
        let wr = normalize_plain_output(&out, Some(&decl), None);
        assert!(!wr.ok);
        assert!(wr.value["error"]
            .as_str()
            .expect("actionable error message")
            .contains("/missing"));
    }

    #[test]
    fn normalize_ok_from_pointer_reads_boolean() {
        let decl = SubprocessOutput {
            format: Some("json".to_string()),
            result_ptr: Some("/result".to_string()),
            ok_from: Some("/ok".to_string()),
            stats: None,
        };
        // Pointer true → ok even though we also check it beats exit code.
        let out = fake_output(r#"{"result": "r", "ok": true}"#, "", 0);
        let wr = normalize_plain_output(&out, Some(&decl), None);
        assert!(wr.ok);
        // Pointer false → failed step despite exit 0.
        let out = fake_output(r#"{"result": "r", "ok": false}"#, "", 0);
        let wr = normalize_plain_output(&out, Some(&decl), None);
        assert!(!wr.ok);
        // Pointer missing / non-bool → failed step.
        let out = fake_output(r#"{"result": "r", "ok": "yes"}"#, "", 0);
        let wr = normalize_plain_output(&out, Some(&decl), None);
        assert!(!wr.ok);
    }

    /// A declared `usage_ptr` whose object carries only one token axis
    /// still lands a usage record — the CLI backends that print a bare
    /// total (or only the splits) used to have their usage dropped.
    #[test]
    fn declared_usage_ptr_accepts_a_partial_usage_object() {
        let decl = SubprocessOutput {
            format: Some("json".to_string()),
            result_ptr: Some("/result".to_string()),
            ok_from: None,
            stats: Some(mlua_swarm_schema::SubprocessStats {
                usage_ptr: Some("/usage".to_string()),
                model_ptr: None,
                num_turns_ptr: None,
            }),
        };

        // Total only.
        let out = fake_output(r#"{"result": "r", "usage": {"total_tokens": 512}}"#, "", 0);
        let usage = normalize_plain_output(&out, Some(&decl), None)
            .stats
            .and_then(|s| s.usage)
            .expect("a total-only usage must be recorded");
        assert_eq!(usage.total_tokens, 512);
        assert_eq!(usage.input_tokens, 0);

        // Splits only (OpenAI spelling) → total derived.
        let out = fake_output(
            r#"{"result": "r", "usage": {"prompt_tokens": 10, "completion_tokens": 4}}"#,
            "",
            0,
        );
        let usage = normalize_plain_output(&out, Some(&decl), None)
            .stats
            .and_then(|s| s.usage)
            .expect("splits-only usage must be recorded");
        assert_eq!(usage.total_tokens, 14);

        // No token axis at all → no usage recorded.
        let out = fake_output(r#"{"result": "r", "usage": {"cached": 3}}"#, "", 0);
        assert!(normalize_plain_output(&out, Some(&decl), None)
            .stats
            .and_then(|s| s.usage)
            .is_none());
    }
}
