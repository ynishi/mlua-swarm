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

use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::types::{CapToken, StepId, WorkerId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter, WorkerError, WorkerResult};
use crate::worker::output::{ContentRef, OutputEvent};
use crate::worker::{Worker, WorkerJoinHandler};
use async_trait::async_trait;
use serde_json::Value;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

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
        }
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

        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args)
            .env("MSE_TOKEN_AGENT_ID", &token.agent_id)
            .env("MSE_TOKEN_NONCE", &token.nonce)
            .env("MSE_TASK_ID", &task_id.0)
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
        tracing::debug!(worker_id = %worker_id.0, step_id = %task_id, "worker spawned (subprocess)");
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
                                Ok(WorkerResult { value, ok: out.status.success() })
                            }
                            Err(e) => Err(WorkerError::Failed(format!("wait_with_output: {e}"))),
                        }
                    }
                    _ = cancel_inner.cancelled() => Err(WorkerError::Cancelled),
                };
                if let Ok(wr) = &result {
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
            Ok(WorkerResult { value, ok: false })
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
