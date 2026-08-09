//! WS client embedding for mse mcp (S3, design
//! (see the WS multi-session design).
//!
//! Owns the in-process `sid → SessionEntry` map backing the session tools
//! (`mse_operator_join` / `mse_pending_wait` / `mse_ack` / `mse_operator_leave`,
//! wired in `main.rs`) and, through the token those entries hold, every
//! Bearer-gated read this client proxies — the 記名 list and the handover
//! surface (`mse_operator_list` / `mse_run_assignees` / `mse_run_handover` /
//! `mse_run_material`). Each `join()` mints an Operator session via
//! `POST /v1/operators`, then attaches a `tokio-tungstenite` WS client to
//! `WS /v1/operators/:sid/ws` with the returned Bearer token; a background
//! reader task drains incoming frames into a per-session pending queue.
//!
//! The wire protocol (`mse_server::operator_ws::protocol::{ServerMsg,
//! ClientMsg}`) is **mirrored locally** rather than imported from the
//! `mse serve` crate directly: the server-side `ServerMsg` only derives
//! `Serialize` (server → client direction) and `ClientMsg` only derives
//! `Deserialize` (client → server direction) — mse mcp needs the opposite of
//! each. Mirroring keeps this client decoupled from the server crate's wire
//! evolution (kept in lockstep by hand; see `ServerMsgMirror` /
//! `ClientMsgMirror` below, which match `protocol.rs` field-for-field).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use mlua_swarm::{RunId, StepId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsSource = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// How many ③ WS upgrades **one** reconnect attempt may spend before the
/// call that triggered it gives up. The budget is per call, not per sid:
/// the next call starts over with a full budget, so a server that is down
/// now and back in an hour is re-attached by whichever call happens after
/// it returns.
///
/// A count rather than a deadline on purpose — the ② session behind the sid
/// has no TTL and no sweeper, so however long the outage lasts, a handshake
/// that finally succeeds is still valid. This client never retires a sid;
/// only `DELETE /v1/operators/:sid` does.
const MAX_RECONNECT_ATTEMPTS: u32 = 3;

/// Timeout for the ② `GET /v1/healthz` probe that gates a reconnect.
///
/// One second, because the route it probes is `async fn healthz() ->
/// &'static str { "ok" }` — no I/O, no lock, no store access — served over
/// loopback in the normal deployment. The only thing a longer budget buys
/// is patience for scheduler preemption on a loaded machine, and 1s leaves
/// roughly 5x headroom over the worst preemption this workspace has
/// measured (186ms, the v0.23.0 `max_hold` CI flake). It is also spent
/// inside an `mse_pending_wait` call, once per poll for as long as the
/// server stays down, so the caller pays it repeatedly.
const HEALTHZ_TIMEOUT: Duration = Duration::from_secs(1);

/// Local mirror of `mse_server::operator_ws::protocol::ServerMsg` (deserialize
/// direction only — the server-side enum only derives `Serialize`). Only used
/// to validate shape + extract `req_id` / discriminant; the actual payload
/// handed back to the MCP caller is built from the raw JSON (see
/// `parse_server_frame`), not from these typed fields, so most fields are
/// write-only from Rust's point of view (`#[allow(dead_code)]` on the enum).
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMsgMirror {
    Ask {
        req_id: String,
        #[serde(default)]
        parent_req_id: Option<String>,
        // Typed `StepId` (issue #14): the mirror's job is shape validation,
        // and the prefix check is part of the shape.
        task_id: StepId,
        question: Value,
    },
    HookBefore {
        req_id: String,
        #[serde(default)]
        parent_req_id: Option<String>,
        task_id: StepId,
        agent: String,
        attempt: u32,
    },
    HookAfter {
        req_id: String,
        #[serde(default)]
        parent_req_id: Option<String>,
        task_id: StepId,
        agent: String,
        attempt: u32,
        result: Value,
    },
    Spawn {
        req_id: String,
        #[serde(default)]
        parent_req_id: Option<String>,
        task_id: StepId,
        agent: String,
        attempt: u32,
        capability_token: String,
        #[serde(default)]
        worker_handle: Option<String>,
        // issue #18 (mlua-swarm): `TaskSpec.initial_directive` /
        // `EngineState.prompts` / `Engine::fetch_prompt` carry `Value`
        // end-to-end; the WS `Spawn.directive` stays `String` (the wire
        // shape unchanged) because
        // `default_spawn_directive_with_task_directive` renders the
        // reminder text down to a plain string just before send.
        directive: String,
    },
}

impl ServerMsgMirror {
    fn kind(&self) -> &'static str {
        match self {
            ServerMsgMirror::Ask { .. } => "ask",
            ServerMsgMirror::HookBefore { .. } => "hook_before",
            ServerMsgMirror::HookAfter { .. } => "hook_after",
            ServerMsgMirror::Spawn { .. } => "spawn",
        }
    }

    fn req_id(&self) -> &str {
        match self {
            ServerMsgMirror::Ask { req_id, .. }
            | ServerMsgMirror::HookBefore { req_id, .. }
            | ServerMsgMirror::HookAfter { req_id, .. }
            | ServerMsgMirror::Spawn { req_id, .. } => req_id,
        }
    }
}

/// Local mirror of `mse_server::operator_ws::protocol::ClientMsg` (serialize
/// direction only). Field-for-field match of the server-side enum, per module doc.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsgMirror {
    Answer {
        req_id: String,
        value: Value,
    },
    HookAck {
        req_id: String,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    SpawnAck {
        req_id: String,
        value: Value,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        /// Operator-proxied per-step run stats (usage / model /
        /// num_turns…): the harness reports the SubAgent's resource
        /// usage to the Operator on completion, and the Operator
        /// attaches it here so the server folds it into the terminal
        /// `StepEntry`. Omitted from the wire when `None`.
        #[serde(skip_serializing_if = "Option::is_none")]
        stats: Option<Value>,
    },
    /// Controlled halt for the current spawn (issue #7). See
    /// server-side `ClientMsg::SpawnHalt` for semantics: server marks
    /// the step as a normal termination (log `info`, not
    /// `WorkerError`), merging `value` + `reason` into the ctx halt
    /// marker.
    SpawnHalt {
        req_id: String,
        value: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

/// One popped server frame — the shape `mse_pending_wait` hands back to the caller.
#[derive(Debug)]
pub struct PendingFrame {
    pub req_id: String,
    pub kind: &'static str,
    /// The full server frame, minus the `type` discriminant (already surfaced
    /// via `kind`), verbatim.
    pub payload: Value,
}

/// Parses one raw WS text frame from the server into a `PendingFrame`.
/// `None` on malformed JSON or an unrecognized `type` discriminant (frames
/// that don't parse are dropped by the reader loop, mirroring the server's
/// own `Err(_) => continue` tolerance for unparseable `ClientMsg` frames).
fn parse_server_frame(text: &str) -> Option<PendingFrame> {
    let parsed: ServerMsgMirror = serde_json::from_str(text).ok()?;
    let raw: Value = serde_json::from_str(text).ok()?;
    let mut obj = match raw {
        Value::Object(m) => m,
        _ => return None,
    };
    obj.remove("type");
    Some(PendingFrame {
        req_id: parsed.req_id().to_string(),
        kind: parsed.kind(),
        payload: Value::Object(obj),
    })
}

/// Builds the outgoing `ClientMsgMirror` for `mse_ack`. Pure / no I/O — kept
/// separate from `OperatorClientState::ack` so the `kind` validation path is
/// unit-testable without a live session or network access.
fn build_client_msg(
    kind: &str,
    req_id: String,
    value: Option<Value>,
    ok: bool,
    error: Option<String>,
    stats: Option<Value>,
) -> Result<ClientMsgMirror, ClientError> {
    match kind {
        "answer" => Ok(ClientMsgMirror::Answer {
            req_id,
            value: value.unwrap_or(Value::Null),
        }),
        "hook_ack" => Ok(ClientMsgMirror::HookAck {
            req_id,
            ok,
            reason: error,
        }),
        "spawn_ack" => Ok(ClientMsgMirror::SpawnAck {
            req_id,
            value: value.unwrap_or_else(|| serde_json::json!({})),
            ok,
            error,
            stats,
        }),
        "spawn_halt" => Ok(ClientMsgMirror::SpawnHalt {
            req_id,
            value: value.unwrap_or_else(|| serde_json::json!({})),
            // `error` field is reused as the halt `reason` string on
            // the outgoing wire message — it's the same channel from
            // the caller's perspective (human-readable log line).
            reason: error,
        }),
        other => Err(ClientError::InvalidAckKind(other.to_string())),
    }
}

/// Per-session FIFO of undelivered `PendingFrame`s + a `Notify` waker for
/// `mse_pending_wait`'s long-poll. Standalone (no WS / network dependency) so
/// it is directly unit-testable.
struct PendingQueue {
    items: Mutex<VecDeque<PendingFrame>>,
    waker: Notify,
}

impl PendingQueue {
    fn new() -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
            waker: Notify::new(),
        }
    }

    async fn push(&self, frame: PendingFrame) {
        self.items.lock().await.push_back(frame);
        self.waker.notify_one();
    }

    /// Pops the oldest frame, waiting up to `timeout` if the queue is
    /// currently empty. Returns `None` once `timeout` elapses with nothing
    /// delivered. Registers interest on `waker` *before* checking the queue
    /// each iteration (standard `tokio::sync::Notify` check-then-wait
    /// pattern) so a `push()` racing with a `wait()` is never lost.
    async fn wait(&self, timeout: Duration) -> Option<PendingFrame> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.waker.notified();
            if let Some(frame) = self.items.lock().await.pop_front() {
                return Some(frame);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }
}

/// ③ re-establishment bookkeeping for one sid, kept behind a single lock:
/// holding that lock for the whole reconnect attempt is what keeps two
/// callers (`pending_wait` and `ack` can both notice the same drop) from
/// re-upgrading the same session in parallel.
struct ReconnectState {
    /// ③ WS upgrades spent by the reconnect attempt currently in progress.
    /// Zero between attempts — it is cleared both when an upgrade succeeds
    /// and when [`MAX_RECONNECT_ATTEMPTS`] have been burnt without one, so
    /// the budget belongs to a single call and never accumulates across
    /// them. That is what keeps a sid usable for the whole life of a run:
    /// a call during an outage fails, the call after the server comes back
    /// re-attaches.
    attempts: u32,
    /// Bumped on every successful ③ (re-)connect. A caller that decided to
    /// reconnect from a failed send reads this before queueing on the lock
    /// and skips its own attempt when the value moved while it waited —
    /// someone else already replaced the socket it was about to replace.
    epoch: u64,
}

struct SessionEntry {
    token: String,
    writer: Mutex<WsSink>,
    pending: Arc<PendingQueue>,
    /// Behind a `Mutex` (like `writer`) because the `sessions` map hands out
    /// `Arc<SessionEntry>`: a reconnect has no `&mut` to swap the handle
    /// in place with, so the swap goes through interior mutability.
    reader_task: Mutex<JoinHandle<()>>,
    reconnect: Mutex<ReconnectState>,
}

/// Route info for one spawned worker, captured from a Spawn frame as it
/// passes through [`OperatorClientState::pending_wait`]. Keyed by the
/// frame's `worker_handle`, it lets the worker HTTP tools
/// (`mse_worker_fetch` / `mse_worker_submit`) resolve `base_url` /
/// `task_id` from the handle alone — the MainAI only has to relay the
/// handle to the SubAgent.
#[derive(Debug, Clone)]
pub struct WorkerRoute {
    /// HTTP root of the server this process is joined to.
    pub base_url: String,
    /// Step id the spawn belongs to (the frame's `task_id`).
    pub task_id: String,
}

/// Errors surfaced to the MCP tool layer (mapped to `McpError` in `main.rs`).
#[derive(Debug)]
pub enum ClientError {
    UnknownSid(String),
    Http(String),
    Ws(String),
    InvalidAckKind(String),
    /// The ③ WS connection for this sid is down and could not be re-established.
    ///
    /// Scoped to the call that produced it: the ② session is left alone, so
    /// the same sid is worth calling again once the server is back.
    SessionClosed(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::UnknownSid(sid) => write!(f, "unknown sid: {sid}"),
            ClientError::Http(m) => write!(f, "http: {m}"),
            ClientError::Ws(m) => write!(f, "ws: {m}"),
            ClientError::InvalidAckKind(k) => {
                write!(
                    f,
                    "invalid ack kind '{k}' (expected answer|hook_ack|spawn_ack|spawn_halt)"
                )
            }
            ClientError::SessionClosed(m) => write!(f, "session closed: {m}"),
        }
    }
}

impl std::error::Error for ClientError {}

/// Owns all live `sid → SessionEntry` state for the mse mcp process. One
/// instance is shared (`Arc`) across all 4 tool handlers in `main.rs`.
pub struct OperatorClientState {
    sessions: Mutex<HashMap<String, Arc<SessionEntry>>>,
    http_base: String,
    /// `worker_handle → WorkerRoute` captured from Spawn frames (see
    /// [`Self::record_spawn_route`]). Entries live for the process
    /// lifetime — a handful of short strings per dispatch, so no eviction
    /// is needed at realistic session sizes.
    worker_routes: Mutex<HashMap<String, WorkerRoute>>,
}

impl OperatorClientState {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            http_base: resolve_http_base(),
            worker_routes: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    fn with_http_base(http_base: impl Into<String>) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            http_base: http_base.into(),
            worker_routes: Mutex::new(HashMap::new()),
        }
    }

    /// HTTP root every session in this process is joined to (`MSE_BASE_URL`
    /// / the built-in default). A launch that targets a different server
    /// cannot be auto-pinned to a session minted here — the sid means
    /// nothing over there.
    pub fn http_base(&self) -> &str {
        &self.http_base
    }

    /// The sid of this process's **only** live Operator session, or `None`
    /// when it holds zero or more than one.
    ///
    /// This is the auto-pin source: with exactly one session there is no
    /// ambiguity about which one a launch from this process belongs to, so
    /// the launch can pin it without the driver naming it. Zero (nothing to
    /// pin) and two-or-more (the process would have to guess) both stay
    /// unpinned — guessing is what the pin exists to prevent.
    pub async fn sole_live_sid(&self) -> Option<String> {
        let sessions = self.sessions.lock().await;
        sole_sid(sessions.keys().map(String::as_str))
    }

    async fn get_entry(&self, sid: &str) -> Result<Arc<SessionEntry>, ClientError> {
        self.sessions
            .lock()
            .await
            .get(sid)
            .cloned()
            .ok_or_else(|| ClientError::UnknownSid(sid.to_string()))
    }

    /// `POST /v1/operators` (mint sid+token) then `WS /v1/operators/:sid/ws`
    /// (Bearer). The token stays in-process (`SessionEntry.token`) — never
    /// returned to the caller. Returns the `sid`.
    ///
    /// `desc` is the session's 記名 (server model §4.2 **D1**): what this
    /// session is here to do, fixed at join and read back off
    /// [`Self::list_operators`]. The server accepts a join without one, so
    /// this is an `Option`; the tool that AIs join through makes it
    /// mandatory instead.
    ///
    /// A join claims no role: the sid the server mints here is the whole of
    /// this session's identity, and which Run it drives is decided per Run
    /// (a launch pin, or `POST /v1/runs/:id/acquire`).
    pub async fn join(
        &self,
        capability_manifest: Option<mlua_swarm::AgentProviderManifest>,
        desc: Option<String>,
    ) -> Result<String, ClientError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| ClientError::Http(e.to_string()))?;
        let resp = client
            .post(format!("{}/v1/operators", self.http_base))
            .json(&serde_json::json!({
                "capability_manifest": capability_manifest,
                "desc": desc,
            }))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ClientError::Http(format!(
                "POST /v1/operators failed: {status} {body}"
            )));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        let sid = body["sid"]
            .as_str()
            .ok_or_else(|| ClientError::Http("missing sid in POST /v1/operators response".into()))?
            .to_string();
        let token = body["token"]
            .as_str()
            .ok_or_else(|| {
                ClientError::Http("missing token in POST /v1/operators response".into())
            })?
            .to_string();

        let (writer, reader) = match connect_ws(&self.http_base, &sid, &token).await {
            Ok(parts) => parts,
            Err(connect_error) => {
                if let Err(rollback_error) =
                    rollback_minted_session(&client, &self.http_base, &sid, &token).await
                {
                    return Err(ClientError::Ws(format!(
                        "{connect_error}; rollback DELETE /v1/operators/{sid} failed: {rollback_error}"
                    )));
                }
                return Err(connect_error);
            }
        };

        let pending = Arc::new(PendingQueue::new());
        let reader_task = spawn_reader(reader, pending.clone());

        let entry = Arc::new(SessionEntry {
            token,
            writer: Mutex::new(writer),
            pending,
            reader_task: Mutex::new(reader_task),
            reconnect: Mutex::new(ReconnectState {
                attempts: 0,
                epoch: 0,
            }),
        });
        self.sessions.lock().await.insert(sid.clone(), entry);
        Ok(sid)
    }

    /// Re-establishes ③ when its reader task has finished — the passive
    /// detection point. A finished reader means the socket delivered a
    /// `Close` or an error, which is the only in-process signal that ③ went
    /// away; the ② session behind the sid is untouched by that and is
    /// exactly what the new socket re-attaches to.
    ///
    /// No-op (and no lock contention beyond one short `reader_task` peek)
    /// while the reader is alive, so the healthy path pays nothing.
    async fn ensure_connected(
        &self,
        sid: &str,
        entry: &Arc<SessionEntry>,
    ) -> Result<(), ClientError> {
        if !entry.reader_task.lock().await.is_finished() {
            return Ok(());
        }
        let mut state = entry.reconnect.lock().await;
        // Re-check under the lock: `pending_wait` and `ack` can both spot
        // the same drop, and the one that queued second must not tear down
        // the socket the first one just installed.
        if !entry.reader_task.lock().await.is_finished() {
            return Ok(());
        }
        self.reconnect(sid, entry, &mut state).await
    }

    /// Re-establishes ③ after a send failed — the active detection point.
    /// A rejected `writer.send` is the most reliable evidence that the
    /// socket is gone, and it can arrive while the reader task has not
    /// noticed yet, so this path does not consult `reader_task`.
    ///
    /// Skips its own attempt when another caller re-established ③ while
    /// this one waited for the lock (the epoch moved); the caller's retry
    /// then simply uses that fresh socket.
    async fn reconnect_after_failed_send(
        &self,
        sid: &str,
        entry: &Arc<SessionEntry>,
    ) -> Result<(), ClientError> {
        let epoch_before = entry.reconnect.lock().await.epoch;
        let mut state = entry.reconnect.lock().await;
        if state.epoch != epoch_before {
            return Ok(());
        }
        self.reconnect(sid, entry, &mut state).await
    }

    /// The reconnect itself. Runs with `state` held for its whole duration,
    /// which is what serializes concurrent attempts on one sid.
    ///
    /// ② is probed first: when the server is not answering `GET
    /// /v1/healthz` there is nothing to hand a WS handshake to, so this
    /// gives up without spending a single ③ upgrade. On a later call the
    /// probe is what notices the server came back.
    ///
    /// Otherwise ③ is retried until it succeeds or the call's
    /// [`MAX_RECONNECT_ATTEMPTS`] are gone. Either way the attempt counter
    /// ends at zero — the ceiling bounds **this** call (it always returns,
    /// so there is no unbounded retry loop) and never the sid, which stays
    /// as valid as ② says it is. On success the `writer` and the reader
    /// task are swapped in place while `pending` is carried over untouched
    /// — frames that arrived before the drop and were never popped stay
    /// queued.
    async fn reconnect(
        &self,
        sid: &str,
        entry: &Arc<SessionEntry>,
        state: &mut ReconnectState,
    ) -> Result<(), ClientError> {
        if !server_healthz_ok(&self.http_base).await {
            return Err(ClientError::SessionClosed(format!(
                "{sid}: {} is not answering GET /v1/healthz",
                self.http_base
            )));
        }
        loop {
            match connect_ws(&self.http_base, sid, &entry.token).await {
                Ok((writer, reader)) => {
                    *entry.writer.lock().await = writer;
                    let replaced = std::mem::replace(
                        &mut *entry.reader_task.lock().await,
                        spawn_reader(reader, entry.pending.clone()),
                    );
                    // The old reader has normally finished already; abort
                    // covers the failed-send path, where it may still be
                    // parked on a socket nothing will ever write to again.
                    replaced.abort();
                    state.attempts = 0;
                    state.epoch = state.epoch.wrapping_add(1);
                    return Ok(());
                }
                Err(error) => {
                    state.attempts += 1;
                    if state.attempts >= MAX_RECONNECT_ATTEMPTS {
                        let spent = state.attempts;
                        state.attempts = 0;
                        return Err(ClientError::SessionClosed(format!(
                            "{sid}: {spent} WS re-upgrades in a row failed (last: {error}); \
                             the ② session is untouched, so a later call retries"
                        )));
                    }
                }
            }
        }
    }

    /// Pops one pending frame for `sid`, waiting up to `timeout_ms`.
    /// `Ok(None)` = timed out with nothing delivered.
    ///
    /// Re-establishes ③ first when it has dropped, so a dead socket
    /// surfaces as `ClientError::SessionClosed` instead of an endless run
    /// of empty long-polls. `timeout_ms` is untouched by that: it is the
    /// budget for waiting on frames and says nothing about ③'s liveness.
    pub async fn pending_wait(
        &self,
        sid: &str,
        timeout_ms: u64,
    ) -> Result<Option<PendingFrame>, ClientError> {
        let entry = self.get_entry(sid).await?;
        self.ensure_connected(sid, &entry).await?;
        let frame = entry.pending.wait(Duration::from_millis(timeout_ms)).await;
        if let Some(f) = &frame {
            self.record_spawn_route(f).await;
        }
        Ok(frame)
    }

    /// Captures `worker_handle → {base_url, task_id}` from a Spawn frame so
    /// the worker HTTP tools can later resolve the route from the handle
    /// alone. No-op for non-spawn frames and frames without a handle. The
    /// MainAI always pops the Spawn frame (via `mse_pending_wait`) before
    /// dispatching the SubAgent that uses the handle, so recording at pop
    /// time is ordered before any lookup by construction.
    async fn record_spawn_route(&self, frame: &PendingFrame) {
        if frame.kind != "spawn" {
            return;
        }
        let (Some(handle), Some(task_id)) = (
            frame.payload.get("worker_handle").and_then(Value::as_str),
            frame.payload.get("task_id").and_then(Value::as_str),
        ) else {
            return;
        };
        self.worker_routes.lock().await.insert(
            handle.to_string(),
            WorkerRoute {
                base_url: self.http_base.clone(),
                task_id: task_id.to_string(),
            },
        );
    }

    /// Looks up the route captured for `worker_handle` (see
    /// [`Self::record_spawn_route`]).
    pub async fn worker_route(&self, worker_handle: &str) -> Option<WorkerRoute> {
        self.worker_routes.lock().await.get(worker_handle).cloned()
    }

    /// Sends the `ClientMsg` corresponding to `kind` over `sid`'s WS
    /// connection. `kind` validation happens before the session lookup, so an
    /// invalid `kind` fails the same way regardless of whether `sid` exists.
    ///
    /// ③ is re-established twice over: once up front if the reader task has
    /// already noticed the drop, and once more if the send itself fails —
    /// the failed send is the sharper signal of the two, and the message is
    /// then sent exactly one more time over the fresh socket.
    // The argument list mirrors the `mse_ack` MCP tool's flat parameter
    // surface one-to-one (kind-discriminated union on the wire); bundling
    // them into a struct only this call would consume adds indirection
    // without removing the union shape — same rationale as the server's
    // `build_router_full` allow.
    #[allow(clippy::too_many_arguments)]
    pub async fn ack(
        &self,
        sid: &str,
        req_id: String,
        kind: &str,
        value: Option<Value>,
        ok: bool,
        error: Option<String>,
        stats: Option<Value>,
    ) -> Result<(), ClientError> {
        let msg = build_client_msg(kind, req_id, value, ok, error, stats)?;
        let entry = self.get_entry(sid).await?;
        let text = serde_json::to_string(&msg).map_err(|e| ClientError::Ws(e.to_string()))?;
        self.ensure_connected(sid, &entry).await?;
        if send_text(&entry, &text).await.is_err() {
            self.reconnect_after_failed_send(sid, &entry).await?;
            return send_text(&entry, &text).await;
        }
        Ok(())
    }

    /// `GET /v1/operators` (Bearer) — the 記名 list, returned verbatim.
    ///
    /// # Why this needs a sid at all
    ///
    /// The route used to be unauthenticated and is now Bearer-gated
    /// (server model **D3**), so a caller has to present *some* live
    /// session's token; any one opens the list. This process only has
    /// tokens for the sessions it minted, so `sid` selects which one to
    /// present. `None` means "the sole live session", the same auto-pin
    /// rule [`Self::sole_live_sid`] applies elsewhere — and it fails
    /// loudly with [`ClientError::UnknownSid`] rather than guessing when
    /// this process holds none or several.
    pub async fn list_operators(
        &self,
        sid: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Value, ClientError> {
        let mut url = format!("{}/v1/operators", self.http_base);
        if let Some(limit) = limit {
            url.push_str(&format!("?limit={limit}"));
        }
        self.get_as_operator(sid, &url).await
    }

    /// `GET /v1/runs/:id/assignees` (Bearer) — who holds each of the Run's
    /// Operator seats, and which seats nobody holds, verbatim.
    ///
    /// Same bearer rule as [`Self::list_operators`] (**D3**: any live
    /// session's token opens it), and the same `sid` selection.
    pub async fn run_assignees(
        &self,
        sid: Option<&str>,
        run_id: &RunId,
    ) -> Result<Value, ClientError> {
        let url = format!("{}/v1/runs/{run_id}/assignees", self.http_base);
        self.get_as_operator(sid, &url).await
    }

    /// `GET /v1/runs/:id/handover` (Bearer) — the four-axis snapshot,
    /// verbatim.
    ///
    /// Verbatim matters more here than anywhere else on this client: the
    /// server answers axes 2 and 3 from **one** `RunRecord` read precisely
    /// so a seat cannot change hands between them, and re-assembling the
    /// body from separate calls would put that skew back.
    pub async fn run_handover(
        &self,
        sid: Option<&str>,
        run_id: &RunId,
    ) -> Result<Value, ClientError> {
        let url = format!("{}/v1/runs/{run_id}/handover", self.http_base);
        self.get_as_operator(sid, &url).await
    }

    /// `GET /v1/runs/:id/material?step_id=<id>` (Bearer) — the material for
    /// one step, verbatim.
    ///
    /// The step id is required: the route answers about one step, and the
    /// server has no default for "which one".
    pub async fn run_material(
        &self,
        sid: Option<&str>,
        run_id: &RunId,
        step_id: &StepId,
    ) -> Result<Value, ClientError> {
        let url = format!(
            "{}/v1/runs/{run_id}/material?step_id={step_id}",
            self.http_base
        );
        self.get_as_operator(sid, &url).await
    }

    /// `POST /v1/runs/:id/acquire` — take one of the Run's Operator seats.
    /// The server's response body is returned verbatim.
    ///
    /// # No Bearer, on purpose
    ///
    /// This is the one call in this module that presents no token, and it
    /// takes no `sid` to present one from. The route is deliberately
    /// ungated (**B2**: the bearer takes no part in assignment; **B3**: an
    /// acquire does not need the outgoing holder's credential), and
    /// attaching a token here would make the bearer decide who holds a
    /// seat — the mistaken-handover guard is the *reading* that comes
    /// first, not this call.
    ///
    /// `op` is therefore whoever the caller says takes the seat, not
    /// whoever this process happens to be joined as.
    pub async fn run_acquire(
        &self,
        run_id: &RunId,
        op: &str,
        desc: &str,
        slot: Option<&str>,
    ) -> Result<Value, ClientError> {
        let url = format!("{}/v1/runs/{run_id}/acquire", self.http_base);
        let resp = http_client()?
            .post(&url)
            .json(&serde_json::json!({
                "op": op,
                "desc": desc,
                "slot": slot,
            }))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        json_or_http_error(resp, "POST", &url).await
    }

    /// One Bearer-gated `GET` presented as an Operator: resolve which
    /// session's token to show, show it, and hand back the server's body
    /// untouched.
    ///
    /// Shared by every read on this client that **D3** gates, so the sid
    /// rule and the "verbatim body" contract are written once. A response
    /// this process re-shaped would be a second, quietly different account
    /// of who holds what.
    async fn get_as_operator(&self, sid: Option<&str>, url: &str) -> Result<Value, ClientError> {
        let sid = self.bearer_sid(sid).await?;
        let entry = self.get_entry(&sid).await?;
        let resp = http_client()?
            .get(url)
            .bearer_auth(&entry.token)
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        json_or_http_error(resp, "GET", url).await
    }

    /// Which session's Bearer token a **D3**-gated read presents: the one
    /// named, or this process's sole live session.
    ///
    /// Zero or several is a loud [`ClientError::UnknownSid`] rather than a
    /// pick — the same reasoning as [`Self::sole_live_sid`]: presenting
    /// some *other* task's session would answer the caller's question
    /// under an identity it did not choose.
    async fn bearer_sid(&self, sid: Option<&str>) -> Result<String, ClientError> {
        match sid {
            Some(sid) => Ok(sid.to_string()),
            None => self.sole_live_sid().await.ok_or_else(|| {
                ClientError::UnknownSid(
                    "no sid given and this process does not hold exactly one live session; \
                     pass the sid whose Bearer token should be presented"
                        .to_string(),
                )
            }),
        }
    }

    /// `DELETE /v1/operators/:sid` (Bearer) + abort the reader task + drop
    /// the local entry. The local entry is removed and the reader task
    /// aborted before the HTTP call, so process-local state is always
    /// cleaned up even if the server-side teardown request fails.
    pub async fn leave(&self, sid: &str) -> Result<(), ClientError> {
        let entry = {
            let mut map = self.sessions.lock().await;
            map.remove(sid)
                .ok_or_else(|| ClientError::UnknownSid(sid.to_string()))?
        };
        entry.reader_task.lock().await.abort();

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| ClientError::Http(e.to_string()))?;
        let resp = client
            .delete(format!("{}/v1/operators/{sid}", self.http_base))
            .bearer_auth(&entry.token)
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ClientError::Http(format!(
                "DELETE /v1/operators/{sid} failed: {status} {body}"
            )));
        }
        Ok(())
    }
}

/// The `reqwest` client every plain HTTP call on this module uses — the
/// same 10s budget the join and leave paths already build inline.
fn http_client() -> Result<reqwest::Client, ClientError> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| ClientError::Http(e.to_string()))
}

/// The server's body as JSON on success, or the status and body it
/// answered with as a [`ClientError::Http`].
///
/// Deserializing straight into [`Value`] is what makes "verbatim" true:
/// nothing here names a field, so a route that grows one carries it
/// through to the caller without this client being taught about it.
async fn json_or_http_error(
    resp: reqwest::Response,
    method: &str,
    url: &str,
) -> Result<Value, ClientError> {
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(ClientError::Http(format!(
            "{method} {url} failed: {status} {body}"
        )));
    }
    resp.json()
        .await
        .map_err(|e| ClientError::Http(e.to_string()))
}

/// The auto-pin rule of [`OperatorClientState::sole_live_sid`], split out
/// so it is unit-testable without a live WebSocket: exactly one live
/// session yields its sid, zero or many yield `None`.
fn sole_sid<'a>(mut sids: impl Iterator<Item = &'a str>) -> Option<String> {
    let first = sids.next()?;
    match sids.next() {
        None => Some(first.to_string()),
        Some(_) => None,
    }
}

/// Best-effort rollback for the two-step join protocol. Once POST minted a
/// role-owning session, any failure before the WebSocket becomes usable must
/// release that session with the same bearer token or it becomes an orphan
/// that blocks later joins for the same role.
async fn rollback_minted_session(
    client: &reqwest::Client,
    http_base: &str,
    sid: &str,
    token: &str,
) -> Result<(), String> {
    let response = client
        .delete(format!("{http_base}/v1/operators/{sid}"))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(format!("{status} {body}"))
    }
}

impl Default for OperatorClientState {
    fn default() -> Self {
        Self::new()
    }
}

/// Opens one ③ WS connection: `WS /v1/operators/:sid/ws` with `token` as the
/// Bearer credential, split into its sink / stream halves.
///
/// Used both for the first attach in [`OperatorClientState::join`] and for
/// every later re-attach — they are literally the same request. The server
/// accepts an unlimited number of upgrades for a given sid+token pair and
/// swaps the sender of the existing session in place, so re-running this
/// against a live ② session is a reconnect, not a second session.
///
/// Note what this function does *not* do: it never rolls the ② session
/// back. `join` owns that decision (it minted the sid moments earlier and
/// must not leak it); the reconnect path must never delete a sid the
/// driver is still pinned to.
async fn connect_ws(
    http_base: &str,
    sid: &str,
    token: &str,
) -> Result<(WsSink, WsSource), ClientError> {
    let ws_url = format!("{}/v1/operators/{}/ws", http_to_ws_base(http_base), sid);
    let mut req = ws_url
        .into_client_request()
        .map_err(|e| ClientError::Ws(e.to_string()))?;
    req.headers_mut().insert(
        "authorization",
        HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| ClientError::Ws(e.to_string()))?,
    );
    let (ws_stream, _) = tokio_tungstenite::connect_async(req)
        .await
        .map_err(|e| ClientError::Ws(e.to_string()))?;
    Ok(ws_stream.split())
}

/// Writes one text frame to `entry`'s current ③ sink. Split out of
/// [`OperatorClientState::ack`] so the send and its one retry go through
/// the same code, and so the sink lock is only ever held for the send
/// itself — a reconnect must never find it pinned by a caller that is
/// about to ask for one.
async fn send_text(entry: &Arc<SessionEntry>, text: &str) -> Result<(), ClientError> {
    entry
        .writer
        .lock()
        .await
        .send(Message::Text(text.to_string()))
        .await
        .map_err(|e| ClientError::Ws(e.to_string()))
}

/// ② `GET /v1/healthz` — "is the server process still there at all?".
/// `true` only for HTTP 200 with body `ok`; every transport failure, every
/// other status and every other body is `false`.
///
/// Mirrors `server::launchd::healthz_ok`, which cannot be reused here: it
/// takes a bare `host:port` bind and hardcodes the `http://` scheme, while
/// this client works from a full `http(s)://` base URL.
async fn server_healthz_ok(http_base: &str) -> bool {
    let Ok(client) = reqwest::Client::builder().timeout(HEALTHZ_TIMEOUT).build() else {
        return false;
    };
    match client.get(format!("{http_base}/v1/healthz")).send().await {
        Ok(response) if response.status().is_success() => response
            .text()
            .await
            .map(|body| body.trim() == "ok")
            .unwrap_or(false),
        _ => false,
    }
}

fn spawn_reader(mut reader: WsSource, pending: Arc<PendingQueue>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(item) = reader.next().await {
            let txt = match item {
                Ok(Message::Text(t)) => t,
                Ok(Message::Close(_)) | Err(_) => break,
                _ => continue,
            };
            if let Some(frame) = parse_server_frame(&txt) {
                pending.push(frame).await;
            }
        }
    })
}

/// `MSE_HTTP` env override, default `http://127.0.0.1:7777` — same literal
/// default `mse serve` binds by default (`server_control::DEFAULT_BIND`).
fn resolve_http_base() -> String {
    std::env::var("MSE_HTTP").unwrap_or_else(|_| "http://127.0.0.1:7777".to_string())
}

/// `http://` → `ws://`, `https://` → `wss://`. Falls back to prefixing `ws://`
/// for a bare host:port (defensive; `resolve_http_base` always yields a
/// scheme-prefixed value, so this branch is only reachable via a malformed
/// `MSE_HTTP` override).
fn http_to_ws_base(http_base: &str) -> String {
    if let Some(rest) = http_base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = http_base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        format!("ws://{http_base}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── parse_server_frame ──────────────────────────────────────────────

    #[test]
    fn parse_server_frame_ask() {
        let text = r#"{"type":"ask","req_id":"r1","task_id":"ST-1","question":{"q":"?"}}"#;
        let frame = parse_server_frame(text).expect("should parse");
        assert_eq!(frame.req_id, "r1");
        assert_eq!(frame.kind, "ask");
        assert_eq!(frame.payload["task_id"], "ST-1");
        assert_eq!(frame.payload["question"], serde_json::json!({"q": "?"}));
        assert!(frame.payload.get("type").is_none(), "type key stripped");
    }

    #[test]
    fn parse_server_frame_hook_before() {
        let text =
            r#"{"type":"hook_before","req_id":"r2","task_id":"ST-1","agent":"a","attempt":1}"#;
        let frame = parse_server_frame(text).expect("should parse");
        assert_eq!(frame.req_id, "r2");
        assert_eq!(frame.kind, "hook_before");
        assert_eq!(frame.payload["agent"], "a");
        assert_eq!(frame.payload["attempt"], 1);
    }

    #[test]
    fn parse_server_frame_hook_after() {
        let text = r#"{"type":"hook_after","req_id":"r3","task_id":"ST-1","agent":"a","attempt":2,"result":{"ok":true}}"#;
        let frame = parse_server_frame(text).expect("should parse");
        assert_eq!(frame.req_id, "r3");
        assert_eq!(frame.kind, "hook_after");
        assert_eq!(frame.payload["result"], serde_json::json!({"ok": true}));
    }

    #[test]
    fn parse_server_frame_spawn() {
        let text = r#"{"type":"spawn","req_id":"r4","task_id":"ST-1","agent":"a","attempt":1,"capability_token":"tok","directive":"do it"}"#;
        let frame = parse_server_frame(text).expect("should parse");
        assert_eq!(frame.req_id, "r4");
        assert_eq!(frame.kind, "spawn");
        assert_eq!(frame.payload["capability_token"], "tok");
        assert_eq!(frame.payload["directive"], "do it");
        assert!(frame.payload.get("worker_handle").is_none());
    }

    #[test]
    fn parse_server_frame_spawn_with_worker_handle() {
        let text = r#"{"type":"spawn","req_id":"r5","task_id":"ST-1","agent":"a","attempt":1,"capability_token":"tok","worker_handle":"wh-abc","directive":"do it"}"#;
        let frame = parse_server_frame(text).expect("should parse");
        assert_eq!(frame.payload["worker_handle"], "wh-abc");
    }

    // ─── auto-pin source (sole live session) ─────────────────────────────

    #[test]
    fn sole_sid_returns_the_only_session() {
        assert_eq!(
            sole_sid(["S-only"].into_iter()),
            Some("S-only".to_string()),
            "one live session is unambiguous — that is the auto-pin"
        );
    }

    #[test]
    fn sole_sid_declines_zero_and_many() {
        assert_eq!(
            sole_sid([].into_iter()),
            None,
            "no live session leaves nothing to pin"
        );
        assert_eq!(
            sole_sid(["S-a", "S-b"].into_iter()),
            None,
            "with several live sessions the process would have to guess, which is \
             exactly what the pin exists to prevent"
        );
    }

    #[tokio::test]
    async fn sole_live_sid_is_none_before_any_join() {
        assert_eq!(OperatorClientState::new().sole_live_sid().await, None);
    }

    // ─── worker route capture (issue: wrapper Bash removal follow-up) ────

    #[tokio::test]
    async fn spawn_route_recorded_and_resolvable_by_handle() {
        let state = OperatorClientState::with_http_base("http://127.0.0.1:7777");
        let frame = parse_server_frame(
            r#"{"type":"spawn","req_id":"r9","task_id":"ST-9","agent":"a","attempt":1,"capability_token":"tok","worker_handle":"wh-route","directive":"d"}"#,
        )
        .expect("should parse");
        state.record_spawn_route(&frame).await;
        let route = state.worker_route("wh-route").await.expect("route cached");
        assert_eq!(route.base_url, "http://127.0.0.1:7777");
        assert_eq!(route.task_id, "ST-9");
        assert!(state.worker_route("wh-unknown").await.is_none());
    }

    #[tokio::test]
    async fn non_spawn_frames_do_not_record_routes() {
        let state = OperatorClientState::with_http_base("http://x");
        let frame =
            parse_server_frame(r#"{"type":"ask","req_id":"r1","task_id":"ST-1","question":{}}"#)
                .expect("should parse");
        state.record_spawn_route(&frame).await;
        assert!(state.worker_route("ST-1").await.is_none());
    }

    #[test]
    fn parse_server_frame_rejects_unknown_type() {
        assert!(parse_server_frame(r#"{"type":"unknown_kind","req_id":"r6"}"#).is_none());
    }

    #[test]
    fn parse_server_frame_rejects_malformed_json() {
        assert!(parse_server_frame("not json").is_none());
        assert!(parse_server_frame("").is_none());
    }

    // ─── build_client_msg ────────────────────────────────────────────────

    #[test]
    fn build_client_msg_answer_serializes_expected_shape() {
        let msg = build_client_msg(
            "answer",
            "r1".into(),
            Some(serde_json::json!({"verdict": "ok"})),
            true,
            None,
            None,
        )
        .expect("valid kind");
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["type"], "answer");
        assert_eq!(v["req_id"], "r1");
        assert_eq!(v["value"], serde_json::json!({"verdict": "ok"}));
    }

    #[test]
    fn build_client_msg_hook_ack_omits_reason_when_none() {
        let msg =
            build_client_msg("hook_ack", "r2".into(), None, true, None, None).expect("valid kind");
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["type"], "hook_ack");
        assert_eq!(v["ok"], true);
        assert!(v.get("reason").is_none());
    }

    #[test]
    fn build_client_msg_hook_ack_carries_reason_as_error() {
        let msg = build_client_msg(
            "hook_ack",
            "r3".into(),
            None,
            false,
            Some("rejected".into()),
            None,
        )
        .expect("valid kind");
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["reason"], "rejected");
    }

    #[test]
    fn build_client_msg_spawn_ack_defaults_value_to_empty_object() {
        let msg =
            build_client_msg("spawn_ack", "r4".into(), None, true, None, None).expect("valid kind");
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["type"], "spawn_ack");
        assert_eq!(v["value"], serde_json::json!({}));
    }

    #[test]
    fn build_client_msg_rejects_unknown_kind() {
        let err = build_client_msg("bogus", "r5".into(), None, true, None, None).unwrap_err();
        assert!(matches!(err, ClientError::InvalidAckKind(k) if k == "bogus"));
    }

    /// Issue #7: `spawn_halt` serializes as its own wire type and carries
    /// the caller-supplied partial value + reason (from the `error`
    /// field, reused).
    #[test]
    fn build_client_msg_spawn_halt_carries_value_and_reason() {
        let msg = build_client_msg(
            "spawn_halt",
            "r6".into(),
            Some(serde_json::json!({"partial": 1})),
            true,
            Some("dogfood shape verified".into()),
            None,
        )
        .expect("valid kind");
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["type"], "spawn_halt");
        assert_eq!(v["req_id"], "r6");
        assert_eq!(v["value"], serde_json::json!({"partial": 1}));
        assert_eq!(v["reason"], "dogfood shape verified");
        // `ok` is not part of the spawn_halt wire shape (halt is always
        // a normal termination — no ok/failure axis).
        assert!(v.get("ok").is_none());
    }

    #[test]
    fn build_client_msg_spawn_halt_defaults_value_to_empty_object() {
        let msg = build_client_msg("spawn_halt", "r7".into(), None, true, None, None)
            .expect("valid kind");
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["type"], "spawn_halt");
        assert_eq!(v["value"], serde_json::json!({}));
        assert!(v.get("reason").is_none());
    }

    #[test]
    fn error_message_lists_all_four_ack_kinds() {
        let err = ClientError::InvalidAckKind("x".into());
        let msg = format!("{err}");
        for kind in ["answer", "hook_ack", "spawn_ack", "spawn_halt"] {
            assert!(msg.contains(kind), "kind `{kind}` missing from: {msg}");
        }
    }

    // ─── PendingQueue ────────────────────────────────────────────────────

    fn frame(req_id: &str) -> PendingFrame {
        PendingFrame {
            req_id: req_id.to_string(),
            kind: "ask",
            payload: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn pending_queue_push_then_pop_is_fifo() {
        let q = PendingQueue::new();
        q.push(frame("a")).await;
        q.push(frame("b")).await;
        let first = q.wait(Duration::from_millis(10)).await.unwrap();
        let second = q.wait(Duration::from_millis(10)).await.unwrap();
        assert_eq!(first.req_id, "a");
        assert_eq!(second.req_id, "b");
    }

    #[tokio::test]
    async fn pending_queue_wait_times_out_on_empty_queue() {
        let q = PendingQueue::new();
        let got = q.wait(Duration::from_millis(30)).await;
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn pending_queue_wait_resolves_when_pushed_concurrently() {
        let q = Arc::new(PendingQueue::new());
        let q2 = q.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            q2.push(frame("late")).await;
        });
        let got = q.wait(Duration::from_secs(2)).await;
        assert_eq!(got.unwrap().req_id, "late");
    }

    // ─── OperatorClientState error paths (no network required) ───────────

    #[tokio::test]
    async fn pending_wait_unknown_sid_errors() {
        let state = OperatorClientState::with_http_base("http://127.0.0.1:1");
        let err = state.pending_wait("no-such-sid", 10).await.unwrap_err();
        assert!(matches!(err, ClientError::UnknownSid(s) if s == "no-such-sid"));
    }

    #[tokio::test]
    async fn ack_unknown_kind_errors_before_sid_lookup() {
        let state = OperatorClientState::with_http_base("http://127.0.0.1:1");
        let err = state
            .ack("no-such-sid", "r1".into(), "bogus", None, true, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, ClientError::InvalidAckKind(k) if k == "bogus"));
    }

    #[tokio::test]
    async fn ack_unknown_sid_errors_for_valid_kind() {
        let state = OperatorClientState::with_http_base("http://127.0.0.1:1");
        let err = state
            .ack("no-such-sid", "r1".into(), "answer", None, true, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, ClientError::UnknownSid(s) if s == "no-such-sid"));
    }

    #[tokio::test]
    async fn leave_unknown_sid_errors() {
        let state = OperatorClientState::with_http_base("http://127.0.0.1:1");
        let err = state.leave("no-such-sid").await.unwrap_err();
        assert!(matches!(err, ClientError::UnknownSid(s) if s == "no-such-sid"));
    }

    #[tokio::test]
    async fn join_unreachable_host_returns_http_error_not_panic() {
        let state = OperatorClientState::with_http_base("http://127.0.0.1:1");
        let err = state.join(None, None).await.unwrap_err();
        assert!(matches!(err, ClientError::Http(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn join_rolls_back_the_minted_session_when_ws_connect_fails() {
        use axum::extract::State;
        use axum::http::{HeaderMap, StatusCode};
        use axum::routing::{delete, post};
        use axum::{Json, Router};
        use std::sync::atomic::{AtomicBool, Ordering};

        let deleted = Arc::new(AtomicBool::new(false));
        let app = Router::new()
            .route(
                "/v1/operators",
                post(|| async {
                    Json(serde_json::json!({
                        "sid": "S-rollback",
                        "token": "rollback-token",
                    }))
                }),
            )
            .route(
                "/v1/operators/:sid",
                delete(
                    |State(deleted): State<Arc<AtomicBool>>, headers: HeaderMap| async move {
                        let authorized = headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            == Some("Bearer rollback-token");
                        if authorized {
                            deleted.store(true, Ordering::SeqCst);
                            StatusCode::NO_CONTENT
                        } else {
                            StatusCode::UNAUTHORIZED
                        }
                    },
                ),
            )
            .with_state(deleted.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind rollback test server");
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let state = OperatorClientState::with_http_base(base);
        let error = state
            .join(None, None)
            .await
            .expect_err("missing WS route must fail the upgrade");

        assert!(matches!(error, ClientError::Ws(_)), "got: {error:?}");
        assert!(
            deleted.load(Ordering::SeqCst),
            "a failed WS join must delete the session it minted"
        );
        server.abort();
    }

    // ─── http_to_ws_base ───────────────────────────────────────────────────

    #[test]
    fn http_to_ws_base_converts_scheme() {
        assert_eq!(
            http_to_ws_base("http://127.0.0.1:7777"),
            "ws://127.0.0.1:7777"
        );
        assert_eq!(http_to_ws_base("https://example.com"), "wss://example.com");
    }

    // ─── ③ reconnect ─────────────────────────────────────────────────────
    //
    // These run against `stub::StubServer`, a scripted stand-in for `mse
    // serve`: it mints one ② session, answers ② `GET /v1/healthz`, and
    // plays one `WsBehavior` per ③ upgrade request. That is enough to
    // reproduce a real dropped socket (the stub closes it) and a real
    // re-upgrade (the client dials again), which is what the reconnect path
    // is made of.

    /// A scripted stand-in for `mse serve`, sized for the ③ reconnect
    /// tests: `POST /v1/operators` mints one fixed sid+token, `GET
    /// /v1/healthz` is toggleable, and each ③ upgrade request consumes the
    /// next entry of a fixed script (`WsBehavior::Refuse` once it runs out).
    mod stub {
        use super::*;
        use axum::extract::ws::{Message as AxumMessage, WebSocketUpgrade};
        use axum::extract::State;
        use axum::http::{HeaderMap, StatusCode};
        use axum::response::{IntoResponse, Response};
        use axum::routing::{delete, get, post};
        use axum::{Json, Router};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        const STUB_SID: &str = "S-reconnect";
        const STUB_TOKEN: &str = "reconnect-token";

        /// What the stub does with one ③ upgrade request.
        #[derive(Clone, Copy)]
        pub(super) enum WsBehavior {
            /// Accept, then close at once — the ③ drop this module exists for.
            CloseImmediately,
            /// Accept, deliver one raw server frame, then close.
            SendThenClose(&'static str),
            /// Accept and keep the socket open for the rest of the test.
            Hold,
            /// Answer 500 instead of upgrading — one failed ③ re-upgrade.
            Refuse,
        }

        /// The body the stub answers each handover-surface route with,
        /// and what the client presented to get it.
        ///
        /// The pair is the point: what a test needs to see is that a read
        /// carried a Bearer, that the acquire carried none, and that the
        /// caller got these bytes back unaltered either way.
        pub(super) struct StubRequest {
            /// Which route was hit.
            pub label: &'static str,
            /// The `Authorization` header as received — `None` when the
            /// request carried none.
            pub authorization: Option<String>,
            /// The query string (GET) or the request body (POST).
            pub detail: String,
        }

        /// A seat list with one held and one vacant seat. The vacant one
        /// carries `holder: null` **as a key**, which is the property a
        /// verbatim proxy has to preserve.
        pub(super) const ASSIGNEES_BODY: &str = r#"{"run_id":"R-stub","generation":2,"seats":[{"slot":"phase_a_op","vacant":false,"holder":{"op":"S-held","desc":"driving phase a","gen":2},"declared":true},{"slot":"phase_b_op","vacant":true,"holder":null,"declared":true}],"seats_source":"blueprint","note":null}"#;

        /// A four-axis snapshot: the seats above, a trace reference, and
        /// one un-answered step whose `final_present` is the field a
        /// re-run decision hangs on.
        pub(super) const HANDOVER_BODY: &str = r#"{"run_id":"R-stub","generation":2,"trace":{"route":"/v1/runs/R-stub/trace","latest_seq":41},"seats":[{"slot":"phase_a_op","vacant":true,"holder":null,"declared":true}],"seats_source":"blueprint","note":null,"unanswered":[{"slot":"phase_a_op","op":null,"generation":null,"req_id":"rq-1","kind":"spawn","step_id":"ST-7","attempt":1,"final_present":true,"final_ok":false,"material_route":"/v1/runs/R-stub/material?step_id=ST-7"}],"unread_seats":[]}"#;

        pub(super) const MATERIAL_BODY: &str = r#"{"run_id":"R-stub","run_link":"confirmed","note":null,"payload":{"task_id":"ST-7","attempt":1,"agent":"impl","prompt":"do the thing"},"final_present":true,"final_ok":false}"#;

        /// **Q4**'s three: the generation the acquirer dispatches under,
        /// the holder it displaced, and what became of that holder's
        /// in-flight requests.
        pub(super) const ACQUIRE_BODY: &str = r#"{"run_id":"R-stub","slot":"phase_a_op","gen":3,"previous":{"op":"S-held","desc":"driving phase a","gen":2},"t_discard":{"discarded":2,"not_discarded":"requests with no run and requests with no seat"}}"#;

        struct StubState {
            script: Vec<WsBehavior>,
            upgrades: AtomicUsize,
            authorized_upgrades: AtomicUsize,
            healthy: AtomicBool,
            /// Every handover-surface request received, in order.
            /// `std::sync` rather than `tokio`: the handlers touch it
            /// without awaiting, and so do the assertions.
            handover_requests: std::sync::Mutex<Vec<StubRequest>>,
        }

        pub(super) struct StubServer {
            base: String,
            state: Arc<StubState>,
            task: tokio::task::JoinHandle<()>,
        }

        async fn ws_stub(
            State(state): State<Arc<StubState>>,
            headers: HeaderMap,
            ws: WebSocketUpgrade,
        ) -> Response {
            let index = state.upgrades.fetch_add(1, Ordering::SeqCst);
            let expected = format!("Bearer {STUB_TOKEN}");
            if headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                == Some(expected.as_str())
            {
                state.authorized_upgrades.fetch_add(1, Ordering::SeqCst);
            }
            match state.script.get(index).copied() {
                Some(WsBehavior::CloseImmediately) => ws.on_upgrade(|socket| async move {
                    let _ = socket.close().await;
                }),
                Some(WsBehavior::SendThenClose(text)) => {
                    ws.on_upgrade(move |mut socket| async move {
                        let _ = socket.send(AxumMessage::Text(text.to_string())).await;
                        let _ = socket.close().await;
                    })
                }
                Some(WsBehavior::Hold) => ws
                    .on_upgrade(|mut socket| async move { while socket.recv().await.is_some() {} }),
                Some(WsBehavior::Refuse) | None => {
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }

        /// Records one handover-surface request and answers with the
        /// canned body for that route.
        fn answer(
            state: &Arc<StubState>,
            label: &'static str,
            headers: &HeaderMap,
            detail: String,
            body: &'static str,
        ) -> Response {
            state
                .handover_requests
                .lock()
                .expect("stub request log")
                .push(StubRequest {
                    label,
                    authorization: headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string),
                    detail,
                });
            (
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response()
        }

        impl StubServer {
            /// Binds an ephemeral port and starts serving `script`.
            pub(super) async fn start(script: Vec<WsBehavior>) -> Self {
                let state = Arc::new(StubState {
                    script,
                    upgrades: AtomicUsize::new(0),
                    authorized_upgrades: AtomicUsize::new(0),
                    healthy: AtomicBool::new(true),
                    handover_requests: std::sync::Mutex::new(Vec::new()),
                });
                let app = Router::new()
                    .route(
                        "/v1/operators",
                        post(|| async {
                            Json(serde_json::json!({
                                "sid": STUB_SID,
                                "token": STUB_TOKEN,
                            }))
                        }),
                    )
                    .route(
                        "/v1/healthz",
                        get(|State(state): State<Arc<StubState>>| async move {
                            if state.healthy.load(Ordering::SeqCst) {
                                "ok".into_response()
                            } else {
                                StatusCode::SERVICE_UNAVAILABLE.into_response()
                            }
                        }),
                    )
                    .route(
                        "/v1/operators/:sid",
                        delete(|| async { StatusCode::NO_CONTENT }),
                    )
                    .route("/v1/operators/:sid/ws", get(ws_stub))
                    .route(
                        "/v1/runs/:id/assignees",
                        get(
                            |State(state): State<Arc<StubState>>, headers: HeaderMap| async move {
                                answer(&state, "assignees", &headers, String::new(), ASSIGNEES_BODY)
                            },
                        ),
                    )
                    .route(
                        "/v1/runs/:id/handover",
                        get(
                            |State(state): State<Arc<StubState>>, headers: HeaderMap| async move {
                                answer(&state, "handover", &headers, String::new(), HANDOVER_BODY)
                            },
                        ),
                    )
                    .route(
                        "/v1/runs/:id/material",
                        get(
                            |State(state): State<Arc<StubState>>,
                             axum::extract::RawQuery(query): axum::extract::RawQuery,
                             headers: HeaderMap| async move {
                                answer(
                                    &state,
                                    "material",
                                    &headers,
                                    query.unwrap_or_default(),
                                    MATERIAL_BODY,
                                )
                            },
                        ),
                    )
                    .route(
                        "/v1/runs/:id/acquire",
                        post(
                            |State(state): State<Arc<StubState>>,
                             headers: HeaderMap,
                             body: String| async move {
                                answer(&state, "acquire", &headers, body, ACQUIRE_BODY)
                            },
                        ),
                    )
                    .with_state(state.clone());
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind reconnect stub server");
                let base = format!("http://{}", listener.local_addr().unwrap());
                let task = tokio::spawn(async move {
                    let _ = axum::serve(listener, app).await;
                });
                Self { base, state, task }
            }

            /// HTTP root to hand to [`OperatorClientState::with_http_base`].
            pub(super) fn base(&self) -> String {
                self.base.clone()
            }

            /// ③ upgrade requests received so far, refused ones included.
            pub(super) fn upgrades(&self) -> usize {
                self.state.upgrades.load(Ordering::SeqCst)
            }

            /// Of those, how many carried the minted Bearer token.
            pub(super) fn authorized_upgrades(&self) -> usize {
                self.state.authorized_upgrades.load(Ordering::SeqCst)
            }

            /// The handover-surface request the stub received last, or
            /// `None` when it received none.
            pub(super) fn last_handover_request(&self) -> Option<StubRequest> {
                let log = self
                    .state
                    .handover_requests
                    .lock()
                    .expect("stub request log");
                log.last().map(|request| StubRequest {
                    label: request.label,
                    authorization: request.authorization.clone(),
                    detail: request.detail.clone(),
                })
            }

            /// Flips ② `GET /v1/healthz` between `200 ok` and `503`.
            pub(super) fn set_healthy(&self, healthy: bool) {
                self.state.healthy.store(healthy, Ordering::SeqCst);
            }

            pub(super) fn shutdown(self) {
                self.task.abort();
            }
        }
    }

    use stub::{StubServer, WsBehavior};

    // ─── the handover surface (§4.3 / §4.5 / W5) ─────────────────────────
    //
    // What these pin down is the asymmetry the routes were built with: the
    // three reads present a Bearer (which is why an AI can reach them at
    // all — the token lives only in this process), and the acquire
    // presents none (**B2**: the bearer takes no part in assignment).
    // Plus: every one of the four hands the server's body back untouched.

    fn run_id(id: &str) -> RunId {
        RunId::parse(id.to_string()).expect("valid run id")
    }

    /// A joined stub server whose ③ socket stays up, ready for the HTTP
    /// reads to present its token.
    async fn joined_stub() -> (StubServer, OperatorClientState, String) {
        let stub = StubServer::start(vec![WsBehavior::Hold]).await;
        let state = OperatorClientState::with_http_base(stub.base());
        let sid = state.join(None, None).await.expect("join");
        (stub, state, sid)
    }

    #[tokio::test]
    async fn run_assignees_presents_a_bearer_and_returns_the_body_verbatim() {
        let (stub, state, sid) = joined_stub().await;
        let body = state
            .run_assignees(Some(&sid), &run_id("R-stub"))
            .await
            .expect("the read succeeds");

        let request = stub.last_handover_request().expect("the stub was called");
        assert_eq!(request.label, "assignees");
        assert!(
            request.authorization.is_some(),
            "the route is Bearer-gated, and presenting the token is the whole reason this tool \
             exists — without it the AI gets the 401 it got before"
        );
        assert_eq!(
            body,
            serde_json::from_str::<Value>(stub::ASSIGNEES_BODY).unwrap(),
            "the server's body is handed back untouched"
        );
        let vacant = &body["seats"][1];
        assert_eq!(vacant["vacant"], true);
        assert!(
            vacant.get("holder").is_some() && vacant["holder"].is_null(),
            "a vacant seat keeps its explicit null holder: dropping the key would make \
             'nobody is on this' read as 'holders were not reported': {vacant}"
        );
        stub.shutdown();
    }

    #[tokio::test]
    async fn run_handover_returns_the_whole_snapshot_as_one_value() {
        let (stub, state, sid) = joined_stub().await;
        let body = state
            .run_handover(Some(&sid), &run_id("R-stub"))
            .await
            .expect("the read succeeds");

        let request = stub.last_handover_request().expect("the stub was called");
        assert_eq!(request.label, "handover");
        assert!(
            request.authorization.is_some(),
            "Bearer-gated, like /assignees"
        );
        assert_eq!(
            body,
            serde_json::from_str::<Value>(stub::HANDOVER_BODY).unwrap(),
            "one read, one value: re-assembling the axes client-side is what the single \
             server-side read exists to prevent"
        );
        // The two fields a next-action decision is actually made on.
        assert_eq!(body["trace"]["latest_seq"], 41);
        assert_eq!(body["unanswered"][0]["final_present"], true);
        stub.shutdown();
    }

    #[tokio::test]
    async fn run_material_sends_the_step_id_and_presents_a_bearer() {
        let (stub, state, sid) = joined_stub().await;
        let step_id = StepId::parse("ST-7".to_string()).expect("valid step id");
        let body = state
            .run_material(Some(&sid), &run_id("R-stub"), &step_id)
            .await
            .expect("the read succeeds");

        let request = stub.last_handover_request().expect("the stub was called");
        assert_eq!(request.label, "material");
        assert_eq!(
            request.detail, "step_id=ST-7",
            "the route answers about one step, so the id has to reach it"
        );
        assert!(
            request.authorization.is_some(),
            "Bearer-gated, like /assignees"
        );
        assert_eq!(
            body,
            serde_json::from_str::<Value>(stub::MATERIAL_BODY).unwrap()
        );
        stub.shutdown();
    }

    /// **B2** / **B3**: taking a seat is ungated, and this client must not
    /// quietly gate it. A bearer here would make the token decide who
    /// holds a seat — and would make an acquire impossible from a process
    /// holding no session, which is exactly the driver that has to be able
    /// to take over.
    #[tokio::test]
    async fn run_acquire_presents_no_bearer_and_reports_what_it_displaced() {
        let stub = StubServer::start(vec![]).await;
        let state = OperatorClientState::with_http_base(stub.base());
        assert_eq!(
            state.sole_live_sid().await,
            None,
            "no session was joined: an acquire must work anyway"
        );

        let body = state
            .run_acquire(
                &run_id("R-stub"),
                "S-taker",
                "picking up the stalled fix",
                None,
            )
            .await
            .expect("an acquire needs no session of its own");

        let request = stub.last_handover_request().expect("the stub was called");
        assert_eq!(request.label, "acquire");
        assert!(
            request.authorization.is_none(),
            "the bearer must take no part in assignment; got: {:?}",
            request.authorization
        );
        let sent: Value = serde_json::from_str(&request.detail).expect("the body is JSON");
        assert_eq!(sent["op"], "S-taker");
        assert_eq!(sent["desc"], "picking up the stalled fix");
        assert!(
            sent["slot"].is_null(),
            "an unnamed seat is left to the server's rule"
        );

        assert_eq!(
            body,
            serde_json::from_str::<Value>(stub::ACQUIRE_BODY).unwrap(),
            "Q4 / Q5 are the response's own fields — a client that summarised them would \
             hide who was displaced"
        );
        assert_eq!(body["gen"], 3);
        assert_eq!(body["previous"]["op"], "S-held");
        assert_eq!(body["t_discard"]["discarded"], 2);
        stub.shutdown();
    }

    #[tokio::test]
    async fn run_acquire_names_the_seat_when_the_caller_does() {
        let stub = StubServer::start(vec![]).await;
        let state = OperatorClientState::with_http_base(stub.base());
        state
            .run_acquire(
                &run_id("R-stub"),
                "S-taker",
                "taking phase b",
                Some("phase_b_op"),
            )
            .await
            .expect("acquire");

        let request = stub.last_handover_request().expect("the stub was called");
        let sent: Value = serde_json::from_str(&request.detail).expect("the body is JSON");
        assert_eq!(sent["slot"], "phase_b_op");
        stub.shutdown();
    }

    /// The reads fail loudly rather than picking a session for the caller
    /// — the same rule `list_operators` follows, now shared by all four
    /// through `bearer_sid`.
    #[tokio::test]
    async fn a_bearer_read_without_a_sid_fails_when_this_process_holds_no_session() {
        let state = OperatorClientState::with_http_base("http://127.0.0.1:1");
        for error in [
            state.run_assignees(None, &run_id("R-x")).await.unwrap_err(),
            state.run_handover(None, &run_id("R-x")).await.unwrap_err(),
            state
                .run_material(
                    None,
                    &run_id("R-x"),
                    &StepId::parse("ST-1".to_string()).unwrap(),
                )
                .await
                .unwrap_err(),
            state.list_operators(None, None).await.unwrap_err(),
        ] {
            assert!(
                matches!(error, ClientError::UnknownSid(_)),
                "got: {error:?} — a read must not guess whose token to present"
            );
        }
    }

    #[tokio::test]
    async fn a_bearer_read_with_an_unknown_sid_errors_before_any_request() {
        let state = OperatorClientState::with_http_base("http://127.0.0.1:1");
        let error = state
            .run_handover(Some("S-nope"), &run_id("R-x"))
            .await
            .unwrap_err();
        assert!(matches!(error, ClientError::UnknownSid(s) if s == "S-nope"));
    }

    /// A non-2xx answer surfaces with its status and body rather than as a
    /// silently empty snapshot.
    #[tokio::test]
    async fn a_failed_read_reports_the_status_and_body() {
        let stub = StubServer::start(vec![WsBehavior::Hold]).await;
        let state = OperatorClientState::with_http_base(stub.base());
        let sid = state.join(None, None).await.expect("join");
        // `/v1/runs/:id/steps` is not a route this stub serves, so the
        // reqwest call gets a real 404 off the same server.
        let error = state
            .get_as_operator(Some(&sid), &format!("{}/v1/runs/R-stub/nope", stub.base()))
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(matches!(error, ClientError::Http(_)), "got: {message}");
        assert!(message.contains("404"), "the status is named: {message}");
        stub.shutdown();
    }

    /// Blocks until `sid`'s ③ reader task has observed the drop. The close
    /// travels over a real socket, so "the server closed it" and "this
    /// process noticed" are separate moments and the second one is what the
    /// reconnect path keys on.
    async fn await_reader_finished(state: &OperatorClientState, sid: &str) {
        let entry = state.get_entry(sid).await.expect("session present");
        for _ in 0..200 {
            if entry.reader_task.lock().await.is_finished() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("③ reader task never finished");
    }

    #[tokio::test]
    async fn pending_wait_reconnects_when_the_reader_task_has_finished() {
        let stub = StubServer::start(vec![WsBehavior::CloseImmediately, WsBehavior::Hold]).await;
        let state = OperatorClientState::with_http_base(stub.base());
        let sid = state.join(None, None).await.expect("join");
        await_reader_finished(&state, &sid).await;

        let frame = state
            .pending_wait(&sid, 20)
            .await
            .expect("a re-established ③ long-polls as usual");
        assert!(frame.is_none(), "nothing was sent, so the poll times out");
        assert_eq!(
            stub.upgrades(),
            2,
            "the dropped ③ must be repaired by a second upgrade"
        );
        assert_eq!(
            stub.authorized_upgrades(),
            2,
            "the re-upgrade reuses the sid+token minted by ②, not a new session"
        );
        let entry = state.get_entry(&sid).await.expect("session kept");
        assert!(
            !entry.reader_task.lock().await.is_finished(),
            "a fresh reader task is in place"
        );
        stub.shutdown();
    }

    #[tokio::test]
    async fn reconnect_declines_without_a_ws_attempt_when_healthz_is_down() {
        let stub = StubServer::start(vec![WsBehavior::CloseImmediately]).await;
        let state = OperatorClientState::with_http_base(stub.base());
        let sid = state.join(None, None).await.expect("join");
        await_reader_finished(&state, &sid).await;
        stub.set_healthy(false);

        let error = state
            .pending_wait(&sid, 20)
            .await
            .expect_err("a ② that is not answering closes the sid");
        assert!(
            matches!(error, ClientError::SessionClosed(_)),
            "got: {error:?}"
        );
        assert_eq!(
            stub.upgrades(),
            1,
            "only join's upgrade — a down ② must not be handed a single ③ retry"
        );
        let entry = state.get_entry(&sid).await.expect("session kept");
        assert_eq!(
            entry.reconnect.lock().await.attempts,
            0,
            "the attempt spent none of its ③ budget"
        );
        stub.shutdown();
    }

    #[tokio::test]
    async fn frames_delivered_before_the_drop_survive_the_reconnect() {
        let stub = StubServer::start(vec![
            WsBehavior::SendThenClose(
                r#"{"type":"ask","req_id":"r-kept","task_id":"ST-1","question":{"q":"?"}}"#,
            ),
            WsBehavior::Hold,
        ])
        .await;
        let state = OperatorClientState::with_http_base(stub.base());
        let sid = state.join(None, None).await.expect("join");
        await_reader_finished(&state, &sid).await;
        let entry = state.get_entry(&sid).await.expect("session kept");
        assert_eq!(
            entry.pending.items.lock().await.len(),
            1,
            "the frame arrived before the drop and was never popped"
        );

        let frame = state
            .pending_wait(&sid, 200)
            .await
            .expect("reconnect succeeds")
            .expect("the queued frame is still there afterwards");
        assert_eq!(frame.req_id, "r-kept");
        assert_eq!(stub.upgrades(), 2, "the reconnect did happen");
        stub.shutdown();
    }

    #[tokio::test]
    async fn three_failed_ws_upgrades_end_the_call_but_leave_the_sid_retryable() {
        let stub = StubServer::start(vec![
            WsBehavior::CloseImmediately, // join, then the drop
            WsBehavior::Refuse,           // call 1: 1/3 fails
            WsBehavior::Refuse,           // call 1: 2/3 fails
            WsBehavior::Refuse,           // call 1: 3/3 fails, budget gone
            WsBehavior::Refuse,           // call 2: 1/3 fails
            WsBehavior::Refuse,           // call 2: 2/3 fails
            WsBehavior::Hold,             // call 2: 3/3 succeeds
        ])
        .await;
        let state = OperatorClientState::with_http_base(stub.base());
        let sid = state.join(None, None).await.expect("join");
        await_reader_finished(&state, &sid).await;
        let entry = state.get_entry(&sid).await.expect("session kept");

        // Call 1: the server refuses every upgrade, so this call runs out
        // of budget and reports it.
        let error = state
            .pending_wait(&sid, 20)
            .await
            .expect_err("a ③ that will not come back ends the call");
        assert!(
            matches!(error, ClientError::SessionClosed(_)),
            "got: {error:?}"
        );
        assert_eq!(
            stub.upgrades(),
            1 + MAX_RECONNECT_ATTEMPTS as usize,
            "the call spends exactly its budget, no more"
        );
        assert_eq!(
            entry.reconnect.lock().await.attempts,
            0,
            "giving up clears the counter — the budget was this call's, not the sid's"
        );

        // Call 2: same sid, full budget again. This is the case that
        // matters — a server that was down during call 1 and is back now
        // must be re-attached without the driver re-joining.
        assert!(state
            .pending_wait(&sid, 20)
            .await
            .expect("the sid is still good, so the next call reconnects")
            .is_none());
        assert_eq!(
            stub.upgrades(),
            1 + 2 * MAX_RECONNECT_ATTEMPTS as usize,
            "call 2 was allowed its own three attempts"
        );
        assert!(
            !entry.reader_task.lock().await.is_finished(),
            "③ is live again on the original sid"
        );
        assert_eq!(entry.reconnect.lock().await.attempts, 0);
        stub.shutdown();
    }
}
