//! WS client embedding for mse mcp (S3, design
//! (see the WS multi-session design).
//!
//! Owns the in-process `sid → SessionEntry` map backing the 4 MCP tools
//! (`mse_operator_join` / `mse_pending_wait` / `mse_ack` / `mse_operator_leave`,
//! wired in `main.rs`). Each `join()` mints an Operator session via
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
        task_id: String,
        question: Value,
    },
    HookBefore {
        req_id: String,
        #[serde(default)]
        parent_req_id: Option<String>,
        task_id: String,
        agent: String,
        attempt: u32,
    },
    HookAfter {
        req_id: String,
        #[serde(default)]
        parent_req_id: Option<String>,
        task_id: String,
        agent: String,
        attempt: u32,
        result: Value,
    },
    Spawn {
        req_id: String,
        #[serde(default)]
        parent_req_id: Option<String>,
        task_id: String,
        agent: String,
        attempt: u32,
        capability_token: String,
        #[serde(default)]
        worker_handle: Option<String>,
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

struct SessionEntry {
    token: String,
    writer: Mutex<WsSink>,
    pending: Arc<PendingQueue>,
    reader_task: JoinHandle<()>,
}

/// Errors surfaced to the MCP tool layer (mapped to `McpError` in `main.rs`).
#[derive(Debug)]
pub enum ClientError {
    UnknownSid(String),
    Http(String),
    Ws(String),
    InvalidAckKind(String),
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
        }
    }
}

impl std::error::Error for ClientError {}

/// Owns all live `sid → SessionEntry` state for the mse mcp process. One
/// instance is shared (`Arc`) across all 4 tool handlers in `main.rs`.
pub struct OperatorClientState {
    sessions: Mutex<HashMap<String, Arc<SessionEntry>>>,
    http_base: String,
}

impl OperatorClientState {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            http_base: resolve_http_base(),
        }
    }

    #[cfg(test)]
    fn with_http_base(http_base: impl Into<String>) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            http_base: http_base.into(),
        }
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
    /// returned to the caller. Returns `(sid, roles)`.
    pub async fn join(&self, roles: Vec<String>) -> Result<(String, Vec<String>), ClientError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| ClientError::Http(e.to_string()))?;
        let resp = client
            .post(format!("{}/v1/operators", self.http_base))
            .json(&serde_json::json!({ "roles": roles }))
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
        let resolved_roles: Vec<String> = body["roles"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let ws_url = format!(
            "{}/v1/operators/{}/ws",
            http_to_ws_base(&self.http_base),
            sid
        );
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
        let (writer, reader) = ws_stream.split();

        let pending = Arc::new(PendingQueue::new());
        let reader_task = spawn_reader(reader, pending.clone());

        let entry = Arc::new(SessionEntry {
            token,
            writer: Mutex::new(writer),
            pending,
            reader_task,
        });
        self.sessions.lock().await.insert(sid.clone(), entry);
        Ok((sid, resolved_roles))
    }

    /// Pops one pending frame for `sid`, waiting up to `timeout_ms`.
    /// `Ok(None)` = timed out with nothing delivered.
    pub async fn pending_wait(
        &self,
        sid: &str,
        timeout_ms: u64,
    ) -> Result<Option<PendingFrame>, ClientError> {
        let entry = self.get_entry(sid).await?;
        Ok(entry.pending.wait(Duration::from_millis(timeout_ms)).await)
    }

    /// Sends the `ClientMsg` corresponding to `kind` over `sid`'s WS
    /// connection. `kind` validation happens before the session lookup, so an
    /// invalid `kind` fails the same way regardless of whether `sid` exists.
    pub async fn ack(
        &self,
        sid: &str,
        req_id: String,
        kind: &str,
        value: Option<Value>,
        ok: bool,
        error: Option<String>,
    ) -> Result<(), ClientError> {
        let msg = build_client_msg(kind, req_id, value, ok, error)?;
        let entry = self.get_entry(sid).await?;
        let text = serde_json::to_string(&msg).map_err(|e| ClientError::Ws(e.to_string()))?;
        let result = entry
            .writer
            .lock()
            .await
            .send(Message::Text(text))
            .await
            .map_err(|e| ClientError::Ws(e.to_string()));
        result
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
        entry.reader_task.abort();

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

impl Default for OperatorClientState {
    fn default() -> Self {
        Self::new()
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
        let text = r#"{"type":"ask","req_id":"r1","task_id":"t1","question":{"q":"?"}}"#;
        let frame = parse_server_frame(text).expect("should parse");
        assert_eq!(frame.req_id, "r1");
        assert_eq!(frame.kind, "ask");
        assert_eq!(frame.payload["task_id"], "t1");
        assert_eq!(frame.payload["question"], serde_json::json!({"q": "?"}));
        assert!(frame.payload.get("type").is_none(), "type key stripped");
    }

    #[test]
    fn parse_server_frame_hook_before() {
        let text = r#"{"type":"hook_before","req_id":"r2","task_id":"t1","agent":"a","attempt":1}"#;
        let frame = parse_server_frame(text).expect("should parse");
        assert_eq!(frame.req_id, "r2");
        assert_eq!(frame.kind, "hook_before");
        assert_eq!(frame.payload["agent"], "a");
        assert_eq!(frame.payload["attempt"], 1);
    }

    #[test]
    fn parse_server_frame_hook_after() {
        let text = r#"{"type":"hook_after","req_id":"r3","task_id":"t1","agent":"a","attempt":2,"result":{"ok":true}}"#;
        let frame = parse_server_frame(text).expect("should parse");
        assert_eq!(frame.req_id, "r3");
        assert_eq!(frame.kind, "hook_after");
        assert_eq!(frame.payload["result"], serde_json::json!({"ok": true}));
    }

    #[test]
    fn parse_server_frame_spawn() {
        let text = r#"{"type":"spawn","req_id":"r4","task_id":"t1","agent":"a","attempt":1,"capability_token":"tok","directive":"do it"}"#;
        let frame = parse_server_frame(text).expect("should parse");
        assert_eq!(frame.req_id, "r4");
        assert_eq!(frame.kind, "spawn");
        assert_eq!(frame.payload["capability_token"], "tok");
        assert_eq!(frame.payload["directive"], "do it");
        assert!(frame.payload.get("worker_handle").is_none());
    }

    #[test]
    fn parse_server_frame_spawn_with_worker_handle() {
        let text = r#"{"type":"spawn","req_id":"r5","task_id":"t1","agent":"a","attempt":1,"capability_token":"tok","worker_handle":"wh-abc","directive":"do it"}"#;
        let frame = parse_server_frame(text).expect("should parse");
        assert_eq!(frame.payload["worker_handle"], "wh-abc");
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
        )
        .expect("valid kind");
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["type"], "answer");
        assert_eq!(v["req_id"], "r1");
        assert_eq!(v["value"], serde_json::json!({"verdict": "ok"}));
    }

    #[test]
    fn build_client_msg_hook_ack_omits_reason_when_none() {
        let msg = build_client_msg("hook_ack", "r2".into(), None, true, None).expect("valid kind");
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
        )
        .expect("valid kind");
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["reason"], "rejected");
    }

    #[test]
    fn build_client_msg_spawn_ack_defaults_value_to_empty_object() {
        let msg = build_client_msg("spawn_ack", "r4".into(), None, true, None).expect("valid kind");
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["type"], "spawn_ack");
        assert_eq!(v["value"], serde_json::json!({}));
    }

    #[test]
    fn build_client_msg_rejects_unknown_kind() {
        let err = build_client_msg("bogus", "r5".into(), None, true, None).unwrap_err();
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
        let msg = build_client_msg("spawn_halt", "r7".into(), None, true, None).expect("valid kind");
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
            .ack("no-such-sid", "r1".into(), "bogus", None, true, None)
            .await
            .unwrap_err();
        assert!(matches!(err, ClientError::InvalidAckKind(k) if k == "bogus"));
    }

    #[tokio::test]
    async fn ack_unknown_sid_errors_for_valid_kind() {
        let state = OperatorClientState::with_http_base("http://127.0.0.1:1");
        let err = state
            .ack("no-such-sid", "r1".into(), "answer", None, true, None)
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
        let err = state.join(vec![]).await.unwrap_err();
        assert!(matches!(err, ClientError::Http(_)), "got: {err:?}");
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
}
