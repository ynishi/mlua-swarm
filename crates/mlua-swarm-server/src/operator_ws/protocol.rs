//! Wire format (= S↔C JSON message schema) for `WS /v1/operators/:sid/ws`.
//!
//! `ServerMsg` = 4 messages the server pushes to the client (Ask / HookBefore /
//! HookAfter / Spawn).
//! `ClientMsg` = 4 messages the client replies with (Answer / HookAck / SpawnAck / SpawnHalt).
//!
//! ## Layer 2 audit (issue #7): halt vs error separation across verbs
//!
//! `spawn_halt` was added to disambiguate a controlled halt from a real
//! worker error on the `spawn_ack` axis. The other client verbs were
//! audited for the same shape:
//!
//! - **`answer`** (SeniorBridge.ask reply): carries `value` only — no
//!   ok / failure axis exists. A "halt during a question" would already
//!   be expressible by replying with a value that the middleware treats
//!   as an abort signal (e.g. `null` or a domain-specific sentinel).
//!   No sibling `answer_halt` is added.
//! - **`hook_ack`** (SpawnHook.before OK/NG): `ok = false` here is a
//!   genuine gate rejection — that is the entire purpose of the hook,
//!   not a mix-signal. There is no confusion to fix; no `hook_halt` is
//!   added.
//! - **`spawn_ack`**: the subject of layer 1. `spawn_halt` handles it.
//! For the parent module's message-flow figure, see the doc of `mod.rs`.
//!
//! `PendingReply` is the intermediate representation delivered over the internal
//! `oneshot` reply channel, used to resolve a `ClientMsg` (arriving from a client)
//! against the session's pending `HashMap` keyed by `req_id`.
//! See `session::WSOperatorSession::resolve_pending` for details.

use mlua_swarm::{StepId, WorkerBinding};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// parent_req_id schema field carry: the engine middleware does not fire the
// true nested-ask case (another ask running mid-ask), so this stays None.
// The field is kept for schema compatibility; when a middleware extension
// starts firing the true nested case, it can be reintroduced via task_local
// or similar.
pub(super) fn current_parent_req_id() -> Option<String> {
    None
}

pub(super) fn default_ok_true() -> bool {
    true
}

/// Server → client push messages on `WS /v1/operators/:sid/ws`. Each variant
/// pairs with a `ClientMsg` reply carrying the same `req_id` (except
/// `HookAfter`, which is fire-and-forget).
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// `SeniorBridge.ask` request.
    Ask {
        /// Correlation key the client must echo back in `ClientMsg::Answer`.
        req_id: String,
        /// Reserved for nested-ask correlation; currently always `None`
        /// (see [`current_parent_req_id`]).
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_req_id: Option<String>,
        /// Task the question originates from. Typed [`StepId`] since issue
        /// #14 — serde keeps the wire shape a plain string.
        task_id: StepId,
        /// Free-form question payload produced by the engine middleware.
        question: Value,
    },
    /// `SpawnHook.before` request (= the client returns OK / NG via ack).
    HookBefore {
        /// Correlation key the client must echo back in `ClientMsg::HookAck`.
        req_id: String,
        /// Reserved for nested-ask correlation; currently always `None`.
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_req_id: Option<String>,
        /// Task whose spawn is being gated.
        task_id: StepId,
        /// Agent ref about to be spawned.
        agent: String,
        /// 1-based dispatch attempt counter for this agent step.
        attempt: u32,
    },
    /// `SpawnHook.after` notification (= no client ack, fire-and-forget).
    HookAfter {
        /// Correlation key (informational only — no reply is expected).
        req_id: String,
        /// Reserved for nested-ask correlation; currently always `None`.
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_req_id: Option<String>,
        /// Task the spawn belonged to.
        task_id: StepId,
        /// Agent ref that was spawned.
        agent: String,
        /// 1-based dispatch attempt counter for this agent step.
        attempt: u32,
        /// Worker result payload observed after the spawn completed.
        result: Value,
    },
    /// `Operator.execute` request (= delegates the whole spawn to an external
    /// Operator, via `OperatorDelegateMiddleware`). The client replies with the
    /// `WorkerResult`-equivalent (= value + ok) in `spawn_ack`.
    ///
    /// **Thin control channel** (the Spawn thin-control axis): the server sends only
    /// the `capability_token`. `system_prompt` / `prompt` are NOT carried in the
    /// WS payload. The MainAI (WS Client) forwards the token to the SubAgent,
    /// and the SubAgent hits `/v1/worker/prompt` + `/v1/worker/result` itself
    /// with `Authorization: Bearer <capability_token>` — fetching prompt /
    /// system and posting the result (= heavy payloads go over HTTP; WS stays
    /// purely thin control).
    ///
    /// `capability_token` is `CapToken::encode()` form (= URL-safe base64 of
    /// serde_json): a session token with `Role::Worker` + `["*"]` scopes + 600s
    /// TTL. The HMAC sig is verified server-side by `verify_token_for_task` —
    /// a self-contained capability token (= no server lookup required).
    ///
    /// `directive` (= immediate instruction for the MainAI; fix for observation #7):
    /// Under thin-push discipline, if the payload were only routing fields, the
    /// MainAI (a large LLM) would fire the drift "I have a token → I should
    /// fetch it myself" / "I got the prompt → I should embed it literally into
    /// the SubAgent" 100% of the time (= bias accumulation across 50–100 parallel
    /// agents dulls decisions). To structurally remove this drift, a literal
    /// instruction text — "launch a SubAgent, hand it the token + endpoint, and
    /// let the SubAgent do the fetch / execution / post" — is explicitly embedded
    /// into the payload (= implicit convention → literal statement).
    ///
    /// This field carries **natural-language text intended for the MainAI to read**
    /// (= not a JSON schema target for parsing). See
    /// `operator_ws::session::default_spawn_directive()` for the server-side
    /// default text.
    Spawn {
        /// Correlation key the client must echo back in `ClientMsg::SpawnAck`.
        req_id: String,
        /// Reserved for nested-ask correlation; currently always `None`.
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_req_id: Option<String>,
        /// Task the delegated spawn belongs to.
        task_id: StepId,
        /// Agent ref the Operator is asked to execute.
        agent: String,
        /// 1-based dispatch attempt counter for this agent step.
        attempt: u32,
        /// `CapToken::encode()` form Bearer credential for the worker HTTP
        /// endpoints (see the variant doc above for the thin-control contract).
        capability_token: String,
        /// Short handle (= `wh-XXXXXXXX`, 12 chars). An alternate Bearer path
        /// paired with `capability_token`. When `/v1/worker/submit` receives a
        /// handle in Bearer, the server resolves nonce → `task_id` via the
        /// `worker_handles` map (the short-handle switchover — removes
        /// base64 copy-paste accidents). SubAgents (mse-worker) should use
        /// **this field** instead of `capability_token` as the recommended path.
        #[serde(skip_serializing_if = "Option::is_none")]
        worker_handle: Option<String>,
        /// Worker binding resolved from the Blueprint at compile time. `None`
        /// never reaches the wire on the WS thin path (compile-time gate,
        /// see `Operator::requires_worker_binding`), but the field stays
        /// optional for forward compatibility.
        #[serde(skip_serializing_if = "Option::is_none")]
        worker: Option<WorkerBinding>,
        /// Literal natural-language instruction for the MainAI (see the
        /// variant doc above for why this is embedded in the payload).
        directive: String,
    },
}

/// Client → server reply messages on `WS /v1/operators/:sid/ws`. Each variant
/// resolves the pending oneshot registered under its `req_id`
/// (see [`super::session::WSOperatorSession::resolve_pending`]).
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    /// Reply to `ServerMsg::Ask` (`SeniorBridge.ask` result).
    Answer {
        /// Correlation key copied from the originating `ServerMsg::Ask`.
        req_id: String,
        /// Answer payload returned to the engine middleware.
        value: Value,
    },
    /// Ack for `SpawnHook.before`. `ok=false` rejects the spawn
    /// (= `MainAIMiddleware` converts it into `SpawnError::RejectedByMiddleware`).
    /// `reason` propagates as `Err(reason)`.
    HookAck {
        /// Correlation key copied from the originating `ServerMsg::HookBefore`.
        req_id: String,
        /// `true` allows the spawn; `false` rejects it.
        ok: bool,
        /// Optional rejection reason surfaced to the engine when `ok=false`.
        #[serde(default)]
        reason: Option<String>,
    },
    /// Ack for `Operator.execute` (Spawn). `value = WorkerResult.value`,
    /// `ok = WorkerResult.ok`. When `error` is `Some`, the `Operator` returns
    /// it as `WorkerError`.
    ///
    /// After the thin-path switch (= the thin-control axis): if the MainAI returns this ack
    /// **after** the SubAgent has hit HTTP `/v1/worker/result`, the server-side
    /// dispatch path can complete with both the `Final` in `output_tail` and
    /// this ack's `value` aligned. Sending an empty JSON `{}` for `value` makes
    /// the `task.last_result` written by the HTTP path (= `post_result`)
    /// canonical (= the ack-side `value` is duplicate / informational).
    SpawnAck {
        /// Correlation key copied from the originating `ServerMsg::Spawn`.
        req_id: String,
        /// `WorkerResult.value` equivalent; empty `{}` defers to the HTTP-path
        /// result (see the variant doc above).
        #[serde(default)]
        value: Value,
        /// `WorkerResult.ok` equivalent; defaults to `true` when omitted.
        #[serde(default = "default_ok_true")]
        ok: bool,
        /// When `Some`, the Operator surfaces it as a `WorkerError`.
        #[serde(default)]
        error: Option<String>,
    },
    /// Controlled halt for the current spawn (issue #7). Distinct from
    /// `SpawnAck { ok: false, error: Some(_) }`, which is the fail-loud
    /// path for real worker errors. `spawn_halt` signals the operator's
    /// intent to end the current spawn as a normal termination:
    ///
    /// - The step return value is `WorkerResult { value: <halt marker>,
    ///   ok: true }` — no `WorkerError` is raised, so log level stays
    ///   `info` and downstream retry logic doesn't fire.
    /// - The optional `value` payload is merged into the halt marker
    ///   under `value`, so partial results reach `final_ctx` verbatim.
    /// - `reason` is a human-readable log line.
    ///
    /// The halt marker written to ctx has the shape:
    /// ```json
    /// { "halted": true, "reason": "<reason or null>", "value": <payload or {}> }
    /// ```
    /// Blueprint flows that need to short-circuit downstream steps on
    /// halt can `branch` on `$.<step_out>.halted`.
    ///
    /// **Scope note**: this halts one spawn, not the whole swarm. For
    /// swarm-wide cancellation see `swarm_cancel`.
    SpawnHalt {
        /// Correlation key copied from the originating `ServerMsg::Spawn`.
        req_id: String,
        /// Optional partial ctx value to carry into `WorkerResult.value`.
        /// Merged under the `value` key of the halt marker; defaults to `{}`.
        #[serde(default)]
        value: Value,
        /// Optional human-readable halt reason (for logs). Included in
        /// the halt marker under `reason`.
        #[serde(default)]
        reason: Option<String>,
    },
}

/// Intermediate representation for the session's `req_id` ↔ oneshot reply
/// channel. The resolved form of `ClientMsg` looked up on the session side by
/// `req_id` (= runtime-only, not wire format).
pub(super) enum PendingReply {
    /// Answer (return `Value` of `SeniorBridge.ask`).
    Answer(Value),
    /// `hook_ack` (OK / NG for `before`).
    HookAck { ok: bool, reason: Option<String> },
    /// `spawn_ack` (return of `Operator.execute` = `WorkerResult`-equivalent).
    SpawnAck {
        value: Value,
        ok: bool,
        error: Option<String>,
    },
    /// `spawn_halt` — controlled halt for the current spawn (issue #7).
    /// See the `ClientMsg::SpawnHalt` doc for semantics.
    SpawnHalt {
        value: Value,
        reason: Option<String>,
    },
}
