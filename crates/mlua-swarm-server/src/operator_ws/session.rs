//! `WSOperatorSession`: 1 sid = 1 session = 3 traits co-hosted (`SeniorBridge` /
//! `SpawnHook` / `Operator`). Registered simultaneously into 3 registries under
//! the same sid — the canonical pattern where 1 WS connection covers all 3
//! faces of the Operator role (judgment / observation / execution).
//!
//! `tx` is a `Mutex<Option<Sender>>`: `None` on disconnect, swappable to
//! `Some(new_tx)` on reconnect. The `pending` `HashMap` persists on the session
//! side, so a client holding answer/ack values across a disconnect can reconnect
//! and resend them.
//!
//! Disconnect and teardown are separate events. Clearing `tx` says "this
//! socket went away, a reconnect may follow"; `WSOperatorSession::close`
//! says "this session is over" and pushes that fact out to the socket
//! itself (see its doc).
//!
//! For the detailed S↔C message flow, see the overview figure in `mod.rs`.

use async_trait::async_trait;
use mlua_swarm::core::agent_context::{AgentContextView, PROJECTION_PLACEMENT_KEY};
use mlua_swarm::core::projection::{
    FileProjectionAdapter, ProjectionAdapter, ProjectionKey, ProjectionRef,
};
use mlua_swarm::core::projection_placement::ProjectionPlacement;
use mlua_swarm::{
    CapToken, Ctx, Operator, SeniorBridge, SessionId, SpawnHook, StepId, WorkerBinding,
    WorkerError, WorkerResult,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, Mutex};

use super::protocol::{current_parent_req_id, PendingReply, ServerMsg};

/// 1 sid = 1 session. Looked up by sid in the `operator_sessions` store on reconnect.
pub struct WSOperatorSession {
    sid: SessionId,
    /// The current mpsc sender on the write path. `None` on disconnect;
    /// swapped to `Some(new_tx)` on reconnect.
    tx: Mutex<Option<mpsc::UnboundedSender<ServerMsg>>>,
    /// `req_id` → pending oneshot. Resolved when `answer` / `hook_ack` /
    /// `spawn_ack` arrives.
    pending: Mutex<HashMap<String, oneshot::Sender<PendingReply>>>,
    /// Server-initiated close signal for whichever socket is pumping this
    /// session. `None` = none requested; `Some(reason)` is latched by
    /// [`Self::close`] and never cleared — a session is torn down once.
    /// The pump subscribes via [`Self::close_signal`].
    close: watch::Sender<Option<Arc<str>>>,
    /// Public HTTP base URL the server is reachable at (from
    /// `AppState.base_url`, sourced from the binary at boot time).
    /// Rendered literally into the Spawn `directive`'s `base_url` line
    /// when `Some`; `None` falls back to a `mse_doctor`-pointer
    /// placeholder (issue #8).
    base_url: Option<std::sync::Arc<str>>,
}

impl WSOperatorSession {
    /// Test-only shorthand for "a session that already has a sender".
    ///
    /// Production has no such constructor any more: every session is born
    /// disconnected via [`Self::disconnected_with_base_url`] — at mint
    /// (`login::operators_create`) or at boot
    /// (`login::restored_operator_session_entry`) — and acquires its sender
    /// through [`Self::replace_tx`] when a socket attaches. This used to be
    /// the first-connect constructor in `login::handle_operator_socket`;
    /// that arm registered the session it built, which is exactly what a
    /// connect racing a teardown must not be able to do.
    ///
    /// `base_url` is the server's public HTTP root (e.g.
    /// `"http://127.0.0.1:7777"`), threaded from `AppState.base_url`.
    /// When `Some`, it is rendered literally into Spawn directives
    /// (issue #8); `None` falls back to a `mse_doctor`-pointer
    /// placeholder.
    #[cfg(test)]
    pub(super) fn new_with_base_url(
        sid: SessionId,
        tx: mpsc::UnboundedSender<ServerMsg>,
        base_url: Option<std::sync::Arc<str>>,
    ) -> Self {
        Self {
            sid,
            tx: Mutex::new(Some(tx)),
            pending: Mutex::new(HashMap::new()),
            close: watch::Sender::new(None),
            base_url,
        }
    }

    /// Constructor for a session that exists with no socket behind it yet:
    /// the boot-time restore path (`OperatorSessionPersistence::restore`),
    /// which puts a persisted login record into the engine's three
    /// registries before its owning client has reconnected.
    ///
    /// `tx: None` is the very state a live session falls into on
    /// [`Self::clear_tx`] / [`Self::clear_tx_if`], so nothing downstream
    /// needs a new branch: [`Self::send_and_await`] / [`Self::send_oneway`]
    /// answer `"ws operator disconnected"` rather than parking, and
    /// [`Self::is_connected`] answers `false`. Being registered and being
    /// reachable stay separate facts — the client's first connect walks
    /// `login::handle_operator_socket`'s existing reconnect arm and only
    /// swaps the sender in ([`Self::replace_tx`]).
    pub(super) fn disconnected_with_base_url(
        sid: SessionId,
        base_url: Option<std::sync::Arc<str>>,
    ) -> Self {
        Self {
            sid,
            tx: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            close: watch::Sender::new(None),
            base_url,
        }
    }

    /// Swaps in a new tx on reconnect. Expected to be called only from the handler side.
    pub(super) async fn replace_tx(&self, new_tx: mpsc::UnboundedSender<ServerMsg>) {
        *self.tx.lock().await = Some(new_tx);
    }

    /// Whether this session currently has a live WebSocket sender.
    pub(super) async fn is_connected(&self) -> bool {
        self.tx.lock().await.is_some()
    }

    /// Clear the sender only when it still belongs to the connection that is
    /// shutting down. A stale socket can finish after a reconnect installed a
    /// replacement sender; that stale cleanup must not disconnect the new
    /// socket.
    pub(super) async fn clear_tx_if(&self, expected: &mpsc::UnboundedSender<ServerMsg>) {
        let mut current = self.tx.lock().await;
        if current
            .as_ref()
            .is_some_and(|sender| sender.same_channel(expected))
        {
            *current = None;
        }
    }

    /// Clears tx to `None` on disconnect. Expected to be called only from the handler side.
    pub(crate) async fn clear_tx(&self) {
        *self.tx.lock().await = None;
    }

    /// Ask the socket currently pumping this session to shut down, carrying
    /// `reason` to the client.
    ///
    /// Clearing `tx` is not enough to end a connection: the pump
    /// (`login::handle_operator_socket`) holds its own clone of the sender,
    /// so the mpsc channel — and with it the write task and the socket —
    /// stays alive even after the session has let go. A client torn down by
    /// a third party (`DELETE /v1/operators/by-role/:role`) was left holding
    /// a socket nothing would ever speak on again, with no error to notice:
    /// its session was already out of `operator_sessions`, so no frame could
    /// be routed to it and no reconnect could find it.
    ///
    /// Latching this signal is what the pump watches; it answers by sending
    /// a **WS Close frame** (a standard disconnect every client already
    /// understands — no new `ServerMsg` variant, so the wire shape is
    /// unchanged) and ending both of its tasks.
    ///
    /// Called on **teardown** only. A plain disconnect must not fire it:
    /// the two are separate events, and a reconnect is legitimate after the
    /// second but impossible after the first.
    pub(crate) fn close(&self, reason: &str) {
        self.close.send_replace(Some(Arc::from(reason)));
    }

    /// Subscribe to [`Self::close`]. Because the signal is latched rather
    /// than edge-triggered, a subscriber that arrives *after* the teardown
    /// still observes it (see `login::close_requested`) — which is what a
    /// socket that connected while its session was being torn down needs.
    pub(super) fn close_signal(&self) -> watch::Receiver<Option<Arc<str>>> {
        self.close.subscribe()
    }

    /// Drains every in-flight `pending` entry, dropping its oneshot
    /// `Sender`. Dropping the sender closes the reply channel, so the
    /// matching `send_and_await` unparks from `orx.await` with an `Err`
    /// immediately — an in-flight `ask` / `hook_before` / `spawn` fails
    /// loud right away (for `Operator::execute`, a `WorkerError::Failed`)
    /// instead of orphaning in `orx.await` until the run's sync timeout
    /// (up to 300s) fires. `reason` is recorded for the log only — a
    /// dropped `oneshot::Sender` can carry no payload, so the receiver
    /// observes the generic "reply path closed" error.
    ///
    /// Called on **session teardown** (`DELETE /v1/operators/:sid` and
    /// `/by-role`), where the session is being removed from
    /// `operator_sessions` and can never be reconnected — so the
    /// disconnect-survives-in-`pending` reconnect/resend contract (see the
    /// module doc) does not apply. It is deliberately NOT called on a plain
    /// WS disconnect, which keeps `pending` alive for a reconnecting client
    /// to resend against.
    pub(crate) async fn fail_pending(&self, reason: &str) {
        let drained: Vec<(String, oneshot::Sender<PendingReply>)> =
            self.pending.lock().await.drain().collect();
        if !drained.is_empty() {
            tracing::warn!(
                sid = %self.sid,
                count = drained.len(),
                reason,
                "ws operator session: failing in-flight pending replies"
            );
        }
        // `drained` drops here: each `oneshot::Sender` closes its channel,
        // unparking the corresponding `send_and_await` with an `Err`.
    }

    /// Resolves the pending oneshot when a `ClientMsg` arrives on the handler's
    /// read task. If `req_id` is not registered, no-op (= silently drops unknown acks).
    pub(super) async fn resolve_pending(&self, req_id: &str, reply: PendingReply) {
        if let Some(otx) = self.pending.lock().await.remove(req_id) {
            let _ = otx.send(reply);
        }
    }

    /// Inserts an entry into pending, sends S→C, and waits for the reply. No
    /// timeout in v1.5 (= an ask during a disconnect immediately returns `Err`
    /// on send failure; reconnect-wait behavior is v2).
    async fn send_and_await(&self, req_id: String, msg: ServerMsg) -> Result<PendingReply, String> {
        let (otx, orx) = oneshot::channel::<PendingReply>();
        self.pending.lock().await.insert(req_id.clone(), otx);

        // Fetch `tx` and send. When None, we are disconnected — fail fast.
        let send_result = {
            let guard = self.tx.lock().await;
            match guard.as_ref() {
                Some(tx) => tx
                    .send(msg)
                    .map_err(|_| "ws send channel closed".to_string()),
                None => Err("ws operator disconnected".to_string()),
            }
        };
        if let Err(e) = send_result {
            self.pending.lock().await.remove(&req_id);
            return Err(e);
        }

        orx.await
            .map_err(|_| "ws operator: oneshot cancelled (= reply path closed)".to_string())
    }

    /// Fire-and-forget send for `after` (= no reply expected).
    async fn send_oneway(&self, msg: ServerMsg) -> Result<(), String> {
        let guard = self.tx.lock().await;
        match guard.as_ref() {
            Some(tx) => tx
                .send(msg)
                .map_err(|_| "ws send channel closed".to_string()),
            None => Err("ws operator disconnected".to_string()),
        }
    }
}

#[async_trait]
impl SeniorBridge for WSOperatorSession {
    async fn ask(&self, task_id: &StepId, question: Value) -> Result<Value, String> {
        let req_id = format!("{}-ask-{}", self.sid, uuid::Uuid::new_v4());
        let msg = ServerMsg::Ask {
            req_id: req_id.clone(),
            parent_req_id: current_parent_req_id(),
            task_id: task_id.clone(),
            question,
        };
        match self.send_and_await(req_id, msg).await? {
            PendingReply::Answer(v) => Ok(v),
            PendingReply::HookAck { .. } => {
                Err("ws operator: unexpected hook_ack reply to ask".into())
            }
            PendingReply::SpawnAck { .. } => {
                Err("ws operator: unexpected spawn_ack reply to ask".into())
            }
            PendingReply::SpawnHalt { .. } => {
                Err("ws operator: unexpected spawn_halt reply to ask".into())
            }
        }
    }
}

#[async_trait]
impl SpawnHook for WSOperatorSession {
    async fn before(&self, ctx: &Ctx) -> Result<(), String> {
        let req_id = format!("{}-hb-{}", self.sid, uuid::Uuid::new_v4());
        let msg = ServerMsg::HookBefore {
            req_id: req_id.clone(),
            parent_req_id: current_parent_req_id(),
            task_id: ctx.task_id.clone(),
            agent: ctx.agent.clone(),
            attempt: ctx.attempt,
        };
        match self.send_and_await(req_id, msg).await? {
            PendingReply::HookAck { ok: true, .. } => Ok(()),
            PendingReply::HookAck { ok: false, reason } => {
                Err(reason.unwrap_or_else(|| "ws operator: spawn rejected".into()))
            }
            PendingReply::Answer(_) => {
                Err("ws operator: unexpected answer reply to hook_before".into())
            }
            PendingReply::SpawnAck { .. } => {
                Err("ws operator: unexpected spawn_ack reply to hook_before".into())
            }
            PendingReply::SpawnHalt { .. } => {
                Err("ws operator: unexpected spawn_halt reply to hook_before".into())
            }
        }
    }

    async fn after(&self, ctx: &Ctx, result: &Value) -> Result<(), String> {
        let req_id = format!("{}-ha-{}", self.sid, uuid::Uuid::new_v4());
        let msg = ServerMsg::HookAfter {
            req_id,
            parent_req_id: current_parent_req_id(),
            task_id: ctx.task_id.clone(),
            agent: ctx.agent.clone(),
            attempt: ctx.attempt,
            result: result.clone(),
        };
        // `after` is fire-and-forget — swallow send failures.
        let _ = self.send_oneway(msg).await;
        Ok(())
    }
}

#[async_trait]
impl Operator for WSOperatorSession {
    /// Thin control channel impl (the Spawn thin-control axis): `system` / `prompt`
    /// have already been baked into engine state on the server side
    /// (= `bake_worker_system_prompt` in `OperatorSpawner.spawn` + the existing
    /// `fetch_prompt` path). This impl encodes `worker_token` and hands it to
    /// the MainAI in a single Spawn message; the SubAgent then hits
    /// `/v1/worker/prompt` + `/v1/worker/result` itself over HTTP. `system` is
    /// intentionally **not used here** (heavy payloads are not carried on WS —
    /// thin-path discipline); `prompt` (issue #18) is used only to recover a
    /// `Value` for the `Spawn.directive` reminder line (see
    /// `default_spawn_directive_with_task_directive`) — the SubAgent still
    /// self-fetches the full prompt over HTTP, unchanged.
    ///
    /// The SubAgent's result post (= HTTP POST `/v1/worker/result`) appends
    /// `Final` to `output_tail`; when the MainAI returns `SpawnAck`, this
    /// `execute` returns `WorkerResult` and control returns to the dispatch path.
    ///
    /// `worker` is required (see `requires_worker_binding`) — the compile-time
    /// gate in `OperatorSpawnerFactory::build` is the primary defense, but a
    /// `None` can still reach here on paths that bypass compilation (e.g. an
    /// operator-sid-pin path). This runtime check is the defensive second
    /// layer: fail the task loud rather than silently degrade to the old
    /// hardcoded `"mse-worker"` literal.
    async fn execute(
        &self,
        ctx: &Ctx,
        _system: Option<String>,
        prompt: Value,
        worker: Option<WorkerBinding>,
        worker_token: CapToken,
    ) -> Result<WorkerResult, WorkerError> {
        let Some(worker) = worker else {
            return Err(WorkerError::Failed(format!(
                "agent '{}' has no worker_binding; WS thin-path requires one \
                 (Blueprint AgentDef.profile.worker_binding)",
                ctx.agent
            )));
        };
        let req_id = format!("{}-spawn-{}", self.sid, uuid::Uuid::new_v4());
        let worker_handle = ctx
            .meta
            .runtime
            .get("worker_handle")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let data_sink_endpoint = ctx
            .meta
            .runtime
            .get("data_sink_endpoint")
            .and_then(|v| v.as_str());
        // issue #13 run_id propagation: `EngineDispatcher::with_run` (when
        // the launch carries a `RunContext`) inserts this into
        // `Ctx.meta.runtime["run_id"]`; `None` on launches with no run
        // tracing (see `Engine::dispatch_attempt_with`'s `run_id` param).
        let run_id = ctx.meta.runtime.get("run_id").and_then(|v| v.as_str());
        // GH #20 Contract C: `project_name_alias` / `project_root` /
        // `work_dir` (previously read individually here) now come off one
        // materialized `AgentContextView` — reads back the view
        // `AgentContextMiddleware` stashed into
        // `ctx.meta.runtime[AGENT_CONTEXT_KEY]`, falling back to a
        // field-by-field pull off `ctx.meta.runtime` when that middleware
        // was never layered (backward compat). See the module doc on
        // `mlua_swarm::core::agent_context` for the full narrative.
        let view = AgentContextView::materialized_or_from_ctx(ctx);
        // issue #18: `prompt` is `TaskSpec.initial_directive`, threaded as
        // `Value` end-to-end through `EngineState.prompts` /
        // `Engine::fetch_prompt`. The WS Spawn frame text render is the
        // sole String boundary on this axis — no re-parse round trip,
        // and Object / Array / Number seeds keep their structural shape
        // all the way to the render call.
        let directive = default_spawn_directive_with_task_directive(
            &ctx.agent,
            ctx.task_id.as_str(),
            &worker.variant,
            &view,
            data_sink_endpoint,
            self.base_url.as_deref(),
            run_id,
            &prompt,
        );
        // GH #27 (follow-up to #23): the ProjectionPlacement resolver
        // `AgentContextMiddleware` resolved (via `Engine::projection_placement_for`,
        // which this WS session has no direct handle to call itself) and
        // stashed into `ctx.meta.runtime[PROJECTION_PLACEMENT_KEY]` — falls
        // back to the byte-compat default when absent or undeserializable
        // (middleware never layered onto this spawner stack, e.g. tests
        // driving `execute` directly against a bare `Ctx`).
        let projection_placement = ctx
            .meta
            .runtime
            .get(PROJECTION_PLACEMENT_KEY)
            .and_then(|v| serde_json::from_value::<ProjectionPlacement>(v.clone()).ok())
            .unwrap_or_default();
        // issue #21/ST2 in-flight projection hook: materializes `view`
        // (already `apply_policy`-filtered — see `AgentContextMiddleware`)
        // to file and appends a `ctx_projection:` pointer line. See
        // `append_projection_pointer`'s doc for the fallback contract.
        let directive = append_projection_pointer(
            directive,
            &ctx.task_id,
            &view,
            run_id,
            &projection_placement,
        );
        let msg = ServerMsg::Spawn {
            req_id: req_id.clone(),
            parent_req_id: current_parent_req_id(),
            task_id: ctx.task_id.clone(),
            agent: ctx.agent.clone(),
            attempt: ctx.attempt,
            capability_token: worker_token.encode(),
            worker_handle,
            worker: Some(worker),
            directive,
        };
        match self.send_and_await(req_id, msg).await {
            Ok(PendingReply::SpawnAck {
                value,
                ok,
                error: None,
                stats,
            }) => Ok(WorkerResult {
                value,
                ok,
                // Operator-proxied stats (the harness reports the
                // SubAgent's usage to the Operator, who attaches it to
                // this ack). Best-effort decode — an unknown shape is
                // dropped, never a spawn failure.
                stats: stats.and_then(decode_ack_stats),
            }),
            Ok(PendingReply::SpawnAck {
                error: Some(msg), ..
            }) => Err(WorkerError::Failed(msg)),
            // `spawn_halt` (issue #7): controlled halt. Return
            // `Ok(WorkerResult { ok: true, value: halt_marker })` so the
            // step lands as a normal termination rather than a
            // `WorkerError::Failed` — log stays `info`, downstream retry
            // logic doesn't fire. The halt marker carries the caller's
            // partial value and reason string in a fixed shape.
            Ok(PendingReply::SpawnHalt { value, reason }) => {
                let marker = serde_json::json!({
                    "halted": true,
                    "reason": reason,
                    "value": value,
                });
                Ok(WorkerResult {
                    value: marker,
                    ok: true,
                    stats: None,
                })
            }
            Ok(_) => Err(WorkerError::Failed(
                "ws operator: unexpected non-spawn reply".into(),
            )),
            Err(e) => Err(WorkerError::Failed(format!("ws operator spawn: {e}"))),
        }
    }

    fn requires_worker_binding(&self) -> bool {
        true
    }
}

/// Decode an ack-attached stats `Value` into
/// [`mlua_swarm::store::trace::WorkerStats`], defaulting `worker_kind`
/// to `"operator"` (the axis this ack rode in on). Best-effort: a shape
/// that doesn't decode, or an all-empty stats object, maps to `None` —
/// stats must never gate the ack itself.
///
/// A failed decode is logged at `warn`: dropping it silently is what
/// made a whole run's ack-reported stats vanish with `mse_ack` still
/// answering `{"sent": true}`, leaving `swarm_run_stats` at
/// `steps_with_stats: 0` and no signal anywhere pointing at the shape
/// that was rejected.
fn decode_ack_stats(v: serde_json::Value) -> Option<mlua_swarm::store::trace::WorkerStats> {
    let mut stats: mlua_swarm::store::trace::WorkerStats = match serde_json::from_value(v.clone()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                error = %e,
                stats = %v,
                "spawn_ack: attached stats failed to decode — dropped (the ack itself \
                 still succeeded). Expected an object with optional worker_kind / \
                 model / num_turns / adapter_data plus an optional usage object of \
                 optional input_tokens / output_tokens / total_tokens"
            );
            return None;
        }
    };
    // Emptiness is judged on what the ack actually reported — BEFORE
    // the `worker_kind` default. Defaulting first made `is_empty()`
    // unreachable (the label alone is never empty), so an ack carrying
    // `stats: {}` recorded a content-free entry that still counted
    // toward `swarm_run_stats.steps_with_stats` — the very number this
    // path exists to make trustworthy.
    if stats.is_empty() {
        return None;
    }
    if stats.worker_kind.is_none() {
        stats.worker_kind = Some("operator".to_string());
    }
    Some(stats)
}

/// Literal instruction text for the MainAI (= WS Client = Operator role). Fix
/// for observation #7.
///
/// Minimal hand-off form parallel to /orch (agent_primitive): sends an
/// `[agent_primitive dispatch=@<agent>]` marker + worker endpoint + auth +
/// task_id in the payload; the MainAI **kicks a SubAgent by specifying AgentId +
/// Token** and **forwards the return string verbatim into `SpawnAck.value`**.
///
/// The detailed instructions for the SubAgent are consolidated into the
/// agent.md `system` (= the body fetched by `GET /v1/worker/prompt`); the
/// directive is narrowed to the minimum routing information.
///
/// # `project_name_alias` / `project_root` / `work_dir` / `task_metadata`
/// (GH #20 Contract C — `AgentContextView.to_directive_header`)
///
/// These task-level context header lines are no longer read individually
/// here — they come off one materialized `view: &AgentContextView`
/// (see `mlua_swarm::core::agent_context` for the full Contract C
/// narrative) via [`AgentContextView::to_directive_header`], rendered
/// verbatim at the top of the "worker endpoint" block below. Format is
/// byte-identical to the pre-#20 individual splices
/// (`project_name_alias: {a}` / `project_root: {p}` / `work_dir: {w}`,
/// each independently absent-or-present, no empty-string placeholder) —
/// the additive change is the new `task_metadata: {compact-json}` line
/// (closes the F2 gap tracked in the `operator-execution-model` guide)
/// plus one line per `view.extra` entry.
///
/// `project_name_alias` is ALSO used below (via `view.project_name_alias`)
/// to expand the "LDS Session Alias" mandatory reminder block for the
/// MainAI — the engine itself performs no other action on the alias; the
/// expansion here is what the MainAI actually reads.
///
/// # `subagent_type` (Blueprint-baked worker binding)
///
/// Resolved from `AgentDef.profile.worker_binding` (see `WorkerBinding`) and
/// literally substituted for the old hardcoded `"mse-worker"` string — the
/// Blueprint is the single source of truth for which Claude Code SubAgent
/// definition the MainAI must dispatch. There is deliberately **no fallback**
/// to another `subagent_type` here: if the named SubAgent definition is not
/// registered, the MainAI is instructed to fail the SpawnAck loud rather than
/// silently substitute a different one.
/// `base_url` is the server's public HTTP root (e.g.
/// `"http://127.0.0.1:7777"`). When `Some`, it is rendered verbatim into
/// the SubAgent prompt block so the operator can copy the frame
/// straight through without a `mse_doctor` lookup (issue #8). When
/// `None`, a fallback placeholder points the reader at `mse_doctor` —
/// no fake port number appears in the directive.
///
/// `run_id` (issue #13 ID-hierarchy persistence) is `Some` whenever this
/// dispatch's `Ctx.meta.runtime["run_id"]` is populated (see
/// `Engine::dispatch_attempt_with`), and is rendered into the observation
/// route hint below (`GET /v1/runs/{run_id}`) so a MainAI reading the
/// directive can drill into that specific kick's `RunRecord.step_entries`
/// trace. `None` falls back to a generic `<run_id>` placeholder. Kept as
/// its own parameter (not read off `view`) — the directive's observation
/// route hint is a separate rendering concern from the task-level context
/// header.
#[allow(clippy::too_many_arguments)]
pub(super) fn default_spawn_directive(
    agent: &str,
    task_id: &str,
    subagent_type: &str,
    view: &AgentContextView,
    data_sink_endpoint: Option<&str>,
    base_url: Option<&str>,
    run_id: Option<&str>,
) -> String {
    // GH #20: task-level context header lines (project_name_alias /
    // project_root / work_dir / task_metadata / extra), rendered by the
    // materialized view itself. See the doc above.
    let context_header = view.to_directive_header();
    // Endpoint hint for the Data path (Big Response routing). Only when
    // Some, inject a convention line telling the MainAgent to pass the Big
    // EMIT POST target URL into the SubAgent prompt or environment when it
    // kicks a SubAgent. Audience: MainAgent (the SubAgent-launcher side).
    // A single authenticated emit endpoint: the token can be passed as
    // Bearer or `?token=`; both consume the same CapToken material.
    let data_endpoint_block = match data_sink_endpoint {
        Some(base) => format!(
            "\n\
             [Data path endpoint — MainAgent reminder]\n\
             When you kick a SubAgent, inject the following two lines into\n\
             its prompt / environment so Big Response payloads (4k+ tokens,\n\
             files, intermediate artifacts) flow directly to the Store owner,\n\
             bypassing the MainAgent (context stays small; only the out_id\n\
             ref is passed around).\n  \
             DATA_EMIT: {base}/v1/data/emit  (POST, auth = Bearer worker_handle or ?token=)\n  \
             DATA_GET:  {base}/v1/data/<out_id|out_name>  (the next SubAgent fetches from $IN_REFS)\n\
             When a SubAgent produces a Big Response, POST it to DATA_EMIT\n\
             and return only the one-line out_id ref (do not mix the body\n\
             in; the MainAgent must not answer directly).\n\
             \n"
        ),
        None => String::new(),
    };
    let main_ai_reminder = match view.project_name_alias.as_deref() {
        Some(a) => format!(
            "\n\
             [LDS Session Alias Reminder — MainAI mandatory]\n\
             Before kicking the SubAgent below, call:\n  \
             mcp__lds__session_create(root=<working_dir>, alias=\"{a}\")\n\
             (= establish a single task-level lds session; reuse on repeated dispatch).\n\
             Then add this literal line to the SubAgent prompt body below:\n  \
             LDS Session Alias: {a}\n\
             The SubAgent will call mcp__lds__session_start(alias=\"{a}\") on init,\n\
             keeping worktree ownership unified across dispatches.\n\
             (Full discipline rationale is inlined above; reach is via this directive itself,\n\
              not via any external doc path. The 2 steps above are the complete contract.)\n\
             \n"
        ),
        None => String::new(),
    };
    // Issue #8: render the actual server bind literally when it was
    // sourced at boot; fall back to a pointer at `mse_doctor` rather
    // than a fake port number.
    let base_url_line = match base_url {
        Some(u) => u.to_string(),
        None => "<your server's actual bind — check with mse_doctor>".to_string(),
    };
    // issue #13: the real drill-down route is `GET /v1/runs/{run_id}` (a
    // single `RunRecord`, `step_entries` trace included) — `GET
    // /v1/tasks/{id}` does exist but returns the coarser `TaskRecord` +
    // every `RunRecord` kicked from it, not this specific kick.
    let run_route_line = match run_id {
        Some(rid) => format!("GET <base_url>/v1/runs/{rid}"),
        None => "GET <base_url>/v1/runs/<run_id>".to_string(),
    };
    format!(
        "[agent_primitive dispatch=@{agent}]\n\
         worker endpoint:\n  \
         GET  <base_url>/v1/worker/prompt?task_id={task_id}\n  \
         POST <base_url>/v1/worker/submit\n\
         auth: Bearer <worker_handle from THIS Spawn payload (= short `wh-XXXXXXXX` form)>\n\
         task_id: {task_id}\n\
         agent_id: {agent}\n\
         {context_header}\
         {data_endpoint_block}\
         {main_ai_reminder}\
         Kick a SubAgent via Agent tool with subagent_type=\"{subagent_type}\" (= project-local \
         `.claude/agents/{subagent_type}.md`, this agent's Blueprint-declared worker binding). \
         The prompt you pass to it MUST be EXACTLY these 4 lines (no preamble, no extra text):\n\
         \n  \
         agent_id: {agent}\n  \
         worker_handle: <THIS Spawn payload's `worker_handle` field (short string `wh-XXXXXXXX`)>\n  \
         base_url: {base_url_line}\n  \
         task_id: {task_id}\n\
         \n\
         The SubAgent self-fetches system + prompt via GET (Bearer = handle), \
         executes as agent @{agent}, POSTs raw body to /v1/worker/submit (Bearer = handle, \
         server resolves task_id from handle), and replies `OUTPUT` 1 word. You then forward \
         SpawnAck {{req_id, value:{{}}, ok:true}} through your operator client — MCP path: \
         mse_ack(sid, req_id, kind=\"spawn_ack\", ok=true) (= empty value because canonical \
         body lives in output_tail via the POST). \
         Do NOT fetch /v1/worker/prompt yourself. Do NOT wrap, summarize, or field-select \
         the SubAgent reply. Observation / debug is a separate channel (= agent-inspect MCP / \
         {run_route_line}), do NOT mix it into the forward path. \
         If the SubAgent type is not registered, FAIL LOUD: reply SpawnAck ok=false with an \
         error explaining the missing `.claude/agents/{subagent_type}.md` — do NOT fall back \
         to another subagent_type."
    )
}

/// Wraps [`default_spawn_directive`]'s routing/reminder text as the WS
/// `Spawn.directive` `Value` (issue #18), additionally splicing in a
/// `task_directive` line built from `TaskSpec.initial_directive` when the
/// task was seeded with one.
///
/// This is the sole place the render from `Value` (`task_directive`) down
/// to `String` literal happens for the WS Spawn path — the coercion that
/// used to sit in `EngineDispatcher::dispatch` moved here. `task_directive
/// == Value::Null` (no seed, or the caller could not recover one) omits
/// the line entirely, leaving the output byte-identical to
/// [`default_spawn_directive`]'s own text — this preserves every existing
/// [`default_spawn_directive`] test unchanged, since that function's
/// signature and body are untouched by issue #18.
#[allow(clippy::too_many_arguments)]
pub(super) fn default_spawn_directive_with_task_directive(
    agent: &str,
    task_id: &str,
    subagent_type: &str,
    view: &AgentContextView,
    data_sink_endpoint: Option<&str>,
    base_url: Option<&str>,
    run_id: Option<&str>,
    task_directive: &Value,
) -> String {
    let base = default_spawn_directive(
        agent,
        task_id,
        subagent_type,
        view,
        data_sink_endpoint,
        base_url,
        run_id,
    );
    // Strings pass through verbatim; anything else (Object / Array /
    // Number / Bool) is serde-stringified — the same coercion pattern
    // `EngineDispatcher::dispatch` used to apply eagerly, now applied
    // lazily at this render boundary only.
    let task_directive_line = match task_directive {
        Value::Null => String::new(),
        Value::String(s) => format!("task_directive: {s}\n"),
        other => format!("task_directive: {other}\n"),
    };
    format!("{base}{task_directive_line}")
}

/// issue #21/ST2 in-flight projection hook: projects `view` (the
/// spawn-time, already `apply_policy`-filtered [`AgentContextView`] — see
/// `AgentContextMiddleware`'s module doc) to file via a fresh
/// [`FileProjectionAdapter`] rooted at the materialize root
/// `placement.resolve_root(view)` resolves (GH #27, follow-up to #23 —
/// see `mlua_swarm::core::projection_placement`'s module doc for the "3
/// path" convergence this closes: this hook used to check `view.work_dir`
/// ONLY, with no fallback to `view.project_root`, an asymmetry against
/// the other two call sites the shared resolver now removes), and appends
/// a single `ctx_projection: {json}\n` line to `directive` — a
/// `{key}: {value}\n` splice matching [`AgentContextView::to_directive_header`]'s
/// own line convention (e.g. its `task_metadata: {compact-json}\n` line),
/// never the projected value itself (pointer-only supply; see
/// `mlua_swarm::core::projection`'s module doc for why).
///
/// An unresolved root, `view` failing to serialize, or the materialize
/// write itself failing, all fall back to `directive` unchanged (no
/// pointer line) rather than failing the spawn — subtask-2's Invariants
/// require this hook to never turn a would-have-succeeded spawn into a
/// failure.
///
/// # projection-adapter ST5: `ctx_step_dir` line retired
///
/// This spawn-time `ctx_projection:` line supplies *this spawning agent's
/// own* `AgentContextView` (kept, unchanged, above). A companion
/// `ctx_step_dir:` line — pointing the worker at
/// `<root>/workspace/tasks/<task_id>/ctx/` plus the `mse_ctx_get` MCP tool
/// as a way to pull a *prior* step's OUTPUT out of it — existed from
/// subtask-4 through ST4; ST5 retires both (see
/// `mlua_swarm::core::agent_context`'s module doc): the Worker axis now
/// gets prior steps' OUTPUT pointers automatically, pre-filtered through
/// `ContextPolicy.steps`, on `AgentContextView.steps` (assembled by
/// `crates/mlua-swarm-server/src/worker.rs`'s `GET /v1/worker/prompt`
/// handler) — no separate directory hint or MCP tool call needed.
fn append_projection_pointer(
    directive: String,
    task_id: &StepId,
    view: &AgentContextView,
    run_id: Option<&str>,
    placement: &ProjectionPlacement,
) -> String {
    let Some(root) = placement.resolve_root(view) else {
        return directive;
    };
    match serde_json::to_value(view) {
        Ok(ctx_data) => {
            let key = ProjectionKey {
                task_id: task_id.to_string(),
                run_id: run_id.map(str::to_string),
                step: None,
                path: None,
            };
            let adapter = FileProjectionAdapter::with_placement(root, placement.clone());
            match adapter.project(&key, &ctx_data) {
                Ok(reference) => {
                    let pointer_value = match &reference {
                        ProjectionRef::File { path } => serde_json::json!({ "file": path }),
                        ProjectionRef::Query { endpoint, key } => {
                            serde_json::json!({ "endpoint": endpoint, "key": key })
                        }
                    };
                    format!("{directive}ctx_projection: {pointer_value}\n")
                }
                Err(err) => {
                    tracing::warn!(
                        %task_id,
                        error = %err,
                        "projection hook: materialize failed, spawning without a pointer"
                    );
                    directive
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                %task_id,
                error = %err,
                "projection hook: AgentContextView serialize failed, spawning without a pointer"
            );
            directive
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::core::agent_context::{
        TASK_METADATA_KEY, TASK_PROJECT_ROOT_KEY, TASK_WORK_DIR_KEY,
    };

    /// Test helper: builds an `AgentContextView` with only
    /// `project_name_alias` / `project_root` / `work_dir` set (the three
    /// fields `default_spawn_directive`'s retired individual params used
    /// to carry) — everything else stays at `Default`. Mirrors the
    /// pre-#20 call shape so the mechanical rewrite of every existing
    /// test stays a 1:1 argument swap.
    fn view_with(
        alias: Option<&str>,
        project_root: Option<&str>,
        work_dir: Option<&str>,
    ) -> AgentContextView {
        AgentContextView {
            project_name_alias: alias.map(String::from),
            project_root: project_root.map(String::from),
            work_dir: work_dir.map(String::from),
            ..AgentContextView::default()
        }
    }

    #[test]
    fn ack_stats_survive_a_total_only_operator_report() {
        // The shape a Claude Code driver relays from the harness
        // completion notice: one token total, no split. This used to
        // fail the WorkerStats decode outright, so a whole run's
        // ack-reported stats vanished and swarm_run_stats read
        // steps_with_stats: 0 while every mse_ack answered {sent: true}.
        let stats = decode_ack_stats(serde_json::json!({
            "usage": {"total_tokens": 198471},
            "model": "opus",
            "num_turns": 22,
        }))
        .expect("a total-only report must land on the StepEntry");
        assert_eq!(stats.usage.expect("usage").total_tokens, 198471);
        assert_eq!(stats.model.as_deref(), Some("opus"));
        assert_eq!(stats.num_turns, Some(22));
        assert_eq!(
            stats.worker_kind.as_deref(),
            Some("operator"),
            "the ack axis labels itself"
        );
    }

    #[test]
    fn ack_stats_of_an_undecodable_shape_are_dropped_not_fatal() {
        // Best-effort stays best-effort — the drop is now logged at
        // warn, but it must still never gate the ack.
        assert!(decode_ack_stats(serde_json::json!("not-an-object")).is_none());
        assert!(
            decode_ack_stats(serde_json::json!({})).is_none(),
            "an all-empty stats object records nothing"
        );
    }

    #[tokio::test]
    async fn connection_state_tracks_the_current_sender() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let session = WSOperatorSession::new_with_base_url(
            SessionId::parse("S-connection-state").unwrap(),
            tx.clone(),
            None,
        );
        assert!(session.is_connected().await);

        session.clear_tx_if(&tx).await;
        assert!(!session.is_connected().await);
    }

    #[tokio::test]
    async fn stale_disconnect_does_not_clear_a_reconnected_sender() {
        let (old_tx, _old_rx) = mpsc::unbounded_channel();
        let (new_tx, _new_rx) = mpsc::unbounded_channel();
        let session = WSOperatorSession::new_with_base_url(
            SessionId::parse("S-reconnect-state").unwrap(),
            old_tx.clone(),
            None,
        );

        session.replace_tx(new_tx).await;
        session.clear_tx_if(&old_tx).await;

        assert!(session.is_connected().await);
    }

    #[test]
    fn directive_omits_project_name_alias_when_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(!d.contains("project_name_alias:"));
        assert!(!d.contains("LDS Session Alias"));
        assert!(!d.contains("session_create"));
    }

    #[test]
    fn directive_emits_project_name_alias_when_some() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(Some("mse-task-7785"), None, None),
            None,
            None,
            None,
        );
        // Header line (expanded verbatim from the value).
        assert!(
            d.contains("project_name_alias: mse-task-7785"),
            "directive missing project_name_alias header: {d}"
        );
        // MainAI mandatory reminder (= session_create + SubAgent prompt inject)
        assert!(
            d.contains("mcp__lds__session_create(root=<working_dir>, alias=\"mse-task-7785\")"),
            "directive missing session_create reminder: {d}"
        );
        assert!(
            d.contains("LDS Session Alias: mse-task-7785"),
            "directive missing SubAgent prompt inject line: {d}"
        );
        // Reach discipline: the rationale is inlined into the directive (no external doc path reference).
        assert!(
            d.contains("inlined above") || d.contains("complete contract"),
            "directive should inline rationale rather than point at external doc: {d}"
        );
        // The SoT is not pointed at an AI personal memory file (which is
        // outside the MainAI's reach) — reach-axis consistency. Path
        // references coming from the subagent registration convention (for
        // example `agents/<variant>.md`) are a separate case and are
        // allowed. This one forbidden pattern is assembled by string
        // concat rather than written literally, so that a scan for it does
        // not match its own assertion — do not "simplify" it back into a
        // literal. (Unrelated: the wrapper-path assertion further down
        // does carry a literal, because `.claude/agents/<variant>.md` is
        // the documented Claude Code wrapper convention this server
        // renders — see `DEFAULT_WRAPPER_DIR` — not a leaked path.)
        let forbidden_doc_ref = format!(".{}/CLAUDE.md", "claude");
        assert!(
            !d.contains(&forbidden_doc_ref),
            "directive must not reference {forbidden_doc_ref} (out of MainAI scope): {d}"
        );
    }

    #[test]
    fn directive_omits_data_endpoint_when_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(!d.contains("[Data path endpoint"));
        assert!(!d.contains("DATA_EMIT"));
        assert!(!d.contains("DATA_GET"));
    }

    #[test]
    fn directive_emits_data_endpoint_when_some() {
        let base = "http://127.0.0.1:7785";
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            Some(base),
            None,
            None,
        );
        assert!(
            d.contains("[Data path endpoint"),
            "directive missing data endpoint block header: {d}"
        );
        assert!(
            d.contains(&format!("DATA_EMIT: {base}/v1/data/emit")),
            "directive missing single-mouth emit line: {d}"
        );
        assert!(
            d.contains("Bearer worker_handle or ?token="),
            "directive missing auth transport hint: {d}"
        );
        assert!(
            d.contains(&format!("DATA_GET:  {base}/v1/data/<out_id|out_name>")),
            "directive missing GET line: {d}"
        );
        assert!(
            !d.contains("emit-auth"),
            "old split endpoint must not leak into directive: {d}"
        );
        assert!(
            d.contains("bypassing the MainAgent") && d.contains("out_id ref"),
            "directive should carry the ownership + bypass reasoning: {d}"
        );
    }

    #[test]
    fn directive_carries_declared_subagent_type_and_has_no_fallback() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(
            d.contains("subagent_type=\"code-worker\""),
            "directive must carry the Blueprint-declared subagent_type literally: {d}"
        );
        assert!(
            d.contains(".claude/agents/code-worker.md"),
            "directive must reference the declared subagent's own .md path: {d}"
        );
        // The old hardcoded default and its silent-fallback text must be gone.
        assert!(
            !d.contains("general-purpose"),
            "directive must not fall back to subagent_type=\"general-purpose\": {d}"
        );
        assert!(
            !d.contains("mse-worker\""),
            "directive must not carry the old hardcoded \"mse-worker\" literal: {d}"
        );
        assert!(
            d.contains("FAIL LOUD"),
            "directive must instruct the MainAI to fail loud instead of falling back: {d}"
        );
    }

    // ─── Issue #8: base_url rendering + fallback framing ─────────────────

    /// Layer 1: when `base_url` is `Some`, it must land verbatim in the
    /// SubAgent-prompt block, so the operator can copy the frame
    /// through without a `mse_doctor` lookup.
    #[test]
    fn directive_renders_actual_base_url_when_some() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            Some("http://127.0.0.1:8888"),
            None,
        );
        assert!(
            d.contains("base_url: http://127.0.0.1:8888"),
            "directive must render the actual bind literally: {d}"
        );
        assert!(
            !d.contains("mse_doctor"),
            "no mse_doctor detour when bind is known: {d}"
        );
    }

    /// Layer 3: when `base_url` is `None` (unit tests, mock harnesses,
    /// pre-serve rendering) the fallback line must point the reader at
    /// `mse_doctor` — never a fake port number.
    #[test]
    fn directive_falls_back_to_mse_doctor_pointer_when_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(
            d.contains("check with mse_doctor"),
            "fallback must point at mse_doctor: {d}"
        );
    }

    /// Regression guard: the historical `7786` example port (the whole
    /// origin of issue #8) must not survive in the rendered directive
    /// under any input combination.
    #[test]
    fn directive_never_contains_stale_example_port_7786() {
        for base in [
            None,
            Some("http://127.0.0.1:7777"),
            Some("http://192.0.2.1:9000"),
        ] {
            let d = default_spawn_directive(
                "implementer",
                "task-x",
                "code-worker",
                &view_with(Some("mse-task-alias"), None, None),
                Some("http://127.0.0.1:7785"),
                base,
                None,
            );
            assert!(
                !d.contains("7786"),
                "stale example port 7786 leaked: base={base:?}, d={d}"
            );
        }
    }

    // ─── Issue #13: run_id observation route (doc-drift fix) ─────────────

    /// Regression guard: the stale `GET /v1/tasks/{id}` observation hint
    /// (a route that never returns a single `RunRecord`) must be gone —
    /// the directive must point at the real drill-down route instead.
    #[test]
    fn directive_never_contains_stale_tasks_id_route() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            Some("R-abc123"),
        );
        assert!(
            !d.contains("/v1/tasks/{id}") && !d.contains("/v1/tasks/{{id}}"),
            "stale /v1/tasks/{{id}} observation hint leaked: {d}"
        );
    }

    /// When `run_id` is `Some`, it is rendered literally into the
    /// observation route hint (`GET /v1/runs/<run_id>`).
    #[test]
    fn directive_renders_actual_run_id_when_some() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            Some("R-abc123"),
        );
        assert!(
            d.contains("GET <base_url>/v1/runs/R-abc123"),
            "directive missing real run_id in observation route: {d}"
        );
    }

    /// `run_id: None` (no run tracing for this launch) falls back to a
    /// generic placeholder route rather than a stale/incorrect one.
    #[test]
    fn directive_falls_back_to_run_id_placeholder_when_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(
            d.contains("GET <base_url>/v1/runs/<run_id>"),
            "directive missing placeholder observation route: {d}"
        );
    }

    // ─── Issue #17: project_root / work_dir header lines ─────────────────

    /// Both absent → neither header line appears (no empty-string
    /// placeholder either).
    #[test]
    fn directive_omits_project_root_and_work_dir_when_both_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(!d.contains("project_root:"));
        assert!(!d.contains("work_dir:"));
    }

    /// Both present → both header lines render literally, alongside
    /// `project_name_alias`'s existing splice.
    #[test]
    fn directive_splices_project_root_and_work_dir_when_both_present() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, Some("/repo"), Some("/repo/work")),
            None,
            None,
            None,
        );
        assert!(
            d.contains("project_root: /repo"),
            "directive missing project_root header: {d}"
        );
        assert!(
            d.contains("work_dir: /repo/work"),
            "directive missing work_dir header: {d}"
        );
    }

    /// Partial: `project_root` present, `work_dir` absent — each field is
    /// independent, so only the present one renders.
    #[test]
    fn directive_splices_project_root_only_when_work_dir_absent() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, Some("/repo"), None),
            None,
            None,
            None,
        );
        assert!(
            d.contains("project_root: /repo"),
            "directive missing project_root header: {d}"
        );
        assert!(!d.contains("work_dir:"));
    }

    // ─── GH #20: task_metadata header line (Contract C, closes the F2 gap) ─

    /// `task_metadata` renders as a new `task_metadata: {compact-json}`
    /// line — the F2 gap the `operator-execution-model` guide tracked
    /// (`task_metadata`'s inner keys were never spliced into the
    /// directive before GH #20).
    #[test]
    fn directive_splices_task_metadata_when_some() {
        let view = AgentContextView {
            task_metadata: Some(serde_json::json!({"issue": 20})),
            ..view_with(None, Some("/repo"), None)
        };
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view,
            None,
            None,
            None,
        );
        assert!(
            d.contains(r#"task_metadata: {"issue":20}"#),
            "directive missing task_metadata header: {d}"
        );
        // Additive-only: the pre-existing project_root line still renders.
        assert!(d.contains("project_root: /repo"));
    }

    /// `task_metadata: None` (absent) omits the line entirely — no
    /// empty-string placeholder, matching every other header line's
    /// absent-field contract.
    #[test]
    fn directive_omits_task_metadata_when_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(!d.contains("task_metadata:"));
    }

    // ─── Issue #7: spawn_halt handling in Operator::execute ──────────────

    fn test_ctx(task_id: &str) -> mlua_swarm::Ctx {
        mlua_swarm::Ctx::new(mlua_swarm::StepId::parse(task_id).unwrap(), 1, "a")
    }

    fn test_worker_binding() -> mlua_swarm::WorkerBinding {
        mlua_swarm::WorkerBinding {
            variant: "test-variant".into(),
            tools: vec![],
            request_digest: None,
            requested_model: None,
        }
    }

    fn test_cap_token() -> mlua_swarm::CapToken {
        mlua_swarm::CapToken {
            agent_id: "a".into(),
            role: mlua_swarm::Role::Worker,
            scopes: vec!["*".into()],
            issued_at: 0,
            expire_at: u64::MAX / 2,
            max_uses: None,
            nonce: "test-nonce".into(),
            sig_hex: "".into(),
        }
    }

    /// A `PendingReply::SpawnHalt` reply must translate into a
    /// `Ok(WorkerResult { ok: true, value: <halt marker> })` — a normal
    /// termination, not a `WorkerError::Failed` (fail-loud). This is
    /// the whole point of the new verb: distinguishing a controlled
    /// halt from a real worker error at the log / retry-signal level.
    #[tokio::test]
    async fn spawn_halt_reply_lands_as_ok_worker_result_with_marker() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-halt").unwrap(),
            tx,
            None,
        ));

        // Kick execute() in a background task so we can grab the
        // req_id the server assigns and inject a matching SpawnHalt.
        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-halt"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn { req_id, .. } => req_id,
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnHalt {
                    value: serde_json::json!({"partial": "abc"}),
                    reason: Some("shape verified".into()),
                },
            )
            .await;

        let result = handle.await.expect("join").expect("execute Ok");
        assert!(
            result.ok,
            "spawn_halt must land as ok=true (normal termination), got: {result:?}"
        );
        assert_eq!(result.value["halted"], true);
        assert_eq!(result.value["reason"], "shape verified");
        assert_eq!(result.value["value"], serde_json::json!({"partial": "abc"}));
    }

    /// `spawn_ack { ok: false, error: Some(_) }` must retain its
    /// current fail-loud behaviour (backward compat guard).
    #[tokio::test]
    async fn spawn_ack_with_error_still_lands_as_worker_error() {
        use mlua_swarm::{Operator, WorkerError};
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-err").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-err"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn { req_id, .. } => req_id,
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: false,
                    error: Some("real crash".into()),
                    stats: None,
                },
            )
            .await;

        let err = handle.await.expect("join").expect_err("must be error");
        assert!(matches!(err, WorkerError::Failed(msg) if msg.contains("real crash")));
    }

    /// B-2: a spawn parked in `execute` (awaiting a `SpawnAck` that never
    /// arrives) must unblock with a `WorkerError::Failed` as soon as
    /// `fail_pending` drains the pending map — the teardown path's
    /// immediate-fail guarantee, so a torn-down session does not leave a
    /// spawn orphaned until the run's sync timeout fires.
    #[tokio::test]
    async fn fail_pending_unblocks_a_parked_spawn_with_worker_error() {
        use mlua_swarm::{Operator, WorkerError};
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-teardown").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-teardown"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        // Wait until the Spawn is actually parked (its pending entry is
        // registered) before tearing down, so the drain has something to
        // fail rather than racing the insert.
        let _sent = rx.recv().await.expect("Spawn sent");

        session.fail_pending("operator session torn down").await;

        let err = handle
            .await
            .expect("join")
            .expect_err("a parked spawn must fail once pending is drained");
        assert!(
            matches!(err, WorkerError::Failed(_)),
            "fail_pending must surface a WorkerError::Failed, got: {err:?}"
        );
    }

    // ─── Issue #17: end-to-end `execute()` splice (ctx.meta.runtime → Spawn.directive) ───

    /// `Ctx.meta.runtime` carrying both `project_root` and `work_dir`
    /// (the `TaskInputMiddleware` injection shape) must land in the
    /// `ServerMsg::Spawn.directive` actually sent over the wire — not
    /// just in the pure `default_spawn_directive` helper.
    #[tokio::test]
    async fn execute_splices_project_root_and_work_dir_from_ctx_meta_runtime() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-ctxroot").unwrap(),
            tx,
            None,
        ));

        let mut ctx = test_ctx("ST-ctxroot");
        ctx.meta.runtime.insert(
            TASK_PROJECT_ROOT_KEY.to_string(),
            serde_json::json!("/repo"),
        );
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            serde_json::json!("/repo/work"),
        );

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                // issue #18: `Spawn.directive` is now `Value`; extract the
                // `String` it wraps (always a `Value::String` on this
                // path — see `default_spawn_directive_with_task_directive`).
                let directive = directive.as_str();
                assert!(
                    directive.contains("project_root: /repo"),
                    "directive missing project_root splice: {directive}"
                );
                assert!(
                    directive.contains("work_dir: /repo/work"),
                    "directive missing work_dir splice: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");
    }

    /// Partial: only `project_root` present in `ctx.meta.runtime` (no
    /// `TaskInputMiddleware`-populated `work_dir`) — the splice is
    /// per-field independent, matching `TaskInputMiddleware`'s own
    /// per-field-optional contract.
    #[tokio::test]
    async fn execute_splices_project_root_only_when_ctx_meta_runtime_partial() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-ctxpartial").unwrap(),
            tx,
            None,
        ));

        let mut ctx = test_ctx("ST-ctxpartial");
        ctx.meta.runtime.insert(
            TASK_PROJECT_ROOT_KEY.to_string(),
            serde_json::json!("/repo"),
        );

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                let directive = directive.as_str();
                assert!(
                    directive.contains("project_root: /repo"),
                    "directive missing project_root splice: {directive}"
                );
                assert!(!directive.contains("work_dir:"));
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");
    }

    /// Neither present in `ctx.meta.runtime` (no `TaskInputMiddleware`
    /// layered for this launch) — the directive carries neither header
    /// line, matching pre-issue-#17 behavior exactly.
    #[tokio::test]
    async fn execute_omits_project_root_and_work_dir_when_ctx_meta_runtime_absent() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-ctxabsent").unwrap(),
            tx,
            None,
        ));

        let ctx = test_ctx("ST-ctxabsent");

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                let directive = directive.as_str();
                assert!(!directive.contains("project_root:"));
                assert!(!directive.contains("work_dir:"));
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");
    }

    /// GH #20 / F2 gap: `task_metadata` in `ctx.meta.runtime` (the
    /// `TaskInputMiddleware` injection shape) now reaches the
    /// `ServerMsg::Spawn.directive` actually sent over the wire, via
    /// `AgentContextView::materialized_or_from_ctx` falling back to
    /// `from_ctx` when `AgentContextMiddleware` was not layered.
    #[tokio::test]
    async fn execute_splices_task_metadata_from_ctx_meta_runtime() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-ctxmeta").unwrap(),
            tx,
            None,
        ));

        let mut ctx = test_ctx("ST-ctxmeta");
        ctx.meta.runtime.insert(
            TASK_METADATA_KEY.to_string(),
            serde_json::json!({"issue": 20}),
        );

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                let directive = directive.as_str();
                assert!(
                    directive.contains(r#"task_metadata: {"issue":20}"#),
                    "directive missing task_metadata splice: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");
    }

    // ─── Issue #18: `Value` pass-through render boundary
    //     (`default_spawn_directive_with_task_directive`) ───

    /// A `String` seed splices in verbatim, unquoted (matching
    /// `Value::String(s) => s.clone()` — no JSON-quoting artifact).
    #[test]
    fn with_task_directive_splices_string_seed_verbatim() {
        let directive = default_spawn_directive_with_task_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
            &serde_json::json!("do the thing"),
        );
        let text = directive.as_str();
        assert!(
            text.contains("task_directive: do the thing"),
            "missing task_directive line for a String seed: {text}"
        );
    }

    /// An Object seed renders as its JSON literal (issue #18 Invariant 3 —
    /// same shape `Engine::start_task` / `Engine::dispatch_attempt_with`
    /// produce for the Worker HTTP path via `render_directive_to_string`).
    #[test]
    fn with_task_directive_renders_object_seed_as_json_literal() {
        let directive = default_spawn_directive_with_task_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
            &serde_json::json!({"key": "value"}),
        );
        let text = directive.as_str();
        assert!(
            text.contains(r#"task_directive: {"key":"value"}"#),
            "missing JSON-literal task_directive line for an Object seed: {text}"
        );
    }

    /// `Value::Null` (no seed recovered) omits the line entirely — the
    /// output is byte-identical to `default_spawn_directive`'s own text,
    /// preserving every pre-issue-#18 caller unchanged.
    #[test]
    fn with_task_directive_omits_line_when_null() {
        let wrapped = default_spawn_directive_with_task_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
            &serde_json::Value::Null,
        );
        let plain = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert_eq!(
            wrapped,
            serde_json::Value::String(plain),
            "Value::Null seed must not add a task_directive line"
        );
    }

    /// End-to-end via `execute()`: an Object-shaped `Step.in` seed, once
    /// rendered to a JSON-literal `String` by the engine (the Worker HTTP
    /// path's `render_directive_to_string`), reaches `ServerMsg::Spawn`
    /// with the same JSON literal spliced into `directive` — the WS
    /// render layer is the sole `Value → String` coercion point on this
    /// path (issue #18).
    #[tokio::test]
    async fn execute_splices_json_literal_task_directive_for_object_seed() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-objseed").unwrap(),
            tx,
            None,
        ));

        let ctx = test_ctx("ST-objseed");
        // Issue #18: `Value` flows end-to-end from `Step.in` through the
        // engine, so the Object seed reaches `execute()` as `Value` — no
        // stringification upstream. Only the WS Spawn frame render
        // performs the `Value → String` coercion.
        let rendered_prompt = serde_json::json!({"key": "value"});

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    rendered_prompt,
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                let directive = directive.as_str();
                assert!(
                    directive.contains(r#"task_directive: {"key":"value"}"#),
                    "directive missing JSON-literal task_directive splice: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");
    }

    // ─── issue #21/ST2: in-flight projection hook (`append_projection_pointer`) ───

    /// `view.work_dir` present → the spawn directive carries a
    /// `ctx_projection:` pointer line, and the pointed-at file actually
    /// exists on disk (subtask-2 Tests #3).
    #[tokio::test]
    async fn execute_with_work_dir_appends_ctx_projection_pointer_and_materializes_file() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let dir = tempfile::TempDir::new().unwrap();
        let mut ctx = test_ctx("ST-proj-1");
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            Value::String(dir.path().to_string_lossy().into_owned()),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-proj-1").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                assert!(
                    directive.contains("ctx_projection:"),
                    "directive missing ctx_projection pointer line: {directive}"
                );
                // ST5 (`projection-adapter`) removal confirmation: the
                // pre-ST5 `ctx_step_dir:` companion line (pointing a
                // worker at the raw materialize directory + the retired
                // `mse_ctx_get` MCP tool) must never reappear — the
                // Worker axis now gets prior steps' OUTPUT pointers
                // automatically via `context.steps`.
                assert!(
                    !directive.contains("ctx_step_dir:"),
                    "directive must not carry the retired ctx_step_dir line: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");

        let expected_file = dir.path().join("workspace/tasks/ST-proj-1/ctx/_ctx.md");
        assert!(
            expected_file.exists(),
            "materialized projection file missing at {expected_file:?}"
        );
    }

    /// `view.work_dir` absent → the spawn directive carries no
    /// `ctx_projection:` line, and the spawn still succeeds (non-fatal
    /// fallback, subtask-2 Tests #4 + Invariant "must never turn a
    /// would-have-succeeded spawn into a failure").
    #[tokio::test]
    async fn execute_without_work_dir_spawns_without_ctx_projection_pointer() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-proj-2").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-proj-2"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                assert!(
                    !directive.contains("ctx_projection:"),
                    "directive must not carry a pointer line when work_dir is absent \
                     (fallback): {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle
            .await
            .expect("join")
            .expect("execute Ok — a materialize skip must not fail the spawn");
    }

    // ──────────────────────────────────────────────────────────────
    // GH #27 (follow-up to #23): ProjectionPlacement resolver wiring
    // ──────────────────────────────────────────────────────────────

    /// `view.work_dir` ABSENT but `view.project_root` present, with the
    /// byte-compat default `ProjectionPlacement` (`root_preference =
    /// WorkDir`, falling back to `project_root`) — the asymmetry fix: a
    /// pre-GH-#27 build would have skipped the pointer entirely here
    /// (`view.work_dir` ONLY, no fallback); this build now falls back the
    /// SAME way the submit-time sink and server read-back always did.
    #[tokio::test]
    async fn execute_with_project_root_only_appends_ctx_projection_pointer_default_placement() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let dir = tempfile::TempDir::new().unwrap();
        let mut ctx = test_ctx("ST-proj-3");
        ctx.meta.runtime.insert(
            TASK_PROJECT_ROOT_KEY.to_string(),
            Value::String(dir.path().to_string_lossy().into_owned()),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-proj-3").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                assert!(
                    directive.contains("ctx_projection:"),
                    "work_dir absent must still fall back to project_root: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");

        let expected_file = dir.path().join("workspace/tasks/ST-proj-3/ctx/_ctx.md");
        assert!(
            expected_file.exists(),
            "materialized projection file missing at {expected_file:?}"
        );
    }

    /// A `ProjectionPlacement` stashed into
    /// `ctx.meta.runtime[PROJECTION_PLACEMENT_KEY]` (the same channel
    /// `AgentContextMiddleware` populates at spawn time) with
    /// `root_preference = ProjectRoot` and a custom `dir_template` changes
    /// BOTH which root is preferred (even though `work_dir` is ALSO
    /// present) AND the target directory layout the in-flight pointer
    /// materializes to.
    #[tokio::test]
    async fn execute_with_custom_projection_placement_uses_declared_root_and_template() {
        use mlua_swarm::core::projection_placement::{ProjectionPlacement, RootPreference};
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let work_dir = tempfile::TempDir::new().unwrap();
        let project_root = tempfile::TempDir::new().unwrap();
        let mut ctx = test_ctx("ST-proj-4");
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            Value::String(work_dir.path().to_string_lossy().into_owned()),
        );
        ctx.meta.runtime.insert(
            TASK_PROJECT_ROOT_KEY.to_string(),
            Value::String(project_root.path().to_string_lossy().into_owned()),
        );
        let placement = ProjectionPlacement {
            root_preference: RootPreference::ProjectRoot,
            dir_template: "custom/{task_id}/out".to_string(),
        };
        ctx.meta.runtime.insert(
            PROJECTION_PLACEMENT_KEY.to_string(),
            serde_json::to_value(&placement).expect("placement serializes"),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-proj-4").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                assert!(
                    directive.contains("ctx_projection:"),
                    "directive missing ctx_projection pointer line: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");

        let expected_file = project_root.path().join("custom/ST-proj-4/out/_ctx.md");
        assert!(
            expected_file.exists(),
            "materialized projection file missing at custom placement target {expected_file:?}"
        );
        let unexpected_file = work_dir
            .path()
            .join("workspace/tasks/ST-proj-4/ctx/_ctx.md");
        assert!(
            !unexpected_file.exists(),
            "declared root_preference=ProjectRoot must not fall back to work_dir: {unexpected_file:?}"
        );
    }
}
