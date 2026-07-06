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
//! For the detailed S↔C message flow, see the overview figure in `mod.rs`.

use async_trait::async_trait;
use mlua_swarm::{
    CapToken, Ctx, Operator, SeniorBridge, SpawnHook, TaskId, WorkerBinding, WorkerError,
    WorkerResult,
};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot, Mutex};

use super::protocol::{current_parent_req_id, PendingReply, ServerMsg};

/// 1 sid = 1 session. Looked up by sid in the `operator_sessions` store on reconnect.
pub struct WSOperatorSession {
    sid: String,
    /// The current mpsc sender on the write path. `None` on disconnect;
    /// swapped to `Some(new_tx)` on reconnect.
    tx: Mutex<Option<mpsc::UnboundedSender<ServerMsg>>>,
    /// `req_id` → pending oneshot. Resolved when `answer` / `hook_ack` /
    /// `spawn_ack` arrives.
    pending: Mutex<HashMap<String, oneshot::Sender<PendingReply>>>,
}

impl WSOperatorSession {
    /// `login.rs::handle_operator_socket` is the sole constructor call site.
    /// Auth (Bearer token match) is checked there against `OperatorSessionEntry.token`
    /// *before* upgrade — this struct no longer carries its own auth_token copy.
    pub(super) fn new(sid: String, tx: mpsc::UnboundedSender<ServerMsg>) -> Self {
        Self {
            sid,
            tx: Mutex::new(Some(tx)),
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Swaps in a new tx on reconnect. Expected to be called only from the handler side.
    pub(super) async fn replace_tx(&self, new_tx: mpsc::UnboundedSender<ServerMsg>) {
        *self.tx.lock().await = Some(new_tx);
    }

    /// Clears tx to `None` on disconnect. Expected to be called only from the handler side.
    pub(crate) async fn clear_tx(&self) {
        *self.tx.lock().await = None;
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
    async fn ask(&self, task_id: &TaskId, question: Value) -> Result<Value, String> {
        let req_id = format!("{}-ask-{}", self.sid, uuid::Uuid::new_v4());
        let msg = ServerMsg::Ask {
            req_id: req_id.clone(),
            parent_req_id: current_parent_req_id(),
            task_id: task_id.0.clone(),
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
            task_id: ctx.task_id.0.clone(),
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
            task_id: ctx.task_id.0.clone(),
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
    /// `/v1/worker/prompt` + `/v1/worker/result` itself over HTTP. The `system`
    /// / `prompt` arguments are intentionally **not used here** (= heavy payloads
    /// are not carried on WS — thin-path discipline).
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
        _prompt: String,
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
        let project_name_alias = ctx
            .meta
            .runtime
            .get("project_name_alias")
            .and_then(|v| v.as_str());
        let data_sink_endpoint = ctx
            .meta
            .runtime
            .get("data_sink_endpoint")
            .and_then(|v| v.as_str());
        let directive = default_spawn_directive(
            &ctx.agent,
            &ctx.task_id.0,
            &worker.variant,
            project_name_alias,
            data_sink_endpoint,
        );
        let msg = ServerMsg::Spawn {
            req_id: req_id.clone(),
            parent_req_id: current_parent_req_id(),
            task_id: ctx.task_id.0.clone(),
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
            }) => Ok(WorkerResult { value, ok }),
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
/// # `project_name_alias` literal expansion
///
/// When the caller sets `Blueprint.metadata.project_name_alias = Some(a)`
/// (schema field defined in `mlua-swarm-blueprint-schema::BlueprintMetadata`),
/// the value flows into `ctx.meta.runtime["project_name_alias"]` via the
/// `ProjectNameAliasLayer` SpawnerLayer (see
/// `mlua_swarm::middleware::project_name_alias`). This function
/// then expands the alias **literally** into the Spawn directive text — as the
/// `project_name_alias: {a}` header line and as the "LDS Session Alias" mandatory
/// reminder block for the MainAI. The engine itself performs no other action on
/// the alias; the expansion here is what the MainAI actually reads.
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
pub(super) fn default_spawn_directive(
    agent: &str,
    task_id: &str,
    subagent_type: &str,
    project_name_alias: Option<&str>,
    data_sink_endpoint: Option<&str>,
) -> String {
    // Expanded only when Blueprint.metadata.project_name_alias is Some.
    // Presents a discipline reminder to the MainAI plus the literal line the
    // SubAgent prompt should inject.
    let project_alias_line = match project_name_alias {
        Some(a) => format!("project_name_alias: {a}\n"),
        None => String::new(),
    };
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
    let main_ai_reminder = match project_name_alias {
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
    format!(
        "[agent_primitive dispatch=@{agent}]\n\
         worker endpoint:\n  \
         GET  <base_url>/v1/worker/prompt?task_id={task_id}\n  \
         POST <base_url>/v1/worker/submit\n\
         auth: Bearer <worker_handle from THIS Spawn payload (= short `wh-XXXXXXXX` form)>\n\
         task_id: {task_id}\n\
         agent_id: {agent}\n\
         {project_alias_line}\
         {data_endpoint_block}\
         {main_ai_reminder}\
         Kick a SubAgent via Agent tool with subagent_type=\"{subagent_type}\" (= project-local \
         `.claude/agents/{subagent_type}.md`, this agent's Blueprint-declared worker binding). \
         The prompt you pass to it MUST be EXACTLY these 4 lines (no preamble, no extra text):\n\
         \n  \
         agent_id: {agent}\n  \
         worker_handle: <THIS Spawn payload's `worker_handle` field (short string `wh-XXXXXXXX`)>\n  \
         base_url: <server HTTP root, e.g. http://127.0.0.1:7786>\n  \
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
         GET /v1/tasks/{{id}}), do NOT mix it into the forward path. \
         If the SubAgent type is not registered, FAIL LOUD: reply SpawnAck ok=false with an \
         error explaining the missing `.claude/agents/{subagent_type}.md` — do NOT fall back \
         to another subagent_type."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directive_omits_project_name_alias_when_none() {
        let d = default_spawn_directive("impl-lead", "task-x", "mse-worker-coder", None, None);
        assert!(!d.contains("project_name_alias:"));
        assert!(!d.contains("LDS Session Alias"));
        assert!(!d.contains("session_create"));
    }

    #[test]
    fn directive_emits_project_name_alias_when_some() {
        let d = default_spawn_directive(
            "impl-lead",
            "task-x",
            "mse-worker-coder",
            Some("mse-task-7785"),
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
        // example `agents/mse-worker.md`) are a separate case and are
        // allowed. The pattern is assembled by string concat so that no
        // gitignored dir literal remains in the source and the
        // internal-doc-leak / secret-pre-commit-checker mechanical pattern
        // match is avoided.
        let forbidden_doc_ref = format!(".{}/CLAUDE.md", "claude");
        assert!(
            !d.contains(&forbidden_doc_ref),
            "directive must not reference {forbidden_doc_ref} (out of MainAI scope): {d}"
        );
    }

    #[test]
    fn directive_omits_data_endpoint_when_none() {
        let d = default_spawn_directive("impl-lead", "task-x", "mse-worker-coder", None, None);
        assert!(!d.contains("[Data path endpoint"));
        assert!(!d.contains("DATA_EMIT"));
        assert!(!d.contains("DATA_GET"));
    }

    #[test]
    fn directive_emits_data_endpoint_when_some() {
        let base = "http://127.0.0.1:7785";
        let d =
            default_spawn_directive("impl-lead", "task-x", "mse-worker-coder", None, Some(base));
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
        let d = default_spawn_directive("impl-lead", "task-x", "mse-worker-coder", None, None);
        assert!(
            d.contains("subagent_type=\"mse-worker-coder\""),
            "directive must carry the Blueprint-declared subagent_type literally: {d}"
        );
        assert!(
            d.contains(".claude/agents/mse-worker-coder.md"),
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

    // ─── Issue #7: spawn_halt handling in Operator::execute ──────────────

    fn test_ctx(task_id: &str) -> mlua_swarm::Ctx {
        mlua_swarm::Ctx::new(mlua_swarm::TaskId(task_id.into()), 1, "a")
    }

    fn test_worker_binding() -> mlua_swarm::WorkerBinding {
        mlua_swarm::WorkerBinding {
            variant: "test-variant".into(),
            tools: vec![],
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
        let session = std::sync::Arc::new(WSOperatorSession::new("sid-halt".into(), tx));

        // Kick execute() in a background task so we can grab the
        // req_id the server assigns and inject a matching SpawnHalt.
        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("task-halt"),
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
        let session = std::sync::Arc::new(WSOperatorSession::new("sid-err".into(), tx));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("task-err"),
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
                },
            )
            .await;

        let err = handle.await.expect("join").expect_err("must be error");
        assert!(matches!(err, WorkerError::Failed(msg) if msg.contains("real crash")));
    }
}
