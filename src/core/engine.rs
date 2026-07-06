//! `Engine` — the long-running stateful runtime plus the `with_state`
//! helper (R1-R4 discipline).
//!
//! The engine owns the Domain side of the Data / Domain split:
//! flow control (dispatch / verdict), state (`EngineState`), and the
//! `submit_output` / `output_tail` surface that feeds it. Data-plane
//! traffic (Big Response bodies) is delegated to the `output_store` module
//! plus its paired `SpawnerLayer`s and passes through here without the
//! engine core needing to grow.

use crate::core::config::EngineCfg;
use crate::core::ctx::{Ctx, OperatorInfo, OperatorKind, SeniorBridge, SpawnHook};
use crate::core::errors::EngineError;
use crate::core::state::{
    CapTokenRecord, DispatchOutcome, EngineState, Event, EventStream, OperatorSession, ResumeKey,
    ResumePending, TaskSpec, TaskState, TaskStatus,
};
use crate::types::{
    default_role_verb_table, now_unix, CapToken, Role, RoleVerbGate, RunId, SessionId, StepId,
    TokenSigner, Verb,
};
use crate::worker::adapter::SpawnerAdapter;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex};

/// Process-wide long-running runtime. Cheap to `clone()` — an `Arc`
/// lives inside.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    state: Mutex<EngineState>,
    cfg: EngineCfg,
    signer: TokenSigner,
    gate: RoleVerbGate,
    event_tx: broadcast::Sender<Event>,
    /// ID-keyed bridge registry (register-by-ID design). `SeniorBridge`
    /// and `SpawnHook` are registered by ID; sessions bind to those IDs
    /// only. Persistence stores just the ID, and on reattach the caller
    /// re-registers under the same ID to restore presence.
    senior_bridges: tokio::sync::RwLock<HashMap<String, Arc<dyn SeniorBridge>>>,
    spawn_hooks: tokio::sync::RwLock<HashMap<String, Arc<dyn SpawnHook>>>,
    /// ID registry for full-spawn Operator backends (backends that take the
    /// entire spawn via `execute`). Sibling to `senior_bridges` /
    /// `spawn_hooks`. `OperatorDelegateMiddleware` looks these up via
    /// `ctx` and, when `kind = MainAi` / `Composite`, bypasses
    /// `inner.spawn` and calls `operator.execute` instead.
    operators: tokio::sync::RwLock<HashMap<String, Arc<dyn crate::operator::Operator>>>,
    /// Base and hint layer factories for the `SpawnerStack`. At
    /// `service::linker::link` time, `compiled.router` is wrapped with
    /// the base factories plus the hint factories resolved from
    /// `blueprint.spawner_hints.layers`. This is the engine-side
    /// counterpart to the discipline "Flow / Blueprint doesn't spell out
    /// middleware implementations — it declares the capabilities it needs
    /// as hint keys".
    layer_registry: crate::middleware::LayerRegistry,
}

impl Engine {
    /// Backwards-compatible constructor that starts the engine without a
    /// layer registry, preserving the signature already used by ~88
    /// existing call sites. Use this when automatic middleware wrapping
    /// at bind time is not needed. Callers such as `mlua-swarm-server` go through
    /// `new_with_layers(cfg, registry)` to enable the hint-resolution path.
    pub fn new(cfg: EngineCfg) -> Self {
        Self::new_with_layers(cfg, crate::middleware::LayerRegistry::new())
    }

    /// Construct an `Engine` with an explicit `LayerRegistry`, enabling
    /// hint-resolution: `spawner_hints.layers` declared on a `Blueprint`
    /// are resolved against this registry when the spawner stack is bound
    /// at `service::linker::link` time.
    pub fn new_with_layers(
        cfg: EngineCfg,
        layer_registry: crate::middleware::LayerRegistry,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        let signer = TokenSigner::new(&cfg.token_secret);
        Self {
            inner: Arc::new(EngineInner {
                state: Mutex::new(EngineState::new()),
                cfg,
                signer,
                gate: default_role_verb_table(),
                event_tx,
                senior_bridges: tokio::sync::RwLock::new(HashMap::new()),
                spawn_hooks: tokio::sync::RwLock::new(HashMap::new()),
                operators: tokio::sync::RwLock::new(HashMap::new()),
                layer_registry,
            }),
        }
    }

    /// Rebuild this `Engine` with a different `RoleVerbGate`. The gate is
    /// treated as fixed-at-build-time, so this constructs a fresh
    /// `EngineInner` (fresh empty `EngineState`) rather than mutating in
    /// place — mainly a testing convenience for swapping gate rules.
    pub fn with_gate(self, gate: RoleVerbGate) -> Self {
        // The gate is fixed at build time — the intent is to build a fresh
        // instance rather than mutating in place. As a testing convenience we
        // do allow swapping the inner Arc. Simpler form: just rebuild
        // Arc<EngineInner>.
        let inner = Arc::new(EngineInner {
            state: Mutex::new(EngineState::new()),
            cfg: self.inner.cfg.clone(),
            signer: self.inner.signer.clone(),
            gate,
            event_tx: self.inner.event_tx.clone(),
            senior_bridges: tokio::sync::RwLock::new(HashMap::new()),
            spawn_hooks: tokio::sync::RwLock::new(HashMap::new()),
            operators: tokio::sync::RwLock::new(HashMap::new()),
            layer_registry: self.inner.layer_registry.clone(),
        });
        Self { inner }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Accessors. Production code drives execution through compile +
    // `service::linker::link` + `dispatch_attempt_with(spawner)` inside
    // `TaskLaunchService`; `Engine` itself is a pure execution surface — it
    // does not own a BlueprintStore / EnhanceAdapter / Compiler, nor a
    // global spawner (the spawner is carried per-request, never stashed on
    // the engine).
    // ═══════════════════════════════════════════════════════════════════════

    /// Access the `EngineCfg` this engine was built with.
    pub fn cfg(&self) -> &EngineCfg {
        &self.inner.cfg
    }

    /// Expose the internal `LayerRegistry` — used when deriving a
    /// sub-engine that needs the same registry re-injected. The
    /// per-request sub-engine in `mlua-swarm-server` reads the parent engine's
    /// registry through this accessor and passes it to
    /// `Engine::new_with_layers(cfg, parent.layer_registry().clone())`.
    pub fn layer_registry(&self) -> &crate::middleware::LayerRegistry {
        &self.inner.layer_registry
    }

    /// Access the `TokenSigner` used to mint/verify `CapToken`s.
    pub fn signer(&self) -> &TokenSigner {
        &self.inner.signer
    }

    /// Clone a handle to the process-wide `Event` broadcast sender. Prefer
    /// `subscribe` for a ready-to-use receiver.
    pub fn event_tx(&self) -> broadcast::Sender<Event> {
        self.inner.event_tx.clone()
    }

    /// Subscribe to the engine's `Event` broadcast stream.
    pub fn subscribe(&self) -> EventStream {
        self.inner.event_tx.subscribe()
    }

    // ═══════════════════════════════════════════════════════════════════════
    // §7 with_state — single Mutex + R1-R4 (try_lock + bounded retry + max-hold panic)
    // ═══════════════════════════════════════════════════════════════════════

    /// The closure is a **sync** `FnOnce` — you cannot pass an async
    /// closure, which enforces R3 at the type level. Exceeding `max_hold`
    /// panics so that R4 violations surface immediately.
    pub async fn with_state<F, R>(&self, op: &'static str, f: F) -> Result<R, EngineError>
    where
        F: FnOnce(&mut EngineState) -> R,
    {
        let cfg = &self.inner.cfg;

        // R2: try_lock + bounded retry
        let mut guard_opt = None;
        for attempt in 0..=cfg.max_retry {
            match self.inner.state.try_lock() {
                Ok(g) => {
                    guard_opt = Some(g);
                    break;
                }
                Err(_) if cfg.try_only => return Err(EngineError::LockBusy(op)),
                Err(_) => {
                    let backoff = cfg.backoff_ms_step * (attempt as u64 + 1);
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                }
            }
        }
        let mut guard = guard_opt.ok_or(EngineError::LockBusyAfterRetry(op))?;

        // R4: max_hold guard
        let start = Instant::now();
        let result = f(&mut guard);
        let elapsed_ms = start.elapsed().as_millis();
        drop(guard);

        if elapsed_ms > cfg.max_hold_ms {
            panic!(
                "Engine.with_state('{op}') held {elapsed_ms}ms > max {}ms — suspected R3 violation (long op inside lock)",
                cfg.max_hold_ms
            );
        }
        Ok(result)
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Token verify (= sig + expire + gate + uses_left)
    // ═══════════════════════════════════════════════════════════════════════

    /// Four steps: (1) signature verify, (2) expiry check, (3) role × verb
    /// gate, (4) `uses_left` consume.
    pub async fn verify_token(&self, token: &CapToken, verb: Verb) -> Result<(), EngineError> {
        // (1) sig
        if !self.inner.signer.verify_sig(token) {
            return Err(EngineError::BadSignature);
        }
        // (2) expire
        if token.is_expired(now_unix()) {
            return Err(EngineError::TokenExpired);
        }
        // (3) role × verb gate
        if !self.inner.gate.is_allowed(token.role, verb) {
            return Err(EngineError::RoleViolation {
                role: token.role,
                verb,
            });
        }
        // (4) server-side uses_left consume
        let fp = token.fingerprint();
        self.with_state("token.consume", move |s| {
            let rec = s
                .tokens
                .get_mut(&fp)
                .ok_or_else(|| EngineError::TokenNotFound(fp.clone()))?;
            rec.consume()
                .map_err(|_: crate::core::state::CapTokenConsumeError| {
                    EngineError::TokenUsesExhausted
                })?;
            Ok::<(), EngineError>(())
        })
        .await??;
        Ok(())
    }

    /// `verify_token` plus the **task-ownership gate**.
    ///
    /// When a Worker-role token calls a state-touch verb (`fetch_prompt` /
    /// `post_result` / `read_task_state` / `cancel_task` / `poll_task`),
    /// the gate checks that `CapTokenRecord.task_id` matches the argument
    /// `task_id`; a mismatch returns `EngineError::TokenTaskMismatch`.
    /// Operator / Senior / Observer tokens are outside the ownership gate
    /// and may touch any task.
    ///
    /// **Verbs exempt from the gate.** `start_task` and `dispatch_attempt`
    /// stay outside so recursive swarming keeps working; depth is capped
    /// by `max_spawn_depth`.
    pub async fn verify_token_for_task(
        &self,
        token: &CapToken,
        verb: Verb,
        task_id: &StepId,
    ) -> Result<(), EngineError> {
        self.verify_token(token, verb).await?;
        if token.role != Role::Worker {
            return Ok(());
        }
        let fp = token.fingerprint();
        let arg_tid = task_id.clone();
        self.with_state("token.ownership_gate", move |s| {
            let bound = s.tokens.get(&fp).and_then(|r| r.task_id.as_ref()).cloned();
            match bound {
                Some(t) if t == arg_tid => Ok(()),
                Some(t) => Err(EngineError::TokenTaskMismatch {
                    bound: t.into_string(),
                    arg: arg_tid.into_string(),
                }),
                None => Err(EngineError::TokenNotFound(fp.clone())),
            }
        })
        .await??;
        Ok(())
    }

    /// Resolve the bound `task_id` from a Worker-role token. Used on the
    /// simple `/v1/worker/submit` endpoint, where the worker POSTs with a
    /// token but no `task_id`. Returns `Err` if the token role is not
    /// Worker, or if no bound task is set.
    pub async fn task_id_from_token(&self, token: &CapToken) -> Result<StepId, EngineError> {
        if token.role != Role::Worker {
            return Err(EngineError::RoleViolation {
                role: token.role,
                verb: Verb::PostResult,
            });
        }
        let fp = token.fingerprint();
        self.with_state("task_id_from_token", move |s| {
            s.tokens
                .get(&fp)
                .and_then(|r| r.task_id.as_ref())
                .cloned()
                .ok_or_else(|| EngineError::TokenNotFound(fp.clone()))
        })
        .await?
    }

    /// Resolve a short worker handle (`wh-XXXXXXXX`) to the bound
    /// `task_id`. Used on `/v1/worker/submit` when the Bearer is a short
    /// handle string rather than a full `CapToken` JSON. A missing entry
    /// returns `TokenNotFound`, i.e. "the handle is not in the store".
    pub async fn task_id_from_handle(&self, handle: &str) -> Result<StepId, EngineError> {
        let h = handle.to_string();
        self.with_state("task_id_from_handle", move |s| {
            let fp = s
                .worker_handles
                .get(&h)
                .cloned()
                .ok_or_else(|| EngineError::TokenNotFound(format!("handle={h}")))?;
            s.tokens
                .get(&fp)
                .and_then(|r| r.task_id.as_ref())
                .cloned()
                .ok_or_else(|| EngineError::TokenNotFound(format!("fp={fp}")))
        })
        .await?
    }

    /// Submit a worker result via a short handle. Skips token verification
    /// and updates `output_tail` `Final` + `task.last_result` directly in
    /// a thin path. The caller is expected to have already resolved
    /// `task_id` via `task_id_from_handle` — the handle's presence in
    /// `worker_handles` means it was minted server-side and is therefore
    /// trusted.
    pub async fn submit_worker_result_trusted(
        &self,
        task_id: &StepId,
        attempt: u32,
        value: Value,
        ok: bool,
    ) -> Result<(), EngineError> {
        let task_id_for_apply = task_id.clone();
        let value_for_event = value.clone();
        self.with_state("submit_worker_result_trusted.output", move |s| {
            let ev = crate::worker::output::OutputEvent::Final {
                content: crate::worker::output::ContentRef::Inline {
                    value: value_for_event,
                },
                ok,
            };
            s.output_store
                .entry((task_id_for_apply.clone(), attempt))
                .or_default()
                .push(ev.clone());
            s.push_event(crate::core::state::Event::WorkerOutput {
                task_id: task_id_for_apply,
                attempt,
                event: ev,
            });
        })
        .await?;
        let task_id_for_result = task_id.clone();
        let value_for_result = value.clone();
        self.with_state("submit_worker_result_trusted.last_result", move |s| {
            if let Some(t) = s.tasks.get_mut(&task_id_for_result) {
                t.last_result = Some(value_for_result);
                t.updated_at = now_unix();
            }
        })
        .await?;
        Ok(())
    }

    /// Mint a short handle and register it in the `worker_handles` map.
    /// Called immediately after the worker-token mint inside
    /// `dispatch_attempt_with`, and issues a handle bound to the same
    /// token fingerprint. Format is `wh-<8 hex chars>` (11 chars total),
    /// designed to remove the base64 copy-paste failure mode.
    async fn mint_worker_handle(&self, worker_fp: String) -> Result<String, EngineError> {
        // The handle is a sole bearer secret on the `/v1/worker/submit`
        // short-handle path (`submit_worker_result_trusted` skips token
        // verification), so it must be unguessable — OS RNG, not the
        // predictable uid counter. 8 hex chars (~4B entropy) keeps the
        // documented `wh-<8 hex>` wire shape; collision between live
        // handles is negligible at in-process handle counts.
        let short = crate::types::secure_hex(4);
        let handle = format!("wh-{short}");
        let h = handle.clone();
        self.with_state("mint_worker_handle", move |s| {
            s.worker_handles.insert(h, worker_fp);
        })
        .await?;
        Ok(handle)
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Session API
    // ═══════════════════════════════════════════════════════════════════════

    /// Attach a new session with default `OperatorInfo` (`Automate`, no
    /// bridges/hooks). Shorthand for `attach_with(.., OperatorInfo::default())`.
    pub async fn attach(
        &self,
        operator_id: impl Into<String>,
        role: Role,
        ttl: Duration,
    ) -> Result<CapToken, EngineError> {
        self.attach_with(
            operator_id,
            role,
            ttl,
            crate::core::ctx::OperatorInfo::default(),
        )
        .await
    }

    // ═══════════════════════════════════════════════════════════════════════
    // BridgeRegistry API.
    // ═══════════════════════════════════════════════════════════════════════

    /// Register a `SeniorBridge` under a name. An existing entry with the
    /// same name is overwritten. On the persisted-session reattach path,
    /// the caller re-registers under the same ID beforehand and the
    /// bridge becomes effective again.
    pub async fn register_senior_bridge(
        &self,
        id: impl Into<String>,
        bridge: Arc<dyn SeniorBridge>,
    ) {
        self.inner
            .senior_bridges
            .write()
            .await
            .insert(id.into(), bridge);
    }

    /// Register a `SpawnHook` under a name. An existing entry with the
    /// same name is overwritten.
    pub async fn register_spawn_hook(&self, id: impl Into<String>, hook: Arc<dyn SpawnHook>) {
        self.inner.spawn_hooks.write().await.insert(id.into(), hook);
    }

    /// Register an `Operator` (a spawn-body backend) under a name. An
    /// existing entry with the same name is overwritten.
    /// `OperatorDelegateMiddleware` looks this up via `ctx` and, when
    /// `kind = MainAi` / `Composite`, bypasses `inner.spawn` and calls
    /// `operator.execute` instead.
    pub async fn register_operator(
        &self,
        id: impl Into<String>,
        operator: Arc<dyn crate::operator::Operator>,
    ) {
        self.inner
            .operators
            .write()
            .await
            .insert(id.into(), operator);
    }

    /// Unregister a `SeniorBridge` by name (e.g. on WebSocket disconnect
    /// or explicit teardown). A missing ID is a no-op.
    pub async fn unregister_senior_bridge(&self, id: &str) {
        self.inner.senior_bridges.write().await.remove(id);
    }

    /// Unregister a `SpawnHook` by name. A missing ID is a no-op.
    pub async fn unregister_spawn_hook(&self, id: &str) {
        self.inner.spawn_hooks.write().await.remove(id);
    }

    /// Unregister an `Operator` backend by name. A missing ID is a no-op.
    pub async fn unregister_operator(&self, id: &str) {
        self.inner.operators.write().await.remove(id);
    }

    /// Snapshot the list of registered `SpawnHook` IDs (for test
    /// observation and debugging).
    pub async fn list_spawn_hook_ids(&self) -> Vec<String> {
        self.inner
            .spawn_hooks
            .read()
            .await
            .keys()
            .cloned()
            .collect()
    }

    /// Snapshot the list of registered `SeniorBridge` IDs.
    pub async fn list_senior_bridge_ids(&self) -> Vec<String> {
        self.inner
            .senior_bridges
            .read()
            .await
            .keys()
            .cloned()
            .collect()
    }

    /// Snapshot the list of registered `Operator` IDs.
    pub async fn list_operator_ids(&self) -> Vec<String> {
        self.inner.operators.read().await.keys().cloned().collect()
    }

    /// Attach specifying IDs directly. The caller is expected to have
    /// pre-registered them via `register_senior_bridge` /
    /// `register_spawn_hook` / `register_operator`. This is the canonical
    /// path when persistence is in play.
    ///
    /// `kind` is the "Runtime Global" tier of the `OperatorKind` cascade
    /// (stored verbatim on `OperatorSession.operator_kind`): `Some(_)` is
    /// an explicit request (including `Some(OperatorKind::Automate)`) that
    /// outranks the BP-level tiers; `None` leaves it unspecified so the
    /// BP-level tiers / final default decide. See
    /// `crate::core::ctx::collapse_operator_kind`.
    #[allow(clippy::too_many_arguments)]
    pub async fn attach_with_ids(
        &self,
        operator_id: impl Into<String>,
        role: Role,
        ttl: Duration,
        kind: Option<OperatorKind>,
        bridge_id: Option<String>,
        hook_id: Option<String>,
        operator_backend_id: Option<String>,
        operator_kind_overrides: HashMap<String, OperatorKind>,
        bp_agent_kinds: HashMap<String, OperatorKind>,
        bp_global_kind: Option<OperatorKind>,
    ) -> Result<CapToken, EngineError> {
        let operator_id = operator_id.into();
        let token = self
            .inner
            .signer
            .session(operator_id.clone(), role, vec!["*".into()], ttl);
        let session_id = SessionId::new();
        let fp = token.fingerprint();
        let now = now_unix();
        let token_for_store = token.clone();

        self.with_state("attach_with_ids", |s| {
            s.tokens
                .insert(fp.clone(), CapTokenRecord::from_token(token_for_store));
            s.sessions.insert(
                session_id.clone(),
                OperatorSession {
                    id: session_id.clone(),
                    operator_id: operator_id.clone(),
                    role,
                    attached_at: now,
                    last_seen: now,
                    attached: true,
                    owned_task_ids: Vec::new(),
                    token_fp: fp.clone(),
                    operator_kind: kind,
                    runtime_agent_kinds: operator_kind_overrides,
                    bp_agent_kinds,
                    bp_global_kind,
                    bridge_id,
                    hook_id,
                    operator_backend_id,
                },
            );
            s.push_event(Event::SessionAttached {
                session_id: session_id.clone(),
                role,
            });
        })
        .await?;

        let _ = self
            .inner
            .event_tx
            .send(Event::SessionAttached { session_id, role });
        Ok(token)
    }

    /// Build an `OperatorInfo` by looking up the session's registered IDs
    /// on the `BridgeRegistry`, plus resolving the 4-tier `OperatorKind`
    /// cascade for `agent_name` via `crate::core::ctx::collapse_operator_kind`.
    /// Used when `dispatch_attempt` injects `Ctx`. An unresolved ID
    /// (nothing registered) is silently `None` — the bridge / hook simply
    /// does not fire and the default behaviour applies.
    async fn resolve_operator_info(
        &self,
        session: &OperatorSession,
        agent_name: &str,
    ) -> OperatorInfo {
        let senior_bridge = if let Some(id) = &session.bridge_id {
            self.inner.senior_bridges.read().await.get(id).cloned()
        } else {
            None
        };
        let spawn_hook = if let Some(id) = &session.hook_id {
            self.inner.spawn_hooks.read().await.get(id).cloned()
        } else {
            None
        };
        let operator = if let Some(id) = &session.operator_backend_id {
            self.inner.operators.read().await.get(id).cloned()
        } else {
            None
        };
        let runtime_agent = session.runtime_agent_kinds.get(agent_name).copied();
        // "Runtime Global" tier: `Some(_)` is always an explicit request
        // (see the field doc on `OperatorSession.operator_kind`).
        let runtime_global = session.operator_kind;
        let bp_agent = session.bp_agent_kinds.get(agent_name).copied();
        let bp_global = session.bp_global_kind;
        let kind = crate::core::ctx::collapse_operator_kind(
            runtime_agent,
            runtime_global,
            bp_agent,
            bp_global,
        );
        OperatorInfo {
            kind,
            id: session.operator_id.clone(),
            senior_bridge,
            spawn_hook,
            operator,
        }
    }

    /// Convenience attach that takes an `OperatorInfo` (three
    /// `Arc<dyn ...>` fields plus `kind`) **inline**.
    ///
    /// # Pipeline
    ///
    /// Each `Arc<dyn ...>` is auto-registered on the engine's registry
    /// under a synthetic ID (`br-<hex>` / `hk-<hex>` / `ob-<hex>`), and
    /// the session stores that synthetic ID. Subsequent `dispatch_attempt`
    /// calls rebuild the `Arc`s from those IDs via
    /// `resolve_operator_info`, and the three middlewares fire as usual.
    ///
    /// # ⚠ Non-persisted sessions only
    ///
    /// Because this API takes inline `Arc`s, the reattach path after
    /// session persistence cannot rebuild them — the synthetic IDs are
    /// not present in a freshly started process's registry. If you need
    /// persistence, use [`Self::attach_with_ids`] with `register_*` calls
    /// beforehand to go through **named IDs** instead.
    ///
    /// Handy for tests and short-lived in-process sessions. Production
    /// WebSocket callbacks and the like should prefer `attach_with_ids`
    /// as the canonical path.
    pub async fn attach_with(
        &self,
        operator_id: impl Into<String>,
        role: Role,
        ttl: Duration,
        operator_info: crate::core::ctx::OperatorInfo,
    ) -> Result<CapToken, EngineError> {
        let operator_id = operator_id.into();
        // The caller always hands in a fully-formed `OperatorInfo`
        // (including its `kind`), so it is stored as an explicit "Runtime
        // Global" tier request (`Some(kind)`) — this path never persists
        // BP-level tiers (both stay empty below), so `Some(kind)` resolves
        // to the same `kind` at dispatch either way; see
        // `OperatorSession.operator_kind` doc.
        let kind = operator_info.kind;
        // BridgeRegistry auto-register: when the caller hands in an
        // `Arc<dyn>` directly, register it under a synthesised ID (the inline
        // path aware of persistence). Callers who want to pre-register with a
        // named ID should use `register_senior_bridge` / `register_spawn_hook`
        // + `attach_with_ids`.
        let bridge_id = if let Some(bridge) = operator_info.senior_bridge.clone() {
            let id = format!("br-{}", crate::types::uid_hex(8));
            self.inner
                .senior_bridges
                .write()
                .await
                .insert(id.clone(), bridge);
            Some(id)
        } else {
            None
        };
        let hook_id = if let Some(hook) = operator_info.spawn_hook.clone() {
            let id = format!("hk-{}", crate::types::uid_hex(8));
            self.inner
                .spawn_hooks
                .write()
                .await
                .insert(id.clone(), hook);
            Some(id)
        } else {
            None
        };
        let operator_backend_id = if let Some(operator) = operator_info.operator.clone() {
            // `ob-` = operator-backend registry id. Renamed from `op-` in the
            // issue #11 prefix reconciliation: `op-` used to collide with the
            // WS operator sid shape (now unified into `S-<hex>` anyway), and a
            // shared prefix across two unrelated registries made log filtering
            // by prefix silently ambiguous.
            let id = format!("ob-{}", crate::types::uid_hex(8));
            self.inner
                .operators
                .write()
                .await
                .insert(id.clone(), operator);
            Some(id)
        } else {
            None
        };

        let token = self
            .inner
            .signer
            .session(operator_id.clone(), role, vec!["*".into()], ttl);
        let session_id = SessionId::new();
        let fp = token.fingerprint();
        let now = now_unix();
        let token_for_store = token.clone();

        self.with_state("attach_with", |s| {
            s.tokens
                .insert(fp.clone(), CapTokenRecord::from_token(token_for_store));
            s.sessions.insert(
                session_id.clone(),
                OperatorSession {
                    id: session_id.clone(),
                    operator_id,
                    role,
                    attached_at: now,
                    last_seen: now,
                    attached: true,
                    owned_task_ids: Vec::new(),
                    token_fp: fp.clone(),
                    operator_kind: Some(kind),
                    runtime_agent_kinds: HashMap::new(),
                    bp_agent_kinds: HashMap::new(),
                    bp_global_kind: None,
                    bridge_id,
                    hook_id,
                    operator_backend_id,
                },
            );
            s.push_event(Event::SessionAttached {
                session_id: session_id.clone(),
                role,
            });
        })
        .await?;

        let _ = self
            .inner
            .event_tx
            .send(Event::SessionAttached { session_id, role });
        Ok(token)
    }

    /// Mark the session bound to `token` as detached (`attached = false`).
    /// Tasks are left in place — a later `attach`/`attach_with_ids` call
    /// carrying the same registered bridge/hook IDs can pick them back up.
    pub async fn detach(&self, token: &CapToken) -> Result<(), EngineError> {
        self.verify_token(token, Verb::DetachSession).await?;
        let fp = token.fingerprint();
        self.with_state("detach", move |s| {
            let sid = s
                .sessions
                .iter()
                .find(|(_, sess)| sess.token_fp == fp)
                .map(|(id, _)| id.clone());
            if let Some(sid) = sid {
                if let Some(sess) = s.sessions.get_mut(&sid) {
                    sess.attached = false;
                }
                s.push_event(Event::SessionDetached {
                    session_id: sid.clone(),
                });
                let _ = sid;
            }
        })
        .await?;
        Ok(())
    }

    /// Refresh the session's `last_seen` timestamp and mark it `attached`.
    /// Called periodically by an attached client to avoid being flipped to
    /// detached by `start_detach_loop`.
    pub async fn heartbeat(&self, token: &CapToken) -> Result<(), EngineError> {
        self.verify_token(token, Verb::Heartbeat).await?;
        let now = now_unix();
        let fp = token.fingerprint();
        self.with_state("heartbeat", move |s| {
            if let Some(sess) = s.sessions.values_mut().find(|sess| sess.token_fp == fp) {
                sess.last_seen = now;
                sess.attached = true;
            }
        })
        .await?;
        Ok(())
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Task lifecycle
    // ═══════════════════════════════════════════════════════════════════════

    /// Create a new `TaskState` from `spec` and register its initial
    /// prompt. When the calling token is a Worker (i.e. this is a
    /// recursive spawn), the new task inherits `parent.spawn_depth + 1`
    /// and is rejected with `SpawnDepthExceeded` once `max_spawn_depth` is
    /// hit; an Operator-issued call starts at depth 0.
    pub async fn start_task(
        &self,
        token: &CapToken,
        spec: TaskSpec,
    ) -> Result<StepId, EngineError> {
        self.verify_token(token, Verb::StartTask).await?;
        let task_id = StepId::new();
        let directive = spec.initial_directive.clone();
        let task_id_clone = task_id.clone();
        let fp = token.fingerprint();
        let max_depth = self.inner.cfg.max_spawn_depth;
        self.with_state("start_task", move |s| {
            // Recursive swarm depth gate (recursion guard):
            // Worker tokens carry CapTokenRecord.parent_task_id. Give the
            // child parent's spawn_depth + 1; if it exceeds `max`, raise an
            // error. Operator tokens (parent_task_id=None) start at depth 0.
            let parent_depth_opt = s
                .tokens
                .get(&fp)
                .and_then(|rec| rec.task_id.as_ref())
                .and_then(|tid| s.tasks.get(tid))
                .map(|t| t.spawn_depth);
            let depth = match parent_depth_opt {
                Some(d) => {
                    if d + 1 >= max_depth {
                        return Err(EngineError::SpawnDepthExceeded {
                            current: d + 1,
                            max: max_depth,
                        });
                    }
                    d + 1
                }
                None => 0,
            };

            let mut task = TaskState::new(task_id_clone.clone(), spec);
            task.spawn_depth = depth;
            s.tasks.insert(task_id_clone.clone(), task);
            s.prompts.insert((task_id_clone.clone(), 1), directive);
            // Link to the owner session (only Operator tokens match; Worker tokens have no session).
            if let Some(sess) = s.sessions.values_mut().find(|sess| sess.token_fp == fp) {
                sess.owned_task_ids.push(task_id_clone.clone());
            }
            s.push_event(Event::TaskCreated {
                task_id: task_id_clone.clone(),
            });
            Ok::<(), EngineError>(())
        })
        .await??;
        let _ = self.inner.event_tx.send(Event::TaskCreated {
            task_id: task_id.clone(),
        });
        Ok(task_id)
    }

    /// Fetch a snapshot of `TaskState` for `task_id`, subject to the
    /// task-ownership gate (see `verify_token_for_task`).
    pub async fn read_task_state(
        &self,
        token: &CapToken,
        task_id: &StepId,
    ) -> Result<TaskState, EngineError> {
        self.verify_token_for_task(token, Verb::ReadTaskState, task_id)
            .await?;
        let task_id = task_id.clone();
        self.with_state("read_task_state", move |s| {
            s.tasks
                .get(&task_id)
                .cloned()
                .ok_or_else(|| EngineError::TaskNotFound(task_id.to_string()))
        })
        .await?
    }

    /// Mark `task_id` as `Cancelled` and wake any caller blocked in
    /// `poll_task` for it.
    pub async fn cancel_task(&self, token: &CapToken, task_id: &StepId) -> Result<(), EngineError> {
        self.verify_token_for_task(token, Verb::CancelTask, task_id)
            .await?;
        let tid = task_id.clone();
        self.with_state("cancel_task", move |s| {
            let task = s
                .tasks
                .get_mut(&tid)
                .ok_or_else(|| EngineError::TaskNotFound(tid.to_string()))?;
            task.status = TaskStatus::Cancelled;
            task.updated_at = now_unix();
            s.push_event(Event::TaskCancelled {
                task_id: tid.clone(),
            });
            Ok::<(), EngineError>(())
        })
        .await??;
        self.wake_task(task_id).await?;
        Ok(())
    }

    /// Dispatch a single attempt through the given `spawner`.
    ///
    /// The lock is only held for snapshot capture; the actual spawn and
    /// completion await happen outside the lock (R3 discipline).
    ///
    /// Sits on the Domain side of the Data / Domain split. The dispatch
    /// path itself does not touch big response bodies — those flow through
    /// the Data plane (`output_store` module + sink / input_inject
    /// `SpawnerLayer`s) around this method.
    ///
    /// The caller does the compile plus `service::linker::link` and
    /// carries the same stack through each dispatch. Because the spawner
    /// is passed per-request rather than looked up from engine-global
    /// state, parallel requests against a single `Engine` instance
    /// (different Blueprints, different spawners) do not race.
    ///
    /// `run_id`, when `Some` (issue #13 run_id propagation —
    /// `EngineDispatcher` threads it in from its `RunContext`), is
    /// inserted into `Ctx.meta.runtime["run_id"]` (a plain JSON string)
    /// alongside `worker_handle`, so `Operator::execute` implementations
    /// (e.g. `WSOperatorSession`) can read it back and surface it to the
    /// worker (Spawn directive / prompt). `None` (every pre-existing
    /// caller / test) omits the key entirely — unchanged behavior.
    pub async fn dispatch_attempt_with(
        &self,
        token: &CapToken,
        task_id: &StepId,
        spawner: &Arc<dyn SpawnerAdapter>,
        run_id: Option<&RunId>,
    ) -> Result<DispatchOutcome, EngineError> {
        self.verify_token(token, Verb::DispatchAttempt).await?;
        let task_id = task_id.clone();

        // 1) Under the lock: increment the attempt number, mark Running, snapshot the
        //    prompt, and pull `operator_info` from the session so we can inject it into Ctx.
        let fp = token.fingerprint();
        let tid_for_prep = task_id.clone();
        let (attempt, agent, session_snapshot) = self
            .with_state("dispatch.prep", move |s| {
                let task = s
                    .tasks
                    .get_mut(&tid_for_prep)
                    .ok_or_else(|| EngineError::TaskNotFound(tid_for_prep.to_string()))?;
                task.attempt += 1;
                task.status = TaskStatus::Running;
                task.updated_at = now_unix();
                // The spawner pulls the prompt via engine.fetch_prompt. In prep,
                // if the prompts table has no entry for this attempt yet,
                // fall back and insert `initial_directive` so the subsequent
                // fetch_prompt succeeds.
                let attempt = task.attempt;
                let initial = task.spec.initial_directive.clone();
                s.prompts
                    .entry((tid_for_prep.clone(), attempt))
                    .or_insert(initial);
                let task = s
                    .tasks
                    .get(&tid_for_prep)
                    .ok_or_else(|| EngineError::TaskNotFound(tid_for_prep.to_string()))?;
                let agent = task.spec.agent.clone();
                // Session snapshot (looked up by token nonce). When no session
                // exists (worker token invoked directly / test injection), fall
                // back to None → default OperatorInfo.
                let sess_clone = s
                    .sessions
                    .values()
                    .find(|sess| sess.token_fp == fp)
                    .cloned();
                Ok::<_, EngineError>((attempt, agent, sess_clone))
            })
            .await??;
        // BridgeRegistry lookup + per-agent OperatorKind cascade.
        let operator_info = match session_snapshot {
            Some(sess) => self.resolve_operator_info(&sess, &agent).await,
            None => OperatorInfo::default(),
        };

        // 2) Outside the lock: worker token mint + spawn.
        //
        // Session-style mint (max_uses=None). Within one attempt the worker is
        // expected to hit `verify_token + fetch_prompt + fetch_data + post_result`
        // multiple times in order, so `one_time` would exhaust the token on the
        // very first verb. Capability is guarded by (a) the role × verb gate and
        // (b) the short TTL (1800s).
        let worker_token = self.inner.signer.session(
            format!("worker-of-{task_id}"),
            Role::Worker,
            vec!["*".into()],
            Duration::from_secs(1800),
        );
        let worker_fp = worker_token.fingerprint();
        let task_id_for_worker = task_id.clone();
        let worker_token_for_store = worker_token.clone();
        self.with_state("dispatch.mint_worker", move |s| {
            s.tokens.insert(
                worker_fp,
                CapTokenRecord::from_worker_token(worker_token_for_store, task_id_for_worker),
            );
        })
        .await?;

        // Mint a short handle (`wh-XXXXXXXX`) and register it in worker_handles.
        // Used by the simplified Bearer path for SubAgents (short-handle form
        // avoids base64 copy-paste incidents).
        let worker_handle = self.mint_worker_handle(worker_token.fingerprint()).await?;

        let mut ctx = Ctx::new(task_id.clone(), attempt, agent.clone());
        ctx.operator = operator_info; // activates MainAIMiddleware / Senior bridge
        ctx.meta
            .runtime
            .insert("worker_handle".to_string(), Value::String(worker_handle));
        if let Some(rid) = run_id {
            ctx.meta
                .runtime
                .insert("run_id".to_string(), Value::String(rid.to_string()));
        }

        let worker = spawner
            .spawn(self, &ctx, task_id.clone(), attempt, worker_token)
            .await
            .map_err(|e| EngineError::DispatchFailed(e.to_string()))?;

        // 3) Outside the lock: await worker.join() (signal-only). WorkerError is
        //    stringified. The value is fetched via output_tail (sink path).
        let signal_result: Result<(), String> = worker.join().await.map_err(|e| e.to_string());

        // Pull the last Final from output_tail and use it as the value.
        let value_ok: Result<(Value, bool), String> = match signal_result {
            Ok(()) => {
                let tail = self.output_tail(&task_id, attempt).await;
                let last_final = tail.iter().rev().find_map(|ev| match ev {
                    crate::worker::output::OutputEvent::Final { content, ok } => {
                        Some((content.clone(), *ok))
                    }
                    _ => None,
                });
                match last_final {
                    Some((crate::worker::output::ContentRef::Inline { value }, ok)) => {
                        Ok((value, ok))
                    }
                    Some((
                        crate::worker::output::ContentRef::FileRef {
                            path,
                            mime,
                            size_hint,
                        },
                        ok,
                    )) => Ok((
                        serde_json::json!({
                            "file_ref": path.to_string_lossy(),
                            "mime": mime,
                            "size_hint": size_hint,
                        }),
                        ok,
                    )),
                    None => Err("no Final in output_tail".to_string()),
                }
            }
            Err(msg) => Err(msg),
        };

        // 4) Under the lock: apply (split the borrow scope so push_event and task mut can co-exist).
        let outcome = self
            .with_state("dispatch.apply", |s| {
                if !s.tasks.contains_key(&task_id) {
                    return Err(EngineError::TaskNotFound(task_id.to_string()));
                }
                match value_ok {
                    Ok((value, ok)) => {
                        let pass = ok;
                        {
                            let task = s.tasks.get_mut(&task_id).unwrap();
                            task.last_result = Some(value.clone());
                            task.updated_at = now_unix();
                            task.status = if pass {
                                TaskStatus::Pass
                            } else {
                                TaskStatus::Blocked
                            };
                        }
                        s.push_event(Event::TaskAttemptCompleted {
                            task_id: task_id.clone(),
                            attempt,
                            result: value.clone(),
                        });
                        if pass {
                            s.push_event(Event::TaskPass {
                                task_id: task_id.clone(),
                                result: value.clone(),
                            });
                            Ok::<_, EngineError>(DispatchOutcome::Pass(value))
                        } else {
                            s.push_event(Event::TaskBlocked {
                                task_id: task_id.clone(),
                                result: value.clone(),
                            });
                            Ok(DispatchOutcome::Blocked(value))
                        }
                    }
                    Err(msg) => {
                        let task = s.tasks.get_mut(&task_id).unwrap();
                        task.status = TaskStatus::Blocked;
                        task.updated_at = now_unix();
                        Err(EngineError::DispatchFailed(msg))
                    }
                }
            })
            .await??;

        // event broadcast (outside the lock — push_event feeds the in-memory tail; broadcast is a separate path).
        let _ = self.inner.event_tx.send(Event::TaskAttemptCompleted {
            task_id: task_id.clone(),
            attempt,
            result: match &outcome {
                DispatchOutcome::Pass(v) | DispatchOutcome::Blocked(v) => v.clone(),
                _ => Value::Null,
            },
        });

        // Wake any callers waiting in poll_task.
        self.wake_task(&task_id).await?;

        Ok(outcome)
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Worker-side API (= prompt / data fetch + result post)
    // ═══════════════════════════════════════════════════════════════════════

    /// Fetch the directive/prompt string for `task_id`'s current attempt.
    /// Falls back to `initial_directive` when no prompt has been recorded
    /// yet for that attempt.
    pub async fn fetch_prompt(
        &self,
        token: &CapToken,
        task_id: &StepId,
    ) -> Result<String, EngineError> {
        self.verify_token_for_task(token, Verb::FetchPrompt, task_id)
            .await?;
        let task_id = task_id.clone();
        self.with_state("fetch_prompt", move |s| {
            let task = s
                .tasks
                .get(&task_id)
                .ok_or_else(|| EngineError::TaskNotFound(task_id.to_string()))?;
            s.prompts
                .get(&(task_id.clone(), task.attempt.max(1)))
                .cloned()
                .ok_or_else(|| {
                    EngineError::ResourceNotFound(format!(
                        "prompt({}, attempt={})",
                        task_id, task.attempt
                    ))
                })
        })
        .await?
    }

    /// Combined fetch for `HTTP /v1/worker/prompt`: returns `prompt` +
    /// (optional) `system` + `agent` + `attempt` in a single round trip.
    /// The verb gate reuses `FetchPrompt` — same semantics as "the worker
    /// pulls its task input".
    ///
    /// `system` is the value written by `OperatorSpawner::spawn` through
    /// `bake_worker_system_prompt` when it ran; otherwise `None` (no
    /// profile present, or the bake never happened).
    pub async fn fetch_worker_payload(
        &self,
        token: &CapToken,
        task_id: &StepId,
    ) -> Result<crate::types::WorkerPayload, EngineError> {
        self.verify_token_for_task(token, Verb::FetchPrompt, task_id)
            .await?;
        let task_id_clone = task_id.clone();
        self.with_state("fetch_worker_payload", move |s| {
            let task = s
                .tasks
                .get(&task_id_clone)
                .ok_or_else(|| EngineError::TaskNotFound(task_id_clone.to_string()))?;
            let attempt = task.attempt.max(1);
            let prompt = s
                .prompts
                .get(&(task_id_clone.clone(), attempt))
                .cloned()
                .ok_or_else(|| {
                    EngineError::ResourceNotFound(format!(
                        "prompt({}, attempt={})",
                        task_id_clone, attempt
                    ))
                })?;
            let system = s
                .systems
                .get(&(task_id_clone.clone(), attempt))
                .cloned()
                .unwrap_or(None);
            let agent = task.spec.agent.clone();
            Ok::<_, EngineError>(crate::types::WorkerPayload {
                task_id: task_id_clone.clone(),
                attempt,
                agent,
                prompt,
                system,
            })
        })
        .await?
    }

    /// Fetch a worker payload via a short handle. Skips token verification
    /// and returns `prompt` + `system` + `agent` + `attempt` in a thin
    /// path. The caller is expected to have already resolved `task_id`
    /// via `task_id_from_handle` — the handle's presence in
    /// `worker_handles` means it was minted server-side and is therefore
    /// trusted.
    pub async fn fetch_worker_payload_trusted(
        &self,
        task_id: &StepId,
    ) -> Result<crate::types::WorkerPayload, EngineError> {
        let task_id_clone = task_id.clone();
        self.with_state("fetch_worker_payload_trusted", move |s| {
            let task = s
                .tasks
                .get(&task_id_clone)
                .ok_or_else(|| EngineError::TaskNotFound(task_id_clone.to_string()))?;
            let attempt = task.attempt.max(1);
            let prompt = s
                .prompts
                .get(&(task_id_clone.clone(), attempt))
                .cloned()
                .ok_or_else(|| {
                    EngineError::ResourceNotFound(format!(
                        "prompt({}, attempt={})",
                        task_id_clone, attempt
                    ))
                })?;
            let system = s
                .systems
                .get(&(task_id_clone.clone(), attempt))
                .cloned()
                .unwrap_or(None);
            let agent = task.spec.agent.clone();
            Ok::<_, EngineError>(crate::types::WorkerPayload {
                task_id: task_id_clone.clone(),
                attempt,
                agent,
                prompt,
                system,
            })
        })
        .await?
    }

    /// Read the current attempt number for a task (server-side lookup, no
    /// token verification). Used on `HTTP /v1/worker/result` when the
    /// worker omits `attempt` and the server has to fill it in.
    pub async fn task_attempt(&self, task_id: &StepId) -> Result<u32, EngineError> {
        let task_id = task_id.clone();
        self.with_state("task_attempt", move |s| {
            s.tasks
                .get(&task_id)
                .map(|t| t.attempt)
                .ok_or_else(|| EngineError::TaskNotFound(task_id.to_string()))
        })
        .await?
    }

    /// Server-side admin API that lets `OperatorSpawner::spawn` bake the
    /// rendered `system_prompt` into engine state. There is no verb gate
    /// — the only expected caller is inside the spawner. SubAgents fetch
    /// this alongside the prompt on the `/v1/worker/prompt` path.
    pub async fn bake_worker_system_prompt(
        &self,
        task_id: &StepId,
        attempt: u32,
        system: Option<String>,
    ) -> Result<(), EngineError> {
        let task_id = task_id.clone();
        self.with_state("bake_worker_system_prompt", move |s| {
            s.systems.insert((task_id, attempt), system);
        })
        .await?;
        Ok(())
    }

    /// Fetch an arbitrary named resource previously stored via
    /// `set_resource`. Not task-scoped — any valid token with the
    /// `FetchData` verb may read any key.
    pub async fn fetch_data(&self, token: &CapToken, key: &str) -> Result<Value, EngineError> {
        self.verify_token(token, Verb::FetchData).await?;
        let key = key.to_string();
        self.with_state("fetch_data", move |s| {
            s.resources
                .get(&key)
                .cloned()
                .ok_or(EngineError::ResourceNotFound(key))
        })
        .await?
    }

    // ───────────────────────────────────────────────────────────────────────
    // Output path.
    // ───────────────────────────────────────────────────────────────────────

    /// Send one output event from inside a `SpawnerAdapter` or worker.
    /// Structuring is assumed to be complete by the time we cross the
    /// `SpawnerAdapter` boundary; this API just appends to the
    /// `OutputStore`, pushes to the `EventLog`, and (for `Final`) emits
    /// the `TaskAttemptCompleted` event.
    ///
    /// This is Domain-side plumbing: it feeds the engine's verdict flow,
    /// not the Data-plane store in the `output_store` module. It also
    /// does not wake the dispatch path — that is done through the
    /// spawner's completion oneshot when the worker terminates.
    pub async fn submit_output(
        &self,
        token: &crate::types::CapToken,
        task_id: &StepId,
        attempt: u32,
        event: crate::worker::output::OutputEvent,
    ) -> Result<(), EngineError> {
        self.verify_token_for_task(token, crate::types::Verb::EmitOutput, task_id)
            .await?;
        let task_id_for_apply = task_id.clone();
        let event_clone = event.clone();
        self.with_state("submit_output", move |s| {
            s.output_store
                .entry((task_id_for_apply.clone(), attempt))
                .or_default()
                .push(event_clone.clone());
            s.push_event(crate::core::state::Event::WorkerOutput {
                task_id: task_id_for_apply,
                attempt,
                event: event_clone,
            });
        })
        .await?;
        Ok(())
    }

    /// Snapshot the entire output tail for a given `(task_id, attempt)`.
    /// Used by the dispatch path when pulling `Final`, and by observers
    /// reading the trace.
    pub async fn output_tail(
        &self,
        task_id: &StepId,
        attempt: u32,
    ) -> Vec<crate::worker::output::OutputEvent> {
        let key = (task_id.clone(), attempt);
        self.with_state("output_tail", move |s| {
            s.output_store.get(&key).cloned().unwrap_or_default()
        })
        .await
        .unwrap_or_default()
    }

    /// Record an interim `last_result` for `task_id` without changing its
    /// `status`. Distinct from the terminal `Final` output event handled
    /// through `submit_output` / `dispatch_attempt_with`.
    pub async fn post_result(
        &self,
        token: &CapToken,
        task_id: &StepId,
        result: Value,
    ) -> Result<(), EngineError> {
        self.verify_token_for_task(token, Verb::PostResult, task_id)
            .await?;
        let task_id = task_id.clone();
        let result_clone = result.clone();
        self.with_state("post_result", move |s| {
            let task = s
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| EngineError::TaskNotFound(task_id.to_string()))?;
            task.last_result = Some(result_clone);
            task.updated_at = now_unix();
            Ok::<(), EngineError>(())
        })
        .await??;
        Ok(())
    }

    /// Store a named resource value, retrievable later via `fetch_data`.
    /// No token is required — this is a server-side/admin-style setter
    /// (mirrors `bake_worker_system_prompt`).
    pub async fn set_resource(
        &self,
        key: impl Into<String>,
        value: Value,
    ) -> Result<(), EngineError> {
        let key = key.into();
        self.with_state("set_resource", move |s| {
            s.resources.insert(key, value);
        })
        .await?;
        Ok(())
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Senior suspend / resume
    // ═══════════════════════════════════════════════════════════════════════

    /// Ask a question of the Senior, mark the task `Suspended`, and
    /// return a `ResumeKey`. The suspended state persists until another
    /// task calls `resume(key, answer)`.
    ///
    /// Resume-side waiting is `Notify`-based, so a caller (typically
    /// MainAI) can detach, reattach from a different process, and still
    /// pull the answer out via `await_resume(key, timeout)` — the answer
    /// is stored inside `EngineState`.
    pub async fn query_senior(
        &self,
        token: &CapToken,
        task_id: &StepId,
        question: Value,
    ) -> Result<ResumeKey, EngineError> {
        self.verify_token(token, Verb::QuerySenior).await?;
        let task_id = task_id.clone();
        let key = ResumeKey::for_senior(&task_id);
        let task_notify = self
            .with_state("query_senior.notify_ensure", |s| {
                s.ensure_task_notify(&task_id)
            })
            .await?;

        let key_clone = key.clone();
        let task_id_inner = task_id.clone();
        let question_clone = question.clone();
        self.with_state("query_senior.suspend", move |s| {
            let task = s
                .tasks
                .get_mut(&task_id_inner)
                .ok_or_else(|| EngineError::TaskNotFound(task_id_inner.to_string()))?;
            task.status = TaskStatus::Suspended;
            task.suspended_on = Some(key_clone.clone());
            task.updated_at = now_unix();
            s.pending_resumes
                .insert(key_clone.clone(), ResumePending::new());
            s.push_event(Event::SeniorQueried {
                task_id: task_id_inner.clone(),
                question: question_clone.clone(),
            });
            s.push_event(Event::TaskSuspended {
                task_id: task_id_inner.clone(),
                key: key_clone.clone(),
            });
            Ok::<(), EngineError>(())
        })
        .await??;

        // Notify callers waiting for a task status change (Running → Suspended).
        task_notify.notify_waiters();

        let _ = self
            .inner
            .event_tx
            .send(Event::SeniorQueried { task_id, question });
        Ok(key)
    }

    /// Store the answer for a `ResumeKey` in `EngineState` and wake the
    /// waiting caller via `Notify`. Also flips the suspended task's
    /// status back to `Running` and fires the per-task notifier.
    pub async fn resume(&self, key: ResumeKey, answer: Value) -> Result<(), EngineError> {
        let answer_for_state = answer.clone();
        let answer_for_event = answer.clone();
        let key_clone = key.clone();
        let (notify, task_notify, task_id_opt) = self
            .with_state("resume.set", move |s| {
                let pending = s
                    .pending_resumes
                    .get_mut(&key_clone)
                    .ok_or(EngineError::ResumeKeyNotFound)?;
                pending.answer = Some(answer_for_state);
                let notify = pending.notify.clone();

                let task_id = s
                    .tasks
                    .iter()
                    .find(|(_, t)| t.suspended_on.as_ref() == Some(&key_clone))
                    .map(|(id, _)| id.clone());

                let task_notify = task_id.as_ref().map(|tid| s.ensure_task_notify(tid));

                if let Some(tid) = &task_id {
                    if let Some(task) = s.tasks.get_mut(tid) {
                        task.suspended_on = None;
                        task.status = TaskStatus::Running;
                        task.updated_at = now_unix();
                    }
                    s.push_event(Event::TaskResumed {
                        task_id: tid.clone(),
                        key: key_clone.clone(),
                    });
                    s.push_event(Event::SeniorAnswered {
                        task_id: tid.clone(),
                        answer: answer_for_event.clone(),
                    });
                }
                Ok::<_, EngineError>((notify, task_notify, task_id))
            })
            .await??;

        // Outside the lock: notify_waiters for both the ResumePending and task-status waits.
        notify.notify_waiters();
        if let Some(n) = task_notify {
            n.notify_waiters();
        }

        if let Some(tid) = task_id_opt {
            let _ = self
                .inner
                .event_tx
                .send(Event::TaskResumed { task_id: tid, key });
        }
        Ok(())
    }

    /// Wait for the resume answer. Even if the caller (an Operator)
    /// detached and reattached, the answer is available immediately here
    /// — if it was already stored, this returns without waiting on the
    /// notifier.
    ///
    /// `timeout = Duration::ZERO` performs an instant check without
    /// waiting.
    pub async fn await_resume(
        &self,
        key: ResumeKey,
        timeout: Duration,
    ) -> Result<Value, EngineError> {
        // (1) Under the lock: clone the notify handle and check for an existing answer.
        let key_clone = key.clone();
        let (notify, existing) = self
            .with_state("await_resume.snapshot", move |s| {
                let pending = s
                    .pending_resumes
                    .get(&key_clone)
                    .ok_or(EngineError::ResumeKeyNotFound)?;
                Ok::<_, EngineError>((pending.notify.clone(), pending.answer.clone()))
            })
            .await??;

        // (2) If an answer has already been stored, return immediately (detach / reattach pattern).
        if let Some(v) = existing {
            return Ok(v);
        }

        // (3) Outside the lock: wait on the notify with a timeout.
        if timeout.is_zero() {
            return Err(EngineError::PollTimeout);
        }
        let waited = tokio::time::timeout(timeout, notify.notified()).await;
        if waited.is_err() {
            return Err(EngineError::PollTimeout);
        }

        // (4) Under the lock: re-read the answer (should be present now that we were notified).
        let key_clone = key.clone();
        self.with_state("await_resume.read", move |s| {
            let pending = s
                .pending_resumes
                .get(&key_clone)
                .ok_or(EngineError::ResumeKeyNotFound)?;
            pending
                .answer
                .clone()
                .ok_or_else(|| EngineError::Internal("notified but answer missing".into()))
        })
        .await?
    }

    // ═══════════════════════════════════════════════════════════════════════
    // poll_task — the "wait" path that waits for task-status changes (works for long-poll and regular wait).
    // ═══════════════════════════════════════════════════════════════════════

    /// Wait until the task's status **transitions to terminal or
    /// `Suspended`**, then return the latest `TaskState`. Returns
    /// immediately if the task is already in a terminal state.
    /// Exceeding the timeout returns `EngineError::PollTimeout`.
    ///
    /// A `hold` of `Duration::from_secs(0)` returns a snapshot immediately
    /// (no wait). Larger holds — tens of minutes up to days — are fine;
    /// the wait state is kept in memory inside the engine and does not
    /// degrade.
    pub async fn poll_task(
        &self,
        token: &CapToken,
        task_id: &StepId,
        hold: Duration,
    ) -> Result<TaskState, EngineError> {
        self.verify_token_for_task(token, Verb::PollTask, task_id)
            .await?;
        let task_id_inner = task_id.clone();

        // (1) Under the lock: take a snapshot and clone task_notify.
        let (state, notify) = self
            .with_state("poll_task.snapshot", move |s| {
                let task = s
                    .tasks
                    .get(&task_id_inner)
                    .cloned()
                    .ok_or_else(|| EngineError::TaskNotFound(task_id_inner.to_string()))?;
                let notify = s.ensure_task_notify(&task_id_inner);
                Ok::<_, EngineError>((task, notify))
            })
            .await??;

        // (2) Immediate-return condition: already terminal / Suspended (nothing left to wait on).
        if matches!(
            state.status,
            TaskStatus::Pass | TaskStatus::Blocked | TaskStatus::Cancelled | TaskStatus::Suspended
        ) {
            return Ok(state);
        }
        if hold.is_zero() {
            return Ok(state);
        }

        // (3) Outside the lock: wait on Notify with a timeout.
        let waited = tokio::time::timeout(hold, notify.notified()).await;
        if waited.is_err() {
            return Err(EngineError::PollTimeout);
        }

        // (4) Under the lock: take a fresh snapshot.
        let task_id_inner = task_id.clone();
        self.with_state("poll_task.reread", move |s| {
            s.tasks
                .get(&task_id_inner)
                .cloned()
                .ok_or_else(|| EngineError::TaskNotFound(task_id_inner.to_string()))
        })
        .await?
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Background: heartbeat miss → detach loop
    // ═══════════════════════════════════════════════════════════════════════

    /// Background loop that scans sessions every `heartbeat_interval` and
    /// flips `attached = false` on any session whose `last_seen` exceeds
    /// `heartbeat_miss_threshold * interval`.
    ///
    /// The tasks themselves are kept (assuming
    /// `keepalive_on_idle = true`), so another client can reattach with
    /// the same token and resume immediately. Dropping the returned
    /// `JoinHandle` does not stop the loop — the handle exists so callers
    /// who want to abort can hold onto it.
    pub fn start_detach_loop(&self) -> tokio::task::JoinHandle<()> {
        let engine = self.clone();
        let cfg = self.inner.cfg.long_hold.clone();
        let interval = cfg.heartbeat_interval;
        let miss_secs = cfg.heartbeat_interval.as_secs() * cfg.heartbeat_miss_threshold as u64;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // first tick is immediate
            loop {
                ticker.tick().await;
                let now = now_unix();
                let detached = engine
                    .with_state("detach_loop.scan", |s| {
                        let mut detached = Vec::new();
                        for (sid, sess) in s.sessions.iter_mut() {
                            if !sess.attached {
                                continue;
                            }
                            if now.saturating_sub(sess.last_seen) >= miss_secs {
                                sess.attached = false;
                                detached.push(sid.clone());
                            }
                        }
                        for sid in &detached {
                            s.push_event(Event::SessionDetached {
                                session_id: sid.clone(),
                            });
                        }
                        detached
                    })
                    .await
                    .unwrap_or_default();
                for sid in detached {
                    let _ = engine
                        .inner
                        .event_tx
                        .send(Event::SessionDetached { session_id: sid });
                }
            }
        })
    }

    /// Helper: wake a task whose status has changed. Called from the
    /// method body outside the lock.
    async fn wake_task(&self, task_id: &StepId) -> Result<(), EngineError> {
        let task_id = task_id.clone();
        let notify_opt = self
            .with_state("wake_task.get_notify", move |s| {
                s.task_notifies.get(&task_id).cloned()
            })
            .await?;
        if let Some(n) = notify_opt {
            n.notify_waiters();
        }
        Ok(())
    }
}

// ─── UT: issue #14 — token store keyed by fingerprint, not nonce ────────────
#[cfg(test)]
mod token_fingerprint_store_tests {
    use super::*;

    /// A token that was never attached fails verify with a `TokenNotFound`
    /// that carries the fingerprint — never the nonce. The error string can
    /// surface in HTTP error bodies, so this is the secret-hygiene contract.
    #[tokio::test]
    async fn verify_unknown_token_reports_fingerprint_not_nonce() {
        let engine = Engine::new(EngineCfg::default());
        // Signed by the engine's own signer (sig passes) but never inserted
        // into the store — verify must fail at step (4), the store lookup.
        let token = engine.signer().session(
            "ghost",
            Role::Operator,
            vec!["*".into()],
            Duration::from_secs(60),
        );
        let err = engine
            .verify_token(&token, Verb::ReadTaskState)
            .await
            .expect_err("token is not in the store");
        let msg = err.to_string();
        assert!(
            msg.contains(&token.fingerprint()),
            "error must carry the fingerprint: {msg}"
        );
        assert!(
            !msg.contains(&token.nonce),
            "error must not leak the nonce: {msg}"
        );
    }

    /// attach → verify → heartbeat → detach all resolve the session /
    /// token record through fingerprint keys (mint/verify lifecycle
    /// regression guard for the issue #14 key migration).
    #[tokio::test]
    async fn attach_verify_heartbeat_detach_cycle_with_fp_keying() {
        let engine = Engine::new(EngineCfg::default());
        let token = engine
            .attach("op-1", Role::Operator, Duration::from_secs(60))
            .await
            .expect("attach");
        engine
            .verify_token(&token, Verb::ReadTaskState)
            .await
            .expect("verify consumes via fp key");
        engine
            .heartbeat(&token)
            .await
            .expect("heartbeat finds the session by fp");
        engine
            .detach(&token)
            .await
            .expect("detach finds the session by fp");
    }
}

// ─── UT: `OperatorKind` "Runtime Global" tier — `Option` semantics ─────────
//
// Regression coverage for the "explicit Automate is indistinguishable from
// unspecified" defect: `OperatorSession.operator_kind` (and the
// `attach_with_ids` `kind` parameter it stores) is `Option<OperatorKind>`,
// so `Some(Automate)` is an explicit Runtime Global request that must
// outrank `bp_global`, while `None` must let `bp_global` decide. Exercises
// the real `resolve_operator_info` cascade path (not just
// `collapse_operator_kind` in isolation), attaching via `attach_with_ids`
// exactly as `TaskLaunchService::launch` does.
#[cfg(test)]
mod resolve_operator_info_runtime_global_tests {
    use super::*;

    async fn attach_and_resolve(
        runtime_global: Option<OperatorKind>,
        bp_global: Option<OperatorKind>,
    ) -> OperatorInfo {
        let engine = Engine::new(EngineCfg::default());
        let token = engine
            .attach_with_ids(
                "ut-op",
                Role::Operator,
                Duration::from_secs(30),
                runtime_global,
                None,
                None,
                None,
                HashMap::new(),
                HashMap::new(),
                bp_global,
            )
            .await
            .expect("attach_with_ids ok");
        let session = engine
            .with_state("test.find_session", |s| {
                s.sessions
                    .values()
                    .find(|sess| sess.token_fp == token.fingerprint())
                    .cloned()
            })
            .await
            .expect("with_state ok")
            .expect("session present after attach_with_ids");
        engine.resolve_operator_info(&session, "agent-x").await
    }

    #[tokio::test]
    async fn explicit_some_automate_outranks_bp_global_main_ai() {
        // Runtime Global explicitly requests Automate; bp_global is MainAi.
        // The explicit `Some(Automate)` must win — this is exactly the case
        // the old `== OperatorKind::default()` convention got wrong (it
        // could not tell "explicitly Automate" from "unspecified" and would
        // have let `bp_global` (MainAi) take over instead).
        let info =
            attach_and_resolve(Some(OperatorKind::Automate), Some(OperatorKind::MainAi)).await;
        assert_eq!(
            info.kind,
            OperatorKind::Automate,
            "explicit Some(Automate) runtime_global must outrank bp_global MainAi"
        );
    }

    #[tokio::test]
    async fn none_lets_bp_global_main_ai_win() {
        // Runtime Global left unspecified (`None`); bp_global is MainAi.
        // With nothing more specific set, `bp_global` must decide.
        let info = attach_and_resolve(None, Some(OperatorKind::MainAi)).await;
        assert_eq!(
            info.kind,
            OperatorKind::MainAi,
            "None runtime_global must let bp_global MainAi win"
        );
    }
}

/// issue #13 run_id propagation: `dispatch_attempt_with`'s `run_id` param
/// must land in `Ctx.meta.runtime["run_id"]` (the same slot pattern as the
/// pre-existing `worker_handle`), or be omitted entirely when `None`. Same
/// `CtxProbe` shape as `middleware::worker_binding`'s test module — an
/// inner `SpawnerAdapter` that snapshots the `Ctx` it was called with and
/// fails the spawn (only the ctx snapshot matters here).
#[cfg(test)]
mod dispatch_attempt_with_run_id_tests {
    use super::*;
    use crate::worker::adapter::{SpawnError, SpawnerAdapter};
    use crate::worker::Worker;
    use std::sync::Mutex as StdMutex;

    struct CtxProbe {
        seen: Arc<StdMutex<Option<Ctx>>>,
    }

    #[async_trait::async_trait]
    impl SpawnerAdapter for CtxProbe {
        async fn spawn(
            &self,
            _engine: &Engine,
            ctx: &Ctx,
            _task_id: StepId,
            _attempt: u32,
            _token: CapToken,
        ) -> Result<Box<dyn Worker>, SpawnError> {
            *self.seen.lock().unwrap() = Some(ctx.clone());
            Err(SpawnError::Internal("probe stop".into()))
        }
    }

    async fn dispatch_with_probe(run_id: Option<&RunId>) -> Ctx {
        let engine = Engine::new(EngineCfg::default());
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let tid = engine
            .start_task(
                &token,
                TaskSpec {
                    agent: "probe".into(),
                    initial_directive: "hi".into(),
                },
            )
            .await
            .expect("start_task");
        let seen: Arc<StdMutex<Option<Ctx>>> = Arc::new(StdMutex::new(None));
        let spawner: Arc<dyn SpawnerAdapter> = Arc::new(CtxProbe { seen: seen.clone() });
        // The probe always errors the spawn (`SpawnError::Internal`); we
        // only care about the `Ctx` snapshot it captured, so the dispatch
        // outcome itself (`Err`) is discarded.
        let _ = engine
            .dispatch_attempt_with(&token, &tid, &spawner, run_id)
            .await;
        let captured = seen.lock().unwrap().clone();
        captured.expect("inner ctx captured")
    }

    #[tokio::test]
    async fn run_id_lands_in_ctx_meta_runtime_when_some() {
        let run_id = RunId::new();
        let observed = dispatch_with_probe(Some(&run_id)).await;
        assert_eq!(
            observed.meta.runtime.get("run_id").and_then(|v| v.as_str()),
            Some(run_id.as_str()),
            "ctx.meta.runtime[\"run_id\"] must carry the run_id passed to dispatch_attempt_with"
        );
    }

    #[tokio::test]
    async fn run_id_key_absent_when_none() {
        let observed = dispatch_with_probe(None).await;
        assert!(
            !observed.meta.runtime.contains_key("run_id"),
            "no run_id key must be injected when dispatch_attempt_with is called with None"
        );
    }
}
