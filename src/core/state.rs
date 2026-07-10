//! `EngineState` — the single `Mutex`-guarded state object — plus the
//! supporting types.
//!
//! `EngineState` holds every mutable piece of engine flow state (task
//! table, session table, prompts, token records, worker handles, resume
//! table, per-task notifiers, resources, per-attempt output events, and the
//! event log tail). It sits on the Domain side of the Data / Domain split
//! and is unchanged by the Data-plane (`output_store` module) refactor.

use crate::types::{CapToken, Role, SessionId, StepId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Notify};

// ─── Resume / Task ─────────────────────────────────────────────────────────

/// Opaque handle identifying one `query_senior` suspend/`resume` cycle.
/// Stored on `TaskState.suspended_on` and as the key of
/// `EngineState.pending_resumes`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResumeKey(pub String);

impl ResumeKey {
    /// Generate a random key (`RK-<12 hex bytes>`).
    ///
    /// The prefix moved from `R-` to `RK-` in issue #14: `R-` is reserved
    /// for [`crate::types::RunId`], and sharing it would let a resume key
    /// pass a run-id prefix check.
    pub fn new() -> Self {
        Self(format!("RK-{}", crate::types::uid_hex(12)))
    }

    /// Deterministic key for a Senior-escalation suspend on `task_id`
    /// (`RK-senior-<task_id>`), so repeated escalations on the same task
    /// are addressable without extra bookkeeping.
    pub fn for_senior(task_id: &StepId) -> Self {
        Self(format!("RK-senior-{}", task_id))
    }
}

impl Default for ResumeKey {
    fn default() -> Self {
        Self::new()
    }
}

/// Lifecycle state of a task. `Pending` is the only non-terminal,
/// non-`Suspended` state before the first `dispatch_attempt_with`;
/// `Pass` / `Blocked` / `Cancelled` are terminal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Created via `start_task`, not yet dispatched.
    Pending,
    /// A `dispatch_attempt_with` call is in flight for this task.
    Running,
    /// Suspended awaiting a `query_senior`/`resume` round-trip.
    Suspended,
    /// The last attempt completed with `ok = true`.
    Pass,
    /// The last attempt completed with `ok = false` (or dispatch failed).
    Blocked,
    /// Cancelled via `cancel_task`.
    Cancelled,
}

/// Static task definition supplied to `start_task`: which agent runs it
/// and the initial prompt/directive value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    /// Name of the agent that should execute this task.
    pub agent: String,
    /// Prompt/directive value seeded for attempt 1. Passed through
    /// verbatim from the evaluated `Step.in` (issue #18 — no premature
    /// `Value → String` coercion at this layer). Consumers that need a
    /// rendered `String` (the `EngineState.prompts` table feeding the
    /// Worker HTTP path, and the WS `Spawn.directive` reminder text)
    /// render it at their own late boundary; strings pass through
    /// verbatim, anything else is serde-stringified.
    pub initial_directive: Value,
    /// GH #21 Phase 2 — the Step tier's resolved context bundle, threaded
    /// through from `EngineDispatcher::dispatch`'s `$step_meta` envelope
    /// resolution (`None` when the dispatched `Step.in` carried no
    /// envelope — pre-#21-Phase-2 Blueprints unaffected). Re-read from
    /// the spec on EVERY `Engine::dispatch_attempt_with` attempt (not
    /// cached once), so retries and Run-rekicks all carry it; inserted
    /// into `Ctx.meta.runtime[STEP_CTX_KEY]`
    /// (`crate::core::agent_context::STEP_CTX_KEY`), consumed by
    /// `crate::middleware::agent_context::AgentContextMiddleware`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_ctx: Option<Value>,
}

/// The full mutable record of one task: its static `spec`, current
/// `status`, attempt counter, and bookkeeping timestamps. Cloned out of
/// `EngineState` on every read (e.g. by `read_task_state` / `poll_task`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskState {
    /// Unique task identifier (assigned by `start_task`).
    pub id: StepId,
    /// The static spec this task was created from.
    pub spec: TaskSpec,
    /// Current lifecycle status.
    pub status: TaskStatus,
    /// 1-based counter, bumped by `Engine::dispatch_attempt_with` each
    /// time this task is dispatched.
    pub attempt: u32,
    /// Set while `status == Suspended`; the key needed to `resume` it.
    pub suspended_on: Option<ResumeKey>,
    /// Most recent result value posted via `post_result` or produced by a
    /// completed attempt.
    pub last_result: Option<Value>,
    /// Unix timestamp (seconds) when the task was created.
    pub created_at: u64,
    /// Unix timestamp (seconds) of the last state mutation.
    pub updated_at: u64,
    /// Recursive swarm depth. The root (an Operator calling
    /// `start_task`) is 0; a child spawned by a Worker calling
    /// `start_task` is its parent's `depth + 1`. Exceeding
    /// `EngineCfg.max_spawn_depth` raises `SpawnDepthExceeded`.
    #[serde(default)]
    pub spawn_depth: u32,
}

impl TaskState {
    /// Construct a new `Pending` task with `attempt = 0` and
    /// `spawn_depth = 0`; `created_at`/`updated_at` are set to now.
    pub fn new(id: StepId, spec: TaskSpec) -> Self {
        let now = crate::types::now_unix();
        Self {
            id,
            spec,
            status: TaskStatus::Pending,
            attempt: 0,
            suspended_on: None,
            last_result: None,
            created_at: now,
            updated_at: now,
            spawn_depth: 0,
        }
    }
}

/// Result of a `dispatch_attempt_with` call (or the conceptual outcome of
/// a task attempt more broadly).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DispatchOutcome {
    /// The attempt completed with `ok = true`; carries the result value.
    Pass(Value),
    /// The attempt completed with `ok = false`, or dispatch itself failed;
    /// carries the result/error value.
    Blocked(Value),
    /// The task suspended (e.g. via `query_senior`) before completing;
    /// carries the key needed to `resume` it.
    Suspended(ResumeKey),
    /// The task was cancelled before completing.
    Cancelled,
    /// The attempt did not complete within the allotted time.
    Timeout,
}

// ─── Session ───────────────────────────────────────────────────────────────

/// Persisted record of one attached Operator session: identity, role,
/// heartbeat bookkeeping, owned tasks, and the `OperatorKind` cascade
/// inputs plus registry IDs used to rebuild `OperatorInfo` on dispatch
/// (see `Engine::resolve_operator_info`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorSession {
    /// Unique session identifier (distinct from the token nonce).
    pub id: SessionId,
    /// Caller-supplied name identifying the Operator (not necessarily
    /// unique across sessions).
    pub operator_id: String,
    /// Role the session's token was minted with.
    pub role: Role,
    /// Unix timestamp (seconds) when the session was attached.
    pub attached_at: u64,
    /// Unix timestamp (seconds) of the last heartbeat/attach touch.
    pub last_seen: u64,
    /// Whether the session is currently considered live. Flipped to
    /// `false` by `detach` or by `start_detach_loop` on a heartbeat miss.
    pub attached: bool,
    /// Task IDs started by this session (via `start_task` while this
    /// session's token was current).
    pub owned_task_ids: Vec<StepId>,
    /// Fingerprint (`CapToken::fingerprint`, SHA-256 of the nonce) of the
    /// `CapToken` this session was attached with; used to look sessions up
    /// by token in `with_state` closures. Holds the fingerprint rather
    /// than the nonce so the session table carries no secret material
    /// (issue #14).
    pub token_fp: String,
    /// The Operator's `kind`, plus IDs of
    /// the `SeniorBridge` / `SpawnHook` registered on the engine's
    /// `BridgeRegistry`. Persisted (all `String`; no `Arc<dyn ...>`). At
    /// `dispatch_attempt` time the engine looks these up in the registry
    /// and builds an `OperatorInfo` to inject into `Ctx`.
    ///
    /// # 4-tier `OperatorKind` cascade — "Runtime Global" tier
    ///
    /// This field is the literal value passed to `Engine::attach_with_ids`'s
    /// `kind` parameter, and is fed to `crate::core::ctx::collapse_operator_kind`
    /// as the `runtime_global` tier verbatim: `Some(_)` is always an
    /// explicit Runtime Global request that outranks both BP tiers — even
    /// `Some(OperatorKind::Automate)` — and `None` means "not requested",
    /// letting the BP-level tiers (`bp_agent_kinds` / `bp_global_kind`) take
    /// over. `#[serde(default)]` keeps existing persisted sessions (from
    /// before this field existed / was `Option`) deserializing as `None`.
    /// See `crate::core::ctx::collapse_operator_kind` for the full cascade +
    /// rationale.
    #[serde(default)]
    pub operator_kind: Option<crate::core::ctx::OperatorKind>,
    /// "Runtime Agent-level" tier (highest priority) of the `OperatorKind`
    /// cascade — per-agent override supplied at task-launch time via
    /// `TaskLaunchInput.operator_kind_overrides` / `TaskApplicationInput
    /// .operator_kind_overrides`. Keyed by `AgentDef.name`.
    #[serde(default)]
    pub runtime_agent_kinds: HashMap<String, crate::core::ctx::OperatorKind>,
    /// "BP Agent-level" tier of the `OperatorKind` cascade — baked at
    /// `TaskLaunchService::launch` time from `Blueprint.operators[].kind`,
    /// resolved per-agent via `AgentDef.spec.operator_ref`. Keyed by
    /// `AgentDef.name` (not `OperatorDef.name`).
    #[serde(default)]
    pub bp_agent_kinds: HashMap<String, crate::core::ctx::OperatorKind>,
    /// "BP Global" tier of the `OperatorKind` cascade — baked at
    /// `TaskLaunchService::launch` time from `Blueprint.default_operator_kind`.
    #[serde(default)]
    pub bp_global_kind: Option<crate::core::ctx::OperatorKind>,
    /// ID of the `Arc<dyn SeniorBridge>` registered on the engine's
    /// `BridgeRegistry`, if any; resolved back into `OperatorInfo.senior_bridge`.
    #[serde(default)]
    pub bridge_id: Option<String>,
    /// ID of the `Arc<dyn SpawnHook>` registered on the engine's
    /// `BridgeRegistry`, if any; resolved back into `OperatorInfo.spawn_hook`.
    #[serde(default)]
    pub hook_id: Option<String>,
    /// ID of the `Arc<dyn Operator>` registered on the `OperatorRegistry`.
    /// Used by `OperatorDelegateMiddleware` when `kind = MainAi` /
    /// `Composite` and `operator_id` is `Some`: it delegates the entire
    /// spawn to `operator.execute`.
    #[serde(default)]
    pub operator_backend_id: Option<String>,
}

// ─── Token record (= server-side counter holder) ──────────────────────────

/// Server-side counter/state holder paired 1:1 with a minted `CapToken`
/// (keyed by nonce in `EngineState.tokens`). Tracks remaining uses,
/// revocation, and — for Worker tokens — the task the token is bound to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapTokenRecord {
    /// The token this record backs.
    pub token: CapToken,
    /// Remaining number of verb-consuming calls. `None` means unlimited
    /// (session-style tokens); `Some(0)` makes `consume` fail.
    pub uses_left: Option<u32>, // None = unlimited (session)
    /// When `true`, `consume` always fails regardless of `uses_left`.
    pub revoked: bool,
    /// The task this Worker token is bound to (set when minted via
    /// `dispatch_attempt`). Used on two axes:
    ///   1. **Depth tracking.** When a Worker calls `start_task` to spawn a
    ///      child, the child receives this task's `spawn_depth + 1`.
    ///   2. **Ownership gate.** When a Worker calls a state-touch verb
    ///      (`fetch_prompt` / `post_result` / `read_task_state` /
    ///      `cancel_task` / `poll_task`), the argument's `task_id` must
    ///      match this value. `start_task`
    ///      and `dispatch_attempt` are exempt — recursive swarming must
    ///      stay open, and depth is capped by `max_spawn_depth`.
    ///
    ///      Operator tokens (minted at attach time) leave this `None`, so
    ///      they can touch any task.
    #[serde(default)]
    pub task_id: Option<StepId>,
}

impl CapTokenRecord {
    /// Wrap a freshly minted `CapToken` with no bound task (`task_id =
    /// None`) — the shape used for Operator/session tokens.
    pub fn from_token(token: CapToken) -> Self {
        Self {
            uses_left: token.max_uses,
            token,
            revoked: false,
            task_id: None,
        }
    }

    /// Convenience constructor used when minting a Worker token — binds
    /// the record to the target task.
    pub fn from_worker_token(token: CapToken, task_id: StepId) -> Self {
        Self {
            uses_left: token.max_uses,
            token,
            revoked: false,
            task_id: Some(task_id),
        }
    }

    /// Consume one use. `None` (session token) always returns `Ok`;
    /// `Some(0)` returns `Err`.
    pub fn consume(&mut self) -> Result<(), CapTokenConsumeError> {
        if self.revoked {
            return Err(CapTokenConsumeError::Revoked);
        }
        match self.uses_left.as_mut() {
            None => Ok(()),
            Some(0) => Err(CapTokenConsumeError::Exhausted),
            Some(n) => {
                *n -= 1;
                Ok(())
            }
        }
    }
}

/// Why [`CapTokenRecord::consume`] refused to spend a use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CapTokenConsumeError {
    /// The record was explicitly revoked (`revoked = true`); revocation
    /// is permanent and independent of `uses_left`.
    #[error("token revoked")]
    Revoked,
    /// The record's `uses_left` budget (`Some(0)`) is spent.
    #[error("token uses exhausted")]
    Exhausted,
}

// ─── Event ─────────────────────────────────────────────────────────────────

/// Engine lifecycle event. Every event is both appended to
/// `EngineState.event_log_tail` (in-process ring buffer) and broadcast on
/// `Engine::event_tx` for live subscribers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// A session was attached (`attach` / `attach_with` / `attach_with_ids`).
    SessionAttached {
        /// The newly attached session.
        session_id: SessionId,
        /// Role its token was minted with.
        role: Role,
    },
    /// A session was detached (`detach`, or a heartbeat-miss timeout).
    SessionDetached {
        /// The session that was detached.
        session_id: SessionId,
    },
    /// A new task was created via `start_task`.
    TaskCreated {
        /// The newly created task.
        task_id: StepId,
    },
    /// An attempt began dispatching (not currently emitted by
    /// `dispatch_attempt_with`; reserved for future use).
    TaskAttemptStarted {
        /// The task being dispatched.
        task_id: StepId,
        /// The attempt number.
        attempt: u32,
    },
    /// An attempt finished, Pass or Blocked, with the resulting value.
    TaskAttemptCompleted {
        /// The task whose attempt completed.
        task_id: StepId,
        /// The attempt number that completed.
        attempt: u32,
        /// The result value produced by the attempt.
        result: Value,
    },
    /// The task attempt completed with `ok = true`.
    TaskPass {
        /// The task that passed.
        task_id: StepId,
        /// The result value.
        result: Value,
    },
    /// The task attempt completed with `ok = false`.
    TaskBlocked {
        /// The task that was blocked.
        task_id: StepId,
        /// The result/error value.
        result: Value,
    },
    /// A worker appended an `OutputEvent` via `submit_output`.
    WorkerOutput {
        /// The task the output belongs to.
        task_id: StepId,
        /// The attempt the output belongs to.
        attempt: u32,
        /// The appended output event.
        event: crate::worker::output::OutputEvent,
    },
    /// The task suspended pending a `resume` for `key`.
    TaskSuspended {
        /// The suspended task.
        task_id: StepId,
        /// The key needed to `resume` it.
        key: ResumeKey,
    },
    /// The task resumed after `resume(key, ..)` was called.
    TaskResumed {
        /// The resumed task.
        task_id: StepId,
        /// The key that was resumed.
        key: ResumeKey,
    },
    /// The task was cancelled via `cancel_task`.
    TaskCancelled {
        /// The cancelled task.
        task_id: StepId,
    },
    /// `query_senior` was called, asking `question` on behalf of `task_id`.
    SeniorQueried {
        /// The task that triggered the query.
        task_id: StepId,
        /// The question posed to the Senior.
        question: Value,
    },
    /// A Senior's `answer` was stored via `resume`.
    SeniorAnswered {
        /// The task the answer applies to.
        task_id: StepId,
        /// The Senior's answer.
        answer: Value,
    },
}

/// Receiver half of the engine-wide `Event` broadcast channel, obtained
/// via `Engine::subscribe`.
pub type EventStream = broadcast::Receiver<Event>;

// ─── Resume pending (= Notify-based wait + stored answer) ─────────────────

/// Entry for a task suspended via `query_senior`, waiting to be resumed.
///
/// The `Notify` + `answer: Option<Value>` form (rather than a oneshot
/// channel) is deliberate: the answer stays inside `EngineState` even if
/// the caller (an Operator) **detaches and reattaches**, so it can pull
/// the answer out via `await_resume` after reattach.
#[derive(Debug, Clone)]
pub struct ResumePending {
    /// Wakes any `await_resume` waiter once `answer` is set.
    pub notify: Arc<Notify>,
    /// The stored answer, once `resume` has been called for this key.
    pub answer: Option<Value>,
}

impl ResumePending {
    /// Create an unanswered pending entry (fresh `Notify`, `answer = None`).
    pub fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            answer: None,
        }
    }
}

impl Default for ResumePending {
    fn default() -> Self {
        Self::new()
    }
}

// ─── EngineState (= the locked thing) ──────────────────────────────────────

/// One `(task_id, attempt)` entry of [`EngineState::agent_ctx`] — the
/// materialized [`crate::core::agent_context::AgentContextView`] (Contract
/// C, GH #20) and the effective [`mlua_swarm_schema::ContextPolicy`]
/// `AgentContextMiddleware` already applied to it (`projection-adapter`
/// ST5), folded into one struct (GH #23) so the two values — written
/// together at the same single insert site — can no longer drift apart.
#[derive(Debug, Clone, Default)]
pub struct AgentCtxEntry {
    /// The materialized view; the Worker axis's read-back source
    /// (`Engine::fetch_worker_payload{,_trusted}` / `Engine::agent_context_for`).
    pub view: crate::core::agent_context::AgentContextView,
    /// The resolved policy already applied to `view`; read back by
    /// `Engine::context_policy_for` so a consumer can filter a *different*
    /// step's pointer list against this key's policy without re-deriving it
    /// from the Blueprint.
    pub policy: mlua_swarm_schema::ContextPolicy,
}

/// The single `Mutex`-guarded blob of engine flow state, accessed only
/// through `Engine::with_state` (see the R1-R4 discipline documented
/// there).
#[derive(Debug)]
pub struct EngineState {
    /// All known tasks, keyed by `StepId`.
    pub tasks: HashMap<StepId, TaskState>,
    /// All attached/detached sessions, keyed by `SessionId`.
    pub sessions: HashMap<SessionId, OperatorSession>,
    /// Per-`(task_id, attempt)` prompt/directive value, seeded from
    /// `TaskSpec.initial_directive` and fetched via `fetch_prompt`. Held
    /// as `serde_json::Value` end-to-end (issue #18): the render down to
    /// `String` (strings verbatim, anything else serde-stringified)
    /// happens only at the two consumer boundaries — the Worker HTTP
    /// path (`Engine::fetch_worker_payload*` → `WorkerPayload.prompt:
    /// String`) and the WS Spawn frame text render
    /// (`operator_ws::session::default_spawn_directive_with_task_directive`).
    /// Engine-internal storage and `Engine::fetch_prompt` keep the
    /// `Value` end-to-end, so `Step.in` `Object` / `Array` seeds are not
    /// prematurely flattened.
    pub prompts: HashMap<(StepId, u32), Value>,
    /// Per-attempt `system_prompt`: `AgentDef.profile.system_prompt` is
    /// baked at compile time, rendered inside `OperatorSpawner::spawn`,
    /// and stashed here for the SubAgent to fetch alongside its prompt via
    /// `HTTP /v1/worker/prompt`. The value is `Option<String>` so a missing
    /// profile can be distinguished: an absent key means "not yet baked",
    /// while `Some(None)` means "baked and profile is explicitly absent".
    pub systems: HashMap<(StepId, u32), Option<String>>,
    /// GH #31: per-agent-name "most-recently-baked `system_prompt` size"
    /// bookkeeping, updated by `Engine::bake_worker_system_prompt`
    /// whenever `system.is_some()` (keyed by `TaskState.spec.agent`, the
    /// same lookup `fetch_worker_payload` uses). Last-write-wins is the
    /// deliberate semantics — "most-recently-observed", not "largest" or
    /// "per-attempt" — since this exists only to let `bp_doctor`-style
    /// tooling ask "how big does this agent's rendered system prompt
    /// currently run" without plumbing the live `Engine` into
    /// `BlueprintsState`. Read via `Engine::agent_last_rendered_size`.
    pub agent_render_sizes: HashMap<String, usize>,
    /// Per-attempt materialized [`crate::core::agent_context::AgentContextView`]
    /// (Contract C, GH #20) paired with the effective
    /// [`mlua_swarm_schema::ContextPolicy`] `AgentContextMiddleware` already
    /// applied to that same view — the Worker axis's read-back source.
    /// Written by `crate::middleware::agent_context::AgentContextMiddleware`
    /// (innermost spawner layer) at dispatch time via one insert (GH #23:
    /// folded from two separately-keyed maps — see below); read by
    /// `Engine::fetch_worker_payload{,_trusted}` / `Engine::agent_context_for`
    /// (the [`AgentCtxEntry::view`] half, threaded into `WorkerPayload.context`)
    /// and `Engine::context_policy_for` (the [`AgentCtxEntry::policy`] half,
    /// read by `crates/mlua-swarm-server/src/worker.rs`'s `GET
    /// /v1/worker/prompt` handler to filter `WorkerPayload.context.steps` via
    /// `ContextPolicy::allows_step` without re-deriving the policy from the
    /// Blueprint at fetch time). Keyed the same way as `prompts` / `systems`
    /// — `Ctx` itself is not stored, so the entry has to be snapshotted here
    /// to still be servable at fetch time. An absent entry (pre-ST5 spawns,
    /// or a spawner stack that never layered `AgentContextMiddleware`) means
    /// no materialized view and a pass-all policy
    /// (`mlua_swarm_schema::ContextPolicy::default()`) for that key.
    ///
    /// GH #23: this map used to be two separately-keyed maps
    /// (`agent_contexts: HashMap<_, AgentContextView>` /
    /// `context_policies: HashMap<_, ContextPolicy>`) sharing a key and kept
    /// in sync only by convention (the single insert site in
    /// `AgentContextMiddleware` writing both in the same `with_state` call).
    /// Folding both values into one map of an [`AgentCtxEntry`] struct makes
    /// that pairing structural — the two values can no longer drift apart
    /// key-by-key. TODO(GH #23): this map (nor `prompts` / `systems`) still
    /// has no removal path — entries accumulate for the process lifetime; a
    /// long-running server needs a task-completion sweep (tracked
    /// separately from this fold).
    pub agent_ctx: HashMap<(StepId, u32), AgentCtxEntry>,
    /// GH #23: per-dispatch snapshot of the Blueprint-wide
    /// [`crate::core::step_naming::StepNaming`] addressing-space table —
    /// built once by `blueprint::compiler::Compiler::compile` and stashed
    /// here, per `StepId`, by `crate::blueprint::EngineDispatcher::dispatch`
    /// (its single insert site; the same `Arc` is shared across every
    /// Step dispatched from the same Blueprint launch). `Engine::step_naming_for`
    /// reads it back so later consumers (GH #23 subtask-2/3 —
    /// `ContextPolicy.allows_step`, `StepPointer`/`StepSummary` assembly,
    /// the REST `:step` resolver, `FileProjectionAdapter`) do not have to
    /// re-derive the table from the Blueprint at read time. An absent
    /// entry means the dispatcher was never given a `StepNaming` (e.g. a
    /// direct `EngineDispatcher::with_spawner` caller that skipped
    /// `with_step_naming`) — callers fall back to the pre-GH-#23 runtime
    /// union rule in that case.
    pub step_namings: HashMap<StepId, Arc<crate::core::step_naming::StepNaming>>,
    /// GH #27 (follow-up to #23): per-dispatch snapshot of the
    /// Blueprint-wide [`crate::core::projection_placement::ProjectionPlacement`]
    /// resolver — built once by `blueprint::compiler::Compiler::compile`
    /// and stashed here, per `StepId`, by
    /// `crate::blueprint::EngineDispatcher::dispatch` (the same insert
    /// site, and the same "construct once, read many" contract, as
    /// [`Self::step_namings`]). `Engine::projection_placement_for` reads
    /// it back so every one of the 3 materialize call sites (submit-time
    /// sink, server read-back, spawn-time pointer) resolves the SAME root
    /// preference / directory template. An absent entry means the
    /// dispatcher was never given a `ProjectionPlacement` (e.g. a direct
    /// `EngineDispatcher::with_spawner` caller that skipped
    /// `with_projection_placement`) — callers fall back to
    /// `ProjectionPlacement::default()` (byte-compat with the pre-#27
    /// hardcoded layout) in that case.
    pub projection_placements:
        HashMap<StepId, Arc<crate::core::projection_placement::ProjectionPlacement>>,
    /// All minted `CapToken` records, keyed by token fingerprint
    /// (`CapToken::fingerprint` = SHA-256 of the nonce; issue #14 — the
    /// key is loggable, the nonce is not).
    pub tokens: HashMap<String, CapTokenRecord>, // key = token fingerprint
    /// Short worker handle (`wh-XXXXXXXX`, 12 chars) → token-fingerprint
    /// lookup map. Resolves the `worker_handle` field a SubAgent receives
    /// with its prompt. There is no signature verification: `task_id` is
    /// resolved by a plain `HashMap` lookup — deliberately thin for the
    /// local running over WebSocket, and adopted specifically to remove
    /// the base64 copy-paste failure mode.
    pub worker_handles: HashMap<String, String>,
    /// Outstanding `query_senior` suspensions awaiting `resume`.
    pub pending_resumes: HashMap<ResumeKey, ResumePending>,
    /// Per-task notifier — `notify_waiters` fires on every task-status
    /// change. Used by `poll_task` on the caller side, and by callers that
    /// need to `await` again after detach/reattach.
    pub task_notifies: HashMap<StepId, Arc<Notify>>,
    /// Arbitrary named resources set via `set_resource` and read via
    /// `fetch_data`.
    pub resources: HashMap<String, Value>,
    /// Per-attempt output-event log. The `SpawnerAdapter` appends via
    /// `submit_output`; the dispatch path pulls the terminal
    /// `OutputEvent::Final` off the tail and decides Pass / Blocked.
    pub output_store: HashMap<(StepId, u32), Vec<crate::worker::output::OutputEvent>>,
    /// GH #36 ST1 (named multi-part worker output): the set of `Artifact`
    /// names staged via `Engine::stage_worker_artifact_trusted` (= `POST
    /// /v1/worker/artifact`) for `(task_id, attempt)`, in staging order.
    ///
    /// `output_store` is a SHARED per-attempt tail — besides a worker's own
    /// staged parts, other producers append `OutputEvent::Artifact` events
    /// to the SAME tail too (e.g. `AfterRunAuditMiddleware`'s
    /// `"audit:<step_ref>"` sidecar finding, an intentionally
    /// BP-chain-invisible observation — see `Engine::submit_output`'s doc,
    /// "`Artifact` dual-write" section). `Engine::dispatch_attempt_with`'s
    /// Final-pull assembly must fold ONLY a worker's own named parts into
    /// `"parts"`, not every `Artifact` that happens to land on the tail —
    /// this set is that distinguishing signal, so a step under audit keeps
    /// its BP-chain value byte-identical to pre-GH-#36 unless the WORKER
    /// itself opted in via `/v1/worker/artifact`.
    pub worker_artifact_names: HashMap<(StepId, u32), Vec<String>>,
    /// Bounded in-process tail of recent `Event`s (most recent last),
    /// trimmed to `event_log_max` by `push_event`.
    pub event_log_tail: Vec<Event>,
    /// Maximum length of `event_log_tail` before older entries are
    /// dropped.
    pub event_log_max: usize,
}

impl EngineState {
    /// Construct an empty `EngineState` with `event_log_max = 1024`.
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            sessions: HashMap::new(),
            prompts: HashMap::new(),
            systems: HashMap::new(),
            agent_render_sizes: HashMap::new(),
            agent_ctx: HashMap::new(),
            step_namings: HashMap::new(),
            projection_placements: HashMap::new(),
            tokens: HashMap::new(),
            worker_handles: HashMap::new(),
            pending_resumes: HashMap::new(),
            task_notifies: HashMap::new(),
            resources: HashMap::new(),
            output_store: HashMap::new(),
            worker_artifact_names: HashMap::new(),
            event_log_tail: Vec::new(),
            event_log_max: 1024,
        }
    }

    /// Ensure a per-task `Notify` exists; return the existing one if any.
    pub fn ensure_task_notify(&mut self, task_id: &StepId) -> Arc<Notify> {
        self.task_notifies
            .entry(task_id.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Append `ev` to `event_log_tail`, trimming the oldest entries once
    /// `event_log_max` is exceeded.
    pub fn push_event(&mut self, ev: Event) {
        self.event_log_tail.push(ev);
        if self.event_log_tail.len() > self.event_log_max {
            let overflow = self.event_log_tail.len() - self.event_log_max;
            self.event_log_tail.drain(..overflow);
        }
    }
}

impl Default for EngineState {
    fn default() -> Self {
        Self::new()
    }
}
