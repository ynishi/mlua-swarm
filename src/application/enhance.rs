//! `EnhanceApplication` — the dispatcher for the `POST /v1/issues`
//! path.
//!
//! The entry point (`POST /v1/issues`) only does `IssueStore.create` —
//! a synchronous enqueue. The actual dispatch is drained by a
//! consumer loop calling `tick()`:
//!
//! ```text
//! POST /v1/issues ──→ IssueStore (queue)
//!                            ↓
//!     consumer loop (tokio::spawn) ── tick() ──┐
//!                                              ↓
//!                  IssueStore.pop_pending + EnhanceSettingStore.get
//!                                              ↓
//!                  BPStore.read_head(setting.blueprint.id)      (fetched on use)
//!                                              ↓
//!                  TaskLaunchService.launch(...)                (engine bind + attach + start_task)
//!                       the drain stops waiting on this after
//!                       EnhanceSetting.ttl_secs                  (see the ceiling block below)
//! ```
//!
//! One tick is one epoch. `EnhanceSetting.ttl_secs` is **not** a ceiling on
//! that epoch: it is a ceiling on how long the drain waits for one call —
//! `TaskLaunchService::launch` — and nothing else. Two consequences, both
//! load-bearing, both spelled out in the ceiling block in
//! [`EnhanceApplication::dispatch_one`]:
//!
//! - **It bounds the wait, which is the thing it was built to bound.** The
//!   knob exists so the Swarm can give up on an Operator that stopped
//!   answering (model §4.4 **R5** places the bound in infra, not in the
//!   model), and that wait is an await point, so dropping the launch future
//!   releases it. It does **not** stop a worker already running in an
//!   in-process lane: those run in `tokio::spawn`ed tasks that this future
//!   does not own, and nothing in this repository fires the
//!   `CancellationToken` they select on. The drain is unwedged either way;
//!   the work may still be running behind it, which is what the reason text
//!   tells the operator to check before re-posting.
//! - **It does not span the epoch.** The store calls on either side of
//!   `launch` — `setting_store.get`, `resolve_blueprint`,
//!   `bp_store.read_head`, `bp_store.write_new`, `log_store.append`, and
//!   `issue_store.update_status` in both `tick` arms — are outside it and
//!   are bounded by nothing.
//!
//! So the ceiling removes exactly one wedge — a `patch-spawner` whose call
//! never returns — and leaves every other way to stall the single-threaded
//! drain in place.
//!
//! Current scope:
//!
//! - Engine task-completion → `Issue.update_status` is a carry.
//! - Setting `VersionSelector` (`Fixed` / `Latest` / `SemverReq`) is
//!   a carry — today we always use `BPStore.read_head`.
//! - The agent-selection convention is
//!   `setting.blueprint.agents.first().name`.

use super::semver_resolve::SemverResolveError;
use super::{Application, VersionSelector};
use crate::blueprint::store::{
    blueprint_version, BlueprintEpoch, BlueprintId, BlueprintStore, BlueprintStoreError,
    CommitMetadata, ContentHash, Traced,
};
use crate::blueprint::{AgentDef, Blueprint};
use crate::core::errors::EngineError;
use crate::enhance::blueprint::AG_PATCH_SPAWNER;
use crate::service::{TaskLaunchError, TaskLaunchInput, TaskLaunchOutput, TaskLaunchService};
use crate::store::enhance_log::{
    EnhanceLogEntry, EnhanceLogStore, EnhanceLogStoreError, VerdictSummary,
};
use crate::store::enhance_setting::{
    EnhanceSettingId, EnhanceSettingStore, EnhanceSettingStoreError,
};
use crate::store::issue::{IssueId, IssuePayload, IssueStatus, IssueStore, IssueStoreError};
use crate::types::Role;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Failure modes of [`EnhanceApplication::tick`] and the internal
/// `dispatch_one` step it wraps.
#[derive(Debug, Error)]
pub enum EnhanceApplicationError {
    /// The `IssueStore` returned an error (enqueue, pop, or status
    /// update).
    #[error("issue store: {0}")]
    Issue(#[from] IssueStoreError),

    /// The `EnhanceSettingStore` returned an error while fetching the
    /// active setting.
    #[error("setting store: {0}")]
    Setting(#[from] EnhanceSettingStoreError),

    /// The `BlueprintStore` returned an error while resolving the
    /// orbit or target Blueprint.
    #[error("blueprint store: {0}")]
    Bp(#[from] BlueprintStoreError),

    /// The `EnhanceLogStore` returned an error while appending the
    /// outcome entry.
    #[error("enhance log store: {0}")]
    Log(#[from] EnhanceLogStoreError),

    /// `TaskLaunchService::launch` failed after setup succeeded.
    #[error("launch: {0}")]
    Launch(#[from] TaskLaunchError),

    /// Serializing the target Blueprint (or a directive derived from
    /// it) to JSON/YAML failed.
    #[error("serialize directive: {0}")]
    Serialize(#[from] serde_json::Error),

    /// A stored version's `version_label` is not valid semver.
    #[error("invalid semver version_label {label:?}: {source}")]
    InvalidSemver {
        /// The offending label string.
        label: String,
        /// The underlying semver parse error.
        #[source]
        source: semver::Error,
    },

    /// No stored version's label satisfies the setting's `SemverReq`.
    #[error("no version matches semver req: {req}")]
    NoMatchingVersion {
        /// The requirement string that matched nothing.
        req: String,
    },

    /// The engine reported an error (attach / dispatch).
    #[error("engine: {0}")]
    Engine(#[from] EngineError),

    /// `final_ctx.commit` did not match the strict shape
    /// `extract_commit` expects, or the committer/store hashes
    /// disagreed.
    #[error("commit shape: {0}")]
    CommitShape(String),

    /// The system clock reported a time before the UNIX epoch while
    /// computing `now_ms`.
    #[error("system time before UNIX epoch: {0}")]
    Clock(#[from] std::time::SystemTimeError),

    /// The setting carries an `EnhanceSetting::spawner` override, but the
    /// orbit Blueprint declares no agent under the name the flow's
    /// `Step.ref` points at. Fail loud: silently ignoring the override
    /// would run the Blueprint's own spawner while the operator believes
    /// the swap took effect.
    #[error("spawner override: orbit blueprint declares no agent named {name:?}")]
    SpawnerAgentNotFound {
        /// The agent name the override targets (= the flow `Step.ref`).
        name: String,
    },

    /// The drain stopped waiting on `TaskLaunchService::launch` after
    /// `EnhanceSetting::ttl_secs` and dropped it. Carries the number it blew
    /// through so the reason text an operator reads names the knob they can
    /// raise, not just the fact that something stopped.
    ///
    /// Two facts the reason text has to carry, because an operator acts on
    /// it (see the ceiling block in `dispatch_one` for the evidence):
    ///
    /// - Nothing was committed. That one is structural, not best-effort.
    /// - Only the wait ended. A worker already running in an in-process lane
    ///   keeps running — this ceiling bounds an Operator wait, and those
    ///   lanes are not waits. So a re-post can put a second writer under the
    ///   same `project_root` as the first, which is the one thing an
    ///   operator must not do by reflex on reading "timed out".
    #[error(
        "enhance epoch exceeded the {ttl_secs}s ceiling declared by enhance setting \
         {setting_id:?} (ttl_secs); nothing was committed and the target Blueprint is \
         unchanged. Only the wait ended: a worker already running in an in-process lane \
         is still running, and re-posting now would put a second writer under the same \
         project_root. Check that it has exited (and what it left there) before \
         re-posting, and raise ttl_secs if the epoch legitimately needs longer"
    )]
    EpochCeilingExceeded {
        /// The `EnhanceSettingId` whose `ttl_secs` bounded this epoch.
        setting_id: String,
        /// The ceiling in seconds, as declared by the setting.
        ttl_secs: u64,
    },

    /// `EnhanceSetting::ttl_secs` is `0`.
    ///
    /// Zero is refused rather than read as "no ceiling". This repo has no
    /// zero-means-unbounded TTL: `mse serve` refuses
    /// `worker_token_ttl_secs: 0` at startup and `POST /v1/tasks` rejects
    /// `timeout_secs: 0` with a `400`, both on the same ground — a zero TTL
    /// is a typo, not a policy. Inventing the sentinel here would recreate
    /// the exact defect this ceiling exists to remove: a field whose value
    /// does not mean what the field says it means.
    ///
    /// The guard belongs one layer out, at `POST /v1/enhance-settings`,
    /// where it could be a `400` at write time instead of a rejected issue
    /// at dispatch time. It lives here because dispatch is the last place
    /// that can still refuse to run an epoch it would abort on its first
    /// poll.
    #[error(
        "enhance setting {setting_id:?} declares ttl_secs: 0, which would abort every epoch \
         before its first step completes; set ttl_secs to the number of seconds one epoch \
         may run"
    )]
    ZeroTtl {
        /// The `EnhanceSettingId` carrying the zero.
        setting_id: String,
    },
}

impl From<SemverResolveError> for EnhanceApplicationError {
    fn from(e: SemverResolveError) -> Self {
        match e {
            SemverResolveError::Store(e) => EnhanceApplicationError::Bp(e),
            SemverResolveError::InvalidSemver { label, source } => {
                EnhanceApplicationError::InvalidSemver { label, source }
            }
            SemverResolveError::NoMatchingVersion { req } => {
                EnhanceApplicationError::NoMatchingVersion { req }
            }
        }
    }
}

/// Result of a single `tick`. `task_id` is gone — the flow-eval path
/// runs many steps to completion instead of being tied to a single
/// task id, so the entire `final_ctx` is the result. Outcomes are
/// checked through `status`.
#[derive(Debug, Clone)]
pub struct TickOutcome {
    /// The issue that was popped and dispatched this tick.
    pub issue_id: IssueId,
    /// The resulting status persisted to the `IssueStore`.
    pub status: IssueStatus,
}

/// Configuration parameters for `EnhanceApplication`.
///
/// `ttl` moved onto `EnhanceSetting` so editing the setting acts as
/// a hot reload — including the epoch ceiling, which is re-read from the
/// setting on every tick rather than frozen at construction. This
/// `Config` only holds the identity information needed to stand up an
/// Application instance.
pub struct EnhanceApplicationConfig {
    /// A short identifier for this Application instance (used in logs).
    pub name: String,
    /// The `EnhanceSetting` this instance reads on every tick.
    pub setting_id: EnhanceSettingId,
    /// The Operator id attached for every dispatched task.
    pub operator_id: String,
    /// The Operator's role for every dispatched task.
    pub role: Role,
}

/// The `POST /v1/issues` dispatcher — enqueues via [`Application::handle`],
/// drains via [`EnhanceApplication::tick`] / [`EnhanceApplication::run_forever`].
pub struct EnhanceApplication {
    name: String,
    setting_id: EnhanceSettingId,
    operator_id: String,
    role: Role,
    issue_store: Arc<dyn IssueStore>,
    setting_store: Arc<dyn EnhanceSettingStore>,
    bp_store: Arc<dyn BlueprintStore>,
    log_store: Arc<dyn EnhanceLogStore>,
    launch: Arc<TaskLaunchService>,
}

impl EnhanceApplication {
    /// Wire up an `EnhanceApplication` from its config and store/service
    /// dependencies.
    pub fn new(
        cfg: EnhanceApplicationConfig,
        issue_store: Arc<dyn IssueStore>,
        setting_store: Arc<dyn EnhanceSettingStore>,
        bp_store: Arc<dyn BlueprintStore>,
        log_store: Arc<dyn EnhanceLogStore>,
        launch: Arc<TaskLaunchService>,
    ) -> Self {
        Self {
            name: cfg.name,
            setting_id: cfg.setting_id,
            operator_id: cfg.operator_id,
            role: cfg.role,
            issue_store,
            setting_store,
            bp_store,
            log_store,
            launch,
        }
    }

    /// The `IssueStore` this Application enqueues into and drains from.
    pub fn issue_store(&self) -> &Arc<dyn IssueStore> {
        &self.issue_store
    }

    /// The `BlueprintStore` used to resolve orbit/target Blueprints and
    /// to persist Applied commits.
    pub fn bp_store(&self) -> &Arc<dyn BlueprintStore> {
        &self.bp_store
    }

    /// The `EnhanceLogStore` every dispatch outcome is appended to.
    pub fn log_store(&self) -> &Arc<dyn EnhanceLogStore> {
        &self.log_store
    }

    /// Pop one pending issue and dispatch it to the engine. Returns
    /// `None` when nothing is pending.
    ///
    /// `dispatch_one` returns `Err` only for **infra faults** — store,
    /// launch, clock, shape errors, and the like. Flow verifier denials
    /// come back through `dispatch_one` on the `Ok` path with a
    /// `Rejected` status, and the corresponding entry has already been
    /// appended to `log_store` in the same commit. Even on an infra
    /// fault, `tick` best-effort tries to update the store-side
    /// status; if the store itself is broken the error propagates.
    pub async fn tick(&self) -> Result<Option<TickOutcome>, EnhanceApplicationError> {
        let Some(payload) = self.issue_store.pop_pending().await? else {
            return Ok(None);
        };
        match self.dispatch_one(&payload).await {
            Ok(status) => {
                self.issue_store
                    .update_status(&payload.issue_id, status.clone())
                    .await?;
                Ok(Some(TickOutcome {
                    issue_id: payload.issue_id,
                    status,
                }))
            }
            Err(e) => {
                // Infra fault: record status as Rejected, then propagate Err.
                let reason = format!("dispatch failed: {e}");
                self.issue_store
                    .update_status(&payload.issue_id, IssueStatus::Rejected { reason })
                    .await?;
                Err(e)
            }
        }
    }

    /// Handle one issue as one enhance-flow completion.
    ///
    /// Flow:
    /// 1. Fetch the setting (the enhance-orbit BP id, `verifier_axes`,
    ///    and `ttl_secs`).
    /// 2. Resolve the orbit BP (for example the built-in
    ///    `enhance-default` flow), then apply the setting's
    ///    `spawner` override to it when one is declared.
    /// 3. Resolve the target BP (`payload.blueprint_id`) — the
    ///    object being modified, injected into `init_ctx` as
    ///    `prev_bp`.
    /// 4. Assemble `init_ctx` (`issue` / `prev_bp_yaml` / `prev_hash`
    ///    / `epoch_id` / `verifiers`).
    /// 5. Run to completion via `TaskLaunchService::launch`, waiting at
    ///    most `ttl_secs` for it — pull `final_ctx` once every step
    ///    finishes, or give up waiting with
    ///    [`EnhanceApplicationError::EpochCeilingExceeded`]. Giving up does
    ///    not stop the flow; see the ceiling block below.
    /// 6. Derive `IssueStatus` from `final_ctx.commit`; when Applied,
    ///    persist via `bp_store.write_new`.
    /// 7. Append a `LogEntry` to `log_store` — exactly one entry per
    ///    outcome, Applied or Rejected.
    async fn dispatch_one(
        &self,
        payload: &IssuePayload,
    ) -> Result<IssueStatus, EnhanceApplicationError> {
        let setting = self.setting_store.get(&self.setting_id).await?;

        // Refuse a zero ceiling before touching a store. Every step below
        // this point costs work an epoch with a 0s ceiling could only throw
        // away on its first poll, and a `ZeroTtl` reason names the setting
        // field to fix where an `EpochCeilingExceeded` reason would just
        // report a stopwatch. See that variant's doc for why `0` is not
        // read as "unbounded".
        if setting.ttl_secs == 0 {
            return Err(EnhanceApplicationError::ZeroTtl {
                setting_id: self.setting_id.to_string(),
            });
        }

        let mut traced_orch = self
            .resolve_blueprint(&setting.blueprint_id, &setting.version)
            .await?;
        apply_spawner_override(&mut traced_orch.value, setting.spawner.as_ref())?;

        let traced_target = self.bp_store.read_head(&payload.blueprint_id).await?;
        let prev_bp_yaml = serde_yaml::to_string(&traced_target.value).map_err(|e| {
            EnhanceApplicationError::Serialize(serde::ser::Error::custom(format!(
                "prev_bp yaml: {e}"
            )))
        })?;
        let prev_version = blueprint_version(&traced_target.value).map_err(|e| {
            EnhanceApplicationError::Serialize(serde::ser::Error::custom(format!("prev_hash: {e}")))
        })?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64;
        let epoch = BlueprintEpoch::new(payload.blueprint_id.clone(), prev_version, now_ms);
        let prev_hash_hex = hex::encode(prev_version.0 .0);

        let init_ctx = serde_json::json!({
            "issue": {
                "issue_id":     payload.issue_id.as_str(),
                "blueprint_id": payload.blueprint_id.as_str(),
                "intent":       payload.intent,
            },
            "prev_bp_yaml": prev_bp_yaml,
            "prev_hash":    prev_hash_hex.clone(),
            "epoch_id":     epoch.clone(),
            "verifiers":    setting.verifier_axes.clone(),
        });

        // ── The epoch ceiling ────────────────────────────────────────────
        //
        // `ttl` is handed to the launch twice, and only one of the two does
        // anything.
        //
        // Inside `launch` it is stamped onto the minted `Role::Operator`
        // token's `expire_at`, where `82d9da9` ("stop expiring operator
        // tokens") made `Engine::verify_token` skip the expiry check for
        // that role. That exemption is correct and is not being walked
        // back: the operator session token never leaves the process, so its
        // TTL bounded no reachable capability — all it did was fail the
        // *next* legitimate `start_task` once a single step outlived the
        // attach. What the exemption left behind is the reason this block
        // exists: a number the author sets, still accepted, still
        // serialized, gating nothing.
        //
        // Around `launch` it bounds how long this dispatcher waits for the
        // flow to come back. The `/v1/tasks` detached path has carried the
        // same `tokio::time::timeout` wrap around its driver since GH #37
        // (`run_flow_form`, the `if detach` arm); the enhance consumer loop
        // had nothing equivalent, so a `patch-spawner` whose model call hung
        // wedged `tick()` forever and with it the whole single-threaded drain
        // — every later issue stuck `pending` behind one stalled epoch.
        // Reusing that mechanism rather than inventing a second one keeps one
        // answer to "what unwedges the drain" in the codebase.
        //
        // Be precise about what that buys, because the obvious reading is
        // wrong twice over.
        //
        // (1) What it bounds is waiting — which is what it was built to
        // bound. This TTL exists so the Swarm can give up on an Operator
        // that has stopped answering; model §4.4 **R5** puts the bound here
        // deliberately ("model は待ち時間の上限を規定しない — 上限は infra が
        // 持ち config で伸ばす"). That wait is an await point:
        // `WSOperatorSession::send_and_await` parks on `orx.await`
        // (`crates/mlua-swarm-server/src/operator_ws/session.rs`), so
        // dropping this future releases it where it stands. For the lane
        // this knob names, the ceiling does exactly what it says.
        //
        // It does not reach work that is not a wait, and is not meant to.
        // `tokio::time::timeout` polls the inner future first and returns
        // `Ok` the moment it is `Ready`, deadline or not, so a step that
        // never awaits cannot be interrupted by it at all; and the
        // in-process lanes (`InProcSpawner`, the agent-block orbit, the
        // subprocess backend) run their work in a `tokio::spawn`ed task
        // that is not a child of this future, so dropping the wait does not
        // reach them either. Read that as a property of those lanes rather
        // than a hole in the ceiling: a computation running in this process
        // is not an unresponsive Operator, which is the thing this TTL was
        // put here to see through.
        //
        // (2) It covers `launch` and nothing else. Outside it, and bounded
        // by nothing: `setting_store.get`, `resolve_blueprint`, and
        // `bp_store.read_head` before; `bp_store.write_new` and
        // `log_store.append` after; `issue_store.update_status` in both of
        // `tick`'s arms. A `BlueprintStore` blocking on git index-lock
        // contention therefore wedges the drain exactly as a hung spawner
        // used to.
        //
        // Widening the wrap to span the epoch was considered and rejected on
        // two counts. It would not bound those store calls:
        // `Git2BlueprintStore` (`src/blueprint/store/git2_store.rs`) contains
        // no `.await` and no `spawn_blocking` in the whole file, so its
        // `async fn`s do their git2 work inline and return `Ready` on the
        // first poll — by (1), a wrap around them can never fire, which
        // would add a second knob that does nothing and recreate the exact
        // defect this ceiling exists to remove. And a wrap that *could* fire
        // around `write_new` would let the ceiling land mid-commit,
        // destroying the one guarantee below that is real. The honest fix
        // for the store calls is to make them cancel-safe (`spawn_blocking`)
        // first; until then this is a KNOWN LIMITATION, stated rather than
        // papered over.
        //
        // Two alternatives were rejected. Deprecating the field and warning
        // on use (the cheap fix) removes the lie but leaves the enhance
        // orbit unbounded, so it trades a misleading knob for a missing
        // one. Reaping expired operator sessions on a timer would restore
        // the TTL's *original* meaning, but that is precisely the misfire
        // `82d9da9` deleted — killing a session out from under a step that
        // is merely slow — and would reintroduce it by the back door.
        //
        // STORE STATE after a fired ceiling: nothing partial. This epoch's
        // only write to the `BlueprintStore` is the single `write_new` in
        // the Applied arm below, strictly after `launch` returns, so a
        // ceiling that fires can only fire before it — the target
        // Blueprint's head is byte-identical to what it was when the issue
        // was popped, and there is no half-written version for a later epoch
        // to reconcile against. The abort is reported as an infra fault, so
        // `tick`'s `Err` arm marks the issue terminal `Rejected` with the
        // reason text above and nothing is appended to `log_store` —
        // consistent with every other infra fault, and it keeps the log's
        // invariant that an entry carries per-axis verdicts (a timed-out
        // epoch has none to carry). Recovery is re-posting the issue, not
        // resuming: this launch passes `run_ctx: None`, so no `RunRecord`
        // and no replay log exist for `POST /v1/runs/:id/resume` to address.
        // A re-post starts from the same `prev_hash` precisely because
        // nothing was committed.
        //
        // That claim is about stores. Execution is a separate claim, and a
        // weaker one.
        //
        // EXECUTION after a fired ceiling: the wait ends, the work may not.
        // The Operator lane's wait is released, per (1). A worker already
        // running inside one of the in-process lanes is not: `InProcSpawner`
        // (`src/worker/adapter.rs`), the agent-block orbit
        // (`src/worker/agent_block/runtime.rs`) and the subprocess backend
        // (`src/worker/process_spawner.rs`) each run their work in a
        // `tokio::spawn`ed task, and each selects on a `CancellationToken`
        // that **nothing in this repository fires** — `Engine::cancel_task`
        // (`src/core/engine.rs`) sets `TaskStatus::Cancelled` and wakes
        // pollers without touching it. So such a worker runs to its own end,
        // and its `submit_output` lands against an epoch nobody is reading
        // (inert here: `TaskLaunchInput::automate` sets `task_input: None`,
        // so the file-materialize half writes nothing).
        //
        // The consequence an operator has to act on: **a re-post can overlap
        // the previous epoch's worker**, with both writing under the same
        // `project_root`. Check that the earlier one has exited before
        // re-posting, or raise `ttl_secs` so the epoch is not cut short to
        // begin with. `mse://guides/enhance-flow` carries this as the
        // recovery procedure.
        //
        // Making the ceiling reach those lanes would mean giving this
        // process a drop-cancels-work semantics, which is a much larger
        // change than a TTL on an Operator wait and is not what this knob
        // asked for. It is recorded here as the boundary, not as a to-do.
        //
        // Mutations that DO survive the abort, named so the paragraph above
        // is not read as "and nothing else": `engine.register_verdict_contracts`
        // (`src/service/task_launch.rs`) persists, and the operator session
        // attached during launch is never detached, because the token lives
        // in the `TaskLaunchOutput` of the future being dropped. That
        // session leak is pre-existing and shared with the success path,
        // which never detaches either (`token: _` below).
        let ttl = Duration::from_secs(setting.ttl_secs);
        let launch = self.launch.launch(TaskLaunchInput::automate(
            traced_orch.value,
            self.operator_id.clone(),
            self.role,
            ttl,
            init_ctx,
        ));
        let TaskLaunchOutput {
            token: _,
            final_ctx,
        } = match tokio::time::timeout(ttl, launch).await {
            Ok(launched) => launched?,
            Err(_elapsed) => {
                tracing::warn!(
                    issue_id = %payload.issue_id,
                    blueprint_id = %payload.blueprint_id,
                    setting_id = %self.setting_id,
                    ttl_secs = setting.ttl_secs,
                    prev_hash = %prev_hash_hex,
                    "enhance epoch hit its ttl_secs ceiling; this dispatcher stopped waiting \
                     and nothing was committed. A worker already running in an in-process \
                     lane is NOT stopped by this — it runs to its own end, so check that it \
                     has exited (and what it left under project_root) before re-posting, or \
                     raise ttl_secs"
                );
                return Err(EnhanceApplicationError::EpochCeilingExceeded {
                    setting_id: self.setting_id.to_string(),
                    ttl_secs: setting.ttl_secs,
                });
            }
        };

        // Strict commit extract (no 1-value default; missing required fields surface as Err).
        let commit_decision = extract_commit(&final_ctx)?;

        // When Applied, persist via bp_store.write_new (the core GOAL IO path).
        let (status, log_entry) = match commit_decision {
            CommitDecision::Applied {
                new_bp,
                new_version_hex,
                rationale,
                bump,
                verdicts,
            } => {
                let patch_hash = ContentHash::from_bytes(rationale.as_bytes());
                let metadata = CommitMetadata {
                    epoch_id: epoch.clone(),
                    rationale: rationale.clone(),
                    patch_hash,
                };
                let new_version = self
                    .bp_store
                    .write_new(
                        &payload.blueprint_id,
                        &new_bp,
                        std::slice::from_ref(&prev_version),
                        metadata,
                    )
                    .await?;
                let new_version_hex_actual = hex::encode(new_version.0 .0);
                // If commit.new_version (the committer-computed hash) disagrees with the
                // version assigned by bp_store, the canonicalisation is out of sync — Err.
                if new_version_hex_actual != new_version_hex {
                    return Err(EnhanceApplicationError::CommitShape(format!(
                        "new_version mismatch: committer={new_version_hex} store={new_version_hex_actual}"
                    )));
                }
                let entry = EnhanceLogEntry {
                    issue_id: payload.issue_id.clone(),
                    blueprint_id: payload.blueprint_id.clone(),
                    prev_hash: prev_hash_hex.clone(),
                    new_hash: new_version_hex_actual.clone(),
                    intent: payload.intent.clone(),
                    rationale: rationale.clone(),
                    verdicts,
                    status: "applied".into(),
                    reasons: vec![],
                    ts_ms: now_ms,
                };
                // CommitMetadata does not carry the bump label; surface it in
                // the trace so the committer's version decision is observable.
                tracing::info!(%bump, issue_id = %payload.issue_id, "commit bump label (not persisted in CommitMetadata)");
                (
                    IssueStatus::Applied {
                        new_version: new_version_hex_actual,
                    },
                    entry,
                )
            }
            CommitDecision::Rejected {
                reasons,
                rationale,
                verdicts,
            } => {
                let entry = EnhanceLogEntry {
                    issue_id: payload.issue_id.clone(),
                    blueprint_id: payload.blueprint_id.clone(),
                    prev_hash: prev_hash_hex.clone(),
                    new_hash: String::new(),
                    intent: payload.intent.clone(),
                    rationale,
                    verdicts,
                    status: "rejected".into(),
                    reasons: reasons.clone(),
                    ts_ms: now_ms,
                };
                (
                    IssueStatus::Rejected {
                        reason: format!("verifier deny: {}", reasons.join("; ")),
                    },
                    entry,
                )
            }
        };

        self.log_store.append(log_entry).await?;
        Ok(status)
    }

    /// Resolve a BP per the `VersionSelector`. `Latest` uses
    /// `read_head`; `Fixed` uses `read_version`; `SemverReq` scans the
    /// history and picks the semver-matching
    /// `BlueprintMetadata.version_label`.
    async fn resolve_blueprint(
        &self,
        bp_id: &BlueprintId,
        selector: &VersionSelector,
    ) -> Result<Traced<Blueprint>, EnhanceApplicationError> {
        match selector {
            VersionSelector::Latest => Ok(self.bp_store.read_head(bp_id).await?),
            VersionSelector::Fixed { value } => {
                Ok(self.bp_store.read_version(bp_id, *value).await?)
            }
            VersionSelector::SemverReq { req } => {
                let v = super::semver_resolve::resolve_semver(self.bp_store.as_ref(), bp_id, req)
                    .await?;
                Ok(self.bp_store.read_version(bp_id, v).await?)
            }
        }
    }

    /// The consumer loop. At server startup, launch it with
    /// `tokio::spawn(app.run_forever(interval))`; stop it with
    /// `JoinHandle::abort()`.
    ///
    /// Behaviour:
    ///
    /// - `tick()` returns `Some` → immediately run another tick (burst
    ///   drain).
    /// - `tick()` returns `None` → sleep for `interval` (no-work
    ///   back-off).
    /// - `tick()` returns `Err` → log it and sleep for `interval`
    ///   (a dispatch failure must not kill the loop).
    pub async fn run_forever(self: Arc<Self>, interval: Duration) {
        loop {
            match self.tick().await {
                Ok(Some(_)) => continue,
                Ok(None) => tokio::time::sleep(interval).await,
                Err(e) => {
                    eprintln!("[{}] tick error: {e}", self.name);
                    tokio::time::sleep(interval).await;
                }
            }
        }
    }
}

/// Input to [`EnhanceApplication::handle`] — the `POST /v1/issues` request
/// body once decoded.
#[derive(Debug, Clone)]
pub struct EnhanceApplicationInput {
    /// The Blueprint this issue proposes to modify.
    pub blueprint_id: BlueprintId,
    /// Free-form description of the change being requested.
    pub intent: String,
    /// Caller-supplied issue id, echoed back as `handle`'s `Output`.
    pub issue_id: IssueId,
}

/// Swap the orbit Blueprint's [`AG_PATCH_SPAWNER`] agent for the
/// definition the setting declares.
///
/// `spawner = None` leaves the Blueprint untouched — whatever it
/// declares is what runs (the pre-override behaviour, byte-for-byte).
/// `Some(def)` replaces the matching entry in `blueprint.agents` in
/// place, which is the whole point of the knob: the spawner's execution
/// backend can be changed without rewriting the Blueprint.
///
/// Two deliberate strictnesses:
///
/// - No agent under that name → `Err`. The flow step references the
///   agent by name, so a missing target means the override is inert; a
///   silent no-op here is the worst possible way for that to surface.
/// - The swapped-in `name` is forced back to [`AG_PATCH_SPAWNER`], so a
///   caller supplying a differently-named `AgentDef` cannot break the
///   flow's `Step.ref` wiring.
///
/// The mutation is scoped to the in-memory copy used for this dispatch —
/// nothing is written back to the `BlueprintStore`.
fn apply_spawner_override(
    blueprint: &mut Blueprint,
    spawner: Option<&AgentDef>,
) -> Result<(), EnhanceApplicationError> {
    let Some(spawner) = spawner else {
        return Ok(());
    };
    let slot = blueprint
        .agents
        .iter_mut()
        .find(|a| a.name == AG_PATCH_SPAWNER)
        .ok_or_else(|| EnhanceApplicationError::SpawnerAgentNotFound {
            name: AG_PATCH_SPAWNER.to_string(),
        })?;
    let mut swapped = spawner.clone();
    swapped.name = AG_PATCH_SPAWNER.to_string();
    *slot = swapped;
    Ok(())
}

/// Internal verdict produced by strictly parsing `committer.lua`'s
/// output (`ctx.commit`).
///
/// Strict discipline: missing required fields or wrong types surface
/// as `CommitShape` errors — no 1-value defaulting.
enum CommitDecision {
    Applied {
        new_bp: Box<Blueprint>,
        new_version_hex: String,
        rationale: String,
        bump: String,
        verdicts: Vec<VerdictSummary>,
    },
    Rejected {
        reasons: Vec<String>,
        rationale: String,
        verdicts: Vec<VerdictSummary>,
    },
}

fn extract_commit(
    final_ctx: &serde_json::Value,
) -> Result<CommitDecision, EnhanceApplicationError> {
    let shape_err =
        |msg: String| -> EnhanceApplicationError { EnhanceApplicationError::CommitShape(msg) };

    let commit = final_ctx
        .get("commit")
        .ok_or_else(|| shape_err("final_ctx missing $.commit".into()))?;
    let committed = commit
        .get("committed")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| shape_err("commit.committed missing or not bool".into()))?;
    let rationale = commit
        .get("rationale")
        .and_then(|v| v.as_str())
        .ok_or_else(|| shape_err("commit.rationale missing or not string".into()))?
        .to_string();
    let verdicts = parse_verdicts_summary(commit)?;

    if committed {
        let new_version_hex = commit
            .get("new_version")
            .and_then(|v| v.as_str())
            .ok_or_else(|| shape_err("commit.new_version missing or not string".into()))?
            .to_string();
        if new_version_hex.is_empty() {
            return Err(shape_err("commit.new_version is empty (Applied)".into()));
        }
        let bump = commit
            .get("bump")
            .and_then(|v| v.as_str())
            .ok_or_else(|| shape_err("commit.bump missing or not string".into()))?
            .to_string();
        let new_bp_json = commit
            .get("new_bp_json")
            .ok_or_else(|| shape_err("commit.new_bp_json missing".into()))?
            .clone();
        let new_bp: Box<Blueprint> = serde_json::from_value(new_bp_json)
            .map_err(|e| shape_err(format!("commit.new_bp_json deserialize: {e}")))?;
        Ok(CommitDecision::Applied {
            new_bp,
            new_version_hex,
            rationale,
            bump,
            verdicts,
        })
    } else {
        let reasons_arr = commit
            .get("reasons")
            .and_then(|v| v.as_array())
            .ok_or_else(|| shape_err("commit.reasons missing or not array".into()))?;
        let reasons: Vec<String> = reasons_arr
            .iter()
            .map(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| shape_err("commit.reasons[] contains non-string element".into()))
            })
            .collect::<Result<_, _>>()?;
        if reasons.is_empty() {
            return Err(shape_err(
                "commit.reasons is empty (Rejected requires at least 1)".into(),
            ));
        }
        Ok(CommitDecision::Rejected {
            reasons,
            rationale,
            verdicts,
        })
    }
}

fn parse_verdicts_summary(
    commit: &serde_json::Value,
) -> Result<Vec<VerdictSummary>, EnhanceApplicationError> {
    let arr = commit
        .get("verdicts_summary")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            EnhanceApplicationError::CommitShape(
                "commit.verdicts_summary missing or not array".into(),
            )
        })?;
    arr.iter()
        .map(|v| {
            let axis = v
                .get("axis")
                .and_then(|x| x.as_str())
                .ok_or_else(|| {
                    EnhanceApplicationError::CommitShape("verdicts_summary[].axis missing".into())
                })?
                .to_string();
            let status = v
                .get("status")
                .and_then(|x| x.as_str())
                .ok_or_else(|| {
                    EnhanceApplicationError::CommitShape("verdicts_summary[].status missing".into())
                })?
                .to_string();
            let detail = match status.as_str() {
                "pass" => v
                    .get("evidence")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| {
                        EnhanceApplicationError::CommitShape(
                            "verdicts_summary[].evidence missing for pass".into(),
                        )
                    })?
                    .to_string(),
                "deny" => v
                    .get("reason")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| {
                        EnhanceApplicationError::CommitShape(
                            "verdicts_summary[].reason missing for deny".into(),
                        )
                    })?
                    .to_string(),
                other => {
                    return Err(EnhanceApplicationError::CommitShape(format!(
                        "verdicts_summary[].status must be pass|deny, got {other}"
                    )))
                }
            };
            Ok(VerdictSummary {
                axis,
                status,
                detail,
            })
        })
        .collect()
}

#[async_trait]
impl Application for EnhanceApplication {
    type Input = EnhanceApplicationInput;
    type Output = IssueId;
    type Error = EnhanceApplicationError;

    fn name(&self) -> &str {
        &self.name
    }

    /// Just push the issue onto `IssueStore` — a synchronous enqueue;
    /// dispatch is entirely the consumer loop's job.
    async fn handle(&self, input: Self::Input) -> Result<Self::Output, Self::Error> {
        self.issue_store
            .create(IssuePayload {
                issue_id: input.issue_id.clone(),
                blueprint_id: input.blueprint_id,
                intent: input.intent,
            })
            .await?;
        Ok(input.issue_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blueprint::AgentKind;
    use crate::enhance::blueprint::default_blueprint;

    fn spawner_of(bp: &Blueprint) -> &AgentDef {
        bp.agents
            .iter()
            .find(|a| a.name == AG_PATCH_SPAWNER)
            .expect("blueprint declares a patch-spawner agent")
    }

    fn subprocess_spawner(name: &str) -> AgentDef {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "kind": "subprocess",
            "spec": { "program": "true", "args": [] },
        }))
        .expect("literal is a valid AgentDef")
    }

    #[test]
    fn no_override_keeps_the_blueprints_own_spawner() {
        let mut bp = default_blueprint();
        let before = spawner_of(&bp).clone();
        apply_spawner_override(&mut bp, None).unwrap();
        assert_eq!(spawner_of(&bp), &before);
        assert_eq!(spawner_of(&bp).kind, AgentKind::AgentBlock);
    }

    #[test]
    fn override_swaps_the_spawner_and_forces_the_referenced_name() {
        let mut bp = default_blueprint();
        let agents_before = bp.agents.len();
        // A deliberately mis-named override: the flow references the
        // agent by name, so the swap must rename it back.
        let def = subprocess_spawner("my-own-spawner");
        apply_spawner_override(&mut bp, Some(&def)).unwrap();

        let swapped = spawner_of(&bp);
        assert_eq!(swapped.kind, AgentKind::Subprocess);
        assert_eq!(swapped.name, AG_PATCH_SPAWNER);
        assert_eq!(swapped.spec, def.spec);
        // A swap, not an insert — the other three agents are untouched.
        assert_eq!(bp.agents.len(), agents_before);
        assert!(!bp.agents.iter().any(|a| a.name == "my-own-spawner"));
    }

    // ─── UT: the `EnhanceSetting.ttl_secs` ceiling ────────────────────
    //
    // The ceiling is the only thing that gives `ttl_secs` an effect since
    // `82d9da9` exempted Operator tokens from expiry, so these tests pin
    // three things: an overrun stops the wait with nothing committed, a run
    // inside the ceiling still commits exactly as before, and — the one
    // that keeps the docs honest — the worker stops with it. Losing the
    // first is a regression back to an inert knob and an unbounded drain;
    // losing the second is a ceiling that fires on healthy runs; losing the
    // third puts a live worker back on the tree an operator is about to
    // re-post against, and makes the module doc, the `EpochCeilingExceeded`
    // text and `mse://guides/enhance-flow` describe behaviour the code no
    // longer has.

    use crate::blueprint::compiler::{Compiler, RustFnInProcessSpawnerFactory, SpawnerRegistry};
    use crate::blueprint::store::{BlueprintId, CommitMetadata, InMemoryBlueprintStore};
    use crate::core::config::EngineCfg;
    use crate::core::engine::Engine;
    use crate::enhance::setting::{EnhanceSetting, EnhanceSettingMeta};
    use crate::store::enhance_log::InMemoryEnhanceLogStore;
    use crate::store::enhance_setting::InMemoryEnhanceSettingStore;
    use crate::store::issue::InMemoryIssueStore;
    use crate::worker::adapter::WorkerResult;
    use mlua_flow_ir::{Expr, Node as FlowNode};
    use serde_json::json;

    const TARGET_BP: &str = "target-ut";

    /// A one-step orbit Blueprint whose `patch-spawner` is the `RustFn`
    /// registered under `fn_id`, writing its result to `$.commit` — the
    /// key `extract_commit` reads. Everything else is inherited from the
    /// real `default_blueprint()` so the compile path under test is the
    /// production one.
    fn orbit_bp(fn_id: &str) -> Blueprint {
        let mut bp = default_blueprint();
        bp.id = "orbit-ut".into();
        bp.flow = FlowNode::Step {
            ref_: AG_PATCH_SPAWNER.into(),
            in_: Expr::Lit {
                value: serde_json::Value::Null,
            },
            out: Expr::Path {
                at: "$.commit".parse().expect("literal test path: $.commit"),
            },
        };
        bp.agents = vec![serde_json::from_value(json!({
            "name": AG_PATCH_SPAWNER,
            "kind": "rust_fn",
            "spec": { "fn_id": fn_id },
        }))
        .expect("literal is a valid AgentDef")];
        bp
    }

    /// Everything an `EnhanceApplication` needs, kept alongside it so a
    /// test can assert on what the stores hold after a tick.
    struct Harness {
        app: EnhanceApplication,
        issues: Arc<InMemoryIssueStore>,
        bps: Arc<InMemoryBlueprintStore>,
        logs: Arc<InMemoryEnhanceLogStore>,
        target_id: BlueprintId,
    }

    async fn harness(
        fn_id: &str,
        factory: RustFnInProcessSpawnerFactory,
        ttl_secs: u64,
    ) -> Harness {
        let mut registry = SpawnerRegistry::new();
        registry.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));
        let launch =
            TaskLaunchService::new(Engine::new(EngineCfg::default()), Compiler::new(registry));

        // Seed the target Blueprint — `dispatch_one` reads its head to
        // build `prev_bp_yaml` / `prev_hash`.
        let bps = Arc::new(InMemoryBlueprintStore::new());
        let target_id = BlueprintId::new(TARGET_BP.to_string());
        let target = default_blueprint();
        let seed_version =
            crate::blueprint::store::blueprint_version(&target).expect("seed hashes");
        bps.write_new(
            &target_id,
            &target,
            &[],
            CommitMetadata::seed(target_id.clone(), seed_version, 0),
        )
        .await
        .expect("seed the target Blueprint");

        let settings = Arc::new(InMemoryEnhanceSettingStore::new());
        let setting_id = EnhanceSettingId::default_id();
        settings
            .put(
                &setting_id,
                EnhanceSetting {
                    id: setting_id.to_string(),
                    blueprint_id: BlueprintId::new("orbit-ut".to_string()),
                    ttl_secs,
                    version: crate::application::VersionSelector::default(),
                    verifier_axes: vec![],
                    spawner: None,
                    meta: EnhanceSettingMeta::default(),
                },
            )
            .await
            .expect("put the setting");

        // The orbit Blueprint is resolved out of the same store.
        let orbit = orbit_bp(fn_id);
        let orbit_id = BlueprintId::new("orbit-ut".to_string());
        let orbit_version =
            crate::blueprint::store::blueprint_version(&orbit).expect("orbit hashes");
        bps.write_new(
            &orbit_id,
            &orbit,
            &[],
            CommitMetadata::seed(orbit_id.clone(), orbit_version, 0),
        )
        .await
        .expect("seed the orbit Blueprint");

        let issues = Arc::new(InMemoryIssueStore::new());
        let logs = Arc::new(InMemoryEnhanceLogStore::new());
        let app = EnhanceApplication::new(
            EnhanceApplicationConfig {
                name: "ut".into(),
                setting_id,
                operator_id: "ut-op".into(),
                role: Role::Operator,
            },
            issues.clone(),
            settings,
            bps.clone(),
            logs.clone(),
            Arc::new(launch),
        );
        Harness {
            app,
            issues,
            bps,
            logs,
            target_id,
        }
    }

    async fn post_issue(h: &Harness, issue_id: &str) -> IssueId {
        let id = IssueId::new(issue_id);
        h.app
            .handle(EnhanceApplicationInput {
                blueprint_id: h.target_id.clone(),
                intent: "add a smoke tag".into(),
                issue_id: id.clone(),
            })
            .await
            .expect("enqueue");
        id
    }

    /// An epoch that outruns `ttl_secs` stops the drain's wait, and that
    /// leaves the target Blueprint exactly where it was — the half-epoch
    /// question, pinned. Also pins the operator-visible surface: the
    /// issue's terminal reason names the ceiling, the knob, and the fact
    /// that the worker was not stopped (that last one is the difference
    /// between an operator who checks before re-posting and one who starts
    /// a second run on top of a live one).
    #[tokio::test]
    async fn epoch_that_outruns_ttl_secs_stops_the_wait_with_nothing_committed() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("hang", |_inv| async move {
            // Far longer than the 1s ceiling; the timeout drops this
            // future rather than waiting for it, so the test does not.
            tokio::time::sleep(Duration::from_secs(300)).await;
            Ok(WorkerResult {
                value: json!({}),
                ok: true,
                stats: None,
            })
        });
        let h = harness("hang", factory, 1).await;
        let issue_id = post_issue(&h, "h-ceiling").await;
        let head_before = h.bps.read_head(&h.target_id).await.expect("head before");

        let err = h
            .app
            .tick()
            .await
            .expect_err("an epoch past its ceiling must surface as an infra fault");
        assert!(
            matches!(
                err,
                EnhanceApplicationError::EpochCeilingExceeded { ttl_secs: 1, ref setting_id }
                    if setting_id == "default"
            ),
            "the ceiling must abort with EpochCeilingExceeded, got: {err}"
        );

        // What an operator sees afterwards.
        match h.issues.status(&issue_id).await.expect("issue status") {
            IssueStatus::Rejected { reason } => {
                assert!(
                    reason.contains("exceeded the 1s ceiling") && reason.contains("ttl_secs"),
                    "the reason must name the ceiling and the knob to raise, got: {reason}"
                );
                assert!(
                    reason.contains("nothing was committed"),
                    "the reason must say the target Blueprint is untouched, got: {reason}"
                );
                // The residue, on the surface an operator actually reads.
                // "Timed out" reads as "it stopped" unless the text says
                // otherwise, and acting on that reading — re-posting at once
                // — is what puts a second writer under one project_root.
                assert!(
                    reason.contains("Only the wait ended"),
                    "the reason must say the worker may still be running, got: {reason}"
                );
                assert!(
                    reason.contains("second writer"),
                    "the reason must name the hazard a blind re-post creates, got: {reason}"
                );
            }
            other => panic!("a timed-out epoch must be terminal Rejected, got {other:?}"),
        }

        // Nothing partial: the head is byte-identical and no version was
        // appended.
        let head_after = h.bps.read_head(&h.target_id).await.expect("head after");
        assert_eq!(
            head_before.value, head_after.value,
            "a fired ceiling must not write to the BlueprintStore"
        );
        assert_eq!(
            h.bps
                .history(&h.target_id, 10)
                .await
                .expect("history")
                .len(),
            1,
            "only the seed commit may exist after a timed-out epoch"
        );
        assert!(
            h.logs.list_all().await.expect("log").is_empty(),
            "an epoch that never reached the committer appends no log entry"
        );
    }

    /// A fired ceiling ends the wait and leaves the worker running.
    ///
    /// This is the boundary of what `ttl_secs` is, pinned so nobody has to
    /// rediscover it from the timeout's name. The knob bounds a wait on an
    /// unresponsive Operator (model §4.4 **R5**), and that wait is an await
    /// point. An in-process worker is not a wait: it runs inside its own
    /// `tokio::spawn` (`src/worker/adapter.rs`) which is not a child of the
    /// `launch` future, and the `CancellationToken` it selects on is fired
    /// by nothing in `src/` — `Engine::cancel_task` sets a status and wakes
    /// pollers without touching it. So dropping the future ends this
    /// dispatcher's wait and nothing else.
    ///
    /// That is the fact the `EpochCeilingExceeded` text and
    /// `mse://guides/enhance-flow` tell an operator to act on: re-posting
    /// immediately puts a second writer under the same `project_root`. If
    /// this test ever starts failing, that guidance has become wrong and
    /// both surfaces have to move with the code.
    ///
    /// Real time rather than `start_paused`: the claim is about work that
    /// does or does not keep running, so a clock the test controls would
    /// prove less than the thing being asserted.
    #[tokio::test]
    async fn a_fired_ceiling_does_not_stop_the_worker() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let ran_past_the_ceiling = Arc::new(AtomicBool::new(false));
        let flag = ran_past_the_ceiling.clone();
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("outlive", move |_inv| {
            let flag = flag.clone();
            async move {
                // Outlasts the 1s ceiling, so the timeout fires mid-sleep —
                // i.e. with the worker parked on an await, the one place a
                // drop could have reached it if the lanes worked that way.
                tokio::time::sleep(Duration::from_millis(1_600)).await;
                flag.store(true, Ordering::SeqCst);
                Ok(WorkerResult {
                    value: json!({}),
                    ok: true,
                    stats: None,
                })
            }
        });
        let h = harness("outlive", factory, 1).await;
        post_issue(&h, "h-residue").await;

        let err = h.app.tick().await.expect_err("the ceiling must fire");
        assert!(
            matches!(err, EnhanceApplicationError::EpochCeilingExceeded { .. }),
            "expected the ceiling, got: {err}"
        );
        assert!(
            !ran_past_the_ceiling.load(Ordering::SeqCst),
            "precondition: the worker must still be mid-sleep when the ceiling fires, \
             otherwise this test proves nothing"
        );

        // Give the worker more than the rest of its sleep. The flag flipping
        // here is the residue itself: the issue is already terminal and the
        // work is still going.
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        assert!(
            ran_past_the_ceiling.load(Ordering::SeqCst),
            "the worker was expected to run on past the ceiling — if it stopped, the \
             ceiling now reaches the work and the reason text plus \
             mse://guides/enhance-flow, which both tell operators to check for a live \
             worker before re-posting, have to be corrected in the same change"
        );
    }

    /// The counter-direction: an epoch that finishes inside the ceiling is
    /// unaffected by the wrap and still lands its outcome in the enhance
    /// log. Without this, deleting the ceiling's `Ok` arm would go
    /// unnoticed.
    #[tokio::test]
    async fn epoch_within_the_ceiling_still_reaches_the_committer() {
        let factory =
            RustFnInProcessSpawnerFactory::new().register_fn("commit", |_inv| async move {
                Ok(WorkerResult {
                    value: json!({
                        "committed": false,
                        "rationale": "the patch was refused",
                        "reasons": ["noop: patch is no-op"],
                        "verdicts_summary": [
                            {"axis": "noop", "status": "deny", "reason": "new_hash == prev_hash"}
                        ],
                    }),
                    ok: true,
                    stats: None,
                })
            });
        let h = harness("commit", factory, 60).await;
        let issue_id = post_issue(&h, "h-ok").await;

        let outcome = h
            .app
            .tick()
            .await
            .expect("a within-ceiling epoch must not surface as an infra fault")
            .expect("one issue was pending");
        assert_eq!(outcome.issue_id, issue_id);
        match outcome.status {
            IssueStatus::Rejected { ref reason } => assert!(
                reason.starts_with("verifier deny:"),
                "a committer rejection must keep its own reason, not the ceiling's: {reason}"
            ),
            ref other => panic!("expected a verifier rejection, got {other:?}"),
        }
        assert_eq!(
            h.logs.list_all().await.expect("log").len(),
            1,
            "an epoch that reached the committer appends exactly one log entry"
        );
    }

    /// `ttl_secs: 0` is refused as a typo rather than read as "no
    /// ceiling", and refused before any store is touched.
    #[tokio::test]
    async fn zero_ttl_secs_is_refused_and_names_the_field() {
        let factory =
            RustFnInProcessSpawnerFactory::new().register_fn("unused", |_inv| async move {
                Ok(WorkerResult {
                    value: json!({}),
                    ok: true,
                    stats: None,
                })
            });
        let h = harness("unused", factory, 0).await;
        let issue_id = post_issue(&h, "h-zero").await;

        let err = h
            .app
            .tick()
            .await
            .expect_err("a zero ceiling must be refused, not treated as unbounded");
        assert!(
            matches!(
                err,
                EnhanceApplicationError::ZeroTtl { ref setting_id } if setting_id == "default"
            ),
            "expected ZeroTtl, got: {err}"
        );
        match h.issues.status(&issue_id).await.expect("issue status") {
            IssueStatus::Rejected { reason } => assert!(
                reason.contains("ttl_secs: 0"),
                "the reason must name the field and its bad value, got: {reason}"
            ),
            other => panic!("expected terminal Rejected, got {other:?}"),
        }
        assert!(
            h.logs.list_all().await.expect("log").is_empty(),
            "a refused setting never reaches the committer"
        );
    }

    #[test]
    fn override_without_a_matching_agent_fails_loud() {
        let mut bp = default_blueprint();
        bp.agents.retain(|a| a.name != AG_PATCH_SPAWNER);
        let err = apply_spawner_override(&mut bp, Some(&subprocess_spawner(AG_PATCH_SPAWNER)))
            .expect_err("a missing override target must not be ignored");
        assert!(matches!(
            err,
            EnhanceApplicationError::SpawnerAgentNotFound { ref name } if name == AG_PATCH_SPAWNER
        ));
        assert!(err.to_string().contains(AG_PATCH_SPAWNER));
    }
}
