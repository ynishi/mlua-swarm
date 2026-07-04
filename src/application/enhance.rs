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
//! ```
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
use crate::blueprint::Blueprint;
use crate::core::errors::EngineError;
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
/// a hot reload. This `Config` only holds the identity information
/// needed to stand up an Application instance.
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
    ///    and `ttl`).
    /// 2. Resolve the orbit BP (for example the built-in
    ///    `enhance-default` flow).
    /// 3. Resolve the target BP (`payload.blueprint_id`) — the
    ///    object being modified, injected into `init_ctx` as
    ///    `prev_bp`.
    /// 4. Assemble `init_ctx` (`issue` / `prev_bp_yaml` / `prev_hash`
    ///    / `epoch_id` / `verifiers`).
    /// 5. Run to completion via `TaskLaunchService::launch` — pull
    ///    `final_ctx` once every step finishes.
    /// 6. Derive `IssueStatus` from `final_ctx.commit`; when Applied,
    ///    persist via `bp_store.write_new`.
    /// 7. Append a `LogEntry` to `log_store` — exactly one entry per
    ///    outcome, Applied or Rejected.
    async fn dispatch_one(
        &self,
        payload: &IssuePayload,
    ) -> Result<IssueStatus, EnhanceApplicationError> {
        let setting = self.setting_store.get(&self.setting_id).await?;

        let traced_orch = self
            .resolve_blueprint(&setting.blueprint_id, &setting.version)
            .await?;

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

        let TaskLaunchOutput {
            token: _,
            final_ctx,
        } = self
            .launch
            .launch(TaskLaunchInput::automate(
                traced_orch.value,
                self.operator_id.clone(),
                self.role,
                Duration::from_secs(setting.ttl_secs),
                init_ctx,
            ))
            .await?;

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
