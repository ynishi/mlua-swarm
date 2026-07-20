//! `TaskApplication` — the `POST /v1/tasks` entry point.
//!
//! Input: `BlueprintRef` (Inline / Id) plus a `TaskSpec`. Output:
//! `(CapToken, StepId, version)`. Once the Blueprint is resolved, the
//! engine-side operations (`bind` + `attach` + `start_task`) are
//! delegated to [`TaskLaunchService`].

use super::semver_resolve::SemverResolveError;
use super::Application;
use crate::blueprint::store::{BlueprintId, BlueprintStore, BlueprintStoreError, BlueprintVersion};
use crate::blueprint::Blueprint;
use crate::core::config::CheckPolicy;
use crate::core::ctx::OperatorKind;
use crate::service::{
    TaskInputSpec, TaskLaunchError, TaskLaunchInput, TaskLaunchOutput, TaskLaunchService,
};
use crate::store::run::RunContext;
use crate::types::{CapToken, Role};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// How a task entry says the Blueprint should be resolved.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BlueprintRef {
    /// The Blueprint value is embedded directly in the request; no
    /// store lookup happens.
    Inline {
        /// The Blueprint to run as-is.
        value: Box<Blueprint>,
    },
    /// Resolve the Blueprint from the `BlueprintStore` by id.
    Id {
        /// The `BlueprintId` to look up in the store.
        id: BlueprintId,
        /// Which generation to pick; defaults to `Latest`.
        #[serde(default)]
        version: VersionSelector,
    },
}

/// How to pick a generation — a `version` inside the store.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VersionSelector {
    /// Use the store's current head version.
    #[default]
    Latest,
    /// Use one exact, previously-committed version.
    Fixed {
        /// The exact version to read.
        value: BlueprintVersion,
    },
    /// Scan the store's history and pick the highest version whose
    /// `BlueprintMetadata.version_label` satisfies `req`.
    SemverReq {
        /// The semver requirement every candidate label is matched
        /// against.
        req: semver::VersionReq,
    },
}

/// Input to [`TaskApplication::handle`] — the `POST /v1/tasks` request
/// body once decoded.
#[derive(Debug, Clone)]
pub struct TaskApplicationInput {
    /// Accepts both Inline (a Blueprint value directly) and Id
    /// (store fetch + a `VersionSelector`).
    pub blueprint: BlueprintRef,
    /// Caller-supplied id for the Operator that owns this run.
    pub operator_id: String,
    /// The Operator's role for this run.
    pub role: Role,
    /// How long the attached session is allowed to live.
    pub ttl: Duration,
    /// Initial `ctx` for flow.ir `eval`. Read by every `Step.in`.
    pub init_ctx: Value,
    /// "Runtime Global" tier of the `OperatorKind` cascade. `Some(_)` is
    /// always an explicit request — including `Some(OperatorKind::Automate)`
    /// — that outranks the BP-level tiers (`OperatorDef.kind` /
    /// `Blueprint.default_operator_kind`); `None` leaves it unspecified so
    /// those tiers / the final default decide. Under `MainAi` /
    /// `Composite`, `MainAIMiddleware`'s `spawn_hook` before/after
    /// callbacks become effective. See
    /// `crate::core::ctx::collapse_operator_kind`.
    pub operator_kind: Option<crate::core::ctx::OperatorKind>,
    /// `SeniorBridge` registry ID. `None` — none in use;
    /// `Some(id)` — attach a bridge previously registered on the
    /// engine.
    pub bridge_id: Option<String>,
    /// `SpawnHook` registry ID. Same shape as above — attach a hook
    /// previously registered on the engine.
    pub hook_id: Option<String>,
    /// Operator registry ID — used on the path that hands the whole
    /// spawn off to an external Operator.
    pub operator_backend_id: Option<String>,
    /// "Runtime Agent-level" tier (highest priority) of the `OperatorKind`
    /// cascade — per-agent override, keyed by `AgentDef.name`. Empty by
    /// default. See `crate::core::ctx::collapse_operator_kind` for the full tier
    /// list.
    pub operator_kind_overrides: HashMap<String, OperatorKind>,
    /// Task-level canonical execution context (issue #19 ST2). When
    /// `Some`, the resolved sibling fields (`project_root` / `work_dir`
    /// / `task_metadata`) are threaded down to [`TaskLaunchInput`] and
    /// consumed by
    /// [`crate::middleware::task_input::TaskInputMiddleware::new_from_fields`].
    /// `None` — no Task-level context is layered on the spawner stack
    /// (default; keeps the wire body opt-in).
    pub task_input: Option<TaskInputSpec>,
    /// The "launch request" tier (tier 1) of the
    /// `check_policy` cascade, threaded straight down to
    /// [`TaskLaunchInput::check_policy`]. `None` (the default via
    /// [`Self::automate`]) leaves this tier unspecified — the Blueprint tier
    /// / server-wide default decide. Wired from the `POST /v1/tasks`
    /// request body's top-level `check_policy` field.
    pub check_policy: Option<CheckPolicy>,
}

impl TaskApplicationInput {
    /// Helper for existing callers on the default path — no hooks and no
    /// per-agent `OperatorKind` overrides. Leaves the "Runtime Global" tier
    /// unspecified (`None`), so the BP-level tiers / final default
    /// (`OperatorKind::Automate`) decide — this preserves today's
    /// behaviour for every existing caller without silently forcing
    /// `Automate` as an explicit override that would outrank a BP-declared
    /// `MainAi`/`Composite` kind.
    pub fn automate(
        blueprint: BlueprintRef,
        operator_id: impl Into<String>,
        role: Role,
        ttl: Duration,
        init_ctx: Value,
    ) -> Self {
        Self {
            blueprint,
            operator_id: operator_id.into(),
            role,
            ttl,
            init_ctx,
            operator_kind: None,
            bridge_id: None,
            hook_id: None,
            operator_backend_id: None,
            operator_kind_overrides: HashMap::new(),
            task_input: None,
            check_policy: None,
        }
    }
}

/// Result of a successful [`TaskApplication::handle`] call.
#[derive(Debug, Clone)]
pub struct TaskApplicationOutput {
    /// The capability token for the attached session.
    pub token: CapToken,
    /// The final `ctx` after the flow ran to completion.
    pub final_ctx: Value,
    /// Only `Some` when resolution went through the store
    /// (`BlueprintRef::Id`); `None` on the Inline path.
    pub bound_version: Option<BlueprintVersion>,
}

/// Failure modes of [`TaskApplication::handle`] and
/// [`TaskApplication::resolve`].
#[derive(Debug, Error)]
pub enum TaskApplicationError {
    /// `BlueprintRef::Id` was used but this `TaskApplication` was
    /// built via [`TaskApplication::new_inline_only`] (no store).
    #[error("store not configured (BlueprintRef::Id requires store)")]
    NoStore,
    /// The `BlueprintStore` returned an error while resolving the ref.
    #[error("store: {0}")]
    Store(#[from] BlueprintStoreError),
    /// `TaskLaunchService::launch` failed after resolution succeeded.
    #[error("launch: {0}")]
    Launch(#[from] TaskLaunchError),
    /// A stored version's `version_label` is not valid semver.
    #[error("invalid semver version_label {label:?}: {source}")]
    InvalidSemver {
        /// The offending label string.
        label: String,
        /// The underlying semver parse error.
        #[source]
        source: semver::Error,
    },
    /// No stored version's label satisfies the `SemverReq`.
    #[error("no version matches semver req: {req}")]
    NoMatchingVersion {
        /// The requirement string that matched nothing.
        req: String,
    },
}

impl From<SemverResolveError> for TaskApplicationError {
    fn from(e: SemverResolveError) -> Self {
        match e {
            SemverResolveError::Store(e) => TaskApplicationError::Store(e),
            SemverResolveError::InvalidSemver { label, source } => {
                TaskApplicationError::InvalidSemver { label, source }
            }
            SemverResolveError::NoMatchingVersion { req } => {
                TaskApplicationError::NoMatchingVersion { req }
            }
        }
    }
}

/// The `POST /v1/tasks` [`Application`] — resolves a `BlueprintRef` and
/// runs it to completion through [`TaskLaunchService`].
pub struct TaskApplication {
    launch: Arc<TaskLaunchService>,
    /// Only needed when resolving `BlueprintRef::Id`; `None` in
    /// Inline-only mode.
    store: Option<Arc<dyn BlueprintStore>>,
}

impl TaskApplication {
    /// Build a `TaskApplication` that can resolve both `Inline` and
    /// `Id` `BlueprintRef`s (the `Id` path reads through `store`).
    pub fn new(launch: Arc<TaskLaunchService>, store: Arc<dyn BlueprintStore>) -> Self {
        Self {
            launch,
            store: Some(store),
        }
    }

    /// Build a `TaskApplication` restricted to `Inline` `BlueprintRef`s
    /// — no store is configured, so `Id` resolution always fails with
    /// `TaskApplicationError::NoStore`.
    pub fn new_inline_only(launch: Arc<TaskLaunchService>) -> Self {
        Self {
            launch,
            store: None,
        }
    }

    /// Resolve a `BlueprintRef` and return the real Blueprint plus,
    /// when it went through the store, the resolved version.
    pub async fn resolve(
        &self,
        bp_ref: &BlueprintRef,
    ) -> Result<(Blueprint, Option<BlueprintVersion>), TaskApplicationError> {
        match bp_ref {
            BlueprintRef::Inline { value } => Ok((value.as_ref().clone(), None)),
            BlueprintRef::Id { id, version } => {
                let store = self.store.as_ref().ok_or(TaskApplicationError::NoStore)?;
                let bp_id = id.clone();
                let traced = match version {
                    VersionSelector::Latest => store.read_head(&bp_id).await?,
                    VersionSelector::Fixed { value } => store.read_version(&bp_id, *value).await?,
                    VersionSelector::SemverReq { req } => {
                        let v = super::semver_resolve::resolve_semver(store.as_ref(), &bp_id, req)
                            .await?;
                        store.read_version(&bp_id, v).await?
                    }
                };
                let ver = traced.trace.version;
                Ok((traced.value, Some(ver)))
            }
        }
    }

    /// Pre-flight compile check: resolve `bp_ref` and drive it through
    /// `Compiler::compile` without launching. Returns `Ok(())` when the
    /// Blueprint would compile cleanly (every `operator_ref` /
    /// `meta_ref` / `audits[].agent` / verdict cond shape resolves), or
    /// the same `TaskApplicationError` variants
    /// [`Self::handle_with_run`] would surface for a resolve or compile
    /// failure. No engine attach, no spawn, no `RunRecord` mutation.
    ///
    /// Used by `POST /v1/runs/:id/rerun-from` (GH #71 Layer A) as a
    /// fast-fail gate: a deterministic compile-time failure — the
    /// canonical case is an unbound `operator_ref` after an operator was
    /// removed from `Blueprint.operators` between the original dispatch
    /// and the rerun — surfaces as a `422` here BEFORE the handler
    /// physically truncates the replay log via
    /// `ReplayStore::delete_from`. Without this pre-check the same
    /// failure fires later inside the detached `tokio::spawn`, after the
    /// truncation has already consumed the pre-cut entries and left the
    /// caller with no replay log to retry against.
    pub async fn precompile(&self, bp_ref: &BlueprintRef) -> Result<(), TaskApplicationError> {
        let (bp, _v) = self.resolve(bp_ref).await?;
        self.launch
            .compiler()
            .compile(&bp)
            .map_err(TaskLaunchError::from)?;
        Ok(())
    }

    /// Resolve the `BlueprintRef` (Inline / Id) and run the flow to
    /// completion through `TaskLaunchService::launch`, threading `run_ctx`
    /// (issue #13 run_id propagation) into the launch input.
    ///
    /// [`Application::handle`] delegates here with `run_ctx: None` — a
    /// separate method rather than a new field on [`TaskApplicationInput`]
    /// so the pre-existing exhaustive `TaskApplicationInput { .. }` struct
    /// literal in `mlua-swarm-cli`'s MCP adapter (which has no `run_ctx`)
    /// keeps compiling unchanged. Server entry points that mint a `RunId`
    /// up front (`POST /v1/tasks`, `POST /v1/tasks/:id/runs`) call this
    /// directly with `Some(run_ctx)`.
    pub async fn handle_with_run(
        &self,
        input: TaskApplicationInput,
        run_ctx: Option<RunContext>,
    ) -> Result<TaskApplicationOutput, TaskApplicationError> {
        let (blueprint, bound_version) = self.resolve(&input.blueprint).await?;
        let TaskLaunchOutput { token, final_ctx } = self
            .launch
            .launch(TaskLaunchInput {
                blueprint,
                operator_id: input.operator_id,
                role: input.role,
                ttl: input.ttl,
                operator_kind: input.operator_kind,
                bridge_id: input.bridge_id,
                hook_id: input.hook_id,
                operator_backend_id: input.operator_backend_id,
                operator_kind_overrides: input.operator_kind_overrides,
                init_ctx: input.init_ctx,
                run_ctx,
                task_input: input.task_input,
                check_policy: input.check_policy,
            })
            .await?;
        Ok(TaskApplicationOutput {
            token,
            final_ctx,
            bound_version,
        })
    }
}

#[async_trait]
impl Application for TaskApplication {
    type Input = TaskApplicationInput;
    type Output = TaskApplicationOutput;
    type Error = TaskApplicationError;

    fn name(&self) -> &str {
        "task"
    }

    /// Resolve the `BlueprintRef` (Inline / Id) and run the flow to
    /// completion through `TaskLaunchService::launch`. Delegates to
    /// [`TaskApplication::handle_with_run`] with `run_ctx: None` (no run
    /// tracing) — callers that need `RunRecord.step_entries` tracing call
    /// `handle_with_run` directly instead.
    async fn handle(&self, input: Self::Input) -> Result<Self::Output, Self::Error> {
        self.handle_with_run(input, None).await
    }
}

// ──────────────────────────────────────────────────────────────────────────
// UT
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blueprint::compiler::{Compiler, SpawnerRegistry};
    use crate::blueprint::store::{
        blueprint_version, BlueprintId, BlueprintStore, BlueprintStoreError, CommitMetadata,
        InMemoryBlueprintStore,
    };
    use crate::blueprint::{
        current_schema_version, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
        CompilerStrategy,
    };
    use crate::core::config::EngineCfg;
    use crate::core::ctx::OperatorKind;
    use crate::core::engine::Engine;
    use mlua_flow_ir::Node as FlowNode;

    fn empty_bp() -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "ut-bp".into(),
            flow: FlowNode::Seq { children: vec![] },
            agents: vec![],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        }
    }

    fn bp_with_label(id: &str, version_label: Option<&str>) -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: id.into(),
            flow: FlowNode::Seq { children: vec![] },
            agents: vec![],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata {
                description: None,
                origin: Default::default(),
                tags: vec![],
                version_label: version_label.map(|s| s.to_string()),
                project_name_alias: None,
                default_run_ttl_secs: None,
                strict_verdict_handling: None,
            },
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        }
    }

    fn build_app_with_store() -> (TaskApplication, Arc<dyn BlueprintStore>) {
        let reg = SpawnerRegistry::new();
        let compiler = Compiler::new(reg);
        let engine = Engine::new(EngineCfg::default());
        let launch = Arc::new(TaskLaunchService::new(engine, compiler));
        let store: Arc<dyn BlueprintStore> = Arc::new(InMemoryBlueprintStore::new());
        (TaskApplication::new(launch, store.clone()), store)
    }

    fn build_app_inline_only() -> TaskApplication {
        let reg = SpawnerRegistry::new();
        let compiler = Compiler::new(reg);
        let engine = Engine::new(EngineCfg::default());
        let launch = Arc::new(TaskLaunchService::new(engine, compiler));
        TaskApplication::new_inline_only(launch)
    }

    async fn seed(store: &Arc<dyn BlueprintStore>, bp: &Blueprint) -> BlueprintVersion {
        let id = bp.id.clone();
        let v = blueprint_version(bp).expect("hash");
        store
            .write_new(&id, bp, &[], CommitMetadata::seed(id.clone(), v, 0))
            .await
            .expect("seed");
        v
    }

    #[test]
    fn automate_helper_sets_defaults() {
        let input = TaskApplicationInput::automate(
            BlueprintRef::Inline {
                value: Box::new(empty_bp()),
            },
            "op-1",
            Role::Operator,
            Duration::from_secs(10),
            serde_json::json!({}),
        );
        assert!(
            input.operator_kind.is_none(),
            "automate() leaves the Runtime Global tier unspecified (None), \
             not an explicit Some(Automate) override"
        );
        assert!(input.bridge_id.is_none());
        assert!(input.hook_id.is_none());
        assert_eq!(input.operator_id, "op-1");
    }

    #[test]
    fn struct_literal_allows_callback_ids() {
        let input = TaskApplicationInput {
            blueprint: BlueprintRef::Inline {
                value: Box::new(empty_bp()),
            },
            operator_id: "op-2".into(),
            role: Role::Operator,
            ttl: Duration::from_secs(5),
            init_ctx: serde_json::json!({}),
            operator_kind: Some(OperatorKind::MainAi),
            bridge_id: Some("br-x".into()),
            hook_id: Some("hk-y".into()),
            operator_backend_id: None,
            operator_kind_overrides: HashMap::new(),
            task_input: None,
            check_policy: None,
        };
        assert!(matches!(input.operator_kind, Some(OperatorKind::MainAi)));
        assert_eq!(input.bridge_id.as_deref(), Some("br-x"));
        assert_eq!(input.hook_id.as_deref(), Some("hk-y"));
    }

    // ──────────────────────────────────────────────────────────────────
    // resolve / resolve_semver carve
    // ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn resolve_inline_returns_bp_and_no_version() {
        let app = build_app_inline_only();
        let bp = empty_bp();
        let (got, ver) = app
            .resolve(&BlueprintRef::Inline {
                value: Box::new(bp.clone()),
            })
            .await
            .expect("resolve inline ok");
        assert_eq!(got.id, bp.id);
        assert!(ver.is_none(), "the Inline path yields bound_version=None");
    }

    #[tokio::test]
    async fn resolve_id_latest_returns_bp_and_version() {
        let (app, store) = build_app_with_store();
        let bp = bp_with_label("rid-latest", Some("0.1.0"));
        let v = seed(&store, &bp).await;
        let (got, ver) = app
            .resolve(&BlueprintRef::Id {
                id: bp.id.clone(),
                version: VersionSelector::Latest,
            })
            .await
            .expect("resolve id latest ok");
        assert_eq!(got.id, bp.id);
        assert_eq!(ver, Some(v), "Latest = seed version");
    }

    #[tokio::test]
    async fn resolve_id_fixed_picks_exact_version() {
        let (app, store) = build_app_with_store();
        let id = "rid-fixed";
        let bp1 = bp_with_label(id, Some("1.0.0"));
        let bp2 = bp_with_label(id, Some("2.0.0"));
        let v1 = seed(&store, &bp1).await;
        let _v2 = seed(&store, &bp2).await;
        let (got, ver) = app
            .resolve(&BlueprintRef::Id {
                id: BlueprintId::new(id),
                version: VersionSelector::Fixed { value: v1 },
            })
            .await
            .expect("resolve id fixed ok");
        assert_eq!(ver, Some(v1));
        assert_eq!(
            got.metadata.version_label.as_deref(),
            Some("1.0.0"),
            "Fixed{{v1}} resolves to v1 = 1.0.0"
        );
    }

    #[tokio::test]
    async fn resolve_id_semver_picks_highest_matching() {
        let (app, store) = build_app_with_store();
        let id = "rid-semver";
        let _ = seed(&store, &bp_with_label(id, Some("1.0.0"))).await;
        let _ = seed(&store, &bp_with_label(id, Some("1.2.0"))).await;
        let _ = seed(&store, &bp_with_label(id, Some("2.0.0"))).await;
        let req = semver::VersionReq::parse("^1").expect("req");
        let (got, ver) = app
            .resolve(&BlueprintRef::Id {
                id: BlueprintId::new(id),
                version: VersionSelector::SemverReq { req },
            })
            .await
            .expect("resolve semver ok");
        assert!(ver.is_some());
        assert_eq!(
            got.metadata.version_label.as_deref(),
            Some("1.2.0"),
            "^1 max = 1.2.0 (2.0.0 is out of range; 1.0.0 is lower)"
        );
    }

    #[tokio::test]
    async fn resolve_id_semver_no_match_errs() {
        let (app, store) = build_app_with_store();
        let id = "rid-semver-nomatch";
        let _ = seed(&store, &bp_with_label(id, Some("1.0.0"))).await;
        let req = semver::VersionReq::parse("^3").expect("req");
        let err = app
            .resolve(&BlueprintRef::Id {
                id: BlueprintId::new(id),
                version: VersionSelector::SemverReq { req },
            })
            .await
            .expect_err("expected NoMatchingVersion");
        match err {
            TaskApplicationError::NoMatchingVersion { req } => {
                assert!(req.contains("^3"), "req string carry: {req}");
            }
            other => panic!("expected NoMatchingVersion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resolve_id_semver_invalid_label_errs() {
        let (app, store) = build_app_with_store();
        let id = "rid-semver-bad";
        let _ = seed(&store, &bp_with_label(id, Some("not-semver"))).await;
        let req = semver::VersionReq::parse("^1").expect("req");
        let err = app
            .resolve(&BlueprintRef::Id {
                id: BlueprintId::new(id),
                version: VersionSelector::SemverReq { req },
            })
            .await
            .expect_err("expected InvalidSemver");
        match err {
            TaskApplicationError::InvalidSemver { label, .. } => {
                assert_eq!(label, "not-semver");
            }
            other => panic!("expected InvalidSemver, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resolve_id_without_store_errs_no_store() {
        let app = build_app_inline_only();
        let err = app
            .resolve(&BlueprintRef::Id {
                id: BlueprintId::new("anything"),
                version: VersionSelector::Latest,
            })
            .await
            .expect_err("expected NoStore");
        assert!(matches!(err, TaskApplicationError::NoStore), "got {err:?}");
    }

    #[tokio::test]
    async fn resolve_id_not_found_errs_store() {
        let (app, _store) = build_app_with_store();
        let err = app
            .resolve(&BlueprintRef::Id {
                id: BlueprintId::new("never-seeded"),
                version: VersionSelector::Latest,
            })
            .await
            .expect_err("expected Store(IdNotFound|HeadEmpty)");
        match err {
            TaskApplicationError::Store(
                BlueprintStoreError::IdNotFound(_) | BlueprintStoreError::HeadEmpty(_),
            ) => {}
            other => panic!("expected Store(IdNotFound|HeadEmpty), got {other:?}"),
        }
    }
}
