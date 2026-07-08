//! `AgentContextMiddleware` — the innermost `SpawnerLayer` that
//! materializes [`AgentContextView`] once per spawn (Contract C, GH #20),
//! now also the receptacle that resolves and merges the BP-declared
//! agent-context supply tiers (GH #21 Phase 1).
//!
//! Layered FIRST (innermost, i.e. added earliest in
//! `service::task_launch::TaskLaunchService::launch`, before the alias /
//! worker-binding blocks) so it observes the `ctx.meta.runtime` keys the
//! outer `TaskInputMiddleware` / `ProjectNameAliasMiddleware` /
//! `WorkerBindingMiddleware` layers insert. See the module doc on
//! [`crate::core::agent_context`] for the full Contract C narrative and
//! the two-axis fan-out diagram.
//!
//! Unlike `TaskInputMiddleware` / `ProjectNameAliasMiddleware` (which only
//! mutate `ctx`), this layer ALSO snapshots the view into
//! `EngineState.agent_contexts`, keyed `(task_id, attempt)` — the Worker
//! axis (`Engine::fetch_worker_payload{,_trusted}`) reads it back from
//! there, mirroring how `EngineState.prompts` / `.systems` are populated
//! and later fetched.
//!
//! # GH #21 Phase 1: the supply tiers this layer merges
//!
//! `service::task_launch::derive_agent_ctx` / `derive_context_policies`
//! resolve four pieces out of the launched `Blueprint` — `default_agent_ctx`
//! / per-agent `AgentMeta.ctx` (the context tiers) and
//! `default_context_policy` / per-agent `AgentMeta.context_policy` (the
//! policy tiers) — and hand them to [`AgentContextMiddleware::new`]. On
//! every spawn:
//!
//! 1. The context tiers are shallow-merged (agent wins on key collision;
//!    a tier whose declared value isn't a JSON `Object` is warned and
//!    skipped, never failing the spawn).
//! 2. Every merged key is inserted into `ctx.meta.runtime`
//!    **only-if-absent** — an outer, runtime-supplied value (e.g.
//!    `TaskInputMiddleware`'s `work_dir`) always outranks this BP-declared
//!    default, with no priority code beyond insertion order
//!    (`SpawnerStack` outer-to-inner = later tier wins the race to insert
//!    first).
//! 3. [`AgentContextView::from_ctx`] then reads the (possibly
//!    BP-defaulted) `ctx.meta.runtime` back out, so known-key defaults
//!    (`project_root` / `work_dir` / …) flow into the view automatically;
//!    merged keys that aren't one of those named fields are folded into
//!    `view.extra` instead (also only-if-absent).
//! 4. The policy tiers resolve to a single effective [`ContextPolicy`]
//!    (per-agent outranks BP-global; pass-all when neither is declared)
//!    and are applied via [`AgentContextView::apply_policy`].
//!
//! # GH #21 Phase 2: the Step tier
//!
//! Before the Agent/BP-global merge above, this layer also reads
//! `ctx.meta.runtime[STEP_CTX_KEY]` — the Step tier's resolved bundle,
//! threaded through by `Engine::dispatch_attempt_with` from
//! `TaskSpec.step_ctx` (itself resolved by `EngineDispatcher::dispatch`'s
//! `$step_meta` envelope handling in `crate::blueprint`). When it is a
//! JSON `Object`, its keys are applied with the SAME only-if-absent +
//! extra-fold mechanics as the Agent/BP-global tiers, but ORDERED FIRST —
//! so a Step-declared key wins over an Agent/BP-global-declared key for
//! the same name (Run/Task tier keys are already individually present in
//! `ctx.meta.runtime` by the time this layer runs and are therefore
//! untouched either way — the full cascade is Run > Task > Step > Agent >
//! BP-global). A non-`Object` `STEP_CTX_KEY` value is warned and skipped,
//! same as a malformed Agent/BP-global tier. The raw `STEP_CTX_KEY`
//! bundle itself stays in the runtime bag verbatim (in-process workers
//! may read it directly) — it is NOT folded into `view.extra` as a whole;
//! only its individual keys are.

use crate::core::agent_context::{
    AgentContextView, ContextPolicy, AGENT_CONTEXT_KEY, PROJECT_NAME_ALIAS_KEY, RUN_ID_KEY,
    STEP_CTX_KEY, TASK_METADATA_KEY, TASK_PROJECT_ROOT_KEY, TASK_WORK_DIR_KEY,
};
use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::middleware::SpawnerLayer;
use crate::types::{CapToken, StepId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter};
use crate::worker::Worker;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// The 5 `AgentContextView` named-field runtime keys — a merged tier key
/// that matches one of these becomes a design-time default for the
/// matching named field (via [`AgentContextView::from_ctx`]) rather than
/// an `extra` entry.
const NAMED_RUNTIME_KEYS: [&str; 5] = [
    TASK_PROJECT_ROOT_KEY,
    TASK_WORK_DIR_KEY,
    TASK_METADATA_KEY,
    RUN_ID_KEY,
    PROJECT_NAME_ALIAS_KEY,
];

/// `SpawnerLayer` that materializes an [`AgentContextView`] from `ctx`
/// (after merging in the BP-declared agent-context supply tiers — see the
/// module doc), snapshots it into engine state (Worker axis source), and
/// stashes the serialized view into `ctx.meta.runtime[AGENT_CONTEXT_KEY]`
/// (Spawner axis source) before delegating to `inner`.
pub struct AgentContextMiddleware {
    /// "BP Global" tier of the agent-context supply axis
    /// (`Blueprint.default_agent_ctx`). `None` = no BP-global default.
    global_ctx: Option<Value>,
    /// "BP Agent-level" tier, keyed by `AgentDef.name` (`AgentMeta.ctx`).
    /// An agent absent from this map declares no per-agent context.
    per_agent_ctx: HashMap<String, Value>,
    /// "BP Global" tier of the [`ContextPolicy`] cascade
    /// (`Blueprint.default_context_policy`).
    default_policy: Option<ContextPolicy>,
    /// "BP Agent-level" tier, keyed by `AgentDef.name`
    /// (`AgentMeta.context_policy`) — outranks `default_policy` for the
    /// matching agent.
    per_agent_policy: HashMap<String, ContextPolicy>,
}

impl AgentContextMiddleware {
    /// Wraps the 4-piece agent-context supply state derived at launch time
    /// (`service::task_launch::derive_agent_ctx` /
    /// `derive_context_policies`) to apply on every spawn.
    pub fn new(
        global_ctx: Option<Value>,
        per_agent_ctx: HashMap<String, Value>,
        default_policy: Option<ContextPolicy>,
        per_agent_policy: HashMap<String, ContextPolicy>,
    ) -> Self {
        Self {
            global_ctx,
            per_agent_ctx,
            default_policy,
            per_agent_policy,
        }
    }
}

impl Default for AgentContextMiddleware {
    /// All-empty state (no BP-declared context or policy tiers, pass-all
    /// filtering) — the pre-#21 (#20) behavior: `AgentContextView` is
    /// materialized straight off `ctx` with no merge, no filtering.
    fn default() -> Self {
        Self::new(None, HashMap::new(), None, HashMap::new())
    }
}

impl SpawnerLayer for AgentContextMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(AgentContextWrapped {
            inner,
            global_ctx: self.global_ctx.clone(),
            per_agent_ctx: self.per_agent_ctx.clone(),
            default_policy: self.default_policy.clone(),
            per_agent_policy: self.per_agent_policy.clone(),
        })
    }
}

struct AgentContextWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    global_ctx: Option<Value>,
    per_agent_ctx: HashMap<String, Value>,
    default_policy: Option<ContextPolicy>,
    per_agent_policy: HashMap<String, ContextPolicy>,
}

impl AgentContextWrapped {
    /// Shallow-merges `global_ctx` ⊕ `per_agent_ctx[agent]` (agent wins on
    /// key collision). A tier whose declared `Value` isn't a JSON `Object`
    /// is logged and skipped — this never fails the spawn (see the module
    /// doc's supply-tier narrative).
    fn merge_ctx_tiers(&self, agent: &str) -> serde_json::Map<String, Value> {
        let mut merged = serde_json::Map::new();
        if let Some(global) = &self.global_ctx {
            match global.as_object() {
                Some(obj) => {
                    for (k, v) in obj {
                        merged.insert(k.clone(), v.clone());
                    }
                }
                None => tracing::warn!(
                    value = %global,
                    "AgentContextMiddleware: default_agent_ctx is not a JSON object; skipping this tier"
                ),
            }
        }
        if let Some(per_agent) = self.per_agent_ctx.get(agent) {
            match per_agent.as_object() {
                Some(obj) => {
                    for (k, v) in obj {
                        merged.insert(k.clone(), v.clone());
                    }
                }
                None => tracing::warn!(
                    agent = %agent,
                    value = %per_agent,
                    "AgentContextMiddleware: AgentMeta.ctx is not a JSON object; skipping this tier"
                ),
            }
        }
        merged
    }
}

#[async_trait]
impl SpawnerAdapter for AgentContextWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        // Step 0 (GH #21 Phase 2): unpack the Step tier's STEP_CTX_KEY
        // bundle (if present and a JSON Object) into a flat key/value
        // map — same shape as merge_ctx_tiers' output, applied FIRST
        // below so a Step-declared key wins over an Agent/BP-global one
        // on collision (see the module doc's Step-tier narrative). A
        // non-Object value is warned and skipped, never failing the
        // spawn.
        let step_tier: serde_json::Map<String, Value> = match ctx.meta.runtime.get(STEP_CTX_KEY) {
            Some(Value::Object(obj)) => obj.clone(),
            Some(other) => {
                tracing::warn!(
                    value = %other,
                    "AgentContextMiddleware: step_ctx runtime bundle is not a JSON object; skipping the Step tier"
                );
                serde_json::Map::new()
            }
            None => serde_json::Map::new(),
        };

        // Step 1: resolve the merged BP-declared context tiers for this
        // agent (empty when neither tier is declared — the #20 no-op
        // path).
        let merged = self.merge_ctx_tiers(&ctx.agent);

        // Step 2: insert every merged key into ctx.meta.runtime
        // only-if-absent — Step tier FIRST, then Agent/BP-global. An
        // outer runtime-supplied value (TaskInput / alias / worker-binding
        // / Run, all layered OUTSIDE this one or inserted directly by
        // Engine::dispatch_attempt_with before this stack runs) always
        // wins the race — see the module doc.
        let mut new_ctx = ctx.clone();
        for (k, v) in &step_tier {
            new_ctx
                .meta
                .runtime
                .entry(k.clone())
                .or_insert_with(|| v.clone());
        }
        for (k, v) in &merged {
            new_ctx
                .meta
                .runtime
                .entry(k.clone())
                .or_insert_with(|| v.clone());
        }

        // Step 3: materialize the view off the (possibly BP-defaulted)
        // ctx, then fold Step + merged keys that aren't one of the 5
        // named fields into view.extra, only-if-absent (Step tier FIRST,
        // same ordering as Step 2) — do NOT re-handle named keys here,
        // from_ctx already picked them up out of ctx.meta.runtime.
        let mut view = AgentContextView::from_ctx(&new_ctx);
        for (k, v) in step_tier.iter().chain(merged.iter()) {
            if !NAMED_RUNTIME_KEYS.contains(&k.as_str()) {
                view.extra.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }

        // Step 4: resolve the effective ContextPolicy (per-agent outranks
        // BP-global; pass-all when neither is declared) and apply it.
        let policy = self
            .per_agent_policy
            .get(&ctx.agent)
            .or(self.default_policy.as_ref());
        let view = match policy {
            Some(p) => view.apply_policy(p),
            None => view,
        };

        // Worker axis source: snapshot into EngineState.agent_contexts,
        // keyed the same way EngineState.prompts / .systems are — Ctx
        // itself is not stored, so the view has to be captured here to
        // still be servable when fetch_worker_payload{,_trusted} runs
        // later.
        let view_for_state = view.clone();
        let task_id_for_state = task_id.clone();
        engine
            .with_state("agent_context.materialize", move |s| {
                s.agent_contexts
                    .insert((task_id_for_state, attempt), view_for_state);
            })
            .await
            .map_err(|e| SpawnError::Internal(format!("agent_context state insert: {e}")))?;

        // Spawner axis source: stash the serialized view into ctx so
        // AgentContextView::materialized_or_from_ctx can read it back
        // downstream (WS session.rs / in-process AgentBlock runtime.rs).
        // Serialization failure never fails the spawn — proceed with the
        // ctx as merged so far (downstream falls back to
        // AgentContextView::from_ctx, same as if this layer were absent).
        match serde_json::to_value(&view) {
            Ok(value) => {
                new_ctx
                    .meta
                    .runtime
                    .insert(AGENT_CONTEXT_KEY.to_string(), value);
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    task_id = %task_id,
                    "AgentContextView failed to serialize; proceeding without agent_context in ctx.meta.runtime"
                );
            }
        }

        self.inner
            .spawn(engine, &new_ctx, task_id, attempt, token)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::agent_context::{TASK_PROJECT_ROOT_KEY, TASK_WORK_DIR_KEY};
    use crate::core::config::EngineCfg;
    use crate::types::Role;
    use serde_json::Value;
    use std::sync::Mutex;
    use std::time::Duration;

    /// Inner spawner stub that records the `Ctx` it was called with and
    /// fails the spawn (we only care about the ctx snapshot).
    struct CtxProbe {
        seen: Arc<Mutex<Option<Ctx>>>,
    }

    #[async_trait]
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

    fn probe_stack(
        layer: AgentContextMiddleware,
    ) -> (Arc<dyn SpawnerAdapter>, Arc<Mutex<Option<Ctx>>>) {
        let seen = Arc::new(Mutex::new(None));
        let inner = Arc::new(CtxProbe { seen: seen.clone() });
        let wrapped = layer.wrap(inner);
        (wrapped, seen)
    }

    #[tokio::test]
    async fn snapshots_view_into_engine_state_agent_contexts() {
        let (stack, _seen) = probe_stack(AgentContextMiddleware::default());
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-1").unwrap();
        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        ctx.meta.runtime.insert(
            TASK_PROJECT_ROOT_KEY.to_string(),
            Value::String("/repo".to_string()),
        );
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id.clone(), 1, token).await;

        let snapshotted = engine
            .with_state("test.read_agent_contexts", move |s| {
                s.agent_contexts.get(&(task_id, 1)).cloned()
            })
            .await
            .expect("with_state")
            .expect("agent_contexts entry present");
        assert_eq!(snapshotted.project_root.as_deref(), Some("/repo"));
    }

    #[tokio::test]
    async fn inner_spawner_observes_agent_context_key_in_ctx() {
        let (stack, seen) = probe_stack(AgentContextMiddleware::default());
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-2").unwrap();
        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            Value::String("/repo/work".to_string()),
        );
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");
        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");
        assert_eq!(view.work_dir.as_deref(), Some("/repo/work"));
    }

    #[tokio::test]
    async fn policy_exclude_is_reflected_in_both_state_and_ctx() {
        let policy = ContextPolicy {
            include: None,
            exclude: vec!["work_dir".to_string()],
        };
        let (stack, seen) = probe_stack(AgentContextMiddleware::new(
            None,
            HashMap::new(),
            Some(policy),
            HashMap::new(),
        ));
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-3").unwrap();
        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        ctx.meta.runtime.insert(
            TASK_PROJECT_ROOT_KEY.to_string(),
            Value::String("/repo".to_string()),
        );
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            Value::String("/repo/work".to_string()),
        );
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id.clone(), 1, token).await;

        let snapshotted = engine
            .with_state("test.read_agent_contexts_excluded", move |s| {
                s.agent_contexts.get(&(task_id, 1)).cloned()
            })
            .await
            .expect("with_state")
            .expect("agent_contexts entry present");
        assert_eq!(snapshotted.project_root.as_deref(), Some("/repo"));
        assert!(snapshotted.work_dir.is_none());

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");
        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");
        assert_eq!(view.project_root.as_deref(), Some("/repo"));
        assert!(view.work_dir.is_none());
    }

    // ──────────────────────────────────────────────────────────────
    // issue #21 Phase 1: BP-declared supply tiers (derive_agent_ctx /
    // derive_context_policies output, wired into AgentContextMiddleware)
    // ──────────────────────────────────────────────────────────────

    /// Only-if-absent proof (1/2): a runtime key an outer layer already
    /// inserted (simulated here — `TaskInputMiddleware` layers OUTSIDE
    /// this one, so it always runs first) must survive an agent-declared
    /// default for the same key.
    #[tokio::test]
    async fn precedence_pre_inserted_runtime_key_survives_agent_declared_default() {
        let mut per_agent_ctx = HashMap::new();
        per_agent_ctx.insert(
            "planner".to_string(),
            serde_json::json!({ "work_dir": "/agent-declared" }),
        );
        let (stack, seen) = probe_stack(AgentContextMiddleware::new(
            None,
            per_agent_ctx,
            None,
            HashMap::new(),
        ));
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-4").unwrap();
        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            Value::String("/task-declared".to_string()),
        );
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");
        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");
        assert_eq!(
            view.work_dir.as_deref(),
            Some("/task-declared"),
            "an already-present runtime key must win over the agent-declared default"
        );
    }

    /// Only-if-absent proof (2/2): the absent case gets the agent-declared
    /// value.
    #[tokio::test]
    async fn precedence_absent_runtime_key_gets_agent_declared_default() {
        let mut per_agent_ctx = HashMap::new();
        per_agent_ctx.insert(
            "planner".to_string(),
            serde_json::json!({ "work_dir": "/agent-declared" }),
        );
        let (stack, seen) = probe_stack(AgentContextMiddleware::new(
            None,
            per_agent_ctx,
            None,
            HashMap::new(),
        ));
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-5").unwrap();
        let ctx = Ctx::new(task_id.clone(), 1, "planner");
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");
        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");
        assert_eq!(view.work_dir.as_deref(), Some("/agent-declared"));
    }

    /// Agent wins over global on key collision.
    #[tokio::test]
    async fn agent_ctx_wins_over_global_on_key_collision() {
        let mut per_agent_ctx = HashMap::new();
        per_agent_ctx.insert(
            "planner".to_string(),
            serde_json::json!({ "work_dir": "/agent" }),
        );
        let (stack, seen) = probe_stack(AgentContextMiddleware::new(
            Some(serde_json::json!({ "work_dir": "/global" })),
            per_agent_ctx,
            None,
            HashMap::new(),
        ));
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-6").unwrap();
        let ctx = Ctx::new(task_id.clone(), 1, "planner");
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");
        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");
        assert_eq!(view.work_dir.as_deref(), Some("/agent"));
    }

    /// Unknown-key end-to-end: a BP-declared key with no matching named
    /// `AgentContextView` field must appear in (a) `ctx.meta.runtime`,
    /// (b) `view.extra`, (c) `to_directive_header()`, and (d) the
    /// serialized view JSON (what `WorkerPayload.context` carries).
    #[tokio::test]
    async fn unknown_key_reaches_runtime_extra_directive_and_serialized_json() {
        let mut per_agent_ctx = HashMap::new();
        per_agent_ctx.insert(
            "planner".to_string(),
            serde_json::json!({ "org_conventions": "x" }),
        );
        let (stack, seen) = probe_stack(AgentContextMiddleware::new(
            None,
            per_agent_ctx,
            None,
            HashMap::new(),
        ));
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-7").unwrap();
        let ctx = Ctx::new(task_id.clone(), 1, "planner");
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");

        // (a) ctx.meta.runtime
        assert_eq!(
            observed.meta.runtime.get("org_conventions"),
            Some(&Value::String("x".to_string())),
            "unknown key must land in ctx.meta.runtime too (in-process workers read ctx directly)"
        );

        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");

        // (d) serialized view JSON
        assert_eq!(
            raw.get("extra").and_then(|e| e.get("org_conventions")),
            Some(&Value::String("x".to_string()))
        );

        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");

        // (b) view.extra
        assert_eq!(
            view.extra.get("org_conventions"),
            Some(&Value::String("x".to_string()))
        );

        // (c) to_directive_header() output line
        let header = view.to_directive_header();
        assert!(
            header.contains("org_conventions: \"x\"\n"),
            "header must render the unknown extra key: {header}"
        );
    }

    /// Policy resolution: a per-agent policy outranks the BP-global
    /// default for the matching agent; another agent with no per-agent
    /// override falls through to the BP-global default.
    #[tokio::test]
    async fn per_agent_policy_outranks_default_and_other_agents_fall_through() {
        let default_policy = ContextPolicy {
            include: None,
            exclude: vec!["work_dir".to_string()],
        };
        let mut per_agent_policy = HashMap::new();
        per_agent_policy.insert(
            "planner".to_string(),
            ContextPolicy {
                include: None,
                exclude: vec![],
            },
        );

        let engine = Engine::new(EngineCfg::default());

        // "planner" has a pass-all override — must win over the BP-global
        // exclude.
        let (stack, seen) = probe_stack(AgentContextMiddleware::new(
            None,
            HashMap::new(),
            Some(default_policy.clone()),
            per_agent_policy.clone(),
        ));
        let task_id = StepId::parse("ST-8a").unwrap();
        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            Value::String("/repo/work".to_string()),
        );
        let token = engine
            .attach("ut-op-a", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;
        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");
        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");
        assert_eq!(
            view.work_dir.as_deref(),
            Some("/repo/work"),
            "per-agent pass-all override must win over the BP-global exclude"
        );

        // "other" has no per-agent override — falls through to the
        // BP-global exclude.
        let (stack2, seen2) = probe_stack(AgentContextMiddleware::new(
            None,
            HashMap::new(),
            Some(default_policy),
            per_agent_policy,
        ));
        let task_id2 = StepId::parse("ST-8b").unwrap();
        let mut ctx2 = Ctx::new(task_id2.clone(), 1, "other");
        ctx2.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            Value::String("/repo/work".to_string()),
        );
        let token2 = engine
            .attach("ut-op-b", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack2.spawn(&engine, &ctx2, task_id2, 1, token2).await;
        let observed2 = seen2.lock().unwrap().clone().expect("inner ctx captured");
        let raw2 = observed2
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");
        let view2: AgentContextView = serde_json::from_value(raw2.clone()).expect("round-trip");
        assert!(
            view2.work_dir.is_none(),
            "an agent without a per-agent override must fall through to the BP-global exclude"
        );
    }

    /// A tier whose declared value isn't a JSON `Object` is warned and
    /// skipped — the spawn still proceeds to `inner.spawn` (never fails
    /// because of the malformed tier).
    #[tokio::test]
    async fn non_object_ctx_tier_value_is_skipped_and_spawn_still_proceeds() {
        let (stack, seen) = probe_stack(AgentContextMiddleware::new(
            Some(Value::String("not-an-object".to_string())),
            HashMap::new(),
            None,
            HashMap::new(),
        ));
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-9").unwrap();
        let ctx = Ctx::new(task_id.clone(), 1, "planner");
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        // CtxProbe (the inner adapter) always errors with "probe stop" —
        // the assertion below only cares that the middleware reached
        // inner.spawn at all (a merge failure would short-circuit before
        // ever calling it).
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone();
        assert!(
            observed.is_some(),
            "a non-Object tier value must not stop the spawn from reaching inner"
        );
    }

    // ──────────────────────────────────────────────────────────────
    // issue #21 Phase 2: the Step tier (ctx.meta.runtime[STEP_CTX_KEY])
    // ──────────────────────────────────────────────────────────────

    /// Step key beats an agent-declared key of the same name (Step
    /// outranks Agent in the cascade).
    #[tokio::test]
    async fn step_key_beats_agent_declared_same_key() {
        let mut per_agent_ctx = HashMap::new();
        per_agent_ctx.insert(
            "planner".to_string(),
            serde_json::json!({ "work_dir": "/agent-declared" }),
        );
        let (stack, seen) = probe_stack(AgentContextMiddleware::new(
            None,
            per_agent_ctx,
            None,
            HashMap::new(),
        ));
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-10").unwrap();
        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        ctx.meta.runtime.insert(
            STEP_CTX_KEY.to_string(),
            serde_json::json!({ "work_dir": "/step-declared" }),
        );
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");
        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");
        assert_eq!(
            view.work_dir.as_deref(),
            Some("/step-declared"),
            "the Step tier must win over an agent-declared default for the same key"
        );
    }

    /// A Task-preinserted (outer-layer) runtime key beats the Step tier —
    /// full precedence Run > Task > Step > Agent > BP-global.
    #[tokio::test]
    async fn task_preinserted_key_beats_step_tier() {
        let (stack, seen) = probe_stack(AgentContextMiddleware::default());
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-11").unwrap();
        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        // Simulates the outer TaskInputMiddleware layer already having
        // inserted this key before this (innermost) layer runs.
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            Value::String("/task-declared".to_string()),
        );
        ctx.meta.runtime.insert(
            STEP_CTX_KEY.to_string(),
            serde_json::json!({ "work_dir": "/step-declared" }),
        );
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");
        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");
        assert_eq!(
            view.work_dir.as_deref(),
            Some("/task-declared"),
            "an already-present (Task-tier) runtime key must win over the Step tier"
        );
    }

    /// Unknown step key end-to-end: a Step-declared key with no matching
    /// named `AgentContextView` field must appear in (a) `ctx.meta.runtime`,
    /// (b) `view.extra`, (c) `to_directive_header()`, and (d) the
    /// serialized view JSON.
    #[tokio::test]
    async fn unknown_step_key_reaches_runtime_extra_directive_and_serialized_json() {
        let (stack, seen) = probe_stack(AgentContextMiddleware::default());
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-12").unwrap();
        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        ctx.meta.runtime.insert(
            STEP_CTX_KEY.to_string(),
            serde_json::json!({ "loop_idx": 2 }),
        );
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");

        // (a) ctx.meta.runtime
        assert_eq!(
            observed.meta.runtime.get("loop_idx"),
            Some(&serde_json::json!(2)),
            "unknown step key must land in ctx.meta.runtime too"
        );

        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");

        // (d) serialized view JSON
        assert_eq!(
            raw.get("extra").and_then(|e| e.get("loop_idx")),
            Some(&serde_json::json!(2))
        );

        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");

        // (b) view.extra
        assert_eq!(view.extra.get("loop_idx"), Some(&serde_json::json!(2)));

        // (c) to_directive_header() output line
        let header = view.to_directive_header();
        assert!(
            header.contains("loop_idx: 2\n"),
            "header must render the unknown Step-tier extra key: {header}"
        );

        // The raw STEP_CTX_KEY bundle itself stays in ctx.meta.runtime
        // verbatim (in-process workers may read it) but is NOT folded
        // whole into view.extra — only its individual keys are.
        assert!(observed.meta.runtime.contains_key(STEP_CTX_KEY));
        assert!(!view.extra.contains_key(STEP_CTX_KEY));
    }

    /// No envelope (no STEP_CTX_KEY in ctx.meta.runtime) → behavior
    /// identical to subtask-1 (GH #21 Phase 1, no Step tier).
    #[tokio::test]
    async fn no_step_ctx_key_behaves_identically_to_phase_1() {
        let mut per_agent_ctx = HashMap::new();
        per_agent_ctx.insert(
            "planner".to_string(),
            serde_json::json!({ "work_dir": "/agent-declared" }),
        );
        let (stack, seen) = probe_stack(AgentContextMiddleware::new(
            None,
            per_agent_ctx,
            None,
            HashMap::new(),
        ));
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-13").unwrap();
        let ctx = Ctx::new(task_id.clone(), 1, "planner");
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        let raw = observed
            .meta
            .runtime
            .get(AGENT_CONTEXT_KEY)
            .expect("agent_context key present");
        let view: AgentContextView = serde_json::from_value(raw.clone()).expect("round-trip");
        assert_eq!(
            view.work_dir.as_deref(),
            Some("/agent-declared"),
            "absent STEP_CTX_KEY must fall straight through to the Agent tier, unchanged from Phase 1"
        );
    }
}
