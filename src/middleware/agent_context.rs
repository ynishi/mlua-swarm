//! `AgentContextMiddleware` — the innermost `SpawnerLayer` that
//! materializes [`AgentContextView`] once per spawn (Contract C, GH #20).
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

use crate::core::agent_context::{AgentContextView, ContextPolicy, AGENT_CONTEXT_KEY};
use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::middleware::SpawnerLayer;
use crate::types::{CapToken, StepId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter};
use crate::worker::Worker;
use async_trait::async_trait;
use std::sync::Arc;

/// `SpawnerLayer` that materializes an [`AgentContextView`] from `ctx`,
/// snapshots it into engine state (Worker axis source), and stashes the
/// serialized view into `ctx.meta.runtime[AGENT_CONTEXT_KEY]` (Spawner
/// axis source) before delegating to `inner`.
pub struct AgentContextMiddleware {
    /// Filter applied to the materialized view before it is snapshotted /
    /// stashed. `ContextPolicy::default()` (pass-all) unless a caller
    /// opts into a narrower one — see [`Self::new`].
    policy: ContextPolicy,
}

impl AgentContextMiddleware {
    /// Wraps a [`ContextPolicy`] to apply on every spawn.
    pub fn new(policy: ContextPolicy) -> Self {
        Self { policy }
    }
}

impl Default for AgentContextMiddleware {
    /// Pass-all policy (`ContextPolicy::default()`) — the wiring at
    /// `service::task_launch` uses this unconditionally today;
    /// Blueprint-driven policy construction is a future issue.
    fn default() -> Self {
        Self::new(ContextPolicy::default())
    }
}

impl SpawnerLayer for AgentContextMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(AgentContextWrapped {
            inner,
            policy: self.policy.clone(),
        })
    }
}

struct AgentContextWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    policy: ContextPolicy,
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
        let view = self.policy.apply(AgentContextView::from_ctx(ctx));

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
        // original ctx (downstream falls back to
        // AgentContextView::from_ctx, same as if this layer were absent).
        let mut new_ctx = ctx.clone();
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
        let (stack, seen) = probe_stack(AgentContextMiddleware::new(policy));
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
}
