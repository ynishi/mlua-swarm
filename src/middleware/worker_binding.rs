//! `WorkerBindingMiddleware` — a `SpawnerLayer` that propagates the
//! Blueprint-baked per-agent [`WorkerBinding`] through `Ctx.meta.runtime`.
//!
//! `service::task_launch` builds an `agent name → WorkerBinding` map from
//! `Blueprint.agents[].profile.worker_binding` at launch time and places
//! this layer on the stack (outermost, next to `ProjectNameAliasMiddleware`).
//! Just before spawn it looks the map up by `ctx.agent` and, on a hit,
//! inserts the serialized binding into `Ctx.meta.runtime` under the
//! `worker_binding` key.
//!
//! Downstream `OperatorDelegateMiddleware` reads it back so the delegate
//! axis (session-global Operator delegation, which has no per-agent
//! `OperatorSpawner` to carry a compile-time-baked binding) can hand
//! `Some(worker)` to `Operator::execute` instead of the historical
//! hardcoded `None`. Agents with no declared binding get no entry — the
//! WS thin-path `requires_worker_binding` fail-loud stays the safety net.
//!
//! Same shape as `ProjectNameAliasMiddleware` / `CompiledAgentTable`: a
//! compile/launch-time table keyed by agent name, looked up via
//! `ctx.agent` at spawn time, no engine state touched.

use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::middleware::SpawnerLayer;
use crate::operator::WorkerBinding;
use crate::types::{CapToken, StepId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter};
use crate::worker::Worker;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

/// Key under `ctx.meta.runtime` that downstream code (the delegate axis)
/// reads with `get`.
pub const WORKER_BINDING_KEY: &str = "worker_binding";

/// `SpawnerLayer` that drops the per-agent binding into `ctx` just before
/// spawn.
pub struct WorkerBindingMiddleware {
    bindings: Arc<HashMap<String, WorkerBinding>>,
}

impl WorkerBindingMiddleware {
    /// Wraps an `agent name → WorkerBinding` map to inject on every spawn
    /// whose `ctx.agent` has an entry.
    pub fn new(bindings: HashMap<String, WorkerBinding>) -> Self {
        Self {
            bindings: Arc::new(bindings),
        }
    }
}

impl SpawnerLayer for WorkerBindingMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(WorkerBindingWrapped {
            inner,
            bindings: self.bindings.clone(),
        })
    }
}

struct WorkerBindingWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    bindings: Arc<HashMap<String, WorkerBinding>>,
}

#[async_trait]
impl SpawnerAdapter for WorkerBindingWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let Some(binding) = self.bindings.get(&ctx.agent) else {
            // No declared binding for this agent — pass through untouched;
            // binding-requiring backends fail loud downstream.
            return self.inner.spawn(engine, ctx, task_id, attempt, token).await;
        };
        let value = serde_json::to_value(binding).map_err(|e| {
            SpawnError::Internal(format!(
                "worker_binding for agent '{}' failed to serialize: {e}",
                ctx.agent
            ))
        })?;
        let mut new_ctx = ctx.clone();
        new_ctx
            .meta
            .runtime
            .insert(WORKER_BINDING_KEY.to_string(), value);
        self.inner
            .spawn(engine, &new_ctx, task_id, attempt, token)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::EngineCfg;
    use crate::types::Role;
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
        bindings: HashMap<String, WorkerBinding>,
    ) -> (Arc<dyn SpawnerAdapter>, Arc<Mutex<Option<Ctx>>>) {
        let seen = Arc::new(Mutex::new(None));
        let inner = Arc::new(CtxProbe { seen: seen.clone() });
        let wrapped = WorkerBindingMiddleware::new(bindings).wrap(inner);
        (wrapped, seen)
    }

    #[tokio::test]
    async fn injects_binding_into_ctx_meta_runtime_on_hit() {
        let mut map = HashMap::new();
        map.insert(
            "planner".to_string(),
            WorkerBinding {
                variant: "mse-worker-knowledge".to_string(),
                tools: vec!["Read".to_string()],
            },
        );
        let (stack, seen) = probe_stack(map);
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId("t-1".to_string());
        let ctx = Ctx::new(task_id.clone(), 1, "planner");
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        let v = observed
            .meta
            .runtime
            .get(WORKER_BINDING_KEY)
            .expect("worker_binding key present");
        let wb: WorkerBinding = serde_json::from_value(v.clone()).expect("round-trip");
        assert_eq!(wb.variant, "mse-worker-knowledge");
        assert_eq!(wb.tools, vec!["Read".to_string()]);
    }

    #[tokio::test]
    async fn passes_through_untouched_on_miss() {
        let (stack, seen) = probe_stack(HashMap::new());
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId("t-2".to_string());
        let ctx = Ctx::new(task_id.clone(), 1, "unbound-agent");
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        assert!(
            !observed.meta.runtime.contains_key(WORKER_BINDING_KEY),
            "no binding entry must be injected on miss"
        );
    }
}
