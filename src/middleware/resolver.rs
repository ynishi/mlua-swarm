//! Routing resolver — dynamic agent resolution at spawn time. Slotted into
//! the `SpawnerStack` as a `SpawnerLayer`. Treats `Ctx.agent` as a hint,
//! rewrites it to the actual dispatch target, and hands the updated `Ctx`
//! down to the inner spawner.
//!
//! Examples:
//!   - `Ctx.agent = "best-coder"` — the resolver picks `claude` or `gpt-4` or `llama`.
//!   - `Ctx.agent = "router:by-prompt"` — branch on prompt contents.
//!   - `Ctx.agent = "ensemble"` — fan out to multiple agents (delegated to a
//!     different layer or a fanout stage).
//!
//! One of the axes that a plugin can drive (see handoff §Plugin).

use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::middleware::SpawnerLayer;
use crate::types::{CapToken, StepId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter};
use crate::worker::Worker;
use async_trait::async_trait;
use std::sync::Arc;

/// Routing resolver trait. Takes the `Ctx.agent` hint and returns a real
/// agent name the inner spawner can resolve. Sync is enough — this is meant
/// for light lookups. Push heavy resolvers into a separate spawner layer or
/// a Lua plugin.
///
/// The `directive` argument was removed in the current design: prompts now travel
/// through engine state and no longer appear in spawner arguments. If you
/// need prompt-content-driven routing, either have the resolver call
/// `engine.fetch_prompt(token, task_id)` from a separate layer, or implement
/// a dedicated prompt-based routing layer (carry).
pub trait AgentResolver: Send + Sync + 'static {
    /// `agent_hint` is the raw `Ctx.agent` value. The returned string is
    /// installed as the new `Ctx.agent` before the inner spawner is called.
    fn resolve(&self, agent_hint: &str, ctx: &Ctx) -> String;
}

/// Wrapper that lets a closure act as an `AgentResolver` via a blanket impl.
pub struct FnResolver<F>(
    /// The closure implementing `Fn(&str, &Ctx) -> String`.
    pub F,
);

impl<F> AgentResolver for FnResolver<F>
where
    F: Fn(&str, &Ctx) -> String + Send + Sync + 'static,
{
    fn resolve(&self, hint: &str, ctx: &Ctx) -> String {
        (self.0)(hint, ctx)
    }
}

/// `SpawnerLayer` implementation — inject into a `SpawnerStack` and use.
pub struct ResolverMiddleware {
    resolver: Arc<dyn AgentResolver>,
}

impl ResolverMiddleware {
    /// Wraps an existing `AgentResolver` implementation.
    pub fn new(resolver: Arc<dyn AgentResolver>) -> Self {
        Self { resolver }
    }

    /// Convenience constructor: wraps a plain closure as the resolver via
    /// `FnResolver`.
    pub fn from_fn<F>(f: F) -> Self
    where
        F: Fn(&str, &Ctx) -> String + Send + Sync + 'static,
    {
        Self {
            resolver: Arc::new(FnResolver(f)),
        }
    }
}

impl SpawnerLayer for ResolverMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(ResolverWrapped {
            inner,
            resolver: self.resolver.clone(),
        })
    }
}

struct ResolverWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    resolver: Arc<dyn AgentResolver>,
}

#[async_trait]
impl SpawnerAdapter for ResolverWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let resolved = self.resolver.resolve(&ctx.agent, ctx);
        if resolved == ctx.agent {
            // no-op: hint and resolved are the same
            self.inner.spawn(engine, ctx, task_id, attempt, token).await
        } else {
            // Clone ctx and overwrite `agent`, then hand it to `inner`.
            let mut new_ctx = ctx.clone();
            new_ctx.agent = resolved;
            self.inner
                .spawn(engine, &new_ctx, task_id, attempt, token)
                .await
        }
    }
}
