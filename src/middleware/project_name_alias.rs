//! `ProjectNameAliasMiddleware` — a `SpawnerLayer` that propagates a
//! task-level project alias.
//!
//! When `Blueprint.metadata.project_name_alias` is `Some(_)`, `service::
//! linker::link` places this layer on the stack. Just before spawn it
//! inserts the literal value into `Ctx.meta.runtime` under the
//! `project_name_alias` key.
//!
//! Downstream Operator / spawner code (for example `mlua-swarm-server`'s
//! `Operator::execute`) reads it via
//! `ctx.meta.runtime.get("project_name_alias")` and splices it into the
//! Spawn directive prompt body, handing MainAI the discipline "run
//! `mcp__lds__session_create(root=..., alias=<this>)` and inject
//! `LDS Session Alias: <this>` into every SubAgent dispatch prompt". The
//! authoritative discipline lives in `docs/project-name-alias.md`, the
//! public doc.
//!
//! Same shape as `AgentResolver` / `ResolverMiddleware`: a thin layer that
//! does not touch engine state.

use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::middleware::SpawnerLayer;
use crate::types::{CapToken, StepId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter};
use crate::worker::Worker;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Key under `ctx.meta.runtime` that downstream Operator code reads with
/// `get`.
pub const PROJECT_NAME_ALIAS_KEY: &str = "project_name_alias";

/// `SpawnerLayer` that drops the received alias into `ctx` just before spawn.
pub struct ProjectNameAliasMiddleware {
    alias: String,
}

impl ProjectNameAliasMiddleware {
    /// Wraps a project alias string to inject on every spawn.
    pub fn new(alias: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
        }
    }
}

impl SpawnerLayer for ProjectNameAliasMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(ProjectNameAliasWrapped {
            inner,
            alias: self.alias.clone(),
        })
    }
}

struct ProjectNameAliasWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    alias: String,
}

#[async_trait]
impl SpawnerAdapter for ProjectNameAliasWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let mut new_ctx = ctx.clone();
        new_ctx.meta.runtime.insert(
            PROJECT_NAME_ALIAS_KEY.to_string(),
            Value::String(self.alias.clone()),
        );
        self.inner
            .spawn(engine, &new_ctx, task_id, attempt, token)
            .await
    }
}
