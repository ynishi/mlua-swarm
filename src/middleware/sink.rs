//! `SinkMiddleware` — the `SpawnerLayer` for the Data plane (Big Response
//! handling).
//!
//! # Role
//!
//! A `SpawnerLayer` that bridges [`crate::store::output`] (the Data owner) and
//! the Agent execution boundary. Just before spawn it injects a **Data-plane
//! endpoint hint** into `Ctx.meta.runtime`, giving SubAgents a place to POST
//! Big Response bodies (4k-token payloads, files, intermediate artifacts)
//! **directly into the store, bypassing MainAgent**.
//!
//! It does not touch the Domain path (the engine's `submit_output` /
//! `output_tail` / dispatch verdict). The flow stays as-is; this layer is
//! strictly additive and runs alongside (the Data / Domain separation axis).
//! For the canonical narrative see the [`crate::store::output`] module doc.
//!
//! # Pattern
//!
//! Same shape as `AgentResolver` and `ProjectNameAliasMiddleware`: edit `ctx`,
//! call the inner spawner, done. Engine state is not touched.
//!
//! # Implementation status
//!
//! - **Current (scaffold):** `SpawnerLayer` trait impl plus the endpoint hint
//!   injection into `Ctx.meta.runtime`. The `Arc<dyn OutputStore>` reference
//!   is held in config, but the real intake path (the `POST /v1/data/emit`
//!   HTTP handler routed to `OutputStore::append`) is still a carry.
//! - **Carry:** add the Big Data endpoint on the `mlua-swarm-server` side, wire the
//!   SubAgent-side EMIT tool call driven by the `agent.md` contract, and
//!   thread it through end-to-end.

use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::middleware::SpawnerLayer;
use crate::store::output::OutputStore;
use crate::types::{CapToken, StepId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter};
use crate::worker::Worker;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Key under `ctx.meta.runtime` that carries the Data-plane endpoint hint.
///
/// Downstream Operator / Spawner code is expected to look this key up and
/// splice a line into the SubAgent's Spawn directive prompt telling it to
/// `POST` Big Data payloads to this endpoint.
pub const DATA_SINK_ENDPOINT_KEY: &str = "data_sink_endpoint";

/// Data-plane `SpawnerLayer`. Config: the store to reference plus the
/// endpoint hint.
///
/// The endpoint hint is the literal URL a SubAgent will `POST` a Big EMIT to
/// (for example `"http://127.0.0.1:7785/v1/data/emit"`). The actual HTTP
/// endpoint lives on the `mlua-swarm-server` side (carry); this layer only routes
/// the hint value into `ctx`.
pub struct SinkMiddleware {
    store: Arc<dyn OutputStore>,
    endpoint_hint: String,
}

impl SinkMiddleware {
    /// Build a new layer. `store` is the Data owner (the real home for Big
    /// bodies); `endpoint_hint` is the URL literal the SubAgent should `POST`
    /// to.
    pub fn new(store: Arc<dyn OutputStore>, endpoint_hint: impl Into<String>) -> Self {
        Self {
            store,
            endpoint_hint: endpoint_hint.into(),
        }
    }

    /// Borrow the inner store (tests / observers).
    pub fn store(&self) -> &Arc<dyn OutputStore> {
        &self.store
    }
}

impl SpawnerLayer for SinkMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(SinkWrapped {
            inner,
            store: self.store.clone(),
            endpoint_hint: self.endpoint_hint.clone(),
        })
    }
}

struct SinkWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    #[allow(dead_code)] // Referenced by the intake path once wired up (carry; scaffold today)
    store: Arc<dyn OutputStore>,
    endpoint_hint: String,
}

#[async_trait]
impl SpawnerAdapter for SinkWrapped {
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
            DATA_SINK_ENDPOINT_KEY.to_string(),
            Value::String(self.endpoint_hint.clone()),
        );
        self.inner
            .spawn(engine, &new_ctx, task_id, attempt, token)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::output::InMemoryOutputStore;

    #[test]
    fn new_layer_holds_store_and_hint() {
        let store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let layer = SinkMiddleware::new(store.clone(), "http://127.0.0.1:7785/v1/data/emit");
        assert_eq!(layer.endpoint_hint, "http://127.0.0.1:7785/v1/data/emit");
        // The stored reference is the same Arc.
        assert!(Arc::ptr_eq(layer.store(), &store));
    }
}
