//! `InputInjectMiddleware` — the `SpawnerLayer` for the Data plane's multi-in
//! prompt injection.
//!
//! # Role
//!
//! A `SpawnerLayer` that, just before spawn, drops the list of already-
//! registered [`OutputRef`]s into `Ctx.meta.runtime`. Downstream Operator /
//! Spawner code (for example `mlua-swarm-server`'s `Operator::execute`) looks this
//! key up and splices a line into the SubAgent's Spawn directive prompt
//! along the lines of "`$IN_REFS = [out_id_1, out_id_2, ...]`, fetch these
//! from the Store".
//!
//! MainAI only carries `OutputRef`s (small ids); the big bodies stay with the
//! store owner. That keeps MainAI context tight even when ten SubAgents each
//! stack up four-kilotoken bodies — MainAI only needs to hold the id list.
//!
//! This layer stays out of the Domain path (the verdict flow). See the
//! [`crate::store::output`] module doc for the canonical narrative.
//!
//! # Pattern
//!
//! Same shape as `AgentResolver`, `ProjectNameAliasMiddleware`, and
//! `SinkMiddleware`: edit `ctx`, call the inner spawner, done. Engine state
//! is not touched.
//!
//! # Implementation status
//!
//! - **Current (scaffold):** `SpawnerLayer` trait impl plus injection of the
//!   `IN_REFS` list into `Ctx.meta.runtime`. Turning that into a real prompt
//!   line (literal expansion inside the Operator's directive) is done on the
//!   Operator side and is still a carry.
//! - **Carry:** literal expansion on the Operator-directive side; a fetch path
//!   from the store for actual bodies (today we only inject the id refs — the
//!   SubAgent tool is responsible for pulling bodies down); end-to-end
//!   wire-through.

use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::middleware::SpawnerLayer;
use crate::store::output::OutputRef;
use crate::types::{CapToken, TaskId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter};
use crate::worker::Worker;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Key under `ctx.meta.runtime` that carries the `IN_REFS` list.
///
/// Downstream Operator / Spawner code is expected to look this key up and
/// splice a literal line into the SubAgent's Spawn directive prompt body
/// telling it to fetch `$IN_REFS = [<out_id>, ...]` from the store.
pub const INPUT_REFS_KEY: &str = "input_refs";

/// Multi-in prompt injection `SpawnerLayer`. Config: the list of
/// `OutputRef`s to inject into the next spawn.
///
/// Per-spawn lists are built in the Blueprint (γ scope) or the Application
/// layer, and are frozen at the moment this layer is placed in the stack.
/// If you need to rewrite them dynamically mid-flight, do it in a different
/// middleware or resolve on the Blueprint side.
pub struct InputInjectMiddleware {
    refs: Vec<OutputRef>,
}

impl InputInjectMiddleware {
    /// Build a new layer. `refs` is the `OutputRef` list to inject into the
    /// spawn; an empty list is fine (the initial agent).
    pub fn new(refs: Vec<OutputRef>) -> Self {
        Self { refs }
    }

    /// Borrow the inner refs list (tests / observers).
    pub fn refs(&self) -> &[OutputRef] {
        &self.refs
    }
}

impl SpawnerLayer for InputInjectMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(InputInjectWrapped {
            inner,
            refs: self.refs.clone(),
        })
    }
}

struct InputInjectWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    refs: Vec<OutputRef>,
}

#[async_trait]
impl SpawnerAdapter for InputInjectWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: TaskId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let mut new_ctx = ctx.clone();
        let refs_json: Vec<Value> = self
            .refs
            .iter()
            .map(|r| Value::String(r.0.clone()))
            .collect();
        new_ctx
            .meta
            .runtime
            .insert(INPUT_REFS_KEY.to_string(), Value::Array(refs_json));
        self.inner
            .spawn(engine, &new_ctx, task_id, attempt, token)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_layer_holds_refs() {
        let r1 = OutputRef::new();
        let r2 = OutputRef::new();
        let layer = InputInjectMiddleware::new(vec![r1.clone(), r2.clone()]);
        assert_eq!(layer.refs(), &[r1, r2]);
    }

    #[test]
    fn empty_refs_are_valid() {
        let layer = InputInjectMiddleware::new(vec![]);
        assert!(layer.refs().is_empty());
    }
}
