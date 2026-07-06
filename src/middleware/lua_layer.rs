//! LuaSpawnerLayer — inserts middleware written in Lua into the `SpawnerStack`.
//!
//! Shape:
//!   - `before_src` = a Lua source that evaluates to a function. Called
//!     immediately before the spawn as `function(ctx_table) ... end`. If it
//!     raises, the spawn is rejected with
//!     `SpawnError::RejectedByMiddleware` (a gate).
//!   - `after_src` = a Lua source that evaluates to
//!     `function(ctx_table, result_table) return result' end`, called after
//!     the worker finishes; the return value becomes the new result flowing
//!     downstream (a transform).
//!
//! # Implementation axis
//!
//! `mlua::Lua` is `!Send`, so the initial form built a fresh `Lua::new()`
//! per call and dropped it (per-call VM). Under high-frequency spawn the
//! overhead becomes visible. This version switches to
//! `mlua-isle::AsyncIslePool` (thread-isolated Lua VM + async + pool) and
//! reuses VMs from the pool. The fully-async chain stays the same shape
//! (no `block_on` / `spawn_blocking`); the `!Send` constraint is resolved
//! inside the isle so it rides on the caller's tokio runtime directly.

use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::middleware::SpawnerLayer;
use crate::types::{CapToken, StepId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter, WorkerError};
use crate::worker::{wrap_join, Worker};
use async_trait::async_trait;
use mlua::LuaSerdeExt;
use mlua_isle::{AsyncIslePool, IsleError, PoolConfig, PoolStrategy};
use serde_json::Value;
use std::sync::{Arc, OnceLock};

/// Default pool config (4 warm VMs reused). Callers that want their own
/// pool can swap it in via `LuaMiddleware::with_pool`.
fn default_pool_config() -> PoolConfig {
    PoolConfig {
        max_size: 4,
        strategy: PoolStrategy::Warm,
    }
}

fn build_default_pool() -> Arc<AsyncIslePool> {
    Arc::new(
        AsyncIslePool::new(|_lua| Ok(()), default_pool_config())
            .expect("AsyncIslePool::new (no-op factory) must succeed"),
    )
}

/// `SpawnerLayer` that runs Lua source as a before-gate and/or an
/// after-transform around a spawn, executed on a pooled `AsyncIslePool`
/// VM. See the module doc for the exact function shapes expected of
/// `before_src` / `after_src`.
#[derive(Clone, Default)]
pub struct LuaMiddleware {
    before_src: Option<String>,
    after_src: Option<String>,
    pool: Option<Arc<AsyncIslePool>>,
}

impl LuaMiddleware {
    /// Empty layer — no before/after hooks, default pool.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets the before-hook source (`function(ctx_table) ... end`).
    /// Raising from this function rejects the spawn.
    pub fn before(mut self, src: impl Into<String>) -> Self {
        self.before_src = Some(src.into());
        self
    }
    /// Sets the after-hook source
    /// (`function(ctx_table, result_table) return result end`). Its
    /// return value replaces the worker's result.
    pub fn after(mut self, src: impl Into<String>) -> Self {
        self.after_src = Some(src.into());
        self
    }
    /// Inject an externally-built AsyncIslePool. Useful when the caller
    /// wants to share a pool with another Lua layer or carry VM
    /// initialisation across calls.
    pub fn with_pool(mut self, pool: Arc<AsyncIslePool>) -> Self {
        self.pool = Some(pool);
        self
    }
}

impl SpawnerLayer for LuaMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        // Pool resolution: caller injection wins over the process-wide default (built lazily, once).
        static DEFAULT_POOL: OnceLock<Arc<AsyncIslePool>> = OnceLock::new();
        let pool = self
            .pool
            .clone()
            .unwrap_or_else(|| DEFAULT_POOL.get_or_init(build_default_pool).clone());
        Arc::new(LuaWrapped {
            inner,
            before_src: self.before_src.clone(),
            after_src: self.after_src.clone(),
            pool,
        })
    }
}

struct LuaWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    before_src: Option<String>,
    after_src: Option<String>,
    pool: Arc<AsyncIslePool>,
}

/// Serializable view of `Ctx` handed to Lua functions (`Arc<dyn>` fields are excluded since they cannot serde).
fn ctx_to_serializable(ctx: &Ctx) -> Value {
    serde_json::json!({
        "task_id": ctx.task_id.as_str(),
        "attempt": ctx.attempt,
        "agent": ctx.agent,
        "operator": {
            "kind": format!("{:?}", ctx.operator.kind),
            "id": ctx.operator.id,
        },
    })
}

/// Helper that returns a closure running the "before hook" inside AsyncIsle.exec.
/// Return value is fixed to the literal string "ok" — success / failure is
/// expressed through IsleError.
fn make_before_exec(
    src: String,
    ctx_json: String,
) -> impl FnOnce(&mlua::Lua) -> Result<String, IsleError> + Send + 'static {
    move |lua| {
        let ctx_val: Value =
            serde_json::from_str(&ctx_json).map_err(|e| IsleError::Lua(e.to_string()))?;
        let ctx_lua: mlua::Value = lua
            .to_value(&ctx_val)
            .map_err(|e| IsleError::Lua(e.to_string()))?;
        let f: mlua::Function = lua
            .load(&src)
            .eval()
            .map_err(|e| IsleError::Lua(e.to_string()))?;
        let _: mlua::Value = f.call(ctx_lua).map_err(|e| IsleError::Lua(e.to_string()))?;
        Ok("ok".to_string())
    }
}

/// Returns a closure that runs the "after hook" inside AsyncIsle.exec.
/// Return value is the new result as a JSON string (`{"value": ..., "ok": bool}`).
fn make_after_exec(
    src: String,
    ctx_json: String,
    result_json: String,
) -> impl FnOnce(&mlua::Lua) -> Result<String, IsleError> + Send + 'static {
    move |lua| {
        let ctx_val: Value =
            serde_json::from_str(&ctx_json).map_err(|e| IsleError::Lua(e.to_string()))?;
        let result_val: Value =
            serde_json::from_str(&result_json).map_err(|e| IsleError::Lua(e.to_string()))?;
        let ctx_lua: mlua::Value = lua
            .to_value(&ctx_val)
            .map_err(|e| IsleError::Lua(e.to_string()))?;
        let result_lua: mlua::Value = lua
            .to_value(&result_val)
            .map_err(|e| IsleError::Lua(e.to_string()))?;
        let f: mlua::Function = lua
            .load(&src)
            .eval()
            .map_err(|e| IsleError::Lua(e.to_string()))?;
        let returned: mlua::Value = f
            .call((ctx_lua, result_lua))
            .map_err(|e| IsleError::Lua(e.to_string()))?;
        let new_result: Value = lua
            .from_value(returned)
            .map_err(|e| IsleError::Lua(e.to_string()))?;
        serde_json::to_string(&new_result).map_err(|e| IsleError::Lua(e.to_string()))
    }
}

#[async_trait]
impl SpawnerAdapter for LuaWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        // ─── before hook (= pool checkout → exec → return) ────────────────
        if let Some(src) = &self.before_src {
            let ctx_json = serde_json::to_string(&ctx_to_serializable(ctx))
                .map_err(|e| SpawnError::Internal(format!("ctx serialize: {e}")))?;
            let isle = self
                .pool
                .checkout()
                .await
                .map_err(|e| SpawnError::Internal(format!("isle pool checkout: {e}")))?;
            let f = make_before_exec(src.clone(), ctx_json);
            isle.exec(f)
                .await
                .map_err(|e| SpawnError::RejectedByMiddleware(format!("lua before: {e}")))?;
            // The isle is dropped here — either returned to the pool (Warm) or shut down (Cold).
        }

        let engine_clone = engine.clone();
        let token_clone = token.clone();
        let task_id_clone = task_id.clone();
        let handle = self
            .inner
            .spawn(engine, ctx, task_id, attempt, token)
            .await?;

        // ─── after hook ───────────────────────────────────────────────────
        let Some(after_src) = self.after_src.clone() else {
            return Ok(handle);
        };
        let ctx_val = ctx_to_serializable(ctx);
        Ok(wrap_completion_with_lua_pool(
            handle,
            after_src,
            ctx_val,
            self.pool.clone(),
            engine_clone,
            token_clone,
            task_id_clone,
            attempt,
        ))
    }
}

/// Helper that wraps the completion signal and drives the Lua after-hook
/// through the pool. Follows the signal-only design: pulls the value from
/// `engine.output_tail`, and pushes the post-Lua `{value, ok}` (Lua-wire
/// JSON) as an override Final via `engine.submit_output`.
#[allow(clippy::too_many_arguments)]
fn wrap_completion_with_lua_pool(
    handle: Box<dyn Worker>,
    after_src: String,
    ctx_val: Value,
    pool: Arc<AsyncIslePool>,
    engine: Engine,
    token: crate::types::CapToken,
    task_id: StepId,
    attempt: u32,
) -> Box<dyn Worker> {
    wrap_join(handle, move |signal| async move {
        match signal {
            Ok(()) => {
                let _ = apply_lua_after_pool(
                    &after_src, &ctx_val, &pool, &engine, &token, &task_id, attempt,
                )
                .await;
                Ok(())
            }
            Err(e) => Err(e),
        }
    })
}

async fn apply_lua_after_pool(
    after_src: &str,
    ctx_val: &Value,
    pool: &AsyncIslePool,
    engine: &Engine,
    token: &crate::types::CapToken,
    task_id: &StepId,
    attempt: u32,
) -> Result<(), WorkerError> {
    // Pull the existing Final from the tail (only Inline is fed through Lua; FileRef passes through as-is).
    let tail = engine.output_tail(task_id, attempt).await;
    let (value, ok) = match tail.iter().rev().find_map(|ev| match ev {
        crate::worker::output::OutputEvent::Final {
            content: crate::worker::output::ContentRef::Inline { value },
            ok,
        } => Some((value.clone(), *ok)),
        _ => None,
    }) {
        Some(v) => v,
        None => return Ok(()), // No Inline Final: do nothing.
    };

    let ctx_json = serde_json::to_string(ctx_val)
        .map_err(|e| WorkerError::Failed(format!("ctx serialize: {e}")))?;
    let result_json = serde_json::to_string(&serde_json::json!({"value": value, "ok": ok}))
        .map_err(|e| WorkerError::Failed(format!("result serialize: {e}")))?;

    let isle = pool
        .checkout()
        .await
        .map_err(|e| WorkerError::Failed(format!("isle pool checkout: {e}")))?;
    let f = make_after_exec(after_src.to_string(), ctx_json, result_json);
    let new_json = isle
        .exec(f)
        .await
        .map_err(|e| WorkerError::Failed(format!("lua after: {e}")))?;
    let new_result: Value = serde_json::from_str(&new_json)
        .map_err(|e| WorkerError::Failed(format!("lua after decode: {e}")))?;
    let new_value = new_result.get("value").cloned().unwrap_or(Value::Null);
    let new_ok = new_result.get("ok").and_then(|v| v.as_bool()).unwrap_or(ok);

    // Push the override Final to the engine so the downstream dispatch's rev().find picks up the latest.
    let _ = engine
        .submit_output(
            token,
            task_id,
            attempt,
            crate::worker::output::OutputEvent::Final {
                content: crate::worker::output::ContentRef::Inline { value: new_value },
                ok: new_ok,
            },
        )
        .await;
    Ok(())
}
