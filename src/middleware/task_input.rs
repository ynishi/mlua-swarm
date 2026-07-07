//! `TaskInputMiddleware` — a `SpawnerLayer` that propagates task-level
//! execution context from `POST /v1/tasks`'s `init_ctx` body into
//! `Ctx.meta.runtime`.
//!
//! `init_ctx` (the flow.ir initial `ctx` value) may optionally carry three
//! top-level fields — `project_root`, `work_dir`, `task_metadata` —
//! alongside whatever free-form JSON callers already put there (backward
//! compat: existing `init_ctx` shapes are untouched, these are additive
//! top-level keys). [`TaskInputMiddleware::from_init_ctx`] extracts them
//! once at launch time; [`crate::service::task_launch::TaskLaunchService::launch`]
//! layers this middleware on the stack only when at least one field is
//! present, mirroring the [`crate::middleware::project_name_alias
//! ::ProjectNameAliasMiddleware`] / [`crate::middleware::worker_binding
//! ::WorkerBindingMiddleware`] conditional-layering convention.
//!
//! Downstream Operator / Spawner code (for example `mlua-swarm-server`'s
//! `Operator::execute`) reads the injected keys back via
//! `ctx.meta.runtime.get(...)` the same way it reads `worker_binding` /
//! `project_name_alias` — splicing them into the SubAgent's Spawn
//! directive prompt is a downstream concern, out of scope here.
//!
//! # Pattern
//!
//! Same shape as `ProjectNameAliasMiddleware`: a task-wide (not per-agent)
//! value set once at launch time and inserted into every spawn's `ctx`,
//! unconditionally (no `ctx.agent` lookup — unlike `WorkerBindingMiddleware`,
//! which is keyed by agent name).

use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::middleware::SpawnerLayer;
use crate::types::{CapToken, StepId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter};
use crate::worker::Worker;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Key under `ctx.meta.runtime` that carries the project root path.
pub const TASK_PROJECT_ROOT_KEY: &str = "project_root";

/// Key under `ctx.meta.runtime` that carries the work dir path.
pub const TASK_WORK_DIR_KEY: &str = "work_dir";

/// Key under `ctx.meta.runtime` that carries the free-form task metadata
/// object.
pub const TASK_METADATA_KEY: &str = "task_metadata";

/// `SpawnerLayer` that drops task-level execution context (`project_root` /
/// `work_dir` / `task_metadata`) into `ctx` just before spawn.
///
/// Each field is independent: any subset may be `Some` (see
/// [`Self::from_init_ctx`] for how they are extracted from `init_ctx`).
/// Absent fields insert no key at all — no empty-string / `Value::Null`
/// placeholder — so downstream `.get(...)` misses cleanly instead of
/// observing a hollow value.
pub struct TaskInputMiddleware {
    project_root: Option<String>,
    work_dir: Option<String>,
    task_metadata: Option<Value>,
}

impl TaskInputMiddleware {
    /// Builds a layer from already-resolved field values. Prefer
    /// [`Self::from_init_ctx`] when the source is a raw `init_ctx` body.
    pub fn new(
        project_root: Option<String>,
        work_dir: Option<String>,
        task_metadata: Option<Value>,
    ) -> Self {
        Self {
            project_root,
            work_dir,
            task_metadata,
        }
    }

    /// Extracts `project_root` (string) / `work_dir` (string) /
    /// `task_metadata` (object) from a top-level `init_ctx` object, each
    /// independently optional.
    ///
    /// Returns `None` when `init_ctx` is not a JSON object, or is an object
    /// with none of the three keys present in the expected shape (a
    /// present-but-wrong-typed value, e.g. `"work_dir": 42`, is treated the
    /// same as absent — this is a best-effort task-level convenience
    /// injection, not a request-body validator; malformed request bodies
    /// are the request layer's concern). Callers only layer the returned
    /// middleware onto the spawner stack when this is `Some`, keeping the
    /// no-op path identical to today's behavior.
    pub fn from_init_ctx(init_ctx: &Value) -> Option<Self> {
        let obj = init_ctx.as_object()?;
        let project_root = obj
            .get(TASK_PROJECT_ROOT_KEY)
            .and_then(Value::as_str)
            .map(str::to_string);
        let work_dir = obj
            .get(TASK_WORK_DIR_KEY)
            .and_then(Value::as_str)
            .map(str::to_string);
        let task_metadata = obj
            .get(TASK_METADATA_KEY)
            .filter(|v| v.is_object())
            .cloned();
        if project_root.is_none() && work_dir.is_none() && task_metadata.is_none() {
            return None;
        }
        Some(Self::new(project_root, work_dir, task_metadata))
    }
}

impl SpawnerLayer for TaskInputMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(TaskInputWrapped {
            inner,
            project_root: self.project_root.clone(),
            work_dir: self.work_dir.clone(),
            task_metadata: self.task_metadata.clone(),
        })
    }
}

struct TaskInputWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    project_root: Option<String>,
    work_dir: Option<String>,
    task_metadata: Option<Value>,
}

#[async_trait]
impl SpawnerAdapter for TaskInputWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let mut new_ctx = ctx.clone();
        if let Some(project_root) = &self.project_root {
            new_ctx.meta.runtime.insert(
                TASK_PROJECT_ROOT_KEY.to_string(),
                Value::String(project_root.clone()),
            );
        }
        if let Some(work_dir) = &self.work_dir {
            new_ctx.meta.runtime.insert(
                TASK_WORK_DIR_KEY.to_string(),
                Value::String(work_dir.clone()),
            );
        }
        if let Some(task_metadata) = &self.task_metadata {
            new_ctx
                .meta
                .runtime
                .insert(TASK_METADATA_KEY.to_string(), task_metadata.clone());
        }
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
    use serde_json::json;
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
        layer: TaskInputMiddleware,
    ) -> (Arc<dyn SpawnerAdapter>, Arc<Mutex<Option<Ctx>>>) {
        let seen = Arc::new(Mutex::new(None));
        let inner = Arc::new(CtxProbe { seen: seen.clone() });
        let wrapped = layer.wrap(inner);
        (wrapped, seen)
    }

    #[tokio::test]
    async fn injects_all_three_fields_into_ctx_meta_runtime() {
        let layer = TaskInputMiddleware::new(
            Some("/repo".to_string()),
            Some("/repo/work".to_string()),
            Some(json!({ "issue": 17 })),
        );
        let (stack, seen) = probe_stack(layer);
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-1").unwrap();
        let ctx = Ctx::new(task_id.clone(), 1, "planner");
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        assert_eq!(
            observed.meta.runtime.get(TASK_PROJECT_ROOT_KEY),
            Some(&Value::String("/repo".to_string()))
        );
        assert_eq!(
            observed.meta.runtime.get(TASK_WORK_DIR_KEY),
            Some(&Value::String("/repo/work".to_string()))
        );
        assert_eq!(
            observed.meta.runtime.get(TASK_METADATA_KEY),
            Some(&json!({ "issue": 17 }))
        );
    }

    #[tokio::test]
    async fn partial_fields_only_insert_present_keys() {
        let layer = TaskInputMiddleware::new(Some("/repo".to_string()), None, None);
        let (stack, seen) = probe_stack(layer);
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-2").unwrap();
        let ctx = Ctx::new(task_id.clone(), 1, "planner");
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        assert_eq!(
            observed.meta.runtime.get(TASK_PROJECT_ROOT_KEY),
            Some(&Value::String("/repo".to_string()))
        );
        assert!(
            !observed.meta.runtime.contains_key(TASK_WORK_DIR_KEY),
            "work_dir absent must not insert a key"
        );
        assert!(
            !observed.meta.runtime.contains_key(TASK_METADATA_KEY),
            "task_metadata absent must not insert a key"
        );
    }

    #[tokio::test]
    async fn empty_fields_layer_is_a_no_op() {
        let layer = TaskInputMiddleware::new(None, None, None);
        let (stack, seen) = probe_stack(layer);
        let engine = Engine::new(EngineCfg::default());
        let task_id = StepId::parse("ST-3").unwrap();
        let ctx = Ctx::new(task_id.clone(), 1, "planner");
        let token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let _ = stack.spawn(&engine, &ctx, task_id, 1, token).await;

        let observed = seen.lock().unwrap().clone().expect("inner ctx captured");
        assert!(observed.meta.runtime.is_empty());
    }

    #[test]
    fn from_init_ctx_extracts_all_three_fields() {
        let init_ctx = json!({
            "project_root": "/repo",
            "work_dir": "/repo/work",
            "task_metadata": { "issue": 17 },
            "other": "kept-as-is-by-flow-eval-not-this-layer",
        });
        let layer = TaskInputMiddleware::from_init_ctx(&init_ctx).expect("some fields present");
        assert_eq!(layer.project_root.as_deref(), Some("/repo"));
        assert_eq!(layer.work_dir.as_deref(), Some("/repo/work"));
        assert_eq!(layer.task_metadata, Some(json!({ "issue": 17 })));
    }

    #[test]
    fn from_init_ctx_partial_fields_present() {
        let init_ctx = json!({ "project_root": "/repo" });
        let layer = TaskInputMiddleware::from_init_ctx(&init_ctx).expect("one field present");
        assert_eq!(layer.project_root.as_deref(), Some("/repo"));
        assert!(layer.work_dir.is_none());
        assert!(layer.task_metadata.is_none());
    }

    #[test]
    fn from_init_ctx_no_recognized_fields_returns_none() {
        let init_ctx = json!({ "input": "hi" });
        assert!(TaskInputMiddleware::from_init_ctx(&init_ctx).is_none());
    }

    #[test]
    fn from_init_ctx_non_object_init_ctx_returns_none() {
        assert!(TaskInputMiddleware::from_init_ctx(&json!("just a string")).is_none());
        assert!(TaskInputMiddleware::from_init_ctx(&json!(null)).is_none());
    }

    #[test]
    fn from_init_ctx_wrong_typed_field_is_treated_as_absent() {
        // work_dir as a number is not the documented `Object` /
        // `String` shape — treated the same as absent rather than
        // erroring (best-effort convenience injection, not a validator).
        let init_ctx = json!({ "work_dir": 42, "task_metadata": "not-an-object" });
        assert!(TaskInputMiddleware::from_init_ctx(&init_ctx).is_none());
    }
}
