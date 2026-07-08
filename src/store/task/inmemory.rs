//! `InMemoryTaskStore` — a process-volatile `TaskStore` used by the current
//! default.

use super::{Inner, SharedInner, TaskId, TaskRecord, TaskRecordStatus, TaskStore, TaskStoreError};
use async_trait::async_trait;
use std::sync::Mutex;

/// Process-volatile [`TaskStore`] used as the current default. Entries are
/// lost on restart; persistent backends (SQLite / Git / mini-app / …) are
/// future carries.
#[derive(Default)]
pub struct InMemoryTaskStore {
    inner: SharedInner,
}

impl InMemoryTaskStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

#[async_trait]
impl TaskStore for InMemoryTaskStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    async fn create(&self, record: TaskRecord) -> Result<(), TaskStoreError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.records.contains_key(&record.id) {
            return Err(TaskStoreError::Duplicate(record.id));
        }
        inner.order.push(record.id.clone());
        inner.records.insert(record.id.clone(), record);
        Ok(())
    }

    async fn get(&self, id: &TaskId) -> Result<TaskRecord, TaskStoreError> {
        let inner = self.inner.lock().unwrap();
        inner
            .records
            .get(id)
            .cloned()
            .ok_or_else(|| TaskStoreError::NotFound(id.clone()))
    }

    async fn list(&self) -> Result<Vec<TaskRecord>, TaskStoreError> {
        let inner = self.inner.lock().unwrap();
        let mut records: Vec<TaskRecord> = inner
            .order
            .iter()
            .filter_map(|id| inner.records.get(id).cloned())
            .collect();
        records.sort_by_key(|r| std::cmp::Reverse(r.created_at));
        Ok(records)
    }

    async fn update_status(
        &self,
        id: &TaskId,
        status: TaskRecordStatus,
    ) -> Result<(), TaskStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| TaskStoreError::NotFound(id.clone()))?;
        record.status = status;
        record.updated_at = crate::types::now_unix();
        Ok(())
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mk(id: &str, created_at: u64) -> TaskRecord {
        TaskRecord {
            id: TaskId::parse(id).unwrap(),
            goal: format!("goal for {id}"),
            blueprint_ref: json!({"id": "bp-1"}),
            input_ctx: json!({"k": "v"}),
            task_input_spec: None,
            status: TaskRecordStatus::Pending,
            created_at,
            updated_at: created_at,
        }
    }

    #[tokio::test]
    async fn create_then_get() {
        let s = InMemoryTaskStore::new();
        s.create(mk("T-1", 100)).await.unwrap();
        let got = s.get(&TaskId::parse("T-1").unwrap()).await.unwrap();
        assert_eq!(got.id, TaskId::parse("T-1").unwrap());
        assert_eq!(got.goal, "goal for T-1");
        assert_eq!(got.status, TaskRecordStatus::Pending);
    }

    #[tokio::test]
    async fn duplicate_create_rejected() {
        let s = InMemoryTaskStore::new();
        s.create(mk("T-1", 100)).await.unwrap();
        let err = s.create(mk("T-1", 200)).await.unwrap_err();
        assert!(matches!(err, TaskStoreError::Duplicate(_)));
    }

    #[tokio::test]
    async fn get_missing_returns_not_found() {
        let s = InMemoryTaskStore::new();
        let err = s.get(&TaskId::parse("T-nope").unwrap()).await.unwrap_err();
        assert!(matches!(err, TaskStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn list_returns_newest_first() {
        let s = InMemoryTaskStore::new();
        s.create(mk("T-1", 100)).await.unwrap();
        s.create(mk("T-2", 300)).await.unwrap();
        s.create(mk("T-3", 200)).await.unwrap();
        let list = s.list().await.unwrap();
        let ids: Vec<_> = list.iter().map(|r| r.id.to_string()).collect();
        assert_eq!(ids, vec!["T-2", "T-3", "T-1"]);
    }

    #[tokio::test]
    async fn update_status_bumps_updated_at_and_persists_status() {
        let s = InMemoryTaskStore::new();
        s.create(mk("T-1", 100)).await.unwrap();
        s.update_status(&TaskId::parse("T-1").unwrap(), TaskRecordStatus::Running)
            .await
            .unwrap();
        let got = s.get(&TaskId::parse("T-1").unwrap()).await.unwrap();
        assert_eq!(got.status, TaskRecordStatus::Running);
        assert!(got.updated_at >= got.created_at);
    }

    #[tokio::test]
    async fn update_status_unknown_fails() {
        let s = InMemoryTaskStore::new();
        let err = s
            .update_status(&TaskId::parse("T-nope").unwrap(), TaskRecordStatus::Done)
            .await
            .unwrap_err();
        assert!(matches!(err, TaskStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn name_is_in_memory() {
        assert_eq!(InMemoryTaskStore::new().name(), "in-memory");
    }

    #[tokio::test]
    async fn task_input_spec_round_trips_through_create_and_get() {
        // Issue #19 ST4: `TaskRecord.task_input_spec` is a plain field on
        // this backend (no encode/decode step), but a regression guard
        // here still catches an accidental drop/clear in `create`/`get`.
        let s = InMemoryTaskStore::new();
        let mut record = mk("T-1", 100);
        record.task_input_spec = Some(json!({"project_root": "/repo"}));
        s.create(record).await.unwrap();
        let got = s.get(&TaskId::parse("T-1").unwrap()).await.unwrap();
        assert_eq!(got.task_input_spec, Some(json!({"project_root": "/repo"})));
    }
}
