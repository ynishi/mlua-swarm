//! `InMemoryRunStore` — a process-volatile `RunStore` used by the current
//! default.

use super::{
    Inner, RunId, RunRecord, RunStatus, RunStore, RunStoreError, SharedInner, StepEntry, TaskId,
};
use async_trait::async_trait;
use std::sync::Mutex;

/// Process-volatile [`RunStore`] used as the current default. Entries are
/// lost on restart; persistent backends (SQLite / Git / mini-app / …) are
/// future carries.
#[derive(Default)]
pub struct InMemoryRunStore {
    inner: SharedInner,
}

impl InMemoryRunStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

#[async_trait]
impl RunStore for InMemoryRunStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    async fn create(&self, record: RunRecord) -> Result<(), RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.records.contains_key(&record.id) {
            return Err(RunStoreError::Duplicate(record.id));
        }
        inner.order.push(record.id.clone());
        inner.records.insert(record.id.clone(), record);
        Ok(())
    }

    async fn get(&self, id: &RunId) -> Result<RunRecord, RunStoreError> {
        let inner = self.inner.lock().unwrap();
        inner
            .records
            .get(id)
            .cloned()
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))
    }

    async fn list_by_task(&self, task_id: &TaskId) -> Result<Vec<RunRecord>, RunStoreError> {
        let inner = self.inner.lock().unwrap();
        let mut records: Vec<RunRecord> = inner
            .order
            .iter()
            .filter_map(|id| inner.records.get(id).cloned())
            .filter(|r| &r.task_id == task_id)
            .collect();
        records.sort_by_key(|r| r.created_at);
        Ok(records)
    }

    async fn append_step_entry(&self, id: &RunId, entry: StepEntry) -> Result<(), RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))?;
        record.step_entries.push(entry);
        record.updated_at = crate::types::now_unix();
        Ok(())
    }

    async fn update_status(&self, id: &RunId, status: RunStatus) -> Result<(), RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))?;
        record.status = status;
        record.updated_at = crate::types::now_unix();
        Ok(())
    }

    async fn set_result(
        &self,
        id: &RunId,
        result_ref: serde_json::Value,
    ) -> Result<(), RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))?;
        record.result_ref = Some(result_ref);
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

    fn mk(id: &str, task_id: &str, created_at: u64) -> RunRecord {
        RunRecord {
            id: RunId::parse(id).unwrap(),
            task_id: TaskId::parse(task_id).unwrap(),
            status: RunStatus::Pending,
            step_entries: vec![],
            operator_sid: None,
            result_ref: None,
            created_at,
            updated_at: created_at,
        }
    }

    #[tokio::test]
    async fn create_then_get() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.task_id, TaskId::parse("T-1").unwrap());
        assert_eq!(got.status, RunStatus::Pending);
        assert!(got.step_entries.is_empty());
    }

    #[tokio::test]
    async fn duplicate_create_rejected() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let err = s.create(mk("R-1", "T-1", 200)).await.unwrap_err();
        assert!(matches!(err, RunStoreError::Duplicate(_)));
    }

    #[tokio::test]
    async fn get_missing_returns_not_found() {
        let s = InMemoryRunStore::new();
        let err = s.get(&RunId::parse("R-nope").unwrap()).await.unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn list_by_task_filters_and_orders_ascending() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 300)).await.unwrap();
        s.create(mk("R-2", "T-2", 50)).await.unwrap();
        s.create(mk("R-3", "T-1", 100)).await.unwrap();
        let list = s
            .list_by_task(&TaskId::parse("T-1").unwrap())
            .await
            .unwrap();
        let ids: Vec<_> = list.iter().map(|r| r.id.to_string()).collect();
        assert_eq!(ids, vec!["R-3", "R-1"]);
    }

    #[tokio::test]
    async fn append_step_entry_accumulates_in_order() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.append_step_entry(
            &RunId::parse("R-1").unwrap(),
            StepEntry {
                step_id: crate::types::StepId::parse("ST-1").unwrap(),
                step_ref: Some("step-a".into()),
                status: Some("dispatched".into()),
                at: 101,
            },
        )
        .await
        .unwrap();
        s.append_step_entry(
            &RunId::parse("R-1").unwrap(),
            StepEntry {
                step_id: crate::types::StepId::parse("ST-2").unwrap(),
                step_ref: Some("step-b".into()),
                status: Some("passed".into()),
                at: 102,
            },
        )
        .await
        .unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.step_entries.len(), 2);
        assert_eq!(got.step_entries[0].step_ref, Some("step-a".into()));
        assert_eq!(got.step_entries[1].step_ref, Some("step-b".into()));
        assert!(got.updated_at >= got.created_at);
    }

    #[tokio::test]
    async fn append_step_entry_unknown_run_fails() {
        let s = InMemoryRunStore::new();
        let err = s
            .append_step_entry(
                &RunId::parse("R-nope").unwrap(),
                StepEntry {
                    step_id: crate::types::StepId::parse("ST-1").unwrap(),
                    step_ref: None,
                    status: None,
                    at: 1,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn update_status_persists() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.update_status(&RunId::parse("R-1").unwrap(), RunStatus::Running)
            .await
            .unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.status, RunStatus::Running);
    }

    #[tokio::test]
    async fn set_result_persists() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.set_result(&RunId::parse("R-1").unwrap(), json!({"ok": true}))
            .await
            .unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.result_ref, Some(json!({"ok": true})));
    }

    #[tokio::test]
    async fn name_is_in_memory() {
        assert_eq!(InMemoryRunStore::new().name(), "in-memory");
    }
}
