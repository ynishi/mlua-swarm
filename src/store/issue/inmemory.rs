//! `InMemoryIssueStore` — a process-volatile `IssueStore` used by the current
//! default.

use super::{Inner, IssueId, IssuePayload, IssueStatus, IssueStore, IssueStoreError, SharedInner};
use async_trait::async_trait;
use std::sync::Mutex;

/// Process-volatile [`IssueStore`] used as the current default. Entries
/// are lost on restart; persistent backends (SQLite / Git / mini-app /
/// …) are future carries.
#[derive(Default)]
pub struct InMemoryIssueStore {
    inner: SharedInner,
}

impl InMemoryIssueStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

#[async_trait]
impl IssueStore for InMemoryIssueStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    async fn create(&self, payload: IssuePayload) -> Result<(), IssueStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let id = payload.issue_id.clone();
        if inner.payloads.contains_key(&id) {
            return Err(IssueStoreError::Duplicate(id));
        }
        inner.order.push(id.clone());
        inner.statuses.insert(id.clone(), IssueStatus::Pending);
        inner.pending.push_back(id.clone());
        inner.payloads.insert(id, payload);
        Ok(())
    }

    async fn get(&self, id: &IssueId) -> Result<IssuePayload, IssueStoreError> {
        let inner = self.inner.lock().unwrap();
        inner
            .payloads
            .get(id)
            .cloned()
            .ok_or_else(|| IssueStoreError::NotFound(id.clone()))
    }

    async fn status(&self, id: &IssueId) -> Result<IssueStatus, IssueStoreError> {
        let inner = self.inner.lock().unwrap();
        inner
            .statuses
            .get(id)
            .cloned()
            .ok_or_else(|| IssueStoreError::NotFound(id.clone()))
    }

    async fn list(&self) -> Result<Vec<(IssueId, IssueStatus)>, IssueStoreError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .order
            .iter()
            .map(|id| {
                let st = inner
                    .statuses
                    .get(id)
                    .cloned()
                    .unwrap_or(IssueStatus::Pending);
                (id.clone(), st)
            })
            .collect())
    }

    async fn pop_pending(&self) -> Result<Option<IssuePayload>, IssueStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let Some(id) = inner.pending.pop_front() else {
            return Ok(None);
        };
        let payload = inner
            .payloads
            .get(&id)
            .cloned()
            .ok_or_else(|| IssueStoreError::NotFound(id.clone()))?;
        inner.statuses.insert(id, IssueStatus::InFlight);
        Ok(Some(payload))
    }

    async fn update_status(
        &self,
        id: &IssueId,
        status: IssueStatus,
    ) -> Result<(), IssueStoreError> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.payloads.contains_key(id) {
            return Err(IssueStoreError::NotFound(id.clone()));
        }
        inner.statuses.insert(id.clone(), status);
        Ok(())
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blueprint::store::BlueprintId;

    fn mk(id: &str) -> IssuePayload {
        IssuePayload {
            issue_id: IssueId::new(id),
            blueprint_id: BlueprintId::new("main"),
            intent: format!("intent for {id}"),
        }
    }

    #[tokio::test]
    async fn create_then_get() {
        let s = InMemoryIssueStore::new();
        s.create(mk("i1")).await.unwrap();
        let got = s.get(&IssueId::new("i1")).await.unwrap();
        assert_eq!(got.issue_id, IssueId::new("i1"));
        assert_eq!(
            s.status(&IssueId::new("i1")).await.unwrap(),
            IssueStatus::Pending
        );
    }

    #[tokio::test]
    async fn duplicate_create_rejected() {
        let s = InMemoryIssueStore::new();
        s.create(mk("i1")).await.unwrap();
        let err = s.create(mk("i1")).await.unwrap_err();
        assert!(matches!(err, IssueStoreError::Duplicate(_)));
    }

    #[tokio::test]
    async fn pop_pending_fifo_and_transitions_inflight() {
        let s = InMemoryIssueStore::new();
        s.create(mk("a")).await.unwrap();
        s.create(mk("b")).await.unwrap();

        let p1 = s.pop_pending().await.unwrap().unwrap();
        assert_eq!(p1.issue_id, IssueId::new("a"));
        assert_eq!(
            s.status(&IssueId::new("a")).await.unwrap(),
            IssueStatus::InFlight
        );

        let p2 = s.pop_pending().await.unwrap().unwrap();
        assert_eq!(p2.issue_id, IssueId::new("b"));

        assert!(s.pop_pending().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn update_status_to_applied() {
        let s = InMemoryIssueStore::new();
        s.create(mk("x")).await.unwrap();
        s.pop_pending().await.unwrap();
        s.update_status(
            &IssueId::new("x"),
            IssueStatus::Applied {
                new_version: "abc123".into(),
            },
        )
        .await
        .unwrap();
        match s.status(&IssueId::new("x")).await.unwrap() {
            IssueStatus::Applied { new_version } => assert_eq!(new_version, "abc123"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_returns_insertion_order() {
        let s = InMemoryIssueStore::new();
        s.create(mk("a")).await.unwrap();
        s.create(mk("b")).await.unwrap();
        s.create(mk("c")).await.unwrap();
        let v = s.list().await.unwrap();
        let ids: Vec<_> = v.iter().map(|(i, _)| i.0.clone()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn update_status_unknown_fails() {
        let s = InMemoryIssueStore::new();
        let err = s
            .update_status(&IssueId::new("nope"), IssueStatus::Pending)
            .await
            .unwrap_err();
        assert!(matches!(err, IssueStoreError::NotFound(_)));
    }
}
