//! `InMemoryOperatorSessionStore` — a process-volatile
//! [`OperatorSessionStore`] used as the default when no store path is
//! configured. Byte-for-byte the pre-persistence behaviour: sessions die
//! with the process.

use super::{
    Inner, OperatorSessionRecord, OperatorSessionStore, OperatorSessionStoreError, SessionId,
    SharedInner,
};
use async_trait::async_trait;
use std::sync::Mutex;

/// Process-volatile [`OperatorSessionStore`] default backend.
#[derive(Default)]
pub struct InMemoryOperatorSessionStore {
    inner: SharedInner,
}

impl InMemoryOperatorSessionStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

#[async_trait]
impl OperatorSessionStore for InMemoryOperatorSessionStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    async fn put(&self, record: OperatorSessionRecord) -> Result<(), OperatorSessionStoreError> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.records.contains_key(&record.sid) {
            inner.order.push(record.sid.clone());
        }
        inner.records.insert(record.sid.clone(), record);
        Ok(())
    }

    async fn delete(&self, sid: &SessionId) -> Result<(), OperatorSessionStoreError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.records.remove(sid).is_none() {
            return Err(OperatorSessionStoreError::NotFound(sid.clone()));
        }
        inner.order.retain(|s| s != sid);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<OperatorSessionRecord>, OperatorSessionStoreError> {
        let inner = self.inner.lock().unwrap();
        let mut records: Vec<OperatorSessionRecord> = inner
            .order
            .iter()
            .filter_map(|sid| inner.records.get(sid).cloned())
            .collect();
        records.sort_by_key(|r| r.joined_at_secs);
        Ok(records)
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(sid: &str, joined_at_secs: u64) -> OperatorSessionRecord {
        OperatorSessionRecord {
            sid: SessionId::parse(sid).unwrap(),
            token_digest: OperatorSessionRecord::digest_of(&format!("bearer-{sid}")),
            roles: vec!["main-ai".into()],
            capability_manifest: None,
            joined_at_secs,
        }
    }

    #[tokio::test]
    async fn put_then_list() {
        let s = InMemoryOperatorSessionStore::new();
        s.put(mk("S-1", 100)).await.unwrap();
        s.put(mk("S-2", 50)).await.unwrap();
        let list = s.list().await.unwrap();
        let sids: Vec<_> = list.iter().map(|r| r.sid.to_string()).collect();
        assert_eq!(sids, vec!["S-2", "S-1"], "ascending by joined_at_secs");
    }

    #[tokio::test]
    async fn put_is_upsert() {
        let s = InMemoryOperatorSessionStore::new();
        s.put(mk("S-1", 100)).await.unwrap();
        let mut updated = mk("S-1", 100);
        updated.roles = vec!["other-role".into()];
        s.put(updated).await.unwrap();
        let list = s.list().await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].roles, vec!["other-role".to_string()]);
    }

    #[tokio::test]
    async fn verify_bearer_accepts_only_the_minted_bearer() {
        let record = mk("S-1", 100);
        assert!(record.verify_bearer("bearer-S-1"));
        assert!(!record.verify_bearer("bearer-S-2"));
        assert!(!record.verify_bearer(""));
        // The stored value is the digest, not the bearer.
        assert_ne!(record.token_digest, "bearer-S-1");
        assert_eq!(record.token_digest.len(), 64, "hex SHA-256");
    }

    #[tokio::test]
    async fn delete_removes_and_missing_is_not_found() {
        let s = InMemoryOperatorSessionStore::new();
        s.put(mk("S-1", 100)).await.unwrap();
        s.delete(&SessionId::parse("S-1").unwrap()).await.unwrap();
        assert!(s.list().await.unwrap().is_empty());
        let err = s
            .delete(&SessionId::parse("S-1").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(err, OperatorSessionStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn name_is_in_memory() {
        assert_eq!(InMemoryOperatorSessionStore::new().name(), "in-memory");
    }
}
