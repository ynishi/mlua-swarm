//! `EnhanceSettingStore` — a key-value store for `EnhanceSetting`.
//!
//! v0.10.0 replaced the old versioned `EnhanceConfigStore` (with
//! `read_head` / `write_new` / `history`) with a plain CRUD shape.
//! `EnhanceSetting` no longer carries a version of its own — Blueprint
//! version management runs on a separate path that commits the embedded
//! `EnhanceSetting.blueprint` to `BlueprintStore` (carry).
//!
//! Only an in-memory implementation ships today; a Git2 backend is a
//! carry for a future turn.

use crate::enhance::setting::EnhanceSetting;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

/// Identifier — `the server` is expected to use `"default"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EnhanceSettingId(pub String);

impl EnhanceSettingId {
    /// Wrap an arbitrary string as an id.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// The id used by the server's single default setting: `"default"`.
    pub fn default_id() -> Self {
        Self("default".into())
    }

    /// Borrow the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EnhanceSettingId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Errors surfaced by an [`EnhanceSettingStore`] implementation.
#[derive(Debug, Error)]
pub enum EnhanceSettingStoreError {
    /// No setting exists for the given id.
    #[error("not found: {0}")]
    NotFound(EnhanceSettingId),
}

/// CRUD persistence interface for [`EnhanceSetting`].
#[async_trait]
pub trait EnhanceSettingStore: Send + Sync {
    /// Backend name — for diagnostics/logging.
    fn name(&self) -> &str;

    /// Fetch a setting by id.
    async fn get(&self, id: &EnhanceSettingId) -> Result<EnhanceSetting, EnhanceSettingStoreError>;

    /// Insert or overwrite the setting for `id`.
    async fn put(
        &self,
        id: &EnhanceSettingId,
        setting: EnhanceSetting,
    ) -> Result<(), EnhanceSettingStoreError>;

    /// Remove the setting for `id`. Returns `NotFound` if absent.
    async fn delete(&self, id: &EnhanceSettingId) -> Result<(), EnhanceSettingStoreError>;

    /// List every stored setting id.
    async fn list(&self) -> Result<Vec<EnhanceSettingId>, EnhanceSettingStoreError>;
}

/// Process-volatile [`EnhanceSettingStore`] backed by a `HashMap`. The
/// only backend that ships today; a Git2 backend is a future carry.
#[derive(Default)]
pub struct InMemoryEnhanceSettingStore {
    inner: Mutex<HashMap<EnhanceSettingId, EnhanceSetting>>,
}

impl InMemoryEnhanceSettingStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl EnhanceSettingStore for InMemoryEnhanceSettingStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    async fn get(&self, id: &EnhanceSettingId) -> Result<EnhanceSetting, EnhanceSettingStoreError> {
        self.inner
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| EnhanceSettingStoreError::NotFound(id.clone()))
    }

    async fn put(
        &self,
        id: &EnhanceSettingId,
        setting: EnhanceSetting,
    ) -> Result<(), EnhanceSettingStoreError> {
        self.inner.lock().unwrap().insert(id.clone(), setting);
        Ok(())
    }

    async fn delete(&self, id: &EnhanceSettingId) -> Result<(), EnhanceSettingStoreError> {
        if self.inner.lock().unwrap().remove(id).is_none() {
            return Err(EnhanceSettingStoreError::NotFound(id.clone()));
        }
        Ok(())
    }

    async fn list(&self) -> Result<Vec<EnhanceSettingId>, EnhanceSettingStoreError> {
        Ok(self.inner.lock().unwrap().keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::VersionSelector;
    use crate::blueprint::store::BlueprintId;
    use crate::enhance::setting::EnhanceSettingMeta;

    fn dummy_setting(id: &str, bp: &str) -> EnhanceSetting {
        EnhanceSetting {
            id: id.into(),
            blueprint_id: BlueprintId::new(bp.to_string()),
            ttl_secs: 10,
            version: VersionSelector::default(),
            verifier_axes: vec!["des".into()],
            meta: EnhanceSettingMeta::default(),
        }
    }

    #[test]
    fn enhance_setting_id_default_is_default_literal() {
        assert_eq!(EnhanceSettingId::default_id().as_str(), "default");
    }

    #[test]
    fn enhance_setting_id_display_is_inner_string() {
        let id = EnhanceSettingId::new("foo");
        assert_eq!(format!("{id}"), "foo");
    }

    #[tokio::test]
    async fn inmemory_put_then_get_returns_same_setting() {
        let store = InMemoryEnhanceSettingStore::new();
        let id = EnhanceSettingId::new("s1");
        let s = dummy_setting("s1", "bp-1");
        store.put(&id, s.clone()).await.unwrap();
        let got = store.get(&id).await.unwrap();
        assert_eq!(got.id, "s1");
        assert_eq!(got.blueprint_id.as_str(), "bp-1");
    }

    #[tokio::test]
    async fn inmemory_get_missing_returns_not_found() {
        let store = InMemoryEnhanceSettingStore::new();
        let err = store.get(&EnhanceSettingId::new("nope")).await.unwrap_err();
        assert!(matches!(err, EnhanceSettingStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn inmemory_delete_missing_returns_not_found() {
        let store = InMemoryEnhanceSettingStore::new();
        let err = store
            .delete(&EnhanceSettingId::new("nope"))
            .await
            .unwrap_err();
        assert!(matches!(err, EnhanceSettingStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn inmemory_put_then_delete_then_get_is_not_found() {
        let store = InMemoryEnhanceSettingStore::new();
        let id = EnhanceSettingId::new("s2");
        store.put(&id, dummy_setting("s2", "bp-x")).await.unwrap();
        store.delete(&id).await.unwrap();
        assert!(matches!(
            store.get(&id).await.unwrap_err(),
            EnhanceSettingStoreError::NotFound(_)
        ));
    }

    #[tokio::test]
    async fn inmemory_list_returns_all_inserted_ids() {
        let store = InMemoryEnhanceSettingStore::new();
        store
            .put(&EnhanceSettingId::new("a"), dummy_setting("a", "bp-a"))
            .await
            .unwrap();
        store
            .put(&EnhanceSettingId::new("b"), dummy_setting("b", "bp-b"))
            .await
            .unwrap();
        let mut ids: Vec<String> = store
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|i| i.0)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn inmemory_put_overwrites_existing_setting() {
        let store = InMemoryEnhanceSettingStore::new();
        let id = EnhanceSettingId::new("s3");
        store.put(&id, dummy_setting("s3", "bp-old")).await.unwrap();
        store.put(&id, dummy_setting("s3", "bp-new")).await.unwrap();
        let got = store.get(&id).await.unwrap();
        assert_eq!(got.blueprint_id.as_str(), "bp-new");
    }

    #[tokio::test]
    async fn inmemory_name_is_in_memory() {
        let store = InMemoryEnhanceSettingStore::new();
        assert_eq!(store.name(), "in-memory");
    }
}
