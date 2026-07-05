//! `SqliteEnhanceSettingStore` — SQLite-backed [`EnhanceSettingStore`].
//!
//! Body shape is captured as a single JSON blob per row (the setting is
//! already `Serialize + Deserialize`), so schema evolution of
//! `EnhanceSetting` does not require a migration on this table.

use super::{EnhanceSetting, EnhanceSettingId, EnhanceSettingStore, EnhanceSettingStoreError};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::path::Path;

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS enhance_settings (\
  id       TEXT PRIMARY KEY, \
  body_json TEXT NOT NULL\
);\
";

/// SQLite-backed [`EnhanceSettingStore`].
pub struct SqliteEnhanceSettingStore {
    isle: AsyncIsle,
}

impl SqliteEnhanceSettingStore {
    /// Open (or create) a SQLite file and apply the schema.
    pub async fn open(
        path: impl AsRef<Path>,
    ) -> Result<(Self, AsyncIsleDriver), EnhanceSettingStoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), |conn| {
            conn.execute_batch(SCHEMA_SQL)
        })
        .await
        .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), EnhanceSettingStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(|conn| conn.execute_batch(SCHEMA_SQL))
            .await
            .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }
}

fn map_isle_err(e: IsleError) -> EnhanceSettingStoreError {
    EnhanceSettingStoreError::Other(format!("sqlite: {e}"))
}

#[async_trait]
impl EnhanceSettingStore for SqliteEnhanceSettingStore {
    fn name(&self) -> &str {
        "sqlite"
    }

    async fn get(&self, id: &EnhanceSettingId) -> Result<EnhanceSetting, EnhanceSettingStoreError> {
        let id_str = id.0.clone();
        let id_for_notfound = id.clone();
        let row = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT body_json FROM enhance_settings WHERE id = ?1",
                    params![id_str],
                    |row| row.get::<_, String>(0),
                )
                .optional()
            })
            .await
            .map_err(map_isle_err)?;
        match row {
            Some(json_text) => serde_json::from_str::<EnhanceSetting>(&json_text)
                .map_err(|e| EnhanceSettingStoreError::Other(format!("decode: {e}"))),
            None => Err(EnhanceSettingStoreError::NotFound(id_for_notfound)),
        }
    }

    async fn put(
        &self,
        id: &EnhanceSettingId,
        setting: EnhanceSetting,
    ) -> Result<(), EnhanceSettingStoreError> {
        let id_str = id.0.clone();
        let json_text = serde_json::to_string(&setting)
            .map_err(|e| EnhanceSettingStoreError::Other(format!("encode: {e}")))?;
        self.isle
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO enhance_settings (id, body_json) VALUES (?1, ?2) \
                     ON CONFLICT(id) DO UPDATE SET body_json = excluded.body_json",
                    params![id_str, json_text],
                )
                .map(|_| ())
            })
            .await
            .map_err(map_isle_err)
    }

    async fn delete(&self, id: &EnhanceSettingId) -> Result<(), EnhanceSettingStoreError> {
        let id_str = id.0.clone();
        let id_for_notfound = id.clone();
        let n = self
            .isle
            .call(move |conn| {
                conn.execute(
                    "DELETE FROM enhance_settings WHERE id = ?1",
                    params![id_str],
                )
            })
            .await
            .map_err(map_isle_err)?;
        if n == 0 {
            Err(EnhanceSettingStoreError::NotFound(id_for_notfound))
        } else {
            Ok(())
        }
    }

    async fn list(&self) -> Result<Vec<EnhanceSettingId>, EnhanceSettingStoreError> {
        let rows = self
            .isle
            .call(|conn| {
                let mut stmt = conn.prepare("SELECT id FROM enhance_settings ORDER BY id ASC")?;
                let iter = stmt.query_map([], |row| row.get::<_, String>(0))?;
                let mut out = Vec::new();
                for r in iter {
                    out.push(r?);
                }
                Ok(out)
            })
            .await
            .map_err(map_isle_err)?;
        Ok(rows.into_iter().map(EnhanceSettingId::new).collect())
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

    #[tokio::test]
    async fn put_then_get_returns_same_setting() {
        let (s, driver) = SqliteEnhanceSettingStore::open_in_memory().await.unwrap();
        let id = EnhanceSettingId::new("s1");
        s.put(&id, dummy_setting("s1", "bp-1")).await.unwrap();
        let got = s.get(&id).await.unwrap();
        assert_eq!(got.id, "s1");
        assert_eq!(got.blueprint_id.as_str(), "bp-1");
        assert_eq!(got.ttl_secs, 10);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn put_overwrites_existing() {
        let (s, driver) = SqliteEnhanceSettingStore::open_in_memory().await.unwrap();
        let id = EnhanceSettingId::new("s1");
        s.put(&id, dummy_setting("s1", "bp-1")).await.unwrap();
        let mut updated = dummy_setting("s1", "bp-2");
        updated.ttl_secs = 99;
        s.put(&id, updated).await.unwrap();
        let got = s.get(&id).await.unwrap();
        assert_eq!(got.blueprint_id.as_str(), "bp-2");
        assert_eq!(got.ttl_secs, 99);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn get_missing_returns_not_found() {
        let (s, driver) = SqliteEnhanceSettingStore::open_in_memory().await.unwrap();
        let err = s.get(&EnhanceSettingId::new("nope")).await.unwrap_err();
        assert!(matches!(err, EnhanceSettingStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn delete_missing_returns_not_found() {
        let (s, driver) = SqliteEnhanceSettingStore::open_in_memory().await.unwrap();
        let err = s.delete(&EnhanceSettingId::new("nope")).await.unwrap_err();
        assert!(matches!(err, EnhanceSettingStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_returns_sorted_ids() {
        let (s, driver) = SqliteEnhanceSettingStore::open_in_memory().await.unwrap();
        s.put(&EnhanceSettingId::new("b"), dummy_setting("b", "bp"))
            .await
            .unwrap();
        s.put(&EnhanceSettingId::new("a"), dummy_setting("a", "bp"))
            .await
            .unwrap();
        s.put(&EnhanceSettingId::new("c"), dummy_setting("c", "bp"))
            .await
            .unwrap();
        let ids: Vec<_> = s.list().await.unwrap().into_iter().map(|i| i.0).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.db");

        {
            let (s, driver) = SqliteEnhanceSettingStore::open(&path).await.unwrap();
            s.put(
                &EnhanceSettingId::new("keep"),
                dummy_setting("keep", "bp-x"),
            )
            .await
            .unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }

        let (s, driver) = SqliteEnhanceSettingStore::open(&path).await.unwrap();
        let got = s.get(&EnhanceSettingId::new("keep")).await.unwrap();
        assert_eq!(got.blueprint_id.as_str(), "bp-x");
        drop(s);
        driver.shutdown().await.unwrap();
    }
}
