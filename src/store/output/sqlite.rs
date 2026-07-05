//! `SqliteOutputStore` — SQLite-backed [`OutputStore`].
//!
//! One row per emit (`OutputRecord`). `event` and `parent_refs` are stored
//! as JSON blobs (both types already carry `Serialize + Deserialize`), so
//! adding a new [`OutputEvent`] variant does not require a schema migration.
//!
//! Ordering guarantees:
//!
//! - `list_for_attempt` returns rows in insertion order (per the trait
//!   contract) via an autoincrementing `seq` column.
//! - `get_latest_by_name` picks the row with the largest `seq` for a given
//!   `producer_agent`.

use super::{OutputEvent, OutputRecord, OutputRef, OutputStore, OutputStoreError};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::path::Path;

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS outputs (\
  id             TEXT PRIMARY KEY, \
  task_id        TEXT NOT NULL, \
  attempt        INTEGER NOT NULL, \
  producer_agent TEXT NOT NULL, \
  event_json     TEXT NOT NULL, \
  parent_refs_json TEXT NOT NULL, \
  seq            INTEGER NOT NULL\
);\
CREATE INDEX IF NOT EXISTS ix_outputs_attempt ON outputs(task_id, attempt, seq);\
CREATE INDEX IF NOT EXISTS ix_outputs_producer ON outputs(producer_agent, seq);\
";

/// SQLite-backed [`OutputStore`].
pub struct SqliteOutputStore {
    isle: AsyncIsle,
}

impl SqliteOutputStore {
    /// Open (or create) a SQLite file and apply the schema.
    pub async fn open(path: impl AsRef<Path>) -> Result<(Self, AsyncIsleDriver), OutputStoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), |conn| {
            conn.execute_batch(SCHEMA_SQL)
        })
        .await
        .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), OutputStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(|conn| conn.execute_batch(SCHEMA_SQL))
            .await
            .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }
}

fn map_isle_err(e: IsleError) -> OutputStoreError {
    OutputStoreError::Internal(format!("sqlite: {e}"))
}

fn decode_record(
    id: String,
    task_id: String,
    attempt: i64,
    producer_agent: String,
    event_json: String,
    parent_refs_json: String,
) -> Result<OutputRecord, OutputStoreError> {
    let event: OutputEvent = serde_json::from_str(&event_json)
        .map_err(|e| OutputStoreError::Internal(format!("decode event: {e}")))?;
    let parent_refs: Vec<OutputRef> = serde_json::from_str(&parent_refs_json)
        .map_err(|e| OutputStoreError::Internal(format!("decode parent_refs: {e}")))?;
    Ok(OutputRecord {
        id: OutputRef(id),
        task_id,
        attempt: attempt as u32,
        producer_agent,
        event,
        parent_refs,
    })
}

#[async_trait]
impl OutputStore for SqliteOutputStore {
    async fn append(
        &self,
        task_id: &str,
        attempt: u32,
        producer_agent: &str,
        event: OutputEvent,
        parent_refs: Vec<OutputRef>,
    ) -> Result<OutputRef, OutputStoreError> {
        let id = OutputRef::new();
        let id_str = id.0.clone();
        let task_id = task_id.to_string();
        let attempt = attempt as i64;
        let producer_agent = producer_agent.to_string();
        let event_json = serde_json::to_string(&event)
            .map_err(|e| OutputStoreError::Internal(format!("encode event: {e}")))?;
        let parent_refs_json = serde_json::to_string(&parent_refs)
            .map_err(|e| OutputStoreError::Internal(format!("encode parent_refs: {e}")))?;

        self.isle
            .call(move |conn| {
                let tx = conn.transaction()?;
                let seq: i64 =
                    tx.query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM outputs", [], |row| {
                        row.get(0)
                    })?;
                tx.execute(
                    "INSERT INTO outputs (id, task_id, attempt, producer_agent, event_json, \
                     parent_refs_json, seq) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        id_str,
                        task_id,
                        attempt,
                        producer_agent,
                        event_json,
                        parent_refs_json,
                        seq,
                    ],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(map_isle_err)?;
        Ok(id)
    }

    async fn get(&self, id: &OutputRef) -> Result<OutputRecord, OutputStoreError> {
        let id_str = id.0.clone();
        let id_for_notfound = id.0.clone();
        let row = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT id, task_id, attempt, producer_agent, event_json, parent_refs_json \
                     FROM outputs WHERE id = ?1",
                    params![id_str],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                        ))
                    },
                )
                .optional()
            })
            .await
            .map_err(map_isle_err)?;
        match row {
            Some((id, task_id, attempt, producer, event, parent)) => {
                decode_record(id, task_id, attempt, producer, event, parent)
            }
            None => Err(OutputStoreError::NotFound(id_for_notfound)),
        }
    }

    async fn get_latest_by_name(&self, name: &str) -> Result<OutputRecord, OutputStoreError> {
        let name_str = name.to_string();
        let name_for_notfound = name.to_string();
        let row = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT id, task_id, attempt, producer_agent, event_json, parent_refs_json \
                     FROM outputs WHERE producer_agent = ?1 ORDER BY seq DESC LIMIT 1",
                    params![name_str],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                        ))
                    },
                )
                .optional()
            })
            .await
            .map_err(map_isle_err)?;
        match row {
            Some((id, task_id, attempt, producer, event, parent)) => {
                decode_record(id, task_id, attempt, producer, event, parent)
            }
            None => Err(OutputStoreError::NotFound(name_for_notfound)),
        }
    }

    async fn list_for_attempt(
        &self,
        task_id: &str,
        attempt: u32,
    ) -> Result<Vec<OutputRecord>, OutputStoreError> {
        let task_id = task_id.to_string();
        let attempt = attempt as i64;
        let rows = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, task_id, attempt, producer_agent, event_json, parent_refs_json \
                     FROM outputs WHERE task_id = ?1 AND attempt = ?2 ORDER BY seq ASC",
                )?;
                let iter = stmt.query_map(params![task_id, attempt], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                })?;
                let mut out = Vec::new();
                for r in iter {
                    out.push(r?);
                }
                Ok(out)
            })
            .await
            .map_err(map_isle_err)?;
        rows.into_iter()
            .map(|(id, task_id, attempt, producer, event, parent)| {
                decode_record(id, task_id, attempt, producer, event, parent)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::output::ContentRef;

    fn mk_final(text: &str, ok: bool) -> OutputEvent {
        OutputEvent::Final {
            content: ContentRef::inline_text(text),
            ok,
        }
    }

    #[tokio::test]
    async fn append_then_get_roundtrip() {
        let (s, driver) = SqliteOutputStore::open_in_memory().await.unwrap();
        let id = s
            .append("task-1", 1, "producer-a", mk_final("hello", true), vec![])
            .await
            .unwrap();
        let got = s.get(&id).await.unwrap();
        assert_eq!(got.id, id);
        assert_eq!(got.task_id, "task-1");
        assert_eq!(got.attempt, 1);
        assert_eq!(got.producer_agent, "producer-a");
        match got.event {
            OutputEvent::Final { ok, .. } => assert!(ok),
            other => panic!("unexpected: {other:?}"),
        }
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn get_not_found_returns_error() {
        let (s, driver) = SqliteOutputStore::open_in_memory().await.unwrap();
        let err = s.get(&OutputRef("missing".into())).await.unwrap_err();
        assert!(matches!(err, OutputStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_for_attempt_orders_by_insertion() {
        let (s, driver) = SqliteOutputStore::open_in_memory().await.unwrap();
        let a = s
            .append("t", 1, "p1", mk_final("a", true), vec![])
            .await
            .unwrap();
        let b = s
            .append("t", 1, "p2", mk_final("b", true), vec![])
            .await
            .unwrap();
        // Not part of the same attempt — must be skipped by the filter.
        let _ = s
            .append("t", 2, "p1", mk_final("other-attempt", true), vec![])
            .await
            .unwrap();
        let c = s
            .append("t", 1, "p3", mk_final("c", true), vec![])
            .await
            .unwrap();

        let listed = s.list_for_attempt("t", 1).await.unwrap();
        let ids: Vec<_> = listed.iter().map(|r| r.id.clone()).collect();
        assert_eq!(ids, vec![a, b, c]);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn get_latest_by_name_returns_newest_emit() {
        let (s, driver) = SqliteOutputStore::open_in_memory().await.unwrap();
        let _ = s
            .append("t", 1, "same-producer", mk_final("v1", true), vec![])
            .await
            .unwrap();
        let _ = s
            .append(
                "t",
                1,
                "other-producer",
                mk_final("unrelated", true),
                vec![],
            )
            .await
            .unwrap();
        let latest_id = s
            .append("t", 2, "same-producer", mk_final("v2", true), vec![])
            .await
            .unwrap();
        let got = s.get_latest_by_name("same-producer").await.unwrap();
        assert_eq!(got.id, latest_id);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn get_latest_by_name_unknown_returns_not_found() {
        let (s, driver) = SqliteOutputStore::open_in_memory().await.unwrap();
        let err = s.get_latest_by_name("nobody").await.unwrap_err();
        assert!(matches!(err, OutputStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn parent_refs_are_persisted() {
        let (s, driver) = SqliteOutputStore::open_in_memory().await.unwrap();
        let a = s
            .append("t", 1, "p", mk_final("parent", true), vec![])
            .await
            .unwrap();
        let b = s
            .append("t", 1, "p", mk_final("child", true), vec![a.clone()])
            .await
            .unwrap();
        let got = s.get(&b).await.unwrap();
        assert_eq!(got.parent_refs, vec![a]);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("outputs.db");
        let id;
        {
            let (s, driver) = SqliteOutputStore::open(&path).await.unwrap();
            id = s
                .append("keep", 1, "p", mk_final("body", true), vec![])
                .await
                .unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }
        let (s, driver) = SqliteOutputStore::open(&path).await.unwrap();
        let got = s.get(&id).await.unwrap();
        assert_eq!(got.task_id, "keep");
        drop(s);
        driver.shutdown().await.unwrap();
    }
}
