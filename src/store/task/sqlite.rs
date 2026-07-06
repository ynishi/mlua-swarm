//! `SqliteTaskStore` — SQLite-backed [`TaskStore`] using [`rusqlite-isle`].
//!
//! The `Connection` is confined to a dedicated OS thread by `AsyncIsle`;
//! every call is a typed closure dispatched over a bounded channel.
//! `blueprint_ref` / `input_ctx` are stored as JSON blobs (both are already
//! `serde_json::Value`), so schema evolution of the caller-side selector
//! shape never requires a migration here.
//!
//! ## Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS tasks (
//!   id                 TEXT PRIMARY KEY,
//!   goal               TEXT NOT NULL,
//!   blueprint_ref_json TEXT NOT NULL,
//!   input_ctx_json     TEXT NOT NULL,
//!   status             TEXT NOT NULL,      -- JSON-encoded `TaskRecordStatus`
//!   created_at         INTEGER NOT NULL,
//!   updated_at         INTEGER NOT NULL
//! );
//! CREATE INDEX IF NOT EXISTS ix_tasks_created_at ON tasks(created_at);
//! ```

use super::{TaskId, TaskRecord, TaskRecordStatus, TaskStore, TaskStoreError};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::path::Path;

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS tasks (\
  id                 TEXT PRIMARY KEY, \
  goal               TEXT NOT NULL, \
  blueprint_ref_json TEXT NOT NULL, \
  input_ctx_json     TEXT NOT NULL, \
  status             TEXT NOT NULL, \
  created_at         INTEGER NOT NULL, \
  updated_at         INTEGER NOT NULL\
);\
CREATE INDEX IF NOT EXISTS ix_tasks_created_at ON tasks(created_at);\
";

/// SQLite-backed persistent [`TaskStore`].
///
/// Open with [`SqliteTaskStore::open`] (file path) or
/// [`SqliteTaskStore::open_in_memory`] (tests). Both return the store plus
/// an [`AsyncIsleDriver`] the caller must `shutdown().await` when done —
/// dropping the driver without a shutdown call leaves the SQLite thread
/// as-is until the process exits.
pub struct SqliteTaskStore {
    isle: AsyncIsle,
}

impl SqliteTaskStore {
    /// Open (or create) a SQLite database file and run the schema
    /// migrations.
    pub async fn open(path: impl AsRef<Path>) -> Result<(Self, AsyncIsleDriver), TaskStoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), |conn| {
            conn.execute_batch(SCHEMA_SQL)
        })
        .await
        .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests, doctests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), TaskStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(|conn| conn.execute_batch(SCHEMA_SQL))
            .await
            .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }
}

fn map_isle_err(e: IsleError) -> TaskStoreError {
    TaskStoreError::Other(format!("sqlite: {e}"))
}

/// One `tasks` SELECT row in column order: id, goal, blueprint_ref_json,
/// input_ctx_json, status, created_at, updated_at.
type TaskRow = (String, String, String, String, String, i64, i64);

fn row_to_record(row: TaskRow) -> Result<TaskRecord, TaskStoreError> {
    let (id, goal, blueprint_ref_json, input_ctx_json, status_json, created_at, updated_at) = row;
    let blueprint_ref: serde_json::Value = serde_json::from_str(&blueprint_ref_json)
        .map_err(|e| TaskStoreError::Other(format!("decode blueprint_ref: {e}")))?;
    let input_ctx: serde_json::Value = serde_json::from_str(&input_ctx_json)
        .map_err(|e| TaskStoreError::Other(format!("decode input_ctx: {e}")))?;
    let status: TaskRecordStatus = serde_json::from_str(&status_json)
        .map_err(|e| TaskStoreError::Other(format!("decode status: {e}")))?;
    Ok(TaskRecord {
        id: TaskId(id),
        goal,
        blueprint_ref,
        input_ctx,
        status,
        created_at: created_at as u64,
        updated_at: updated_at as u64,
    })
}

#[async_trait]
impl TaskStore for SqliteTaskStore {
    fn name(&self) -> &str {
        "sqlite"
    }

    async fn create(&self, record: TaskRecord) -> Result<(), TaskStoreError> {
        let id = record.id.0.clone();
        let id_for_conflict = record.id.clone();
        let goal = record.goal.clone();
        let blueprint_ref_json = serde_json::to_string(&record.blueprint_ref)
            .map_err(|e| TaskStoreError::Other(format!("encode blueprint_ref: {e}")))?;
        let input_ctx_json = serde_json::to_string(&record.input_ctx)
            .map_err(|e| TaskStoreError::Other(format!("encode input_ctx: {e}")))?;
        let status_json = serde_json::to_string(&record.status)
            .map_err(|e| TaskStoreError::Other(format!("encode status: {e}")))?;
        let created_at = record.created_at as i64;
        let updated_at = record.updated_at as i64;

        self.isle
            .call(move |conn| {
                let tx = conn.transaction()?;
                let exists: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM tasks WHERE id = ?1",
                    params![id],
                    |row| row.get(0),
                )?;
                if exists > 0 {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
                        Some(format!("__mlua_swarm_duplicate:{id}")),
                    ));
                }
                tx.execute(
                    "INSERT INTO tasks (id, goal, blueprint_ref_json, input_ctx_json, status, \
                     created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        id,
                        goal,
                        blueprint_ref_json,
                        input_ctx_json,
                        status_json,
                        created_at,
                        updated_at,
                    ],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(|e| match &e {
                IsleError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(msg)))
                    if msg.starts_with("__mlua_swarm_duplicate:") =>
                {
                    TaskStoreError::Duplicate(id_for_conflict.clone())
                }
                _ => map_isle_err(e),
            })
    }

    async fn get(&self, id: &TaskId) -> Result<TaskRecord, TaskStoreError> {
        let id_str = id.0.clone();
        let id_for_notfound = id.clone();
        let row = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT id, goal, blueprint_ref_json, input_ctx_json, status, created_at, \
                     updated_at FROM tasks WHERE id = ?1",
                    params![id_str],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, i64>(6)?,
                        ))
                    },
                )
                .optional()
            })
            .await
            .map_err(map_isle_err)?;
        match row {
            Some(row) => row_to_record(row),
            None => Err(TaskStoreError::NotFound(id_for_notfound)),
        }
    }

    async fn list(&self) -> Result<Vec<TaskRecord>, TaskStoreError> {
        let rows = self
            .isle
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, goal, blueprint_ref_json, input_ctx_json, status, created_at, \
                     updated_at FROM tasks ORDER BY created_at DESC",
                )?;
                let iter = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
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
        rows.into_iter().map(row_to_record).collect()
    }

    async fn update_status(
        &self,
        id: &TaskId,
        status: TaskRecordStatus,
    ) -> Result<(), TaskStoreError> {
        let id_str = id.0.clone();
        let id_for_notfound = id.clone();
        let status_json = serde_json::to_string(&status)
            .map_err(|e| TaskStoreError::Other(format!("encode status: {e}")))?;
        let updated_at = crate::types::now_unix() as i64;
        let n = self
            .isle
            .call(move |conn| {
                conn.execute(
                    "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE id = ?3",
                    params![status_json, updated_at, id_str],
                )
            })
            .await
            .map_err(map_isle_err)?;
        if n == 0 {
            Err(TaskStoreError::NotFound(id_for_notfound))
        } else {
            Ok(())
        }
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
            id: TaskId(id.into()),
            goal: format!("goal for {id}"),
            blueprint_ref: json!({"id": "bp-1"}),
            input_ctx: json!({"k": "v"}),
            status: TaskRecordStatus::Pending,
            created_at,
            updated_at: created_at,
        }
    }

    #[tokio::test]
    async fn create_then_get() {
        let (s, driver) = SqliteTaskStore::open_in_memory().await.unwrap();
        s.create(mk("T-1", 100)).await.unwrap();
        let got = s.get(&TaskId("T-1".into())).await.unwrap();
        assert_eq!(got.goal, "goal for T-1");
        assert_eq!(got.status, TaskRecordStatus::Pending);
        assert_eq!(got.blueprint_ref, json!({"id": "bp-1"}));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_create_rejected() {
        let (s, driver) = SqliteTaskStore::open_in_memory().await.unwrap();
        s.create(mk("T-1", 100)).await.unwrap();
        let err = s.create(mk("T-1", 200)).await.unwrap_err();
        assert!(matches!(err, TaskStoreError::Duplicate(_)), "got: {err:?}");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn get_missing_returns_not_found() {
        let (s, driver) = SqliteTaskStore::open_in_memory().await.unwrap();
        let err = s.get(&TaskId("nope".into())).await.unwrap_err();
        assert!(matches!(err, TaskStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_returns_newest_first() {
        let (s, driver) = SqliteTaskStore::open_in_memory().await.unwrap();
        s.create(mk("T-1", 100)).await.unwrap();
        s.create(mk("T-2", 300)).await.unwrap();
        s.create(mk("T-3", 200)).await.unwrap();
        let list = s.list().await.unwrap();
        let ids: Vec<_> = list.iter().map(|r| r.id.0.clone()).collect();
        assert_eq!(ids, vec!["T-2", "T-3", "T-1"]);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn update_status_persists() {
        let (s, driver) = SqliteTaskStore::open_in_memory().await.unwrap();
        s.create(mk("T-1", 100)).await.unwrap();
        s.update_status(&TaskId("T-1".into()), TaskRecordStatus::Failed)
            .await
            .unwrap();
        let got = s.get(&TaskId("T-1".into())).await.unwrap();
        assert_eq!(got.status, TaskRecordStatus::Failed);
        assert!(got.updated_at >= got.created_at);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn update_status_unknown_fails() {
        let (s, driver) = SqliteTaskStore::open_in_memory().await.unwrap();
        let err = s
            .update_status(&TaskId("nope".into()), TaskRecordStatus::Done)
            .await
            .unwrap_err();
        assert!(matches!(err, TaskStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");

        {
            let (s, driver) = SqliteTaskStore::open(&path).await.unwrap();
            s.create(mk("keep", 42)).await.unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }

        let (s, driver) = SqliteTaskStore::open(&path).await.unwrap();
        let got = s.get(&TaskId("keep".into())).await.unwrap();
        assert_eq!(got.goal, "goal for keep");
        assert_eq!(got.created_at, 42);
        drop(s);
        driver.shutdown().await.unwrap();
    }
}
