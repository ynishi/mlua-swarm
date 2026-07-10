//! `SqliteRunStore` — SQLite-backed [`RunStore`] using [`rusqlite-isle`].
//!
//! The `Connection` is confined to a dedicated OS thread by `AsyncIsle`;
//! every call is a typed closure dispatched over a bounded channel.
//! `step_entries` and `result_ref` are stored as JSON blobs — the former is
//! a pure trace/observability artifact (not queried relationally), the
//! latter is caller-defined payload shape. `append_step_entry` runs as a
//! read-modify-write inside a single transaction so concurrent appenders
//! don't clobber each other's entries.
//!
//! ## Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS runs (
//!   id                 TEXT PRIMARY KEY,
//!   task_id            TEXT NOT NULL,
//!   status             TEXT NOT NULL,      -- JSON-encoded `RunStatus`
//!   step_entries_json  TEXT NOT NULL,      -- JSON-encoded `Vec<StepEntry>`
//!   operator_sid       TEXT,
//!   result_ref_json    TEXT,               -- JSON-encoded `serde_json::Value`, NULL when unset
//!   created_at         INTEGER NOT NULL,
//!   updated_at         INTEGER NOT NULL
//! );
//! CREATE INDEX IF NOT EXISTS ix_runs_task_id ON runs(task_id, created_at);
//! ```

use super::{RunId, RunRecord, RunStatus, RunStore, RunStoreError, StepEntry, TaskId};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::path::Path;

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS runs (\
  id                 TEXT PRIMARY KEY, \
  task_id            TEXT NOT NULL, \
  status             TEXT NOT NULL, \
  step_entries_json  TEXT NOT NULL, \
  operator_sid       TEXT, \
  result_ref_json    TEXT, \
  created_at         INTEGER NOT NULL, \
  updated_at         INTEGER NOT NULL\
);\
CREATE INDEX IF NOT EXISTS ix_runs_task_id ON runs(task_id, created_at);\
";

/// SQLite-backed persistent [`RunStore`].
///
/// Open with [`SqliteRunStore::open`] (file path) or
/// [`SqliteRunStore::open_in_memory`] (tests). Both return the store plus
/// an [`AsyncIsleDriver`] the caller must `shutdown().await` when done —
/// dropping the driver without a shutdown call leaves the SQLite thread
/// as-is until the process exits.
pub struct SqliteRunStore {
    isle: AsyncIsle,
}

impl SqliteRunStore {
    /// Open (or create) a SQLite database file and run the schema
    /// migrations.
    pub async fn open(path: impl AsRef<Path>) -> Result<(Self, AsyncIsleDriver), RunStoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), |conn| {
            conn.execute_batch(SCHEMA_SQL)
        })
        .await
        .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests, doctests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), RunStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(|conn| conn.execute_batch(SCHEMA_SQL))
            .await
            .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }
}

fn map_isle_err(e: IsleError) -> RunStoreError {
    RunStoreError::Other(format!("sqlite: {e}"))
}

/// One `runs` SELECT row in column order: id, task_id, status,
/// step_entries_json, operator_sid, result_ref_json, created_at,
/// updated_at.
type RunRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    i64,
    i64,
);

fn row_to_record(row: RunRow) -> Result<RunRecord, RunStoreError> {
    let (
        id,
        task_id,
        status_json,
        step_entries_json,
        operator_sid,
        result_ref_json,
        created_at,
        updated_at,
    ) = row;
    let status: RunStatus = serde_json::from_str(&status_json)
        .map_err(|e| RunStoreError::Other(format!("decode status: {e}")))?;
    let step_entries: Vec<StepEntry> = serde_json::from_str(&step_entries_json)
        .map_err(|e| RunStoreError::Other(format!("decode step_entries: {e}")))?;
    let result_ref: Option<serde_json::Value> = match result_ref_json {
        Some(text) => Some(
            serde_json::from_str(&text)
                .map_err(|e| RunStoreError::Other(format!("decode result_ref: {e}")))?,
        ),
        None => None,
    };
    // Ids were minted by us before landing in the table; a prefix mismatch
    // here means the row predates the issue #13 prefix reconciliation or
    // the file was written by something else — fail loud either way.
    let id = RunId::parse(id).map_err(|e| RunStoreError::Other(format!("decode id: {e}")))?;
    let task_id =
        TaskId::parse(task_id).map_err(|e| RunStoreError::Other(format!("decode task_id: {e}")))?;
    Ok(RunRecord {
        id,
        task_id,
        status,
        step_entries,
        operator_sid,
        result_ref,
        created_at: created_at as u64,
        updated_at: updated_at as u64,
    })
}

#[async_trait]
impl RunStore for SqliteRunStore {
    fn name(&self) -> &str {
        "sqlite"
    }

    async fn create(&self, record: RunRecord) -> Result<(), RunStoreError> {
        let id = record.id.to_string();
        let id_for_conflict = record.id.clone();
        let task_id = record.task_id.to_string();
        let status_json = serde_json::to_string(&record.status)
            .map_err(|e| RunStoreError::Other(format!("encode status: {e}")))?;
        let step_entries_json = serde_json::to_string(&record.step_entries)
            .map_err(|e| RunStoreError::Other(format!("encode step_entries: {e}")))?;
        let operator_sid = record.operator_sid.clone();
        let result_ref_json = record
            .result_ref
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| RunStoreError::Other(format!("encode result_ref: {e}")))?;
        let created_at = record.created_at as i64;
        let updated_at = record.updated_at as i64;

        self.isle
            .call(move |conn| {
                let tx = conn.transaction()?;
                let exists: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM runs WHERE id = ?1",
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
                    "INSERT INTO runs (id, task_id, status, step_entries_json, operator_sid, \
                     result_ref_json, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        id,
                        task_id,
                        status_json,
                        step_entries_json,
                        operator_sid,
                        result_ref_json,
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
                    RunStoreError::Duplicate(id_for_conflict.clone())
                }
                _ => map_isle_err(e),
            })
    }

    async fn get(&self, id: &RunId) -> Result<RunRecord, RunStoreError> {
        let id_str = id.to_string();
        let id_for_notfound = id.clone();
        let row = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT id, task_id, status, step_entries_json, operator_sid, \
                     result_ref_json, created_at, updated_at FROM runs WHERE id = ?1",
                    params![id_str],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, Option<String>>(5)?,
                            row.get::<_, i64>(6)?,
                            row.get::<_, i64>(7)?,
                        ))
                    },
                )
                .optional()
            })
            .await
            .map_err(map_isle_err)?;
        match row {
            Some(row) => row_to_record(row),
            None => Err(RunStoreError::NotFound(id_for_notfound)),
        }
    }

    async fn list_by_task(&self, task_id: &TaskId) -> Result<Vec<RunRecord>, RunStoreError> {
        let task_id_str = task_id.to_string();
        let rows = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, task_id, status, step_entries_json, operator_sid, \
                     result_ref_json, created_at, updated_at FROM runs \
                     WHERE task_id = ?1 ORDER BY created_at ASC",
                )?;
                let iter = stmt.query_map(params![task_id_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
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

    async fn append_step_entry(&self, id: &RunId, entry: StepEntry) -> Result<(), RunStoreError> {
        let id_str = id.to_string();
        let id_for_notfound = id.clone();
        let updated_at = crate::types::now_unix() as i64;

        let updated = self
            .isle
            .call(move |conn| {
                let tx = conn.transaction()?;
                let existing: Option<String> = tx
                    .query_row(
                        "SELECT step_entries_json FROM runs WHERE id = ?1",
                        params![id_str],
                        |row| row.get(0),
                    )
                    .optional()?;
                let Some(existing_json) = existing else {
                    return Ok(false);
                };
                let mut entries: Vec<StepEntry> = serde_json::from_str(&existing_json)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                entries.push(entry);
                let new_json = serde_json::to_string(&entries)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                tx.execute(
                    "UPDATE runs SET step_entries_json = ?1, updated_at = ?2 WHERE id = ?3",
                    params![new_json, updated_at, id_str],
                )?;
                tx.commit()?;
                Ok(true)
            })
            .await
            .map_err(map_isle_err)?;

        if updated {
            Ok(())
        } else {
            Err(RunStoreError::NotFound(id_for_notfound))
        }
    }

    async fn update_status(&self, id: &RunId, status: RunStatus) -> Result<(), RunStoreError> {
        let id_str = id.to_string();
        let id_for_notfound = id.clone();
        let status_json = serde_json::to_string(&status)
            .map_err(|e| RunStoreError::Other(format!("encode status: {e}")))?;
        let updated_at = crate::types::now_unix() as i64;
        let n = self
            .isle
            .call(move |conn| {
                conn.execute(
                    "UPDATE runs SET status = ?1, updated_at = ?2 WHERE id = ?3",
                    params![status_json, updated_at, id_str],
                )
            })
            .await
            .map_err(map_isle_err)?;
        if n == 0 {
            Err(RunStoreError::NotFound(id_for_notfound))
        } else {
            Ok(())
        }
    }

    async fn set_result(
        &self,
        id: &RunId,
        result_ref: serde_json::Value,
    ) -> Result<(), RunStoreError> {
        let id_str = id.to_string();
        let id_for_notfound = id.clone();
        let result_ref_json = serde_json::to_string(&result_ref)
            .map_err(|e| RunStoreError::Other(format!("encode result_ref: {e}")))?;
        let updated_at = crate::types::now_unix() as i64;
        let n = self
            .isle
            .call(move |conn| {
                conn.execute(
                    "UPDATE runs SET result_ref_json = ?1, updated_at = ?2 WHERE id = ?3",
                    params![result_ref_json, updated_at, id_str],
                )
            })
            .await
            .map_err(map_isle_err)?;
        if n == 0 {
            Err(RunStoreError::NotFound(id_for_notfound))
        } else {
            Ok(())
        }
    }

    async fn list_running(&self) -> Result<Vec<RunRecord>, RunStoreError> {
        let status_json = serde_json::to_string(&RunStatus::Running)
            .map_err(|e| RunStoreError::Other(format!("encode status: {e}")))?;
        let rows = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, task_id, status, step_entries_json, operator_sid, \
                     result_ref_json, created_at, updated_at FROM runs \
                     WHERE status = ?1",
                )?;
                let iter = stmt.query_map(params![status_json], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
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
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.task_id, TaskId::parse("T-1").unwrap());
        assert_eq!(got.status, RunStatus::Pending);
        assert!(got.step_entries.is_empty());
        assert_eq!(got.result_ref, None);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_create_rejected() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let err = s.create(mk("R-1", "T-1", 200)).await.unwrap_err();
        assert!(matches!(err, RunStoreError::Duplicate(_)), "got: {err:?}");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn get_missing_returns_not_found() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        let err = s.get(&RunId::parse("R-nope").unwrap()).await.unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_by_task_filters_and_orders_ascending() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 300)).await.unwrap();
        s.create(mk("R-2", "T-2", 50)).await.unwrap();
        s.create(mk("R-3", "T-1", 100)).await.unwrap();
        let list = s
            .list_by_task(&TaskId::parse("T-1").unwrap())
            .await
            .unwrap();
        let ids: Vec<_> = list.iter().map(|r| r.id.to_string()).collect();
        assert_eq!(ids, vec!["R-3", "R-1"]);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn append_step_entry_accumulates_in_order() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
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
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn append_step_entry_unknown_run_fails() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
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
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn update_status_persists() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.update_status(&RunId::parse("R-1").unwrap(), RunStatus::Done)
            .await
            .unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.status, RunStatus::Done);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn set_result_persists() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.set_result(&RunId::parse("R-1").unwrap(), json!({"ok": true}))
            .await
            .unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.result_ref, Some(json!({"ok": true})));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runs.db");

        {
            let (s, driver) = SqliteRunStore::open(&path).await.unwrap();
            s.create(mk("R-keep", "T-keep", 42)).await.unwrap();
            s.append_step_entry(
                &RunId::parse("R-keep").unwrap(),
                StepEntry {
                    step_id: crate::types::StepId::parse("ST-1").unwrap(),
                    step_ref: Some("step-a".into()),
                    status: Some("dispatched".into()),
                    at: 43,
                },
            )
            .await
            .unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }

        let (s, driver) = SqliteRunStore::open(&path).await.unwrap();
        let got = s.get(&RunId::parse("R-keep").unwrap()).await.unwrap();
        assert_eq!(got.task_id, TaskId::parse("T-keep").unwrap());
        assert_eq!(got.step_entries.len(), 1);
        assert_eq!(got.step_entries[0].step_ref, Some("step-a".into()));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_running_filters_by_status() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.create(mk("R-2", "T-2", 200)).await.unwrap();
        s.create(mk("R-3", "T-3", 300)).await.unwrap();
        s.update_status(&RunId::parse("R-2").unwrap(), RunStatus::Running)
            .await
            .unwrap();
        s.update_status(&RunId::parse("R-3").unwrap(), RunStatus::Done)
            .await
            .unwrap();
        let running = s.list_running().await.unwrap();
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].id, RunId::parse("R-2").unwrap());
        assert_eq!(running[0].status, RunStatus::Running);
        drop(s);
        driver.shutdown().await.unwrap();
    }
}
