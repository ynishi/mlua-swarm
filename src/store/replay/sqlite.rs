//! SQLite-backed [`ReplayStore`] using [`rusqlite-isle`].
//!
//! The [`crate::store::run::sqlite::SqliteRunStore`] pattern is the same:
//! the `Connection` is confined to a dedicated OS thread by `AsyncIsle`
//! and every call is a typed closure dispatched over a bounded channel.
//! `ctx_snapshot_json` and `step_output_json` are stored verbatim as
//! `TEXT`; the caller (the dispatcher) is what canonicalizes shape via
//! [`super::hash_input_value`].
//!
//! ## Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS replay_log (
//!   seq                 INTEGER PRIMARY KEY AUTOINCREMENT,
//!   run_id              TEXT NOT NULL,
//!   step_ref            TEXT NOT NULL,
//!   input_hash          TEXT NOT NULL,
//!   occurrence          INTEGER NOT NULL,
//!   ctx_snapshot_json   TEXT NOT NULL,
//!   step_output_json    TEXT NOT NULL,
//!   created_at          INTEGER NOT NULL,
//!   UNIQUE (run_id, step_ref, input_hash, occurrence)
//! );
//! CREATE INDEX IF NOT EXISTS ix_replay_run ON replay_log(run_id, seq);
//! ```

use super::{ReplayEntry, ReplayStore, ReplayStoreError};
use crate::types::RunId;
use async_trait::async_trait;
use rusqlite::params;
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::path::Path;

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS replay_log (\
  seq                 INTEGER PRIMARY KEY AUTOINCREMENT, \
  run_id              TEXT NOT NULL, \
  step_ref            TEXT NOT NULL, \
  input_hash          TEXT NOT NULL, \
  occurrence          INTEGER NOT NULL, \
  ctx_snapshot_json   TEXT NOT NULL, \
  step_output_json    TEXT NOT NULL, \
  created_at          INTEGER NOT NULL, \
  UNIQUE (run_id, step_ref, input_hash, occurrence)\
);\
CREATE INDEX IF NOT EXISTS ix_replay_run ON replay_log(run_id, seq);\
";

/// SQLite-backed persistent [`ReplayStore`].
///
/// Open with [`SqliteReplayStore::open`] (file path) or
/// [`SqliteReplayStore::open_in_memory`] (tests). Both return the store
/// plus an [`AsyncIsleDriver`] the caller must `shutdown().await` when
/// done — dropping the driver without a shutdown call leaves the SQLite
/// thread as-is until the process exits.
pub struct SqliteReplayStore {
    isle: AsyncIsle,
}

impl SqliteReplayStore {
    /// Open (or create) a SQLite database file and run the schema
    /// migrations.
    pub async fn open(path: impl AsRef<Path>) -> Result<(Self, AsyncIsleDriver), ReplayStoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), |conn| {
            conn.execute_batch(SCHEMA_SQL)
        })
        .await
        .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests, doctests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), ReplayStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(|conn| conn.execute_batch(SCHEMA_SQL))
            .await
            .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }
}

fn map_isle_err(e: IsleError) -> ReplayStoreError {
    ReplayStoreError::Other(format!("sqlite: {e}"))
}

#[async_trait]
impl ReplayStore for SqliteReplayStore {
    fn name(&self) -> &str {
        "sqlite"
    }

    async fn append(&self, entry: ReplayEntry) -> Result<(), ReplayStoreError> {
        let ReplayEntry {
            run_id,
            step_ref,
            input_hash,
            occurrence,
            ctx_snapshot_json,
            step_output_json,
            created_at,
        } = entry;
        let run_id_for_err = run_id.clone();
        let step_ref_for_err = step_ref.clone();
        let input_hash_for_err = input_hash.clone();
        let run_id_str = run_id.to_string();

        self.isle
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO replay_log \
                     (run_id, step_ref, input_hash, occurrence, ctx_snapshot_json, \
                      step_output_json, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        run_id_str,
                        step_ref,
                        input_hash,
                        occurrence as i64,
                        ctx_snapshot_json,
                        step_output_json,
                        created_at as i64,
                    ],
                )?;
                Ok(())
            })
            .await
            .map_err(|e| match &e {
                IsleError::Sqlite(rusqlite::Error::SqliteFailure(err, _))
                    if err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || err.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    ReplayStoreError::Duplicate {
                        run_id: run_id_for_err.clone(),
                        step_ref: step_ref_for_err.clone(),
                        input_hash: input_hash_for_err.clone(),
                        occurrence,
                    }
                }
                _ => map_isle_err(e),
            })
    }

    async fn list_by_run(&self, run_id: &RunId) -> Result<Vec<ReplayEntry>, ReplayStoreError> {
        let run_id_str = run_id.to_string();
        let run_id_owned = run_id.clone();
        let rows: Vec<(String, String, i64, String, String, i64)> = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT step_ref, input_hash, occurrence, ctx_snapshot_json, \
                     step_output_json, created_at FROM replay_log \
                     WHERE run_id = ?1 ORDER BY seq ASC",
                )?;
                let iter = stmt.query_map(params![run_id_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
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

        Ok(rows
            .into_iter()
            .map(
                |(
                    step_ref,
                    input_hash,
                    occurrence,
                    ctx_snapshot_json,
                    step_output_json,
                    created_at,
                )| {
                    ReplayEntry {
                        run_id: run_id_owned.clone(),
                        step_ref,
                        input_hash,
                        occurrence: occurrence as u32,
                        ctx_snapshot_json,
                        step_output_json,
                        created_at: created_at as u64,
                    }
                },
            )
            .collect())
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Tests.
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ctx::Ctx;
    use crate::store::replay::ReplayCursor;
    use crate::types::StepId;
    use serde_json::json;

    fn mk_ctx() -> Ctx {
        let mut ctx = Ctx::new(StepId::new(), 1, "step-a");
        ctx.meta.observer.insert("k".into(), json!("v"));
        ctx
    }

    #[tokio::test]
    async fn sqlite_append_and_list() {
        let (store, driver) = SqliteReplayStore::open_in_memory().await.unwrap();
        let run_id = RunId::new();
        let ctx = mk_ctx();

        let e0 = ReplayEntry::from_completion(
            run_id.clone(),
            "step-a",
            "hash-a",
            0,
            &ctx,
            &json!({ "n": 1 }),
        )
        .unwrap();
        store.append(e0).await.unwrap();

        let listed = store.list_by_run(&run_id).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].step_ref, "step-a");
        assert_eq!(listed[0].occurrence, 0);
        assert_eq!(listed[0].decode_step_output().unwrap(), json!({ "n": 1 }));

        // Ctx round-trip through the SQLite backend.
        let restored = listed[0].decode_ctx_snapshot().unwrap();
        assert_eq!(restored.agent, ctx.agent);
        assert_eq!(restored.attempt, ctx.attempt);
        assert_eq!(restored.meta.observer.get("k"), Some(&json!("v")));

        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_unique_4_tuple_is_enforced() {
        let (store, driver) = SqliteReplayStore::open_in_memory().await.unwrap();
        let run_id = RunId::new();
        let ctx = mk_ctx();

        let e0 = ReplayEntry::from_completion(
            run_id.clone(),
            "step-a",
            "hash-a",
            0,
            &ctx,
            &json!("first"),
        )
        .unwrap();
        store.append(e0.clone()).await.unwrap();

        // Same 4-tuple → Duplicate.
        let dup_err = store.append(e0).await.unwrap_err();
        assert!(
            matches!(dup_err, ReplayStoreError::Duplicate { .. }),
            "same (run_id, step_ref, input_hash, occurrence) must collide"
        );

        // occurrence=1 must NOT collide with occurrence=0 (loop replay
        // discipline).
        let e1 = ReplayEntry::from_completion(
            run_id.clone(),
            "step-a",
            "hash-a",
            1,
            &ctx,
            &json!("second"),
        )
        .unwrap();
        store
            .append(e1)
            .await
            .expect("occurrence=1 must not collide with occurrence=0");

        let listed = store.list_by_run(&run_id).await.unwrap();
        assert_eq!(listed.len(), 2);
        let cursor = ReplayCursor::from_entries(listed);
        assert_eq!(cursor.find("step-a", "hash-a", 0), Some(json!("first")));
        assert_eq!(cursor.find("step-a", "hash-a", 1), Some(json!("second")));

        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_list_by_run_returns_in_seq_order() {
        let (store, driver) = SqliteReplayStore::open_in_memory().await.unwrap();
        let run_id = RunId::new();
        let ctx = mk_ctx();

        for (i, (step, occ)) in [("a", 0), ("b", 0), ("a", 1)].iter().enumerate() {
            store
                .append(
                    ReplayEntry::from_completion(
                        run_id.clone(),
                        *step,
                        "h",
                        *occ,
                        &ctx,
                        &json!({ "idx": i }),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }

        let listed = store.list_by_run(&run_id).await.unwrap();
        let steps: Vec<(String, u32)> = listed
            .iter()
            .map(|e| (e.step_ref.clone(), e.occurrence))
            .collect();
        assert_eq!(
            steps,
            vec![
                ("a".to_string(), 0),
                ("b".to_string(), 0),
                ("a".to_string(), 1),
            ]
        );

        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_name() {
        let (store, driver) = SqliteReplayStore::open_in_memory().await.unwrap();
        assert_eq!(store.name(), "sqlite");
        driver.shutdown().await.unwrap();
    }
}
