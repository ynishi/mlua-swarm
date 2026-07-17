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
//!
//! ## Schema versioning
//!
//! The current schema is tracked with SQLite's `PRAGMA user_version` as the
//! single source of truth (1 = the two-column split above). [`init_schema`]
//! reads the value on every open and dispatches:
//!
//! - `user_version = 0` (fresh DB, or a legacy file created by mse
//!   ≤ v0.10.0 / pre-Core-primitive that used a `value_json` single column):
//!   if a legacy `replay_log` table is present it is `DROP`ed before the
//!   current schema is created, then `user_version` is stamped to `1`.
//!   Legacy rows carry no `ctx_snapshot_json`, so they cannot be replayed
//!   against the current wire anyway — dropping them is safe.
//! - `user_version = 1`: the current schema; `CREATE ... IF NOT EXISTS` is
//!   still run defensively.
//! - `user_version > 1`: reject with an error — an older mse binary must
//!   never touch a store written by a newer one.
//!
//! Future migrations follow the same pattern: add a `1 => migrate_v1_to_v2`
//! arm and stamp `user_version = 2`.

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

/// Latest schema version. Bumped whenever [`SCHEMA_SQL`] changes shape in a
/// way older binaries cannot read.
const CURRENT_SCHEMA_VERSION: i64 = 1;

/// Detect and migrate the `replay_log` schema on open. See the module-level
/// `Schema versioning` doc for the state-machine.
///
/// Returns `rusqlite::Result<()>` so it plugs straight into
/// `AsyncIsle::spawn` / `AsyncIsle::open_in_memory`; errors surface via
/// [`map_isle_err`] as [`ReplayStoreError::Other`] with the message
/// preserved verbatim.
fn init_schema(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    // Read current schema version. Fresh DB reports 0.
    let user_version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;

    match user_version {
        0 => {
            // Detect legacy schema: a `replay_log` table exists but lacks
            // the `ctx_snapshot_json` column. If so, drop it — legacy rows
            // carry no Ctx snapshot, so resume cursor hits cannot use them
            // anyway; data loss is safe.
            let table_present: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type = 'table' AND name = 'replay_log'",
                [],
                |r| r.get(0),
            )?;
            let has_ctx_column: i64 = if table_present > 0 {
                conn.query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('replay_log') \
                     WHERE name = 'ctx_snapshot_json'",
                    [],
                    |r| r.get(0),
                )?
            } else {
                0
            };
            let is_legacy = table_present > 0 && has_ctx_column == 0;
            if is_legacy {
                conn.execute("DROP TABLE replay_log", [])?;
            }
            conn.execute_batch(SCHEMA_SQL)?;
            // PRAGMA cannot bind parameters; the value is a compile-time
            // constant, so string-substitute it directly.
            conn.execute_batch(&format!("PRAGMA user_version = {CURRENT_SCHEMA_VERSION}"))?;
            Ok(())
        }
        v if v == CURRENT_SCHEMA_VERSION => {
            // Current schema. Still run CREATE ... IF NOT EXISTS in case the
            // table was manually deleted while user_version stayed at 1.
            conn.execute_batch(SCHEMA_SQL)
        }
        v => Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
            Some(format!(
                "replay_log schema version {v} is newer than supported \
                 ({CURRENT_SCHEMA_VERSION}); running an older mse binary \
                 against a store written by a newer one is not supported"
            )),
        )),
    }
}

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
    /// migrations. See [`init_schema`] and the module-level `Schema
    /// versioning` doc.
    pub async fn open(path: impl AsRef<Path>) -> Result<(Self, AsyncIsleDriver), ReplayStoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), init_schema)
            .await
            .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests, doctests). In-memory
    /// databases start with `user_version = 0` and are always fresh, so
    /// [`init_schema`] takes the version-0 arm and stamps
    /// `CURRENT_SCHEMA_VERSION` immediately.
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), ReplayStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(init_schema)
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

    // ──────────────────────────────────────────────────────────────────────
    // Schema-migration tests (see `init_schema`).
    // ──────────────────────────────────────────────────────────────────────

    /// Read `PRAGMA user_version` from a raw synchronous rusqlite handle so
    /// we can inspect the file after the isle driver has been shut down.
    fn read_user_version(path: &std::path::Path) -> i64 {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap()
    }

    #[tokio::test]
    async fn sqlite_fresh_open_stamps_current_schema_version() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("replay.sqlite");

        let (store, driver) = SqliteReplayStore::open(&path).await.unwrap();

        // The new schema is usable: round-trip an entry through it.
        let run_id = RunId::new();
        let ctx = mk_ctx();
        let entry = ReplayEntry::from_completion(
            run_id.clone(),
            "step-a",
            "hash-a",
            0,
            &ctx,
            &json!({ "n": 1 }),
        )
        .unwrap();
        store.append(entry).await.unwrap();
        let listed = store.list_by_run(&run_id).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].decode_step_output().unwrap(), json!({ "n": 1 }));

        driver.shutdown().await.unwrap();

        assert_eq!(read_user_version(&path), CURRENT_SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn sqlite_legacy_schema_is_dropped_and_rebuilt() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("replay.sqlite");

        // Seed a legacy-shape DB: single `value_json` column and
        // `user_version = 0` (default). Populate one row so we can prove
        // the migration drops the whole legacy log.
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE replay_log (\
                    seq         INTEGER PRIMARY KEY AUTOINCREMENT, \
                    run_id      TEXT NOT NULL, \
                    step_ref    TEXT NOT NULL, \
                    input_hash  TEXT NOT NULL, \
                    occurrence  INTEGER NOT NULL, \
                    value_json  TEXT NOT NULL, \
                    created_at  INTEGER NOT NULL, \
                    UNIQUE (run_id, step_ref, input_hash, occurrence)\
                );",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO replay_log \
                 (run_id, step_ref, input_hash, occurrence, value_json, created_at) \
                 VALUES ('legacy-run', 'legacy-step', 'legacy-hash', 0, 'legacy-value', 0)",
                [],
            )
            .unwrap();
            // `user_version` stays at 0 (the default), which is what a real
            // legacy file would carry.
        }

        assert_eq!(read_user_version(&path), 0, "seed sanity: user_version=0");

        // Open — should drop the legacy table, create the new schema, and
        // stamp `user_version = 1`.
        let (store, driver) = SqliteReplayStore::open(&path).await.unwrap();

        // The new schema accepts writes that the legacy schema could not
        // have held (the `ctx_snapshot_json` column exists).
        let run_id = RunId::new();
        let ctx = mk_ctx();
        let entry = ReplayEntry::from_completion(
            run_id.clone(),
            "step-a",
            "hash-a",
            0,
            &ctx,
            &json!({ "n": 42 }),
        )
        .unwrap();
        store.append(entry).await.unwrap();
        let listed = store.list_by_run(&run_id).await.unwrap();
        assert_eq!(listed.len(), 1);

        driver.shutdown().await.unwrap();

        assert_eq!(read_user_version(&path), CURRENT_SCHEMA_VERSION);

        // Legacy row must be gone (whole table was dropped).
        let conn = rusqlite::Connection::open(&path).unwrap();
        let legacy_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM replay_log WHERE run_id = 'legacy-run'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(legacy_count, 0, "legacy rows must be dropped");

        // And the new-shape column exists.
        let ctx_col_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('replay_log') \
                 WHERE name = 'ctx_snapshot_json'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ctx_col_count, 1, "ctx_snapshot_json column must be present");
    }

    #[tokio::test]
    async fn sqlite_current_schema_open_is_idempotent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("replay.sqlite");

        // First open: creates the schema and stamps user_version = 1.
        let run_id = {
            let (store, driver) = SqliteReplayStore::open(&path).await.unwrap();
            let run_id = RunId::new();
            let ctx = mk_ctx();
            let entry = ReplayEntry::from_completion(
                run_id.clone(),
                "step-a",
                "hash-a",
                0,
                &ctx,
                &json!({ "n": 7 }),
            )
            .unwrap();
            store.append(entry).await.unwrap();
            driver.shutdown().await.unwrap();
            run_id
        };

        assert_eq!(read_user_version(&path), CURRENT_SCHEMA_VERSION);

        // Second open: should be a no-op migration-wise; prior rows must
        // survive.
        let (store, driver) = SqliteReplayStore::open(&path).await.unwrap();
        let listed = store.list_by_run(&run_id).await.unwrap();
        assert_eq!(listed.len(), 1, "prior rows must survive re-open");
        assert_eq!(listed[0].step_ref, "step-a");
        assert_eq!(listed[0].decode_step_output().unwrap(), json!({ "n": 7 }));
        driver.shutdown().await.unwrap();

        // Still at the current schema version.
        assert_eq!(read_user_version(&path), CURRENT_SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn sqlite_future_schema_version_is_rejected() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("replay.sqlite");

        // Seed a file whose `user_version` is one ahead of what this binary
        // knows how to read.
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(&format!(
                "PRAGMA user_version = {}",
                CURRENT_SCHEMA_VERSION + 1
            ))
            .unwrap();
        }

        let res = SqliteReplayStore::open(&path).await;
        let err = res
            .err()
            .expect("future user_version must be rejected by init_schema");
        // The error should carry the migration message verbatim through
        // `map_isle_err`.
        let msg = err.to_string();
        assert!(
            msg.contains("newer than supported"),
            "unexpected error message: {msg}"
        );
    }
}
