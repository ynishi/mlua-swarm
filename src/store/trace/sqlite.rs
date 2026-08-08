//! `SqliteRunTraceStore` — SQLite-backed [`RunTraceStore`] using
//! [`rusqlite-isle`], sharing the same database FILE as
//! [`crate::store::run::SqliteRunStore`] (`~/.mse/store/run.sqlite` by
//! default) in its own `run_trace` table. Sharing the file keeps "one
//! Run's persistence" a single artifact on disk; each store still owns
//! its own confined `Connection` (its own `AsyncIsle` thread), so both
//! connections set `busy_timeout` to ride out each other's short write
//! transactions.
//!
//! ## Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS run_trace (
//!   run_id       TEXT NOT NULL,
//!   seq          INTEGER NOT NULL,   -- per-run, 1-based, monotonic
//!   ts_ms        INTEGER NOT NULL,
//!   kind         TEXT NOT NULL,      -- namespaced open set
//!   step_ref     TEXT,
//!   attempt      INTEGER,
//!   payload_json TEXT NOT NULL,
//!   PRIMARY KEY (run_id, seq)
//! );
//! ```
//!
//! `seq` assignment and retention pruning run inside one transaction per
//! append, so concurrent appenders to the same Run cannot collide on a
//! seq or over-prune. Filtering (kind prefix / step_ref / attempt) is
//! applied in Rust over the per-Run rows (bounded by the retention
//! ceiling) — the query axes are too dynamic to be worth SQL-side
//! predicates at this scale.

use super::{
    cap_payload, now_unix_ms, RunTraceStore, TraceEvent, TraceEventDraft, TraceQuery,
    TraceStoreError, DEFAULT_TRACE_MAX_EVENTS_PER_RUN,
};
use crate::types::RunId;
use async_trait::async_trait;
use rusqlite::params;
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::path::Path;

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS run_trace (\
  run_id       TEXT NOT NULL, \
  seq          INTEGER NOT NULL, \
  ts_ms        INTEGER NOT NULL, \
  kind         TEXT NOT NULL, \
  step_ref     TEXT, \
  attempt      INTEGER, \
  payload_json TEXT NOT NULL, \
  PRIMARY KEY (run_id, seq)\
);\
";

/// SQLite-backed persistent [`RunTraceStore`].
///
/// Open with [`SqliteRunTraceStore::open`] (file path — typically the
/// SAME path as the `SqliteRunStore`) or
/// [`SqliteRunTraceStore::open_in_memory`] (tests). Both return the
/// store plus an [`AsyncIsleDriver`] the caller must `shutdown().await`
/// when done.
pub struct SqliteRunTraceStore {
    isle: AsyncIsle,
    max_events_per_run: usize,
}

fn init_conn(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    // Two isles (runs / run_trace) share one database file. The busy
    // wait only helps when the busy handler is actually invoked — a
    // DEFERRED read-then-upgrade transaction racing another writer gets
    // an IMMEDIATE `SQLITE_BUSY` (deadlock avoidance bypasses the
    // handler), which is why every write transaction on this file uses
    // `TransactionBehavior::Immediate` (RESERVED up front → the busy
    // wait applies). See `RunTraceStore::append` and the sibling
    // `SqliteRunStore` write paths.
    conn.busy_timeout(std::time::Duration::from_millis(5_000))?;
    conn.execute_batch(SCHEMA_SQL)
}

impl SqliteRunTraceStore {
    /// Open (or create) a SQLite database file and ensure the
    /// `run_trace` table exists. Safe to point at the file another
    /// store already owns — `CREATE TABLE IF NOT EXISTS` never touches
    /// foreign tables.
    pub async fn open(path: impl AsRef<Path>) -> Result<(Self, AsyncIsleDriver), TraceStoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), init_conn)
            .await
            .map_err(map_isle_err)?;
        Ok((
            Self {
                isle,
                max_events_per_run: DEFAULT_TRACE_MAX_EVENTS_PER_RUN,
            },
            driver,
        ))
    }

    /// Open an ephemeral in-memory database (tests, doctests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), TraceStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(init_conn)
            .await
            .map_err(map_isle_err)?;
        Ok((
            Self {
                isle,
                max_events_per_run: DEFAULT_TRACE_MAX_EVENTS_PER_RUN,
            },
            driver,
        ))
    }

    /// Override the per-Run retention ceiling (tests).
    pub fn with_max_events_per_run(mut self, max: usize) -> Self {
        self.max_events_per_run = max;
        self
    }
}

fn map_isle_err(e: IsleError) -> TraceStoreError {
    TraceStoreError::Other(format!("sqlite: {e}"))
}

/// One `run_trace` SELECT row in column order: seq, ts_ms, kind,
/// step_ref, attempt, payload_json.
type TraceRow = (i64, i64, String, Option<String>, Option<i64>, String);

fn row_to_event(run_id: &RunId, row: TraceRow) -> Result<TraceEvent, TraceStoreError> {
    let (seq, ts_ms, kind, step_ref, attempt, payload_json) = row;
    let payload = serde_json::from_str(&payload_json)
        .map_err(|e| TraceStoreError::Other(format!("decode payload: {e}")))?;
    Ok(TraceEvent {
        run_id: run_id.clone(),
        seq: seq as u64,
        ts_ms,
        kind,
        step_ref,
        attempt: attempt.map(|a| a as u32),
        payload,
    })
}

#[async_trait]
impl RunTraceStore for SqliteRunTraceStore {
    fn name(&self) -> &str {
        "sqlite"
    }

    async fn append(
        &self,
        run_id: &RunId,
        draft: TraceEventDraft,
    ) -> Result<TraceEvent, TraceStoreError> {
        let run_id_str = run_id.to_string();
        let ts_ms = now_unix_ms();
        let payload = cap_payload(draft.payload);
        let payload_json = payload.to_string();
        let kind = draft.kind.clone();
        let step_ref = draft.step_ref.clone();
        let attempt = draft.attempt.map(|a| a as i64);
        let max = self.max_events_per_run as i64;

        let seq = self
            .isle
            .call(move |conn| {
                // Immediate (not DEFERRED): grab RESERVED before the
                // MAX(seq) read so a concurrent `runs`-table writer on the
                // shared file triggers the busy WAIT, not an instant
                // SQLITE_BUSY from the deadlock-avoidance path.
                let tx =
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let next: i64 = tx.query_row(
                    "SELECT COALESCE(MAX(seq), 0) + 1 FROM run_trace WHERE run_id = ?1",
                    params![run_id_str],
                    |row| row.get(0),
                )?;
                tx.execute(
                    "INSERT INTO run_trace \
                     (run_id, seq, ts_ms, kind, step_ref, attempt, payload_json) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        run_id_str,
                        next,
                        ts_ms,
                        kind,
                        step_ref,
                        attempt,
                        payload_json
                    ],
                )?;
                // Retention: prune the oldest rows beyond the ceiling
                // (same transaction, so a concurrent appender can't
                // observe an over-long stream).
                tx.execute(
                    "DELETE FROM run_trace WHERE run_id = ?1 AND seq <= ?2 - ?3",
                    params![run_id_str, next, max],
                )?;
                tx.commit()?;
                Ok(next)
            })
            .await
            .map_err(map_isle_err)?;

        Ok(TraceEvent {
            run_id: run_id.clone(),
            seq: seq as u64,
            ts_ms,
            kind: draft.kind,
            step_ref: draft.step_ref,
            attempt: draft.attempt,
            payload,
        })
    }

    async fn list(
        &self,
        run_id: &RunId,
        query: &TraceQuery,
    ) -> Result<Vec<TraceEvent>, TraceStoreError> {
        let run_id_str = run_id.to_string();
        // Fast path (holistic finding): the unfiltered case — notably the
        // `latest=N` `log_tail` poll — pages in SQL instead of loading the
        // whole retained stream (up to the 10k ceiling) to return N rows.
        // Filtered queries (kind/step/attempt) keep the Rust-side scan;
        // their predicate set is too dynamic to be worth SQL predicates.
        // Every branch binds the same `(?1, ?2, ?3)` triple so one
        // `query_map` call serves all three statements; the full-scan
        // branch neutralizes `?2`/`?3` (`?2 >= 0` always true, `LIMIT -1`
        // = no cap).
        let unfiltered =
            query.kinds.is_empty() && query.step_ref.is_none() && query.attempt.is_none();
        let (sql, p2, p3): (&str, i64, i64) = if !unfiltered {
            (
                "SELECT seq, ts_ms, kind, step_ref, attempt, payload_json \
                 FROM run_trace WHERE run_id = ?1 AND ?2 >= 0 ORDER BY seq ASC LIMIT ?3",
                0,
                -1,
            )
        } else if let Some(n) = query.latest {
            (
                "SELECT seq, ts_ms, kind, step_ref, attempt, payload_json FROM \
                 (SELECT * FROM run_trace WHERE run_id = ?1 AND ?2 >= 0 \
                  ORDER BY seq DESC LIMIT ?3) ORDER BY seq ASC",
                0,
                n as i64,
            )
        } else {
            (
                "SELECT seq, ts_ms, kind, step_ref, attempt, payload_json \
                 FROM run_trace WHERE run_id = ?1 AND seq > ?2 ORDER BY seq ASC LIMIT ?3",
                query.after.unwrap_or(0) as i64,
                query.limit.unwrap_or(super::DEFAULT_TRACE_LIST_LIMIT) as i64,
            )
        };
        let rows = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(sql)?;
                let iter = stmt.query_map(params![run_id_str, p2, p3], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
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

        let events = rows
            .into_iter()
            .map(|row| row_to_event(run_id, row))
            .collect::<Result<Vec<_>, _>>()?;
        if unfiltered {
            // Paging already applied in SQL.
            return Ok(events);
        }
        let filtered: Vec<TraceEvent> = events.into_iter().filter(|e| query.matches(e)).collect();
        Ok(query.page(filtered))
    }

    async fn delete_run(&self, run_id: &RunId) -> Result<u64, TraceStoreError> {
        let run_id_str = run_id.to_string();
        let n = self
            .isle
            .call(move |conn| {
                conn.execute(
                    "DELETE FROM run_trace WHERE run_id = ?1",
                    params![run_id_str],
                )
            })
            .await
            .map_err(map_isle_err)?;
        Ok(n as u64)
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rid(s: &str) -> RunId {
        RunId::parse(s).unwrap()
    }

    fn draft(kind: &str) -> TraceEventDraft {
        TraceEventDraft {
            kind: kind.to_string(),
            step_ref: Some("w".into()),
            attempt: Some(1),
            payload: json!({"k": kind}),
        }
    }

    #[tokio::test]
    async fn append_assigns_monotonic_seq_and_roundtrips() {
        let (s, driver) = SqliteRunTraceStore::open_in_memory().await.unwrap();
        let e1 = s
            .append(&rid("R-1"), draft("core.run_started"))
            .await
            .unwrap();
        let e2 = s
            .append(&rid("R-1"), draft("core.step_dispatched"))
            .await
            .unwrap();
        let other = s
            .append(&rid("R-2"), draft("core.run_started"))
            .await
            .unwrap();
        assert_eq!(e1.seq, 1);
        assert_eq!(e2.seq, 2);
        assert_eq!(other.seq, 1, "seq is per-Run");

        let got = s.list(&rid("R-1"), &TraceQuery::default()).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].kind, "core.run_started");
        assert_eq!(got[1].kind, "core.step_dispatched");
        assert_eq!(got[1].step_ref.as_deref(), Some("w"));
        assert_eq!(got[1].attempt, Some(1));
        assert_eq!(got[1].payload, json!({"k": "core.step_dispatched"}));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_filters_and_pages() {
        let (s, driver) = SqliteRunTraceStore::open_in_memory().await.unwrap();
        let r = rid("R-1");
        s.append(&r, draft("core.step_dispatched")).await.unwrap();
        s.append(&r, draft("mw.long_hold_warn")).await.unwrap();
        s.append(&r, draft("core.step_completed")).await.unwrap();

        let core_only = s
            .list(
                &r,
                &TraceQuery {
                    kinds: vec!["core.".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(core_only.len(), 2);

        let latest = s
            .list(
                &r,
                &TraceQuery {
                    latest: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].kind, "core.step_completed");

        let after = s
            .list(
                &r,
                &TraceQuery {
                    after: Some(1),
                    limit: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].seq, 2);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn retention_prunes_oldest_in_same_transaction() {
        let (s, driver) = SqliteRunTraceStore::open_in_memory().await.unwrap();
        let s = s.with_max_events_per_run(3);
        let r = rid("R-1");
        for i in 0..5 {
            s.append(&r, draft(&format!("core.e{i}"))).await.unwrap();
        }
        let all = s.list(&r, &TraceQuery::default()).await.unwrap();
        assert_eq!(all.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3, 4, 5]);
        let e6 = s.append(&r, draft("core.e5")).await.unwrap();
        assert_eq!(e6.seq, 6, "pruning never recycles seqs");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn delete_run_removes_only_that_run() {
        let (s, driver) = SqliteRunTraceStore::open_in_memory().await.unwrap();
        s.append(&rid("R-1"), draft("core.a")).await.unwrap();
        s.append(&rid("R-1"), draft("core.b")).await.unwrap();
        s.append(&rid("R-2"), draft("core.c")).await.unwrap();
        assert_eq!(s.delete_run(&rid("R-1")).await.unwrap(), 2);
        assert!(s
            .list(&rid("R-1"), &TraceQuery::default())
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            s.list(&rid("R-2"), &TraceQuery::default())
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(s.delete_run(&rid("R-nope")).await.unwrap(), 0);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.sqlite");
        {
            let (s, driver) = SqliteRunTraceStore::open(&path).await.unwrap();
            s.append(&rid("R-keep"), draft("core.run_started"))
                .await
                .unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }
        let (s, driver) = SqliteRunTraceStore::open(&path).await.unwrap();
        let got = s
            .list(&rid("R-keep"), &TraceQuery::default())
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, "core.run_started");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shares_file_with_run_store_tables() {
        // The design intent: run.sqlite carries BOTH the `runs` table
        // (SqliteRunStore) and `run_trace` (this store) — prove the two
        // stores coexist on one file without clobbering each other.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.sqlite");

        let (run_store, run_driver) = crate::store::run::SqliteRunStore::open(&path)
            .await
            .unwrap();
        let (trace_store, trace_driver) = SqliteRunTraceStore::open(&path).await.unwrap();

        let record = crate::store::run::RunRecord {
            id: rid("R-1"),
            task_id: crate::types::TaskId::parse("T-1").unwrap(),
            status: crate::store::run::RunStatus::Pending,
            step_entries: vec![],
            degradations: vec![],
            operator_sid: None,
            current: Default::default(),
            next_generation: 0,
            result_ref: None,
            input_json: None,
            created_at: 1,
            updated_at: 1,
        };
        use crate::store::run::RunStore as _;
        run_store.create(record).await.unwrap();
        trace_store
            .append(&rid("R-1"), draft("core.run_started"))
            .await
            .unwrap();

        assert!(run_store.get(&rid("R-1")).await.is_ok());
        assert_eq!(
            trace_store
                .list(&rid("R-1"), &TraceQuery::default())
                .await
                .unwrap()
                .len(),
            1
        );

        drop(run_store);
        drop(trace_store);
        run_driver.shutdown().await.unwrap();
        trace_driver.shutdown().await.unwrap();
    }
}
