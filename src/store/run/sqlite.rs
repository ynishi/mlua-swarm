//! `SqliteRunStore` — SQLite-backed [`RunStore`] using [`rusqlite-isle`].
//!
//! The `Connection` is confined to a dedicated OS thread by `AsyncIsle`;
//! every call is a typed closure dispatched over a bounded channel.
//! `step_entries`, `degradations`, and `result_ref` are stored as JSON
//! blobs — the former two are pure trace/observability artifacts (not
//! queried relationally), the latter is caller-defined payload shape.
//! `append_step_entry`/`append_degradation` run as a read-modify-write
//! inside a single transaction so concurrent appenders don't clobber each
//! other's entries.
//!
//! ## Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS runs (
//!   id                 TEXT PRIMARY KEY,
//!   task_id            TEXT NOT NULL,
//!   status             TEXT NOT NULL,      -- JSON-encoded `RunStatus`
//!   step_entries_json  TEXT NOT NULL,      -- JSON-encoded `Vec<StepEntry>`
//!   degradations_json  TEXT NOT NULL DEFAULT '[]', -- JSON-encoded `Vec<DegradationEntry>` (GH #32)
//!   operator_sid       TEXT,
//!   current_json       TEXT,               -- JSON object: slot -> `Assignee`, NULL when no slot is held
//!   next_generation    INTEGER NOT NULL DEFAULT 0, -- the model's `G`
//!   result_ref_json    TEXT,               -- JSON-encoded `serde_json::Value`, NULL when unset
//!   input_json         TEXT,               -- opaque launch-input snapshot for resume, NULL when unset
//!   created_at         INTEGER NOT NULL,
//!   updated_at         INTEGER NOT NULL
//! );
//! CREATE INDEX IF NOT EXISTS ix_runs_task_id ON runs(task_id, created_at);
//! ```
//!
//! `degradations_json` (GH #32), `input_json` (the resume launch-input
//! snapshot) and the assignment pair `current_json` / `next_generation`
//! were all added after the initial release; each migration is applied
//! idempotently on open via a `PRAGMA table_info(runs)` existence check
//! followed by the matching `ALTER TABLE runs ADD COLUMN …` when missing,
//! so pre-existing database files pick up the columns without a manual
//! migration step. `input_json` and `current_json` are nullable `TEXT` (no
//! default) — rows written before those features read back `None`
//! (`current_json` `NULL` = no slot held); `next_generation` carries
//! `DEFAULT 0` so a back-filled row starts at the launch value of `G`.
//!
//! `current_json` holds the whole `slot -> Assignee` map as one JSON
//! object, not one row per slot: the map is read and rewritten whole on
//! every assignment event anyway (the event has to bump the sibling
//! `next_generation` in the same transaction), and nothing queries a Run
//! *by* who holds one of its slots. A map that has gone empty is stored
//! back as SQL `NULL`, so "no slot held" has exactly one representation on
//! disk.
//!
//! `acquire_assignee`/`vacate_assignee` bump `next_generation` and rewrite
//! `current_json` as a read-modify-write inside one `Immediate`
//! transaction — the same shape as `append_step_entry`, and for the same
//! reason: the increment-and-stamp spans two columns, which a conditional
//! `UPDATE` (the `try_transition` compare-and-set) cannot express. Two
//! acquires naming *different* slots take the same path, so the map merge
//! is serialized too and neither can drop the other's entry.

use super::{
    Assignee, DegradationEntry, RunId, RunListFilter, RunRecord, RunStatus, RunStore,
    RunStoreError, StepEntry, TaskId,
};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::collections::BTreeMap;
use std::path::Path;

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS runs (\
  id                 TEXT PRIMARY KEY, \
  task_id            TEXT NOT NULL, \
  status             TEXT NOT NULL, \
  step_entries_json  TEXT NOT NULL, \
  degradations_json  TEXT NOT NULL DEFAULT '[]', \
  operator_sid       TEXT, \
  current_json       TEXT, \
  next_generation    INTEGER NOT NULL DEFAULT 0, \
  result_ref_json    TEXT, \
  input_json         TEXT, \
  created_at         INTEGER NOT NULL, \
  updated_at         INTEGER NOT NULL\
);\
CREATE INDEX IF NOT EXISTS ix_runs_task_id ON runs(task_id, created_at);\
";

/// Idempotently ensures a nullable column named `column` exists on `runs`,
/// adding it via `ALTER TABLE … ADD COLUMN <column> <decl>` when a
/// pre-existing database file was created before the column was introduced.
/// Fresh databases get every column from [`SCHEMA_SQL`] directly; this only
/// fires the `ALTER TABLE` on older files missing it.
fn migrate_add_column_if_missing(
    conn: &rusqlite::Connection,
    column: &str,
    decl: &str,
) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(runs)")?;
    let has_column = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<String>, _>>()?
        .iter()
        .any(|name| name == column);
    if !has_column {
        conn.execute_batch(&format!("ALTER TABLE runs ADD COLUMN {column} {decl};"))?;
    }
    Ok(())
}

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
            // The trace store (`SqliteRunTraceStore`) shares this file
            // from its own confined connection; a short busy wait
            // absorbs its write transactions instead of surfacing
            // SQLITE_BUSY here.
            conn.busy_timeout(std::time::Duration::from_millis(5_000))?;
            conn.execute_batch(SCHEMA_SQL)?;
            migrate_add_column_if_missing(conn, "degradations_json", "TEXT NOT NULL DEFAULT '[]'")?;
            migrate_add_column_if_missing(conn, "input_json", "TEXT")?;
            migrate_add_column_if_missing(conn, "current_json", "TEXT")?;
            migrate_add_column_if_missing(conn, "next_generation", "INTEGER NOT NULL DEFAULT 0")
        })
        .await
        .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests, doctests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), RunStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(|conn| {
            conn.busy_timeout(std::time::Duration::from_millis(5_000))?;
            conn.execute_batch(SCHEMA_SQL)?;
            migrate_add_column_if_missing(conn, "degradations_json", "TEXT NOT NULL DEFAULT '[]'")?;
            migrate_add_column_if_missing(conn, "input_json", "TEXT")?;
            migrate_add_column_if_missing(conn, "current_json", "TEXT")?;
            migrate_add_column_if_missing(conn, "next_generation", "INTEGER NOT NULL DEFAULT 0")
        })
        .await
        .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }

    /// Shared read-modify-write behind `acquire_assignee` / `vacate_assignee`
    /// — the model's single assignment event (§4.3 **A4**), scoped to one
    /// slot.
    ///
    /// `assign_to` selects the event: `Some((op, desc))` = `Assign`,
    /// `None` = `Vacant`. Both bump the generation counter identically;
    /// only `Assign` mints an [`Assignee`] to stamp it onto. Either way
    /// only `slot`'s entry in the decoded map is written or removed — the
    /// other slots' entries are re-encoded exactly as they were read.
    ///
    /// Returns `Ok(None)` when no row matched, which the callers lift to
    /// [`RunStoreError::NotFound`].
    async fn record_assignment_event(
        &self,
        id: &RunId,
        slot: &str,
        assign_to: Option<(String, String)>,
    ) -> Result<Option<(u64, Option<Assignee>)>, RunStoreError> {
        let id_str = id.to_string();
        let slot = slot.to_string();
        let updated_at = crate::types::now_unix() as i64;

        self.isle
            .call(move |conn| {
                // Immediate — see `create`'s comment for the shared-file
                // busy-wait rationale. Beyond that, the read-increment-
                // stamp-write below MUST be one critical section: two
                // acquires that read the same `next_generation` would hand
                // out the same generation twice. A conditional UPDATE (the
                // `try_transition` compare-and-set) cannot express this,
                // because the new `current_json` depends on the value read
                // from the sibling column.
                let tx =
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let existing: Option<(Option<String>, i64)> = tx
                    .query_row(
                        "SELECT current_json, next_generation FROM runs WHERE id = ?1",
                        params![id_str],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                let Some((current_json, generation)) = existing else {
                    return Ok(None);
                };
                let mut current: BTreeMap<String, Assignee> = match current_json {
                    Some(text) => serde_json::from_str(&text)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    None => BTreeMap::new(),
                };
                // A4: unconditional — the counter counts events, so an
                // Assign to the incumbent and a Vacant on an already-vacant
                // slot both still advance it. It is one counter for the
                // whole Run: this bump is the same one an event on any
                // other slot would make.
                let generation = generation as u64 + 1;
                // Q3: a brand-new instance carries the new generation;
                // `previous` — the entry this slot held — is handed back to
                // the caller untouched.
                let previous = match assign_to {
                    Some((op, desc)) => current.insert(
                        slot.clone(),
                        Assignee {
                            op,
                            desc,
                            gen: generation,
                        },
                    ),
                    None => current.remove(&slot),
                };
                // An emptied map goes back as SQL NULL, matching both the
                // pre-assignment rows and a Run that has never been
                // assigned — one on-disk shape for "no slot held".
                let next_json = if current.is_empty() {
                    None
                } else {
                    Some(
                        serde_json::to_string(&current)
                            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    )
                };
                tx.execute(
                    "UPDATE runs SET current_json = ?1, next_generation = ?2, updated_at = ?3 \
                     WHERE id = ?4",
                    params![next_json, generation as i64, updated_at, id_str],
                )?;
                tx.commit()?;
                Ok(Some((generation, previous)))
            })
            .await
            .map_err(map_isle_err)
    }
}

fn map_isle_err(e: IsleError) -> RunStoreError {
    RunStoreError::Other(format!("sqlite: {e}"))
}

/// One `runs` SELECT row in column order: id, task_id, status,
/// step_entries_json, degradations_json, operator_sid, current_json,
/// next_generation, result_ref_json, input_json, created_at, updated_at.
///
/// Position-coupled with [`RUN_SELECT_COLUMNS`], [`row_to_record`] and
/// every `query_map` closure below — all of them move together.
type RunRow = (
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    i64,
    Option<String>,
    Option<String>,
    i64,
    i64,
);

const RUN_SELECT_COLUMNS: &str = "id, task_id, status, step_entries_json, degradations_json, \
     operator_sid, current_json, next_generation, result_ref_json, input_json, created_at, \
     updated_at";

/// Read one `runs` row into [`RunRow`] positionally. Every `SELECT
/// {RUN_SELECT_COLUMNS}` in this file goes through here, so the column
/// order lives in exactly two places (the const and this function) instead
/// of once per query.
fn read_run_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRow> {
    Ok((
        row.get::<_, String>(0)?,
        row.get::<_, String>(1)?,
        row.get::<_, String>(2)?,
        row.get::<_, String>(3)?,
        row.get::<_, String>(4)?,
        row.get::<_, Option<String>>(5)?,
        row.get::<_, Option<String>>(6)?,
        row.get::<_, i64>(7)?,
        row.get::<_, Option<String>>(8)?,
        row.get::<_, Option<String>>(9)?,
        row.get::<_, i64>(10)?,
        row.get::<_, i64>(11)?,
    ))
}

fn row_to_record(row: RunRow) -> Result<RunRecord, RunStoreError> {
    let (
        id,
        task_id,
        status_json,
        step_entries_json,
        degradations_json,
        operator_sid,
        current_json,
        next_generation,
        result_ref_json,
        input_json,
        created_at,
        updated_at,
    ) = row;
    let status: RunStatus = serde_json::from_str(&status_json)
        .map_err(|e| RunStoreError::Other(format!("decode status: {e}")))?;
    let step_entries: Vec<StepEntry> = serde_json::from_str(&step_entries_json)
        .map_err(|e| RunStoreError::Other(format!("decode step_entries: {e}")))?;
    let degradations: Vec<DegradationEntry> = serde_json::from_str(&degradations_json)
        .map_err(|e| RunStoreError::Other(format!("decode degradations: {e}")))?;
    // A NULL `current_json` is a Run with no slot held — a legitimate
    // state, not a decode failure. A non-NULL value that is not a
    // `slot -> Assignee` object IS a failure and is surfaced: silently
    // reading it back as "no slot held" would turn a corrupt (or
    // wrong-shaped) column into an apparently unassigned Run, and a
    // dispatch would then be routed nowhere with no explanation.
    let current: BTreeMap<String, Assignee> = match current_json {
        Some(text) => serde_json::from_str(&text)
            .map_err(|e| RunStoreError::Other(format!("decode current: {e}")))?,
        None => BTreeMap::new(),
    };
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
        degradations,
        operator_sid,
        current,
        next_generation: next_generation as u64,
        result_ref,
        input_json,
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
        // **A2** on the way in — see `RunStore::create`. Checked before any
        // encoding so a rejected record never reaches the connection.
        record.validate_assignment_generations()?;
        let id = record.id.to_string();
        let id_for_conflict = record.id.clone();
        let task_id = record.task_id.to_string();
        let status_json = serde_json::to_string(&record.status)
            .map_err(|e| RunStoreError::Other(format!("encode status: {e}")))?;
        let step_entries_json = serde_json::to_string(&record.step_entries)
            .map_err(|e| RunStoreError::Other(format!("encode step_entries: {e}")))?;
        let degradations_json = serde_json::to_string(&record.degradations)
            .map_err(|e| RunStoreError::Other(format!("encode degradations: {e}")))?;
        let operator_sid = record.operator_sid.clone();
        // An empty map (no slot held) persists as SQL NULL rather than the
        // JSON literal `{}`, so `row_to_record` can read absence straight
        // off the column type and a never-assigned Run looks identical to a
        // pre-assignment row.
        let current_json = if record.current.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&record.current)
                    .map_err(|e| RunStoreError::Other(format!("encode current: {e}")))?,
            )
        };
        let next_generation = record.next_generation as i64;
        let result_ref_json = record
            .result_ref
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| RunStoreError::Other(format!("encode result_ref: {e}")))?;
        let input_json = record.input_json.clone();
        let created_at = record.created_at as i64;
        let updated_at = record.updated_at as i64;

        self.isle
            .call(move |conn| {
                // Immediate: the trace store shares this file from its own
                // connection; RESERVED-up-front keeps the busy wait
                // effective (a DEFERRED read-then-upgrade racing it gets
                // an instant SQLITE_BUSY instead).
                let tx =
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
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
                    "INSERT INTO runs (id, task_id, status, step_entries_json, \
                     degradations_json, operator_sid, current_json, next_generation, \
                     result_ref_json, input_json, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    params![
                        id,
                        task_id,
                        status_json,
                        step_entries_json,
                        degradations_json,
                        operator_sid,
                        current_json,
                        next_generation,
                        result_ref_json,
                        input_json,
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
                    &format!("SELECT {RUN_SELECT_COLUMNS} FROM runs WHERE id = ?1"),
                    params![id_str],
                    read_run_row,
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
                let mut stmt = conn.prepare(&format!(
                    "SELECT {RUN_SELECT_COLUMNS} FROM runs \
                     WHERE task_id = ?1 ORDER BY created_at ASC"
                ))?;
                let iter = stmt.query_map(params![task_id_str], read_run_row)?;
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
                // Immediate — see `create`'s comment (shared-file busy-wait
                // effectiveness).
                let tx =
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
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

    async fn append_degradation(
        &self,
        id: &RunId,
        entry: DegradationEntry,
    ) -> Result<(), RunStoreError> {
        let id_str = id.to_string();
        let id_for_notfound = id.clone();
        let updated_at = crate::types::now_unix() as i64;

        let updated = self
            .isle
            .call(move |conn| {
                // Immediate — see `create`'s comment (shared-file busy-wait
                // effectiveness).
                let tx =
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let existing: Option<String> = tx
                    .query_row(
                        "SELECT degradations_json FROM runs WHERE id = ?1",
                        params![id_str],
                        |row| row.get(0),
                    )
                    .optional()?;
                let Some(existing_json) = existing else {
                    return Ok(false);
                };
                let mut entries: Vec<DegradationEntry> = serde_json::from_str(&existing_json)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                entries.push(entry);
                let new_json = serde_json::to_string(&entries)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                tx.execute(
                    "UPDATE runs SET degradations_json = ?1, updated_at = ?2 WHERE id = ?3",
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

    async fn try_transition(
        &self,
        id: &RunId,
        from: RunStatus,
        to: RunStatus,
    ) -> Result<bool, RunStoreError> {
        let id_str = id.to_string();
        let from_json = serde_json::to_string(&from)
            .map_err(|e| RunStoreError::Other(format!("encode from status: {e}")))?;
        let to_json = serde_json::to_string(&to)
            .map_err(|e| RunStoreError::Other(format!("encode to status: {e}")))?;
        let updated_at = crate::types::now_unix() as i64;
        // A single conditional UPDATE is the compare-and-set: the `AND
        // status = ?from` predicate makes the read+set atomic at the SQLite
        // level, so two concurrent resumes cannot both flip the same row.
        // `rows_affected == 1` = we won the transition; `0` = the row was
        // absent or no longer `from` (a racing transition already won).
        let n = self
            .isle
            .call(move |conn| {
                conn.execute(
                    "UPDATE runs SET status = ?1, updated_at = ?2 WHERE id = ?3 AND status = ?4",
                    params![to_json, updated_at, id_str, from_json],
                )
            })
            .await
            .map_err(map_isle_err)?;
        Ok(n == 1)
    }

    async fn acquire_assignee(
        &self,
        id: &RunId,
        slot: &str,
        op: &str,
        desc: &str,
    ) -> Result<(u64, Option<Assignee>), RunStoreError> {
        // Refuse before touching the row — a Run must never come to hold an
        // unnamed assignment (A9) or one filed under no slot, and a
        // rejected acquire must not have burned a generation.
        if slot.is_empty() {
            return Err(RunStoreError::AssigneeSlotRequired);
        }
        if desc.trim().is_empty() {
            return Err(RunStoreError::AssigneeDescRequired);
        }
        // A8: no precondition on the slot's incumbent — whoever asks, wins.
        self.record_assignment_event(id, slot, Some((op.to_string(), desc.to_string())))
            .await?
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))
    }

    async fn vacate_assignee(
        &self,
        id: &RunId,
        slot: &str,
    ) -> Result<(u64, Option<Assignee>), RunStoreError> {
        if slot.is_empty() {
            return Err(RunStoreError::AssigneeSlotRequired);
        }
        self.record_assignment_event(id, slot, None)
            .await?
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))
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

    async fn set_input_json(&self, id: &RunId, input_json: String) -> Result<(), RunStoreError> {
        let id_str = id.to_string();
        let id_for_notfound = id.clone();
        let updated_at = crate::types::now_unix() as i64;
        let n = self
            .isle
            .call(move |conn| {
                conn.execute(
                    "UPDATE runs SET input_json = ?1, updated_at = ?2 WHERE id = ?3",
                    params![input_json, updated_at, id_str],
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
                let mut stmt = conn.prepare(&format!(
                    "SELECT {RUN_SELECT_COLUMNS} FROM runs WHERE status = ?1"
                ))?;
                let iter = stmt.query_map(params![status_json], read_run_row)?;
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

    async fn list(&self, filter: &RunListFilter) -> Result<Vec<RunRecord>, RunStoreError> {
        let task_id = filter.task_id.as_ref().map(|t| t.to_string());
        let status_json = filter
            .status
            .map(|s| serde_json::to_string(&s))
            .transpose()
            .map_err(|e| RunStoreError::Other(format!("encode status: {e}")))?;
        let limit = filter.limit.map(|l| l as i64).unwrap_or(-1);
        let offset = filter.offset.map(|o| o as i64).unwrap_or(0);
        let rows = self
            .isle
            .call(move |conn| {
                // `?1 IS NULL OR …` folds each optional filter into one
                // statement; `LIMIT -1` is SQLite's "no cap". `rowid`
                // breaks `created_at` ties newest-insertion-first.
                let mut stmt = conn.prepare(&format!(
                    "SELECT {RUN_SELECT_COLUMNS} FROM runs \
                     WHERE (?1 IS NULL OR task_id = ?1) \
                       AND (?2 IS NULL OR status = ?2) \
                     ORDER BY created_at DESC, rowid DESC \
                     LIMIT ?3 OFFSET ?4"
                ))?;
                let iter =
                    stmt.query_map(params![task_id, status_json, limit, offset], read_run_row)?;
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

    async fn delete(&self, id: &RunId) -> Result<(), RunStoreError> {
        let id_str = id.to_string();
        let id_for_notfound = id.clone();
        let n = self
            .isle
            .call(move |conn| conn.execute("DELETE FROM runs WHERE id = ?1", params![id_str]))
            .await
            .map_err(map_isle_err)?;
        if n == 0 {
            Err(RunStoreError::NotFound(id_for_notfound))
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

    fn mk(id: &str, task_id: &str, created_at: u64) -> RunRecord {
        RunRecord {
            id: RunId::parse(id).unwrap(),
            task_id: TaskId::parse(task_id).unwrap(),
            status: RunStatus::Pending,
            step_entries: vec![],
            degradations: vec![],
            operator_sid: None,
            current: Default::default(),
            next_generation: 0,
            result_ref: None,
            input_json: None,
            created_at,
            updated_at: created_at,
        }
    }

    fn mk_degradation(tool: &str, at: u64) -> DegradationEntry {
        DegradationEntry {
            tool: tool.to_string(),
            error: "boom".to_string(),
            fallback: "cached-default".to_string(),
            note: None,
            step_ref: Some("worker".to_string()),
            attempt: Some(1),
            at,
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

    /// **A2** at `create`, in parity with `InMemoryRunStore` — the check
    /// belongs to the trait contract, not to one backend, and it runs
    /// before any encoding so the connection never sees the row.
    #[tokio::test]
    async fn create_rejects_a_holder_generation_above_the_counter() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        let mut record = mk("R-1", "T-1", 100);
        record.current.insert(
            SLOT_A.to_string(),
            Assignee {
                op: "S-seeded".to_string(),
                desc: "seeded straight into the record".to_string(),
                gen: 99,
            },
        );

        let err = s.create(record).await.unwrap_err();
        assert!(
            matches!(
                err,
                RunStoreError::AssigneeGenerationAhead {
                    gen: 99,
                    next_generation: 0,
                    ..
                }
            ),
            "got: {err:?}"
        );
        assert!(
            matches!(
                s.get(&RunId::parse("R-1").unwrap()).await.unwrap_err(),
                RunStoreError::NotFound(_)
            ),
            "a refused create must leave no row behind"
        );
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
            StepEntry::basic(
                crate::types::StepId::parse("ST-1").unwrap(),
                Some("step-a".into()),
                Some("dispatched".into()),
                None,
                101,
            ),
        )
        .await
        .unwrap();
        s.append_step_entry(
            &RunId::parse("R-1").unwrap(),
            StepEntry::basic(
                crate::types::StepId::parse("ST-2").unwrap(),
                Some("step-b".into()),
                Some("passed".into()),
                None,
                102,
            ),
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
                StepEntry::basic(
                    crate::types::StepId::parse("ST-1").unwrap(),
                    None,
                    None,
                    None,
                    1,
                ),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn append_degradation_accumulates_in_order() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.append_degradation(
            &RunId::parse("R-1").unwrap(),
            mk_degradation("web_search", 101),
        )
        .await
        .unwrap();
        s.append_degradation(
            &RunId::parse("R-1").unwrap(),
            mk_degradation("code_exec", 102),
        )
        .await
        .unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.degradations.len(), 2);
        assert_eq!(got.degradations[0].tool, "web_search");
        assert_eq!(got.degradations[1].tool, "code_exec");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn append_degradation_unknown_run_fails() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        let err = s
            .append_degradation(
                &RunId::parse("R-nope").unwrap(),
                mk_degradation("web_search", 1),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn append_degradation_bumps_updated_at() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.append_degradation(
            &RunId::parse("R-1").unwrap(),
            mk_degradation("web_search", 200),
        )
        .await
        .unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert!(got.updated_at > 100);
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
                StepEntry::basic(
                    crate::types::StepId::parse("ST-1").unwrap(),
                    Some("step-a".into()),
                    Some("dispatched".into()),
                    None,
                    43,
                ),
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

    #[tokio::test]
    async fn try_transition_is_atomic_compare_and_set() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.update_status(&RunId::parse("R-1").unwrap(), RunStatus::Interrupted)
            .await
            .unwrap();

        let first = s
            .try_transition(
                &RunId::parse("R-1").unwrap(),
                RunStatus::Interrupted,
                RunStatus::Running,
            )
            .await
            .unwrap();
        assert!(first, "first CAS must flip Interrupted -> Running");
        assert_eq!(
            s.get(&RunId::parse("R-1").unwrap()).await.unwrap().status,
            RunStatus::Running
        );

        let second = s
            .try_transition(
                &RunId::parse("R-1").unwrap(),
                RunStatus::Interrupted,
                RunStatus::Running,
            )
            .await
            .unwrap();
        assert!(
            !second,
            "a racing second CAS must not flip a now-Running row"
        );

        let absent = s
            .try_transition(
                &RunId::parse("R-nope").unwrap(),
                RunStatus::Interrupted,
                RunStatus::Running,
            )
            .await
            .unwrap();
        assert!(!absent, "an absent Run must report false, not error");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    // ── assignment axis (model §4.3) ──────────────────────────────────

    /// The two slots (Blueprint-declared Operator seats) the tests below
    /// assign to — the shipped per-lane alias shape, where a Blueprint
    /// declares one seat per phase.
    const SLOT_A: &str = "phase-a-op";
    const SLOT_B: &str = "phase-b-op";

    /// A4: a launched Run starts with every slot Vacant and `G == 0`, and
    /// both columns round-trip through the row decode.
    #[tokio::test]
    async fn launch_starts_vacant_at_generation_zero() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert!(got.current.is_empty(), "no slot is held at launch");
        assert_eq!(got.next_generation, 0);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// A4: every event advances `G` by one and the FIRST Assign lands on
    /// `1`. A8: re-acquiring for the incumbent still advances it.
    #[tokio::test]
    async fn acquire_advances_generation_even_for_the_same_op() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let id = RunId::parse("R-1").unwrap();

        let (gen, previous) = s
            .acquire_assignee(&id, SLOT_A, "S-a1", "first hold")
            .await
            .unwrap();
        assert_eq!(gen, 1, "the first Assign stamps generation 1");
        assert_eq!(previous, None);

        let (gen, previous) = s
            .acquire_assignee(&id, SLOT_A, "S-a1", "same holder, new event")
            .await
            .unwrap();
        assert_eq!(gen, 2, "A4: a repeat Assign for the same op still bumps");
        assert_eq!(previous.expect("displaced holder").gen, 1);

        let got = s.get(&id).await.unwrap();
        assert_eq!(got.next_generation, 2);
        assert_eq!(
            got.current.len(),
            1,
            "A1: re-assigning a slot leaves it with exactly one holder"
        );
        let holder = &got.current[SLOT_A];
        assert_eq!(holder.gen, 2);
        assert_eq!(holder.desc, "same holder, new event");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// The slots are independent through the column: writing one slot
    /// re-encodes the other's entry untouched instead of replacing the map.
    #[tokio::test]
    async fn assigning_one_slot_leaves_the_others_intact() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let id = RunId::parse("R-1").unwrap();

        s.acquire_assignee(&id, SLOT_A, "S-a1", "holds phase a")
            .await
            .unwrap();
        let got = s.get(&id).await.unwrap();
        assert!(
            !got.current.contains_key(SLOT_B),
            "an unassigned slot has no entry — that absence IS its Vacant"
        );

        s.acquire_assignee(&id, SLOT_B, "S-b2", "holds phase b")
            .await
            .unwrap();
        let got = s.get(&id).await.unwrap();
        assert_eq!(got.current[SLOT_A].op, "S-a1", "the first seat survived");
        assert_eq!(got.current[SLOT_B].op, "S-b2");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// A4 is Run-wide, not per slot: interleaved assignments to two slots
    /// walk ONE counter, so any two holders can be ordered by `gen`.
    #[tokio::test]
    async fn the_generation_counter_is_shared_across_slots() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let id = RunId::parse("R-1").unwrap();

        let (first, _) = s
            .acquire_assignee(&id, SLOT_A, "S-a1", "holds phase a")
            .await
            .unwrap();
        let (second, _) = s
            .acquire_assignee(&id, SLOT_B, "S-b2", "holds phase b")
            .await
            .unwrap();
        let (third, _) = s
            .acquire_assignee(&id, SLOT_A, "S-a3", "takes over phase a")
            .await
            .unwrap();

        assert_eq!(
            (first, second, third),
            (1, 2, 3),
            "a second slot does not start its own counter at 1"
        );

        let got = s.get(&id).await.unwrap();
        assert_eq!(got.next_generation, 3);
        assert_eq!(got.current[SLOT_A].gen, 3);
        assert_eq!(got.current[SLOT_B].gen, 2);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// A4 (Vacant side): releasing bumps `G` too; the next Assign continues
    /// from the bumped value.
    #[tokio::test]
    async fn vacate_advances_generation_and_clears_the_holder() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let id = RunId::parse("R-1").unwrap();

        s.acquire_assignee(&id, SLOT_A, "S-a1", "first hold")
            .await
            .unwrap();
        let (gen, released) = s.vacate_assignee(&id, SLOT_A).await.unwrap();
        assert_eq!(gen, 2, "A4: Vacant is an event and advances G");
        assert_eq!(released.expect("released holder").op, "S-a1");

        let got = s.get(&id).await.unwrap();
        assert!(
            !got.current.contains_key(SLOT_A),
            "R2: the Run stays, the holder does not"
        );
        assert_eq!(got.next_generation, 2);

        let (gen, previous) = s
            .acquire_assignee(&id, SLOT_A, "S-b2", "after release")
            .await
            .unwrap();
        assert_eq!(gen, 3, "the next Assign continues from the bumped counter");
        assert_eq!(
            previous, None,
            "nothing was displaced — the slot was Vacant"
        );
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// A Vacant applies to the named slot only — the other seats keep the
    /// holders they had, across the column round-trip.
    #[tokio::test]
    async fn vacate_releases_only_the_named_slot() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let id = RunId::parse("R-1").unwrap();

        s.acquire_assignee(&id, SLOT_A, "S-a1", "holds phase a")
            .await
            .unwrap();
        s.acquire_assignee(&id, SLOT_B, "S-b2", "holds phase b")
            .await
            .unwrap();

        let (_, released) = s.vacate_assignee(&id, SLOT_A).await.unwrap();
        assert_eq!(released.expect("released holder").op, "S-a1");

        let got = s.get(&id).await.unwrap();
        assert!(!got.current.contains_key(SLOT_A));
        assert_eq!(
            got.current[SLOT_B].op, "S-b2",
            "vacating one seat must not empty another"
        );
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// A3 / Q3: an acquire mints a NEW `Assignee` and returns the displaced
    /// one with its original stamp intact.
    #[tokio::test]
    async fn acquire_never_rewrites_the_incumbent_assignee() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let id = RunId::parse("R-1").unwrap();

        s.acquire_assignee(&id, SLOT_A, "S-a1", "first hold")
            .await
            .unwrap();
        let held_before = s.get(&id).await.unwrap().current[SLOT_A].clone();
        assert_eq!(held_before.gen, 1);

        let (_, displaced) = s
            .acquire_assignee(&id, SLOT_A, "S-b2", "takeover")
            .await
            .unwrap();

        assert_eq!(
            held_before.gen, 1,
            "A3: gen is immutable for the lifetime of an instance"
        );
        assert_eq!(
            displaced.expect("displaced holder"),
            held_before,
            "Q3: the displaced instance is returned as-is, not mutated"
        );
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// A8: acquire has no precondition on the slot's incumbent — last
    /// writer wins.
    #[tokio::test]
    async fn acquire_displaces_a_live_holder() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let id = RunId::parse("R-1").unwrap();

        s.acquire_assignee(&id, SLOT_A, "S-a1", "first hold")
            .await
            .unwrap();
        let (gen, displaced) = s
            .acquire_assignee(&id, SLOT_A, "S-b2", "takeover")
            .await
            .unwrap();

        assert_eq!(gen, 2);
        assert_eq!(displaced.expect("displaced holder").op, "S-a1");
        let got = s.get(&id).await.unwrap();
        assert_eq!(got.current[SLOT_A].op, "S-b2");
        assert_eq!(got.current.len(), 1, "A1: still one holder for that slot");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// A9: `desc` is mandatory, and so is the slot; a refused event leaves
    /// the row alone.
    #[tokio::test]
    async fn acquire_rejects_a_missing_desc_without_side_effects() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let id = RunId::parse("R-1").unwrap();
        s.acquire_assignee(&id, SLOT_A, "S-a1", "first hold")
            .await
            .unwrap();

        for blank in ["", "   "] {
            let err = s
                .acquire_assignee(&id, SLOT_A, "S-b2", blank)
                .await
                .unwrap_err();
            assert!(
                matches!(err, RunStoreError::AssigneeDescRequired),
                "got: {err:?}"
            );
        }

        let err = s
            .acquire_assignee(&id, "", "S-b2", "no slot named")
            .await
            .unwrap_err();
        assert!(
            matches!(err, RunStoreError::AssigneeSlotRequired),
            "got: {err:?}"
        );
        let err = s.vacate_assignee(&id, "").await.unwrap_err();
        assert!(
            matches!(err, RunStoreError::AssigneeSlotRequired),
            "got: {err:?}"
        );

        let got = s.get(&id).await.unwrap();
        assert_eq!(got.next_generation, 1, "a refused event is not an event");
        assert_eq!(got.current[SLOT_A].op, "S-a1");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// Concurrent acquires run as separate `Immediate` transactions and
    /// must never read the same `G` — nor drop each other's slot entry,
    /// since each rewrites the whole map.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_acquires_hand_out_distinct_generations() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        let s = std::sync::Arc::new(s);
        s.create(mk("R-1", "T-1", 100)).await.unwrap();

        let mut handles = Vec::new();
        for i in 0..8u32 {
            let s = s.clone();
            let slot = if i % 2 == 0 { SLOT_A } else { SLOT_B };
            handles.push(tokio::spawn(async move {
                s.acquire_assignee(
                    &RunId::parse("R-1").unwrap(),
                    slot,
                    &format!("S-{i}"),
                    "concurrent hold",
                )
                .await
                .unwrap()
                .0
            }));
        }
        let mut generations = Vec::new();
        for h in handles {
            generations.push(h.await.unwrap());
        }
        generations.sort_unstable();
        assert_eq!(generations, (1..=8).collect::<Vec<u64>>());
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.next_generation, 8);
        assert_eq!(
            got.current.len(),
            2,
            "both slots ended up held — no writer clobbered the other's entry"
        );
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn assignment_on_an_unknown_run_fails() {
        let (s, driver) = SqliteRunStore::open_in_memory().await.unwrap();
        let missing = RunId::parse("R-nope").unwrap();
        let err = s
            .acquire_assignee(&missing, SLOT_A, "S-a1", "hold")
            .await
            .unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)), "got: {err:?}");
        let err = s.vacate_assignee(&missing, SLOT_A).await.unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)), "got: {err:?}");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// R6: a restart does not drop the assignments — every slot's holder
    /// and `G` come back with the Run.
    #[tokio::test]
    async fn assignment_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runs.db");

        {
            let (s, driver) = SqliteRunStore::open(&path).await.unwrap();
            s.create(mk("R-keep", "T-keep", 42)).await.unwrap();
            let id = RunId::parse("R-keep").unwrap();
            s.acquire_assignee(&id, SLOT_A, "main-ai", "held at restart")
                .await
                .unwrap();
            s.acquire_assignee(&id, SLOT_B, "S-b2", "also held at restart")
                .await
                .unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }

        let (s, driver) = SqliteRunStore::open(&path).await.unwrap();
        let got = s.get(&RunId::parse("R-keep").unwrap()).await.unwrap();
        let holder = &got.current[SLOT_A];
        assert_eq!(holder.op, "main-ai");
        assert_eq!(holder.desc, "held at restart");
        assert_eq!(holder.gen, 1);
        assert_eq!(
            got.current[SLOT_B].gen, 2,
            "the second seat survives with its own stamp"
        );
        assert_eq!(got.next_generation, 2);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// The pre-correction `current_json` shape — a bare single `Assignee`
    /// object, from before `current` became a per-slot map — is refused
    /// loudly rather than read back as "no slot held".
    ///
    /// No released build ever wrote that shape (the single-holder form was
    /// never committed), so this is not a migration path; it is the
    /// assertion that a `current_json` the decoder does not understand
    /// surfaces as an error instead of silently unassigning a Run.
    #[tokio::test]
    async fn a_pre_slot_current_json_fails_loud_rather_than_reading_vacant() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runs.db");

        {
            let (s, driver) = SqliteRunStore::open(&path).await.unwrap();
            s.create(mk("R-legacy", "T-legacy", 7)).await.unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "UPDATE runs SET current_json = ?1, next_generation = 1 WHERE id = 'R-legacy'",
                params![r#"{"op":"main-ai","desc":"held","gen":1}"#],
            )
            .unwrap();
        }

        let (s, driver) = SqliteRunStore::open(&path).await.unwrap();
        let err = s.get(&RunId::parse("R-legacy").unwrap()).await.unwrap_err();
        assert!(
            matches!(&err, RunStoreError::Other(msg) if msg.contains("decode current")),
            "got: {err:?}"
        );
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// A database file written before the assignment columns existed opens
    /// cleanly: the migration adds both, and the pre-existing row reads
    /// back as Vacant at generation 0.
    #[tokio::test]
    async fn legacy_db_without_assignment_columns_migrates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runs.db");

        // The `runs` shape as of the release before this axis landed.
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE runs (\
                   id                 TEXT PRIMARY KEY, \
                   task_id            TEXT NOT NULL, \
                   status             TEXT NOT NULL, \
                   step_entries_json  TEXT NOT NULL, \
                   degradations_json  TEXT NOT NULL DEFAULT '[]', \
                   operator_sid       TEXT, \
                   result_ref_json    TEXT, \
                   input_json         TEXT, \
                   created_at         INTEGER NOT NULL, \
                   updated_at         INTEGER NOT NULL\
                 );",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO runs (id, task_id, status, step_entries_json, degradations_json, \
                 operator_sid, result_ref_json, input_json, created_at, updated_at) \
                 VALUES ('R-old', 'T-old', '\"pending\"', '[]', '[]', NULL, NULL, NULL, 7, 7)",
                [],
            )
            .unwrap();
        }

        let (s, driver) = SqliteRunStore::open(&path).await.unwrap();
        let got = s.get(&RunId::parse("R-old").unwrap()).await.unwrap();
        assert!(
            got.current.is_empty(),
            "a pre-existing row reads back with no slot held"
        );
        assert_eq!(
            got.next_generation, 0,
            "and starts at the launch value of G"
        );
        assert_eq!(got.created_at, 7);

        // The migrated columns are writable, not just readable.
        let (gen, _) = s
            .acquire_assignee(
                &RunId::parse("R-old").unwrap(),
                SLOT_A,
                "S-a1",
                "after migration",
            )
            .await
            .unwrap();
        assert_eq!(gen, 1);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn input_json_roundtrips_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runs.db");
        let snapshot = r#"{"blueprint":"snapshot","init_ctx":{}}"#;

        {
            let (s, driver) = SqliteRunStore::open(&path).await.unwrap();
            let mut rec = mk("R-keep", "T-keep", 42);
            rec.input_json = Some(snapshot.to_string());
            s.create(rec).await.unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }

        let (s, driver) = SqliteRunStore::open(&path).await.unwrap();
        let got = s.get(&RunId::parse("R-keep").unwrap()).await.unwrap();
        assert_eq!(got.input_json.as_deref(), Some(snapshot));
        drop(s);
        driver.shutdown().await.unwrap();
    }
}
