//! `SqliteOperatorSessionStore` — SQLite-backed [`OperatorSessionStore`]
//! using [`rusqlite-isle`].
//!
//! The `Connection` is confined to a dedicated OS thread by `AsyncIsle`;
//! every call is a typed closure dispatched over a bounded channel.
//! `capability_manifest` and the 記名's observed log are stored as JSON
//! blobs — neither is queried relationally; the boot-time `list()`
//! rehydration decodes them back into their Rust shapes.
//!
//! ## The file holds no bearer secret
//!
//! `token_digest` is `hex(SHA-256(bearer))`, never the bearer itself (see
//! [`OperatorSessionRecord`]'s type doc). Two further measures back that up:
//!
//! - the file is `chmod 0600` on unix ([`harden_file_permissions`]) —
//!   best-effort, and skipped entirely on other platforms;
//! - a pre-release database carrying the old plaintext `token` column is
//!   **dropped and recreated** on open ([`purge_legacy_plaintext_table`])
//!   rather than migrated, so no plaintext residue survives the upgrade.
//!   The cost is one forced re-login for sessions minted by a dev build.
//!
//! ## Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS operator_sessions (
//!   sid                       TEXT PRIMARY KEY,
//!   token_digest              TEXT NOT NULL,  -- hex(SHA-256(bearer)), never the bearer
//!   capability_manifest_json  TEXT,           -- JSON-encoded manifest, NULL when unset
//!   joined_at_secs            INTEGER NOT NULL,
//!   join_desc                 TEXT,           -- 記名 confirmed part (D1), NULL when unwritten
//!   observed_json             TEXT,           -- 記名 observed part (D2), JSON array
//!   observed_total            INTEGER NOT NULL DEFAULT 0,
//!   last_access_secs          INTEGER NOT NULL DEFAULT 0  -- O1's expiry clock
//! );
//! ```
//!
//! The 記名 columns (model §4.2) and `last_access_secs` (the 24h horizon) all
//! arrived after the table did, and are added to an older file the same
//! way the `runs` table grows a column — by
//! [`migrate_add_column_if_missing`], except for `last_access_secs`, whose
//! back-fill matters enough to have its own migration
//! ([`migrate_add_last_access_column`]): a plain `DEFAULT 0` would make
//! the first read after an upgrade expire every carried-over session.
//!
//! A column also *left*: `roles_json` held the role aliases a session
//! claimed at join, back when a join claimed any. Role declaration moved
//! onto the Run, so an older file has the column dropped on open by
//! [`migrate_drop_column_if_present`] — see that function for why the
//! column cannot simply be ignored.

use super::{
    ObservedAssignment, OperatorSessionRecord, OperatorSessionStore, OperatorSessionStoreError,
    SessionId,
};
use crate::AgentProviderManifest;
use async_trait::async_trait;
use rusqlite::params;
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::path::Path;

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS operator_sessions (\
  sid                       TEXT PRIMARY KEY, \
  token_digest              TEXT NOT NULL, \
  capability_manifest_json  TEXT, \
  joined_at_secs            INTEGER NOT NULL, \
  join_desc                 TEXT, \
  observed_json             TEXT, \
  observed_total            INTEGER NOT NULL DEFAULT 0, \
  last_access_secs          INTEGER NOT NULL DEFAULT 0\
);\
";

/// Idempotently ensure a column exists on `operator_sessions`, adding it
/// via `ALTER TABLE … ADD COLUMN` when the file predates it.
///
/// Same shape as `crate::store::run::sqlite`'s namesake, and the same
/// reason: [`SCHEMA_SQL`] only runs `CREATE TABLE IF NOT EXISTS`, so a file
/// created before a column existed never gains it otherwise. Unlike
/// [`purge_legacy_plaintext_table`] this migrates rather than drops — these
/// columns hold no secret and an existing session losing its 記名 would be
/// a session nobody can identify, which is the opposite of the point.
fn migrate_add_column_if_missing(
    conn: &rusqlite::Connection,
    column: &str,
    decl: &str,
) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(operator_sessions)")?;
    let has_column = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<String>, _>>()?
        .iter()
        .any(|name| name == column);
    if !has_column {
        conn.execute_batch(&format!(
            "ALTER TABLE operator_sessions ADD COLUMN {column} {decl};"
        ))?;
    }
    Ok(())
}

/// Drop `column` from `operator_sessions` when a file predates its removal.
///
/// The mirror image of [`migrate_add_column_if_missing`], and needed for a
/// reason that one does not have: a column this build no longer writes is
/// not merely untidy when it is `NOT NULL` with no default. `roles_json`
/// was declared exactly that way, so leaving it in place would make every
/// `INSERT` from this build fail the constraint on any file created before
/// the removal — the sessions would stop persisting, and only on upgraded
/// installs.
///
/// Dropping rather than back-filling a placeholder: the value would be a
/// fiction (this build has no roles to write), and a fiction in a column
/// nothing reads is the kind of residue the next reader has to work out
/// the meaning of. `ALTER TABLE … DROP COLUMN` needs SQLite ≥ 3.35, which
/// the bundled `libsqlite3-sys` is well past.
fn migrate_drop_column_if_present(
    conn: &rusqlite::Connection,
    column: &str,
) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(operator_sessions)")?;
    let has_column = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<String>, _>>()?
        .iter()
        .any(|name| name == column);
    if has_column {
        conn.execute_batch(&format!(
            "ALTER TABLE operator_sessions DROP COLUMN {column};"
        ))?;
    }
    Ok(())
}

/// Add `last_access_secs` (the 24h expiry clock) to a file that predates
/// it, and stamp the existing rows with the moment of the upgrade.
///
/// [`migrate_add_column_if_missing`] alone would leave every pre-existing
/// row at the column default, `0` — the epoch — and the first `list()`
/// after the upgrade would judge each of them 56 years idle and delete the
/// lot. That is the wrong reading of a missing value: the rows are not
/// evidence of a session nobody has touched since 1970, they are evidence
/// of a build that did not record touches. There is no access history to
/// recover, so the honest substitute is the last moment we know the server
/// was running with these sessions in it, which is now.
///
/// Each carried-over session therefore gets a full horizon from the
/// upgrade, and one that really is abandoned expires a day later. The
/// back-fill runs only in the branch that adds the column, so a subsequent
/// open — where a `0` would mean a row genuinely written with no access —
/// leaves the values alone.
fn migrate_add_last_access_column(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(operator_sessions)")?;
    let has_column = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<String>, _>>()?
        .iter()
        .any(|name| name == "last_access_secs");
    drop(stmt);
    if has_column {
        return Ok(());
    }
    conn.execute_batch(
        "ALTER TABLE operator_sessions ADD COLUMN last_access_secs INTEGER NOT NULL DEFAULT 0;",
    )?;
    let now = super::expiry_now() as i64;
    let stamped = conn.execute(
        "UPDATE operator_sessions SET last_access_secs = ?1",
        params![now],
    )?;
    if stamped > 0 {
        tracing::info!(
            sessions = stamped,
            "operator session store: added O1's last-access column; the sessions already in \
             the file are stamped with this upgrade, so each gets a full 24h from here"
        );
    }
    Ok(())
}

/// The open-time schema work, shared by the file and in-memory
/// constructors so the two can never drift apart.
fn init_schema(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    conn.busy_timeout(std::time::Duration::from_millis(5_000))?;
    purge_legacy_plaintext_table(conn)?;
    conn.execute_batch(SCHEMA_SQL)?;
    migrate_add_column_if_missing(conn, "join_desc", "TEXT")?;
    migrate_add_column_if_missing(conn, "observed_json", "TEXT")?;
    migrate_add_column_if_missing(conn, "observed_total", "INTEGER NOT NULL DEFAULT 0")?;
    migrate_add_last_access_column(conn)?;
    // Last, and after the adds: a file from before the 記名 columns is
    // brought up to the current shape first, then loses the one column the
    // current shape does not have.
    migrate_drop_column_if_present(conn, "roles_json")
}

/// Drop a pre-release `operator_sessions` table that still carries the
/// plaintext `token` column, so the upgrade leaves no bearer on disk.
///
/// Deliberately a drop rather than an `ALTER TABLE` migration: digesting
/// the existing values in place would rewrite the rows but leave the
/// plaintext recoverable from freed pages, and this column shape only ever
/// existed in unreleased builds. Losing the rows costs one re-login.
///
/// Runs before [`SCHEMA_SQL`], which then recreates the table in the
/// current shape. A file that never had the legacy column (fresh, or
/// already upgraded) is untouched.
fn purge_legacy_plaintext_table(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(operator_sessions)")?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<String>, _>>()?;
    if columns.iter().any(|name| name == "token") {
        tracing::warn!(
            "operator session store: dropping a pre-release table that stored bearer \
             tokens in plaintext; sessions it held are cleared and must re-login"
        );
        conn.execute_batch("DROP TABLE operator_sessions;")?;
    }
    Ok(())
}

/// Restrict the database file to owner-only access (`0600`) on unix.
///
/// Best-effort defence in depth: the file already holds digests rather
/// than bearers, so a failure here is logged and swallowed instead of
/// failing the open. Without it the mode is whatever the process umask
/// yields — commonly `0644`, i.e. world-readable on a shared host.
///
/// `#[cfg(unix)]`-gated, and a no-op elsewhere: the unconditional use of a
/// unix-only API is exactly what broke the v0.1.1 Windows build.
fn harden_file_permissions(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(error) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            tracing::warn!(
                path = %path.display(),
                %error,
                "operator session store: could not restrict file permissions to 0600"
            );
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

/// SQLite-backed persistent [`OperatorSessionStore`].
///
/// Open with [`SqliteOperatorSessionStore::open`] (file path) or
/// [`SqliteOperatorSessionStore::open_in_memory`] (tests). Both return the
/// store plus an [`AsyncIsleDriver`] the caller must `shutdown().await`
/// when done — same contract as every sibling sqlite store.
pub struct SqliteOperatorSessionStore {
    isle: AsyncIsle,
}

impl SqliteOperatorSessionStore {
    /// Open (or create) a SQLite database file and run the schema setup.
    ///
    /// Also purges a pre-release plaintext-token table
    /// ([`purge_legacy_plaintext_table`]) and restricts the file to `0600`
    /// on unix ([`harden_file_permissions`]).
    pub async fn open(
        path: impl AsRef<Path>,
    ) -> Result<(Self, AsyncIsleDriver), OperatorSessionStoreError> {
        let path = path.as_ref().to_path_buf();
        let (isle, driver) = AsyncIsle::spawn(path.clone(), init_schema)
            .await
            .map_err(map_isle_err)?;
        // After the open: SQLite creates the file with umask-derived
        // permissions, so the tightening has to follow it.
        harden_file_permissions(&path);
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests, doctests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), OperatorSessionStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(init_schema)
            .await
            .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }
}

fn map_isle_err(e: IsleError) -> OperatorSessionStoreError {
    OperatorSessionStoreError::Other(format!("sqlite: {e}"))
}

/// One `operator_sessions` SELECT row in column order: sid, token_digest,
/// capability_manifest_json, joined_at_secs, join_desc, observed_json,
/// observed_total, last_access_secs.
type SessionRow = (
    String,
    String,
    Option<String>,
    i64,
    Option<String>,
    Option<String>,
    i64,
    i64,
);

const SESSION_SELECT_COLUMNS: &str = "sid, token_digest, capability_manifest_json, \
     joined_at_secs, join_desc, observed_json, observed_total, last_access_secs";

/// Why one `operator_sessions` row could not be turned into an
/// [`OperatorSessionRecord`].
///
/// Deliberately *not* an [`OperatorSessionStoreError`]: a bad row is not a
/// store failure, and conflating the two is what let a single row abort
/// [`OperatorSessionStore::list`] (and with it the boot that calls it).
/// This type exists so `list` can report the row and carry on.
struct RowDecodeError {
    /// The row's `sid` column verbatim — the only handle on a row whose sid
    /// is itself what failed to decode, so it is kept as the raw string.
    raw_sid: String,
    /// Which column's decode failed: `sid`, `capability_manifest`, or
    /// `observed`.
    column: &'static str,
    /// The decoder's own message.
    detail: String,
}

fn row_to_record(row: SessionRow) -> Result<OperatorSessionRecord, RowDecodeError> {
    let (
        raw_sid,
        token_digest,
        capability_manifest_json,
        joined_at_secs,
        desc,
        observed_json,
        observed_total,
        last_access_secs,
    ) = row;
    // All three decodes below fail the same way and get the same treatment:
    // no column is special-cased, because special-casing one only moves the
    // boot-stopper to the next.
    let fail = |column: &'static str, detail: String| RowDecodeError {
        raw_sid: raw_sid.clone(),
        column,
        detail,
    };
    let sid = SessionId::parse(raw_sid.clone()).map_err(|e| fail("sid", e.to_string()))?;
    let capability_manifest: Option<AgentProviderManifest> = match capability_manifest_json {
        Some(text) => Some(
            serde_json::from_str(&text).map_err(|e| fail("capability_manifest", e.to_string()))?,
        ),
        None => None,
    };
    // The observed part decodes under the same regime as the other three:
    // a log that will not decode drops the session rather than coming back
    // silently emptied, which would read as "this operator has handled
    // nothing" — the one thing the 記名 exists to answer.
    let observed: Vec<ObservedAssignment> = match observed_json {
        Some(text) => serde_json::from_str(&text).map_err(|e| fail("observed", e.to_string()))?,
        None => Vec::new(),
    };
    Ok(OperatorSessionRecord {
        sid,
        token_digest,
        capability_manifest,
        joined_at_secs: joined_at_secs as u64,
        last_access_secs: last_access_secs.max(0) as u64,
        desc,
        observed,
        observed_total: observed_total.max(0) as u64,
    })
}

#[async_trait]
impl OperatorSessionStore for SqliteOperatorSessionStore {
    fn name(&self) -> &str {
        "sqlite"
    }

    async fn put(&self, record: OperatorSessionRecord) -> Result<(), OperatorSessionStoreError> {
        let sid = record.sid.to_string();
        let token_digest = record.token_digest.clone();
        let capability_manifest_json = record
            .capability_manifest
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| {
                OperatorSessionStoreError::Other(format!("encode capability_manifest: {e}"))
            })?;
        let joined_at_secs = record.joined_at_secs as i64;
        let desc = record.desc.clone();
        let observed_json = serde_json::to_string(&record.observed)
            .map_err(|e| OperatorSessionStoreError::Other(format!("encode observed: {e}")))?;
        let observed_total = record.observed_total as i64;
        let last_access_secs = record.last_access_secs as i64;

        self.isle
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO operator_sessions \
                     (sid, token_digest, capability_manifest_json, joined_at_secs, \
                      join_desc, observed_json, observed_total, last_access_secs) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        sid,
                        token_digest,
                        capability_manifest_json,
                        joined_at_secs,
                        desc,
                        observed_json,
                        observed_total,
                        last_access_secs
                    ],
                )
            })
            .await
            .map_err(map_isle_err)?;
        Ok(())
    }

    async fn delete(&self, sid: &SessionId) -> Result<(), OperatorSessionStoreError> {
        let sid_str = sid.to_string();
        let sid_for_notfound = sid.clone();
        let n = self
            .isle
            .call(move |conn| {
                conn.execute(
                    "DELETE FROM operator_sessions WHERE sid = ?1",
                    params![sid_str],
                )
            })
            .await
            .map_err(map_isle_err)?;
        if n == 0 {
            Err(OperatorSessionStoreError::NotFound(sid_for_notfound))
        } else {
            Ok(())
        }
    }

    /// Unfiltered by design (see the trait's contract) — no expiry
    /// judgment, no delete.
    ///
    /// A row that will not decode is an `Err(Other)` here rather than the
    /// `None` [`Self::list`] turns it into. The difference is what the two
    /// calls are for: `list` is boot rehydration, whose caller's error path
    /// takes the whole server down, so one bad row must not be allowed to
    /// become one; this asks about a single named row, and answering
    /// "there is no such row" about one that is sitting in the file would
    /// be the same lie the undecodable-row `warn` exists to avoid.
    async fn get(
        &self,
        sid: &SessionId,
    ) -> Result<Option<OperatorSessionRecord>, OperatorSessionStoreError> {
        let sid_str = sid.to_string();
        let row = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {SESSION_SELECT_COLUMNS} FROM operator_sessions WHERE sid = ?1"
                ))?;
                let mut iter = stmt.query_map(params![sid_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                })?;
                iter.next().transpose()
            })
            .await
            .map_err(map_isle_err)?;
        row.map(|row| {
            row_to_record(row).map_err(
                |RowDecodeError {
                     raw_sid,
                     column,
                     detail,
                 }| {
                    OperatorSessionStoreError::Other(format!(
                        "operator session row {raw_sid} will not decode ({column}): {detail}"
                    ))
                },
            )
        })
        .transpose()
    }

    async fn list(&self) -> Result<Vec<OperatorSessionRecord>, OperatorSessionStoreError> {
        let rows = self
            .isle
            .call(move |conn| {
                // `rowid` breaks `joined_at_secs` ties in insertion order,
                // keeping rehydration deterministic within one second.
                let mut stmt = conn.prepare(&format!(
                    "SELECT {SESSION_SELECT_COLUMNS} FROM operator_sessions \
                     ORDER BY joined_at_secs ASC, rowid ASC"
                ))?;
                let iter = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
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
        // Per row, not all-or-nothing — see `OperatorSessionStore::list`'s
        // contract. The returned `Err` above is a backend failure; a row
        // that will not decode is reported and dropped here instead of
        // being promoted into one.
        let decoded: Vec<OperatorSessionRecord> = rows
            .into_iter()
            .filter_map(|row| match row_to_record(row) {
                Ok(record) => Some(record),
                Err(RowDecodeError {
                    raw_sid,
                    column,
                    detail,
                }) => {
                    tracing::warn!(
                        row_sid = %raw_sid,
                        column,
                        detail = %detail,
                        "operator session store: skipping a row that will not decode; \
                         this session is gone and its owner must re-login, but the \
                         remaining sessions are restored"
                    );
                    None
                }
            })
            .collect();
        // O1's expiry, applied at the one point this table is read (see the
        // trait contract). The deletes come after the read rather than as a
        // `DELETE … WHERE` predicate because the horizon is measured
        // against `last_access_secs()`, which folds the join time in for
        // rows written before that column existed — a fold SQL would have
        // to duplicate.
        let (live, expired) = super::partition_expired(decoded, super::expiry_now(), self.name());
        for sid in expired {
            // A delete that fails leaves the row for the next boot to judge
            // again; it is already excluded from this answer, so the
            // session stays unrestorable either way.
            if let Err(error) = self.delete(&sid).await {
                tracing::warn!(
                    %sid, %error,
                    "operator session store: an expired row could not be deleted; it is \
                     withheld from this restore and will be re-judged at the next boot"
                );
            }
        }
        Ok(live)
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// A live record joined at `joined_at_secs`.
    ///
    /// The join time stays a small literal — several tests below order by
    /// it — while the access clock is set to now, because `list()` deletes
    /// what the 24h horizon has expired and a record last accessed at second 100 of
    /// 1970 is expired by any real clock.
    fn mk(sid: &str, joined_at_secs: u64) -> OperatorSessionRecord {
        OperatorSessionRecord {
            sid: SessionId::parse(sid).unwrap(),
            token_digest: OperatorSessionRecord::digest_of(&format!("bearer-{sid}")),
            capability_manifest: None,
            joined_at_secs,
            last_access_secs: super::super::expiry_now(),
            desc: None,
            observed: Vec::new(),
            observed_total: 0,
        }
    }

    #[tokio::test]
    async fn put_then_list_orders_by_joined_at() {
        let (s, driver) = SqliteOperatorSessionStore::open_in_memory().await.unwrap();
        s.put(mk("S-late", 200)).await.unwrap();
        s.put(mk("S-early", 100)).await.unwrap();
        let list = s.list().await.unwrap();
        let sids: Vec<_> = list.iter().map(|r| r.sid.to_string()).collect();
        assert_eq!(sids, vec!["S-early", "S-late"]);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn put_is_upsert() {
        let (s, driver) = SqliteOperatorSessionStore::open_in_memory().await.unwrap();
        s.put(mk("S-1", 100)).await.unwrap();
        let mut updated = mk("S-1", 100);
        updated.token_digest = OperatorSessionRecord::digest_of("rotated");
        s.put(updated).await.unwrap();
        let list = s.list().await.unwrap();
        assert_eq!(list.len(), 1);
        assert!(list[0].verify_bearer("rotated"));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn delete_removes_and_missing_is_not_found() {
        let (s, driver) = SqliteOperatorSessionStore::open_in_memory().await.unwrap();
        s.put(mk("S-1", 100)).await.unwrap();
        s.delete(&SessionId::parse("S-1").unwrap()).await.unwrap();
        assert!(s.list().await.unwrap().is_empty());
        let err = s
            .delete(&SessionId::parse("S-1").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(err, OperatorSessionStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn manifest_round_trips() {
        let (s, driver) = SqliteOperatorSessionStore::open_in_memory().await.unwrap();
        let mut rec = mk("S-1", 100);
        rec.capability_manifest = Some(
            serde_json::from_value(serde_json::json!({
                "provider_id": "main-ai-self-report",
                "capabilities": [{
                    "launch_variant": "mse-coder",
                    "resolved_model": "claude-sonnet-4",
                    "effective_tools": ["Read", "Edit"]
                }]
            }))
            .unwrap(),
        );
        s.put(rec.clone()).await.unwrap();
        let list = s.list().await.unwrap();
        assert_eq!(list, vec![rec]);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");

        {
            let (s, driver) = SqliteOperatorSessionStore::open(&path).await.unwrap();
            s.put(mk("S-keep", 42)).await.unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }

        let (s, driver) = SqliteOperatorSessionStore::open(&path).await.unwrap();
        let list = s.list().await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].sid, SessionId::parse("S-keep").unwrap());
        assert!(
            list[0].verify_bearer("bearer-S-keep"),
            "the restored digest must still verify the original bearer"
        );
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// The bearer must not be recoverable from the file: what lands in the
    /// `token_digest` column is the digest, and the plaintext appears
    /// nowhere in the database bytes.
    #[tokio::test]
    async fn file_holds_the_digest_and_never_the_bearer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");
        let bearer = "bearer-S-keep";

        {
            let (s, driver) = SqliteOperatorSessionStore::open(&path).await.unwrap();
            s.put(mk("S-keep", 42)).await.unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }

        let bytes = std::fs::read(&path).expect("read db file");
        let haystack = String::from_utf8_lossy(&bytes);
        assert!(
            !haystack.contains(bearer),
            "the plaintext bearer must not appear anywhere in the database file"
        );
        assert!(
            haystack.contains(&OperatorSessionRecord::digest_of(bearer)),
            "the digest is what should be stored"
        );
    }

    /// The 記名 survives the encode/decode round trip: the confirmed part
    /// (**D1**), the observed log (**D2**) and the monotone counter.
    #[tokio::test]
    async fn the_kimei_round_trips() {
        let (s, driver) = SqliteOperatorSessionStore::open_in_memory().await.unwrap();
        let mut rec = mk("S-1", 100);
        rec.desc = Some("rewriting the seat resolver in mlua-swarm-server".to_string());
        rec.record_observed(ObservedAssignment::new(
            "R-1".to_string(),
            "phase-a-op".to_string(),
            Some("resolve issue #10".to_string()),
            Some("/repo".to_string()),
            Some("/repo/.worktrees/topic".to_string()),
            Some(serde_json::json!({"issue": 10})),
            140,
        ));
        s.put(rec.clone()).await.unwrap();

        let list = s.list().await.unwrap();
        assert_eq!(list, vec![rec]);
        assert_eq!(list[0].last_activity_secs(), 140);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// A file created before the 記名 columns existed gains them on open
    /// and keeps its rows — the sessions in it stay logged in, with an
    /// empty 記名 rather than none at all.
    ///
    /// It also carries `roles_json`, so the same open exercises the
    /// removal in the other direction: the pre-記名 shape is the
    /// pre-role-removal shape too, and one open has to land both.
    #[tokio::test]
    async fn a_pre_kimei_file_is_migrated_not_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");

        {
            let conn = rusqlite::Connection::open(&path).expect("open pre-記名 db");
            conn.execute_batch(
                "CREATE TABLE operator_sessions (\
                   sid                       TEXT PRIMARY KEY, \
                   token_digest              TEXT NOT NULL, \
                   roles_json                TEXT NOT NULL, \
                   capability_manifest_json  TEXT, \
                   joined_at_secs            INTEGER NOT NULL\
                 );",
            )
            .expect("create the pre-記名 table");
            conn.execute(
                "INSERT INTO operator_sessions VALUES (?1, ?2, ?3, NULL, ?4)",
                params![
                    "S-old",
                    OperatorSessionRecord::digest_of("bearer-S-old"),
                    r#"["main-ai"]"#,
                    7i64
                ],
            )
            .expect("seed the pre-記名 row");
        }

        let (s, driver) = SqliteOperatorSessionStore::open(&path).await.unwrap();
        let list = s.list().await.unwrap();
        assert_eq!(list.len(), 1, "the row survives the column addition");
        assert_eq!(list[0].desc, None);
        assert!(list[0].observed.is_empty());
        assert_eq!(list[0].observed_total, 0);
        assert!(list[0].verify_bearer("bearer-S-old"));

        // And the migrated file is writable in the new shape. This is the
        // assertion `migrate_drop_column_if_present` exists for: the seeded
        // table declares `roles_json TEXT NOT NULL` with no default, so a
        // build that stopped writing the column but left it in place would
        // fail this `put` on the constraint — persistence silently breaking
        // on upgraded installs only.
        let mut updated = list[0].clone();
        updated.desc = Some("picked this session back up after a restart".to_string());
        s.put(updated).await.unwrap();
        let list = s.list().await.unwrap();
        assert_eq!(
            list[0].desc.as_deref(),
            Some("picked this session back up after a restart")
        );
        assert!(
            !column_names(&path).contains(&"roles_json".to_string()),
            "the role column is dropped, not carried along unwritten"
        );
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// The column names `operator_sessions` currently has, straight off the
    /// file.
    fn column_names(path: &Path) -> Vec<String> {
        let conn = rusqlite::Connection::open(path).expect("open db for the column check");
        let mut stmt = conn
            .prepare("PRAGMA table_info(operator_sessions)")
            .expect("prepare table_info");
        let names = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query table_info")
            .collect::<Result<Vec<String>, _>>()
            .expect("collect column names");
        names
    }

    /// An observed log that will not decode drops the session rather than
    /// restoring it as "has handled nothing" — same regime as `roles`.
    #[tokio::test]
    async fn undecodable_observed_row_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");
        seed_healthy(&path).await;
        {
            let conn = rusqlite::Connection::open(&path).expect("open db for the raw seed");
            conn.execute(
                "INSERT OR REPLACE INTO operator_sessions \
                 (sid, token_digest, capability_manifest_json, joined_at_secs, \
                  join_desc, observed_json, observed_total) \
                 VALUES (?1, ?2, NULL, ?3, NULL, ?4, 1)",
                params![
                    "S-bad-observed",
                    OperatorSessionRecord::digest_of("bearer-S-bad-observed"),
                    2i64,
                    r#"[{"run_id": 42}]"#
                ],
            )
            .expect("seed the raw row");
        }

        let (list, logged) = list_capturing_warnings(&path).await;
        assert_only_healthy_survived(&list);
        assert!(
            logged.contains("S-bad-observed") && logged.contains(r#"column="observed""#),
            "the warn must name the row and the column that failed: {logged}"
        );
    }

    /// A pre-release file carrying the plaintext `token` column is dropped
    /// on open rather than migrated, so no bearer survives the upgrade.
    #[tokio::test]
    async fn legacy_plaintext_table_is_dropped_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");

        // Hand-build the pre-release shape and seed one plaintext row.
        {
            let conn = rusqlite::Connection::open(&path).expect("open legacy db");
            conn.execute_batch(
                "CREATE TABLE operator_sessions (\
                   sid                       TEXT PRIMARY KEY, \
                   token                     TEXT NOT NULL, \
                   roles_json                TEXT NOT NULL, \
                   capability_manifest_json  TEXT, \
                   joined_at_secs            INTEGER NOT NULL\
                 );",
            )
            .expect("create legacy table");
            conn.execute(
                "INSERT INTO operator_sessions VALUES (?1, ?2, ?3, NULL, ?4)",
                params!["S-legacy", "plaintext-bearer", r#"["main-ai"]"#, 1i64],
            )
            .expect("seed legacy row");
        }

        let (s, driver) = SqliteOperatorSessionStore::open(&path).await.unwrap();
        assert!(
            s.list().await.unwrap().is_empty(),
            "the legacy table is dropped, not migrated — its sessions are cleared"
        );
        // The store is usable in the new shape straight afterwards.
        s.put(mk("S-fresh", 10)).await.unwrap();
        assert_eq!(s.list().await.unwrap().len(), 1);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    // ──────────────────────────────────────────────────────────────────
    // Per-row fault tolerance
    //
    // Every row below is written straight through `rusqlite`, bypassing
    // `put`'s typed encode. That is not a shortcut: `put` takes an
    // `OperatorSessionRecord`, whose fields are already `SessionId` /
    // `AgentProviderManifest`, so it *cannot* produce any of these shapes.
    // An older build could, and did — `op-<uuid>` sids were persistable
    // before the `S-<hex>` shape landed.
    // ──────────────────────────────────────────────────────────────────

    /// A shared buffer a `tracing` subscriber can write into, so a test can
    /// assert on the warn a skipped row emits.
    #[derive(Clone, Default)]
    struct CaptureBuf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl CaptureBuf {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    impl std::io::Write for CaptureBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureBuf {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Seed one row directly into the table, bypassing the typed encode.
    fn insert_raw_row(
        path: &Path,
        sid: &str,
        capability_manifest_json: Option<&str>,
        joined_at_secs: i64,
    ) {
        let conn = rusqlite::Connection::open(path).expect("open db for the raw seed");
        conn.execute(
            "INSERT OR REPLACE INTO operator_sessions \
             (sid, token_digest, capability_manifest_json, joined_at_secs) \
             VALUES (?1, ?2, ?3, ?4)",
            params![
                sid,
                OperatorSessionRecord::digest_of(&format!("bearer-{sid}")),
                capability_manifest_json,
                joined_at_secs
            ],
        )
        .expect("seed the raw row");
    }

    /// Create the store file with one healthy row, so the poisoned row a
    /// caller adds afterwards has an intact sibling to be measured against.
    async fn seed_healthy(path: &Path) {
        let (s, driver) = SqliteOperatorSessionStore::open(path).await.unwrap();
        s.put(mk("S-healthy", 1)).await.unwrap();
        drop(s);
        driver.shutdown().await.unwrap();
    }

    /// Reopen the store and `list()` it with a warn-capturing subscriber
    /// installed. Returns the decoded rows and everything logged.
    ///
    /// `#[tokio::test]` runs on a current-thread runtime, so the future is
    /// polled on this thread throughout and the thread-local subscriber
    /// covers the whole call.
    async fn list_capturing_warnings(path: &Path) -> (Vec<OperatorSessionRecord>, String) {
        let buf = CaptureBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);

        let (s, driver) = SqliteOperatorSessionStore::open(path).await.unwrap();
        let list = s
            .list()
            .await
            .expect("one undecodable row must not fail the whole list");
        drop(s);
        driver.shutdown().await.unwrap();

        drop(guard);
        (list, buf.contents())
    }

    fn assert_only_healthy_survived(list: &[OperatorSessionRecord]) {
        let sids: Vec<_> = list.iter().map(|r| r.sid.to_string()).collect();
        assert_eq!(
            sids,
            vec!["S-healthy"],
            "the intact row must survive and the poisoned one must not be returned"
        );
    }

    /// (a) A sid that is not `S-`-shaped. Decoded with the same regime as
    /// the other one — the sid is not special-cased just because it is the
    /// key.
    #[tokio::test]
    async fn undecodable_sid_row_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");
        seed_healthy(&path).await;
        insert_raw_row(&path, "op-legacy-uuid", None, 2);

        let (list, logged) = list_capturing_warnings(&path).await;
        assert_only_healthy_survived(&list);
        assert!(
            logged.contains("op-legacy-uuid") && logged.contains(r#"column="sid""#),
            "the warn must name the row and the column that failed: {logged}"
        );
    }

    /// (b) A capability manifest that is not valid JSON for the manifest
    /// type. Same regime again.
    #[tokio::test]
    async fn undecodable_capability_manifest_row_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");
        seed_healthy(&path).await;
        insert_raw_row(&path, "S-bad-manifest", Some(r#"{"provider_id": 42}"#), 2);

        let (list, logged) = list_capturing_warnings(&path).await;
        assert_only_healthy_survived(&list);
        assert!(
            logged.contains("S-bad-manifest") && logged.contains(r#"column="capability_manifest""#),
            "the warn must name the row and the column that failed: {logged}"
        );
    }

    /// Every undecodable row is dropped, not just the first one, and a file
    /// where *all* rows are poisoned lists empty rather than erroring.
    #[tokio::test]
    async fn several_poisoned_rows_are_all_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");
        seed_healthy(&path).await;
        insert_raw_row(&path, "op-legacy-uuid", None, 3);
        insert_raw_row(&path, "S-bad-manifest", Some(r#"{"provider_id": 42}"#), 4);

        let (list, _logged) = list_capturing_warnings(&path).await;
        assert_only_healthy_survived(&list);
    }

    /// On unix the file is owner-only (`0600`) — the umask default would
    /// otherwise commonly leave it world-readable.
    #[cfg(unix)]
    #[tokio::test]
    async fn file_is_owner_only_on_unix() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");
        let (s, driver) = SqliteOperatorSessionStore::open(&path).await.unwrap();
        s.put(mk("S-1", 1)).await.unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "expected owner-only, got {mode:o}");
        drop(s);
        driver.shutdown().await.unwrap();
    }
}
