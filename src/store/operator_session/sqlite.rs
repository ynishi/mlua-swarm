//! `SqliteOperatorSessionStore` — SQLite-backed [`OperatorSessionStore`]
//! using [`rusqlite-isle`].
//!
//! The `Connection` is confined to a dedicated OS thread by `AsyncIsle`;
//! every call is a typed closure dispatched over a bounded channel. `roles`
//! and `capability_manifest` are stored as JSON blobs — neither is queried
//! relationally; the boot-time `list()` rehydration decodes them back into
//! their Rust shapes.
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
//!   roles_json                TEXT NOT NULL,  -- JSON-encoded `Vec<String>`
//!   capability_manifest_json  TEXT,           -- JSON-encoded manifest, NULL when unset
//!   joined_at_secs            INTEGER NOT NULL
//! );
//! ```

use super::{
    OperatorRef, OperatorSessionRecord, OperatorSessionStore, OperatorSessionStoreError, SessionId,
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
  roles_json                TEXT NOT NULL, \
  capability_manifest_json  TEXT, \
  joined_at_secs            INTEGER NOT NULL\
);\
";

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
        let (isle, driver) = AsyncIsle::spawn(path.clone(), |conn| {
            conn.busy_timeout(std::time::Duration::from_millis(5_000))?;
            purge_legacy_plaintext_table(conn)?;
            conn.execute_batch(SCHEMA_SQL)
        })
        .await
        .map_err(map_isle_err)?;
        // After the open: SQLite creates the file with umask-derived
        // permissions, so the tightening has to follow it.
        harden_file_permissions(&path);
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests, doctests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), OperatorSessionStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(|conn| {
            conn.busy_timeout(std::time::Duration::from_millis(5_000))?;
            purge_legacy_plaintext_table(conn)?;
            conn.execute_batch(SCHEMA_SQL)
        })
        .await
        .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }
}

fn map_isle_err(e: IsleError) -> OperatorSessionStoreError {
    OperatorSessionStoreError::Other(format!("sqlite: {e}"))
}

/// One `operator_sessions` SELECT row in column order: sid, token_digest,
/// roles_json, capability_manifest_json, joined_at_secs.
type SessionRow = (String, String, String, Option<String>, i64);

const SESSION_SELECT_COLUMNS: &str =
    "sid, token_digest, roles_json, capability_manifest_json, joined_at_secs";

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
    /// Which column's decode failed: `sid`, `roles`, or
    /// `capability_manifest`.
    column: &'static str,
    /// The decoder's own message.
    detail: String,
}

fn row_to_record(row: SessionRow) -> Result<OperatorSessionRecord, RowDecodeError> {
    let (raw_sid, token_digest, roles_json, capability_manifest_json, joined_at_secs) = row;
    // All three decodes below fail the same way and get the same treatment.
    // `roles` is the one an older build is known to have poisoned, but the
    // sid and the manifest are no more trustworthy for having been quiet so
    // far — special-casing `roles` would just move the boot-stopper.
    let fail = |column: &'static str, detail: String| RowDecodeError {
        raw_sid: raw_sid.clone(),
        column,
        detail,
    };
    let sid = SessionId::parse(raw_sid.clone()).map_err(|e| fail("sid", e.to_string()))?;
    // The stored JSON is (and stays) an array of plain strings; the element
    // type only decides what the decode validates on the way back in.
    let roles: Vec<OperatorRef> =
        serde_json::from_str(&roles_json).map_err(|e| fail("roles", e.to_string()))?;
    let capability_manifest: Option<AgentProviderManifest> = match capability_manifest_json {
        Some(text) => Some(
            serde_json::from_str(&text).map_err(|e| fail("capability_manifest", e.to_string()))?,
        ),
        None => None,
    };
    Ok(OperatorSessionRecord {
        sid,
        token_digest,
        roles,
        capability_manifest,
        joined_at_secs: joined_at_secs as u64,
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
        let roles_json = serde_json::to_string(&record.roles)
            .map_err(|e| OperatorSessionStoreError::Other(format!("encode roles: {e}")))?;
        let capability_manifest_json = record
            .capability_manifest
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| {
                OperatorSessionStoreError::Other(format!("encode capability_manifest: {e}"))
            })?;
        let joined_at_secs = record.joined_at_secs as i64;

        self.isle
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO operator_sessions \
                     (sid, token_digest, roles_json, capability_manifest_json, joined_at_secs) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        sid,
                        token_digest,
                        roles_json,
                        capability_manifest_json,
                        joined_at_secs
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
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
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
        Ok(rows
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
            .collect())
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// convention-token-ok: mlua-swarm public operator role literal.
    fn role(name: &str) -> OperatorRef {
        OperatorRef::new(name).expect("test role literal is never empty")
    }

    fn mk(sid: &str, joined_at_secs: u64) -> OperatorSessionRecord {
        OperatorSessionRecord {
            sid: SessionId::parse(sid).unwrap(),
            token_digest: OperatorSessionRecord::digest_of(&format!("bearer-{sid}")),
            roles: vec![role("main-ai")],
            capability_manifest: None,
            joined_at_secs,
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
        assert_eq!(list[0].roles, vec![role("main-ai")]);
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
    // `Vec<OperatorRef>` / `AgentProviderManifest`, so it *cannot* produce
    // any of these shapes. An older build could, and did — `roles: [""]`
    // was persistable before the `OperatorRef` typing landed.
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
        roles_json: &str,
        capability_manifest_json: Option<&str>,
        joined_at_secs: i64,
    ) {
        let conn = rusqlite::Connection::open(path).expect("open db for the raw seed");
        conn.execute(
            "INSERT OR REPLACE INTO operator_sessions \
             (sid, token_digest, roles_json, capability_manifest_json, joined_at_secs) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                sid,
                OperatorSessionRecord::digest_of(&format!("bearer-{sid}")),
                roles_json,
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

    /// (a) An empty role alias — the shape an older build could persist via
    /// `POST /v1/operators {"roles":[""]}`, before `roles` decoded into
    /// `Vec<OperatorRef>`. This is the combination that actually exists in
    /// the wild, so it is the one that must not take the boot down with it.
    #[tokio::test]
    async fn empty_role_row_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");
        seed_healthy(&path).await;
        insert_raw_row(&path, "S-empty-role", r#"[""]"#, None, 2);

        let (list, logged) = list_capturing_warnings(&path).await;
        assert_only_healthy_survived(&list);
        assert!(
            logged.contains("S-empty-role") && logged.contains(r#"column="roles""#),
            "the warn must name the row and the column that failed: {logged}"
        );
    }

    /// (b) A sid that is not `S-`-shaped. Decoded with the same regime as
    /// the other two — the sid is not special-cased just because it is the
    /// key.
    #[tokio::test]
    async fn undecodable_sid_row_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");
        seed_healthy(&path).await;
        insert_raw_row(&path, "op-legacy-uuid", r#"["main-ai"]"#, None, 2);

        let (list, logged) = list_capturing_warnings(&path).await;
        assert_only_healthy_survived(&list);
        assert!(
            logged.contains("op-legacy-uuid") && logged.contains(r#"column="sid""#),
            "the warn must name the row and the column that failed: {logged}"
        );
    }

    /// (c) A capability manifest that is not valid JSON for the manifest
    /// type. Same regime again.
    #[tokio::test]
    async fn undecodable_capability_manifest_row_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator_session.db");
        seed_healthy(&path).await;
        insert_raw_row(
            &path,
            "S-bad-manifest",
            r#"["main-ai"]"#,
            Some(r#"{"provider_id": 42}"#),
            2,
        );

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
        insert_raw_row(&path, "S-empty-role", r#"[""]"#, None, 2);
        insert_raw_row(&path, "op-legacy-uuid", r#"["main-ai"]"#, None, 3);
        insert_raw_row(
            &path,
            "S-bad-manifest",
            r#"["main-ai"]"#,
            Some(r#"{"provider_id": 42}"#),
            4,
        );

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
