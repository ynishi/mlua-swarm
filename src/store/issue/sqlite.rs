//! `SqliteIssueStore` — SQLite-backed [`IssueStore`] using [`rusqlite-isle`].
//!
//! The `Connection` is confined to a dedicated OS thread by `AsyncIsle`; every
//! call is a typed closure dispatched over a bounded channel. `pop_pending`
//! runs the "pick oldest pending row + flip to InFlight" as a single
//! transaction inside one closure, so the FIFO invariant is preserved across
//! concurrent callers.
//!
//! ## Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS issues (
//!   issue_id      TEXT PRIMARY KEY,
//!   blueprint_id  TEXT NOT NULL,
//!   intent        TEXT NOT NULL,
//!   status_kind   TEXT NOT NULL,      -- 'pending' | 'inflight' | 'applied' | 'rejected'
//!   status_detail TEXT,               -- new_version (applied) | reason (rejected) | NULL
//!   created_seq   INTEGER NOT NULL,   -- insertion order for `list()`
//!   pending_seq   INTEGER              -- FIFO for `pop_pending`, NULL when not pending
//! );
//! CREATE INDEX IF NOT EXISTS ix_issues_pending_seq ON issues(pending_seq);
//! ```

use super::{IssueId, IssuePayload, IssueStatus, IssueStore, IssueStoreError};
use crate::blueprint::store::BlueprintId;
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::path::Path;

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS issues (\
  issue_id      TEXT PRIMARY KEY, \
  blueprint_id  TEXT NOT NULL, \
  intent        TEXT NOT NULL, \
  status_kind   TEXT NOT NULL, \
  status_detail TEXT, \
  created_seq   INTEGER NOT NULL, \
  pending_seq   INTEGER\
);\
CREATE INDEX IF NOT EXISTS ix_issues_pending_seq ON issues(pending_seq);\
CREATE INDEX IF NOT EXISTS ix_issues_created_seq ON issues(created_seq);\
";

const STATUS_PENDING: &str = "pending";
const STATUS_INFLIGHT: &str = "inflight";
const STATUS_APPLIED: &str = "applied";
const STATUS_REJECTED: &str = "rejected";

/// SQLite-backed persistent [`IssueStore`].
///
/// Open with [`SqliteIssueStore::open`] (file path) or
/// [`SqliteIssueStore::open_in_memory`] (tests). Both return the store plus an
/// [`AsyncIsleDriver`] the caller must `shutdown().await` when done — dropping
/// the driver without a shutdown call leaves the SQLite thread as-is until the
/// process exits.
pub struct SqliteIssueStore {
    isle: AsyncIsle,
}

impl SqliteIssueStore {
    /// Open (or create) a SQLite database file and run the schema migrations.
    pub async fn open(path: impl AsRef<Path>) -> Result<(Self, AsyncIsleDriver), IssueStoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), |conn| {
            conn.execute_batch(SCHEMA_SQL)
        })
        .await
        .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests, doctests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), IssueStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(|conn| conn.execute_batch(SCHEMA_SQL))
            .await
            .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }
}

fn map_isle_err(e: IsleError) -> IssueStoreError {
    IssueStoreError::Other(format!("sqlite: {e}"))
}

/// Encode an `IssueStatus` as `(kind, detail)` for storage.
fn encode_status(s: &IssueStatus) -> (&'static str, Option<String>) {
    match s {
        IssueStatus::Pending => (STATUS_PENDING, None),
        IssueStatus::InFlight => (STATUS_INFLIGHT, None),
        IssueStatus::Applied { new_version } => (STATUS_APPLIED, Some(new_version.clone())),
        IssueStatus::Rejected { reason } => (STATUS_REJECTED, Some(reason.clone())),
    }
}

/// Decode a `(kind, detail)` row back into an `IssueStatus`.
fn decode_status(kind: &str, detail: Option<String>) -> Result<IssueStatus, IssueStoreError> {
    match kind {
        STATUS_PENDING => Ok(IssueStatus::Pending),
        STATUS_INFLIGHT => Ok(IssueStatus::InFlight),
        STATUS_APPLIED => Ok(IssueStatus::Applied {
            new_version: detail.unwrap_or_default(),
        }),
        STATUS_REJECTED => Ok(IssueStatus::Rejected {
            reason: detail.unwrap_or_default(),
        }),
        other => Err(IssueStoreError::Other(format!(
            "invalid status_kind: {other}"
        ))),
    }
}

#[async_trait]
impl IssueStore for SqliteIssueStore {
    fn name(&self) -> &str {
        "sqlite"
    }

    async fn create(&self, payload: IssuePayload) -> Result<(), IssueStoreError> {
        let id = payload.issue_id.0.clone();
        let bp = payload.blueprint_id.as_str().to_string();
        let intent = payload.intent.clone();

        self.isle
            .call(move |conn| {
                let tx = conn.transaction()?;
                // Duplicate check — surface as a distinct error kind at the
                // trait layer (unique constraint violation would work too, but
                // the explicit check keeps the error mapping trivial).
                let exists: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM issues WHERE issue_id = ?1",
                    params![id],
                    |row| row.get(0),
                )?;
                if exists > 0 {
                    // Signal via a sentinel rusqlite::Error; the outer layer
                    // maps it back to Duplicate.
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
                        Some(format!("__mlua_swarm_duplicate:{id}")),
                    ));
                }
                let created_seq: i64 = tx.query_row(
                    "SELECT COALESCE(MAX(created_seq), 0) + 1 FROM issues",
                    [],
                    |row| row.get(0),
                )?;
                let pending_seq: i64 = tx.query_row(
                    "SELECT COALESCE(MAX(pending_seq), 0) + 1 FROM issues",
                    [],
                    |row| row.get(0),
                )?;
                tx.execute(
                    "INSERT INTO issues (issue_id, blueprint_id, intent, status_kind, \
                     status_detail, created_seq, pending_seq) \
                     VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6)",
                    params![id, bp, intent, STATUS_PENDING, created_seq, pending_seq],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(|e| match &e {
                IsleError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(msg)))
                    if msg.starts_with("__mlua_swarm_duplicate:") =>
                {
                    let id = msg
                        .trim_start_matches("__mlua_swarm_duplicate:")
                        .to_string();
                    IssueStoreError::Duplicate(IssueId::new(id))
                }
                _ => map_isle_err(e),
            })
    }

    async fn get(&self, id: &IssueId) -> Result<IssuePayload, IssueStoreError> {
        let id_str = id.0.clone();
        let id_for_notfound = id.clone();
        let row = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT blueprint_id, intent FROM issues WHERE issue_id = ?1",
                    params![id_str],
                    |row| {
                        let bp: String = row.get(0)?;
                        let intent: String = row.get(1)?;
                        Ok((bp, intent))
                    },
                )
                .optional()
            })
            .await
            .map_err(map_isle_err)?;
        match row {
            Some((bp, intent)) => Ok(IssuePayload {
                issue_id: id_for_notfound,
                blueprint_id: BlueprintId::new(bp),
                intent,
            }),
            None => Err(IssueStoreError::NotFound(id_for_notfound)),
        }
    }

    async fn status(&self, id: &IssueId) -> Result<IssueStatus, IssueStoreError> {
        let id_str = id.0.clone();
        let id_for_notfound = id.clone();
        let row = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT status_kind, status_detail FROM issues WHERE issue_id = ?1",
                    params![id_str],
                    |row| {
                        let kind: String = row.get(0)?;
                        let detail: Option<String> = row.get(1)?;
                        Ok((kind, detail))
                    },
                )
                .optional()
            })
            .await
            .map_err(map_isle_err)?;
        match row {
            Some((kind, detail)) => decode_status(&kind, detail),
            None => Err(IssueStoreError::NotFound(id_for_notfound)),
        }
    }

    async fn list(&self) -> Result<Vec<(IssueId, IssueStatus)>, IssueStoreError> {
        let rows = self
            .isle
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT issue_id, status_kind, status_detail \
                     FROM issues ORDER BY created_seq ASC",
                )?;
                let iter = stmt.query_map([], |row| {
                    let id: String = row.get(0)?;
                    let kind: String = row.get(1)?;
                    let detail: Option<String> = row.get(2)?;
                    Ok((id, kind, detail))
                })?;
                let mut out = Vec::new();
                for r in iter {
                    out.push(r?);
                }
                Ok(out)
            })
            .await
            .map_err(map_isle_err)?;

        let mut result = Vec::with_capacity(rows.len());
        for (id, kind, detail) in rows {
            result.push((IssueId::new(id), decode_status(&kind, detail)?));
        }
        Ok(result)
    }

    async fn pop_pending(&self) -> Result<Option<IssuePayload>, IssueStoreError> {
        let picked = self
            .isle
            .call(move |conn| {
                let tx = conn.transaction()?;
                let row: Option<(String, String, String)> = tx
                    .query_row(
                        "SELECT issue_id, blueprint_id, intent FROM issues \
                         WHERE pending_seq IS NOT NULL \
                         ORDER BY pending_seq ASC LIMIT 1",
                        [],
                        |row| {
                            let id: String = row.get(0)?;
                            let bp: String = row.get(1)?;
                            let intent: String = row.get(2)?;
                            Ok((id, bp, intent))
                        },
                    )
                    .optional()?;
                let Some((id, bp, intent)) = row else {
                    return Ok(None);
                };
                tx.execute(
                    "UPDATE issues SET status_kind = ?1, status_detail = NULL, \
                     pending_seq = NULL WHERE issue_id = ?2",
                    params![STATUS_INFLIGHT, id],
                )?;
                tx.commit()?;
                Ok(Some((id, bp, intent)))
            })
            .await
            .map_err(map_isle_err)?;

        Ok(picked.map(|(id, bp, intent)| IssuePayload {
            issue_id: IssueId::new(id),
            blueprint_id: BlueprintId::new(bp),
            intent,
        }))
    }

    async fn update_status(
        &self,
        id: &IssueId,
        status: IssueStatus,
    ) -> Result<(), IssueStoreError> {
        let id_str = id.0.clone();
        let id_for_notfound = id.clone();
        let (kind, detail) = encode_status(&status);
        let clear_pending = !matches!(status, IssueStatus::Pending);
        let n = self
            .isle
            .call(move |conn| {
                if clear_pending {
                    conn.execute(
                        "UPDATE issues SET status_kind = ?1, status_detail = ?2, \
                         pending_seq = NULL WHERE issue_id = ?3",
                        params![kind, detail, id_str],
                    )
                } else {
                    conn.execute(
                        "UPDATE issues SET status_kind = ?1, status_detail = ?2 \
                         WHERE issue_id = ?3",
                        params![kind, detail, id_str],
                    )
                }
            })
            .await
            .map_err(map_isle_err)?;
        if n == 0 {
            Err(IssueStoreError::NotFound(id_for_notfound))
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

    fn mk(id: &str) -> IssuePayload {
        IssuePayload {
            issue_id: IssueId::new(id),
            blueprint_id: BlueprintId::new("main"),
            intent: format!("intent for {id}"),
        }
    }

    #[tokio::test]
    async fn create_then_get_status() {
        let (s, driver) = SqliteIssueStore::open_in_memory().await.unwrap();
        s.create(mk("i1")).await.unwrap();
        let got = s.get(&IssueId::new("i1")).await.unwrap();
        assert_eq!(got.issue_id, IssueId::new("i1"));
        assert_eq!(got.intent, "intent for i1");
        assert_eq!(
            s.status(&IssueId::new("i1")).await.unwrap(),
            IssueStatus::Pending
        );
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_create_rejected() {
        let (s, driver) = SqliteIssueStore::open_in_memory().await.unwrap();
        s.create(mk("i1")).await.unwrap();
        let err = s.create(mk("i1")).await.unwrap_err();
        assert!(matches!(err, IssueStoreError::Duplicate(_)), "got: {err:?}");
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pop_pending_fifo_and_transitions_inflight() {
        let (s, driver) = SqliteIssueStore::open_in_memory().await.unwrap();
        s.create(mk("a")).await.unwrap();
        s.create(mk("b")).await.unwrap();

        let p1 = s.pop_pending().await.unwrap().unwrap();
        assert_eq!(p1.issue_id, IssueId::new("a"));
        assert_eq!(
            s.status(&IssueId::new("a")).await.unwrap(),
            IssueStatus::InFlight
        );

        let p2 = s.pop_pending().await.unwrap().unwrap();
        assert_eq!(p2.issue_id, IssueId::new("b"));

        assert!(s.pop_pending().await.unwrap().is_none());
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn update_status_to_applied_and_rejected() {
        let (s, driver) = SqliteIssueStore::open_in_memory().await.unwrap();
        s.create(mk("x")).await.unwrap();
        s.pop_pending().await.unwrap();
        s.update_status(
            &IssueId::new("x"),
            IssueStatus::Applied {
                new_version: "abc123".into(),
            },
        )
        .await
        .unwrap();
        match s.status(&IssueId::new("x")).await.unwrap() {
            IssueStatus::Applied { new_version } => assert_eq!(new_version, "abc123"),
            other => panic!("unexpected: {other:?}"),
        }

        s.create(mk("y")).await.unwrap();
        s.update_status(
            &IssueId::new("y"),
            IssueStatus::Rejected {
                reason: "bad shape".into(),
            },
        )
        .await
        .unwrap();
        match s.status(&IssueId::new("y")).await.unwrap() {
            IssueStatus::Rejected { reason } => assert_eq!(reason, "bad shape"),
            other => panic!("unexpected: {other:?}"),
        }
        // `y` was updated to Rejected before ever being popped — verify the
        // pending queue no longer offers it.
        assert!(s.pop_pending().await.unwrap().is_none());
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_returns_insertion_order() {
        let (s, driver) = SqliteIssueStore::open_in_memory().await.unwrap();
        s.create(mk("a")).await.unwrap();
        s.create(mk("b")).await.unwrap();
        s.create(mk("c")).await.unwrap();
        let v = s.list().await.unwrap();
        let ids: Vec<_> = v.iter().map(|(i, _)| i.0.clone()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn update_status_unknown_fails() {
        let (s, driver) = SqliteIssueStore::open_in_memory().await.unwrap();
        let err = s
            .update_status(&IssueId::new("nope"), IssueStatus::Pending)
            .await
            .unwrap_err();
        assert!(matches!(err, IssueStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("issues.db");

        {
            let (s, driver) = SqliteIssueStore::open(&path).await.unwrap();
            s.create(mk("keep")).await.unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }

        let (s, driver) = SqliteIssueStore::open(&path).await.unwrap();
        let got = s.get(&IssueId::new("keep")).await.unwrap();
        assert_eq!(got.intent, "intent for keep");
        drop(s);
        driver.shutdown().await.unwrap();
    }
}
