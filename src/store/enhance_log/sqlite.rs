//! `SqliteEnhanceLogStore` — SQLite-backed [`EnhanceLogStore`].
//!
//! One row per `issue_id`. `verdicts` and `reasons` are stored as JSON blobs
//! so schema evolution of `VerdictSummary` does not require a migration.
//! Lists are ordered by `ts_ms ASC` (as promised by the trait contract).

use super::{EnhanceLogEntry, EnhanceLogStore, EnhanceLogStoreError, VerdictSummary};
use crate::blueprint::store::BlueprintId;
use crate::store::issue::IssueId;
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};
use std::path::Path;

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS enhance_log (\
  issue_id     TEXT PRIMARY KEY, \
  blueprint_id TEXT NOT NULL, \
  prev_hash    TEXT NOT NULL, \
  new_hash     TEXT NOT NULL, \
  intent       TEXT NOT NULL, \
  rationale    TEXT NOT NULL, \
  verdicts_json TEXT NOT NULL, \
  status       TEXT NOT NULL, \
  reasons_json TEXT NOT NULL, \
  ts_ms        INTEGER NOT NULL\
);\
CREATE INDEX IF NOT EXISTS ix_enhance_log_bp_ts ON enhance_log(blueprint_id, ts_ms);\
CREATE INDEX IF NOT EXISTS ix_enhance_log_ts ON enhance_log(ts_ms);\
";

/// SQLite-backed [`EnhanceLogStore`]. Append-only in the same sense as the
/// in-memory backend: a duplicate `issue_id` returns `Conflict`, the existing
/// row is left untouched.
pub struct SqliteEnhanceLogStore {
    isle: AsyncIsle,
}

impl SqliteEnhanceLogStore {
    /// Open (or create) a SQLite file and apply the schema.
    pub async fn open(
        path: impl AsRef<Path>,
    ) -> Result<(Self, AsyncIsleDriver), EnhanceLogStoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), |conn| {
            conn.execute_batch(SCHEMA_SQL)
        })
        .await
        .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }

    /// Open an ephemeral in-memory database (tests).
    pub async fn open_in_memory() -> Result<(Self, AsyncIsleDriver), EnhanceLogStoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(|conn| conn.execute_batch(SCHEMA_SQL))
            .await
            .map_err(map_isle_err)?;
        Ok((Self { isle }, driver))
    }
}

fn map_isle_err(e: IsleError) -> EnhanceLogStoreError {
    EnhanceLogStoreError::Other(format!("sqlite: {e}"))
}

fn row_to_entry(
    issue_id: String,
    blueprint_id: String,
    prev_hash: String,
    new_hash: String,
    intent: String,
    rationale: String,
    verdicts_json: String,
    status: String,
    reasons_json: String,
    ts_ms: i64,
) -> Result<EnhanceLogEntry, EnhanceLogStoreError> {
    let verdicts: Vec<VerdictSummary> = serde_json::from_str(&verdicts_json)
        .map_err(|e| EnhanceLogStoreError::Other(format!("decode verdicts: {e}")))?;
    let reasons: Vec<String> = serde_json::from_str(&reasons_json)
        .map_err(|e| EnhanceLogStoreError::Other(format!("decode reasons: {e}")))?;
    Ok(EnhanceLogEntry {
        issue_id: IssueId::new(issue_id),
        blueprint_id: BlueprintId::new(blueprint_id),
        prev_hash,
        new_hash,
        intent,
        rationale,
        verdicts,
        status,
        reasons,
        ts_ms,
    })
}

#[async_trait]
impl EnhanceLogStore for SqliteEnhanceLogStore {
    fn name(&self) -> &str {
        "sqlite"
    }

    async fn append(&self, entry: EnhanceLogEntry) -> Result<(), EnhanceLogStoreError> {
        let issue_id_str = entry.issue_id.0.clone();
        let issue_id_for_conflict = entry.issue_id.clone();
        let blueprint_id = entry.blueprint_id.as_str().to_string();
        let prev_hash = entry.prev_hash.clone();
        let new_hash = entry.new_hash.clone();
        let intent = entry.intent.clone();
        let rationale = entry.rationale.clone();
        let verdicts_json = serde_json::to_string(&entry.verdicts)
            .map_err(|e| EnhanceLogStoreError::Other(format!("encode verdicts: {e}")))?;
        let status = entry.status.clone();
        let reasons_json = serde_json::to_string(&entry.reasons)
            .map_err(|e| EnhanceLogStoreError::Other(format!("encode reasons: {e}")))?;
        let ts_ms = entry.ts_ms;

        self.isle
            .call(move |conn| {
                let tx = conn.transaction()?;
                let exists: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM enhance_log WHERE issue_id = ?1",
                    params![issue_id_str],
                    |row| row.get(0),
                )?;
                if exists > 0 {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
                        Some(format!("__mlua_swarm_conflict:{issue_id_str}")),
                    ));
                }
                tx.execute(
                    "INSERT INTO enhance_log (issue_id, blueprint_id, prev_hash, new_hash, \
                     intent, rationale, verdicts_json, status, reasons_json, ts_ms) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        issue_id_str,
                        blueprint_id,
                        prev_hash,
                        new_hash,
                        intent,
                        rationale,
                        verdicts_json,
                        status,
                        reasons_json,
                        ts_ms,
                    ],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(|e| match &e {
                IsleError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(msg)))
                    if msg.starts_with("__mlua_swarm_conflict:") =>
                {
                    EnhanceLogStoreError::Conflict(issue_id_for_conflict.clone())
                }
                _ => map_isle_err(e),
            })
    }

    async fn get(&self, issue_id: &IssueId) -> Result<EnhanceLogEntry, EnhanceLogStoreError> {
        let id_str = issue_id.0.clone();
        let id_for_notfound = issue_id.clone();
        let row = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT issue_id, blueprint_id, prev_hash, new_hash, intent, rationale, \
                     verdicts_json, status, reasons_json, ts_ms \
                     FROM enhance_log WHERE issue_id = ?1",
                    params![id_str],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, String>(7)?,
                            row.get::<_, String>(8)?,
                            row.get::<_, i64>(9)?,
                        ))
                    },
                )
                .optional()
            })
            .await
            .map_err(map_isle_err)?;
        match row {
            Some((iid, bp, prev, new, intent, rat, verdicts, status, reasons, ts)) => row_to_entry(
                iid, bp, prev, new, intent, rat, verdicts, status, reasons, ts,
            ),
            None => Err(EnhanceLogStoreError::NotFound(id_for_notfound)),
        }
    }

    async fn list_by_blueprint(
        &self,
        blueprint_id: &BlueprintId,
    ) -> Result<Vec<EnhanceLogEntry>, EnhanceLogStoreError> {
        let bp_str = blueprint_id.as_str().to_string();
        let rows = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT issue_id, blueprint_id, prev_hash, new_hash, intent, rationale, \
                     verdicts_json, status, reasons_json, ts_ms \
                     FROM enhance_log WHERE blueprint_id = ?1 ORDER BY ts_ms ASC",
                )?;
                let iter = stmt.query_map(params![bp_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, i64>(9)?,
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
            .map(|(iid, bp, prev, new, intent, rat, verdicts, status, reasons, ts)| {
                row_to_entry(iid, bp, prev, new, intent, rat, verdicts, status, reasons, ts)
            })
            .collect()
    }

    async fn list_all(&self) -> Result<Vec<EnhanceLogEntry>, EnhanceLogStoreError> {
        let rows = self
            .isle
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT issue_id, blueprint_id, prev_hash, new_hash, intent, rationale, \
                     verdicts_json, status, reasons_json, ts_ms \
                     FROM enhance_log ORDER BY ts_ms ASC",
                )?;
                let iter = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, i64>(9)?,
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
            .map(|(iid, bp, prev, new, intent, rat, verdicts, status, reasons, ts)| {
                row_to_entry(iid, bp, prev, new, intent, rat, verdicts, status, reasons, ts)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_entry(issue: &str, bp: &str, ts_ms: i64, status: &str) -> EnhanceLogEntry {
        EnhanceLogEntry {
            issue_id: IssueId::new(issue),
            blueprint_id: BlueprintId::new(bp),
            prev_hash: "prev".into(),
            new_hash: if status == "applied" { "new" } else { "" }.into(),
            intent: format!("intent-{issue}"),
            rationale: format!("rationale-{issue}"),
            verdicts: vec![VerdictSummary {
                axis: "des".into(),
                status: "pass".into(),
                detail: "ok".into(),
            }],
            status: status.into(),
            reasons: if status == "rejected" {
                vec!["des: broken".into()]
            } else {
                vec![]
            },
            ts_ms,
        }
    }

    #[tokio::test]
    async fn append_then_get_roundtrip() {
        let (s, driver) = SqliteEnhanceLogStore::open_in_memory().await.unwrap();
        let e = mk_entry("i1", "bp-1", 100, "applied");
        s.append(e.clone()).await.unwrap();
        let got = s.get(&IssueId::new("i1")).await.unwrap();
        assert_eq!(got, e);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_append_returns_conflict() {
        let (s, driver) = SqliteEnhanceLogStore::open_in_memory().await.unwrap();
        s.append(mk_entry("i1", "bp-1", 100, "applied"))
            .await
            .unwrap();
        let err = s
            .append(mk_entry("i1", "bp-1", 200, "rejected"))
            .await
            .unwrap_err();
        assert!(matches!(err, EnhanceLogStoreError::Conflict(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn get_missing_returns_not_found() {
        let (s, driver) = SqliteEnhanceLogStore::open_in_memory().await.unwrap();
        let err = s.get(&IssueId::new("nope")).await.unwrap_err();
        assert!(matches!(err, EnhanceLogStoreError::NotFound(_)));
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_by_blueprint_ascending_ts() {
        let (s, driver) = SqliteEnhanceLogStore::open_in_memory().await.unwrap();
        s.append(mk_entry("a", "bp-1", 300, "applied"))
            .await
            .unwrap();
        s.append(mk_entry("b", "bp-2", 200, "applied"))
            .await
            .unwrap();
        s.append(mk_entry("c", "bp-1", 100, "rejected"))
            .await
            .unwrap();
        let by_bp1 = s
            .list_by_blueprint(&BlueprintId::new("bp-1"))
            .await
            .unwrap();
        let ids: Vec<_> = by_bp1.iter().map(|e| e.issue_id.0.clone()).collect();
        assert_eq!(ids, vec!["c", "a"]);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_all_ascending_ts() {
        let (s, driver) = SqliteEnhanceLogStore::open_in_memory().await.unwrap();
        s.append(mk_entry("a", "bp-1", 300, "applied"))
            .await
            .unwrap();
        s.append(mk_entry("b", "bp-2", 100, "applied"))
            .await
            .unwrap();
        s.append(mk_entry("c", "bp-1", 200, "applied"))
            .await
            .unwrap();
        let all = s.list_all().await.unwrap();
        let ids: Vec<_> = all.iter().map(|e| e.issue_id.0.clone()).collect();
        assert_eq!(ids, vec!["b", "c", "a"]);
        drop(s);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("enhance_log.db");
        {
            let (s, driver) = SqliteEnhanceLogStore::open(&path).await.unwrap();
            s.append(mk_entry("keep", "bp-1", 42, "applied"))
                .await
                .unwrap();
            drop(s);
            driver.shutdown().await.unwrap();
        }
        let (s, driver) = SqliteEnhanceLogStore::open(&path).await.unwrap();
        let got = s.get(&IssueId::new("keep")).await.unwrap();
        assert_eq!(got.blueprint_id.as_str(), "bp-1");
        assert_eq!(got.ts_ms, 42);
        drop(s);
        driver.shutdown().await.unwrap();
    }
}
