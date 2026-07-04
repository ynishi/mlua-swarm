//! `InMemoryBlueprintStore` — an in-memory backend used in tests.
//!
//! Layout:
//! `BTreeMap<(BlueprintId, BlueprintVersion), Blueprint>` +
//! `HashMap<BlueprintId, head>`. `head_history` is an append-only `Vec`
//! so `history()` can reconstruct lineage.

use super::types::*;
use super::{blueprint_version, canonical_yaml, BlueprintStore};
use crate::blueprint::Blueprint;
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// In-memory backend, used in tests and as a blank default.
#[derive(Default)]
pub struct InMemoryBlueprintStore {
    inner: Mutex<InMemoryInner>,
}

#[derive(Default)]
struct InMemoryInner {
    /// (id, version) → Blueprint payload
    objects: BTreeMap<(BlueprintId, BlueprintVersion), StoredObject>,
    /// id → current head version.
    heads: HashMap<BlueprintId, BlueprintVersion>,
    /// id → version history (newest to oldest, head included).
    history: HashMap<BlueprintId, Vec<BlueprintVersion>>,
}

struct StoredObject {
    blueprint: Blueprint,
    trace: Trace,
    rationale: String,
}

impl InMemoryBlueprintStore {
    /// Start with an empty store (no ids, no objects).
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl BlueprintStore for InMemoryBlueprintStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    async fn read_head(&self, id: &BlueprintId) -> Result<Traced<Blueprint>, BlueprintStoreError> {
        let inner = self.inner.lock().unwrap();
        let head = inner
            .heads
            .get(id)
            .ok_or_else(|| BlueprintStoreError::HeadEmpty(id.clone()))?;
        let obj = inner
            .objects
            .get(&(id.clone(), *head))
            .ok_or(BlueprintStoreError::NotFound {
                id: id.clone(),
                version: *head,
            })?;
        Ok(Traced::new(obj.blueprint.clone(), obj.trace.clone()))
    }

    async fn write_new(
        &self,
        id: &BlueprintId,
        new_bp: &Blueprint,
        parents: &[BlueprintVersion],
        metadata: CommitMetadata,
    ) -> Result<BlueprintVersion, BlueprintStoreError> {
        // Materialize the canonical bytes and compute their ContentHash.
        let _yaml = canonical_yaml(new_bp)?;
        let version = blueprint_version(new_bp)?;

        let parents_refs: Vec<TraceRef> = parents
            .iter()
            .map(|p| TraceRef {
                origin: TraceOrigin::Inline,
                hash: p.0,
            })
            .collect();

        let trace = Trace::new(
            TraceOrigin::Inline,
            version,
            metadata.epoch_id.started_at_ms,
        )
        .with_parents(parents_refs);

        let mut inner = self.inner.lock().unwrap();
        inner.objects.insert(
            (id.clone(), version),
            StoredObject {
                blueprint: new_bp.clone(),
                trace,
                rationale: metadata.rationale.clone(),
            },
        );
        inner.heads.insert(id.clone(), version);
        inner
            .history
            .entry(id.clone())
            .or_default()
            .insert(0, version);
        let _ = metadata;
        Ok(version)
    }

    async fn read_commit_rationale(
        &self,
        id: &BlueprintId,
        version: BlueprintVersion,
    ) -> Result<Option<String>, BlueprintStoreError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .objects
            .get(&(id.clone(), version))
            .map(|o| o.rationale.clone()))
    }

    async fn read_version(
        &self,
        id: &BlueprintId,
        version: BlueprintVersion,
    ) -> Result<Traced<Blueprint>, BlueprintStoreError> {
        let inner = self.inner.lock().unwrap();
        let obj =
            inner
                .objects
                .get(&(id.clone(), version))
                .ok_or(BlueprintStoreError::NotFound {
                    id: id.clone(),
                    version,
                })?;
        Ok(Traced::new(obj.blueprint.clone(), obj.trace.clone()))
    }

    async fn history(
        &self,
        id: &BlueprintId,
        limit: usize,
    ) -> Result<Vec<BlueprintVersion>, BlueprintStoreError> {
        let inner = self.inner.lock().unwrap();
        let hist = inner.history.get(id).cloned().unwrap_or_default();
        Ok(hist.into_iter().take(limit).collect())
    }

    async fn list_ids(&self) -> Result<Vec<BlueprintId>, BlueprintStoreError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.heads.keys().cloned().collect())
    }
}
