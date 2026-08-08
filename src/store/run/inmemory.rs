//! `InMemoryRunStore` — a process-volatile `RunStore` used by the current
//! default.

use super::{
    Assignee, DegradationEntry, Inner, RunId, RunListFilter, RunRecord, RunStatus, RunStore,
    RunStoreError, SharedInner, StepEntry, TaskId,
};
use async_trait::async_trait;
use std::sync::Mutex;

/// Process-volatile [`RunStore`] used as the current default. Entries are
/// lost on restart; persistent backends (SQLite / Git / mini-app / …) are
/// future carries.
#[derive(Default)]
pub struct InMemoryRunStore {
    inner: SharedInner,
}

impl InMemoryRunStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

#[async_trait]
impl RunStore for InMemoryRunStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    async fn create(&self, record: RunRecord) -> Result<(), RunStoreError> {
        // **A2** on the way in — see `RunStore::create`. Checked before the
        // lock so a rejected record never touches the map.
        record.validate_assignment_generations()?;
        let mut inner = self.inner.lock().unwrap();
        if inner.records.contains_key(&record.id) {
            return Err(RunStoreError::Duplicate(record.id));
        }
        inner.order.push(record.id.clone());
        inner.records.insert(record.id.clone(), record);
        Ok(())
    }

    async fn get(&self, id: &RunId) -> Result<RunRecord, RunStoreError> {
        let inner = self.inner.lock().unwrap();
        inner
            .records
            .get(id)
            .cloned()
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))
    }

    async fn list_by_task(&self, task_id: &TaskId) -> Result<Vec<RunRecord>, RunStoreError> {
        let inner = self.inner.lock().unwrap();
        let mut records: Vec<RunRecord> = inner
            .order
            .iter()
            .filter_map(|id| inner.records.get(id).cloned())
            .filter(|r| &r.task_id == task_id)
            .collect();
        records.sort_by_key(|r| r.created_at);
        Ok(records)
    }

    async fn append_step_entry(&self, id: &RunId, entry: StepEntry) -> Result<(), RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))?;
        record.step_entries.push(entry);
        record.updated_at = crate::types::now_unix();
        Ok(())
    }

    async fn append_degradation(
        &self,
        id: &RunId,
        entry: DegradationEntry,
    ) -> Result<(), RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))?;
        record.degradations.push(entry);
        record.updated_at = crate::types::now_unix();
        Ok(())
    }

    async fn update_status(&self, id: &RunId, status: RunStatus) -> Result<(), RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))?;
        record.status = status;
        record.updated_at = crate::types::now_unix();
        Ok(())
    }

    async fn try_transition(
        &self,
        id: &RunId,
        from: RunStatus,
        to: RunStatus,
    ) -> Result<bool, RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        // Held under the single `inner` mutex, so the read + compare + set
        // is atomic against any other appender/transition. An absent row or
        // a status mismatch both report `false` (the caller's race signal),
        // not an error.
        match inner.records.get_mut(id) {
            Some(record) if record.status == from => {
                record.status = to;
                record.updated_at = crate::types::now_unix();
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn acquire_assignee(
        &self,
        id: &RunId,
        slot: &str,
        op: &str,
        desc: &str,
    ) -> Result<(u64, Option<Assignee>), RunStoreError> {
        // A9 (and the slot's own requirement): refuse before taking the
        // lock — a rejected acquire must not burn a generation.
        if slot.is_empty() {
            return Err(RunStoreError::AssigneeSlotRequired);
        }
        if desc.trim().is_empty() {
            return Err(RunStoreError::AssigneeDescRequired);
        }
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))?;
        // A4: the bump is unconditional (an Assign to the incumbent still
        // advances the counter) and Run-wide (a different slot advances the
        // same counter). Held under the single `inner` mutex with no await
        // in between, so concurrent acquires cannot read the same
        // generation. A8: no precondition on the incumbent.
        record.next_generation += 1;
        // Q3: `insert` returns the previous holder OF THIS SLOT, moved out
        // whole — it is handed back with its `gen` intact (A3), never
        // rewritten. Other slots' entries are not read or touched.
        let previous = record.current.insert(
            slot.to_string(),
            Assignee {
                op: op.to_string(),
                desc: desc.to_string(),
                gen: record.next_generation,
            },
        );
        record.updated_at = crate::types::now_unix();
        Ok((record.next_generation, previous))
    }

    async fn vacate_assignee(
        &self,
        id: &RunId,
        slot: &str,
    ) -> Result<(u64, Option<Assignee>), RunStoreError> {
        if slot.is_empty() {
            return Err(RunStoreError::AssigneeSlotRequired);
        }
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))?;
        // A4: Vacant bumps `G` exactly like Assign does; it just mints no
        // Assignee, so the next acquire continues from the bumped value.
        record.next_generation += 1;
        // Only this slot's key leaves the map — a Vacant is per seat.
        let previous = record.current.remove(slot);
        record.updated_at = crate::types::now_unix();
        Ok((record.next_generation, previous))
    }

    async fn set_result(
        &self,
        id: &RunId,
        result_ref: serde_json::Value,
    ) -> Result<(), RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))?;
        record.result_ref = Some(result_ref);
        record.updated_at = crate::types::now_unix();
        Ok(())
    }

    async fn set_input_json(&self, id: &RunId, input_json: String) -> Result<(), RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .records
            .get_mut(id)
            .ok_or_else(|| RunStoreError::NotFound(id.clone()))?;
        record.input_json = Some(input_json);
        record.updated_at = crate::types::now_unix();
        Ok(())
    }

    async fn list_running(&self) -> Result<Vec<RunRecord>, RunStoreError> {
        let inner = self.inner.lock().unwrap();
        let records: Vec<RunRecord> = inner
            .order
            .iter()
            .filter_map(|id| inner.records.get(id).cloned())
            .filter(|r| r.status == RunStatus::Running)
            .collect();
        Ok(records)
    }

    async fn list(&self, filter: &RunListFilter) -> Result<Vec<RunRecord>, RunStoreError> {
        let inner = self.inner.lock().unwrap();
        let mut records: Vec<RunRecord> = inner
            .order
            .iter()
            .filter_map(|id| inner.records.get(id).cloned())
            .filter(|r| {
                filter
                    .task_id
                    .as_ref()
                    .map(|t| &r.task_id == t)
                    .unwrap_or(true)
                    && filter.status.map(|s| r.status == s).unwrap_or(true)
            })
            .collect();
        // Newest-first; `order` index breaks `created_at` ties stably
        // (later insertion sorts first within the same second).
        records.reverse();
        records.sort_by_key(|r| std::cmp::Reverse(r.created_at));
        let offset = filter.offset.unwrap_or(0);
        let records: Vec<RunRecord> = records
            .into_iter()
            .skip(offset)
            .take(filter.limit.unwrap_or(usize::MAX))
            .collect();
        Ok(records)
    }

    async fn delete(&self, id: &RunId) -> Result<(), RunStoreError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.records.remove(id).is_none() {
            return Err(RunStoreError::NotFound(id.clone()));
        }
        inner.order.retain(|r| r != id);
        Ok(())
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
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.task_id, TaskId::parse("T-1").unwrap());
        assert_eq!(got.status, RunStatus::Pending);
        assert!(got.step_entries.is_empty());
    }

    /// **A2** at the one door that takes a caller-built record. Seeding
    /// `current[slot].gen = 99` against `next_generation = 0` would be
    /// permanent: the next acquire stamps generation 1, *below* the
    /// incumbent, and ordering two holders by `gen` — the reason `G` is
    /// Run-wide — silently inverts. So it is refused, and nothing is
    /// stored.
    #[tokio::test]
    async fn create_rejects_a_holder_generation_above_the_counter() {
        let s = InMemoryRunStore::new();
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
        match err {
            RunStoreError::AssigneeGenerationAhead {
                slot,
                gen,
                next_generation,
            } => {
                assert_eq!(slot, SLOT_A);
                assert_eq!(gen, 99);
                assert_eq!(next_generation, 0);
            }
            other => panic!("got: {other:?}"),
        }
        assert!(
            matches!(
                s.get(&RunId::parse("R-1").unwrap()).await.unwrap_err(),
                RunStoreError::NotFound(_)
            ),
            "a refused create must leave no row behind"
        );
    }

    /// The boundary is `>`, not `>=`: a record whose holder was stamped at
    /// exactly `G` satisfies `a.gen ≤ G` and is the normal shape of a Run
    /// that has been assigned once, so round-tripping one through `create`
    /// must keep working.
    #[tokio::test]
    async fn create_accepts_a_holder_generation_equal_to_the_counter() {
        let s = InMemoryRunStore::new();
        let mut record = mk("R-1", "T-1", 100);
        record.next_generation = 1;
        record.current.insert(
            SLOT_A.to_string(),
            Assignee {
                op: "S-a1".to_string(),
                desc: "stamped at G".to_string(),
                gen: 1,
            },
        );

        s.create(record).await.unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.current[SLOT_A].gen, 1);
    }

    #[tokio::test]
    async fn duplicate_create_rejected() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let err = s.create(mk("R-1", "T-1", 200)).await.unwrap_err();
        assert!(matches!(err, RunStoreError::Duplicate(_)));
    }

    #[tokio::test]
    async fn get_missing_returns_not_found() {
        let s = InMemoryRunStore::new();
        let err = s.get(&RunId::parse("R-nope").unwrap()).await.unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn list_by_task_filters_and_orders_ascending() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 300)).await.unwrap();
        s.create(mk("R-2", "T-2", 50)).await.unwrap();
        s.create(mk("R-3", "T-1", 100)).await.unwrap();
        let list = s
            .list_by_task(&TaskId::parse("T-1").unwrap())
            .await
            .unwrap();
        let ids: Vec<_> = list.iter().map(|r| r.id.to_string()).collect();
        assert_eq!(ids, vec!["R-3", "R-1"]);
    }

    #[tokio::test]
    async fn append_step_entry_accumulates_in_order() {
        let s = InMemoryRunStore::new();
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
        assert!(got.updated_at >= got.created_at);
    }

    #[tokio::test]
    async fn append_degradation_accumulates_in_order() {
        let s = InMemoryRunStore::new();
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
        assert!(got.updated_at >= got.created_at);
    }

    #[tokio::test]
    async fn append_degradation_unknown_run_fails() {
        let s = InMemoryRunStore::new();
        let err = s
            .append_degradation(
                &RunId::parse("R-nope").unwrap(),
                mk_degradation("web_search", 1),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn append_degradation_bumps_updated_at() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.append_degradation(
            &RunId::parse("R-1").unwrap(),
            mk_degradation("web_search", 200),
        )
        .await
        .unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert!(got.updated_at > 100);
    }

    #[tokio::test]
    async fn append_step_entry_unknown_run_fails() {
        let s = InMemoryRunStore::new();
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
    }

    #[tokio::test]
    async fn update_status_persists() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.update_status(&RunId::parse("R-1").unwrap(), RunStatus::Running)
            .await
            .unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.status, RunStatus::Running);
    }

    #[tokio::test]
    async fn set_result_persists() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.set_result(&RunId::parse("R-1").unwrap(), json!({"ok": true}))
            .await
            .unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(got.result_ref, Some(json!({"ok": true})));
    }

    #[tokio::test]
    async fn name_is_in_memory() {
        assert_eq!(InMemoryRunStore::new().name(), "in-memory");
    }

    #[tokio::test]
    async fn list_running_filters_by_status() {
        let s = InMemoryRunStore::new();
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
    }

    #[tokio::test]
    async fn try_transition_flips_on_match_and_is_idempotent_under_race() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        s.update_status(&RunId::parse("R-1").unwrap(), RunStatus::Interrupted)
            .await
            .unwrap();

        // First CAS matches `Interrupted` and flips to `Running`.
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

        // Second CAS (a racing double-resume) no longer sees `Interrupted`
        // and must report `false` without touching the row.
        let second = s
            .try_transition(
                &RunId::parse("R-1").unwrap(),
                RunStatus::Interrupted,
                RunStatus::Running,
            )
            .await
            .unwrap();
        assert!(!second, "second CAS must not flip a now-Running row");
    }

    #[tokio::test]
    async fn try_transition_absent_run_reports_false() {
        let s = InMemoryRunStore::new();
        let flipped = s
            .try_transition(
                &RunId::parse("R-nope").unwrap(),
                RunStatus::Interrupted,
                RunStatus::Running,
            )
            .await
            .unwrap();
        assert!(!flipped, "an absent Run must report false, not error");
    }

    // ── assignment axis (model §4.3) ──────────────────────────────────

    /// The two slots (Blueprint-declared Operator seats) the tests below
    /// assign to — the shipped per-lane alias shape, where a Blueprint
    /// declares one seat per phase.
    const SLOT_A: &str = "phase-a-op";
    const SLOT_B: &str = "phase-b-op";

    /// A4: a launched Run starts with every slot Vacant and `G == 0` — the
    /// counter is not pre-advanced by the launch itself.
    #[tokio::test]
    async fn launch_starts_vacant_at_generation_zero() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert!(got.current.is_empty(), "no slot is held at launch");
        assert_eq!(got.next_generation, 0);
    }

    /// A4: every event advances `G` by one, and the FIRST Assign lands on
    /// `1`. A8: re-acquiring for the incumbent still succeeds and still
    /// advances — the counter tracks events, not state changes.
    #[tokio::test]
    async fn acquire_advances_generation_even_for_the_same_op() {
        let s = InMemoryRunStore::new();
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
        assert_eq!(got.current[SLOT_A].gen, 2);
    }

    /// The slots are independent: assigning one leaves every other Vacant.
    #[tokio::test]
    async fn assigning_one_slot_leaves_the_others_vacant() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let id = RunId::parse("R-1").unwrap();

        s.acquire_assignee(&id, SLOT_A, "S-a1", "holds phase a")
            .await
            .unwrap();

        let got = s.get(&id).await.unwrap();
        assert_eq!(got.current[SLOT_A].op, "S-a1");
        assert!(
            !got.current.contains_key(SLOT_B),
            "an unassigned slot has no entry — that absence IS its Vacant"
        );
    }

    /// A4 is Run-wide, not per slot: interleaved assignments to two slots
    /// walk ONE counter, so any two holders can be ordered by `gen`.
    #[tokio::test]
    async fn the_generation_counter_is_shared_across_slots() {
        let s = InMemoryRunStore::new();
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
        assert!(
            got.current[SLOT_A].gen > got.current[SLOT_B].gen,
            "holders of different slots stay comparable by gen"
        );
    }

    /// A4 (Vacant side): releasing bumps `G` too, so the next Assign picks
    /// up from the bumped value rather than reusing the released one.
    #[tokio::test]
    async fn vacate_advances_generation_and_clears_the_holder() {
        let s = InMemoryRunStore::new();
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
    }

    /// A Vacant applies to the named slot only — the other seats keep the
    /// holders they had.
    #[tokio::test]
    async fn vacate_releases_only_the_named_slot() {
        let s = InMemoryRunStore::new();
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
    }

    /// A4: vacating an already-Vacant slot is a real event, not a no-op.
    #[tokio::test]
    async fn vacate_on_a_vacant_run_still_advances_generation() {
        let s = InMemoryRunStore::new();
        s.create(mk("R-1", "T-1", 100)).await.unwrap();
        let id = RunId::parse("R-1").unwrap();
        let (gen, released) = s.vacate_assignee(&id, SLOT_A).await.unwrap();
        assert_eq!(gen, 1);
        assert_eq!(released, None);
    }

    /// A3 / Q3: an acquire mints a NEW `Assignee`; a handle taken before it
    /// still reads its original generation afterwards, and the displaced
    /// holder is handed back with that same stamp.
    #[tokio::test]
    async fn acquire_never_rewrites_the_incumbent_assignee() {
        let s = InMemoryRunStore::new();
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
    }

    /// A8: acquire has no precondition on the slot's incumbent — the later
    /// caller wins outright, no exclusion, no rejection.
    #[tokio::test]
    async fn acquire_displaces_a_live_holder() {
        let s = InMemoryRunStore::new();
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
        assert_eq!(got.current[SLOT_A].op, "S-b2", "last writer wins");
        assert_eq!(got.current.len(), 1, "A1: still one holder for that slot");
    }

    /// A9: `desc` is mandatory, and so is the slot. A rejected acquire must
    /// not have burned a generation or disturbed the incumbent.
    #[tokio::test]
    async fn acquire_rejects_a_missing_desc_without_side_effects() {
        let s = InMemoryRunStore::new();
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
    }

    /// Concurrent acquires must never read the same `G`, so no two callers
    /// can be told they hold the same generation — including when they name
    /// different slots, since the counter is Run-wide.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_acquires_hand_out_distinct_generations() {
        let s = std::sync::Arc::new(InMemoryRunStore::new());
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
        assert_eq!(
            s.get(&RunId::parse("R-1").unwrap())
                .await
                .unwrap()
                .next_generation,
            8
        );
    }

    #[tokio::test]
    async fn assignment_on_an_unknown_run_fails() {
        let s = InMemoryRunStore::new();
        let missing = RunId::parse("R-nope").unwrap();
        let err = s
            .acquire_assignee(&missing, SLOT_A, "S-a1", "hold")
            .await
            .unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)), "got: {err:?}");
        let err = s.vacate_assignee(&missing, SLOT_A).await.unwrap_err();
        assert!(matches!(err, RunStoreError::NotFound(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn input_json_roundtrips_through_create_get() {
        let s = InMemoryRunStore::new();
        let mut rec = mk("R-1", "T-1", 100);
        rec.input_json = Some(r#"{"blueprint":"snapshot"}"#.to_string());
        s.create(rec).await.unwrap();
        let got = s.get(&RunId::parse("R-1").unwrap()).await.unwrap();
        assert_eq!(
            got.input_json.as_deref(),
            Some(r#"{"blueprint":"snapshot"}"#)
        );
    }
}
