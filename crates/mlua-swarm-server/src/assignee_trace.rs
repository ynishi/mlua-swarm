//! The assignment axis of a Run's trace rail — model §4.6 **W4**:
//! *"担当の事象は step の事象と同じ trace に並べる"* (the assignment
//! events go on the same trace as the step events, so a reader never has
//! to line two streams up against each other).
//!
//! A seat changes hands in five places, and they are spread across three
//! modules with three different handles in scope: the HTTP surface writes
//! three of them ([`crate::tasks`] — launch pin, launch-time auto-seat,
//! `POST /v1/runs/:id/acquire`), the login path writes the **O8** cascade
//! ([`crate::operator_ws::login`]), and the router writes the **A7**
//! release ([`crate::operator_ws::router`]). This module is the one
//! spelling of the event those five sites share: same kinds, same payload
//! shape, same vocabulary for *why* a seat emptied.
//!
//! It deliberately takes a [`TraceHandle`] rather than an `AppState`: the
//! **A7** site is below the HTTP surface and holds stores, not state (see
//! [`crate::operator_ws::router::AssigneeRouter`]), and **R7** names that
//! site specifically. A helper only three of the five callers could reach
//! would have left the one event the model asks for by name unwritten.
//!
//! Appends are best-effort, inheriting [`TraceHandle`]'s fail-open
//! contract: a seat that changed hands but could not be traced is still a
//! seat that changed hands, and refusing the handover over an
//! observability write would be the tail wagging the dog.

use mlua_swarm::store::run::Assignee;
use mlua_swarm::store::trace::{kind as trace_kind, TraceHandle};
use serde_json::json;

/// Which of the three assigning paths wrote a holder into a seat — the
/// `source` field of a `core.assignee_assigned` payload.
///
/// **A9** already requires every `Assign` to carry a human-written
/// `desc`, and a reader can usually tell the paths apart by it (see
/// [`crate::tasks::auto_seat_desc`], which says in prose that nobody
/// chose this holder). This enum says the same thing in a field, so a
/// reader filtering the rail does not have to pattern-match English.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AssignSource {
    /// `operator_sid` on the launch request (`POST /v1/tasks` and its
    /// re-kick sibling).
    LaunchPin,
    /// The launch filled a Blueprint-declared seat from an operator
    /// registered under the seat's own name (no pin was given).
    AutoSeat,
    /// `POST /v1/runs/:id/acquire` — the one handover verb.
    Acquire,
}

impl AssignSource {
    /// The wire label. Snake case, matching the `kind` strings' own
    /// convention.
    fn as_str(self) -> &'static str {
        match self {
            Self::LaunchPin => "launch_pin",
            Self::AutoSeat => "auto_seat",
            Self::Acquire => "acquire",
        }
    }
}

/// Why a seat lost its holder — the `reason` field of a
/// `core.assignee_released` payload, and the whole of the model's list.
///
/// The model reaches `Vacant` exactly three ways (see the [`crate::tasks`]
/// module doc: there is deliberately no route that empties a seat), so
/// this enum is closed by the same argument. A fourth variant would mean
/// a fourth way had been added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReleaseReason {
    /// **A7** — the holder was `Disconnected` when a dispatch read it, so
    /// the seat was released at reference time. **R7** ("system 起因で
    /// Assign が外れた ⟹ その event を trace に記録する") is about this
    /// one: nobody asked for the seat to empty, so without a row here the
    /// only trace of it is a dispatch that failed some time later.
    A7Disconnected,
    /// **O8** — the holder's Operator was deleted, and the cascade
    /// released every seat it still held under its sid or any of its role
    /// aliases.
    O8OperatorDeleted,
    /// **A8** — an acquire took the seat. The paired
    /// `core.assignee_assigned` event names the new holder; this row
    /// exists so "when did X lose this Run" is answerable by filtering
    /// one kind, whatever the cause was.
    Displaced,
}

impl ReleaseReason {
    /// The wire label.
    fn as_str(self) -> &'static str {
        match self {
            Self::A7Disconnected => "a7_disconnected",
            Self::O8OperatorDeleted => "o8_operator_deleted",
            Self::Displaced => "displaced",
        }
    }
}

/// Append `core.assignee_assigned` for a seat that just gained `holder`.
///
/// `previous` is the holder this assignment displaced, when there was one
/// — the same value `POST /v1/runs/:id/acquire` answers with (**Q4**),
/// carried onto the rail so the handover is legible from the trace alone.
///
/// Step-less (`step_ref` / `attempt` are both `None`): a holder belongs to
/// the Run, not to a step. The trace rail's two columns are nullable and
/// `core.cancel_requested` already appends this way.
pub(crate) async fn append_assigned(
    trace: &TraceHandle,
    slot: &str,
    holder: &Assignee,
    source: AssignSource,
    previous: Option<&Assignee>,
) {
    trace
        .append(
            trace_kind::ASSIGNEE_ASSIGNED,
            None,
            None,
            json!({
                "slot": slot,
                "assignee": holder,
                "source": source.as_str(),
                "previous": previous,
            }),
        )
        .await;
}

/// Append `core.assignee_released` for a seat that just lost `holder`.
///
/// `holder` is the [`Assignee`] that was actually released — the instance
/// the releasing site *read*, not whoever occupies the seat now. Both
/// release sites are conditional on that reading
/// ([`mlua_swarm::store::run::VacateOutcome`]), and this event is written
/// only on the `Released` arm, so a stale release that wrote nothing
/// leaves no row claiming it did.
pub(crate) async fn append_released(
    trace: &TraceHandle,
    slot: &str,
    holder: &Assignee,
    reason: ReleaseReason,
) {
    trace
        .append(
            trace_kind::ASSIGNEE_RELEASED,
            None,
            None,
            json!({
                "slot": slot,
                "assignee": holder,
                "reason": reason.as_str(),
            }),
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::store::trace::{InMemoryRunTraceStore, RunTraceStore, TraceQuery};
    use mlua_swarm::RunId;
    use std::sync::Arc;

    fn assignee(op: &str, gen: u64) -> Assignee {
        Assignee {
            op: op.to_string(),
            desc: format!("{op} took the seat"),
            gen,
        }
    }

    /// The payload carries the four things the read surfaces need — slot,
    /// op, gen, desc — plus which path wrote it and who it displaced.
    #[tokio::test]
    async fn an_assigned_event_carries_the_seat_the_holder_and_its_predecessor() {
        let store: Arc<dyn RunTraceStore> = Arc::new(InMemoryRunTraceStore::new());
        let run_id = RunId::new();
        let trace = TraceHandle::new(run_id.clone(), store.clone());

        append_assigned(
            &trace,
            "phase-a-op",
            &assignee("S-new", 2),
            AssignSource::Acquire,
            Some(&assignee("S-old", 1)),
        )
        .await;

        let events = store
            .list(&run_id, &TraceQuery::default())
            .await
            .expect("list");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind, trace_kind::ASSIGNEE_ASSIGNED);
        assert!(
            event.step_ref.is_none() && event.attempt.is_none(),
            "a holder belongs to the Run, not to a step: {event:?}"
        );
        assert_eq!(event.payload["slot"], "phase-a-op");
        assert_eq!(event.payload["assignee"]["op"], "S-new");
        assert_eq!(event.payload["assignee"]["gen"], 2);
        assert_eq!(event.payload["assignee"]["desc"], "S-new took the seat");
        assert_eq!(event.payload["source"], "acquire");
        assert_eq!(event.payload["previous"]["op"], "S-old");
    }

    /// A seat filled from nobody says so with an explicit `null` rather
    /// than by omitting the field — the same reason `RunAcquireResponse.
    /// previous` is never skipped.
    #[tokio::test]
    async fn an_assigned_event_on_a_vacant_seat_reports_a_null_predecessor() {
        let store: Arc<dyn RunTraceStore> = Arc::new(InMemoryRunTraceStore::new());
        let run_id = RunId::new();
        let trace = TraceHandle::new(run_id.clone(), store.clone());

        append_assigned(
            &trace,
            "phase-a-op",
            &assignee("S-first", 1),
            AssignSource::LaunchPin,
            None,
        )
        .await;

        let events = store
            .list(&run_id, &TraceQuery::default())
            .await
            .expect("list");
        assert!(events[0].payload["previous"].is_null());
        assert_eq!(events[0].payload["source"], "launch_pin");
    }

    /// Every loss reason has a distinct label, so a reader can tell a
    /// system-initiated release (**A7** / **O8**) from a handover.
    #[tokio::test]
    async fn a_released_event_names_the_reason_the_seat_emptied() {
        let store: Arc<dyn RunTraceStore> = Arc::new(InMemoryRunTraceStore::new());
        let run_id = RunId::new();
        let trace = TraceHandle::new(run_id.clone(), store.clone());

        for reason in [
            ReleaseReason::A7Disconnected,
            ReleaseReason::O8OperatorDeleted,
            ReleaseReason::Displaced,
        ] {
            append_released(&trace, "phase-a-op", &assignee("S-gone", 1), reason).await;
        }

        let events = store
            .list(&run_id, &TraceQuery::default())
            .await
            .expect("list");
        let labels: Vec<&str> = events
            .iter()
            .map(|e| e.payload["reason"].as_str().expect("a reason label"))
            .collect();
        assert_eq!(
            labels,
            vec!["a7_disconnected", "o8_operator_deleted", "displaced"]
        );
        assert!(events
            .iter()
            .all(|e| e.kind == trace_kind::ASSIGNEE_RELEASED));
        assert_eq!(events[0].payload["assignee"]["op"], "S-gone");
    }
}
