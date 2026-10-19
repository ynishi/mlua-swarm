//! Per-agent concurrency limits (`AgentDef.max_concurrency`).
//!
//! A limit is counted across every run on one [`crate::Engine`], keyed by
//! Blueprint id + agent name: a `fanout` that starts N lanes of the same
//! agent, and N runs of the same Blueprint started side by side, share
//! one set of slots. Narrower (per-run) counting would let the number of
//! concurrent steps grow with the number of runs, which is the case the
//! limit exists for — an LLM provider's rate limit is per organization,
//! not per run.
//!
//! The table only hands out semaphores; a step holds a permit for as long
//! as its dispatch lasts (see `EngineDispatcher::dispatch`). Waiting is
//! `tokio::sync::Semaphore::acquire_owned`: waiters are served in arrival
//! order, and a waiter whose future is dropped (its run was cancelled)
//! leaves the queue without taking a slot.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

/// The engine-wide table of per-(Blueprint, agent) semaphores.
#[derive(Default)]
pub(crate) struct ConcurrencyLimits {
    // A plain mutex: the critical section only looks up or inserts an
    // `Arc`, and nothing awaits while holding it.
    slots: Mutex<HashMap<(String, String), Slot>>,
}

struct Slot {
    limit: u32,
    semaphore: Arc<Semaphore>,
}

impl ConcurrencyLimits {
    /// The semaphore for `agent` in Blueprint `blueprint_id`, sized to
    /// `limit`. When the Blueprint was re-registered with a different
    /// limit, a new semaphore of the new size replaces the old one: steps
    /// already holding a permit keep it on the old semaphore, and every
    /// later step waits on the new one.
    pub(crate) fn semaphore(&self, blueprint_id: &str, agent: &str, limit: u32) -> Arc<Semaphore> {
        let mut slots = self
            .slots
            .lock()
            .expect("concurrency limits mutex poisoned");
        let slot = slots
            .entry((blueprint_id.to_string(), agent.to_string()))
            .or_insert_with(|| Slot {
                limit,
                semaphore: Arc::new(Semaphore::new(limit as usize)),
            });
        if slot.limit != limit {
            *slot = Slot {
                limit,
                semaphore: Arc::new(Semaphore::new(limit as usize)),
            };
        }
        Arc::clone(&slot.semaphore)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_key_and_limit_shares_one_semaphore() {
        let t = ConcurrencyLimits::default();
        let a = t.semaphore("bp", "coder", 2);
        let b = t.semaphore("bp", "coder", 2);
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn different_blueprint_or_agent_gets_its_own_semaphore() {
        let t = ConcurrencyLimits::default();
        let a = t.semaphore("bp", "coder", 2);
        assert!(!Arc::ptr_eq(&a, &t.semaphore("bp2", "coder", 2)));
        assert!(!Arc::ptr_eq(&a, &t.semaphore("bp", "reader", 2)));
    }

    #[test]
    fn a_changed_limit_replaces_the_semaphore_with_the_new_size() {
        let t = ConcurrencyLimits::default();
        let old = t.semaphore("bp", "coder", 2);
        let new = t.semaphore("bp", "coder", 5);
        assert!(!Arc::ptr_eq(&old, &new));
        assert_eq!(new.available_permits(), 5);
        assert_eq!(old.available_permits(), 2);
    }
}
