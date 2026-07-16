//! Persistence layer — the sibling of `application` / `service` in the
//! Layered decomposition.
//!
//! Every entry in this module is either a `Store` trait or one of its
//! backend implementations. Domain entities themselves (`Blueprint`,
//! `EnhanceSetting`, `EnhanceLogEntry`, …) live in their originating
//! domain modules; the traits here describe the persistence contract
//! and nothing else.
//!
//! `Blueprint` is the one exception: its store lives at
//! [`crate::blueprint::store`] because the Git-backed backend is deeply
//! tied to `BlueprintVersion` / `ContentHash` / commit-graph semantics
//! and does not fit the flat-CRUD shape used here.

pub mod enhance_log;
pub mod enhance_setting;
pub mod issue;
pub mod output;
pub mod replay;
pub mod run;
pub mod task;
