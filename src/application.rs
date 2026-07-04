//! Application axis — the per-input (IN) use-case entry points that
//! drive processing on top of the Engine.
//!
//! The Engine itself is a pure execution surface. Each Application
//! owns its own input source (an HTTP POST body, an `IssueStore`
//! queue, and so on) and delegates the shared domain operations to
//! [`crate::service::TaskLaunchService`]. Applications do not talk to
//! each other directly — when they need to, they share a
//! `BlueprintStore` as the hub.
//!
//! Applications implemented today:
//!
//! - [`TaskApplication`] — the `POST /v1/tasks` path. Resolves a
//!   `BlueprintRef` (Inline / Id) and starts one task on the Engine
//!   through `TaskLaunchService`.
//! - [`EnhanceApplication`] — the `POST /v1/issues` path. Enqueues on
//!   the `IssueStore`, pops the pending item, fetches the EnhanceBP
//!   head, and starts one task on the Engine through
//!   `TaskLaunchService`. In this model, an issue and a task are one
//!   and the same at the Engine level.

use async_trait::async_trait;

/// The `POST /v1/issues` dispatcher — see [`enhance::EnhanceApplication`].
pub mod enhance;
/// The `POST /v1/tasks` entry point — see [`task::TaskApplication`].
pub mod task;

/// Shared `VersionSelector::SemverReq` resolution — used by both
/// [`task::TaskApplication`] and [`enhance::EnhanceApplication`], each of
/// which maps [`semver_resolve::SemverResolveError`] into its own
/// public error enum.
pub(crate) mod semver_resolve {
    use crate::blueprint::store::{
        BlueprintId, BlueprintStore, BlueprintStoreError, BlueprintVersion,
    };

    /// Failure modes of [`resolve_semver`].
    pub(crate) enum SemverResolveError {
        /// The `BlueprintStore` returned an error while scanning history.
        Store(BlueprintStoreError),
        /// A stored version's `version_label` is not valid semver.
        InvalidSemver {
            /// The offending label string.
            label: String,
            /// The underlying semver parse error.
            source: semver::Error,
        },
        /// No stored version's label satisfies `req`.
        NoMatchingVersion {
            /// The requirement string that matched nothing.
            req: String,
        },
    }

    /// Scan `id`'s history (newest 1024 commits) and pick the highest
    /// version whose `BlueprintMetadata.version_label` parses as semver
    /// and satisfies `req`. Versions with no label, or with a label that
    /// doesn't satisfy `req`, are skipped; a label that fails to parse as
    /// semver is a hard error (`InvalidSemver`) rather than a skip.
    pub(crate) async fn resolve_semver(
        store: &dyn BlueprintStore,
        id: &BlueprintId,
        req: &semver::VersionReq,
    ) -> Result<BlueprintVersion, SemverResolveError> {
        let versions = store
            .history(id, 1024)
            .await
            .map_err(SemverResolveError::Store)?;
        let mut candidates: Vec<(semver::Version, BlueprintVersion)> =
            Vec::with_capacity(versions.len());
        for v in versions {
            let traced = store
                .read_version(id, v)
                .await
                .map_err(SemverResolveError::Store)?;
            let Some(label) = traced.value.metadata.version_label.as_deref() else {
                continue;
            };
            let sv =
                semver::Version::parse(label).map_err(|e| SemverResolveError::InvalidSemver {
                    label: label.to_string(),
                    source: e,
                })?;
            if req.matches(&sv) {
                candidates.push((sv, v));
            }
        }
        candidates.sort_by(|a, b| b.0.cmp(&a.0));
        candidates
            .into_iter()
            .next()
            .map(|(_, v)| v)
            .ok_or_else(|| SemverResolveError::NoMatchingVersion {
                req: req.to_string(),
            })
    }
}

pub use enhance::{
    EnhanceApplication, EnhanceApplicationConfig, EnhanceApplicationError, EnhanceApplicationInput,
    TickOutcome,
};
pub use task::{
    BlueprintRef, TaskApplication, TaskApplicationError, TaskApplicationInput,
    TaskApplicationOutput, VersionSelector,
};

/// An Application is a peer unit of `(input → internal processing →
/// output)`.
#[async_trait]
pub trait Application: Send + Sync {
    /// The request type this Application accepts.
    type Input: Send;
    /// The result type returned on success.
    type Output: Send;
    /// The error type returned on failure.
    type Error: Send + std::fmt::Debug;

    /// A short identifier for this Application instance (used in logs and
    /// diagnostics).
    fn name(&self) -> &str;

    /// Process one `Input` and produce an `Output`, or fail with `Error`.
    async fn handle(&self, input: Self::Input) -> Result<Self::Output, Self::Error>;
}
