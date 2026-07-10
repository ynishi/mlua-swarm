//! `McpQueryAdapter` — server-side [`ProjectionAdapter`], and the REST
//! hierarchy that serves a Run's step OUTPUT as metadata + content
//! (`projection-adapter` ST5's HTTP debug plane — replaces the ST2/ST4
//! `GET /v1/tasks/:id/ctx` single-value endpoint / `ProjectionResponse`).
//!
//! # Two consumers, two roles (ST5)
//!
//! - **Worker axis** (`crates/mlua-swarm-server/src/worker.rs`'s `GET
//!   /v1/worker/prompt` handler) — the *primary* supply path. A worker's
//!   fetch payload carries `context.steps: Vec<StepPointer>`, a
//!   `ContextPolicy.steps`-filtered pointer list assembled automatically at
//!   fetch time; no separate tool call needed.
//! - **HTTP debug plane** (this module's `GET
//!   /v1/tasks/:id/runs/:run/steps*` routes) — the content the above
//!   pointers' `content_url` addresses, plus an unfiltered metadata/content
//!   view for operators / humans debugging a run.
//!
//! # GH #23 subtask-3: table-driven addressing (replaces the runtime union
//! rule)
//!
//! Both consumers share [`McpQueryAdapter::list_steps`]'s enumeration.
//! Previously every distinct `step_ref` name in `RunRecord.step_entries`
//! was resolved through the Data-plane `OutputStore`, **unioned** with
//! `RunRecord.result_ref`'s top-level object keys (the finalized-Run
//! fallback), Data-plane winning a name collision — the pre-GH-#23 runtime
//! union rule. That rule is now statically replaced: every real
//! `Compiler::compile` output carries a
//! [`mlua_swarm::core::step_naming::StepNaming`] table (built once, at
//! compile time — see that module's doc), and this module's enumeration /
//! single-key resolution ([`Self::enumerate_steps`] /
//! [`Self::resolve_async`]) look the table up via
//! `Engine::step_naming_for` and report every step under its ONE canonical
//! name, addressable by that name OR any alias (`Step.ref` / the `out`
//! ctx-path's top-level segment). The runtime union / collision-priority
//! logic itself no longer runs per-request; it is baked into the table
//! once, at register time (`StepNaming::from_blueprint`).
//!
//! [`Self::enumerate_steps_legacy_union`] keeps the OLD runtime-union body
//! verbatim as a **defensive-only** fallback for the rare case no table
//! resolves (a spawn stack the dispatcher never wired
//! `EngineDispatcher::with_step_naming` for — certain test harnesses that
//! seed `OutputStore`/`RunStore` fixtures directly without driving a real
//! dispatch); it is not a "declared Blueprints get the new path, undeclared
//! ones keep the old one" branch — undeclared Blueprints get the SAME
//! table-driven path (their canonical name is simply their own `Step.ref`,
//! byte-identical to the pre-GH-#23 name).
//!
//! ## GH #34 subtask-3: `Artifact` findings surfaced alongside a step's own
//! canonical entry
//!
//! [`Self::enumerate_steps_via_table`]'s per-`step_entries`-row loop, after
//! resolving that row's own canonical name, ALSO lists every
//! `OutputEvent::Artifact` dual-written under the SAME `StepId`
//! (`OutputStore::list_for_attempt`) and inserts each under its OWN
//! `name` — e.g. `AfterRunAuditMiddleware`'s `"audit:<step_ref>"` finding
//! (`Engine::submit_output`'s `Artifact` dual-write, see that method's
//! doc). Purely additive: an artifact's name is never a step's canonical
//! producer name, so it can't collide with / override the entry the
//! canonical-name lookup above just inserted.
//!
//! # Architecture (subtask-4 rework, carried into ST5, table-driven since
//! subtask-3)
//!
//! [`McpQueryAdapter`] reads through **two** backings, tried in order, for
//! every step's OWN dispatch (`RunRecord.step_entries` row → its own
//! `StepId`):
//!
//! 1. **Data-plane, in-flight-safe AND Run-scoped** (subtask-4's original
//!    reason for being; Run-scoped since subtask-3 — see the former KNOWN
//!    LIMITATION below): [`McpQueryAdapter::resolve_async`] /
//!    [`Self::enumerate_steps_via_table`] look up
//!    `OutputStore::get_latest_by_name_in_run(step_entry.step_id, 1,
//!    canonical_name)` — the same store `Engine::submit_output`'s
//!    submit-time projection sink dual-writes into (see
//!    `mlua_swarm::core::engine::Engine::submit_output`'s doc), keyed
//!    Run-scoped by construction (a `StepId` is globally unique per
//!    dispatch, so two concurrent Runs sharing a producer name never
//!    cross-resolve — no narrowing-by-guard needed any more). A hit here
//!    can be a **not-yet-finalized** Run's already-submitted step — the
//!    in-flight case subtask-4 exists for.
//! 2. **Persisted `RunRecord.result_ref` fallback** (unchanged in kind,
//!    now tried under the canonical name AND every alias): used whenever
//!    (1) comes back empty (no Data-plane record for that step's own
//!    dispatch yet — e.g. a Run that predates the engine having an
//!    `OutputStore` wired).
//!
//! Unlike `crate::operator_ws::session`'s spawn-time
//! [`mlua_swarm::core::projection::FileProjectionAdapter`] hook (which
//! materializes the *spawning* agent's own `AgentContextView`), this
//! adapter's Data-plane path serves **prior steps'** submitted OUTPUT —
//! the pull-supply counterpart to `Engine`'s submit-time file sink.
//!
//! ## Former KNOWN LIMITATION (closed by GH #23 subtask-3)
//!
//! `OutputStore::get_latest_by_name` is producer-name-scoped, not
//! Run-scoped (see `mlua_swarm::store::output`'s module doc) — it returns
//! the single newest `Final` submitted anywhere under that producer name,
//! across every Run / Task, so two *concurrent* Runs whose flow.ir happens
//! to dispatch an agent of the identical name could race each other. This
//! module no longer calls that method: every lookup here goes through
//! `OutputStore::get_latest_by_name_in_run`, scoped to the dispatching
//! step's own (globally unique) `StepId`, closing the race by
//! construction, independent of whether the Blueprint declared a
//! `projection_name` (see
//! `mlua_swarm::store::output::OutputStore::get_latest_by_name_in_run`'s
//! doc). `get_latest_by_name` itself is untouched (still used by
//! `Engine::submit_output`'s own fail-open cross-Run compatibility path)
//! — only this module's consumption of it changed.
//!
//! [`ProjectionAdapter::fetch`] is a synchronous trait method, but this
//! adapter's backing stores are async. [`McpQueryAdapter::resolve_async`]
//! is the real, native-async implementation; [`step_content`] (the
//! content-plane HTTP handler) calls [`McpQueryAdapter::list_steps`]
//! directly. [`ProjectionAdapter::fetch`] instead bridges to
//! [`McpQueryAdapter::resolve_async`] via `tokio::task::block_in_place` +
//! `Handle::block_on` purely for trait conformance (dependency inversion —
//! this adapter implements the same `core::projection::ProjectionAdapter`
//! trait [`mlua_swarm::core::projection::FileProjectionAdapter`] does, so a
//! caller holding a `dyn ProjectionAdapter` can use either
//! polymorphically); the hot HTTP path never takes that bridge.

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    Json,
};
use mlua_swarm::core::engine::Engine;
use mlua_swarm::core::projection::{
    ProjectionAdapter, ProjectionError, ProjectionKey, ProjectionRef,
};
use mlua_swarm::core::projection_placement::ProjectionPlacement;
use mlua_swarm::core::step_naming::StepNaming;
use mlua_swarm::store::output::{ContentRef, OutputEvent, OutputStore, OutputStoreError};
use mlua_swarm::store::run::{RunRecord, RunStore};
use mlua_swarm::{RunId, StepId, TaskId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest as _;
use std::sync::Arc;

use crate::tasks::map_task_store_err;
use crate::{ApiError, AppState};

/// Server-side [`ProjectionAdapter`] backed by an [`OutputStore`]
/// (in-flight-safe, subtask-4, Run-scoped since GH #23 subtask-3) with a
/// [`RunStore`]-backed `result_ref` fallback (see the module doc for the
/// full narrative). Holds an [`Engine`] handle (GH #23 subtask-3) so
/// [`Self::step_naming_for_run`] can pull the Blueprint-wide
/// [`StepNaming`] table `Engine::step_naming_for` snapshotted at dispatch
/// time.
pub struct McpQueryAdapter {
    data_store: Arc<dyn OutputStore>,
    run_store: Arc<dyn RunStore>,
    engine: Engine,
}

/// Which backing produced a [`StepSummary`] / a Worker-axis `StepPointer`
/// — Data-plane wins a name collision (module doc's "Architecture"
/// section).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionSource {
    /// Resolved via the in-flight-safe `OutputStore::get_latest_by_name`
    /// path.
    DataPlane,
    /// Resolved via the persisted `RunRecord.result_ref` fallback (the Run
    /// has finalized, or the name only ever existed there).
    ResultRef,
}

/// One step's resolved OUTPUT value plus its provenance — the shared
/// enumeration result [`McpQueryAdapter::list_steps`] returns, consumed by
/// both this module's HTTP handlers and
/// `crates/mlua-swarm-server/src/worker.rs`'s Worker-axis pointer
/// assembly.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedStep {
    /// The producing step's name (`RunRecord.step_entries[].step_ref`, or
    /// a `RunRecord.result_ref` top-level key).
    pub(crate) name: String,
    /// The resolved OUTPUT value (not yet path-narrowed).
    pub(crate) value: Value,
    /// Which backing produced this entry.
    pub(crate) source: ProjectionSource,
}

/// Extracts a JSON value out of an [`OutputEvent`]'s content, when the
/// event is a `Final` (anything else — `Progress` / `Partial` / `Artifact`
/// sharing the same producer name via the separate `POST /v1/data/emit`
/// axis — is not a submission this adapter serves, so callers treat
/// `None` the same as "no record").
fn final_value(event: &OutputEvent) -> Option<Value> {
    match event {
        OutputEvent::Final { content, .. } => Some(content_to_value(content)),
        _ => None,
    }
}

/// Renders a [`ContentRef`] down to a plain [`Value`] — `Inline` passes
/// its value through verbatim; `FileRef` (large / binary content) becomes
/// a small locator object (this adapter's `v1` scope does not read the
/// file back, matching subtask-4's spec: "locator 返却で可").
fn content_to_value(content: &ContentRef) -> Value {
    match content {
        ContentRef::Inline { value } => value.clone(),
        ContentRef::FileRef {
            path,
            mime,
            size_hint,
        } => serde_json::json!({
            "file_ref": path.to_string_lossy(),
            "mime": mime,
            "size_hint": size_hint,
        }),
    }
}

/// GH #23 subtask-3: finds the [`StepId`] of the `run.step_entries` row
/// whose canonical name (via `naming.canonical_of_producer`, or the raw
/// `step_ref` unchanged when `naming` is `None`) equals `canonical`. Tried
/// most-recent-first (`.rev()`) so a step re-dispatched under the same ref
/// (e.g. inside a Loop) resolves to its LATEST occurrence within this
/// Run — matching [`resolve_materialized_file`]'s own "only tries `attempt
/// = 1`" convention, this helper does not attempt to disambiguate
/// multiple attempts of the SAME occurrence, only multiple occurrences.
fn find_step_id_for_canonical(
    run: &RunRecord,
    naming: Option<&StepNaming>,
    canonical: &str,
) -> Option<StepId> {
    run.step_entries
        .iter()
        .rev()
        .find(|entry| {
            let Some(step_ref) = entry.step_ref.as_deref() else {
                return false;
            };
            match naming {
                Some(n) => n.canonical_of_producer(step_ref) == Some(canonical),
                None => step_ref == canonical,
            }
        })
        .map(|entry| entry.step_id.clone())
}

/// GH #23 subtask-3: every name worth trying against `RunRecord.result_ref`
/// for `canonical` — the canonical name itself, every alias `naming`
/// records for it (when `naming` resolves an entry), and `raw_step` (the
/// original, un-canonicalized query) as a final fail-open fallback for
/// when `naming` is `None` entirely or does not carry an entry for
/// `canonical` (defensive-only — see [`McpQueryAdapter::step_naming_for_run`]'s
/// doc). Order matters only for determinism (canonical first); a
/// `result_ref` object has at most one of these keys present.
fn candidate_names<'a>(
    naming: Option<&'a StepNaming>,
    canonical: &'a str,
    raw_step: &'a str,
) -> Vec<&'a str> {
    let mut names = vec![canonical];
    if let Some(entry) = naming.and_then(|n| n.entries().find(|e| e.canonical == canonical)) {
        for alias in &entry.aliases {
            if alias != canonical {
                names.push(alias.as_str());
            }
        }
    }
    if !names.contains(&raw_step) {
        names.push(raw_step);
    }
    names
}

impl McpQueryAdapter {
    /// Builds an adapter reading through `data_store` (in-flight-safe,
    /// Run-scoped, tried first) with `run_store`-backed `result_ref`
    /// fallback, and `engine` for the GH #23 subtask-3 `StepNaming` table
    /// lookup.
    pub fn new(
        data_store: Arc<dyn OutputStore>,
        run_store: Arc<dyn RunStore>,
        engine: Engine,
    ) -> Self {
        Self {
            data_store,
            run_store,
            engine,
        }
    }

    /// GH #23 subtask-3: resolves the Blueprint-wide [`StepNaming`] table
    /// for `run` by trying each of its `step_entries`' own `StepId` via
    /// `Engine::step_naming_for` until one resolves — every dispatched
    /// step of one Blueprint launch shares the SAME `Arc`, snapshotted
    /// under every one of their own ids at dispatch time (see
    /// [`StepNaming`]'s module doc), so any entry's id suffices. `None`
    /// when the Run has no `step_entries` yet, or none of them resolve
    /// (a spawn stack that never called
    /// `EngineDispatcher::with_step_naming` — pre-GH-#23 callers / test
    /// harnesses that seed `OutputStore`/`RunStore` fixtures directly) —
    /// callers fall back to [`Self::enumerate_steps_legacy_union`] in
    /// that case (defensive-only; see the module doc). Delegates to the
    /// free-function sibling [`resolve_step_naming_for_run`] (shared with
    /// [`resolve_materialized_file`], which has no `McpQueryAdapter`
    /// handle).
    async fn step_naming_for_run(&self, run: &RunRecord) -> Option<Arc<StepNaming>> {
        resolve_step_naming_for_run(&self.engine, run).await
    }

    /// GH #23 subtask-3: canonicalizes `raw` (a REST `:step` path segment
    /// — the canonical name itself, or any alias) against `run`'s
    /// [`StepNaming`] table, so `step_get` / `step_content` can find the
    /// matching [`ResolvedStep`] by its (always-canonical) `name` — see
    /// [`Self::enumerate_steps`]. Returns `raw` unchanged when no table
    /// resolves for this Run (fail-open, matching
    /// [`Self::step_naming_for_run`]'s own defensive fallback).
    pub(crate) async fn resolve_step_name(&self, run: &RunRecord, raw: &str) -> String {
        match self.step_naming_for_run(run).await {
            Some(naming) => naming.resolve(raw).unwrap_or(raw).to_string(),
            None => raw.to_string(),
        }
    }

    /// Selects the Run `task_id` + `run_id` address: `run_id` when
    /// `Some`, otherwise the most recently created Run for `task_id`
    /// ([`RunStore::list_by_task`] returns oldest-created-first, so its
    /// last element is the latest). [`ProjectionError::NotFound`] covers
    /// every "nothing here" case uniformly: an unparseable `run_id`, an
    /// unknown Run, a `run_id` that names a Run belonging to a *different*
    /// Task, or a Task with no Runs yet.
    async fn resolve_run(
        &self,
        task_id: &TaskId,
        run_id: Option<&str>,
    ) -> Result<RunRecord, ProjectionError> {
        match run_id {
            Some(rid) => {
                let run_id = RunId::parse(rid.to_string())
                    .map_err(|e| ProjectionError::InvalidKey(format!("run_id: {e}")))?;
                let run = self.run_store.get(&run_id).await.map_err(|_| {
                    ProjectionError::NotFound(ProjectionKey {
                        task_id: task_id.to_string(),
                        run_id: Some(rid.to_string()),
                        step: None,
                        path: None,
                    })
                })?;
                if &run.task_id != task_id {
                    return Err(ProjectionError::NotFound(ProjectionKey {
                        task_id: task_id.to_string(),
                        run_id: Some(rid.to_string()),
                        step: None,
                        path: None,
                    }));
                }
                Ok(run)
            }
            None => {
                let mut runs = self.run_store.list_by_task(task_id).await.map_err(|_| {
                    ProjectionError::NotFound(ProjectionKey {
                        task_id: task_id.to_string(),
                        run_id: None,
                        step: None,
                        path: None,
                    })
                })?;
                runs.pop().ok_or_else(|| {
                    ProjectionError::NotFound(ProjectionKey {
                        task_id: task_id.to_string(),
                        run_id: None,
                        step: None,
                        path: None,
                    })
                })
            }
        }
    }

    /// The real, native-async single-key resolve: selects the Run `key`
    /// addresses via [`Self::resolve_run`], then resolves the value —
    /// Data-plane first (in-flight-safe, Run-scoped since GH #23
    /// subtask-3), falling back to the selected Run's persisted
    /// `result_ref` — see the module doc's Architecture section. Returns
    /// the selected [`RunRecord`] alongside the resolved value so a
    /// caller can report which Run actually served the projection, even
    /// when the caller only supplied `task_id`.
    ///
    /// GH #23 subtask-3: `key.step` (canonical name OR any alias) is
    /// canonicalized against `run`'s [`StepNaming`] table BEFORE either
    /// lookup — the pre-subtask-3 `key.run_id.is_none()` guard (which
    /// narrowed the Data-plane path to only in-flight fetches, hedging
    /// against the cross-Run race the former KNOWN LIMITATION described)
    /// is gone: the Data-plane lookup is now `get_latest_by_name_in_run`,
    /// scoped to the resolving step's own globally-unique `StepId`, so an
    /// explicit `run_id` pin is exactly as race-free as the in-flight
    /// case — narrowing which calls attempt it bought nothing once the
    /// lookup itself is Run-scoped.
    async fn resolve_async(
        &self,
        key: &ProjectionKey,
    ) -> Result<(RunRecord, Value), ProjectionError> {
        let task_id = TaskId::parse(key.task_id.clone())
            .map_err(|e| ProjectionError::InvalidKey(format!("task_id: {e}")))?;
        let run = self.resolve_run(&task_id, key.run_id.as_deref()).await?;

        let Some(raw_step) = &key.step else {
            // `key.step` is `None` — whole-ctx addressing, no step name to
            // canonicalize.
            let ctx_data = run.result_ref.clone().unwrap_or(Value::Null);
            let value = key
                .resolve(&ctx_data)
                .cloned()
                .ok_or_else(|| ProjectionError::NotFound(key.clone()))?;
            return Ok((run, value));
        };

        let naming = self.step_naming_for_run(&run).await;
        let canonical = naming
            .as_deref()
            .and_then(|n| n.resolve(raw_step))
            .unwrap_or(raw_step.as_str())
            .to_string();

        // Data-plane, in-flight-safe, Run-scoped path.
        if let Some(step_id) = find_step_id_for_canonical(&run, naming.as_deref(), &canonical) {
            match self
                .data_store
                .get_latest_by_name_in_run(step_id.as_str(), 1, &canonical)
                .await
            {
                Ok(record) => {
                    if let Some(value) = final_value(&record.event) {
                        let narrowed = match &key.path {
                            None => Some(value),
                            Some(_) => {
                                // Reuse `ProjectionKey::resolve`'s path-walk
                                // only (the step lookup is already done —
                                // this value IS the step's own content, not
                                // a `{step: value}` map to look `step` up
                                // in again).
                                let path_only = ProjectionKey {
                                    task_id: key.task_id.clone(),
                                    run_id: key.run_id.clone(),
                                    step: None,
                                    path: key.path.clone(),
                                };
                                path_only.resolve(&value).cloned()
                            }
                        };
                        if let Some(value) = narrowed {
                            return Ok((run, value));
                        }
                    }
                }
                Err(OutputStoreError::NotFound(_)) => {
                    // No Data-plane record for this step's own dispatch —
                    // fall through to the result_ref fallback below.
                }
                Err(other) => {
                    return Err(ProjectionError::Io(std::io::Error::other(format!(
                        "OutputStore::get_latest_by_name_in_run: {other}"
                    ))));
                }
            }
        }

        // Fallback: the persisted `result_ref`, tried under the canonical
        // name AND every alias (`result_ref` keys are still the raw
        // flow.ir ctx-path segment — an alias, not necessarily the
        // canonical name).
        let ctx_data = run.result_ref.clone().unwrap_or(Value::Null);
        for candidate in candidate_names(naming.as_deref(), &canonical, raw_step) {
            let candidate_key = ProjectionKey {
                task_id: key.task_id.clone(),
                run_id: key.run_id.clone(),
                step: Some(candidate.to_string()),
                path: key.path.clone(),
            };
            if let Some(value) = candidate_key.resolve(&ctx_data) {
                return Ok((run, value.clone()));
            }
        }
        Err(ProjectionError::NotFound(key.clone()))
    }

    /// Enumerates every step visible for the Run addressed by `task_id` +
    /// `run_id` (`None` = latest) — the shared enumeration both this
    /// module's HTTP handlers and the Worker axis's pointer assembly
    /// build from (module doc). Returns the selected [`RunRecord`]
    /// alongside the resolved steps.
    pub(crate) async fn list_steps(
        &self,
        task_id: &TaskId,
        run_id: Option<&str>,
    ) -> Result<(RunRecord, Vec<ResolvedStep>), ProjectionError> {
        let run = self.resolve_run(task_id, run_id).await?;
        let steps = self.enumerate_steps(&run).await;
        Ok((run, steps))
    }

    /// Same enumeration as [`Self::list_steps`], addressed directly by an
    /// already-known [`RunId`] (no `task_id` cross-check, no `"latest"`
    /// ambiguity) — the Worker axis's entry point
    /// (`crates/mlua-swarm-server/src/worker.rs`), which already has the
    /// exact Run its own `AgentContextView.run_id` names, from
    /// `Ctx.meta.runtime[RUN_ID_KEY]` (threaded through by
    /// `Engine::dispatch_attempt_with`).
    pub(crate) async fn list_steps_by_run_id(
        &self,
        run_id: &RunId,
    ) -> Result<(RunRecord, Vec<ResolvedStep>), ProjectionError> {
        let run = self.run_store.get(run_id).await.map_err(|_| {
            ProjectionError::NotFound(ProjectionKey {
                task_id: String::new(),
                run_id: Some(run_id.to_string()),
                step: None,
                path: None,
            })
        })?;
        let steps = self.enumerate_steps(&run).await;
        Ok((run, steps))
    }

    /// GH #23 subtask-3: dispatches to the table-driven enumeration
    /// ([`Self::enumerate_steps_via_table`]) when a [`StepNaming`] table
    /// resolves for `run`, else the defensive-only
    /// [`Self::enumerate_steps_legacy_union`] fallback — see the module
    /// doc's "table-driven addressing" section and
    /// [`Self::step_naming_for_run`]'s doc for when the fallback fires.
    async fn enumerate_steps(&self, run: &RunRecord) -> Vec<ResolvedStep> {
        match self.step_naming_for_run(run).await {
            Some(naming) => self.enumerate_steps_via_table(run, &naming).await,
            None => self.enumerate_steps_legacy_union(run).await,
        }
    }

    /// GH #23 subtask-3: the table-driven replacement for the runtime
    /// union rule. For every `run.step_entries` row, resolves that row's
    /// own `Step.ref` to its canonical name via
    /// `naming.canonical_of_producer` (falling back to the raw ref when
    /// the table has no entry for it — defensive-only, mirrors this
    /// module's other best-effort hooks), then queries the Run-scoped
    /// `OutputStore::get_latest_by_name_in_run` keyed by THIS row's own
    /// `StepId` — globally unique, so no cross-Run bleed by construction,
    /// closing the former KNOWN LIMITATION race regardless of whether the
    /// Blueprint declared a `projection_name`. A canonical name spanning
    /// multiple `step_entries` rows (a step re-dispatched under the same
    /// ref, e.g. inside a Loop) keeps the LATEST successfully-resolved
    /// occurrence (later rows overwrite earlier ones in `resolved`; a row
    /// whose own lookup comes up empty never blanks an earlier
    /// occurrence's already-resolved value). Names still unresolved after
    /// every occurrence is tried fall back to `run.result_ref`, matched
    /// against every alias (or the canonical name itself) — `result_ref`
    /// keys are still the raw flow.ir ctx-path segment.
    async fn enumerate_steps_via_table(
        &self,
        run: &RunRecord,
        naming: &StepNaming,
    ) -> Vec<ResolvedStep> {
        let mut resolved: std::collections::BTreeMap<String, ResolvedStep> =
            std::collections::BTreeMap::new();

        for entry in &run.step_entries {
            let Some(step_ref) = entry.step_ref.as_deref() else {
                continue;
            };
            let canonical = naming
                .canonical_of_producer(step_ref)
                .unwrap_or(step_ref)
                .to_string();
            if let Ok(record) = self
                .data_store
                .get_latest_by_name_in_run(entry.step_id.as_str(), 1, &canonical)
                .await
            {
                if let Some(value) = final_value(&record.event) {
                    resolved.insert(
                        canonical.clone(),
                        ResolvedStep {
                            name: canonical,
                            value,
                            source: ProjectionSource::DataPlane,
                        },
                    );
                }
            }

            // GH #34 subtask-3 gap fix: also surface any `Artifact` events
            // dual-written under THIS row's own `StepId` — e.g.
            // `AfterRunAuditMiddleware`'s `"audit:<step_ref>"` finding,
            // submitted against the AUDITED step's own `(task_id, attempt)`
            // (see `src/middleware.rs`'s `run_one_audit`, and
            // `Engine::submit_output`'s doc, "`Artifact` dual-write"
            // section). Purely additive to the canonical-name lookup
            // above: an artifact's `name` is never a step's own canonical
            // producer name (`"audit:"`-prefixed by convention, but this
            // loop does not special-case the prefix — any `Artifact`
            // dual-written under this `StepId` surfaces under its own
            // `name`), so it can never collide with / override the entry
            // just inserted. `list_for_attempt` returning `Err` (backend
            // hiccup) is treated the same as "no artifacts" — fail-open,
            // matching this whole adapter's read-path discipline.
            if let Ok(records) = self
                .data_store
                .list_for_attempt(entry.step_id.as_str(), 1)
                .await
            {
                for record in records {
                    if let OutputEvent::Artifact { name, content } = &record.event {
                        resolved
                            .entry(name.clone())
                            .or_insert_with(|| ResolvedStep {
                                name: name.clone(),
                                value: content_to_value(content),
                                source: ProjectionSource::DataPlane,
                            });
                    }
                }
            }
        }

        if let Some(Value::Object(map)) = &run.result_ref {
            for entry in naming.entries() {
                if resolved.contains_key(&entry.canonical) {
                    continue;
                }
                let hit = entry
                    .aliases
                    .iter()
                    .find_map(|alias| map.get(alias))
                    .or_else(|| map.get(&entry.canonical));
                if let Some(value) = hit {
                    resolved.insert(
                        entry.canonical.clone(),
                        ResolvedStep {
                            name: entry.canonical.clone(),
                            value: value.clone(),
                            source: ProjectionSource::ResultRef,
                        },
                    );
                }
            }
        }

        resolved.into_values().collect()
    }

    /// Pre-GH-#23 defensive-only fallback: the ORIGINAL runtime union rule
    /// (raw `Step.ref` name ∪ `result_ref` top-level keys, Data-plane wins
    /// a collision), used ONLY when [`Self::step_naming_for_run`] resolves
    /// no [`StepNaming`] table for this Run. NOT a "keep the old path
    /// around for undeclared Blueprints" hedge — every real
    /// `Compiler::compile` output always carries a table (see
    /// [`StepNaming`]'s module doc), so this branch is reached by
    /// defensive-only callers (test harnesses that seed
    /// `OutputStore`/`RunStore` fixtures directly, bypassing a real
    /// dispatch), not by any Blueprint-driven Run.
    async fn enumerate_steps_legacy_union(&self, run: &RunRecord) -> Vec<ResolvedStep> {
        let mut out = Vec::new();
        let mut attempted = std::collections::HashSet::new();
        let mut resolved_names = std::collections::HashSet::new();

        for entry in &run.step_entries {
            let Some(name) = &entry.step_ref else {
                continue;
            };
            if !attempted.insert(name.clone()) {
                continue;
            }
            if let Ok(record) = self.data_store.get_latest_by_name(name).await {
                if let Some(value) = final_value(&record.event) {
                    out.push(ResolvedStep {
                        name: name.clone(),
                        value,
                        source: ProjectionSource::DataPlane,
                    });
                    resolved_names.insert(name.clone());
                }
            }
        }

        if let Some(Value::Object(map)) = &run.result_ref {
            for (name, value) in map {
                if resolved_names.contains(name) {
                    continue;
                }
                out.push(ResolvedStep {
                    name: name.clone(),
                    value: value.clone(),
                    source: ProjectionSource::ResultRef,
                });
            }
        }

        out
    }
}

impl ProjectionAdapter for McpQueryAdapter {
    fn name(&self) -> &'static str {
        "mcp-query"
    }

    /// `ctx_data` is used only to fail loud up front (mirrors
    /// [`mlua_swarm::core::projection::FileProjectionAdapter::project`]'s
    /// own not-found check) — the returned [`ProjectionRef::Query`]
    /// locator carries `key` itself, not a resolved value; the real lookup
    /// happens later, at [`Self::fetch`] time, against whatever the
    /// addressed Run's backing is *then* (which may differ from
    /// `ctx_data`, e.g. after a re-kick, or once a step submits through
    /// the Data-plane store).
    fn project(
        &self,
        key: &ProjectionKey,
        ctx_data: &Value,
    ) -> Result<ProjectionRef, ProjectionError> {
        if key.task_id.is_empty() {
            return Err(ProjectionError::InvalidKey(
                "task_id must not be empty".to_string(),
            ));
        }
        key.resolve(ctx_data)
            .ok_or_else(|| ProjectionError::NotFound(key.clone()))?;
        Ok(ProjectionRef::Query {
            endpoint: format!(
                "/v1/tasks/{}/runs/{}/steps/{}/content",
                key.task_id,
                key.run_id.as_deref().unwrap_or("latest"),
                key.step.as_deref().unwrap_or("_ctx")
            ),
            key: key.clone(),
        })
    }

    fn fetch(&self, key: &ProjectionKey) -> Result<Value, ProjectionError> {
        // See the module doc: this bridge exists for `ProjectionAdapter`
        // trait conformance only. `block_in_place` requires the Tokio
        // multi-thread runtime flavor (the workspace's `tokio` dependency
        // enables `features = ["full"]`, which includes it).
        let handle = tokio::runtime::Handle::try_current().map_err(|e| {
            ProjectionError::Io(std::io::Error::other(format!(
                "McpQueryAdapter::fetch requires a Tokio runtime: {e}"
            )))
        })?;
        let (_run, value) =
            tokio::task::block_in_place(|| handle.block_on(self.resolve_async(key)))?;
        Ok(value)
    }

    fn pointer_line(&self, r: &ProjectionRef) -> String {
        match r {
            ProjectionRef::Query { endpoint, key } => {
                format!("projection(mcp-query): {endpoint} task_id={}", key.task_id)
            }
            ProjectionRef::File { path } => format!("projection(file): {path}"),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// REST hierarchy: StepList / StepSummary / content plane
// ──────────────────────────────────────────────────────────────────────────

/// Response body for `GET /v1/tasks/:id/runs/:run/steps`.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct StepList {
    /// The addressed Task.
    pub task_id: String,
    /// The Run this list resolved `:run` to (the concrete id, even when
    /// the request path said `latest`).
    pub run_id: String,
    /// Every visible step, unfiltered (the HTTP debug plane serves the
    /// full union — `ContextPolicy.steps` filtering only applies to the
    /// Worker axis's `context.steps` pointer list; see the module doc).
    pub steps: Vec<StepSummary>,
}

/// One step's metadata (operator / debug plane) — `GET
/// /v1/tasks/:id/runs/:run/steps/:step`, and each entry of
/// [`StepList::steps`].
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct StepSummary {
    /// The producing step's name.
    pub name: String,
    /// Byte length of the body [`Self::content_url`] serves (the exact
    /// bytes a `GET` of that URL returns for this same `?path=`, if any).
    pub size_bytes: u64,
    /// MIME type [`Self::content_url`] serves this body as
    /// (`text/markdown; charset=utf-8` when materialized-file-backed,
    /// `application/json` otherwise — see the module doc's Content-Type
    /// rule).
    pub content_type: String,
    /// SHA-256 hex digest of the body, matching the content endpoint's
    /// `ETag` value (`sha256:<hex>`, minus the `sha256:` prefix).
    pub sha256: String,
    /// Which backing produced this entry.
    pub source: ProjectionSource,
    /// Absolute filesystem path to the materialized projection file
    /// (`crate::core::projection::FileProjectionAdapter`'s
    /// [`ProjectionPlacement`] resolver's target — byte-compat default
    /// layout `<root>/workspace/tasks/<step_id>/ctx/<name>.md`), when one
    /// exists AND this entry addresses the whole step (no `?path=`
    /// narrowing — a narrowed fragment is never file-backed). `None`
    /// otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    /// Fetch URL for this step's content (`GET
    /// /v1/tasks/:id/runs/:run/steps/:step/content`, `?path=` echoed when
    /// this entry is narrowed) — absolute (`AppState.base_url`-prefixed)
    /// when the server has a configured base URL, relative otherwise.
    pub content_url: String,
    /// First <= 512 bytes of the body, UTF-8-boundary-safe (never splits
    /// a multi-byte character), with a trailing `…` when truncated.
    pub preview: String,
    /// `true` when [`Self::preview`]'s underlying byte count is shorter
    /// than [`Self::size_bytes`] (the body was truncated to build the
    /// preview).
    pub truncated: bool,
}

/// Query params shared by the metadata and content routes: narrows a
/// single step's value via `$.a.b` dot-path form (the leading `$.` is
/// optional) — same syntax `mlua_swarm::core::projection::ProjectionKey`
/// already establishes.
#[derive(Debug, Deserialize, Default, schemars::JsonSchema)]
pub struct StepPathQuery {
    /// `$.a.b` narrowing within the step's value. `None` = the whole
    /// step value.
    #[serde(default)]
    pub path: Option<String>,
}

/// Narrows `value` by `path` (reuses [`ProjectionKey::resolve`]'s
/// path-walk half — the step lookup is already done, this value IS the
/// step's own content).
fn narrow_step_value(value: &Value, path: Option<&str>) -> Option<Value> {
    match path {
        None => Some(value.clone()),
        Some(p) => {
            let path_only = ProjectionKey {
                task_id: String::new(),
                run_id: None,
                step: None,
                path: Some(p.to_string()),
            };
            path_only.resolve(value).cloned()
        }
    }
}

/// The materialize target [`mlua_swarm::core::projection::FileProjectionAdapter`]
/// writes to for a submission, resolved via the SAME
/// [`ProjectionPlacement`] (GH #27, follow-up to #23) the writer
/// consulted — reconstructed here (rather than constructed through the
/// adapter itself) because this module resolves the target for a step
/// *other than* the one materializing it, key-first.
fn materialized_file_path(
    placement: &ProjectionPlacement,
    root: &str,
    step_id: &StepId,
    name: &str,
) -> std::path::PathBuf {
    placement.target_path(root, step_id.as_ref(), name)
}

/// Resolves the materialized file body for `name` (a CANONICAL name — the
/// GH #23 subtask-2 sink writes the materialize target under the
/// canonical name, so this lookup canonicalizes `run.step_entries`' raw
/// `step_ref`s the same way before matching) in `run`, when one exists:
/// finds `name`'s most recent [`mlua_swarm::store::run::StepEntry`]
/// (giving its own dispatch `StepId`) via
/// [`find_step_id_for_canonical`], resolves that step's own
/// `AgentContextView` root via the SAME [`ProjectionPlacement`]
/// [`Engine::submit_output`]'s materialize sink snapshotted at dispatch
/// time (GH #27, follow-up to #23 — see
/// `mlua_swarm::core::projection_placement`'s module doc for the "3 path"
/// convergence), via [`mlua_swarm::core::engine::Engine::agent_context_for`],
/// and reads the file at the resulting path back.
///
/// Only tries `attempt = 1` (the common case — a single dispatch per
/// flow.ir Step) — a step retried under the same `StepId` at a later
/// attempt is a known, accepted limitation (matching this module's other
/// KNOWN LIMITATION notes); the entry still resolves via its Data-plane /
/// `result_ref` value, just without a `file_path`.
async fn resolve_materialized_file(
    state: &AppState,
    run: &RunRecord,
    name: &str,
) -> Option<(std::path::PathBuf, Vec<u8>)> {
    let naming = resolve_step_naming_for_run(&state.engine, run).await;
    let step_id = find_step_id_for_canonical(run, naming.as_deref(), name)?;
    let view = state.engine.agent_context_for(&step_id, 1).await?;
    let placement = state
        .engine
        .projection_placement_for(&step_id)
        .await
        .unwrap_or_default();
    let root = placement.resolve_root(&view)?;
    let path = materialized_file_path(&placement, &root, &step_id, name);
    let bytes = std::fs::read(&path).ok()?;
    Some((path, bytes))
}

/// Free-function sibling of [`McpQueryAdapter::step_naming_for_run`] for
/// [`resolve_materialized_file`], which has no `McpQueryAdapter` handle
/// (it resolves a step OTHER than the one materializing it, from a bare
/// `&AppState`) — same lookup, same fail-open contract.
async fn resolve_step_naming_for_run(engine: &Engine, run: &RunRecord) -> Option<Arc<StepNaming>> {
    for entry in &run.step_entries {
        if let Some(naming) = engine.step_naming_for(&entry.step_id).await {
            return Some(naming);
        }
    }
    None
}

/// Renders the body [`Self`]'s content endpoint serves for `step`,
/// narrowed by `path` when `Some`: whole-step + materialized-file-backed
/// → the raw file bytes (`text/markdown; charset=utf-8`); anything else →
/// the (possibly narrowed) value as pretty JSON (`application/json`).
/// Returns `None` when `path` is `Some` and does not resolve against
/// `step.value` (the caller's 404 case).
async fn render_step_body(
    state: &AppState,
    run: &RunRecord,
    step: &ResolvedStep,
    path: Option<&str>,
) -> Option<(Vec<u8>, &'static str, Option<String>)> {
    if path.is_none() {
        if let Some((file_path, bytes)) = resolve_materialized_file(state, run, &step.name).await {
            return Some((
                bytes,
                "text/markdown; charset=utf-8",
                Some(file_path.to_string_lossy().into_owned()),
            ));
        }
    }
    let narrowed = narrow_step_value(&step.value, path)?;
    let body = serde_json::to_vec_pretty(&narrowed).ok()?;
    Some((body, "application/json", None))
}

/// First <= 512 bytes of `body`, UTF-8-boundary-safe (never splits a
/// multi-byte character), with a trailing `…` when truncated. Returns
/// `(preview, truncated)`. `body` is expected to be valid UTF-8 (JSON /
/// materialized-markdown text, per [`render_step_body`]'s own two output
/// shapes); a malformed byte sequence falls back to a lossy decode rather
/// than panicking.
fn build_preview(body: &[u8]) -> (String, bool) {
    const MAX_PREVIEW_BYTES: usize = 512;
    if body.len() <= MAX_PREVIEW_BYTES {
        return (String::from_utf8_lossy(body).into_owned(), false);
    }
    let preview = match std::str::from_utf8(body) {
        Ok(s) => {
            let mut end = MAX_PREVIEW_BYTES;
            while end > 0 && !s.is_char_boundary(end) {
                end -= 1;
            }
            s[..end].to_string()
        }
        Err(_) => String::from_utf8_lossy(&body[..MAX_PREVIEW_BYTES]).into_owned(),
    };
    (format!("{preview}…"), true)
}

/// `GET /v1/tasks/:id/runs/:run/steps/:step/content`'s URL — absolute
/// (`base_url`-prefixed) when the server has one configured, relative
/// otherwise. `path` is echoed back as `?path=` verbatim (unencoded — the
/// dot-path syntax this module accepts uses no characters reserved in a
/// URL query component).
fn build_content_url(
    base_url: &Option<Arc<str>>,
    task_id: &TaskId,
    run_id: &RunId,
    name: &str,
    path: Option<&str>,
) -> String {
    let mut url = format!("/v1/tasks/{task_id}/runs/{run_id}/steps/{name}/content");
    if let Some(p) = path {
        url.push_str("?path=");
        url.push_str(p);
    }
    match base_url {
        Some(base) => format!("{}{}", base.trim_end_matches('/'), url),
        None => url,
    }
}

/// Builds the full [`StepSummary`] for `step`, narrowed by `path` when
/// `Some`. `None` when `path` does not resolve (the caller's 404 case).
async fn build_step_summary(
    state: &AppState,
    run: &RunRecord,
    step: &ResolvedStep,
    path: Option<&str>,
) -> Option<StepSummary> {
    let (body, content_type, file_path) = render_step_body(state, run, step, path).await?;
    let sha256 = hex::encode(sha2::Sha256::digest(&body));
    let size_bytes = body.len() as u64;
    let (preview, truncated) = build_preview(&body);
    let content_url = build_content_url(&state.base_url, &run.task_id, &run.id, &step.name, path);
    Some(StepSummary {
        name: step.name.clone(),
        size_bytes,
        content_type: content_type.to_string(),
        sha256,
        source: step.source,
        file_path,
        content_url,
        preview,
        truncated,
    })
}

/// Fields a Worker-axis
/// [`mlua_swarm::core::agent_context::StepPointer`] needs —
/// `crates/mlua-swarm-server/src/worker.rs`'s `GET /v1/worker/prompt`
/// handler builds one per visible, policy-allowed step from this.
/// Reuses the same whole-step body [`render_step_body`] renders for the
/// content endpoint (`path = None`), so `sha256` / `size_bytes` always
/// matches what a `GET` of the returned `content_url` serves. `None`
/// when the body cannot be rendered at all (mirrors this crate's other
/// best-effort projection hooks — never turns a would-have-succeeded
/// fetch into a failure; the caller just omits this step's pointer).
pub(crate) async fn resolve_step_pointer_fields(
    state: &AppState,
    run: &RunRecord,
    step: &ResolvedStep,
) -> Option<(u64, Option<String>, String, String)> {
    let (body, _content_type, file_path) = render_step_body(state, run, step, None).await?;
    let sha256 = hex::encode(sha2::Sha256::digest(&body));
    let size_bytes = body.len() as u64;
    let content_url = build_content_url(&state.base_url, &run.task_id, &run.id, &step.name, None);
    Some((size_bytes, file_path, content_url, sha256))
}

/// Shared resolve: `:id` → `TaskId` (existence-checked against
/// `state.task_store` first, so an unknown Task returns its own 404
/// distinct from an unknown Run) + `:run` (`"latest"` or an explicit
/// `R-<hex>`) → the [`McpQueryAdapter`] that resolved it (returned
/// alongside so `step_get` / `step_content` can canonicalize their `:step`
/// path segment through the SAME adapter, via
/// [`McpQueryAdapter::resolve_step_name`] — GH #23 subtask-3), the
/// addressed [`RunRecord`], and its enumerated [`ResolvedStep`]s.
async fn resolve_run_and_steps(
    state: &AppState,
    id: &str,
    run: &str,
) -> Result<(McpQueryAdapter, RunRecord, Vec<ResolvedStep>), ApiError> {
    let task_id = TaskId::parse(id.to_string())
        .map_err(|e| ApiError::bad_request(format!("invalid task id: {e}")))?;
    state
        .task_store
        .get(&task_id)
        .await
        .map_err(map_task_store_err)?;
    let adapter = McpQueryAdapter::new(
        state.data_store.clone(),
        state.run_store.clone(),
        state.engine.clone(),
    );
    let run_sel = if run == "latest" { None } else { Some(run) };
    let (run_record, steps) = adapter
        .list_steps(&task_id, run_sel)
        .await
        .map_err(map_projection_err)?;
    Ok((adapter, run_record, steps))
}

/// `GET /v1/tasks/:id/runs/:run/steps` — every step visible for the
/// addressed Run, unfiltered (see the module doc's role split).
pub async fn steps_list(
    State(state): State<AppState>,
    Path((id, run)): Path<(String, String)>,
) -> Result<Json<StepList>, ApiError> {
    let (_adapter, run_record, steps) = resolve_run_and_steps(&state, &id, &run).await?;
    let mut summaries = Vec::with_capacity(steps.len());
    for step in &steps {
        if let Some(summary) = build_step_summary(&state, &run_record, step, None).await {
            summaries.push(summary);
        }
    }
    Ok(Json(StepList {
        task_id: run_record.task_id.to_string(),
        run_id: run_record.id.to_string(),
        steps: summaries,
    }))
}

/// `GET /v1/tasks/:id/runs/:run/steps/:step?path=$.a.b` — one step's
/// metadata, optionally narrowed. GH #23 subtask-3: `:step` is
/// canonicalized (`adapter.resolve_step_name`) before the lookup, so
/// either the canonical name or any alias 200s — the reported
/// [`StepSummary::name`] is always the canonical form.
pub async fn step_get(
    State(state): State<AppState>,
    Path((id, run, step)): Path<(String, String, String)>,
    Query(q): Query<StepPathQuery>,
) -> Result<Json<StepSummary>, ApiError> {
    let (adapter, run_record, steps) = resolve_run_and_steps(&state, &id, &run).await?;
    let canonical = adapter.resolve_step_name(&run_record, &step).await;
    let resolved = steps
        .into_iter()
        .find(|s| s.name == canonical)
        .ok_or_else(|| ApiError::not_found(format!("step not found: {step}")))?;
    let summary = build_step_summary(&state, &run_record, &resolved, q.path.as_deref())
        .await
        .ok_or_else(|| ApiError::not_found(format!("path not found: {:?}", q.path)))?;
    Ok(Json(summary))
}

/// `GET /v1/tasks/:id/runs/:run/steps/:step/content?path=$.a.b` — the raw
/// body: full bytes, no envelope, no Range support. `Content-Type` and
/// `ETag` follow [`StepSummary::content_type`] / [`StepSummary::sha256`]'s
/// same rules (module doc). GH #23 subtask-3: `:step` is canonicalized
/// the same way [`step_get`] does.
pub async fn step_content(
    State(state): State<AppState>,
    Path((id, run, step)): Path<(String, String, String)>,
    Query(q): Query<StepPathQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let (adapter, run_record, steps) = resolve_run_and_steps(&state, &id, &run).await?;
    let canonical = adapter.resolve_step_name(&run_record, &step).await;
    let resolved = steps
        .into_iter()
        .find(|s| s.name == canonical)
        .ok_or_else(|| ApiError::not_found(format!("step not found: {step}")))?;
    let (body, content_type, _file_path) =
        render_step_body(&state, &run_record, &resolved, q.path.as_deref())
            .await
            .ok_or_else(|| ApiError::not_found(format!("path not found: {:?}", q.path)))?;
    let sha256 = hex::encode(sha2::Sha256::digest(&body));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type).expect("content_type is a static ASCII literal"),
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"sha256:{sha256}\""))
            .expect("hex digest is ASCII-safe for a header value"),
    );
    Ok((StatusCode::OK, headers, body))
}

fn map_projection_err(e: ProjectionError) -> ApiError {
    match e {
        ProjectionError::NotFound(key) => {
            ApiError::not_found(format!("projection not found for key {key:?}"))
        }
        ProjectionError::InvalidKey(msg) => ApiError::bad_request(msg),
        other => ApiError::engine(other),
    }
}

// ──────────────────────────────────────────────────────────────────────────
// UT
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskLaunchRequest;
    use axum::http::StatusCode;
    use mlua_swarm::application::BlueprintRef;
    use mlua_swarm::blueprint::{
        current_schema_version, AgentDef, AgentKind, AgentMeta, Blueprint, BlueprintMetadata,
        CompilerHints, CompilerStrategy, ProjectionPlacementSpec,
    };
    use mlua_swarm::core::config::EngineCfg;
    use mlua_swarm::core::engine::Engine;
    use mlua_swarm::store::output::InMemoryOutputStore;
    use mlua_swarm::store::run::InMemoryRunStore;
    use mlua_swarm::store::task::InMemoryTaskStore;
    use serde_json::json;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    /// A single-step flow.ir Blueprint that echoes `$.greeting` into
    /// `$.out` (AG_IDENTITY wraps its input as `{"echoed": input}`), so
    /// `result_ref = {"out": {"echoed": <greeting>}}` — enough shape to
    /// exercise `step` + `path` narrowing. Mirrors `tasks.rs`'s own test
    /// helper (duplicated here rather than shared — this crate's
    /// established per-module test-helper convention; see e.g.
    /// `tasks::tests::test_state`).
    fn greeting_blueprint() -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "projection-test-greeting-bp".into(),
            flow: serde_json::from_value(json!({
                "kind": "step",
                "ref": mlua_swarm::worker::baseline::AG_IDENTITY,
                "in": {"op": "path", "at": "$.greeting"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents: vec![AgentDef {
                name: mlua_swarm::worker::baseline::AG_IDENTITY.into(),
                kind: AgentKind::RustFn,
                spec: json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
                profile: None,
                meta: None,
            }],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
        }
    }

    fn test_state() -> AppState {
        let engine = Engine::new_with_layers(EngineCfg::default(), crate::default_layer_registry());
        let compiler = mlua_swarm::Compiler::new(crate::default_registry());
        let launch = Arc::new(mlua_swarm::TaskLaunchService::new(engine.clone(), compiler));
        let data_store: Arc<dyn mlua_swarm::store::output::OutputStore> =
            Arc::new(InMemoryOutputStore::new());
        // subtask-4 / ST2 rework: wire the SAME `OutputStore` into the
        // engine's submit-time projection sink (mirrors
        // `crate::build_router_full`'s own wiring), so tests exercising the
        // Data-plane / in-flight path see ordinary worker submissions land
        // here too, not just explicit `POST /v1/data/emit` calls.
        engine.set_output_store(data_store.clone());
        AppState {
            engine,
            sessions: Arc::new(Mutex::new(crate::SessionStore::default())),
            task_app: Arc::new(mlua_swarm::TaskApplication::new_inline_only(launch)),
            ws_operator_factory: None,
            data_store,
            operator_sessions: Arc::new(Mutex::new(HashMap::new())),
            roles_to_sid: Arc::new(Mutex::new(HashMap::new())),
            task_store: Arc::new(InMemoryTaskStore::new()),
            run_store: Arc::new(InMemoryRunStore::new()),
            base_url: None,
            sync_timeout_secs: 300,
        }
    }

    fn greeting_task_req(greeting: &str) -> TaskLaunchRequest {
        TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(greeting_blueprint()),
            },
            init_ctx: json!({ "greeting": greeting }),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            timeout_secs: None,
            goal: Some("projection test goal".to_string()),
            detach: false,
        }
    }

    /// GH #23 subtask-3: a single-step Blueprint whose sole agent
    /// (`AG_IDENTITY`) declares `AgentMeta.projection_name`, distinct from
    /// its own `Step.ref` — the declared-name E2E fixture. Same shape as
    /// [`greeting_blueprint`] otherwise (echoes `$.greeting` into
    /// `$.out`).
    fn declared_projection_name_blueprint(projection_name: &str) -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "projection-test-declared-name-bp".into(),
            flow: serde_json::from_value(json!({
                "kind": "step",
                "ref": mlua_swarm::worker::baseline::AG_IDENTITY,
                "in": {"op": "path", "at": "$.greeting"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents: vec![AgentDef {
                name: mlua_swarm::worker::baseline::AG_IDENTITY.into(),
                kind: AgentKind::RustFn,
                spec: json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
                profile: None,
                meta: Some(AgentMeta {
                    projection_name: Some(projection_name.to_string()),
                    ..Default::default()
                }),
            }],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
        }
    }

    fn declared_task_req(greeting: &str, projection_name: &str) -> TaskLaunchRequest {
        TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(declared_projection_name_blueprint(projection_name)),
            },
            init_ctx: json!({ "greeting": greeting }),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            timeout_secs: None,
            goal: Some("projection test goal (declared name)".to_string()),
            detach: false,
        }
    }

    // ─── Test 8: steps collection, GH #23 subtask-3 table-driven addressing ───

    /// GH #23 subtask-3 (backward-compat): an undeclared step's `Step.ref`
    /// (its own name) and its `out` ctx-path top-level segment are now ONE
    /// canonical addressing space — `enumerate_steps` reports exactly ONE
    /// entry (the canonical name = the ref itself), not the pre-GH-#23
    /// runtime union rule's two separate Data-plane/ResultRef entries.
    /// `step_get_resolves_alias_name_to_canonical_entry` below covers that
    /// "out" still 200s via alias resolution.
    #[tokio::test]
    async fn steps_list_undeclared_step_resolves_to_single_canonical_entry() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hello")))
            .await
            .expect("tasks_start")
            .0;

        let resp = steps_list(
            State(state.clone()),
            Path((posted.task_id.to_string(), "latest".to_string())),
        )
        .await
        .expect("steps_list")
        .0;

        assert_eq!(resp.task_id, posted.task_id.to_string());
        assert_eq!(resp.run_id, posted.run_id.to_string());
        let identity_name = mlua_swarm::worker::baseline::AG_IDENTITY;
        assert_eq!(resp.steps.len(), 1, "steps: {:?}", resp.steps);
        let entry = &resp.steps[0];
        assert_eq!(entry.name, identity_name);
        assert_eq!(entry.source, ProjectionSource::DataPlane);
    }

    /// GH #23 subtask-3 (backward-compat): `step_get`'s `:step` segment
    /// resolves through the `StepNaming` table — the raw `Step.ref` name
    /// and the `out` ctx-path alias both 200, to the SAME content, and
    /// both report the canonical name.
    #[tokio::test]
    async fn step_get_resolves_alias_name_to_canonical_entry() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hi")))
            .await
            .expect("tasks_start")
            .0;

        let identity_name = mlua_swarm::worker::baseline::AG_IDENTITY;
        let via_ref = step_get(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                identity_name.to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_get via own ref name")
        .0;
        let via_alias = step_get(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "out".to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_get via out-top alias")
        .0;

        assert_eq!(via_ref.name, identity_name);
        assert_eq!(
            via_alias.name, identity_name,
            "alias lookup must report the canonical name"
        );
        assert_eq!(
            via_ref.sha256, via_alias.sha256,
            "same OUTPUT regardless of which name was queried"
        );
    }

    /// GH #23 subtask-3 (declared-name E2E): a Blueprint-declared
    /// `projection_name` drives `steps_list` (single canonical entry) AND
    /// `step_get`'s `:step` resolution (canonical name, the raw `Step.ref`
    /// alias, AND the `out`-top alias all 200 to the SAME content).
    #[tokio::test]
    async fn declared_projection_name_e2e_resolves_via_canonical_and_alias() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(declared_task_req("hi", "plan-out")),
        )
        .await
        .expect("tasks_start")
        .0;

        let list = steps_list(
            State(state.clone()),
            Path((posted.task_id.to_string(), "latest".to_string())),
        )
        .await
        .expect("steps_list")
        .0;
        assert_eq!(list.steps.len(), 1, "steps: {:?}", list.steps);
        assert_eq!(list.steps[0].name, "plan-out");
        assert_eq!(list.steps[0].source, ProjectionSource::DataPlane);

        let by_canonical = step_get(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "plan-out".to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_get canonical")
        .0;
        assert_eq!(by_canonical.name, "plan-out");

        let identity_name = mlua_swarm::worker::baseline::AG_IDENTITY;
        let by_ref_alias = step_get(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                identity_name.to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_get ref alias")
        .0;
        assert_eq!(by_ref_alias.name, "plan-out");
        assert_eq!(by_ref_alias.sha256, by_canonical.sha256);

        let by_out_alias = step_get(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "out".to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_get out-top alias")
        .0;
        assert_eq!(by_out_alias.name, "plan-out");
        assert_eq!(by_out_alias.sha256, by_canonical.sha256);
    }

    /// GH #23 subtask-3 (declared-name E2E, materialized file half): the
    /// GH #23 subtask-2 sink writes the materialize target under the
    /// CANONICAL name — `resolve_materialized_file`'s lookup must
    /// canonicalize `run.step_entries`' raw `step_ref` the same way to
    /// find it, so the stem the server reports is `plan-out.md`, not
    /// `identity.md`.
    #[tokio::test]
    async fn declared_projection_name_materialized_file_stem_is_canonical() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state();
        let mut req = declared_task_req("materialized-declared", "plan-out");
        req.work_dir = Some(dir.path().to_string_lossy().into_owned());
        let posted = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("tasks_start")
            .0;

        let summary = step_get(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "plan-out".to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_get")
        .0;

        let file_path = summary.file_path.expect("materialized file_path present");
        assert!(
            file_path.ends_with("plan-out.md"),
            "materialized file stem must be the canonical name: {file_path}"
        );
    }

    /// GH #27 (follow-up to #23), 3-path consistency E2E: a Blueprint
    /// declaring `projection_placement` (`root = "project_root"`, a
    /// custom `dir_template`) drives BOTH the submit-time write (`Engine`'s
    /// `materialize_final_submission`, dispatched off the `Compiler`-built
    /// resolver) AND the server read-back
    /// (`resolve_materialized_file`, which re-fetches the SAME resolver via
    /// `Engine::projection_placement_for`) to the identical custom
    /// location — proof the "3 path" convergence
    /// `crate::core::projection_placement`'s module doc describes holds
    /// end-to-end, and that `work_dir` (absent here) is correctly NOT
    /// preferred when `root = "project_root"` is declared.
    #[tokio::test]
    async fn declared_projection_placement_e2e_write_and_read_back_converge() {
        let project_root_dir = tempfile::TempDir::new().unwrap();
        let state = test_state();
        let mut bp = declared_projection_name_blueprint("plan-out");
        bp.projection_placement = Some(ProjectionPlacementSpec {
            root: Some("project_root".to_string()),
            dir_template: Some("custom/{task_id}/out".to_string()),
        });
        let req = TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(bp),
            },
            init_ctx: json!({ "greeting": "materialized-custom-placement" }),
            project_root: Some(project_root_dir.path().to_string_lossy().into_owned()),
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            timeout_secs: None,
            goal: Some("projection placement test goal".to_string()),
            detach: false,
        };
        let posted = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("tasks_start")
            .0;

        let summary = step_get(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "plan-out".to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_get")
        .0;

        // NOTE: the `{task_id}` the placement resolver substitutes is the
        // dispatched Step's own `StepId` (`ProjectionKey.task_id`), which is
        // NOT the same value as `posted.task_id` (the outer `TaskId` this
        // flow-of-one-Step was launched under) — so the expected path is
        // built from the ACTUAL observed `file_path` shape (prefix / infix
        // / suffix), not a predicted exact id, mirroring
        // `declared_projection_name_materialized_file_stem_is_canonical`'s
        // own suffix-only assertion style.
        let file_path = summary.file_path.expect("materialized file_path present");
        let path = std::path::Path::new(&file_path);
        assert!(
            path.starts_with(project_root_dir.path()),
            "file must be rooted at project_root (root_preference=ProjectRoot): {file_path}"
        );
        assert!(
            file_path.ends_with("out/plan-out.md"),
            "file must follow the custom dir_template's tail: {file_path}"
        );
        assert!(
            file_path.contains("/custom/"),
            "file must follow the custom dir_template's prefix segment: {file_path}"
        );
        assert!(
            path.exists(),
            "the write side must have materialized the file the read-back reports: {file_path}"
        );
    }

    /// GH #23 subtask-3 (collision): a declared `projection_name` that
    /// collides with another Step's own `ref` is rejected at
    /// register/compile time — `StepNaming::from_blueprint`'s hard-error
    /// validation (subtask-1) surfaces end-to-end through the real
    /// `tasks_start` dispatch path, not just the unit-level `StepNaming`
    /// tests.
    #[tokio::test]
    async fn declared_projection_name_colliding_with_another_steps_ref_is_rejected_at_register_time(
    ) {
        use mlua_flow_ir::{Expr, Node as FlowNode};
        use mlua_swarm::worker::adapter::WorkerResult;
        use mlua_swarm::{RustFnInProcessSpawnerFactory, SpawnerRegistry};

        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("step-a", |inv| async move {
                Ok(WorkerResult {
                    value: json!(inv.prompt),
                    ok: true,
                })
            })
            .register_fn("step-b", |inv| async move {
                Ok(WorkerResult {
                    value: json!(inv.prompt),
                    ok: true,
                })
            });
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));

        let engine = Engine::new_with_layers(EngineCfg::default(), crate::default_layer_registry());
        let data_store: Arc<dyn mlua_swarm::store::output::OutputStore> =
            Arc::new(InMemoryOutputStore::new());
        engine.set_output_store(data_store.clone());
        let compiler = mlua_swarm::Compiler::new(reg);
        let launch = Arc::new(mlua_swarm::TaskLaunchService::new(engine.clone(), compiler));
        let state = AppState {
            engine,
            sessions: Arc::new(Mutex::new(crate::SessionStore::default())),
            task_app: Arc::new(mlua_swarm::TaskApplication::new_inline_only(launch)),
            ws_operator_factory: None,
            data_store,
            operator_sessions: Arc::new(Mutex::new(HashMap::new())),
            roles_to_sid: Arc::new(Mutex::new(HashMap::new())),
            task_store: Arc::new(InMemoryTaskStore::new()),
            run_store: Arc::new(InMemoryRunStore::new()),
            base_url: None,
            sync_timeout_secs: 300,
        };

        let flow = FlowNode::Seq {
            children: vec![
                FlowNode::Step {
                    ref_: "step-a".to_string(),
                    in_: Expr::Path {
                        at: "$.greeting".to_string(),
                    },
                    out: Expr::Path {
                        at: "$.a_out".to_string(),
                    },
                },
                FlowNode::Step {
                    ref_: "step-b".to_string(),
                    in_: Expr::Path {
                        at: "$.greeting".to_string(),
                    },
                    out: Expr::Path {
                        at: "$.b_out".to_string(),
                    },
                },
            ],
        };
        let blueprint = Blueprint {
            schema_version: current_schema_version(),
            id: "projection-test-collision-bp".into(),
            flow,
            agents: vec![
                AgentDef {
                    name: "step-a".into(),
                    kind: AgentKind::RustFn,
                    spec: json!({"fn_id": "step-a"}),
                    profile: None,
                    // Declares a projection_name colliding with "step-b"'s
                    // own (undeclared) ref — hard collision.
                    meta: Some(AgentMeta {
                        projection_name: Some("step-b".to_string()),
                        ..Default::default()
                    }),
                },
                AgentDef {
                    name: "step-b".into(),
                    kind: AgentKind::RustFn,
                    spec: json!({"fn_id": "step-b"}),
                    profile: None,
                    meta: None,
                },
            ],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
        };

        let req = TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(blueprint),
            },
            init_ctx: json!({ "greeting": "hi" }),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            timeout_secs: None,
            goal: None,
            detach: false,
        };

        // `TaskLaunchResponse` (the `Ok` side) does not implement `Debug`,
        // so `.expect_err` (which requires `T: Debug`) is not usable here —
        // match instead.
        let result = crate::tasks_start(State(state), Json(req)).await;
        let err = match result {
            Err(e) => e,
            Ok(_) => {
                panic!("declared projection_name colliding with another step's own ref must reject")
            }
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    /// GH #23 subtask-3 (race close): two separate Tasks whose flow.ir
    /// each dispatch the SAME producer name (`AG_IDENTITY`) — pre-GH-#23,
    /// `OutputStore::get_latest_by_name` would resolve whichever Task
    /// submitted LAST, globally, regardless of which Run's steps were
    /// being enumerated (the former KNOWN LIMITATION race). The
    /// Run-scoped `get_latest_by_name_in_run` lookup this subtask wires
    /// (keyed by each step's own globally-unique `StepId`) must keep each
    /// Task's own value distinct even though both share a producer name.
    #[tokio::test]
    async fn steps_list_run_scoped_lookup_does_not_bleed_across_tasks_sharing_a_producer_name() {
        let state = test_state();
        let first = crate::tasks_start(State(state.clone()), Json(greeting_task_req("first-task")))
            .await
            .expect("first tasks_start")
            .0;
        let second =
            crate::tasks_start(State(state.clone()), Json(greeting_task_req("second-task")))
                .await
                .expect("second tasks_start")
                .0;

        let first_steps = steps_list(
            State(state.clone()),
            Path((first.task_id.to_string(), "latest".to_string())),
        )
        .await
        .expect("first steps_list")
        .0;
        let second_steps = steps_list(
            State(state.clone()),
            Path((second.task_id.to_string(), "latest".to_string())),
        )
        .await
        .expect("second steps_list")
        .0;

        let identity_name = mlua_swarm::worker::baseline::AG_IDENTITY;
        let first_entry = first_steps
            .steps
            .iter()
            .find(|s| s.name == identity_name)
            .expect("first entry present");
        let second_entry = second_steps
            .steps
            .iter()
            .find(|s| s.name == identity_name)
            .expect("second entry present");
        assert_eq!(first_entry.source, ProjectionSource::DataPlane);
        assert_eq!(second_entry.source, ProjectionSource::DataPlane);
        assert_ne!(
            first_entry.sha256, second_entry.sha256,
            "each Task's own greeting must resolve, not the globally-latest submission"
        );
    }

    // ─── Test 9: `:run = latest` resolves to newest Run; explicit pin still works ───

    #[tokio::test]
    async fn steps_list_latest_resolves_newest_run_explicit_pin_still_works() {
        let state = test_state();
        let first = crate::tasks_start(State(state.clone()), Json(greeting_task_req("first")))
            .await
            .expect("tasks_start")
            .0;
        let (status, rekicked) = crate::tasks::task_rekick(
            State(state.clone()),
            Path(first.task_id.to_string()),
            Some(Json(crate::tasks::RunKickRequest {
                init_ctx_override: Some(json!({ "greeting": "second" })),
                task_input_override: None,
                timeout_secs: None,
                detach: false,
            })),
        )
        .await
        .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);

        let latest = steps_list(
            State(state.clone()),
            Path((first.task_id.to_string(), "latest".to_string())),
        )
        .await
        .expect("steps_list latest")
        .0;
        assert_eq!(latest.run_id, rekicked.0.run_id.to_string());

        let pinned = steps_list(
            State(state.clone()),
            Path((first.task_id.to_string(), first.run_id.to_string())),
        )
        .await
        .expect("steps_list pinned")
        .0;
        assert_eq!(pinned.run_id, first.run_id.to_string());
    }

    // ─── Test 10: preview <= 512 bytes, UTF-8 boundary safe, truncated flag ───

    #[tokio::test]
    async fn step_get_preview_is_utf8_boundary_safe_and_truncated_flag_is_correct() {
        let state = test_state();
        // A multi-byte fixture: repeat a 3-byte UTF-8 character (U+3042
        // "あ") past the 512-byte preview cap so the boundary-safety guard
        // is actually exercised, then wrap it as the greeting value.
        let long_value = "あ".repeat(300); // 900 bytes
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req(&long_value)))
            .await
            .expect("tasks_start")
            .0;

        let identity_name = mlua_swarm::worker::baseline::AG_IDENTITY;
        let summary = step_get(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                identity_name.to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_get")
        .0;

        assert!(
            summary.preview.len() <= 512 + "…".len(),
            "preview must stay near the 512-byte cap: {} bytes",
            summary.preview.len()
        );
        assert!(
            summary.truncated,
            "a 900-byte body must be reported truncated"
        );
        assert!(
            summary.preview.ends_with('…'),
            "truncated preview must end with an ellipsis: {}",
            summary.preview
        );
        // The boundary-safety guard: a valid `String` never panics on
        // construction from a byte slice that split a multi-byte char —
        // reaching this assertion at all is the proof (an unsafe/naive
        // byte-slice truncation would have panicked above on `str`
        // reconstruction).
        assert!(summary.preview.chars().all(|c| c != '\u{FFFD}'));
    }

    // ─── Test 11: content = full body + Content-Type branch + ETag ────────

    #[tokio::test]
    async fn step_content_in_memory_fallback_is_json_with_matching_etag() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hi")))
            .await
            .expect("tasks_start")
            .0;

        let resp = step_content(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "out".to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_content")
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("content-type header")
            .to_str()
            .expect("ascii");
        assert_eq!(content_type, "application/json");
        let etag = resp
            .headers()
            .get(header::ETAG)
            .expect("etag header")
            .to_str()
            .expect("ascii")
            .to_string();
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let expected_sha = hex::encode(sha2::Sha256::digest(&body_bytes));
        assert_eq!(etag, format!("\"sha256:{expected_sha}\""));
        let parsed: Value = serde_json::from_slice(&body_bytes).expect("valid json body");
        assert_eq!(parsed["echoed"], json!("hi"));
    }

    /// Test 11 (materialized-file half): when the producing step's
    /// submission was materialized to disk (`work_dir` resolved),
    /// `step_content` serves the RAW file bytes as `text/markdown`, not
    /// the in-memory JSON fallback.
    #[tokio::test]
    async fn step_content_materialized_file_is_served_as_markdown() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state();
        let mut req = greeting_task_req("materialized");
        req.work_dir = Some(dir.path().to_string_lossy().into_owned());
        let posted = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("tasks_start")
            .0;

        let identity_name = mlua_swarm::worker::baseline::AG_IDENTITY;
        let resp = step_content(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                identity_name.to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect("step_content")
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("content-type header")
            .to_str()
            .expect("ascii");
        assert_eq!(content_type, "text/markdown; charset=utf-8");
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let body_str = String::from_utf8(body_bytes.to_vec()).expect("utf8 body");
        assert!(
            body_str.contains("```json"),
            "materialized file must carry the fenced json block: {body_str}"
        );
    }

    // ─── Test 12: content `?path=` narrow → application/json fragment ─────

    #[tokio::test]
    async fn step_content_path_narrow_returns_json_fragment() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("narrowed")))
            .await
            .expect("tasks_start")
            .0;

        let resp = step_content(
            State(state.clone()),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "out".to_string(),
            )),
            Query(StepPathQuery {
                path: Some("echoed".to_string()),
            }),
        )
        .await
        .expect("step_content narrowed")
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("content-type header")
            .to_str()
            .expect("ascii");
        assert_eq!(content_type, "application/json");
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let parsed: Value = serde_json::from_slice(&body_bytes).expect("valid json body");
        assert_eq!(parsed, json!("narrowed"));
    }

    // ─── Test 13: unknown task / run / step → 404 ───────────────────────────

    #[tokio::test]
    async fn steps_list_unknown_task_returns_404() {
        let state = test_state();
        let err = steps_list(
            State(state),
            Path(("T-does-not-exist".to_string(), "latest".to_string())),
        )
        .await
        .expect_err("unknown task must 404");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn steps_list_unknown_run_returns_404() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hi")))
            .await
            .expect("tasks_start")
            .0;
        let err = steps_list(
            State(state),
            Path((posted.task_id.to_string(), "R-does-not-exist".to_string())),
        )
        .await
        .expect_err("unknown run must 404");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn step_get_unknown_step_returns_404() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hi")))
            .await
            .expect("tasks_start")
            .0;
        let err = step_get(
            State(state),
            Path((
                posted.task_id.to_string(),
                "latest".to_string(),
                "does-not-exist".to_string(),
            )),
            Query(StepPathQuery::default()),
        )
        .await
        .expect_err("unknown step must 404");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    // ─── Test 14: the old /ctx route is gone ────────────────────────────────

    #[tokio::test]
    async fn old_ctx_route_returns_404_not_found_by_router() {
        let engine = Engine::new(EngineCfg::default());
        let router = mlua_swarm_server_router_for_test(engine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{addr}/v1/tasks/T-anything/ctx"))
            .send()
            .await
            .expect("request");
        assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    }

    /// Local alias so the test above reads as "the crate's router", without
    /// importing `crate::build_router` under a name that shadows this
    /// module's own items.
    fn mlua_swarm_server_router_for_test(engine: Engine) -> axum::Router {
        crate::build_router(engine)
    }

    // ─── McpQueryAdapter: single-key resolve (still exercised standalone) ───

    #[test]
    fn mcp_query_adapter_project_builds_query_ref() {
        let adapter = McpQueryAdapter::new(
            Arc::new(InMemoryOutputStore::new()),
            Arc::new(InMemoryRunStore::new()),
            Engine::new(EngineCfg::default()),
        );
        let key = ProjectionKey {
            task_id: "T-abc".to_string(),
            run_id: None,
            step: Some("planner".to_string()),
            path: None,
        };
        let ctx_data = json!({"planner": {"plan": "do it"}});
        let reference = adapter.project(&key, &ctx_data).expect("project");
        match &reference {
            ProjectionRef::Query { endpoint, key: k } => {
                assert!(endpoint.contains("/steps/planner/content"));
                assert_eq!(k, &key);
            }
            other => panic!("expected Query ref, got {other:?}"),
        }
        let line = adapter.pointer_line(&reference);
        assert!(line.contains("T-abc"));
    }

    #[test]
    fn mcp_query_adapter_project_rejects_key_not_present_in_ctx_data() {
        let adapter = McpQueryAdapter::new(
            Arc::new(InMemoryOutputStore::new()),
            Arc::new(InMemoryRunStore::new()),
            Engine::new(EngineCfg::default()),
        );
        let key = ProjectionKey {
            task_id: "T-abc".to_string(),
            run_id: None,
            step: Some("missing".to_string()),
            path: None,
        };
        let err = adapter.project(&key, &json!({"planner": {}})).unwrap_err();
        assert!(matches!(err, ProjectionError::NotFound(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mcp_query_adapter_fetch_bridges_to_resolve_async() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("bridged")))
            .await
            .expect("tasks_start")
            .0;

        let adapter = McpQueryAdapter::new(
            state.data_store.clone(),
            state.run_store.clone(),
            state.engine.clone(),
        );
        let key = ProjectionKey {
            task_id: posted.task_id.to_string(),
            run_id: None,
            step: Some("out".to_string()),
            path: Some("echoed".to_string()),
        };
        // `fetch` is a sync trait method that bridges to `resolve_async`
        // via `block_in_place` + `Handle::block_on` — calling it directly
        // (not via `spawn_blocking`, which runs on the *blocking* pool
        // rather than a runtime worker thread and is not a valid
        // `block_in_place` call site) from this multi-thread-flavor test
        // task is exactly the context the bridge is built for (module
        // doc).
        let value = adapter.fetch(&key).expect("fetch");
        assert_eq!(value, json!("bridged"));
    }

    // ─── subtask-4 / ST2 rework: Data-plane-backed, in-flight-safe query ───

    /// Subtask 4 Test #5: path narrowing works against the Data-plane
    /// `Final` content — `AG_IDENTITY`'s own name (the producer_agent
    /// `Engine::submit_output`'s dual-write submits under) is queryable
    /// directly against the Run-scoped Data-plane store, narrowed by
    /// `path`. GH #23 subtask-3: `greeting_blueprint`'s flow.ir ctx-path
    /// segment `"out"` is now an ALIAS of this same canonical entry (not
    /// a separate `result_ref`-only name) — `mcp_query_adapter_fetch_bridges_to_resolve_async`
    /// above queries `"out"` and, since the table unifies it with
    /// `AG_IDENTITY`'s canonical entry, now resolves via this SAME
    /// Data-plane path too, not the `result_ref` fallback.
    #[tokio::test]
    async fn resolve_async_path_narrows_within_data_plane_final_content() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(greeting_task_req("hi")))
            .await
            .expect("tasks_start")
            .0;

        let adapter = McpQueryAdapter::new(
            state.data_store.clone(),
            state.run_store.clone(),
            state.engine.clone(),
        );
        let key = ProjectionKey {
            task_id: posted.task_id.to_string(),
            run_id: None,
            step: Some(mlua_swarm::worker::baseline::AG_IDENTITY.to_string()),
            path: Some("echoed".to_string()),
        };
        let (_run, value) = adapter.resolve_async(&key).await.expect("resolve_async");
        assert_eq!(value, json!("hi"));
    }

    /// Subtask 4 Test #1 (the in-flight scenario this rework exists for):
    /// a 2-step `Seq` flow where `step2` blocks on a gate until the test
    /// releases it. By the time `step2` has started, `step1`'s
    /// `dispatch_attempt_with` — and therefore its `submit_output` (and
    /// this rework's dual-write into the Data-plane store), plus its
    /// `RunRecord.step_entries` append — has unconditionally already
    /// completed (flow.ir's `Seq` awaits each child before starting the
    /// next), while the overall Run is still `Running` (not yet
    /// finalized). `GET /v1/tasks/:id/runs/:run/steps/step1` must return
    /// `step1`'s OUTPUT during that window.
    #[tokio::test(flavor = "multi_thread")]
    async fn steps_list_returns_in_flight_step_output_before_run_completes() {
        use mlua_flow_ir::{Expr, Node as FlowNode};
        use mlua_swarm::worker::adapter::WorkerResult;
        use mlua_swarm::{RustFnInProcessSpawnerFactory, SpawnerRegistry};

        let started = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(tokio::sync::Notify::new());
        let started_bg = started.clone();
        let gate_bg = gate.clone();

        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("step1", |inv| async move {
                Ok(WorkerResult {
                    value: json!({ "step1_out": inv.prompt }),
                    ok: true,
                })
            })
            .register_fn("step2", move |_inv| {
                let started = started_bg.clone();
                let gate = gate_bg.clone();
                async move {
                    started.notify_one();
                    gate.notified().await;
                    Ok(WorkerResult {
                        value: json!("step2 done"),
                        ok: true,
                    })
                }
            });
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));

        let engine = Engine::new_with_layers(EngineCfg::default(), crate::default_layer_registry());
        let data_store: Arc<dyn mlua_swarm::store::output::OutputStore> =
            Arc::new(InMemoryOutputStore::new());
        engine.set_output_store(data_store.clone());
        let compiler = mlua_swarm::Compiler::new(reg);
        let launch = Arc::new(mlua_swarm::TaskLaunchService::new(engine.clone(), compiler));
        let state = AppState {
            engine,
            sessions: Arc::new(Mutex::new(crate::SessionStore::default())),
            task_app: Arc::new(mlua_swarm::TaskApplication::new_inline_only(launch)),
            ws_operator_factory: None,
            data_store,
            operator_sessions: Arc::new(Mutex::new(HashMap::new())),
            roles_to_sid: Arc::new(Mutex::new(HashMap::new())),
            task_store: Arc::new(InMemoryTaskStore::new()),
            run_store: Arc::new(InMemoryRunStore::new()),
            base_url: None,
            sync_timeout_secs: 300,
        };

        let flow = FlowNode::Seq {
            children: vec![
                FlowNode::Step {
                    ref_: "step1".to_string(),
                    in_: Expr::Path {
                        at: "$.greeting".to_string(),
                    },
                    out: Expr::Path {
                        at: "$.step1".to_string(),
                    },
                },
                FlowNode::Step {
                    ref_: "step2".to_string(),
                    in_: Expr::Path {
                        at: "$.step1".to_string(),
                    },
                    out: Expr::Path {
                        at: "$.step2".to_string(),
                    },
                },
            ],
        };
        let blueprint = Blueprint {
            schema_version: current_schema_version(),
            id: "projection-test-in-flight-bp".into(),
            flow,
            agents: vec![
                AgentDef {
                    name: "step1".into(),
                    kind: AgentKind::RustFn,
                    spec: json!({"fn_id": "step1"}),
                    profile: None,
                    meta: None,
                },
                AgentDef {
                    name: "step2".into(),
                    kind: AgentKind::RustFn,
                    spec: json!({"fn_id": "step2"}),
                    profile: None,
                    meta: None,
                },
            ],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
        };

        let req = TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(blueprint),
            },
            init_ctx: json!({ "greeting": "hi" }),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            timeout_secs: None,
            goal: None,
            detach: false,
        };

        let state_bg = state.clone();
        let launch_handle =
            tokio::spawn(async move { crate::tasks_start(State(state_bg), Json(req)).await });

        // step2 signals `started` only after step1's dispatch (and its
        // submit_output / Data-plane dual-write, and its step_entries
        // append) has fully returned — see the doc above.
        started.notified().await;

        let in_flight_tasks = state.task_store.list().await.expect("task_store list");
        assert_eq!(in_flight_tasks.len(), 1, "exactly one Task minted");
        let task_id = in_flight_tasks[0].id.clone();

        let resp = steps_list(
            State(state.clone()),
            Path((task_id.to_string(), "latest".to_string())),
        )
        .await
        .expect("steps_list while step2 is still in flight");
        let step1_entry = resp
            .steps
            .iter()
            .find(|s| s.name == "step1")
            .expect("step1 must already be visible");
        assert_eq!(step1_entry.source, ProjectionSource::DataPlane);

        // Release step2 so the background `tasks_start` can complete and
        // the test can join it cleanly.
        gate.notify_one();
        let posted = launch_handle.await.expect("join").expect("tasks_start").0;
        assert_eq!(posted.final_ctx["step2"], json!("step2 done"));
    }
}
