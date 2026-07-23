//! HTTP surface for inspecting Blueprint state (= for debug / animation verification).
//! `/v1/blueprints/:id/head` returns the head Blueprint JSON;
//! `/v1/blueprints/:id/history` returns the commit-version list;
//! `/v1/blueprints/:id/binding-requirements` returns the declaration-side
//! `BindRequest` list an operator's capability manifest must cover.
//! Callers pass a shared `Store` via `Arc` and mount the router.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use mlua_swarm::blueprint::loader::pre_read_default_agent_kind;
use mlua_swarm::blueprint::store::{
    blueprint_version, BlueprintId, BlueprintStore, CommitMetadata,
};
use mlua_swarm::blueprint::{default_global_agent_kind, AgentKind, Blueprint};
use mlua_swarm::core::explain::{explain_agent_ctx, CtxTier};
use mlua_swarm::core::step_naming::StepNaming;
use mlua_swarm::operator::render::template_variables;
use mlua_swarm::{binding_requests, LegacyWorkerBindingPolicy};
use mlua_swarm_compile::{
    env_blueprint_includes, expand_file_refs_with_config, pre_read_in_bp_includes, ResolveConfig,
};
use mlua_swarm_schema::{
    resolve_bound_agents, resolve_bound_agents_strict, resolve_runner, BindRequest, BindingDigest,
    Runner, RunnerResolutionSource,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Router state: BP store + the base dir used to resolve `$file` / `$agent_md`
/// refs + `default_agent_kind` from the CLI (= layer (2) of the 4-tier cascade —
/// the CLI override layer).
/// When `ref_base = None`, ref expansion is skipped (= seed bodies are parsed
/// as raw JSON).
#[derive(Clone)]
pub struct BlueprintsState {
    /// Backing Blueprint store (git2 or in-memory backend).
    pub store: Arc<dyn BlueprintStore>,
    /// Base dir for `$file` / `$agent_md` ref expansion; `None` skips expansion.
    pub ref_base: Option<PathBuf>,
    /// Additional directories (tier 5 of the include cascade — see
    /// `mlua-swarm-compile::ResolveConfig`) searched after `ref_base`
    /// (tier 1). Empty vec = no server-config includes; the register
    /// path still walks the in-bp and env tiers.
    pub ref_includes: Vec<PathBuf>,
    /// CLI-level `default_agent_kind` override (layer (2) of the 4-tier cascade).
    pub cli_default_agent_kind: Option<AgentKind>,
    /// Server-side strict-embed switch (design table row 3, Phase 6 —
    /// issue 4c4e3eb8). When `true`, `POST /v1/blueprints/:id`
    /// refuses raw bodies that still carry `$file` / `$agent_md` refs
    /// (returns 400 with a hint pointing at `mse bp build
    /// --strict-embed`), so ref resolution is pushed onto the client
    /// and the server only ever sees pre-embedded Blueprint JSON.
    /// Default `false` = the server runs the linker itself
    /// (backward-compat). Wired from
    /// [`crate::config::ResolvedConfig::blueprint_strict_embed`] via
    /// the CLI `--blueprint-strict-embed` flag or the config-file
    /// `blueprint_strict_embed` key.
    pub strict_embed: bool,
    /// Migration gate for the deprecated `AgentProfile.worker_binding`
    /// Runner fallback, wired from the same server config the launch path
    /// uses ([`crate::config::ResolvedConfig::legacy_worker_binding_policy`]).
    /// `GET /v1/blueprints/:id/binding-requirements` applies this policy so
    /// its declared requirements match what launch will actually resolve.
    /// Defaults to `Allow` (compat) in [`build_blueprints_router`].
    pub legacy_worker_binding_policy: LegacyWorkerBindingPolicy,
}

/// Minimal entry: no `ref_base` (ref expansion skipped), no CLI default
/// kind override, and `strict_embed = false` (backward-compat = the
/// server accepts raw refs and runs the linker itself when `ref_base` is
/// set).
pub fn build_blueprints_router(store: Arc<dyn BlueprintStore>) -> Router {
    build_blueprints_router_with_refs(
        store,
        None,
        Vec::new(),
        None,
        false,
        LegacyWorkerBindingPolicy::Allow,
    )
}

/// When `ref_base` is set, `seed_blueprint` resolves `{"$file": ...}` /
/// `{"$agent_md": ...}` refs in the body under that base dir and expands them.
/// Path hygiene (absolute paths and `..` are rejected) is enforced inside
/// `expand_file_refs`, sandboxed to the subtree under the base dir.
///
/// `cli_default_agent_kind` = the override from CLI `--default-agent-kind`
/// (= layer (2) of the 4-tier cascade). Falls back when the BP JSON top-level
/// `default_agent_kind` (= (3)) is absent; if that too is absent, uses the
/// Schema `impl Default` = `Operator` (= (1)).
///
/// `legacy_worker_binding_policy` is the migration gate the launch path
/// applies; `GET /v1/blueprints/:id/binding-requirements` reuses it so its
/// declared requirements match what launch resolves.
pub fn build_blueprints_router_with_refs(
    store: Arc<dyn BlueprintStore>,
    ref_base: Option<PathBuf>,
    ref_includes: Vec<PathBuf>,
    cli_default_agent_kind: Option<AgentKind>,
    strict_embed: bool,
    legacy_worker_binding_policy: LegacyWorkerBindingPolicy,
) -> Router {
    let state = BlueprintsState {
        store,
        ref_base,
        ref_includes,
        cli_default_agent_kind,
        strict_embed,
        legacy_worker_binding_policy,
    };
    Router::new()
        .route("/v1/blueprints/:id/head", get(get_head))
        .route("/v1/blueprints/:id/history", get(get_history))
        .route(
            "/v1/blueprints/:id/binding-requirements",
            get(binding_requirements),
        )
        .route(
            "/v1/blueprints/:id/agents/:agent/explain",
            get(explain_agent),
        )
        .route(
            "/v1/blueprints/:id/agents/explain",
            get(explain_agents_batch),
        )
        .route("/v1/blueprints/:id/unarchive", post(unarchive_blueprint))
        .route(
            "/v1/blueprints/:id",
            post(seed_blueprint).delete(archive_blueprint),
        )
        .with_state(state)
}

/// `DELETE /v1/blueprints/:id` — archive (logical soft-delete) the id.
/// Appends an archive marker commit; the underlying Blueprint YAML is
/// preserved as history. After archive, `read_head` /
/// `TaskApplication::resolve` reject with `Archived`, and `list_ids`
/// filters the id out by default.
///
/// Semantic rename: the HTTP path stays `DELETE` for client
/// compatibility, but the behavior is archive, not physical delete.
/// Restore via `POST /v1/blueprints/:id/unarchive`.
///
/// Returns: 204 No Content.
async fn archive_blueprint(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let bp_id = BlueprintId::new(id.clone());
    state.store.archive_id(&bp_id).await.map_err(|e| match e {
        mlua_swarm::blueprint::store::BlueprintStoreError::HeadEmpty(_)
        | mlua_swarm::blueprint::store::BlueprintStoreError::IdNotFound(_) => {
            (StatusCode::NOT_FOUND, format!("archive_id: {e}"))
        }
        other => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("archive_id: {other}"),
        ),
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/blueprints/:id/unarchive` — reverse of archive. Appends
/// an unarchive marker commit so the audit trail records the event.
async fn unarchive_blueprint(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let bp_id = BlueprintId::new(id.clone());
    state
        .store
        .unarchive_id(&bp_id)
        .await
        .map_err(|e| match e {
            mlua_swarm::blueprint::store::BlueprintStoreError::HeadEmpty(_)
            | mlua_swarm::blueprint::store::BlueprintStoreError::IdNotFound(_) => {
                (StatusCode::NOT_FOUND, format!("unarchive_id: {e}"))
            }
            other => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unarchive_id: {other}"),
            ),
        })?;
    Ok(StatusCode::NO_CONTENT)
}

/// Format a Blueprint deserialization failure with a schema pointer, so a
/// register error is self-serviceable (the schema export is the MCP adapter
/// `bp_schema` tool = schemars JSON Schema of `Blueprint`).
fn parse_error_with_schema_hint(e: &serde_json::Error) -> String {
    format!(
        "blueprint parse: {e} \
         (hint: fetch the Blueprint JSON Schema via the MCP adapter bp_schema tool)"
    )
}

/// Walk the raw seed body and collect the relative paths of every
/// `{"$file": "..."}` / `{"$agent_md": "..."}` ref still present.
/// Returns `None` when the body is already fully embedded (= no refs
/// left), `Some(paths)` otherwise. Used by [`seed_blueprint`] to gate
/// the `strict_embed` opt-in (design table row 3 — server-side strict
/// mode refuses raw refs so clients must `mse bp build --strict-embed`
/// upstream).
fn collect_unembedded_refs(val: &serde_json::Value) -> Option<Vec<String>> {
    let mut acc: Vec<String> = Vec::new();
    walk_refs(val, &mut acc);
    if acc.is_empty() {
        None
    } else {
        Some(acc)
    }
}

fn walk_refs(val: &serde_json::Value, acc: &mut Vec<String>) {
    match val {
        serde_json::Value::Object(map) => {
            for key in ["$file", "$agent_md"] {
                if let Some(serde_json::Value::String(rel)) = map.get(key) {
                    acc.push(format!("{key}={rel}"));
                }
            }
            for v in map.values() {
                walk_refs(v, acc);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                walk_refs(v, acc);
            }
        }
        _ => {}
    }
}

/// Format the ref-expand failure with an include-cascade fix hint. The
/// underlying [`mlua_swarm_compile::LoadError::FileRef`] message already
/// names every searched dir (see `linker.rs::resolve_ref_path`); this
/// wrapper appends the actionable knobs so authors know which tier to
/// extend.
fn ref_expand_error_with_fix_hint(e: &mlua_swarm_compile::LoadError) -> String {
    format!(
        "ref expand: {e} \
         (fix: extend the include cascade — add the containing directory via CLI \
         `--include <DIR>` on `mse serve`, env `MSE_BLUEPRINT_INCLUDES`, config-file \
         `blueprint_ref_includes`, or in-bp top-level `blueprint_ref_includes = {{...}}`; \
         or pre-embed refs client-side via `mse bp build --strict-embed`)"
    )
}

/// `POST /v1/blueprints/:id` — register / re-register a Blueprint.
///
/// Semantics:
/// - No prior head → seed as first commit (`write_new`, empty
///   parents). Returns 201.
/// - Prior head with **same** `ContentHash` → idempotent no-op.
///   Returns 200 with `seeded: false`.
/// - Prior head with **different** `ContentHash` → append a new
///   commit on top of the current head (Git-native commit graph
///   advance). Returns 201.
/// - Prior head archived → returns 409 `Archived` (call
///   `POST /:id/unarchive` first).
/// - Concurrent POST on the same id → per-id lock contention returns
///   429 Too Many Requests (client retry).
///
/// Path id vs body.id mismatch returns 400.
///
/// When `BlueprintsState.ref_base = Some(dir)`, `{"$file": ...}` /
/// `{"$agent_md": ...}` refs in the body are expanded under the base
/// dir via `expand_file_refs` before being parsed into a typed
/// `Blueprint` (= path hygiene is applied by the loader, rejecting
/// absolute paths and `..`).
async fn seed_blueprint(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
    Json(raw_body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    // Design table row 3, Phase 6 (issue 4c4e3eb8): strict-embed
    // pre-check. When enabled, refuse any raw body that still carries
    // `$file` / `$agent_md` refs — ref resolution is pushed onto the
    // client. Runs before the ref-base branch so it catches raw refs
    // even when the server has no `ref_base` configured.
    if state.strict_embed {
        if let Some(refs) = collect_unembedded_refs(&raw_body) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "strict_embed: raw body carries unembedded refs ({}); \
                     pre-embed client-side via `mse bp build --strict-embed` \
                     and POST the fully-resolved Blueprint JSON",
                    refs.join(", ")
                ),
            ));
        }
    }
    let body: Blueprint = if let Some(base) = state.ref_base.as_ref() {
        // Four-tier cascade for the kind resolution: (3) BP JSON top-level
        // `default_agent_kind` → (2) CLI value → (1) Schema impl Default =
        // Operator. Handed to expand_file_refs so the loader can resolve the
        // kind when the $agent_md sibling is missing. The sibling `"kind"`
        // literal (tier 4) wins first inside expand_file_refs.
        let default_kind = match pre_read_default_agent_kind(&raw_body) {
            // BP top-level carries a literal → use it verbatim.
            kind if raw_body.get("default_agent_kind").is_some() => kind,
            // BP top-level absent → CLI value fallback → Schema default.
            _ => state
                .cli_default_agent_kind
                .clone()
                .unwrap_or_else(default_global_agent_kind),
        };
        // Six-tier include cascade: (1) ref_base = bp.lua parent, (2)
        // in-bp `blueprint_ref_includes`, (3) env
        // `MSE_BLUEPRINT_INCLUDES`, (5) server config
        // `blueprint_ref_includes`. Tiers 4 (CLI `--include` on the
        // client) and 6 (bundled default) are client-side only —
        // server-side never sees them.
        let cfg = ResolveConfig::new(base.clone())
            .with_in_bp_includes(pre_read_in_bp_includes(&raw_body))
            .with_env_includes(env_blueprint_includes())
            .with_config_includes(state.ref_includes.clone());
        let expanded = expand_file_refs_with_config(raw_body, &cfg, default_kind)
            .map_err(|e| (StatusCode::BAD_REQUEST, ref_expand_error_with_fix_hint(&e)))?;
        serde_json::from_value(expanded)
            .map_err(|e| (StatusCode::BAD_REQUEST, parse_error_with_schema_hint(&e)))?
    } else {
        serde_json::from_value(raw_body)
            .map_err(|e| (StatusCode::BAD_REQUEST, parse_error_with_schema_hint(&e)))?
    };
    let store = state.store;
    if id != body.id.as_str() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("path id={id} != body.id={}", body.id),
        ));
    }
    let bp_id = BlueprintId::new(id.clone());
    let v = blueprint_version(&body).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("bp version: {e}"),
        )
    })?;
    let prev_head = match store.read_head(&bp_id).await {
        Ok(traced) => Some(traced),
        Err(mlua_swarm::blueprint::store::BlueprintStoreError::HeadEmpty(_)) => None,
        Err(mlua_swarm::blueprint::store::BlueprintStoreError::Archived(_)) => {
            return Err((
                StatusCode::CONFLICT,
                format!("blueprint {id} is archived; POST /v1/blueprints/{id}/unarchive first"),
            ));
        }
        Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("read_head: {e}")));
        }
    };
    if let Some(traced) = &prev_head {
        if traced.trace.version == v {
            return Ok((
                StatusCode::OK,
                Json(serde_json::json!({"id": id, "version": format!("{:?}", v), "seeded": false})),
            ));
        }
    }
    let parents: Vec<_> = prev_head
        .as_ref()
        .map(|t| vec![t.trace.version])
        .unwrap_or_default();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .as_millis() as i64;
    let meta = CommitMetadata::seed(bp_id.clone(), v, now_ms);
    store
        .write_new(&bp_id, &body, &parents, meta)
        .await
        .map_err(|e| match &e {
            mlua_swarm::blueprint::store::BlueprintStoreError::LockBusy => (
                StatusCode::TOO_MANY_REQUESTS,
                format!("blueprint {id} lock busy; retry"),
            ),
            mlua_swarm::blueprint::store::BlueprintStoreError::Archived(_) => (
                StatusCode::CONFLICT,
                format!("blueprint {id} is archived; POST /v1/blueprints/{id}/unarchive first"),
            ),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, format!("write_new: {e}")),
        })?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({"id": id, "version": format!("{:?}", v), "seeded": true})),
    ))
}

#[derive(Debug, Serialize)]
struct HeadResponse {
    id: String,
    version: String,
    blueprint: Blueprint,
}

async fn get_head(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
) -> Result<Json<HeadResponse>, (StatusCode, String)> {
    let store = state.store;
    let bp_id = BlueprintId::new(id.clone());
    let traced = store
        .read_head(&bp_id)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("read_head: {e}")))?;
    Ok(Json(HeadResponse {
        id,
        version: format!("{:?}", traced.trace.version),
        blueprint: traced.value,
    }))
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

#[derive(Debug, Serialize)]
struct HistoryEntry {
    /// Content hash (= debug representation of `BlueprintVersion`).
    hash: String,
    /// SemVer label (`Blueprint.metadata.version_label`); `null` when unset.
    version_label: Option<String>,
    /// One-line changelog (= `CommitMetadata.rationale`).
    rationale: String,
}

#[derive(Debug, Serialize)]
struct HistoryResponse {
    count: usize,
    entries: Vec<HistoryEntry>,
}

async fn get_history(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
    Query(q): Query<HistoryQuery>,
) -> Result<Json<HistoryResponse>, (StatusCode, String)> {
    let store = state.store;
    let bp_id = BlueprintId::new(id);
    let versions = store
        .history(&bp_id, q.limit)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("history: {e}")))?;
    let mut entries = Vec::with_capacity(versions.len());
    for v in versions {
        let traced = store.read_version(&bp_id, v).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("read_version: {e}"),
            )
        })?;
        let rationale = store
            .read_commit_rationale(&bp_id, v)
            .await
            .unwrap_or(None)
            .unwrap_or_default();
        entries.push(HistoryEntry {
            hash: format!("{:?}", v),
            version_label: traced.value.metadata.version_label.clone(),
            rationale,
        });
    }
    let count = entries.len();
    Ok(Json(HistoryResponse { count, entries }))
}

// ──────────────────────────────────────────────────────────────────────────
// GET /v1/blueprints/:id/binding-requirements
// ──────────────────────────────────────────────────────────────────────────

/// Response body for `GET /v1/blueprints/:id/binding-requirements`.
///
/// The declaration-side reverse lookup an operator uses to machine-generate
/// its capability manifest: one [`BindRequest`] per Runner-backed agent,
/// byte-identical to what `binding_requests` reconstructs on the launch
/// path. Read-only and provider-free — nothing here mutates the registry or
/// calls a binding provider (requirements are pure declarations; attestation
/// happens only when a Run is dispatched).
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct BindingRequirementsResponse {
    /// Blueprint id (echoed back from the path param).
    pub blueprint_id: String,
    /// `CompilerStrategy.strict_binding` for this Blueprint — whether launch
    /// fails closed when a requirement is left unattested.
    pub strict_binding: bool,
    /// One request per Runner-backed agent, in Blueprint declaration order;
    /// `[]` when no agent resolves to a Runner.
    pub requirements: Vec<BindRequest>,
}

/// `GET /v1/blueprints/:id/binding-requirements` — the reverse lookup an
/// operator uses to machine-generate its capability manifest: what bindings
/// does this registered Blueprint require? Resolves the head Blueprint's
/// Runner-backed agents under the SAME [`LegacyWorkerBindingPolicy`] the
/// launch path applies (so the returned requirements match what launch will
/// actually request), then reconstructs the platform-neutral `BindRequest`
/// list via `binding_requests`.
///
/// Read-only, provider-free, and the same unauthenticated diagnostic trust
/// tier as [`get_head`]: it never mutates the registry and never calls a
/// binding provider.
///
/// - Unknown Blueprint id → 404 (same error mapping as [`get_head`]).
/// - Resolution failure (a legacy `profile.worker_binding` rejected under
///   `LegacyWorkerBindingPolicy::Reject`, or an unresolvable `runner_ref` /
///   `default_runner`) → 422 carrying the resolve error message.
async fn binding_requirements(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
) -> Result<Json<BindingRequirementsResponse>, (StatusCode, String)> {
    let store = state.store;
    let bp_id = BlueprintId::new(id.clone());
    let traced = store
        .read_head(&bp_id)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("read_head: {e}")))?;
    let bp = traced.value;

    // Apply the SAME legacy-worker-binding policy the launch path uses
    // (`TaskLaunchService::load_or_resolve_bound_agents`) so the declared
    // requirements match what launch will actually resolve.
    let bound = match state.legacy_worker_binding_policy {
        LegacyWorkerBindingPolicy::Allow => resolve_bound_agents(&bp),
        LegacyWorkerBindingPolicy::Reject => resolve_bound_agents_strict(&bp),
    }
    .map_err(|e| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("resolve bound agents: {e}"),
        )
    })?;

    Ok(Json(BindingRequirementsResponse {
        blueprint_id: id,
        strict_binding: bp.strategy.strict_binding,
        requirements: binding_requests(&bound),
    }))
}

// ──────────────────────────────────────────────────────────────────────────
// GET /v1/blueprints/:id/agents/:agent/explain
// ──────────────────────────────────────────────────────────────────────────

/// `blueprint` field of [`ExplainAgentResponse`]: which Blueprint this
/// explain view was resolved against.
#[derive(Debug, Serialize)]
struct ExplainBlueprintRef {
    /// Blueprint id (echoed back from the path param).
    id: String,
    /// Head commit version (`Trace.version`, debug-formatted — same
    /// convention as [`HeadResponse::version`]).
    version: String,
}

/// `agent` field of [`ExplainAgentResponse`]: the resolved agent's
/// identity, verbatim from the Blueprint's `AgentDef`.
#[derive(Debug, Serialize)]
struct ExplainAgentRef {
    /// Agent name (= `AgentDef.name`, echoed back from the path param).
    name: String,
    /// Worker IMPL kind (= `AgentDef.kind`).
    kind: AgentKind,
}

/// `worker_binding` field of [`ExplainAgentResponse`] when the agent
/// declares one. Mirrors `mlua_swarm::operator::WorkerBinding::variant`;
/// its `tools` half is reported separately under `declared_tools`, so it
/// is not duplicated here.
#[derive(Debug, Serialize)]
struct ExplainWorkerBinding {
    /// Worker variant name (`AgentDef.profile.worker_binding`).
    variant: String,
}

/// `declared_tools` field of [`ExplainAgentResponse`].
#[derive(Debug, Serialize)]
struct ExplainDeclaredTools {
    /// `AgentDef.profile.tools`, verbatim (`[]` when `profile` is absent).
    tools: Vec<String>,
    /// Always `true` — see [`Self::note`].
    informational: bool,
    /// Explains why `tools` does not grant anything by itself.
    note: String,
}

/// `system_prompt` field of [`ExplainAgentResponse`], present when
/// `AgentDef.profile.system_prompt` is non-empty.
#[derive(Debug, Serialize)]
struct ExplainSystemPrompt {
    /// UTF-8 byte length of the raw (unrendered) template.
    bytes: usize,
    /// Line count of the raw template (`str::lines` count).
    lines: usize,
    /// Variables `mlua_swarm::operator::render::template_variables`
    /// reports the template requires. Empty when
    /// [`Self::template_syntax_error`] is `Some`.
    template_variables: Vec<String>,
    /// `Some(message)` when the template failed to parse; `None`
    /// otherwise.
    template_syntax_error: Option<String>,
    /// Explains the non-`Object` `initial_directive` binding rule.
    note: String,
}

/// One key's entry in [`ExplainEffectiveCtx::keys`].
#[derive(Debug, Serialize)]
struct ExplainCtxKeyEntry {
    /// The value this key resolves to (the winning tier's value).
    value: serde_json::Value,
    /// Which static tier supplied [`Self::value`] — one of
    /// `"agent_inline"` / `"meta_ref"` / `"bp_global"`.
    winning_tier: String,
}

/// `effective_ctx` field of [`ExplainAgentResponse`]: the static 3-tier
/// cascade resolution `mlua_swarm::core::explain::explain_agent_ctx`
/// computes (byte-identical to the runtime merge — see that function's
/// doc for why this reuses rather than reimplements the merge).
#[derive(Debug, Serialize)]
struct ExplainEffectiveCtx {
    /// Per-key winner table.
    keys: BTreeMap<String, ExplainCtxKeyEntry>,
    /// Explains that Run/Task/Step runtime tiers are out of scope here.
    note: String,
}

/// `output` field of [`ExplainAgentResponse`].
#[derive(Debug, Serialize)]
struct ExplainOutput {
    /// The canonical step-projection name
    /// (`StepNaming::canonical_of_producer`), or the agent name itself as
    /// a fallback — see [`Self::naming_warnings`].
    projection_name: String,
    /// Non-empty when [`Self::projection_name`] fell back to the agent
    /// name, or `StepNaming::from_blueprint` itself failed (explain is a
    /// diagnostic view, so neither case 500s — see [`explain_agent`]'s
    /// doc).
    naming_warnings: Vec<String>,
    /// Explains the `{"out","parts"}` OUTPUT shape change for parts
    /// staging.
    parts_note: String,
}

/// `runner` field of [`ExplainAgentResponse`] (GH #46 Milestone 2) — the
/// Runner-tier doctor diagnostics for this agent. Read-only and purely
/// observational: nothing here gates compilation or dispatch (Milestone 3
/// wires the resolved Runner into the launch path; this endpoint stays a
/// diagnostic view), the same "surface it, never block"
/// BLOCK-disabled-by-default convention `bp_doctor`'s agent-md size check
/// already follows.
#[derive(Debug, Serialize)]
struct ExplainRunner {
    /// The Runner this agent resolves to via `resolve_runner`'s 5-tier
    /// cascade, when resolution succeeds. `None` when no tier declares a
    /// Runner (byte-compat: an agent with no `runner` / `runner_ref` /
    /// `profile.worker_binding` / `Blueprint.default_runner` resolves to
    /// `None` here, mirroring [`ExplainAgentResponse::worker_binding`]).
    resolved: Option<Runner>,
    /// Error-level finding: `Some(msg)` when `resolve_runner` returned an
    /// unresolved `runner_ref` / `default_runner` reference
    /// (`RunnerResolveError`, rendered via its `Display`).
    error: Option<String>,
    /// Warn-level finding: `Some(msg)` when the resolved Runner's backend
    /// disagrees with `AgentDef.kind` (`agent_block_in_process` paired
    /// with a non-`agent_block` kind, or a WebSocket Operator backend paired
    /// with `agent_block`). `None` when the pairing is consistent, or when
    /// [`Self::resolved`] is `None`.
    warning: Option<String>,
    /// Declaration tier selected by the immutable binding resolver.
    source: Option<RunnerResolutionSource>,
    /// Run/replay correlation digest over Agent, Runner, and Context policy.
    binding_digest: Option<BindingDigest>,
}

/// GH #46 M2 doctor check: does the resolved Runner's backend agree with
/// `AgentDef.kind` about which backend actually executes this agent? Pure
/// and read-only, never gates compile / dispatch (see [`ExplainRunner`]'s
/// doc).
fn runner_kind_mismatch_warning(
    runner: &Runner,
    kind: &AgentKind,
    agent_name: &str,
) -> Option<String> {
    match (runner, kind) {
        (Runner::AgentBlockInProcess { .. }, AgentKind::AgentBlock) => None,
        (Runner::AgentBlockInProcess { .. }, other) => Some(format!(
            "agent '{agent_name}' resolves to Runner::AgentBlockInProcess but AgentDef.kind = \
             {other:?} (expected AgentBlock)"
        )),
        (Runner::WsOperator { .. }, AgentKind::AgentBlock) => Some(format!(
            "agent '{agent_name}' resolves to Runner::WsOperator but AgentDef.kind = AgentBlock"
        )),
        (Runner::WsOperator { .. }, _) => None,
        (Runner::WsClaudeCode { .. }, AgentKind::AgentBlock) => Some(format!(
            "agent '{agent_name}' resolves to Runner::WsClaudeCode but AgentDef.kind = AgentBlock"
        )),
        (Runner::WsClaudeCode { .. }, _) => None,
    }
}

/// Response body for `GET /v1/blueprints/:id/agents/:agent/explain`.
#[derive(Debug, Serialize)]
struct ExplainAgentResponse {
    /// Which Blueprint this view was resolved against.
    blueprint: ExplainBlueprintRef,
    /// The resolved agent's identity.
    agent: ExplainAgentRef,
    /// The Blueprint-baked worker binding, if declared.
    worker_binding: Option<ExplainWorkerBinding>,
    /// `Some(reason)` when [`Self::worker_binding`] is `None`.
    binding_note: Option<String>,
    /// GH #46 M2 — Runner-tier doctor diagnostics (see [`ExplainRunner`]).
    runner: ExplainRunner,
    /// The agent's declared (informational-only) tool list.
    declared_tools: ExplainDeclaredTools,
    /// The rendered-template diagnostics, when `profile.system_prompt` is
    /// non-empty.
    system_prompt: Option<ExplainSystemPrompt>,
    /// The static ctx cascade resolution.
    effective_ctx: ExplainEffectiveCtx,
    /// The step-projection naming resolution.
    output: ExplainOutput,
}

/// Maps a static [`CtxTier`] to the wire label
/// [`ExplainCtxKeyEntry::winning_tier`] reports.
fn ctx_tier_label(tier: CtxTier) -> &'static str {
    match tier {
        CtxTier::AgentInline => "agent_inline",
        CtxTier::MetaRef => "meta_ref",
        CtxTier::BpGlobal => "bp_global",
    }
}

/// Builds [`ExplainSystemPrompt`] from a non-empty `profile.system_prompt`
/// template.
fn explain_system_prompt(template: &str) -> ExplainSystemPrompt {
    let (variables, template_syntax_error): (Vec<String>, Option<String>) =
        match template_variables(template) {
            Ok(vars) => (vars.into_iter().collect(), None),
            Err(e) => (Vec::new(), Some(e.to_string())),
        };
    ExplainSystemPrompt {
        bytes: template.len(),
        lines: template.lines().count(),
        template_variables: variables,
        template_syntax_error,
        note: "when the step directive is not a JSON object, only `value` is bound at render \
               time"
            .to_string(),
    }
}

/// `GET /v1/blueprints/:id/agents/:agent/explain` — read-only, dry-run
/// visualization of how `agent`'s Blueprint definition materializes into
/// its runtime worker contract (see `workspace/tasks/explain-agent/issue.md`
/// for the full design rationale). Same unauthenticated trust tier as
/// [`get_head`] (an operator-diagnostic route; no engine state is touched
/// — every value here is resolved statically from the head Blueprint
/// alone).
///
/// 404s when the Blueprint id itself is not found (same error mapping as
/// [`get_head`]) or when `agent` is not a name in `bp.agents` (JSON body:
/// `{"error", "agent", "available"}`). A `StepNaming::from_blueprint`
/// failure does not 500 — `output.projection_name` falls back to the
/// agent name and the failure is reported via `output.naming_warnings`
/// (this endpoint is a diagnostic view, not a compile gate).
async fn explain_agent(
    State(state): State<BlueprintsState>,
    Path((id, agent)): Path<(String, String)>,
) -> Result<Json<ExplainAgentResponse>, (StatusCode, String)> {
    let store = state.store;
    let bp_id = BlueprintId::new(id.clone());
    let traced = store
        .read_head(&bp_id)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("read_head: {e}")))?;
    let bp = traced.value;
    let version = format!("{:?}", traced.trace.version);

    let Some(agent_def) = bp.agents.iter().find(|ad| ad.name == agent) else {
        let available: Vec<&str> = bp.agents.iter().map(|ad| ad.name.as_str()).collect();
        return Err((
            StatusCode::NOT_FOUND,
            serde_json::json!({
                "error": "agent not found in blueprint",
                "agent": agent,
                "available": available,
            })
            .to_string(),
        ));
    };

    let profile = agent_def.profile.as_ref();

    let (worker_binding, binding_note) = match profile.and_then(|p| p.worker_binding.as_ref()) {
        Some(variant) => (
            Some(ExplainWorkerBinding {
                variant: variant.clone(),
            }),
            None,
        ),
        None => (
            None,
            Some(
                "no worker_binding declared; WS operator dispatch will fail at compile \
                 (InvalidSpec)"
                    .to_string(),
            ),
        ),
    };

    let declared_tools = ExplainDeclaredTools {
        tools: profile.map(|p| p.tools.clone()).unwrap_or_default(),
        informational: true,
        note: "declared tools do not grant anything; the effective tool surface is the worker \
               wrapper's frontmatter (see operator.rs WorkerBinding doc)"
            .to_string(),
    };

    // GH #46 M2 doctor checks: unresolved runner_ref / default_runner is
    // an error-level finding; a resolved-but-mismatched backend/kind pair
    // is a warn-level finding. Both are purely observational (see
    // `ExplainRunner`'s doc) — this never gates compile / dispatch.
    let bound = resolve_bound_agents(&bp)
        .ok()
        .and_then(|all| all.into_iter().find(|b| b.agent.name == agent_def.name));
    let runner = match resolve_runner(&bp, agent_def) {
        Ok(resolved) => {
            let warning = resolved
                .as_ref()
                .and_then(|r| runner_kind_mismatch_warning(r, &agent_def.kind, &agent_def.name));
            ExplainRunner {
                resolved,
                error: None,
                warning,
                source: bound.as_ref().map(|b| b.runner_source),
                binding_digest: bound.as_ref().map(|b| b.binding_digest.clone()),
            }
        }
        Err(e) => ExplainRunner {
            resolved: None,
            error: Some(e.to_string()),
            warning: None,
            source: None,
            binding_digest: None,
        },
    };

    let system_prompt = profile
        .filter(|p| !p.system_prompt.is_empty())
        .map(|p| explain_system_prompt(&p.system_prompt));

    let ctx_keys = explain_agent_ctx(&bp, &agent).unwrap_or_default();
    let effective_ctx = ExplainEffectiveCtx {
        keys: ctx_keys
            .into_iter()
            .map(|(k, resolution)| {
                (
                    k,
                    ExplainCtxKeyEntry {
                        value: resolution.value,
                        winning_tier: ctx_tier_label(resolution.winning_tier).to_string(),
                    },
                )
            })
            .collect(),
        note: "static tiers only; Run/Task/Step runtime tiers always win over these \
               (only-if-absent insertion order)"
            .to_string(),
    };

    let (projection_name, naming_warnings) = match StepNaming::from_blueprint(&bp) {
        Ok((naming, _soft_warnings)) => match naming.canonical_of_producer(&agent) {
            Some(canonical) => (canonical.to_string(), Vec::new()),
            None => (
                agent.clone(),
                vec![format!(
                    "agent '{agent}' does not appear in the blueprint's flow; using the agent \
                     name as a fallback projection name"
                )],
            ),
        },
        Err(e) => (
            agent.clone(),
            vec![format!("StepNaming::from_blueprint failed: {e}")],
        ),
    };

    let output = ExplainOutput {
        projection_name,
        naming_warnings,
        parts_note: "if the worker stages named artifact parts, the step OUTPUT changes shape \
                     to {\"out\", \"parts\"}; reference via $.<step>.out"
            .to_string(),
    };

    Ok(Json(ExplainAgentResponse {
        blueprint: ExplainBlueprintRef { id, version },
        agent: ExplainAgentRef {
            name: agent_def.name.clone(),
            kind: agent_def.kind.clone(),
        },
        worker_binding,
        binding_note,
        runner,
        declared_tools,
        system_prompt,
        effective_ctx,
        output,
    }))
}

// ──────────────────────────────────────────────────────────────────────────
// GET /v1/blueprints/:id/agents/explain (batch summary)
// ──────────────────────────────────────────────────────────────────────────

/// `worker_binding` field of [`AgentSummary`] — same shape as
/// [`ExplainWorkerBinding`] (kept as a distinct type so the batch response
/// schema doesn't couple to the single-agent view's naming).
#[derive(Debug, Serialize)]
struct WorkerBindingSummary {
    /// Worker variant name (`AgentDef.profile.worker_binding`).
    variant: String,
}

/// One row of [`BatchExplainAgentsResponse::agents`] — a summary, not the
/// full [`ExplainAgentResponse`] detail: a whole-Blueprint sweep response
/// must stay small, so this reports counts/presence rather than the raw
/// `declared_tools` list or the rendered `system_prompt` template. Drill
/// down via `GET /v1/blueprints/:id/agents/:agent/explain` for the full
/// per-agent view.
#[derive(Debug, Serialize)]
struct AgentSummary {
    /// Agent name (`AgentDef.name`).
    name: String,
    /// Worker IMPL kind (`AgentDef.kind`, debug-formatted — same
    /// convention as the `bp_doctor` MCP tool's per-agent `kind` field).
    kind: String,
    /// The Blueprint-baked worker binding, if declared. `null` (not
    /// omitted) when absent — the caller needs to see every agent,
    /// bound or not.
    worker_binding: Option<WorkerBindingSummary>,
    /// `AgentDef.profile.tools.len()`; `0` when `profile` is absent.
    declared_tools_count: usize,
    /// UTF-8 byte length of `profile.system_prompt`; `0` when `profile`
    /// is absent or the template is empty.
    system_prompt_bytes: usize,
    /// Number of keys `explain_agent_ctx` resolves for this agent (the
    /// static 3-tier cascade); `0` when the agent has no static ctx.
    effective_ctx_key_count: usize,
    /// The canonical step-projection name
    /// (`StepNaming::canonical_of_producer`), falling back to the agent
    /// name on a naming miss — same fail-soft convention as
    /// [`ExplainOutput::projection_name`], but without a
    /// `naming_warnings` companion (this is a summary row).
    projection_name: String,
}

/// Response body for `GET /v1/blueprints/:id/agents/explain`.
#[derive(Debug, Serialize)]
struct BatchExplainAgentsResponse {
    /// Which Blueprint this sweep was resolved against.
    blueprint: ExplainBlueprintRef,
    /// One row per `bp.agents` entry, in Blueprint order.
    agents: Vec<AgentSummary>,
}

/// `GET /v1/blueprints/:id/agents/explain` — batch summary sweep across
/// every agent in the Blueprint. Same read-only, dry-run, unauthenticated
/// trust tier as [`explain_agent`] / [`get_head`] — nothing here is
/// resolved beyond the head Blueprint.
///
/// 404s only when the Blueprint id itself is not found (same error
/// mapping as [`get_head`]); a Blueprint with zero agents returns
/// `agents: []`, not 404 (there is no per-agent path segment to fail to
/// resolve here). `StepNaming::from_blueprint` failing does not 500 —
/// every row's `projection_name` falls back to the agent name, mirroring
/// [`explain_agent`]'s per-agent fail-soft convention (this batch view
/// just has no `naming_warnings` companion field to report it through).
async fn explain_agents_batch(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
) -> Result<Json<BatchExplainAgentsResponse>, (StatusCode, String)> {
    let store = state.store;
    let bp_id = BlueprintId::new(id.clone());
    let traced = store
        .read_head(&bp_id)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("read_head: {e}")))?;
    let bp = traced.value;
    let version = format!("{:?}", traced.trace.version);

    // Resolved once for the whole Blueprint (StepNaming::from_blueprint is
    // a whole-BP operation, not per-agent); a failure fails soft the same
    // way explain_agent's per-agent lookup does — every row below falls
    // back to the agent name via `.unwrap_or_else`.
    let naming = StepNaming::from_blueprint(&bp)
        .ok()
        .map(|(naming, _)| naming);

    let agents = bp
        .agents
        .iter()
        .map(|agent_def| {
            let profile = agent_def.profile.as_ref();
            let worker_binding = profile
                .and_then(|p| p.worker_binding.as_ref())
                .map(|variant| WorkerBindingSummary {
                    variant: variant.clone(),
                });
            let declared_tools_count = profile.map(|p| p.tools.len()).unwrap_or(0);
            let system_prompt_bytes = profile.map(|p| p.system_prompt.len()).unwrap_or(0);
            let effective_ctx_key_count = explain_agent_ctx(&bp, &agent_def.name)
                .map(|keys| keys.len())
                .unwrap_or(0);
            let projection_name = naming
                .as_ref()
                .and_then(|naming| naming.canonical_of_producer(&agent_def.name))
                .map(|canonical| canonical.to_string())
                .unwrap_or_else(|| agent_def.name.clone());
            AgentSummary {
                name: agent_def.name.clone(),
                kind: format!("{:?}", agent_def.kind),
                worker_binding,
                declared_tools_count,
                system_prompt_bytes,
                effective_ctx_key_count,
                projection_name,
            }
        })
        .collect();

    Ok(Json(BatchExplainAgentsResponse {
        blueprint: ExplainBlueprintRef { id, version },
        agents,
    }))
}

#[cfg(test)]
mod explain_agent_tests {
    use super::*;
    use mlua_swarm::blueprint::store::InMemoryBlueprintStore;
    use mlua_swarm::blueprint::{
        current_schema_version, AgentDef, AgentMeta, AgentProfile, BlueprintMetadata,
        CompilerHints, CompilerStrategy,
    };
    use serde_json::json;

    fn agent_def(name: &str, profile: Option<AgentProfile>, meta: Option<AgentMeta>) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::RustFn,
            spec: json!({ "fn_id": name }),
            profile,
            meta,
            runner: None,
            runner_ref: None,
            verdict: None,
        }
    }

    /// A single-step Blueprint whose sole Step dispatches `agent_name` —
    /// enough for `StepNaming::from_blueprint` to resolve a real (non-
    /// fallback) `canonical_of_producer` entry.
    fn single_step_bp(
        bp_id: &str,
        agent_name: &str,
        profile: Option<AgentProfile>,
        meta: Option<AgentMeta>,
        default_agent_ctx: Option<serde_json::Value>,
    ) -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: bp_id.into(),
            flow: serde_json::from_value(json!({
                "kind": "step",
                "ref": agent_name,
                "in": {"op": "path", "at": "$.input"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents: vec![agent_def(agent_name, profile, meta)],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        }
    }

    async fn seed(store: &InMemoryBlueprintStore, bp: &Blueprint) {
        let bp_id = BlueprintId::new(bp.id.as_str());
        let v = blueprint_version(bp).expect("version");
        store
            .write_new(&bp_id, bp, &[], CommitMetadata::seed(bp_id.clone(), v, 0))
            .await
            .expect("write_new");
    }

    fn state_with(store: InMemoryBlueprintStore) -> BlueprintsState {
        BlueprintsState {
            store: Arc::new(store),
            ref_base: None,
            ref_includes: Vec::new(),
            cli_default_agent_kind: None,
            strict_embed: false,
            legacy_worker_binding_policy: LegacyWorkerBindingPolicy::Allow,
        }
    }

    #[tokio::test]
    async fn full_case_reports_binding_ctx_override_and_system_prompt() {
        let profile = AgentProfile {
            system_prompt: "Hello {{ name }}, mode={{ mode }}".to_string(),
            tools: vec!["Read".to_string(), "Grep".to_string()],
            worker_binding: Some("mse-worker-knowledge".to_string()),
            ..Default::default()
        };
        let meta = AgentMeta {
            ctx: Some(json!({ "work_dir": "/inline" })),
            ..Default::default()
        };
        let bp = single_step_bp(
            "explain-full-bp",
            "researcher",
            Some(profile),
            Some(meta),
            Some(json!({ "work_dir": "/bp-global", "extra": "kept" })),
        );
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let resp = explain_agent(
            State(state_with(store)),
            Path(("explain-full-bp".to_string(), "researcher".to_string())),
        )
        .await
        .expect("explain_agent")
        .0;

        assert_eq!(resp.blueprint.id, "explain-full-bp");
        assert!(!resp.blueprint.version.is_empty());
        assert_eq!(resp.agent.name, "researcher");
        assert_eq!(resp.agent.kind, AgentKind::RustFn);

        let binding = resp.worker_binding.expect("worker_binding present");
        assert_eq!(binding.variant, "mse-worker-knowledge");
        assert!(resp.binding_note.is_none());

        assert_eq!(
            resp.declared_tools.tools,
            vec!["Read".to_string(), "Grep".to_string()]
        );
        assert!(resp.declared_tools.informational);

        let sp = resp.system_prompt.expect("system_prompt present");
        assert_eq!(sp.bytes, "Hello {{ name }}, mode={{ mode }}".len());
        assert_eq!(sp.lines, 1);
        assert_eq!(
            sp.template_variables,
            vec!["mode".to_string(), "name".to_string()]
        );
        assert!(sp.template_syntax_error.is_none());

        assert_eq!(resp.effective_ctx.keys["work_dir"].value, json!("/inline"));
        assert_eq!(
            resp.effective_ctx.keys["work_dir"].winning_tier,
            "agent_inline"
        );
        assert_eq!(resp.effective_ctx.keys["extra"].value, json!("kept"));
        assert_eq!(resp.effective_ctx.keys["extra"].winning_tier, "bp_global");

        assert_eq!(resp.output.projection_name, "researcher");
        assert!(resp.output.naming_warnings.is_empty());
    }

    #[tokio::test]
    async fn agent_without_worker_binding_reports_binding_note() {
        let profile = AgentProfile {
            tools: vec!["Read".to_string()],
            ..Default::default()
        };
        let bp = single_step_bp("explain-no-binding-bp", "scout", Some(profile), None, None);
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let resp = explain_agent(
            State(state_with(store)),
            Path(("explain-no-binding-bp".to_string(), "scout".to_string())),
        )
        .await
        .expect("explain_agent")
        .0;

        assert!(resp.worker_binding.is_none());
        let note = resp.binding_note.expect("binding_note present");
        assert!(note.contains("no worker_binding declared"));
        assert!(resp.system_prompt.is_none());
    }

    #[tokio::test]
    async fn unknown_agent_name_returns_404_with_available_list() {
        let bp = single_step_bp("explain-404-agent-bp", "foo", None, None, None);
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let err = explain_agent(
            State(state_with(store)),
            Path((
                "explain-404-agent-bp".to_string(),
                "no-such-agent".to_string(),
            )),
        )
        .await
        .expect_err("expected 404");

        assert_eq!(err.0, StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_str(&err.1).expect("json body");
        assert_eq!(body["error"], "agent not found in blueprint");
        assert_eq!(body["agent"], "no-such-agent");
        assert_eq!(body["available"], json!(["foo"]));
    }

    #[tokio::test]
    async fn unknown_blueprint_id_returns_404_same_as_get_head() {
        let store = InMemoryBlueprintStore::new();

        let err = explain_agent(
            State(state_with(store)),
            Path(("no-such-bp".to_string(), "any-agent".to_string())),
        )
        .await
        .expect_err("expected 404");

        assert_eq!(err.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn template_syntax_error_is_reported_without_500() {
        let profile = AgentProfile {
            system_prompt: "hello {{ unclosed".to_string(),
            ..Default::default()
        };
        let bp = single_step_bp(
            "explain-syntax-error-bp",
            "scout",
            Some(profile),
            None,
            None,
        );
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let resp = explain_agent(
            State(state_with(store)),
            Path(("explain-syntax-error-bp".to_string(), "scout".to_string())),
        )
        .await
        .expect("explain_agent")
        .0;

        let sp = resp.system_prompt.expect("system_prompt present");
        assert!(sp.template_variables.is_empty());
        assert!(sp.template_syntax_error.is_some());
    }

    // ─── GH #46 M2: `runner` doctor checks (unknown ref error / backend↔kind mismatch warn) ───

    #[tokio::test]
    async fn runner_resolves_from_legacy_worker_binding_when_nothing_else_declared() {
        let profile = AgentProfile {
            worker_binding: Some("mse-worker-knowledge".to_string()),
            tools: vec!["Read".to_string()],
            ..Default::default()
        };
        let bp = single_step_bp(
            "explain-runner-legacy-bp",
            "scout",
            Some(profile),
            None,
            None,
        );
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let resp = explain_agent(
            State(state_with(store)),
            Path(("explain-runner-legacy-bp".to_string(), "scout".to_string())),
        )
        .await
        .expect("explain_agent")
        .0;

        assert_eq!(
            resp.runner.resolved,
            Some(mlua_swarm_schema::Runner::WsClaudeCode {
                variant: "mse-worker-knowledge".to_string(),
                tools: vec!["Read".to_string()],
            })
        );
        assert!(resp.runner.error.is_none());
        assert!(resp.runner.warning.is_none());
    }

    #[tokio::test]
    async fn runner_reports_unresolved_runner_ref_as_error_level_finding() {
        let mut bp = single_step_bp("explain-runner-unresolved-bp", "scout", None, None, None);
        bp.agents[0].runner_ref = Some("no-such-entry".to_string());
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let resp = explain_agent(
            State(state_with(store)),
            Path((
                "explain-runner-unresolved-bp".to_string(),
                "scout".to_string(),
            )),
        )
        .await
        .expect("explain_agent")
        .0;

        assert!(resp.runner.resolved.is_none());
        let error = resp.runner.error.expect("error-level finding present");
        assert!(
            error.contains("no-such-entry"),
            "error must name the unresolved runner_ref: {error}"
        );
        assert!(resp.runner.warning.is_none());
    }

    #[tokio::test]
    async fn runner_reports_backend_kind_mismatch_as_warn_level_finding() {
        // `AgentDef.kind = RustFn` (via `single_step_bp`'s `agent_def` helper)
        // paired with an `agent_block_in_process` Runner is the documented
        // mismatch (Design §6: "backend ↔ kind mismatch").
        let mut bp = single_step_bp("explain-runner-mismatch-bp", "scout", None, None, None);
        bp.runners = vec![mlua_swarm_schema::RunnerDef {
            name: "in-process".to_string(),
            runner: mlua_swarm_schema::Runner::AgentBlockInProcess {
                tools: vec!["Bash".to_string()],
            },
        }];
        bp.agents[0].runner_ref = Some("in-process".to_string());
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let resp = explain_agent(
            State(state_with(store)),
            Path((
                "explain-runner-mismatch-bp".to_string(),
                "scout".to_string(),
            )),
        )
        .await
        .expect("explain_agent")
        .0;

        assert!(resp.runner.resolved.is_some());
        assert!(resp.runner.error.is_none());
        let warning = resp.runner.warning.expect("warn-level finding present");
        assert!(
            warning.contains("AgentBlockInProcess") && warning.contains("RustFn"),
            "warning must name both the resolved backend and the mismatched kind: {warning}"
        );
    }

    // ─── GH #47: batch summary sweep (explain_agents_batch) ────────────

    /// A 3-agent Blueprint whose flow only dispatches `bound_agent` — the
    /// other two are unreferenced by the flow, so `StepNaming` misses them
    /// (fail-soft fallback to the agent name is exercised for both).
    fn batch_bp() -> Blueprint {
        let bound_profile = AgentProfile {
            system_prompt: "hello world".to_string(),
            tools: vec!["Read".to_string(), "Grep".to_string()],
            worker_binding: Some("mse-worker-knowledge".to_string()),
            ..Default::default()
        };
        let bound_meta = AgentMeta {
            ctx: Some(json!({ "work_dir": "/inline" })),
            ..Default::default()
        };
        Blueprint {
            schema_version: current_schema_version(),
            id: "explain-batch-bp".into(),
            flow: serde_json::from_value(json!({
                "kind": "step",
                "ref": "bound_agent",
                "in": {"op": "path", "at": "$.input"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents: vec![
                agent_def("bound_agent", Some(bound_profile), Some(bound_meta)),
                agent_def("unbound_agent", None, None),
                agent_def("orphan_agent", None, None),
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
            default_agent_ctx: Some(json!({ "work_dir": "/bp-global", "extra": "kept" })),
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        }
    }

    #[tokio::test]
    async fn explain_agents_batch_reports_a_summary_row_per_agent() {
        let bp = batch_bp();
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let resp = explain_agents_batch(
            State(state_with(store)),
            Path("explain-batch-bp".to_string()),
        )
        .await
        .expect("explain_agents_batch")
        .0;

        assert_eq!(resp.blueprint.id, "explain-batch-bp");
        assert!(!resp.blueprint.version.is_empty());
        assert_eq!(resp.agents.len(), 3);

        let bound = resp
            .agents
            .iter()
            .find(|a| a.name == "bound_agent")
            .expect("bound_agent row");
        assert_eq!(bound.kind, format!("{:?}", AgentKind::RustFn));
        let binding = bound
            .worker_binding
            .as_ref()
            .expect("worker_binding present");
        assert_eq!(binding.variant, "mse-worker-knowledge");
        assert_eq!(bound.declared_tools_count, 2);
        assert_eq!(bound.system_prompt_bytes, "hello world".len());
        // work_dir (agent_inline override) + extra (bp-global carry) = 2 keys.
        assert_eq!(bound.effective_ctx_key_count, 2);
        // Referenced by the flow -> a real (non-fallback) canonical name.
        assert_eq!(bound.projection_name, "bound_agent");

        let unbound = resp
            .agents
            .iter()
            .find(|a| a.name == "unbound_agent")
            .expect("unbound_agent row");
        assert!(unbound.worker_binding.is_none());
        assert_eq!(unbound.declared_tools_count, 0);
        assert_eq!(unbound.system_prompt_bytes, 0);
        // Only the bp-global tier applies (no agent-level meta) = 2 keys.
        assert_eq!(unbound.effective_ctx_key_count, 2);
        // Not referenced by the flow -> StepNaming miss -> fallback to name.
        assert_eq!(unbound.projection_name, "unbound_agent");

        let orphan = resp
            .agents
            .iter()
            .find(|a| a.name == "orphan_agent")
            .expect("orphan_agent row");
        assert_eq!(orphan.projection_name, "orphan_agent");
    }

    #[tokio::test]
    async fn explain_agents_batch_zero_agents_returns_empty_list_not_404() {
        let bp = Blueprint {
            schema_version: current_schema_version(),
            id: "explain-batch-empty-bp".into(),
            flow: serde_json::from_value(json!({
                "kind": "step",
                "ref": "unused",
                "in": {"op": "path", "at": "$.input"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents: vec![],
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
            runners: vec![],
            default_runner: None,
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        };
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let resp = explain_agents_batch(
            State(state_with(store)),
            Path("explain-batch-empty-bp".to_string()),
        )
        .await
        .expect("explain_agents_batch")
        .0;

        assert!(resp.agents.is_empty());
    }

    #[tokio::test]
    async fn explain_agents_batch_unknown_blueprint_id_returns_404_same_as_get_head() {
        let store = InMemoryBlueprintStore::new();

        let err = explain_agents_batch(State(state_with(store)), Path("no-such-bp".to_string()))
            .await
            .expect_err("expected 404");

        assert_eq!(err.0, StatusCode::NOT_FOUND);
    }

    // ─── C3: GET /v1/blueprints/:id/binding-requirements ───────────────

    fn state_with_policy(
        store: InMemoryBlueprintStore,
        legacy_worker_binding_policy: LegacyWorkerBindingPolicy,
    ) -> BlueprintsState {
        BlueprintsState {
            store: Arc::new(store),
            ref_base: None,
            ref_includes: Vec::new(),
            cli_default_agent_kind: None,
            strict_embed: false,
            legacy_worker_binding_policy,
        }
    }

    /// A Runner-backed agent: a legacy `profile.worker_binding` resolves to a
    /// `WsClaudeCode` Runner (see `runner_resolves_from_legacy_worker_binding`),
    /// so `binding_requests` reconstructs a request carrying its
    /// variant / tools / model.
    fn runner_agent(name: &str, variant: &str, tools: &[&str], model: &str) -> AgentDef {
        let profile = AgentProfile {
            worker_binding: Some(variant.to_string()),
            tools: tools.iter().map(|t| t.to_string()).collect(),
            model: Some(model.to_string()),
            ..Default::default()
        };
        agent_def(name, Some(profile), None)
    }

    fn bp_with_agents(bp_id: &str, agents: Vec<AgentDef>, strict_binding: bool) -> Blueprint {
        let first = agents
            .first()
            .map(|a| a.name.clone())
            .unwrap_or_else(|| "unused".to_string());
        Blueprint {
            schema_version: current_schema_version(),
            id: bp_id.into(),
            flow: serde_json::from_value(json!({
                "kind": "step",
                "ref": first,
                "in": {"op": "path", "at": "$.input"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents,
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy {
                strict_binding,
                ..Default::default()
            },
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
            runners: vec![],
            default_runner: None,
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        }
    }

    #[tokio::test]
    async fn binding_requirements_lists_one_request_per_runner_backed_agent() {
        let bp = bp_with_agents(
            "binding-reqs-two-runners-bp",
            vec![
                runner_agent(
                    "worker",
                    "mse-worker-knowledge",
                    &["Read", "Grep"],
                    "sonnet",
                ),
                runner_agent("reader", "mse-worker-reader", &["Read"], "haiku"),
            ],
            true,
        );
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let resp = binding_requirements(
            State(state_with(store)),
            Path("binding-reqs-two-runners-bp".to_string()),
        )
        .await
        .expect("binding_requirements")
        .0;

        assert_eq!(resp.blueprint_id, "binding-reqs-two-runners-bp");
        // `strict_binding` is echoed verbatim from the Blueprint strategy.
        assert!(resp.strict_binding);
        assert_eq!(resp.requirements.len(), 2);

        let worker = resp
            .requirements
            .iter()
            .find(|r| r.agent == "worker")
            .expect("worker requirement");
        assert_eq!(
            worker.backend,
            mlua_swarm_schema::BindingBackend::WsClaudeCode
        );
        assert_eq!(
            worker.launch_variant.as_deref(),
            Some("mse-worker-knowledge")
        );
        assert_eq!(worker.requested_tools, vec!["Grep", "Read"]);
        assert_eq!(worker.requested_model.as_deref(), Some("sonnet"));

        let reader = resp
            .requirements
            .iter()
            .find(|r| r.agent == "reader")
            .expect("reader requirement");
        assert_eq!(
            reader.backend,
            mlua_swarm_schema::BindingBackend::WsClaudeCode
        );
        assert_eq!(reader.launch_variant.as_deref(), Some("mse-worker-reader"));
        assert_eq!(reader.requested_tools, vec!["Read"]);
        assert_eq!(reader.requested_model.as_deref(), Some("haiku"));
    }

    #[tokio::test]
    async fn binding_requirements_empty_when_no_runner_backed_agents() {
        // A profile without `worker_binding` (and no runner / runner_ref)
        // resolves to no Runner, so `binding_requests` yields nothing.
        let profile = AgentProfile {
            tools: vec!["Read".to_string()],
            ..Default::default()
        };
        let bp = single_step_bp(
            "binding-reqs-no-runners-bp",
            "scout",
            Some(profile),
            None,
            None,
        );
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let resp = binding_requirements(
            State(state_with(store)),
            Path("binding-reqs-no-runners-bp".to_string()),
        )
        .await
        .expect("binding_requirements")
        .0;

        assert!(!resp.strict_binding);
        assert!(resp.requirements.is_empty());
    }

    #[tokio::test]
    async fn binding_requirements_unknown_blueprint_id_returns_404() {
        let store = InMemoryBlueprintStore::new();

        let err = binding_requirements(State(state_with(store)), Path("no-such-bp".to_string()))
            .await
            .expect_err("expected 404");

        assert_eq!(err.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn binding_requirements_422_when_legacy_binding_rejected_by_policy() {
        // A legacy `profile.worker_binding` is the only Runner source; under
        // `Reject` policy the strict resolver refuses it, so the handler maps
        // the resolve error to 422 (not 500).
        let bp = bp_with_agents(
            "binding-reqs-legacy-reject-bp",
            vec![runner_agent(
                "worker",
                "mse-worker-knowledge",
                &["Read"],
                "sonnet",
            )],
            false,
        );
        let store = InMemoryBlueprintStore::new();
        seed(&store, &bp).await;

        let err = binding_requirements(
            State(state_with_policy(store, LegacyWorkerBindingPolicy::Reject)),
            Path("binding-reqs-legacy-reject-bp".to_string()),
        )
        .await
        .expect_err("expected 422");

        assert_eq!(err.0, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            err.1.contains("resolve bound agents"),
            "422 body must carry the resolve error: {}",
            err.1
        );
    }
}

// ──────────────────────────────────────────────────────────────────────
// Phase 6 (issue 4c4e3eb8) — `seed_blueprint`: strict_embed pre-check
// + include-cascade fix hint on ref-expand failure. Design table row 3.
// ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod seed_strict_embed_tests {
    use super::*;
    use mlua_swarm::blueprint::store::InMemoryBlueprintStore;
    use serde_json::json;
    use std::fs;
    use tempfile::TempDir;

    /// The minimal `agent.md` the `$agent_md` refs in these tests
    /// resolve to. Same shape the `linker.rs` unit tests use.
    const AGENT_MD: &str = "---\n\
name: writer\n\
description: writes\n\
model: sonnet\n\
---\n\
You write.\n";

    fn write_md(dir: &std::path::Path, rel: &str, content: &str) -> PathBuf {
        let p = dir.join(rel);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&p, content).unwrap();
        p
    }

    /// A minimal valid Blueprint JSON body suitable for
    /// `seed_blueprint` — `agents` list carries a single already-
    /// resolved `AgentDef` object. Tests that need to exercise refs
    /// substitute an entry manually.
    fn minimal_bp_body(id: &str) -> serde_json::Value {
        json!({
            "schema_version": mlua_swarm::blueprint::current_schema_version(),
            "id": id,
            "flow": { "kind": "step", "ref": "writer",
                      "in": {"op": "path", "at": "$.input"},
                      "out": {"op": "path", "at": "$.out"} },
            "agents": [
                { "name": "writer", "kind": "rust_fn", "spec": { "fn_id": "writer" } }
            ],
            "operators": [],
            "metas": [],
            "hints": {},
            "strategy": {},
            "metadata": {},
            "spawner_hints": {},
            "default_agent_kind": "operator",
            "default_agent_ctx": null,
            "audits": [],
            "runners": [],
            "blueprint_ref_includes": []
        })
    }

    fn state_for_test(
        store: InMemoryBlueprintStore,
        ref_base: Option<PathBuf>,
        strict_embed: bool,
    ) -> BlueprintsState {
        BlueprintsState {
            store: Arc::new(store),
            ref_base,
            ref_includes: Vec::new(),
            cli_default_agent_kind: None,
            strict_embed,
            legacy_worker_binding_policy: LegacyWorkerBindingPolicy::Allow,
        }
    }

    // (a) Default (strict_embed=false) + resolvable ref → 201 pass.
    #[tokio::test]
    async fn strict_embed_off_resolves_agent_md_ref_and_seeds() {
        let dir = TempDir::new().unwrap();
        write_md(dir.path(), "agents/writer.md", AGENT_MD);
        let mut body = minimal_bp_body("strict-off-resolvable-bp");
        body["agents"] = json!([ { "$agent_md": "agents/writer.md", "kind": "rust_fn" } ]);

        let store = InMemoryBlueprintStore::new();
        let state = state_for_test(store, Some(dir.path().to_path_buf()), false);

        let (status, resp) = seed_blueprint(
            State(state),
            Path("strict-off-resolvable-bp".to_string()),
            Json(body),
        )
        .await
        .expect("seed ok");
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(resp.0["seeded"], json!(true));
    }

    // (b) Default (strict_embed=false) + unresolvable ref → 400 with
    //     fix hint (must name include-cascade knobs).
    #[tokio::test]
    async fn strict_embed_off_unresolvable_ref_returns_400_with_include_cascade_hint() {
        let dir = TempDir::new().unwrap();
        // Do NOT write the file — force cascade miss.
        let mut body = minimal_bp_body("strict-off-unresolvable-bp");
        body["agents"] = json!([ { "$agent_md": "agents/missing.md", "kind": "rust_fn" } ]);

        let store = InMemoryBlueprintStore::new();
        let state = state_for_test(store, Some(dir.path().to_path_buf()), false);

        let err = seed_blueprint(
            State(state),
            Path("strict-off-unresolvable-bp".to_string()),
            Json(body),
        )
        .await
        .expect_err("expected 400");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        let msg = err.1;
        // Underlying linker error names the searched dirs.
        assert!(
            msg.contains("cascade") && msg.contains(dir.path().to_str().unwrap()),
            "linker cascade error must name searched dirs: {msg}"
        );
        // Wrapper adds the include-cascade fix hint pointing at the
        // configurable knobs (server CLI / env / config / in-bp).
        assert!(msg.contains("--include"), "hint names CLI flag: {msg}");
        assert!(
            msg.contains("MSE_BLUEPRINT_INCLUDES"),
            "hint names env var: {msg}"
        );
        assert!(
            msg.contains("blueprint_ref_includes"),
            "hint names config-file / in-bp key: {msg}"
        );
        assert!(
            msg.contains("mse bp build --strict-embed"),
            "hint suggests client-side pre-embed as escape hatch: {msg}"
        );
    }

    // (c) strict_embed=true + raw ref present → 400 with pre-embed hint.
    //     Runs even with no ref_base configured (pre-check is
    //     unconditional on strict_embed).
    #[tokio::test]
    async fn strict_embed_on_refuses_body_with_agent_md_ref() {
        let mut body = minimal_bp_body("strict-on-refs-present-bp");
        body["agents"] = json!([ { "$agent_md": "agents/anything.md", "kind": "rust_fn" } ]);

        let store = InMemoryBlueprintStore::new();
        // ref_base=None on purpose: strict-embed rejects raw refs
        // whether or not the server could resolve them.
        let state = state_for_test(store, None, true);

        let err = seed_blueprint(
            State(state),
            Path("strict-on-refs-present-bp".to_string()),
            Json(body),
        )
        .await
        .expect_err("expected 400");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        let msg = err.1;
        assert!(
            msg.starts_with("strict_embed:"),
            "verdict tag must namespace the error: {msg}"
        );
        assert!(
            msg.contains("$agent_md=agents/anything.md"),
            "message must name every unembedded ref: {msg}"
        );
        assert!(
            msg.contains("mse bp build --strict-embed"),
            "message must point at client-side pre-embed: {msg}"
        );
    }

    // (c-2) strict_embed=true + `$file` ref present → same reject
    //       (walker covers both ref kinds).
    #[tokio::test]
    async fn strict_embed_on_refuses_body_with_file_ref_deep_in_object() {
        let mut body = minimal_bp_body("strict-on-file-ref-bp");
        // Nest the `$file` ref inside a Step directive so we exercise
        // the recursive walker (not just the top-level path).
        body["flow"] = json!({
            "kind": "step",
            "ref": "writer",
            "in": {"op": "lit", "value": { "$file": "prompts/deep.md" } },
            "out": {"op": "path", "at": "$.out"}
        });

        let store = InMemoryBlueprintStore::new();
        let state = state_for_test(store, None, true);

        let err = seed_blueprint(
            State(state),
            Path("strict-on-file-ref-bp".to_string()),
            Json(body),
        )
        .await
        .expect_err("expected 400");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(
            err.1.contains("$file=prompts/deep.md"),
            "walker must find nested `$file` refs: {}",
            err.1
        );
    }

    // (d) strict_embed=true + fully-embedded body (no refs) → 201 pass.
    #[tokio::test]
    async fn strict_embed_on_accepts_fully_embedded_body() {
        let body = minimal_bp_body("strict-on-embedded-bp");

        let store = InMemoryBlueprintStore::new();
        let state = state_for_test(store, None, true);

        let (status, resp) = seed_blueprint(
            State(state),
            Path("strict-on-embedded-bp".to_string()),
            Json(body),
        )
        .await
        .expect("embedded body seeds ok");
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(resp.0["seeded"], json!(true));
    }

    // Direct helper unit test — walker must return `None` on an
    // already-embedded value and `Some(refs)` on a body with refs.
    #[test]
    fn walker_finds_refs_in_arrays_and_nested_objects() {
        let embedded = json!({ "id": "x", "agents": [ { "name": "a", "kind": "rust_fn" } ] });
        assert!(collect_unembedded_refs(&embedded).is_none());

        let with_refs = json!({
            "id": "x",
            "agents": [ { "$agent_md": "a.md" } ],
            "flow": { "in": { "value": { "$file": "p.md" } } }
        });
        let refs = collect_unembedded_refs(&with_refs).expect("some refs");
        assert!(refs.iter().any(|s| s == "$agent_md=a.md"));
        assert!(refs.iter().any(|s| s == "$file=p.md"));
    }
}
