//! Swarm engine host for mlua — a long-running stateful runtime that
//! compiles flow.ir Blueprints and dispatches their agent steps to workers.
//!
//! # Architecture
//!
//! `mlua-swarm` is the host layer of a four-layer stack:
//!
//! ```text
//! flow.ir programs (Lua / JSON Blueprints authored by users or AIs)
//!         │  parsed / compiled
//! flow-ir-core        IR node & expr types + pure evaluation
//!         │  bridged into Lua
//! mlua-flow-ir        Lua <-> IR bridge (mlua-based)
//!         │  hosted by
//! mlua-swarm (this)   engine, workers, operators, middleware, stores
//! ```
//!
//! A [`Blueprint`] declares a `flow` (step / seq / branch / loop / fanout /
//! try / assign nodes) plus the `agents` it references. The [`Compiler`]
//! resolves each agent to a [`SpawnerFactory`] (Lua in-process, Rust fn,
//! subprocess, or WS operator), and the [`Engine`] drives the flow while
//! recording task state as an [`Event`] stream.
//!
//! # Domain vs. Data separation
//!
//! Control-flow values (verdicts, counters, small routing fields) travel in
//! the shared [`Ctx`]; large worker responses are offloaded to an
//! [`OutputStore`] and referenced from `Ctx` by [`OutputRef`]. This keeps
//! flow evaluation cheap and bounded regardless of payload size.
//!
//! # Middleware
//!
//! Worker dispatch passes through a [`SpawnerStack`] assembled from a
//! [`LayerRegistry`]: a base layer set plus per-blueprint hints
//! ([`CompilerHints`]) select layers such as audit, long-hold, main-AI
//! bridging, senior escalation, and operator delegation.
//!
//! # Module map
//!
//! - [`types`] / [`errors`] — [`Role`](types::Role) × [`Verb`](types::Verb)
//!   capability gate, [`CapToken`](types::CapToken) (HMAC-SHA256), ID
//!   newtypes, and the [`EngineError`](errors::EngineError) surface.
//! - [`core`] — engine config, task state machine, [`Ctx`], and the
//!   [`Engine`] itself.
//! - [`blueprint`] — schema shim, compiler, loader (`$agent_md` file-ref
//!   expansion), and versioned stores (in-memory / git2-backed).
//! - [`worker`] / [`operator`] — spawner adapters, process spawning, output
//!   events, and WS operator sessions.
//! - [`middleware`] — the layer registry and the individual layers.
//! - [`lua`] / [`agent_block`] — Lua blueprint bridge, `agents/*.md` loader,
//!   and the agent-block SDK spawner integration.
//! - [`service`] / [`application`] / [`enhance`] — task-launch orchestration,
//!   application façades, and the self-enhancement (patch / verify / commit)
//!   flow.
//! - [`store`] — persistence traits and default backends for outputs,
//!   issues, and enhance settings/logs.
//!
//! # Worker I/O contract (why IN is a fetch and OUT is a file)
//!
//! Every worker step follows one asymmetric I/O shape, and the asymmetry
//! is deliberate — each side sits where an LLM worker is *reliable*:
//!
//! - **IN — one authenticated HTTP fetch.** The worker pulls its prompt
//!   and context with `GET /v1/worker/prompt` (Bearer = capability
//!   token). The server assembles the view fresh per attempt (system
//!   prompt, directive, `AgentContextView`, prior-step pointers), so a
//!   fetch always returns the current attempt's truth — no stale files
//!   to pre-write or clean up, and the payload never has to travel
//!   through the orchestrating operator's own context window (the Spawn
//!   directive relays only a short handle). The fetch doubles as the
//!   trust handshake: the capability token scopes *which* task's IN this
//!   worker may read.
//! - **OUT — one tool call, never a self-chosen file.** Producing OUT
//!   happens at the *end* of a worker's run — the point where a
//!   long-context LLM is least dependable about paths and formats.
//!   Letting it pick a file name there structurally invites hallucinated
//!   paths and plausible-looking-but-wrong files. So the exit is pinned
//!   to calls that carry no path and no format choice: `POST
//!   /v1/worker/submit` for the final body, `POST
//!   /v1/worker/artifact?name=<name>` per named part.
//! - **Files are the server's job.** Turning submitted OUT into the IN
//!   files the *next* step reads (plain `Read` on a path — the cheapest,
//!   most reliable worker primitive, with partial reads for free) is
//!   owned by the submit-time projection sink and
//!   [`FileProjectionAdapter`](core::projection::FileProjectionAdapter):
//!   the final body lands as `<ctx-dir>/<step>.md`, each staged part
//!   lands raw as `<ctx-dir>/<name>`. Placement, naming, and format are
//!   adapter policy — deliberately *not* baked into worker defaults, so
//!   workers stay generic and the policy stays swappable
//!   ([`ProjectionPlacement`]).
//!
//! See `mse://guides/worker-io-contract` (an `mse mcp` resource) for the
//! consumer-side view of the same contract.

#![warn(missing_docs)]

pub mod application;
pub mod binding;
pub mod blueprint;
pub mod core;
pub mod enhance;
pub mod lua;
pub mod middleware;
pub mod operator;
pub mod service;
pub mod store;
pub mod types;
pub mod worker;

// Symbol re-exports (preserve external API surface).
pub use application::{
    Application, BlueprintRef, EnhanceApplication, EnhanceApplicationConfig,
    EnhanceApplicationError, EnhanceApplicationInput, TaskApplication, TaskApplicationError,
    TaskApplicationInput, TaskApplicationOutput, TickOutcome, VersionSelector,
};
pub use binding::{
    attest_bound_agents, binding_requests, AgentBindingProvider, BindingProviderError,
};
pub use blueprint::compiler::{
    CompileError, CompiledAgentTable, CompiledBlueprint, Compiler, HostBridge,
    LuaInProcessSpawnerFactory, LuaScriptSource, OperatorSpawnerFactory,
    RustFnInProcessSpawnerFactory, SpawnerFactory, SpawnerFactoryKind, SpawnerRegistry,
    SubprocessProcessSpawnerFactory,
};
pub use blueprint::loader::{expand_file_refs, load_blueprint_from_path, LoadError};
pub use blueprint::{
    current_schema_version, AgentDef, AgentKind, AgentMeta, Blueprint, BlueprintMetadata,
    BlueprintOrigin, CompilerHints, CompilerStrategy, EngineDispatcher, SpawnerHints,
    CURRENT_SCHEMA_VERSION,
};
pub use core::config::{EngineCfg, LongHoldConfig};
pub use core::ctx::{
    collapse_operator_kind, Ctx, CtxMeta, OperatorInfo, OperatorKind, SeniorBridge, SpawnHook,
};
pub use core::engine::Engine;
pub use core::errors::EngineError;
pub use core::projection_placement::{
    ProjectionPlacement, ProjectionPlacementError, RootPreference,
};
pub use core::state::{
    CapTokenConsumeError, CapTokenRecord, DispatchOutcome, Event, EventStream, OperatorSession,
    ResumeKey, ResumePending, TaskSpec, TaskState, TaskStatus,
};
pub use core::step_naming::{StepNameEntry, StepNaming, StepNamingError, StepNamingWarning};
pub use lua::bridge::{parse_lua_blueprint, parse_lua_blueprint_with_ctx};
pub use middleware::lua_layer::LuaMiddleware;
pub use middleware::project_name_alias::{ProjectNameAliasMiddleware, PROJECT_NAME_ALIAS_KEY};
pub use middleware::resolver::{AgentResolver, FnResolver, ResolverMiddleware};
pub use middleware::{
    AuditMiddleware, LayerFactory, LayerRegistry, LongHoldMiddleware, MainAIMiddleware,
    OperatorDelegateMiddleware, SeniorEscalationMiddleware, SpawnerLayer, SpawnerStack,
};
pub use operator::{Operator, OperatorSpawner, WorkerBinding};
pub use service::{
    TaskInputSpec, TaskLaunchError, TaskLaunchInput, TaskLaunchOutput, TaskLaunchService,
};
pub use store::output::{
    InMemoryOutputStore, OutputRecord, OutputRef, OutputStore, OutputStoreError,
};
pub use types::{
    default_role_verb_table, CapToken, CapTokenDecodeError, Role, RoleVerbGate, RunId, SessionId,
    StepId, TaskId, Verb, WorkerId, WorkerPayload,
};
pub use worker::adapter::{
    InProcSpawner, SpawnError, SpawnerAdapter, WorkerError, WorkerFn, WorkerInvocation,
    WorkerResult,
};
pub use worker::agent_block::AgentBlockInProcessSpawnerFactory;
pub use worker::output::{ContentRef, OutputEvent, OutputSink};
pub use worker::process_spawner::{ProcessSpawner, StreamMode};
pub use worker::{MiddlewareWorker, Worker, WorkerJoinHandler};
