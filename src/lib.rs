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

#![warn(missing_docs)]

pub mod application;
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
pub use core::state::{
    CapTokenConsumeError, CapTokenRecord, DispatchOutcome, Event, EventStream, OperatorSession,
    ResumeKey, ResumePending, TaskSpec, TaskState, TaskStatus,
};
pub use lua::bridge::{parse_lua_blueprint, parse_lua_blueprint_with_ctx};
pub use middleware::lua_layer::LuaMiddleware;
pub use middleware::project_name_alias::{ProjectNameAliasMiddleware, PROJECT_NAME_ALIAS_KEY};
pub use middleware::resolver::{AgentResolver, FnResolver, ResolverMiddleware};
pub use middleware::{
    AuditMiddleware, LayerFactory, LayerRegistry, LongHoldMiddleware, MainAIMiddleware,
    OperatorDelegateMiddleware, SeniorEscalationMiddleware, SpawnerLayer, SpawnerStack,
};
pub use operator::{Operator, OperatorSpawner};
pub use service::{TaskLaunchError, TaskLaunchInput, TaskLaunchOutput, TaskLaunchService};
pub use store::output::{
    InMemoryOutputStore, OutputRecord, OutputRef, OutputStore, OutputStoreError,
};
pub use types::{
    default_role_verb_table, CapToken, CapTokenDecodeError, Role, RoleVerbGate, SessionId, TaskId,
    Verb, WorkerId, WorkerPayload,
};
pub use worker::adapter::{
    InProcSpawner, SpawnError, SpawnerAdapter, WorkerError, WorkerFn, WorkerInvocation,
    WorkerResult,
};
pub use worker::agent_block::AgentBlockInProcessSpawnerFactory;
pub use worker::output::{ContentRef, OutputEvent, OutputSink};
pub use worker::process_spawner::{ProcessSpawner, StreamMode};
pub use worker::{MiddlewareWorker, Worker, WorkerJoinHandler};
