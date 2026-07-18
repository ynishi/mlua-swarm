# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.12.3](https://github.com/ynishi/mlua-swarm/compare/v0.12.2...v0.12.3) - 2026-07-18

### Added

- *(dsl)* add opt-in `chain = true` to B.pipeline sugar (GH #65)

### Fixed

- *(cli)* restore #[test] on pipeline_template_documents_init_ctx_seeding

### Other

- Merge branch 'fix/gh-67-swarm-status-stale'
- Merge branch 'fix/gh-66-scaffold-operator-kind'
- Merge branch 'fix/gh-65-pipeline-chain-option'

## [0.12.2](https://github.com/ynishi/mlua-swarm/compare/v0.12.1...v0.12.2) - 2026-07-18

### Added

- *(cli)* add fix_hint payload to compile-lint failures (GH #62 Axis B.1)
- *(cli)* add mse bp new scaffolding CLI + bp_new MCP tool (GH #62 Axis A)
- *(cli)* front-load worker_binding into bp_build compile-lint + bp_doctor family (GH #61)
- *(cli)* add bp_build MCP tool for direct .bp.lua registration

### Fixed

- *(dsl)* default `halted_at` in B.pipeline so verdict-less pipelines compile

## [0.12.1](https://github.com/ynishi/mlua-swarm/compare/v0.12.0...v0.12.1) - 2026-07-17

### Added

- *(bp_doctor)* add tool_lint + output_contract_lint families (GH #45)

### Other

- *(serve)* lock in operator-session non-persistence across restart
- strip internal subtask/ST narrative from public code doc
- *(mcp-resources)* surface GET /v1/worker/prompt schema (WorkerPayload)

## [0.12.0](https://github.com/ynishi/mlua-swarm/compare/v0.11.0...v0.12.0) - 2026-07-17

### Added

- *(serve,tests)* boot resumable-log classification + in-process E2E for restart-crossing resume
- *(replay)* wire server / CLI / resume endpoint over Ctx-snapshot replay Core
- *(store)* add Ctx-snapshot replay Core primitive

### Fixed

- *(store)* auto-migrate legacy SqliteReplayStore schema on open

### Other

- *(dispatch)* route EngineDispatcher through replay-aware dispatch_attempt_with_run_ctx
- *(mcp-resources)* bundle the replay & resume wire narrative

### Added

- *(store)* add Ctx-snapshot replay Core primitive (`store::replay` module with `ReplayStore` trait, `InMemoryReplayStore`, `SqliteReplayStore`, and `ReplayCursor`) plus `Engine::dispatch_attempt_with_run_ctx` for deterministic step-level replay of a `Ctx` across a fresh engine
- *(store)* extend `RunContext` with opt-in `replay_store` / `replay_cursor` fields

### Changed

- *(store)* `SqliteReplayStore::open` now auto-migrates the `replay_log` schema on open, tracked by `PRAGMA user_version`. Existing `~/.mse/store/replay.sqlite` files created by mse `< v0.11.0` have their legacy `replay_log` table dropped and recreated in the current shape (two-column split with `ctx_snapshot_json` + `step_output_json`). Legacy entries carry no Ctx snapshot and cannot be replayed against the current wire, so the loss is safe. Files written by a newer schema version are rejected instead of being silently mangled

## [0.11.0](https://github.com/ynishi/mlua-swarm/compare/v0.10.0...v0.11.0) - 2026-07-16

### Added

- *(compiler)* add VerdictValueUnhandled reverse-direction lint (GH #50 follow-up)
- materialize staged named parts to ctx files at submit time
- reject strict launches with no resolvable root before dispatch
- add check_policy 3-tier cascade for pre-dispatch validation
- *(engine)* add CheckPolicy for submit-time projection sink fail-loud opt-in
- *(server)* wire CheckPolicy through config file + CLI flag + doctor
- *(dsl)* authoring enablers + generalized equivalence fixtures (GH #54)

### Other

- document the worker I/O contract (fetch IN, path-free tool-call OUT)
- fix pre-existing rustfmt drift in cli resources and dsl_node_parity test
- *(cli)* bundle DSL authoring guide + .bp.lua samples as MCP resources

## [0.10.0](https://github.com/ynishi/mlua-swarm/compare/v0.9.2...v0.10.0) - 2026-07-14

### Added

- *(cli)* flow/bp Lua authoring DSL + `mse bp build` subcommand
- verdict contract enforcement completeness — completion-time check on all submit routes (GH #51)
- verdict contract — per-agent verdict declaration enforced at compile and submit (GH #50)
- WorkerModel schema — Runner enum, runners registry, resolve cascade (GH #46 Milestone 2)

### Other

- Merge branch 'origin/main' into main
- Merge branch 'topic/verdict-enforcement-completeness' into main
- *(bp)* document canonical verdict-return patterns for BP flow

## [0.9.2](https://github.com/ynishi/mlua-swarm/compare/v0.9.1...v0.9.2) - 2026-07-12

### Added

- bp_explain_agent — dry-run explain of BP agent materialization
- bp_explain_agents batch + tool_drift wrapper_only 2-tier split
- *(server)* @file:<abs-path> submit sentinel + per-step opt-in (GH #42 + GH #43)

### Fixed

- bp_explain_agent contract allow-list uses full MCP tool identifiers

## [0.9.1](https://github.com/ynishi/mlua-swarm/compare/v0.9.0...v0.9.1) - 2026-07-11

### Other

- bump mlua-flow-ir 0.1.2 -> 0.2.0

## [0.9.0](https://github.com/ynishi/mlua-swarm/compare/v0.8.0...v0.9.0) - 2026-07-10

### Added

- worker degradation reporting + sync-launch timeout bump (GH #32, GH #39)
- named multi-part worker output on the Blueprint chain ([#36](https://github.com/ynishi/mlua-swarm/pull/36))
- RunStatus::Interrupted + boot-time recovery sweep (issue #35 ST2)
- system_ref resolution in mse_worker_fetch + bp_doctor delivery note (GH #31)
- *(server)* add /v1/worker/prompt/system and /v1/agents/:name/render-size routes
- *(core,server)* Subtask 1 — SystemRef wire shape + Engine threshold branch (GH #31)
- *(schema,core,server,cli)* Blueprint-declared after-run audit hooks (GH #34)
- detach the flow-eval driver from the sync launch request ([#37](https://github.com/ynishi/mlua-swarm/pull/37))
- add lifecycle occupancy guard to restart/shutdown MCP tools
- persist-by-default Task/Run stores + --ephemeral opt-out (#35 ST1)

### Fixed

- apply GH #33 sync-hang guards to task_rekick (issue #35 ST3)
- *(server,cli)* fail loud on sync task launch instead of hanging (GH #33)

### Other

- expand rustdoc/guide for audit middleware and Worker axis
- document GH #35 restart-resilience feature set

## [0.8.0](https://github.com/ynishi/mlua-swarm/compare/v0.7.0...v0.8.0) - 2026-07-09

### Added

- *(schema,core,server,cli)* single projection-placement resolver with configurable materialize location (GH #27)
- *(schema,core,server,cli)* unify step-output naming via Blueprint-declared projection names (GH #23)
- *(cli)* add bp_doctor MCP tool + agent-md authoring guide (GH #28)

## [0.7.0](https://github.com/ynishi/mlua-swarm/compare/v0.6.0...v0.7.0) - 2026-07-09

### Added

- *(schema,core,server,cli)* ContextPolicy-driven step OUTPUT projection supply
- *(mcp)* expose MCP tool inputSchemas as `mse://api/mcp-tools`

### Fixed

- *(mcp)* pin schemars type on init_ctx / mse_ack.value (GH #24)

## [0.6.0](https://github.com/ynishi/mlua-swarm/compare/v0.5.0...v0.6.0) - 2026-07-08

### Added

- *(schema,core)* BP-level declarative context supply tiers (BP-global / Agent / Step) via AgentContextView
- *(core,server,cli)* unify task-level context exposure via AgentContextView (Contract C)
- *(schema,mcp)* schemars JsonSchema derives + mse://api/http-endpoints resource
- *(server,service)* per-Run init_ctx override + 3-layer merge chain + TaskRecord.task_input_spec persistence
- *(schema,service)* Blueprint.default_init_ctx with BP -> Task merge chain
- *(operator-ws,agent-block)* splice task-level project_root/work_dir into Direction and priority chain
- *(middleware)* add TaskInputMiddleware for Task-level context injection
- *(compiler)* kind=lua reachable on the default registry via inline spec.source

### Other

- TaskInputMiddleware reads sibling fields directly, drop ST1 init_ctx fold-back
- thread TaskSpec.initial_directive Value end-to-end, render at consumer boundaries only
- *(server)* rename FlowTasksReq to TaskLaunchRequest and split init_ctx roles
- *(mcp)* publish mse://guides/operator-execution-model resource

## [0.5.0](https://github.com/ynishi/mlua-swarm/compare/v0.4.1...v0.5.0) - 2026-07-07

### Added

- *(engine)* propagate run_id through dispatch and record step entries
- *(store)* add TaskStore and RunStore (inmemory + sqlite)
- *(server)* persist tasks/runs and expose GET drill-down routes
- *(mcp)* auto-resolve worker route from worker_handle in worker tools
- *(mcp)* add mse_worker_fetch / mse_worker_submit worker HTTP tools
- *(mcp)* adopt typed run/task ids and expose step trace in swarm_status

### Fixed

- *(worker)* surface WorkerId in the spawn trace log
- *(ids)* unify the operator sid on the SessionId shape (S-<hex>)

### Other

- *(ids)* [**breaking**] prefix-validated ID newtypes, token fingerprint keys, BlueprintId convergence
- *(types)* [**breaking**] rename per-step TaskId to StepId; add TaskId/RunId newtypes
- apply cargo fmt to pre-existing drift in cli/server sources
- *(mcp)* add the mse://guides/id-lifecycle canonical ID inventory
- *(mcp)* document the ID hierarchy and run-trace drill-down

## [0.4.1](https://github.com/ynishi/mlua-swarm/compare/v0.4.0...v0.4.1) - 2026-07-06

### Other

- backfill v0.4.0 CHANGELOG + wire changelog_include for unified changelog

## [0.4.0](https://github.com/ynishi/mlua-swarm/compare/v0.3.0...v0.4.0) - 2026-07-06

### Added

- *(mcp)* unify `swarm_run` blueprint under `BlueprintSelector` (inline | id | file)
- *(mcp)* add `spawn_halt` verb to `mse_ack` for controlled halt
- *(server)* render actual server bind into Spawn directive `base_url`

### Fixed

- *(compiler)* make worker_binding message actionable on both paths + guide

### Other

- *(docs)* lock blueprint-authoring guide field names against flow-ir schema

### Changelog notes

The three `Added` entries and the `Other` entry above were added
retroactively (shipping in the v0.4.1 release train). release-plz
auto-generated the v0.4.0 entry with only the `Fixed` line because
those four commits only touched files under `crates/*/`, and the
per-package `changelog_update` flag was only enabled on the root
crate. The same follow-up also wires `changelog_include` on the root
package so future releases produce a unified CHANGELOG covering all
four crates in the lockstep `version_group = "mse"`.

## [0.3.0](https://github.com/ynishi/mlua-swarm/compare/v0.2.1...v0.3.0) - 2026-07-05

### Added

- *(operator)* wire worker binding through the delegate axis

## [0.2.1](https://github.com/ynishi/mlua-swarm/compare/v0.2.0...v0.2.1) - 2026-07-05

### Added

- adopt mlua-flow-ir 0.1.1 with call_extern (externs) support

## [0.2.0](https://github.com/ynishi/mlua-swarm/compare/v0.1.4...v0.2.0) - 2026-07-05

### Added

- *(operator)* bake worker binding from Blueprint into spawn path

### Other

- resolve remaining clippy warnings
- apply cargo fmt to sqlite store modules

## [0.1.4](https://github.com/ynishi/mlua-swarm/compare/v0.1.3...v0.1.4) - 2026-07-05

### Added

- *(release)* adopt release-plz for automated version bump + crates.io publish

### Fixed

- *(release)* stop member-crate CHANGELOG stubs + fix compare-link tag names

## [0.1.3] — 2026-07-05

### Fixed
- `server.json` version was still `0.1.0` in the v0.1.2 tag, causing
  the `publish-mcp-registry` job to fail its
  `SERVER_VERSION != tag` guard. Bumped alongside the Cargo
  workspace. The OCI `identifier` (`ghcr.io/ynishi/mse:<version>`)
  is bumped in lockstep — Docker image tag is still the actual
  version literal per the cargo-dist runbook.

### Note
- The `publish-homebrew-formula` job also failed for the v0.1.2 tag,
  because the `ynishi/homebrew-tap` repository was created empty and
  has no `main` branch for the formula commit to target. This is
  bootstrap state, not code state, and is fixed out-of-band by
  seeding the tap repo before this tag is pushed.

## [0.1.2] — 2026-07-05

### Fixed
- Windows build: `mse serve`'s SIGTERM handler and `mse mcp`'s
  `launchctl` uid lookup were unconditionally referencing
  `tokio::signal::unix` / `nix::unistd::Uid`, which do not exist on
  Windows. Both are now `#[cfg(unix)]`-gated (the SIGTERM waiter
  becomes a never-resolving future on non-Unix so the `ctrl_c` arm of
  the shutdown `select!` still fires; the `launchctl` module's uid is a
  placeholder on non-Unix since `launchctl` itself is absent there).
  `nix` moved to a `[target.'cfg(unix)'.dependencies]` block. Detected
  by cargo-dist's `x86_64-pc-windows-msvc` target on the v0.1.1 tag.

### Added
- `.github/workflows/ci.yml` — `cargo check` on
  Ubuntu / macOS / Windows and `cargo test` on Ubuntu / macOS for every
  push/PR. Catches unix-only-API regressions before they reach a
  cargo-dist release tag.

## [0.1.1] — 2026-07-05

### Added
- SQLite-backed persistence for all four in-crate stores via `rusqlite-isle`
  (thread-isolated `Connection`, single-writer FIFO discipline):
  `SqliteIssueStore`, `SqliteEnhanceSettingStore`, `SqliteEnhanceLogStore`,
  `SqliteOutputStore`. In-memory backends remain the default.
- `mse serve` gained four `--<store>-store-path` flags (and matching
  `<store>_store_path` TOML fields) selecting the SQLite backend per store:
  `--issue-store-path`, `--enhance-setting-store-path`,
  `--enhance-log-store-path`, `--output-store-path`. Omit any of them to
  fall back to the in-memory default.
- `mlua_swarm_server::build_router_with_ws_factory_and_output` — new
  5-argument variant that lets callers inject a custom `Arc<dyn OutputStore>`
  (the 4-argument form now delegates with `None` for compatibility).
- Graceful SQLite shutdown: `mse serve` collects each backend's
  `AsyncIsleDriver` and drains them via `driver.shutdown().await` after the
  Ctrl-C / SIGTERM handler, so SQLite threads join cleanly.
- `EnhanceSettingStoreError::Other` and `EnhanceLogStoreError::Other`
  variants to carry backend-specific failures (SQLite / IO / serde).

## [0.1.0] — 2026-07-04

### Added
- Initial release of mlua-swarm engine.
- `mlua-swarm-schema`: Blueprint schema (Blueprint / AgentDef / AgentKind / Hints / Strategy / Metadata).
- `mlua-swarm` (root): Swarm engine host — long-running stateful runtime with Role/Verb gate, CapToken, 3-stage pipeline, Middleware overlay, git2 / inmemory Blueprint store, enhance pipeline.
- `mlua-swarm-server`: HTTP + WebSocket server (task API, Blueprint store, Operator WS sessions).
- `mlua-swarm-cli`: `mse` binary with `serve` and `mcp` subcommands (MCP adapter for AI agents).

[Unreleased]: https://github.com/ynishi/mlua-swarm/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/ynishi/mlua-swarm/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/ynishi/mlua-swarm/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/ynishi/mlua-swarm/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ynishi/mlua-swarm/releases/tag/v0.1.0
