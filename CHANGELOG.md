# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
