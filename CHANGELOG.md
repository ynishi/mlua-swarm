# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- SQLite-backed persistence for all four in-crate stores via `rusqlite-isle`
  (thread-isolated `Connection`, single-writer FIFO discipline):
  `SqliteIssueStore`, `SqliteEnhanceSettingStore`, `SqliteEnhanceLogStore`,
  `SqliteOutputStore`. In-memory backends remain the default.
- `mse serve --issue-store-path <path>` (and `issue_store_path` in the TOML
  config file) — when set, `IssueStore` is backed by SQLite at that path;
  omit to fall back to the process-volatile `InMemoryIssueStore`.
- `EnhanceSettingStoreError::Other` and `EnhanceLogStoreError::Other`
  variants to carry backend-specific failures (SQLite / IO / serde).

## [0.1.0] — 2026-07-04

### Added
- Initial release of mlua-swarm engine.
- `mlua-swarm-schema`: Blueprint schema (Blueprint / AgentDef / AgentKind / Hints / Strategy / Metadata).
- `mlua-swarm` (root): Swarm engine host — long-running stateful runtime with Role/Verb gate, CapToken, 3-stage pipeline, Middleware overlay, git2 / inmemory Blueprint store, enhance pipeline.
- `mlua-swarm-server`: HTTP + WebSocket server (task API, Blueprint store, Operator WS sessions).
- `mlua-swarm-cli`: `mse` binary with `serve` and `mcp` subcommands (MCP adapter for AI agents).

[Unreleased]: https://github.com/ynishi/mlua-swarm/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ynishi/mlua-swarm/releases/tag/v0.1.0
