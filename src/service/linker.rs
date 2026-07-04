//! Spawner Linker — a Service-internal helper that takes a compiled
//! `CompiledAgentTable` (the base `SpawnerAdapter`), wraps it with
//! `SpawnerLayer`s via the `LayerRegistry`, and returns the finished
//! `Arc<dyn SpawnerAdapter>`.
//!
//! The old inline loop inside `task_launch.rs` scattered the linker
//! responsibility across the Service path. This module consolidates
//! it into one place, so the Blueprint → Compiled → Linked
//! three-stage split is expressed at the file boundary.
//!
//! # Path
//!
//! ```text
//! Compiler.compile(&Blueprint) → CompiledBlueprint { router: Arc<CompiledAgentTable>, ... }
//!     │
//!     │ link(router, blueprint.spawner_hints.layers, engine)
//!     ▼
//! `Arc<dyn SpawnerAdapter>`   (base + every base_factories[*] + every lookup_hint(hints)[*] wrapped)
//!     │
//!     ▼ EngineDispatcher::with_spawner
//! engine.dispatch_attempt_with(..., &linked) → flow eval
//! ```
//!
//! Unregistered hint keys are **silently skipped** — the
//! `LayerRegistry` default is lenient, so Blueprints stay portable. A
//! strict mode is a carry.

use crate::core::engine::Engine;
use crate::middleware::SpawnerStack;
use crate::worker::adapter::SpawnerAdapter;
use std::sync::Arc;

/// Wrap the compiled base `SpawnerAdapter` with Layers and return the
/// finished value.
///
/// - `base`: `Compiler.compile()`'s `CompiledBlueprint.router`
///   (`Arc<CompiledAgentTable>`), upcast to `Arc<dyn SpawnerAdapter>`.
/// - `layer_hints`: `Blueprint.spawner_hints.layers` — the capability
///   key strings.
/// - `engine`: needed both to look factories up on the
///   `LayerRegistry` and to run them (each base/hint factory takes
///   `&Engine` and returns `Arc<dyn SpawnerLayer>`).
///
/// Order: `base_factories` first (wrapped for every Blueprint), then
/// `lookup_hint` (the layers this Blueprint declares).
pub fn link(
    base: Arc<dyn SpawnerAdapter>,
    layer_hints: &[String],
    engine: &Engine,
) -> Arc<dyn SpawnerAdapter> {
    let layer_registry = engine.layer_registry();
    let mut stack = SpawnerStack::new(base);
    for factory in layer_registry.base_factories() {
        stack = stack.layer_dyn(factory(engine));
    }
    for hint_key in layer_hints {
        if let Some(factory) = layer_registry.lookup_hint(hint_key) {
            stack = stack.layer_dyn(factory(engine));
        }
        // Unregistered hints are silently skipped — the lenient
        // default.
    }
    stack.build()
}
