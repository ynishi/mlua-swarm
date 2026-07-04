//! AgentBlock — headless agent execution. This is an axis separate from
//! Operator (the MainAI-coupled path); when coupling is needed, wrap it
//! at the middleware layer (`crate::middleware`).

pub mod runtime;

pub use runtime::AgentBlockInProcessSpawnerFactory;
