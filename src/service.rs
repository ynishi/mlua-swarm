//! Domain-service layer.
//!
//! Extracts the domain operations that `Application`s share into
//! `Service`s. Sits one layer above the direct engine API and one
//! layer below `Application`.

pub mod linker;
pub mod task_launch;

pub use linker::link;
pub use task_launch::{
    TaskInputSpec, TaskLaunchError, TaskLaunchInput, TaskLaunchOutput, TaskLaunchService,
};
