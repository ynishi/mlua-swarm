//! `ProjectionPlacement` — the single resolver that decides where a
//! Step's materialized OUTPUT file lives on disk.
//!
//! # Architecture
//!
//! Before this module, the target path
//! `<root>/workspace/tasks/<task_id>/ctx/<name>.md` was hardcoded
//! independently at three call sites — the submit-time sink
//! (`crate::core::engine::Engine::submit_output`'s
//! `materialize_final_submission`, via
//! [`crate::core::projection::FileProjectionAdapter`]), the server-side
//! read-back (`crates/mlua-swarm-server/src/projection.rs`'s
//! `materialized_file_path` / `resolve_materialized_file`), and the
//! spawn-time in-flight pointer
//! (`crates/mlua-swarm-server/src/operator_ws/session.rs`'s
//! `append_projection_pointer`). The three copies had drifted: the first
//! two resolved `root` as `work_dir` falling back to `project_root`, the
//! third used `work_dir` ONLY (no fallback) — an asymmetry a Blueprint
//! whose spawn only carries `project_root` silently loses its in-flight
//! `ctx_projection` pointer to.
//!
//! [`ProjectionPlacement`] collapses all three copies into one type: the
//! sole construction site is `Compiler::compile` (see [`Self::from_spec`]),
//! built once from `Blueprint.projection_placement`
//! (`mlua_swarm_schema::ProjectionPlacementSpec`), then threaded — the
//! SAME "construct once, read many" pattern
//! `crate::core::step_naming::StepNaming` established for GH #23 —
//! through `crate::blueprint::EngineDispatcher::with_projection_placement`
//! into `EngineState.projection_placements` (keyed by `StepId`), and read
//! back via `crate::core::engine::Engine::projection_placement_for`. All
//! three call sites above now resolve `root` via [`Self::resolve_root`]
//! and the target path via [`Self::target_path`] — the fallback order and
//! the directory template are, by construction, identical wherever the
//! resolver is consulted.
//!
//! # Byte-compat default
//!
//! [`ProjectionPlacement::default`] reproduces the pre-GH-#27 hardcoded
//! behavior exactly: `root_preference = WorkDir` (work_dir falling back
//! to project_root) and `dir_template = "workspace/tasks/{task_id}/ctx"`.
//! Every Blueprint that never declares `projection_placement` resolves
//! through this default, so this module changes no observable behavior
//! for pre-#27 Blueprints.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::core::agent_context::AgentContextView;

/// The `{task_id}` placeholder substituted into
/// [`ProjectionPlacement::dir_template`] at materialize time.
const TASK_ID_PLACEHOLDER: &str = "{task_id}";

/// The byte-compat default directory template (pre-GH-#27 hardcoded
/// value).
const DEFAULT_DIR_TEMPLATE: &str = "workspace/tasks/{task_id}/ctx";

/// Which of the spawn-time `work_dir` / `project_root` [`AgentContextView`]
/// fields [`ProjectionPlacement::resolve_root`] prefers, falling back to
/// the other when the preferred one is absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RootPreference {
    /// Prefer `work_dir`, falling back to `project_root` — the pre-GH-#27
    /// behavior every existing call site resolved (or, for the spawn-time
    /// pointer, should have resolved — see the module doc's asymmetry
    /// note).
    #[default]
    WorkDir,
    /// Prefer `project_root`, falling back to `work_dir`.
    ProjectRoot,
}

/// The projection placement resolver (see the module doc for the full
/// architecture narrative). Small and `Clone` — every consumer holds its
/// own copy rather than sharing a lock. `Serialize`/`Deserialize`: the
/// spawn-time in-flight pointer
/// (`crates/mlua-swarm-server/src/operator_ws/session.rs`'s
/// `append_projection_pointer`) has no direct `Engine` handle, so
/// `crate::middleware::agent_context::AgentContextMiddleware` (which does)
/// resolves this once per spawn and stashes it JSON-serialized into
/// `ctx.meta.runtime[crate::core::agent_context::PROJECTION_PLACEMENT_KEY]`
/// — the same channel `AGENT_CONTEXT_KEY` already establishes for
/// [`AgentContextView`] itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionPlacement {
    /// Root resolution preference.
    pub root_preference: RootPreference,
    /// Target directory template, relative to the resolved root, with
    /// [`TASK_ID_PLACEHOLDER`] substituted at materialize time. Validated
    /// by [`Self::validate_dir_template`] before being accepted into a
    /// `ProjectionPlacement` — every instance in circulation already
    /// satisfies that contract.
    pub dir_template: String,
}

impl Default for ProjectionPlacement {
    /// The byte-compat default (see the module doc).
    fn default() -> Self {
        Self {
            root_preference: RootPreference::default(),
            dir_template: DEFAULT_DIR_TEMPLATE.to_string(),
        }
    }
}

/// Everything that can go wrong building a [`ProjectionPlacement`] from a
/// Blueprint-declared `mlua_swarm_schema::ProjectionPlacementSpec`. Every
/// variant carries the offending literal for the caller's error message
/// (the same convention `blueprint::compiler::CompileError` follows).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProjectionPlacementError {
    /// `dir_template` was the empty string.
    #[error("projection_placement.dir_template must not be empty")]
    EmptyDirTemplate,
    /// `dir_template` did not contain the `{task_id}` placeholder.
    #[error("projection_placement.dir_template must contain the '{{task_id}}' placeholder: {0:?}")]
    MissingTaskIdPlaceholder(String),
    /// `dir_template` was an absolute path.
    #[error(
        "projection_placement.dir_template must be a relative path, got an absolute one: {0:?}"
    )]
    AbsolutePath(String),
    /// `dir_template` contained a `..` path segment.
    #[error("projection_placement.dir_template must not contain '..' path segments: {0:?}")]
    ParentDirComponent(String),
    /// `root` was neither `"work_dir"` nor `"project_root"`.
    #[error(r#"projection_placement.root must be "work_dir" or "project_root", got {0:?}"#)]
    InvalidRoot(String),
}

impl ProjectionPlacement {
    /// Builds a `ProjectionPlacement` from a Blueprint's optional
    /// `projection_placement` spec — the sole construction site
    /// (`blueprint::compiler::Compiler::compile`, mirroring
    /// `crate::core::step_naming::StepNaming::from_blueprint`'s
    /// single-construction-site contract). `None` (no
    /// `Blueprint.projection_placement` declared) returns
    /// [`Self::default`] unchanged — every pre-GH-#27 Blueprint compiles
    /// byte-identically through this path. A declared spec's `dir_template`
    /// is validated via [`Self::validate_dir_template`]; `root`, when
    /// present, must be exactly `"work_dir"` or `"project_root"`.
    pub fn from_spec(
        spec: Option<&mlua_swarm_schema::ProjectionPlacementSpec>,
    ) -> Result<Self, ProjectionPlacementError> {
        let Some(spec) = spec else {
            return Ok(Self::default());
        };
        let root_preference = match spec.root.as_deref() {
            None => RootPreference::default(),
            Some("work_dir") => RootPreference::WorkDir,
            Some("project_root") => RootPreference::ProjectRoot,
            Some(other) => return Err(ProjectionPlacementError::InvalidRoot(other.to_string())),
        };
        let dir_template = match &spec.dir_template {
            None => DEFAULT_DIR_TEMPLATE.to_string(),
            Some(template) => {
                Self::validate_dir_template(template)?;
                template.clone()
            }
        };
        Ok(Self {
            root_preference,
            dir_template,
        })
    }

    /// Validates a `dir_template` candidate: non-empty, contains the
    /// `{task_id}` placeholder, is relative (not absolute, no leading
    /// `/`), and contains no `..` path segment. Shared by [`Self::from_spec`]
    /// so every declared `ProjectionPlacement` in circulation already
    /// satisfies this contract — [`Self::target_dir`] never has to
    /// re-check it.
    pub fn validate_dir_template(template: &str) -> Result<(), ProjectionPlacementError> {
        if template.is_empty() {
            return Err(ProjectionPlacementError::EmptyDirTemplate);
        }
        if !template.contains(TASK_ID_PLACEHOLDER) {
            return Err(ProjectionPlacementError::MissingTaskIdPlaceholder(
                template.to_string(),
            ));
        }
        if template.starts_with('/') || Path::new(template).is_absolute() {
            return Err(ProjectionPlacementError::AbsolutePath(template.to_string()));
        }
        if template.split('/').any(|segment| segment == "..") {
            return Err(ProjectionPlacementError::ParentDirComponent(
                template.to_string(),
            ));
        }
        Ok(())
    }

    /// Resolves the materialize root off `view`'s `work_dir` /
    /// `project_root` fields, per [`Self::root_preference`], falling back
    /// to the other when the preferred one is absent. `None` when neither
    /// is present — every call site's existing fail-open contract
    /// (unresolved root ⇒ skip materialize, never fail the caller) is
    /// preserved unchanged.
    pub fn resolve_root(&self, view: &AgentContextView) -> Option<String> {
        match self.root_preference {
            RootPreference::WorkDir => view.work_dir.clone().or_else(|| view.project_root.clone()),
            RootPreference::ProjectRoot => {
                view.project_root.clone().or_else(|| view.work_dir.clone())
            }
        }
    }

    /// The materialize target DIRECTORY for `task_id`, rooted at `root`:
    /// [`Self::dir_template`] with [`TASK_ID_PLACEHOLDER`] substituted,
    /// joined onto `root` one path segment at a time (so a
    /// multi-segment template like the default
    /// `"workspace/tasks/{task_id}/ctx"` builds the same cross-platform
    /// `PathBuf` the pre-GH-#27 hardcoded `.join("workspace").join("tasks")…`
    /// chain did).
    pub fn target_dir(&self, root: impl AsRef<Path>, task_id: &str) -> PathBuf {
        let rendered = self.dir_template.replace(TASK_ID_PLACEHOLDER, task_id);
        let mut path = root.as_ref().to_path_buf();
        for segment in rendered.split('/') {
            if !segment.is_empty() {
                path = path.join(segment);
            }
        }
        path
    }

    /// The materialize target FILE for `task_id` / `stem` (`stem` is the
    /// canonical step name, or `"_ctx"` for the whole-ctx submission —
    /// see [`crate::core::projection::ProjectionKey::step_slug`]):
    /// [`Self::target_dir`] joined with `"<stem>.md"`.
    pub fn target_path(&self, root: impl AsRef<Path>, task_id: &str, stem: &str) -> PathBuf {
        self.target_dir(root, task_id).join(format!("{stem}.md"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(work_dir: Option<&str>, project_root: Option<&str>) -> AgentContextView {
        AgentContextView {
            work_dir: work_dir.map(String::from),
            project_root: project_root.map(String::from),
            ..AgentContextView::default()
        }
    }

    // ─── from_spec ───────────────────────────────────────────────────

    #[test]
    fn from_spec_none_is_byte_compat_default() {
        let placement = ProjectionPlacement::from_spec(None).expect("no spec is Ok");
        assert_eq!(placement, ProjectionPlacement::default());
        assert_eq!(placement.root_preference, RootPreference::WorkDir);
        assert_eq!(placement.dir_template, "workspace/tasks/{task_id}/ctx");
    }

    #[test]
    fn from_spec_declared_root_project_root() {
        let spec = mlua_swarm_schema::ProjectionPlacementSpec {
            root: Some("project_root".to_string()),
            dir_template: None,
        };
        let placement = ProjectionPlacement::from_spec(Some(&spec)).expect("valid spec");
        assert_eq!(placement.root_preference, RootPreference::ProjectRoot);
        // dir_template still defaults when unset alongside a declared root.
        assert_eq!(placement.dir_template, "workspace/tasks/{task_id}/ctx");
    }

    #[test]
    fn from_spec_declared_custom_dir_template() {
        let spec = mlua_swarm_schema::ProjectionPlacementSpec {
            root: None,
            dir_template: Some("custom/{task_id}/out".to_string()),
        };
        let placement = ProjectionPlacement::from_spec(Some(&spec)).expect("valid spec");
        assert_eq!(placement.root_preference, RootPreference::WorkDir);
        assert_eq!(placement.dir_template, "custom/{task_id}/out");
    }

    #[test]
    fn from_spec_rejects_invalid_root_literal() {
        let spec = mlua_swarm_schema::ProjectionPlacementSpec {
            root: Some("nope".to_string()),
            dir_template: None,
        };
        let err = ProjectionPlacement::from_spec(Some(&spec)).expect_err("invalid root rejected");
        assert!(matches!(err, ProjectionPlacementError::InvalidRoot(r) if r == "nope"));
    }

    // ─── validate_dir_template ───────────────────────────────────────

    #[test]
    fn validate_dir_template_rejects_empty() {
        let err = ProjectionPlacement::validate_dir_template("").unwrap_err();
        assert_eq!(err, ProjectionPlacementError::EmptyDirTemplate);
    }

    #[test]
    fn validate_dir_template_rejects_missing_task_id_placeholder() {
        let err = ProjectionPlacement::validate_dir_template("workspace/tasks/ctx").unwrap_err();
        assert!(
            matches!(err, ProjectionPlacementError::MissingTaskIdPlaceholder(t) if t == "workspace/tasks/ctx")
        );
    }

    #[test]
    fn validate_dir_template_rejects_absolute_path() {
        let err = ProjectionPlacement::validate_dir_template("/abs/{task_id}").unwrap_err();
        assert!(matches!(err, ProjectionPlacementError::AbsolutePath(t) if t == "/abs/{task_id}"));
    }

    #[test]
    fn validate_dir_template_rejects_parent_dir_component() {
        let err = ProjectionPlacement::validate_dir_template("../{task_id}/ctx").unwrap_err();
        assert!(
            matches!(err, ProjectionPlacementError::ParentDirComponent(t) if t == "../{task_id}/ctx")
        );
    }

    #[test]
    fn validate_dir_template_accepts_default() {
        ProjectionPlacement::validate_dir_template("workspace/tasks/{task_id}/ctx")
            .expect("default template is valid");
    }

    #[test]
    fn from_spec_propagates_dir_template_validation_error() {
        let spec = mlua_swarm_schema::ProjectionPlacementSpec {
            root: None,
            dir_template: Some(String::new()),
        };
        let err = ProjectionPlacement::from_spec(Some(&spec)).expect_err("empty template rejected");
        assert_eq!(err, ProjectionPlacementError::EmptyDirTemplate);
    }

    // ─── resolve_root ────────────────────────────────────────────────

    #[test]
    fn resolve_root_work_dir_preference_prefers_work_dir() {
        let placement = ProjectionPlacement::default();
        let v = view(Some("/work"), Some("/proj"));
        assert_eq!(placement.resolve_root(&v).as_deref(), Some("/work"));
    }

    #[test]
    fn resolve_root_work_dir_preference_falls_back_to_project_root() {
        let placement = ProjectionPlacement::default();
        let v = view(None, Some("/proj"));
        assert_eq!(placement.resolve_root(&v).as_deref(), Some("/proj"));
    }

    #[test]
    fn resolve_root_project_root_preference_prefers_project_root() {
        let placement = ProjectionPlacement {
            root_preference: RootPreference::ProjectRoot,
            ..ProjectionPlacement::default()
        };
        let v = view(Some("/work"), Some("/proj"));
        assert_eq!(placement.resolve_root(&v).as_deref(), Some("/proj"));
    }

    #[test]
    fn resolve_root_project_root_preference_falls_back_to_work_dir() {
        let placement = ProjectionPlacement {
            root_preference: RootPreference::ProjectRoot,
            ..ProjectionPlacement::default()
        };
        let v = view(Some("/work"), None);
        assert_eq!(placement.resolve_root(&v).as_deref(), Some("/work"));
    }

    #[test]
    fn resolve_root_neither_present_is_none() {
        let placement = ProjectionPlacement::default();
        let v = view(None, None);
        assert_eq!(placement.resolve_root(&v), None);
    }

    // ─── target_dir / target_path ────────────────────────────────────

    #[test]
    fn target_path_default_matches_pre_gh24_hardcoded_layout() {
        let placement = ProjectionPlacement::default();
        let path = placement.target_path("/root", "T-1", "_ctx");
        assert_eq!(
            path,
            std::path::Path::new("/root/workspace/tasks/T-1/ctx/_ctx.md")
        );
    }

    #[test]
    fn target_path_custom_template_substitutes_task_id() {
        let placement = ProjectionPlacement {
            root_preference: RootPreference::default(),
            dir_template: "custom/{task_id}/out".to_string(),
        };
        let path = placement.target_path("/root", "T-2", "planner");
        assert_eq!(
            path,
            std::path::Path::new("/root/custom/T-2/out/planner.md")
        );
    }

    #[test]
    fn target_dir_and_target_path_are_write_read_consistent() {
        // 3-path consistency proof (unit level): the same (root, task_id,
        // stem) resolved through the SAME `ProjectionPlacement` instance
        // always yields the identical path, regardless of how many times
        // it is called — a write-time call and a later read-time call
        // converge on the same location by construction.
        let placement = ProjectionPlacement {
            root_preference: RootPreference::ProjectRoot,
            dir_template: "custom/{task_id}/out".to_string(),
        };
        let written = placement.target_path("/proj", "T-3", "reviewer");
        let read_back = placement.target_path("/proj", "T-3", "reviewer");
        assert_eq!(written, read_back);
        assert_eq!(
            written,
            std::path::Path::new("/proj/custom/T-3/out/reviewer.md")
        );
    }

    // ─── serde round-trip (ctx.meta.runtime channel) ────────────────

    /// GH #27: `ProjectionPlacement` round-trips through JSON unchanged —
    /// the exact channel `AgentContextMiddleware` uses to stash a
    /// resolved instance into `ctx.meta.runtime` for the spawn-time
    /// in-flight pointer (which has no direct `Engine` handle to call
    /// `Engine::projection_placement_for` with).
    #[test]
    fn serde_round_trip_preserves_custom_placement() {
        let placement = ProjectionPlacement {
            root_preference: RootPreference::ProjectRoot,
            dir_template: "custom/{task_id}/out".to_string(),
        };
        let json = serde_json::to_value(&placement).expect("serializes");
        let back: ProjectionPlacement = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, placement);
    }

    #[test]
    fn serde_round_trip_preserves_default_placement() {
        let placement = ProjectionPlacement::default();
        let json = serde_json::to_value(&placement).expect("serializes");
        let back: ProjectionPlacement = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, placement);
    }
}
