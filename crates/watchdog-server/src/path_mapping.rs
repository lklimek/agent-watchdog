use std::path::{Component, Path, PathBuf};

use thiserror::Error;

/// Explicit projection from a runtime-native host prefix to its read-only
/// mount inside the supported Docker container.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorktreePathMapping {
    native_root: PathBuf,
    mounted_root: PathBuf,
}

impl WorktreePathMapping {
    /// Validate a concrete native prefix and canonical mounted capability root.
    ///
    /// The native prefix need not exist inside the container. The mounted root
    /// must exist and may not be the filesystem root.
    ///
    /// # Errors
    ///
    /// Returns [`PathMappingError`] for a relative, ambiguous, overbroad, or
    /// unavailable mapping.
    pub fn new(
        native_root: impl Into<PathBuf>,
        mounted_root: impl Into<PathBuf>,
    ) -> Result<Self, PathMappingError> {
        let native_root = native_root.into();
        if !is_concrete_absolute(&native_root) {
            return Err(PathMappingError::InvalidNativeRoot);
        }
        let mounted_root = mounted_root
            .into()
            .canonicalize()
            .map_err(|_| PathMappingError::InvalidMountedRoot)?;
        if mounted_root == Path::new("/") || !mounted_root.is_dir() {
            return Err(PathMappingError::InvalidMountedRoot);
        }
        Ok(Self {
            native_root,
            mounted_root,
        })
    }

    /// Runtime-visible host prefix retained for human-facing metadata.
    #[must_use]
    pub fn native_root(&self) -> &Path {
        &self.native_root
    }

    /// Canonical in-container capability root used for safe filesystem access.
    #[must_use]
    pub fn mounted_root(&self) -> &Path {
        &self.mounted_root
    }

    pub(crate) fn validate_native_path(&self, candidate: &Path) -> Option<String> {
        let relative = candidate.strip_prefix(&self.native_root).ok()?;
        self.project_relative(relative)?;
        self.native_root
            .join(relative)
            .to_str()
            .map(ToOwned::to_owned)
    }

    pub(crate) fn project_native_path(&self, candidate: &Path) -> Option<PathBuf> {
        let relative = candidate.strip_prefix(&self.native_root).ok()?;
        self.project_relative(relative)
    }

    pub(crate) fn project_native_directory(&self, candidate: &Path) -> Option<(String, PathBuf)> {
        let native = self.validate_native_path(candidate)?;
        let mounted = self.project_native_path(candidate)?;
        mounted.is_dir().then_some((native, mounted))
    }

    fn project_relative(&self, relative: &Path) -> Option<PathBuf> {
        if relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return None;
        }
        let projected = self.mounted_root.join(relative).canonicalize().ok()?;
        if !projected.starts_with(&self.mounted_root) {
            return None;
        }
        Some(projected)
    }
}

/// Invalid host-to-container worktree projection.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PathMappingError {
    /// Host prefix is relative, root-wide, or contains ambiguous components.
    #[error("Native worktree prefix is invalid")]
    InvalidNativeRoot,
    /// Container capability root is absent, not a directory, or root-wide.
    #[error("Mounted worktree root is invalid")]
    InvalidMountedRoot,
}

fn is_concrete_absolute(path: &Path) -> bool {
    path.is_absolute()
        && path != Path::new("/")
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}
