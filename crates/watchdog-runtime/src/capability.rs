use std::{
    fs::{self, File},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use rustix::fs::{Mode, OFlags, ResolveFlags, open, openat2};
use thiserror::Error;

/// Held directory capability used for every runtime-state file access.
#[derive(Clone)]
pub struct CapabilityRoot {
    canonical: Arc<PathBuf>,
    directory: Arc<std::os::fd::OwnedFd>,
}

impl std::fmt::Debug for CapabilityRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CapabilityRoot")
            .finish_non_exhaustive()
    }
}

impl CapabilityRoot {
    /// Canonicalize and hold a configured concrete read-only root.
    ///
    /// # Errors
    ///
    /// Returns [`PathAccessError`] when the root is absent, not a directory, or
    /// cannot be opened without write access.
    pub fn new(root: impl AsRef<Path>) -> Result<Self, PathAccessError> {
        let canonical = fs::canonicalize(root).map_err(|_| PathAccessError::RootUnavailable)?;
        if !canonical.is_dir() {
            return Err(PathAccessError::RootNotDirectory);
        }
        let directory = open(
            &canonical,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| PathAccessError::RootUnavailable)?;
        Ok(Self {
            canonical: Arc::new(canonical),
            directory: Arc::new(directory),
        })
    }

    /// Canonical configured root, suitable for diagnostics and watch setup.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.canonical
    }

    /// Open a regular file beneath the held root using kernel containment.
    ///
    /// # Errors
    ///
    /// Returns [`PathAccessError`] for lexical traversal, symlink escape,
    /// replacement races, or a non-regular target.
    pub fn open_file(&self, relative: &Path) -> Result<File, PathAccessError> {
        validate_relative(relative, false)?;
        let descriptor = self.open(relative, OFlags::RDONLY | OFlags::CLOEXEC)?;
        let file = File::from(descriptor);
        if !file
            .metadata()
            .map_err(|_| PathAccessError::KernelRejected)?
            .is_file()
        {
            return Err(PathAccessError::TargetNotFile);
        }
        Ok(file)
    }

    pub(crate) fn open_directory(&self, relative: &Path) -> Result<File, PathAccessError> {
        validate_relative(relative, true)?;
        self.open(
            if relative.as_os_str().is_empty() {
                Path::new(".")
            } else {
                relative
            },
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        )
        .map(File::from)
    }

    pub(crate) fn watch_directory(&self, relative: &Path) -> Result<PathBuf, PathAccessError> {
        let directory = self.open_directory(relative)?;
        let held_metadata = directory
            .metadata()
            .map_err(|_| PathAccessError::KernelRejected)?;
        if !held_metadata.is_dir() {
            return Err(PathAccessError::TargetNotDirectory);
        }
        let path = self.absolute(relative);
        let canonical = fs::canonicalize(path).map_err(|_| PathAccessError::KernelRejected)?;
        if !canonical.starts_with(self.path()) {
            return Err(PathAccessError::KernelRejected);
        }
        let path_metadata =
            fs::metadata(&canonical).map_err(|_| PathAccessError::KernelRejected)?;
        if held_metadata.dev() != path_metadata.dev() || held_metadata.ino() != path_metadata.ino()
        {
            return Err(PathAccessError::KernelRejected);
        }
        Ok(canonical)
    }

    pub(crate) fn absolute(&self, relative: &Path) -> PathBuf {
        self.path().join(relative)
    }

    fn open(
        &self,
        relative: &Path,
        flags: OFlags,
    ) -> Result<std::os::fd::OwnedFd, PathAccessError> {
        openat2(
            self.directory.as_ref(),
            relative,
            flags,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_MAGICLINKS,
        )
        .map_err(|_| PathAccessError::KernelRejected)
    }
}

/// Stable path-policy failures that never echo an untrusted path.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PathAccessError {
    /// Configured root cannot be canonicalized or opened.
    #[error("Configured capability root is unavailable")]
    RootUnavailable,
    /// Configured root is not a directory.
    #[error("Configured capability root is not a directory")]
    RootNotDirectory,
    /// Relative path is empty where a file is required, absolute, or traverses upward.
    #[error("Requested path is not a valid capability-relative path")]
    InvalidRelativePath,
    /// Kernel containment rejected the lookup or a path race.
    #[error("Kernel rejected access outside the held capability root")]
    KernelRejected,
    /// File operation resolved to a non-regular target.
    #[error("Capability target is not a regular file")]
    TargetNotFile,
    /// Directory operation resolved to a non-directory target.
    #[error("Capability target is not a directory")]
    TargetNotDirectory,
}

fn validate_relative(path: &Path, allow_empty: bool) -> Result<(), PathAccessError> {
    if path.is_absolute() || (!allow_empty && path.as_os_str().is_empty()) {
        return Err(PathAccessError::InvalidRelativePath);
    }
    if path
        .components()
        .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(PathAccessError::InvalidRelativePath);
    }
    Ok(())
}
