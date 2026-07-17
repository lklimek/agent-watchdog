use std::{
    collections::VecDeque,
    fs,
    os::{fd::AsRawFd, unix::ffi::OsStrExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use thiserror::Error;

use crate::{CapabilityRoot, PathAccessError};

/// Limits applied to one directory reconciliation pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(
    clippy::struct_field_names,
    reason = "the public budget contract names each independent maximum explicitly"
)]
pub struct ScanBudget {
    max_depth: usize,
    max_entries: usize,
    max_path_bytes: usize,
    max_elapsed: Duration,
}

impl ScanBudget {
    /// Validate depth, entry, and path-byte limits with a short time budget.
    ///
    /// # Errors
    ///
    /// Returns [`ScanBudgetError`] if any limit is zero.
    pub const fn new(
        max_depth: usize,
        max_entries: usize,
        max_path_bytes: usize,
    ) -> Result<Self, ScanBudgetError> {
        if max_depth == 0 || max_entries == 0 || max_path_bytes == 0 {
            return Err(ScanBudgetError);
        }
        Ok(Self {
            max_depth,
            max_entries,
            max_path_bytes,
            max_elapsed: Duration::from_millis(50),
        })
    }

    /// Override the maximum wall duration for one best-effort pass.
    ///
    /// # Errors
    ///
    /// Returns [`ScanBudgetError`] for a zero duration.
    pub const fn with_max_elapsed(
        mut self,
        max_elapsed: Duration,
    ) -> Result<Self, ScanBudgetError> {
        if max_elapsed.is_zero() {
            return Err(ScanBudgetError);
        }
        self.max_elapsed = max_elapsed;
        Ok(self)
    }
}

/// Zero-sized scan limits cannot make bounded progress.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Directory scan budgets must be positive")]
pub struct ScanBudgetError;

/// Bounded non-symlink-following directory enumerator.
#[derive(Clone, Copy, Debug)]
pub struct DirectoryScanner {
    budget: ScanBudget,
}

impl DirectoryScanner {
    /// Construct a scanner with validated limits.
    #[must_use]
    pub const fn new(budget: ScanBudget) -> Self {
        Self { budget }
    }

    /// Enumerate subdirectories beneath a capability root without following links.
    ///
    /// # Errors
    ///
    /// Returns [`PathAccessError`] when the requested scan root is invalid.
    pub fn scan(
        &self,
        root: &CapabilityRoot,
        relative: &Path,
    ) -> Result<ScanResult, PathAccessError> {
        root.open_directory(relative)?;
        let started = Instant::now();
        let mut queue = VecDeque::from([(relative.to_path_buf(), 0_usize)]);
        let mut directories = Vec::new();
        let mut path_bytes = 0_usize;
        let mut uncertainty = None;

        while let Some((directory, depth)) = queue.pop_front() {
            if started.elapsed() >= self.budget.max_elapsed {
                uncertainty.get_or_insert(ScanUncertainty::TimeBudget);
                break;
            }
            let Ok(handle) = root.open_directory(&directory) else {
                uncertainty.get_or_insert(ScanUncertainty::PathRace);
                continue;
            };
            let fd_path = PathBuf::from(format!("/proc/self/fd/{}", handle.as_raw_fd()));
            let Ok(entries) = fs::read_dir(fd_path) else {
                uncertainty.get_or_insert(ScanUncertainty::PathRace);
                continue;
            };
            let mut entries = entries.filter_map(Result::ok).collect::<Vec<_>>();
            entries.sort_by_key(fs::DirEntry::file_name);
            for entry in entries {
                if directories.len() >= self.budget.max_entries {
                    uncertainty.get_or_insert(ScanUncertainty::EntryBudget);
                    return Ok(ScanResult {
                        directories,
                        uncertainty,
                    });
                }
                let Ok(file_type) = entry.file_type() else {
                    uncertainty.get_or_insert(ScanUncertainty::PathRace);
                    continue;
                };
                if !file_type.is_dir() || file_type.is_symlink() {
                    continue;
                }
                let child = directory.join(entry.file_name());
                let child_bytes = child.as_os_str().as_bytes().len();
                if path_bytes.saturating_add(child_bytes) > self.budget.max_path_bytes {
                    uncertainty.get_or_insert(ScanUncertainty::PathByteBudget);
                    return Ok(ScanResult {
                        directories,
                        uncertainty,
                    });
                }
                path_bytes = path_bytes.saturating_add(child_bytes);
                directories.push(root.absolute(&child));
                if depth.saturating_add(1) < self.budget.max_depth {
                    queue.push_back((child, depth.saturating_add(1)));
                } else {
                    uncertainty.get_or_insert(ScanUncertainty::DepthBudget);
                }
            }
        }
        Ok(ScanResult {
            directories,
            uncertainty,
        })
    }
}

/// One bounded directory scan with optional degradation evidence.
#[derive(Clone, Eq, PartialEq)]
pub struct ScanResult {
    directories: Vec<PathBuf>,
    uncertainty: Option<ScanUncertainty>,
}

impl std::fmt::Debug for ScanResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScanResult")
            .field("directory_count", &self.directories.len())
            .field("uncertainty", &self.uncertainty)
            .finish()
    }
}

impl ScanResult {
    /// Concrete in-root directories discovered under the budget.
    #[must_use]
    pub fn directories(&self) -> &[PathBuf] {
        &self.directories
    }

    /// First condition requiring another bounded reconciliation pass.
    #[must_use]
    pub const fn uncertainty(&self) -> Option<ScanUncertainty> {
        self.uncertainty
    }
}

/// Reason a scan could not prove complete coverage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanUncertainty {
    /// Maximum directory depth was reached.
    DepthBudget,
    /// Maximum directory entries were returned.
    EntryBudget,
    /// Maximum cumulative relative-path bytes were returned.
    PathByteBudget,
    /// Maximum wall duration elapsed.
    TimeBudget,
    /// A directory changed or vanished during enumeration.
    PathRace,
}
