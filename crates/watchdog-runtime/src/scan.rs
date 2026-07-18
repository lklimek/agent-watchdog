use std::{
    collections::{BTreeMap, VecDeque},
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

/// Lexicographic traversal order within each scanned directory.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ScanOrder {
    /// Visit lower filenames first.
    #[default]
    Ascending,
    /// Visit higher filenames first.
    Descending,
}

/// Bounded non-symlink-following directory enumerator.
#[derive(Clone, Copy, Debug)]
pub struct DirectoryScanner {
    budget: ScanBudget,
    order: ScanOrder,
}

impl DirectoryScanner {
    /// Construct a scanner with validated limits.
    #[must_use]
    pub const fn new(budget: ScanBudget) -> Self {
        Self {
            budget,
            order: ScanOrder::Ascending,
        }
    }

    /// Select the lexicographic traversal order for each directory.
    #[must_use]
    pub const fn with_order(mut self, order: ScanOrder) -> Self {
        self.order = order;
        self
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
        let mut files = Vec::new();
        let mut entry_count = 0_usize;
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
            let Ok(mut directory_entries) = fs::read_dir(fd_path) else {
                uncertainty.get_or_insert(ScanUncertainty::PathRace);
                continue;
            };
            let remaining = self.budget.max_entries.saturating_sub(entry_count);
            if remaining == 0 {
                if directory_entries.next().is_some() {
                    uncertainty.get_or_insert(ScanUncertainty::EntryBudget);
                    break;
                }
                continue;
            }
            let mut entries = BTreeMap::new();
            let mut entry_budget_exhausted = false;
            for entry in &mut directory_entries {
                if started.elapsed() >= self.budget.max_elapsed {
                    uncertainty.get_or_insert(ScanUncertainty::TimeBudget);
                    break;
                }
                let Ok(entry) = entry else {
                    uncertainty.get_or_insert(ScanUncertainty::PathRace);
                    continue;
                };
                entries.insert(entry.file_name(), entry);
                if entries.len() > remaining {
                    entry_budget_exhausted = true;
                    match self.order {
                        ScanOrder::Ascending => {
                            entries.pop_last();
                        }
                        ScanOrder::Descending => {
                            entries.pop_first();
                        }
                    }
                }
            }
            let mut entries = entries.into_values().collect::<Vec<_>>();
            if self.order == ScanOrder::Descending {
                entries.reverse();
            }
            for entry in entries {
                entry_count = entry_count.saturating_add(1);
                let Ok(file_type) = entry.file_type() else {
                    uncertainty.get_or_insert(ScanUncertainty::PathRace);
                    continue;
                };
                if file_type.is_symlink() || (!file_type.is_dir() && !file_type.is_file()) {
                    continue;
                }
                let child = directory.join(entry.file_name());
                let child_bytes = child.as_os_str().as_bytes().len();
                if path_bytes.saturating_add(child_bytes) > self.budget.max_path_bytes {
                    uncertainty.get_or_insert(ScanUncertainty::PathByteBudget);
                    return Ok(ScanResult {
                        directories,
                        files,
                        uncertainty,
                    });
                }
                path_bytes = path_bytes.saturating_add(child_bytes);
                if file_type.is_file() {
                    files.push(root.absolute(&child));
                } else {
                    directories.push(root.absolute(&child));
                    if depth.saturating_add(1) < self.budget.max_depth {
                        queue.push_back((child, depth.saturating_add(1)));
                    } else {
                        uncertainty.get_or_insert(ScanUncertainty::DepthBudget);
                    }
                }
            }
            if entry_budget_exhausted {
                uncertainty.get_or_insert(ScanUncertainty::EntryBudget);
                break;
            }
        }
        Ok(ScanResult {
            directories,
            files,
            uncertainty,
        })
    }
}

/// One bounded directory scan with optional degradation evidence.
#[derive(Clone, Eq, PartialEq)]
pub struct ScanResult {
    directories: Vec<PathBuf>,
    files: Vec<PathBuf>,
    uncertainty: Option<ScanUncertainty>,
}

impl std::fmt::Debug for ScanResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScanResult")
            .field("directory_count", &self.directories.len())
            .field("file_count", &self.files.len())
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

    /// Concrete in-root regular files discovered under the budget.
    #[must_use]
    pub fn files(&self) -> &[PathBuf] {
        &self.files
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
