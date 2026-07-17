//! Runtime adapter contracts and shared ingestion utilities.

#[cfg(target_os = "linux")]
mod capability;
#[cfg(target_os = "linux")]
mod incremental;
#[cfg(target_os = "linux")]
mod scan;
#[cfg(target_os = "linux")]
mod watcher;
mod worktree;

#[cfg(target_os = "linux")]
pub use capability::{CapabilityRoot, PathAccessError};
#[cfg(target_os = "linux")]
pub use incremental::{
    AppendedRecords, FileCursor, FileIdentity, IncrementalReadError, IncrementalReader, ReadBudget,
    ReadBudgetError, ReadOutcome, ReconcileReason,
};
#[cfg(target_os = "linux")]
pub use scan::{DirectoryScanner, ScanBudget, ScanBudgetError, ScanResult, ScanUncertainty};
#[cfg(target_os = "linux")]
pub use watcher::{
    WatchError, WatchService, WatchSignal, WatchTargetId, WatchTargetIdError, WatchUncertainty,
};
pub use worktree::{Attribution, WorktreeOwners};
