//! Runtime adapter contracts and shared ingestion utilities.

#[cfg(target_os = "linux")]
mod capability;
mod coordinator;
mod health;
#[cfg(target_os = "linux")]
mod incremental;
mod queue;
#[cfg(target_os = "linux")]
mod scan;
#[cfg(target_os = "linux")]
mod watcher;
mod worktree;

#[cfg(target_os = "linux")]
pub use capability::{CapabilityRoot, PathAccessError};
pub use coordinator::{CoordinatorError, EventSequence, SessionCoordinator};
pub use health::{ComponentHealth, ComponentId, ComponentStatus, HealthRegistry, HealthScope};
#[cfg(target_os = "linux")]
pub use incremental::{
    AppendedRecords, FileCursor, FileIdentity, IncrementalReadError, IncrementalReader, ReadBudget,
    ReadBudgetError, ReadOutcome, ReconcileReason,
};
pub use queue::{AdmissionError, ObservationClass, QueueCapacityError, SessionQueue};
#[cfg(target_os = "linux")]
pub use scan::{
    DirectoryScanner, ScanBudget, ScanBudgetError, ScanOrder, ScanResult, ScanUncertainty,
};
#[cfg(target_os = "linux")]
pub use watcher::{
    WatchError, WatchService, WatchSignal, WatchTargetId, WatchTargetIdError, WatchUncertainty,
};
pub use worktree::{Attribution, WorktreeOwners};
