use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use thiserror::Error;
use watchdog_domain::{
    AdapterIdentity, BoundedText, ChildSessionId, Clock, EvidenceTrust, ObservationEnvelope,
    ObservationId, ObservationPayload, ObservationSource, SessionIdentity, SessionKind,
};
use watchdog_runtime::{Attribution, WorktreeOwners};
use watchdog_store::{StoreError, StoredSessionRecord, WatchdogStore};

use crate::AgentApi;

const MAX_CHILD_SESSIONS: u32 = 1_000;

/// Best-effort result of attributing one coalesced inotify batch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FilesystemActivityReport {
    attributed: u32,
    ambiguous: u32,
    unowned: u32,
    warnings: u32,
}

impl FilesystemActivityReport {
    /// Child sessions refreshed by unambiguous worktree ownership.
    #[must_use]
    pub const fn attributed(self) -> u32 {
        self.attributed
    }

    /// Changed paths withheld because more than one child owns the worktree.
    #[must_use]
    pub const fn ambiguous(self) -> u32 {
        self.ambiguous
    }

    /// Changed paths with no current child owner.
    #[must_use]
    pub const fn unowned(self) -> u32 {
        self.unowned
    }

    /// Attributable observations that could not be applied safely.
    #[must_use]
    pub const fn warnings(self) -> u32 {
        self.warnings
    }
}

/// Converts capability-projected worktree invalidations into typed progress.
#[derive(Clone)]
pub struct FilesystemActivityReconciler {
    api: AgentApi,
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
}

impl FilesystemActivityReconciler {
    /// Construct the reconciler over shared durable state.
    #[must_use]
    pub fn new(api: AgentApi, store: WatchdogStore, clock: Arc<dyn Clock>) -> Self {
        Self { api, store, clock }
    }

    /// Attribute a coalesced set of native worktree paths.
    ///
    /// Shared worktrees remain neutral without exact process/file evidence.
    /// Duplicate paths and duplicate child attribution in one batch coalesce.
    ///
    /// # Errors
    ///
    /// Returns [`FilesystemActivityError`] when durable ownership metadata
    /// cannot be loaded. Individual stale sessions are counted as warnings.
    pub async fn reconcile(
        &self,
        changed_paths: &[PathBuf],
    ) -> Result<FilesystemActivityReport, FilesystemActivityError> {
        let records = self
            .store
            .sessions_by_kind(SessionKind::Child, MAX_CHILD_SESSIONS)
            .await?;
        let (owners, records) = self.load_owners(records).await?;
        let mut report = FilesystemActivityReport::default();
        let mut paths = changed_paths.to_vec();
        paths.sort();
        paths.dedup();
        let mut attributed = BTreeMap::<ChildSessionId, usize>::new();
        for (index, path) in paths.iter().enumerate() {
            match owners.attribute(path, None) {
                Attribution::Child(child) => {
                    attributed.entry(child).or_insert(index);
                }
                Attribution::Ambiguous => {
                    report.ambiguous = report.ambiguous.saturating_add(1);
                }
                Attribution::Unowned => {
                    report.unowned = report.unowned.saturating_add(1);
                }
            }
        }
        for (child, index) in attributed {
            let Some(record) = records.get(&child) else {
                report.warnings = report.warnings.saturating_add(1);
                continue;
            };
            if self.ingest(record, index).await.is_err() {
                report.warnings = report.warnings.saturating_add(1);
            } else {
                report.attributed = report.attributed.saturating_add(1);
            }
        }
        Ok(report)
    }

    async fn load_owners(
        &self,
        records: Vec<StoredSessionRecord>,
    ) -> Result<
        (
            WorktreeOwners,
            BTreeMap<ChildSessionId, StoredSessionRecord>,
        ),
        StoreError,
    > {
        let mut owners = WorktreeOwners::new();
        let mut by_child = BTreeMap::new();
        for record in records {
            let SessionIdentity::Child(child) = record.session else {
                continue;
            };
            if let Some(metadata) = self.store.session_metadata(record.session).await?
                && let Some(directory) = metadata.startup_directory()
            {
                let directory = PathBuf::from(directory);
                if is_concrete_native_path(&directory) {
                    owners.register(directory, child);
                }
            }
            by_child.insert(child, record);
        }
        Ok((owners, by_child))
    }

    async fn ingest(&self, record: &StoredSessionRecord, index: usize) -> Result<(), ()> {
        let now = self.clock.now();
        let source = ObservationSource::new(
            AdapterIdentity::new(record.native.runtime(), "filesystem-v1").map_err(|_| ())?,
            "worktree:inotify",
            EvidenceTrust::Corroborating,
            None,
        )
        .map_err(|_| ())?;
        let event_key = format!(
            "{}:{}:{index}",
            record.session.session_id(),
            now.monotonic_ms()
        );
        let observation = ObservationEnvelope::new(
            ObservationId::from_native(record.native.runtime(), "filesystem", event_key)
                .map_err(|_| ())?,
            record.native.clone(),
            now,
            source,
            ObservationPayload::Progress(
                BoundedText::new("progress", "Filesystem activity in owned worktree")
                    .map_err(|_| ())?,
            ),
        )
        .map_err(|_| ())?;
        self.api
            .ingest_native_observation(observation)
            .await
            .map(|_| ())
            .map_err(|_| ())
    }
}

fn is_concrete_native_path(path: &Path) -> bool {
    path.is_absolute()
        && path != Path::new("/")
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

impl std::fmt::Debug for FilesystemActivityReconciler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FilesystemActivityReconciler")
            .finish_non_exhaustive()
    }
}

/// Durable ownership metadata could not be loaded.
#[derive(Debug, Error)]
pub enum FilesystemActivityError {
    /// `SQLite` access or decoding failed.
    #[error("Filesystem activity ownership could not be loaded")]
    Store(#[from] StoreError),
}
