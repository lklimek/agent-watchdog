use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use thiserror::Error;
use watchdog_domain::{
    AdapterIdentity, BoundedText, ChildSessionId, Clock, DomainInputError, EvidenceTrust,
    ObservationEnvelope, ObservationError, ObservationId, ObservationPayload, ObservationSource,
    SessionIdentity, SessionKind,
};
use watchdog_runtime::{Attribution, WorktreeOwners};
use watchdog_store::{StoreError, StoredSessionRecord, WatchdogStore};

use crate::{AgentApi, AgentApiError};

const MAX_CHILD_SESSIONS: u32 = 1_000;
const MAX_REGISTERED_PATHS: u32 = 4_096;

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
        let registered_paths = self
            .store
            .registered_watch_paths(MAX_REGISTERED_PATHS)
            .await?;
        let (owners, records) = self.load_owners(records, &registered_paths).await?;
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
                tracing::warn!(
                    event = "filesystem_activity.session_missing",
                    session_id = %child.session_id(),
                    "Attributed filesystem activity has no retained child session"
                );
                report.warnings = report.warnings.saturating_add(1);
                continue;
            };
            match self.ingest(record, index).await {
                Ok(()) => report.attributed = report.attributed.saturating_add(1),
                Err(error) => {
                    tracing::warn!(
                        event = "filesystem_activity.observation_failed",
                        session_id = %record.session.session_id(),
                        error = %error,
                        "Filesystem activity observation could not be ingested"
                    );
                    report.warnings = report.warnings.saturating_add(1);
                }
            }
        }
        Ok(report)
    }

    async fn load_owners(
        &self,
        records: Vec<StoredSessionRecord>,
        registered_paths: &[watchdog_store::RegisteredWatchPathRecord],
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
        for registered in registered_paths {
            let SessionIdentity::Child(child) = registered.session() else {
                continue;
            };
            let directory = PathBuf::from(registered.native_path());
            if by_child.contains_key(&child) && is_concrete_native_path(&directory) {
                owners.register(directory, child);
            }
        }
        Ok((owners, by_child))
    }

    async fn ingest(
        &self,
        record: &StoredSessionRecord,
        index: usize,
    ) -> Result<(), FilesystemActivityIngestError> {
        let now = self.clock.now();
        let source = ObservationSource::new(
            AdapterIdentity::new(record.native.runtime(), "filesystem-v1")?,
            "worktree:inotify",
            EvidenceTrust::Corroborating,
            None,
        )?;
        let event_key = format!(
            "{}:{}:{index}",
            record.session.session_id(),
            now.monotonic_ms()
        );
        let observation = ObservationEnvelope::new(
            ObservationId::from_native(record.native.runtime(), "filesystem", event_key)?,
            record.native.clone(),
            now,
            source,
            ObservationPayload::Progress(BoundedText::new(
                "progress",
                "Filesystem activity in owned worktree",
            )?),
        )?;
        self.api
            .ingest_native_observation(observation)
            .await
            .map(|_| ())?;
        Ok(())
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

#[derive(Debug, Error)]
enum FilesystemActivityIngestError {
    #[error("Filesystem activity evidence is invalid: {0}")]
    Domain(#[from] DomainInputError),
    #[error("Filesystem activity observation is invalid: {0}")]
    Observation(#[from] ObservationError),
    #[error("Filesystem activity observation was rejected: {0}")]
    AgentApi(#[from] AgentApiError),
}

/// Durable ownership metadata could not be loaded.
#[derive(Debug, Error)]
pub enum FilesystemActivityError {
    /// `SQLite` access or decoding failed.
    #[error("Filesystem activity ownership could not be loaded")]
    Store(#[from] StoreError),
}
