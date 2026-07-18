use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
    },
};

use notify::{Config, ErrorKind, Event, EventKind, INotifyWatcher, RecursiveMode, Watcher};
use thiserror::Error;

use crate::{CapabilityRoot, PathAccessError};

type TargetMap = Arc<RwLock<BTreeMap<PathBuf, BTreeSet<WatchTargetId>>>>;

/// Positive identifier for a deduplicated adapter watch target.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WatchTargetId(u64);

impl WatchTargetId {
    /// Validate a positive target identifier.
    ///
    /// # Errors
    ///
    /// Returns [`WatchTargetIdError`] for zero.
    pub const fn new(value: u64) -> Result<Self, WatchTargetIdError> {
        if value == 0 {
            return Err(WatchTargetIdError);
        }
        Ok(Self(value))
    }

    /// Numeric target identity.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Target zero is reserved for missing identity.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Watch target ID must be positive")]
pub struct WatchTargetIdError;

/// Shared Linux inotify service with a bounded nonblocking event boundary.
pub struct WatchService {
    watcher: INotifyWatcher,
    targets: TargetMap,
    receiver: Receiver<WatchSignal>,
    queue_saturated: Arc<AtomicBool>,
    backend_degraded: Arc<AtomicBool>,
    kernel_rescan: Arc<AtomicBool>,
}

impl std::fmt::Debug for WatchService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let watched_paths = self.targets.read().map_or(0, |targets| targets.len());
        formatter
            .debug_struct("WatchService")
            .field("watched_paths", &watched_paths)
            .finish_non_exhaustive()
    }
}

impl WatchService {
    /// Start explicit inotify with symlink following disabled.
    ///
    /// # Errors
    ///
    /// Returns [`WatchError`] for a zero queue or backend initialization failure.
    pub fn new(queue_capacity: usize) -> Result<Self, WatchError> {
        if queue_capacity == 0 {
            return Err(WatchError::InvalidQueueCapacity);
        }
        let (sender, receiver) = sync_channel(queue_capacity);
        let targets = Arc::new(RwLock::new(BTreeMap::new()));
        let queue_saturated = Arc::new(AtomicBool::new(false));
        let backend_degraded = Arc::new(AtomicBool::new(false));
        let kernel_rescan = Arc::new(AtomicBool::new(false));
        let callback_targets = Arc::clone(&targets);
        let callback_queue = Arc::clone(&queue_saturated);
        let callback_backend = Arc::clone(&backend_degraded);
        let callback_rescan = Arc::clone(&kernel_rescan);
        let callback = move |event: notify::Result<Event>| match event {
            Ok(event) => handle_event(
                event,
                &callback_targets,
                &sender,
                &callback_queue,
                &callback_backend,
                &callback_rescan,
            ),
            Err(_) => callback_backend.store(true, Ordering::Release),
        };
        let watcher = INotifyWatcher::new(callback, Config::default().with_follow_symlinks(false))
            .map_err(|error| map_watch_error(&error))?;
        Ok(Self {
            watcher,
            targets,
            receiver,
            queue_saturated,
            backend_degraded,
            kernel_rescan,
        })
    }

    /// Add one capability-checked non-recursive target, deduplicating its path.
    ///
    /// # Errors
    ///
    /// Returns [`WatchError`] when path policy, locking, or the backend rejects
    /// registration. Upstream path content is never retained in the error.
    pub fn add_target(
        &mut self,
        target: WatchTargetId,
        root: &CapabilityRoot,
        relative: &Path,
    ) -> Result<(), WatchError> {
        let path = root.watch_directory(relative)?;
        let mut targets = self
            .targets
            .write()
            .map_err(|_| WatchError::StatePoisoned)?;
        let is_new_path = !targets.contains_key(&path);
        targets.entry(path.clone()).or_default().insert(target);
        drop(targets);
        if is_new_path && let Err(error) = self.watcher.watch(&path, RecursiveMode::NonRecursive) {
            self.targets
                .write()
                .map_err(|_| WatchError::StatePoisoned)?
                .remove(&path);
            return Err(map_watch_error(&error));
        }
        Ok(())
    }

    /// Receive the next invalidation or an uncertainty requiring reconciliation.
    #[must_use]
    pub fn next_signal(&self) -> Option<WatchSignal> {
        if self.backend_degraded.swap(false, Ordering::AcqRel) {
            return Some(WatchSignal::ReconcileAll(WatchUncertainty::BackendError));
        }
        if self.kernel_rescan.swap(false, Ordering::AcqRel) {
            return Some(WatchSignal::ReconcileAll(WatchUncertainty::KernelRescan));
        }
        if self.queue_saturated.swap(false, Ordering::AcqRel) {
            return Some(WatchSignal::ReconcileAll(
                WatchUncertainty::LocalQueueSaturated,
            ));
        }
        match self.receiver.try_recv() {
            Ok(signal) => Some(signal),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                Some(WatchSignal::ReconcileAll(WatchUncertainty::BackendError))
            }
        }
    }
}

/// Event-driven invalidation with no raw filesystem paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchSignal {
    /// Adapter target IDs affected by one or more coalesced paths.
    Targets(Vec<WatchTargetId>),
    /// Watch coverage is uncertain; all active targets need bounded reconciliation.
    ReconcileAll(WatchUncertainty),
}

/// Watch conditions that suspend destructive action until reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchUncertainty {
    /// Kernel inotify queue reported lost events.
    KernelRescan,
    /// Application callback queue was full and remained nonblocking.
    LocalQueueSaturated,
    /// Backend returned an error stripped of its concrete paths.
    BackendError,
}

/// Stable watcher setup failures without upstream paths or messages.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WatchError {
    /// Bounded callback queue must have capacity.
    #[error("Watch callback queue capacity must be positive")]
    InvalidQueueCapacity,
    /// Capability path policy rejected the directory.
    #[error("Watch target is outside its capability root")]
    Path(#[from] PathAccessError),
    /// Watch registry lock was poisoned.
    #[error("Watch target registry is unavailable")]
    StatePoisoned,
    /// Kernel or backend I/O setup failed.
    #[error("Filesystem watch backend is unavailable")]
    Backend,
    /// Configured path does not exist.
    #[error("Filesystem watch target is unavailable")]
    PathUnavailable,
    /// Host inotify watch limit was reached.
    #[error("Host inotify watch limit was reached; increase max_user_watches")]
    WatchLimit,
    /// Internal watcher configuration was rejected.
    #[error("Filesystem watch configuration is invalid")]
    InvalidConfiguration,
}

fn handle_event(
    event: Event,
    targets: &TargetMap,
    sender: &SyncSender<WatchSignal>,
    queue_saturated: &AtomicBool,
    backend_degraded: &AtomicBool,
    kernel_rescan: &AtomicBool,
) {
    if event.need_rescan() {
        kernel_rescan.store(true, Ordering::Release);
        return;
    }
    if matches!(&event.kind, EventKind::Access(_) | EventKind::Other) {
        return;
    }
    let Ok(targets) = targets.read() else {
        backend_degraded.store(true, Ordering::Release);
        return;
    };
    let mut affected = BTreeSet::new();
    for path in event.paths {
        if let Some(ids) = targets.get(&path) {
            affected.extend(ids.iter().copied());
        }
        if let Some(ids) = path.parent().and_then(|parent| targets.get(parent)) {
            affected.extend(ids.iter().copied());
        }
    }
    if affected.is_empty() {
        return;
    }
    match sender.try_send(WatchSignal::Targets(affected.into_iter().collect())) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => queue_saturated.store(true, Ordering::Release),
        Err(TrySendError::Disconnected(_)) => backend_degraded.store(true, Ordering::Release),
    }
}

fn map_watch_error(error: &notify::Error) -> WatchError {
    match &error.kind {
        ErrorKind::PathNotFound | ErrorKind::WatchNotFound => WatchError::PathUnavailable,
        ErrorKind::InvalidConfig(_) => WatchError::InvalidConfiguration,
        ErrorKind::MaxFilesWatch => WatchError::WatchLimit,
        ErrorKind::Generic(_) | ErrorKind::Io(_) => WatchError::Backend,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        path::PathBuf,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
            mpsc::sync_channel,
        },
    };

    use notify::{
        Event, EventKind,
        event::{AccessKind, Flag},
    };

    use super::{TargetMap, WatchTargetId, handle_event};

    #[test]
    fn kernel_rescan_flag_bypasses_the_bounded_event_queue() {
        let targets: TargetMap = Arc::new(RwLock::new(BTreeMap::default()));
        let (sender, receiver) = sync_channel(1);
        let queue_saturated = AtomicBool::new(false);
        let backend_degraded = AtomicBool::new(false);
        let kernel_rescan = AtomicBool::new(false);

        handle_event(
            Event::new(EventKind::Other).set_flag(Flag::Rescan),
            &targets,
            &sender,
            &queue_saturated,
            &backend_degraded,
            &kernel_rescan,
        );

        assert!(kernel_rescan.load(Ordering::Acquire));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn filesystem_reads_do_not_trigger_reconciliation_feedback() {
        let path = PathBuf::from("/watched/team/config.json");
        let target = WatchTargetId::new(1).expect("fixture target should be valid");
        let targets: TargetMap = Arc::new(RwLock::new(BTreeMap::from([(
            path.clone(),
            BTreeSet::from([target]),
        )])));
        let (sender, receiver) = sync_channel(1);
        let queue_saturated = AtomicBool::new(false);
        let backend_degraded = AtomicBool::new(false);
        let kernel_rescan = AtomicBool::new(false);

        handle_event(
            Event::new(EventKind::Access(AccessKind::Read)).add_path(path),
            &targets,
            &sender,
            &queue_saturated,
            &backend_degraded,
            &kernel_rescan,
        );

        assert!(receiver.try_recv().is_err());
        assert!(!queue_saturated.load(Ordering::Acquire));
        assert!(!backend_degraded.load(Ordering::Acquire));
        assert!(!kernel_rescan.load(Ordering::Acquire));
    }
}
