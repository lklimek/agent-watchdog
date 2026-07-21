use std::{
    collections::{BTreeSet, HashSet},
    fmt,
    path::PathBuf,
    sync::Arc,
};

#[cfg(test)]
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    sync::{
        RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(test)]
use serde::Deserialize;
use thiserror::Error;
#[cfg(test)]
use watchdog_domain::ProcessId;
use watchdog_domain::{
    DetailedState, DomainEvent, DomainEventKind, DurationMs, EvidenceTrust, MainSessionId,
    ProcessIdentity, RuntimeKind, SessionIdentity, SessionKind, TerminationActionOutcome,
    TerminationBlocker, TerminationCandidate, TerminationComponent, TerminationFacts,
    TerminationHealth, TerminationStage, TimePoint, WallTimeMs, assess_termination,
};
use watchdog_process::{
    LinuxProcessControl, LinuxProcessSampler, ProcessControl, ProcessReadError, ProcessSignal,
};
#[cfg(test)]
use watchdog_runtime::{CapabilityRoot, DirectoryScanner, ScanBudget};
use watchdog_runtime::{CoordinatorError, EventSequence};
use watchdog_store::{
    OutboxDestination, StoreError, TerminationAdvance, TerminationSafetyRecord,
    TerminationSagaRecord, WatchdogStore,
};

use crate::{AgentApi, HealthService};

const TEN_MINUTES_MS: u64 = 10 * 60_000;
const MAX_CHILD_SESSIONS: u32 = 1_000;
const MAX_RELATIONS_PER_ROOT: u32 = 1_000;
#[cfg(test)]
const COMPANION_SCAN_DEPTH: usize = 4;
#[cfg(test)]
const COMPANION_SCAN_ENTRIES: usize = 2_048;
#[cfg(test)]
const COMPANION_SCAN_PATH_BYTES: usize = 2 * 1_024 * 1_024;
#[cfg(test)]
const COMPANION_RPC_BYTES: usize = 64 * 1_024;
#[cfg(test)]
const COMPANION_RPC_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(test)]
const COMPANION_CONTENT_BYTES: usize = 8 * 1_024 * 1_024;
#[cfg(test)]
const COMPANION_CONTENT_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, Eq, PartialEq)]
enum FreshProcessEvidence {
    Present(ProcessIdentity),
    Exited,
    Unavailable,
}

trait FreshProcessSampler: fmt::Debug + Send + Sync {
    fn read_identity(&self, expected: &ProcessIdentity) -> FreshProcessEvidence;
}

impl FreshProcessSampler for LinuxProcessSampler {
    fn read_identity(&self, expected: &ProcessIdentity) -> FreshProcessEvidence {
        match LinuxProcessSampler::read_identity(self, expected.pid()) {
            Ok(identity) => FreshProcessEvidence::Present(identity),
            Err(ProcessReadError::Read { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                FreshProcessEvidence::Exited
            }
            Err(_) => FreshProcessEvidence::Unavailable,
        }
    }
}

/// Result of one bounded child-only termination reconciliation pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TerminationMonitorReport {
    evaluated_children: u32,
    changed_sagas: u32,
}

impl TerminationMonitorReport {
    pub(crate) const fn evaluated_children(self) -> u32 {
        self.evaluated_children
    }

    pub(crate) const fn changed_sagas(self) -> u32 {
        self.changed_sagas
    }
}

/// Periodic composition of fresh store, health, relation, and process facts.
#[derive(Clone)]
pub(crate) struct TerminationMonitor {
    store: WatchdogStore,
    clock: Arc<dyn watchdog_domain::Clock>,
    health: HealthService,
    sampler: Arc<dyn FreshProcessSampler>,
    engine: TerminationEngine,
    terminate_after_stalled: DurationMs,
}

impl fmt::Debug for TerminationMonitor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TerminationMonitor")
            .finish_non_exhaustive()
    }
}

impl TerminationMonitor {
    pub(crate) fn new(
        api: &AgentApi,
        store: WatchdogStore,
        clock: Arc<dyn watchdog_domain::Clock>,
        health: HealthService,
        config: TerminationConfig,
        terminate_after_stalled: DurationMs,
    ) -> Result<Self, TerminationEngineError> {
        let sampler =
            LinuxProcessSampler::new(32_768).map_err(|_| TerminationEngineError::ProcessSampler)?;
        let engine = TerminationEngine::new(
            store.clone(),
            api.event_sequence(),
            Arc::new(LinuxProcessControl::new()),
            Arc::new(NoGracefulCanceller),
            config,
        );
        Ok(Self::with_parts(
            store,
            clock,
            health,
            Arc::new(sampler),
            engine,
            terminate_after_stalled,
        ))
    }

    fn with_parts(
        store: WatchdogStore,
        clock: Arc<dyn watchdog_domain::Clock>,
        health: HealthService,
        sampler: Arc<dyn FreshProcessSampler>,
        engine: TerminationEngine,
        terminate_after_stalled: DurationMs,
    ) -> Self {
        Self {
            store,
            clock,
            health,
            sampler,
            engine,
            terminate_after_stalled,
        }
    }

    pub(crate) fn update_policy(
        &mut self,
        config: TerminationConfig,
        terminate_after_stalled: DurationMs,
    ) {
        self.engine.set_config(config);
        self.terminate_after_stalled = terminate_after_stalled;
    }

    pub(crate) async fn reconcile(
        &self,
    ) -> Result<TerminationMonitorReport, TerminationEngineError> {
        let children = self
            .store
            .sessions_by_kind(SessionKind::Child, MAX_CHILD_SESSIONS)
            .await?;
        let trustworthy = self.trustworthy_relations(&children).await?;
        let graceful_ready = self.engine.prepare_graceful().await;
        let now = self.clock.now();
        let mut report = TerminationMonitorReport::default();
        for child in children {
            let Some(stored) = self.store.snapshot(child.session).await? else {
                continue;
            };
            let Some(snapshot) = stored.reducer_snapshot() else {
                continue;
            };
            let watchdog_domain::SessionIdentity::Child(child_id) = child.session else {
                continue;
            };
            report.evaluated_children = report.evaluated_children.saturating_add(1);
            let fresh_evidence = snapshot
                .process_identity()
                .map_or(FreshProcessEvidence::Unavailable, |expected| {
                    self.sampler.read_identity(expected)
                });
            let fresh_process = match &fresh_evidence {
                FreshProcessEvidence::Present(identity)
                    if snapshot.process_identity() == Some(identity) =>
                {
                    Some(identity)
                }
                FreshProcessEvidence::Present(_)
                | FreshProcessEvidence::Exited
                | FreshProcessEvidence::Unavailable => None,
            };
            let process_identity_changed = matches!(
                &fresh_evidence,
                FreshProcessEvidence::Present(identity)
                    if snapshot.process_identity() != Some(identity)
            );
            let facts = TerminationFacts {
                snapshot,
                runtime: child.native.runtime(),
                trustworthy_relation: trustworthy.contains(&child_id),
                active_operation: matches!(
                    snapshot.state(),
                    DetailedState::Running | DetailedState::WaitingForTool
                ),
                fresh_process,
                health: self.termination_health(
                    child.native.runtime(),
                    child.session,
                    graceful_ready,
                ),
                now,
                terminate_after_stalled: self.terminate_after_stalled,
            };
            let status = self
                .engine
                .reconcile(TerminationContext {
                    candidate: TerminationCandidate::new(child_id),
                    root: child.root,
                    facts,
                    native_id: child.native.native_id(),
                    child_exited: fresh_evidence == FreshProcessEvidence::Exited,
                    process_identity_changed,
                })
                .await?;
            if !matches!(
                status,
                TerminationStatus::NoAction | TerminationStatus::Suspended
            ) {
                report.changed_sagas = report.changed_sagas.saturating_add(1);
            }
        }
        Ok(report)
    }

    async fn trustworthy_relations(
        &self,
        children: &[watchdog_store::StoredSessionRecord],
    ) -> Result<HashSet<watchdog_domain::ChildSessionId>, StoreError> {
        let mut roots = HashSet::new();
        for child in children {
            roots.insert(child.root);
        }
        let mut trustworthy = HashSet::new();
        for root in roots {
            for relation in self
                .store
                .relations_for_root(root, MAX_RELATIONS_PER_ROOT)
                .await?
            {
                if relation.selected
                    && relation.valid_until.is_none()
                    && relation.provenance.trust() != EvidenceTrust::Uncertain
                {
                    trustworthy.insert(relation.child);
                }
            }
        }
        Ok(trustworthy)
    }

    fn termination_health(
        &self,
        runtime: RuntimeKind,
        session: SessionIdentity,
        graceful_ready: bool,
    ) -> TerminationHealth {
        if self.health.destructive_automation_allowed(runtime, session)
            && (runtime != RuntimeKind::CodexCompanion || graceful_ready)
        {
            TerminationHealth::healthy()
        } else {
            TerminationHealth::healthy().with_unhealthy(TerminationComponent::Queue)
        }
    }
}

/// Validated automated-termination configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TerminationConfig {
    automation_enabled: bool,
    sigkill_enabled: bool,
    warning_grace: DurationMs,
    action_grace: DurationMs,
}

impl TerminationConfig {
    /// Construct a validated staged-termination policy.
    ///
    /// # Errors
    ///
    /// Returns [`TerminationConfigError`] when either grace duration is zero.
    pub const fn new(
        automation_enabled: bool,
        sigkill_enabled: bool,
        warning_grace: DurationMs,
        action_grace: DurationMs,
    ) -> Result<Self, TerminationConfigError> {
        if warning_grace.value() == 0 {
            return Err(TerminationConfigError::ZeroWarningGrace);
        }
        if action_grace.value() == 0 {
            return Err(TerminationConfigError::ZeroActionGrace);
        }
        Ok(Self {
            automation_enabled,
            sigkill_enabled,
            warning_grace,
            action_grace,
        })
    }
}

impl Default for TerminationConfig {
    fn default() -> Self {
        Self {
            automation_enabled: true,
            sigkill_enabled: true,
            warning_grace: DurationMs::new(TEN_MINUTES_MS),
            action_grace: DurationMs::new(TEN_MINUTES_MS),
        }
    }
}

/// Invalid termination-saga timing configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TerminationConfigError {
    /// Parent warning must precede escalation by a positive interval.
    #[error("Termination warning grace must be positive")]
    ZeroWarningGrace,
    /// Reconciliation must have a positive interval between destructive stages.
    #[error("Termination action grace must be positive")]
    ZeroActionGrace,
}

/// Runtime-qualified child identity supplied only to a supported cancellation API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifiedChild<'a> {
    /// Typed child identity.
    pub child: watchdog_domain::ChildSessionId,
    /// Runtime namespace selecting the cancellation adapter.
    pub runtime: RuntimeKind,
    /// Bounded native ID already retained by Watchdog.
    pub native_id: &'a str,
    /// Fresh exact process identity already admitted by the safety assessment.
    pub expected_process: &'a ProcessIdentity,
}

/// Whether the runtime has a supported native cancellation operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GracefulCancelSupport {
    /// The supported runtime API accepted the request.
    Requested,
    /// No supported native cancellation API exists for this child.
    Unsupported,
}

/// Bounded native cancellation failure without runtime payload content.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GracefulCancelError {
    /// The supported cancellation transport was unavailable.
    #[error("Runtime cancellation transport unavailable")]
    Unavailable,
    /// The runtime rejected the well-formed cancellation request.
    #[error("Runtime rejected graceful cancellation")]
    Rejected,
    /// Fresh runtime evidence no longer identifies the admitted child.
    #[error("Runtime cancellation identity changed")]
    EvidenceChanged,
}

/// Injectable supported-runtime cancellation dispatcher.
pub trait GracefulCanceller: fmt::Debug + Send + Sync {
    /// Refresh bounded native cancellation capabilities for one monitor pass.
    ///
    /// # Errors
    ///
    /// Returns a bounded failure when native state cannot be scanned completely.
    fn prepare(&self) -> Result<(), GracefulCancelError> {
        Ok(())
    }

    /// Replace reloadable capability roots before the next monitor pass.
    fn configure_roots(&self, _roots: &[PathBuf]) {}

    /// Request cancellation without editing native runtime state files.
    ///
    /// # Errors
    ///
    /// Returns a bounded failure class without native response content.
    fn request_cancel(
        &self,
        child: VerifiedChild<'_>,
    ) -> Result<GracefulCancelSupport, GracefulCancelError>;
}

/// Conservative dispatcher used when no supported native API is configured.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoGracefulCanceller;

impl GracefulCanceller for NoGracefulCanceller {
    fn request_cancel(
        &self,
        _child: VerifiedChild<'_>,
    ) -> Result<GracefulCancelSupport, GracefulCancelError> {
        Ok(GracefulCancelSupport::Unsupported)
    }
}

#[cfg(test)]
#[derive(Debug)]
struct CompanionGracefulCanceller {
    roots: RwLock<Vec<PathBuf>>,
    targets: RwLock<BTreeMap<String, CompanionTarget>>,
    prepared: AtomicBool,
    circuit_open: AtomicBool,
}

#[cfg(test)]
#[derive(Clone, Debug)]
enum CompanionTarget {
    Active {
        job_pid: u32,
        thread_id: String,
        turn_id: String,
        broker: Option<VerifiedCompanionBroker>,
    },
    Inactive,
    Unsupported,
}

#[cfg(test)]
#[derive(Clone, Debug, Deserialize)]
struct CompanionBrokerState {
    endpoint: String,
    pid: u32,
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct VerifiedCompanionBroker {
    state: CompanionBrokerState,
    process: ProcessIdentity,
}

#[cfg(test)]
#[derive(Debug)]
struct CapabilityReadBudget {
    started: Instant,
    remaining_bytes: usize,
}

#[cfg(test)]
impl CapabilityReadBudget {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            remaining_bytes: COMPANION_CONTENT_BYTES,
        }
    }

    fn charge(&mut self, bytes: usize) -> Result<(), GracefulCancelError> {
        if self.started.elapsed() >= COMPANION_CONTENT_TIMEOUT || bytes > self.remaining_bytes {
            return Err(GracefulCancelError::Unavailable);
        }
        self.remaining_bytes -= bytes;
        Ok(())
    }
}

#[cfg(test)]
impl CompanionGracefulCanceller {
    fn new(roots: Vec<PathBuf>) -> Self {
        Self {
            roots: RwLock::new(roots),
            targets: RwLock::new(BTreeMap::new()),
            prepared: AtomicBool::new(false),
            circuit_open: AtomicBool::new(false),
        }
    }

    fn scan_targets(&self) -> Result<BTreeMap<String, CompanionTarget>, GracefulCancelError> {
        let parser =
            watchdog_companion::CompanionParser::new(watchdog_companion::TESTED_COMPANION_VERSION)
                .map_err(|_| GracefulCancelError::Rejected)?;
        let budget = ScanBudget::new(
            COMPANION_SCAN_DEPTH,
            COMPANION_SCAN_ENTRIES,
            COMPANION_SCAN_PATH_BYTES,
        )
        .map_err(|_| GracefulCancelError::Rejected)?;
        let scanner = DirectoryScanner::new(budget);
        let roots = self
            .roots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut targets = BTreeMap::new();
        let mut content_budget = CapabilityReadBudget::new();
        let process_sampler =
            LinuxProcessSampler::new(32_768).map_err(|_| GracefulCancelError::Unavailable)?;
        for configured_root in roots {
            let root = CapabilityRoot::new(configured_root)
                .map_err(|_| GracefulCancelError::Unavailable)?;
            let scan = scanner
                .scan(&root, Path::new("."))
                .map_err(|_| GracefulCancelError::Unavailable)?;
            if scan.uncertainty().is_some() {
                return Err(GracefulCancelError::Unavailable);
            }
            let directories =
                std::iter::once(root.path().to_owned()).chain(scan.directories().iter().cloned());
            for directory in directories {
                let relative = directory
                    .strip_prefix(root.path())
                    .map_err(|_| GracefulCancelError::Rejected)?;
                let state_path = directory.join("state.json");
                let state_present = scan.files().contains(&state_path);
                let Some(summary) = read_capability_file(
                    &root,
                    &relative.join("state.json"),
                    watchdog_companion::MAX_SUMMARY_BYTES,
                    state_present,
                    &mut content_budget,
                )?
                else {
                    continue;
                };
                let snapshot = parser
                    .parse_summary(&summary)
                    .map_err(|_| GracefulCancelError::Rejected)?;
                content_budget.charge(0)?;
                let broker_path = directory.join("broker.json");
                let broker = read_companion_broker(
                    &root,
                    relative,
                    scan.files().contains(&broker_path),
                    &mut content_budget,
                    process_sampler,
                )?;
                for job in snapshot.jobs() {
                    let native_id = job.subject().native_id().to_owned();
                    let target = if matches!(
                        job.state(),
                        DetailedState::Completed
                            | DetailedState::Failed
                            | DetailedState::Cancelled
                            | DetailedState::Disappeared
                    ) {
                        CompanionTarget::Inactive
                    } else {
                        let Some(cancellation) = job.graceful_cancellation() else {
                            if targets
                                .insert(native_id, CompanionTarget::Unsupported)
                                .is_some()
                            {
                                return Err(GracefulCancelError::Rejected);
                            }
                            continue;
                        };
                        let job_pid = job
                            .pid()
                            .filter(|pid| *pid > 0)
                            .ok_or(GracefulCancelError::EvidenceChanged)?;
                        CompanionTarget::Active {
                            job_pid,
                            thread_id: cancellation.thread_id().to_owned(),
                            turn_id: cancellation.turn_id().to_owned(),
                            broker: broker.clone(),
                        }
                    };
                    if targets.insert(native_id, target).is_some() {
                        return Err(GracefulCancelError::Rejected);
                    }
                }
            }
        }
        Ok(targets)
    }
}

#[cfg(test)]
impl GracefulCanceller for CompanionGracefulCanceller {
    fn prepare(&self) -> Result<(), GracefulCancelError> {
        self.prepared.store(false, Ordering::Release);
        self.circuit_open.store(false, Ordering::Release);
        let targets = self.scan_targets()?;
        *self
            .targets
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = targets;
        self.prepared.store(true, Ordering::Release);
        Ok(())
    }

    fn configure_roots(&self, roots: &[PathBuf]) {
        *self
            .roots
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = roots.to_vec();
        self.prepared.store(false, Ordering::Release);
    }

    fn request_cancel(
        &self,
        child: VerifiedChild<'_>,
    ) -> Result<GracefulCancelSupport, GracefulCancelError> {
        if child.runtime != RuntimeKind::CodexCompanion {
            return Ok(GracefulCancelSupport::Unsupported);
        }
        if !self.prepared.load(Ordering::Acquire) {
            return Err(GracefulCancelError::Unavailable);
        }
        if self.circuit_open.load(Ordering::Acquire) {
            return Err(GracefulCancelError::Unavailable);
        }
        let Some(target) = self
            .targets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(child.native_id)
            .cloned()
        else {
            return Err(GracefulCancelError::EvidenceChanged);
        };
        let CompanionTarget::Active {
            job_pid,
            thread_id,
            turn_id,
            broker,
        } = target
        else {
            return match target {
                CompanionTarget::Inactive => Err(GracefulCancelError::EvidenceChanged),
                CompanionTarget::Unsupported => Ok(GracefulCancelSupport::Unsupported),
                CompanionTarget::Active { .. } => unreachable!("active target was destructured"),
            };
        };
        if child.expected_process.pid().value() != job_pid {
            return Err(GracefulCancelError::EvidenceChanged);
        }
        let Some(broker) = broker else {
            return Ok(GracefulCancelSupport::Unsupported);
        };
        if let Err(error) = interrupt_companion_turn(&broker, &thread_id, &turn_id) {
            self.circuit_open.store(true, Ordering::Release);
            return Err(error);
        }
        Ok(GracefulCancelSupport::Requested)
    }
}

#[cfg(test)]
fn read_companion_broker(
    root: &CapabilityRoot,
    relative: &Path,
    present: bool,
    budget: &mut CapabilityReadBudget,
    process_sampler: LinuxProcessSampler,
) -> Result<Option<VerifiedCompanionBroker>, GracefulCancelError> {
    let Some(broker) = read_capability_file(
        root,
        &relative.join("broker.json"),
        COMPANION_RPC_BYTES,
        present,
        budget,
    )?
    else {
        return Ok(None);
    };
    let broker: CompanionBrokerState =
        serde_json::from_slice(&broker).map_err(|_| GracefulCancelError::Rejected)?;
    if broker.pid == 0 {
        return Err(GracefulCancelError::Rejected);
    }
    let pid = ProcessId::new(broker.pid).map_err(|_| GracefulCancelError::Rejected)?;
    let Ok(process) = process_sampler.read_identity(pid) else {
        return Ok(None);
    };
    Ok(Some(VerifiedCompanionBroker {
        state: broker,
        process,
    }))
}

#[cfg(test)]
fn read_capability_file(
    root: &CapabilityRoot,
    relative: &Path,
    maximum: usize,
    present: bool,
    budget: &mut CapabilityReadBudget,
) -> Result<Option<Vec<u8>>, GracefulCancelError> {
    if !present {
        return Ok(None);
    }
    let file = root
        .open_file(relative)
        .map_err(|_| GracefulCancelError::Unavailable)?;
    let length = file
        .metadata()
        .map_err(|_| GracefulCancelError::Unavailable)?
        .len();
    if length > maximum as u64 {
        return Err(GracefulCancelError::Rejected);
    }
    let length = usize::try_from(length).map_err(|_| GracefulCancelError::Rejected)?;
    budget.charge(length)?;
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GracefulCancelError::Unavailable)?;
    budget.charge(0)?;
    if bytes.len() > maximum {
        return Err(GracefulCancelError::Rejected);
    }
    Ok(Some(bytes))
}

#[cfg(test)]
fn connect_companion_broker(
    broker: &VerifiedCompanionBroker,
) -> Result<UnixStream, GracefulCancelError> {
    let endpoint = broker
        .state
        .endpoint
        .strip_prefix("unix:")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or(GracefulCancelError::Rejected)?;
    if endpoint.file_name().and_then(|name| name.to_str()) != Some("broker.sock")
        || !endpoint
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("cxc-"))
    {
        return Err(GracefulCancelError::Rejected);
    }
    let process_root = PathBuf::from(format!("/proc/{}/root", broker.state.pid))
        .join(endpoint.strip_prefix("/").unwrap_or(&endpoint));
    for path in [endpoint, process_root] {
        let Ok(stream) = UnixStream::connect(path) else {
            continue;
        };
        let peer = rustix::net::sockopt::socket_peercred(&stream)
            .map_err(|_| GracefulCancelError::Unavailable)?;
        if u32::try_from(peer.pid.as_raw_pid()).ok() != Some(broker.state.pid) {
            return Err(GracefulCancelError::Rejected);
        }
        let pid = ProcessId::new(broker.state.pid).map_err(|_| GracefulCancelError::Rejected)?;
        let fresh_process = LinuxProcessSampler::new(32_768)
            .map_err(|_| GracefulCancelError::Unavailable)?
            .read_identity(pid)
            .map_err(|_| GracefulCancelError::Unavailable)?;
        if fresh_process != broker.process {
            return Err(GracefulCancelError::EvidenceChanged);
        }
        return Ok(stream);
    }
    Err(GracefulCancelError::Unavailable)
}

#[cfg(test)]
fn interrupt_companion_turn(
    broker: &VerifiedCompanionBroker,
    thread_id: &str,
    turn_id: &str,
) -> Result<(), GracefulCancelError> {
    let mut stream = connect_companion_broker(broker)?;
    stream
        .set_read_timeout(Some(COMPANION_RPC_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(COMPANION_RPC_TIMEOUT)))
        .map_err(|_| GracefulCancelError::Unavailable)?;
    stream
        .write_all(b"{\"id\":1,\"method\":\"initialize\",\"params\":{}}\n")
        .map_err(|_| GracefulCancelError::Unavailable)?;
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|_| GracefulCancelError::Unavailable)?,
    );
    read_rpc_response(&mut reader, 1)?;
    let request = serde_json::json!({
        "id": 2,
        "method": "turn/interrupt",
        "params": {"threadId": thread_id, "turnId": turn_id},
    });
    serde_json::to_writer(&mut stream, &request).map_err(|_| GracefulCancelError::Rejected)?;
    stream
        .write_all(b"\n")
        .map_err(|_| GracefulCancelError::Unavailable)?;
    read_rpc_response(&mut reader, 2)
}

#[cfg(test)]
fn read_rpc_response(
    reader: &mut BufReader<UnixStream>,
    expected_id: u64,
) -> Result<(), GracefulCancelError> {
    let mut response = String::new();
    reader
        .take(u64::try_from(COMPANION_RPC_BYTES).unwrap_or(u64::MAX))
        .read_line(&mut response)
        .map_err(|_| GracefulCancelError::Unavailable)?;
    let response: serde_json::Value =
        serde_json::from_str(&response).map_err(|_| GracefulCancelError::Rejected)?;
    if response.get("id").and_then(serde_json::Value::as_u64) != Some(expected_id)
        || response.get("error").is_some()
    {
        return Err(GracefulCancelError::Rejected);
    }
    Ok(())
}

/// One fresh reconciliation input to the restartable termination saga.
#[derive(Clone, Copy, Debug)]
pub struct TerminationContext<'a> {
    /// Typed child-only candidate.
    pub candidate: TerminationCandidate,
    /// Owning main-session tree.
    pub root: MainSessionId,
    /// Runtime and normalized safety facts.
    pub facts: TerminationFacts<'a>,
    /// Runtime-native session identity for supported cancellation APIs.
    pub native_id: &'a str,
    /// Fresh native/process evidence proves the child has exited.
    pub child_exited: bool,
    /// Fresh evidence found the PID occupied by a different exact process.
    pub process_identity_changed: bool,
}

/// Observable result of one saga reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminationStatus {
    /// No eligible or existing saga required mutation.
    NoAction,
    /// Warning grace began.
    Started,
    /// One later stage committed.
    Advanced,
    /// A transient safety condition suspended all side effects.
    Suspended,
    /// Contrary evidence or a safety failure permanently stopped this saga.
    Aborted,
    /// Fresh evidence proved the child exited.
    Completed,
}

/// Durable child-only termination saga coordinator.
#[derive(Clone)]
pub struct TerminationEngine {
    store: WatchdogStore,
    event_sequence: Arc<EventSequence>,
    process: Arc<dyn ProcessControl>,
    graceful: Arc<dyn GracefulCanceller>,
    config: TerminationConfig,
}

impl fmt::Debug for TerminationEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TerminationEngine")
            .field("process", &self.process)
            .field("graceful", &self.graceful)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl TerminationEngine {
    /// Construct the side-effect boundary from injected verified operations.
    #[must_use]
    pub fn new(
        store: WatchdogStore,
        event_sequence: Arc<EventSequence>,
        process: Arc<dyn ProcessControl>,
        graceful: Arc<dyn GracefulCanceller>,
        config: TerminationConfig,
    ) -> Self {
        Self {
            store,
            event_sequence,
            process,
            graceful,
            config,
        }
    }

    const fn set_config(&mut self, config: TerminationConfig) {
        self.config = config;
    }

    async fn prepare_graceful(&self) -> bool {
        let graceful = Arc::clone(&self.graceful);
        tokio::task::spawn_blocking(move || graceful.prepare())
            .await
            .is_ok_and(|result| result.is_ok())
    }

    async fn request_graceful(
        &self,
        child: watchdog_domain::ChildSessionId,
        runtime: RuntimeKind,
        native_id: &str,
        expected_process: &ProcessIdentity,
    ) -> Result<GracefulCancelSupport, GracefulCancelError> {
        let graceful = Arc::clone(&self.graceful);
        let native_id = native_id.to_owned();
        let expected_process = expected_process.clone();
        tokio::task::spawn_blocking(move || {
            graceful.request_cancel(VerifiedChild {
                child,
                runtime,
                native_id: &native_id,
                expected_process: &expected_process,
            })
        })
        .await
        .map_err(|_| GracefulCancelError::Unavailable)?
    }

    /// Reconcile one child saga without ever accepting a main-session identity.
    ///
    /// Every side effect follows a fresh pure-gate evaluation. Saga, event, and
    /// delivery rows commit atomically after the external action succeeds or a
    /// bounded outcome is known.
    ///
    /// # Errors
    ///
    /// Returns [`TerminationEngineError`] for identity, time, event allocation,
    /// or transactional persistence failure. Process-control failures are
    /// converted into a durable aborted diagnostic rather than retried blindly.
    pub async fn reconcile(
        &self,
        context: TerminationContext<'_>,
    ) -> Result<TerminationStatus, TerminationEngineError> {
        if context.facts.snapshot.root() != context.root {
            return Err(TerminationEngineError::IdentityMismatch);
        }
        let child = context.candidate.session_id();
        let existing = self.store.termination_saga(child).await?;
        if matches!(
            existing.as_ref().map(|saga| saga.stage),
            Some(TerminationStage::Completed | TerminationStage::Aborted)
        ) {
            return Ok(TerminationStatus::NoAction);
        }

        if let Some(status) = self.finish_if_exited(&context, existing.as_ref()).await? {
            return Ok(status);
        }

        let assessment = assess_termination(context.candidate, context.facts);
        if !self.config.automation_enabled {
            return self
                .disable(&context, existing.as_ref(), assessment.blockers())
                .await;
        }

        let Some(existing) = existing else {
            return self.start(&context, assessment.eligible()).await;
        };

        if let Some(status) = self.identity_changed(&context, &existing).await? {
            return Ok(status);
        }
        if !assessment.eligible() {
            return self
                .handle_blockers(&context, &existing, assessment.blockers())
                .await;
        }
        let legacy_sigkill_opt_out = existing.stage == TerminationStage::Sigterm
            && existing.next_action_at.is_none()
            && existing.last_outcome == Some(TerminationActionOutcome::AutomationDisabled)
            && self.config.sigkill_enabled;
        if !legacy_sigkill_opt_out
            && existing
                .next_action_at
                .is_none_or(|deadline| context.facts.now.wall_time() < deadline)
        {
            return Ok(TerminationStatus::NoAction);
        }
        self.advance_due(&context, &existing).await
    }

    async fn advance_due(
        &self,
        context: &TerminationContext<'_>,
        existing: &TerminationSagaRecord,
    ) -> Result<TerminationStatus, TerminationEngineError> {
        let child = context.candidate.session_id();
        match existing.stage {
            TerminationStage::WarningGrace => {
                let Some(expected_process) = context.facts.fresh_process else {
                    return Ok(TerminationStatus::Suspended);
                };
                let result = self
                    .request_graceful(
                        child,
                        context.facts.runtime,
                        context.native_id,
                        expected_process,
                    )
                    .await;
                if result == Err(GracefulCancelError::EvidenceChanged) {
                    self.persist(
                        context,
                        existing,
                        TerminationStage::Aborted,
                        None,
                        TerminationActionOutcome::SafetyAborted,
                        safety(
                            context,
                            Some(BTreeSet::from([TerminationBlocker::ProcessIdentityChanged])),
                        ),
                    )
                    .await?;
                    return Ok(TerminationStatus::Aborted);
                }
                let outcome = match result {
                    Ok(GracefulCancelSupport::Requested) => {
                        TerminationActionOutcome::GracefulRequested
                    }
                    Ok(GracefulCancelSupport::Unsupported) => {
                        TerminationActionOutcome::GracefulUnsupported
                    }
                    Err(_) => TerminationActionOutcome::GracefulFailed,
                };
                self.persist(
                    context,
                    existing,
                    TerminationStage::GracefulCancel,
                    Some(add_duration(context.facts.now, self.config.action_grace)?),
                    outcome,
                    safety(context, None),
                )
                .await?;
                Ok(TerminationStatus::Advanced)
            }
            TerminationStage::GracefulCancel => {
                self.signal(
                    context,
                    existing,
                    ProcessSignal::Terminate,
                    TerminationStage::Sigterm,
                )
                .await
            }
            TerminationStage::Sigterm if self.config.sigkill_enabled => {
                self.signal(
                    context,
                    existing,
                    ProcessSignal::Kill,
                    TerminationStage::Sigkill,
                )
                .await
            }
            TerminationStage::Sigterm => {
                if existing.last_outcome == Some(TerminationActionOutcome::SigkillDisabled) {
                    return Ok(TerminationStatus::NoAction);
                }
                self.persist(
                    context,
                    existing,
                    TerminationStage::Sigterm,
                    existing.next_action_at,
                    TerminationActionOutcome::SigkillDisabled,
                    safety(context, None),
                )
                .await?;
                Ok(TerminationStatus::Advanced)
            }
            TerminationStage::Sigkill | TerminationStage::Completed | TerminationStage::Aborted => {
                Ok(TerminationStatus::NoAction)
            }
        }
    }

    async fn finish_if_exited(
        &self,
        context: &TerminationContext<'_>,
        existing: Option<&TerminationSagaRecord>,
    ) -> Result<Option<TerminationStatus>, TerminationEngineError> {
        if !context.child_exited
            && !matches!(
                context.facts.snapshot.state(),
                DetailedState::Completed | DetailedState::Cancelled
            )
        {
            return Ok(None);
        }
        let Some(saga) = existing else {
            return Ok(Some(TerminationStatus::NoAction));
        };
        self.persist(
            context,
            saga,
            TerminationStage::Completed,
            None,
            TerminationActionOutcome::ChildExited,
            safety(context, None),
        )
        .await?;
        Ok(Some(TerminationStatus::Completed))
    }

    async fn disable(
        &self,
        context: &TerminationContext<'_>,
        existing: Option<&TerminationSagaRecord>,
        blockers: &BTreeSet<TerminationBlocker>,
    ) -> Result<TerminationStatus, TerminationEngineError> {
        let Some(saga) = existing else {
            return Ok(TerminationStatus::Suspended);
        };
        self.persist(
            context,
            saga,
            TerminationStage::Aborted,
            None,
            TerminationActionOutcome::AutomationDisabled,
            safety(context, Some(blockers.clone())),
        )
        .await?;
        Ok(TerminationStatus::Aborted)
    }

    async fn start(
        &self,
        context: &TerminationContext<'_>,
        eligible: bool,
    ) -> Result<TerminationStatus, TerminationEngineError> {
        if !eligible {
            return Ok(TerminationStatus::Suspended);
        }
        let initial = TerminationSagaRecord {
            child: context.candidate.session_id(),
            stage: TerminationStage::WarningGrace,
            revision: 0,
            next_action_at: None,
            safety: safety(context, None),
            last_outcome: None,
        };
        self.persist(
            context,
            &initial,
            TerminationStage::WarningGrace,
            Some(add_duration(context.facts.now, self.config.warning_grace)?),
            TerminationActionOutcome::WarningScheduled,
            safety(context, None),
        )
        .await?;
        Ok(TerminationStatus::Started)
    }

    async fn identity_changed(
        &self,
        context: &TerminationContext<'_>,
        existing: &TerminationSagaRecord,
    ) -> Result<Option<TerminationStatus>, TerminationEngineError> {
        if !context.process_identity_changed
            && existing.safety.process.as_ref() == context.facts.fresh_process
        {
            return Ok(None);
        }
        if !context.process_identity_changed && context.facts.fresh_process.is_none() {
            return Ok(Some(TerminationStatus::Suspended));
        }
        self.persist(
            context,
            existing,
            TerminationStage::Aborted,
            None,
            TerminationActionOutcome::SafetyAborted,
            safety(
                context,
                Some(BTreeSet::from([TerminationBlocker::ProcessIdentityChanged])),
            ),
        )
        .await?;
        Ok(Some(TerminationStatus::Aborted))
    }

    async fn handle_blockers(
        &self,
        context: &TerminationContext<'_>,
        existing: &TerminationSagaRecord,
        blockers: &BTreeSet<TerminationBlocker>,
    ) -> Result<TerminationStatus, TerminationEngineError> {
        if transient_only(blockers) {
            return Ok(TerminationStatus::Suspended);
        }
        self.persist(
            context,
            existing,
            TerminationStage::Aborted,
            None,
            TerminationActionOutcome::SafetyAborted,
            safety(context, Some(blockers.clone())),
        )
        .await?;
        Ok(TerminationStatus::Aborted)
    }

    async fn signal(
        &self,
        context: &TerminationContext<'_>,
        existing: &TerminationSagaRecord,
        signal: ProcessSignal,
        stage: TerminationStage,
    ) -> Result<TerminationStatus, TerminationEngineError> {
        let Some(expected) = context.facts.fresh_process else {
            return Ok(TerminationStatus::Suspended);
        };
        let result = self
            .process
            .open_verified(expected)
            .and_then(|handle| handle.signal(signal));
        if result.is_err() {
            let mut blockers = BTreeSet::from([TerminationBlocker::ProcessControlFailure]);
            blockers.extend(assess_termination(context.candidate, context.facts).blockers());
            self.persist(
                context,
                existing,
                TerminationStage::Aborted,
                None,
                TerminationActionOutcome::SafetyAborted,
                safety(context, Some(blockers)),
            )
            .await?;
            return Ok(TerminationStatus::Aborted);
        }
        let next = (stage == TerminationStage::Sigterm)
            .then(|| add_duration(context.facts.now, self.config.action_grace))
            .transpose()?;
        self.persist(
            context,
            existing,
            stage,
            next,
            TerminationActionOutcome::SignalSent,
            safety(context, None),
        )
        .await?;
        Ok(TerminationStatus::Advanced)
    }

    async fn persist(
        &self,
        context: &TerminationContext<'_>,
        existing: &TerminationSagaRecord,
        stage: TerminationStage,
        next_action_at: Option<WallTimeMs>,
        outcome: TerminationActionOutcome,
        safety: TerminationSafetyRecord,
    ) -> Result<(), TerminationEngineError> {
        let revision = existing
            .revision
            .checked_add(1)
            .ok_or(TerminationEngineError::RevisionExhausted)?;
        let saga = TerminationSagaRecord {
            child: context.candidate.session_id(),
            stage,
            revision,
            next_action_at,
            safety,
            last_outcome: Some(outcome),
        };
        let event = DomainEvent::new(
            self.event_sequence.allocate()?,
            context.root,
            watchdog_domain::SessionIdentity::Child(context.candidate.session_id()),
            context.facts.now.wall_time(),
            DomainEventKind::TerminationChanged { stage, outcome },
        );
        let advance = TerminationAdvance::new(saga, event, destinations(stage));
        self.store.apply_termination_advance(&advance).await?;
        Ok(())
    }
}

/// Saga coordination failures; no native payload or credential content is retained.
#[derive(Debug, Error)]
pub enum TerminationEngineError {
    /// Context root does not match the normalized snapshot.
    #[error("Termination context identity mismatch")]
    IdentityMismatch,
    /// Wall deadline cannot be represented safely.
    #[error("Termination deadline exceeds wall-clock range")]
    TimeOverflow,
    /// No further monotonic saga revision exists.
    #[error("Termination saga revision exhausted")]
    RevisionExhausted,
    /// Durable event allocation failed.
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    /// Atomic saga/event persistence failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Linux process sampler could not be initialized.
    #[error("Termination process sampler initialization failed")]
    ProcessSampler,
}

fn safety(
    context: &TerminationContext<'_>,
    override_blockers: Option<BTreeSet<TerminationBlocker>>,
) -> TerminationSafetyRecord {
    let assessment = assess_termination(context.candidate, context.facts);
    TerminationSafetyRecord {
        passed_gates: assessment.passed().clone(),
        blockers: override_blockers.unwrap_or_else(|| assessment.blockers().clone()),
        process: context.facts.fresh_process.cloned(),
    }
}

fn transient_only(blockers: &BTreeSet<TerminationBlocker>) -> bool {
    !blockers.is_empty()
        && blockers.iter().all(|blocker| {
            matches!(
                blocker,
                TerminationBlocker::ComponentUnhealthy
                    | TerminationBlocker::MissingProcess
                    | TerminationBlocker::ReconciliationRequired
            )
        })
}

fn add_duration(
    now: TimePoint,
    duration: DurationMs,
) -> Result<WallTimeMs, TerminationEngineError> {
    let duration =
        i64::try_from(duration.value()).map_err(|_| TerminationEngineError::TimeOverflow)?;
    now.wall_time()
        .value()
        .checked_add(duration)
        .map(WallTimeMs::new)
        .ok_or(TerminationEngineError::TimeOverflow)
}

fn destinations(stage: TerminationStage) -> Vec<OutboxDestination> {
    let mut destinations = vec![OutboxDestination::ParentInbox, OutboxDestination::Sse];
    if stage == TerminationStage::WarningGrace {
        destinations.extend([
            OutboxDestination::Browser,
            OutboxDestination::HomeAssistant,
            OutboxDestination::Webhook,
        ]);
    }
    destinations
}

#[cfg(test)]
mod monitor_tests {
    use std::{
        fs,
        io::{BufRead as _, BufReader, Write as _},
        os::unix::net::UnixListener,
        thread,
    };

    use watchdog_domain::{
        AdapterIdentity, BoundedText, Clock as _, NativeSessionKey, ObservationEnvelope,
        ObservationId, ObservationPayload, ObservationSource, ProcessId, ReducerPolicy,
        SessionIdentity, TimePoint,
    };
    use watchdog_runtime::{ComponentId, ComponentStatus, HealthScope};
    use watchdog_testkit::FakeClock;

    use super::*;
    use crate::{RegisterSession, TransportKey};

    #[test]
    fn production_graceful_fallback_is_explicitly_unsupported() {
        let process = companion_process(42);
        assert_eq!(
            NoGracefulCanceller.request_cancel(companion_child(&process)),
            Ok(GracefulCancelSupport::Unsupported)
        );
    }

    #[test]
    fn companion_graceful_canceller_sends_native_turn_interrupt() {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        let workspace = directory.path().join("workspace");
        fs::create_dir(&workspace).expect("workspace should exist");
        fs::write(
            workspace.join("state.json"),
            br#"{"version":1,"jobs":[{"id":"companion-job","workspaceRoot":"/work/tree","sessionId":"claude-main","status":"running","phase":"running","pid":42,"threadId":"thread-1","turnId":"turn-1","updatedAt":"now"}]}"#,
        )
        .expect("summary should be written");
        let broker_directory = directory.path().join("cxc-session");
        fs::create_dir(&broker_directory).expect("broker directory should exist");
        let socket_path = broker_directory.join("broker.sock");
        let listener = UnixListener::bind(&socket_path).expect("broker socket should bind");
        fs::write(
            workspace.join("broker.json"),
            serde_json::to_vec(&serde_json::json!({
                "endpoint": format!("unix:{}", socket_path.display()),
                "pid": std::process::id()
            }))
            .expect("broker state should serialize"),
        )
        .expect("broker state should be written");
        let broker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("canceller should connect");
            let mut reader = BufReader::new(stream.try_clone().expect("stream should clone"));
            let mut initialize = String::new();
            reader
                .read_line(&mut initialize)
                .expect("initialize should arrive");
            stream
                .write_all(b"{\"id\":1,\"result\":{}}\n")
                .expect("initialize response should send");
            let mut interrupt = String::new();
            reader
                .read_line(&mut interrupt)
                .expect("interrupt should arrive");
            stream
                .write_all(b"{\"id\":2,\"result\":{}}\n")
                .expect("interrupt response should send");
            serde_json::from_str::<serde_json::Value>(&interrupt).expect("interrupt should be JSON")
        });

        let canceller = CompanionGracefulCanceller::new(vec![directory.path().to_owned()]);
        canceller
            .prepare()
            .expect("complete native state should prepare");
        let expected_process = ProcessIdentity::new(
            ProcessId::new(42).expect("fixture PID should validate"),
            7,
            BoundedText::new("executable", "/usr/bin/codex").expect("executable should validate"),
        );
        assert_eq!(
            canceller.request_cancel(VerifiedChild {
                child: watchdog_domain::ChildSessionId::from(
                    watchdog_domain::SessionId::from_native(
                        &NativeSessionKey::new(RuntimeKind::CodexCompanion, "companion-job")
                            .expect("native identity should validate"),
                    ),
                ),
                runtime: RuntimeKind::CodexCompanion,
                native_id: "companion-job",
                expected_process: &expected_process,
            }),
            Ok(GracefulCancelSupport::Requested)
        );
        let interrupt = broker.join().expect("broker should finish");
        assert_eq!(interrupt["method"], "turn/interrupt");
        assert_eq!(interrupt["params"]["threadId"], "thread-1");
        assert_eq!(interrupt["params"]["turnId"], "turn-1");
    }

    #[test]
    fn companion_cancellation_rejects_incomplete_malformed_scans() {
        let malformed = tempfile::tempdir().expect("malformed fixture should exist");
        fs::write(malformed.path().join("state.json"), b"not-json")
            .expect("malformed summary should be written");
        assert_eq!(
            CompanionGracefulCanceller::new(vec![malformed.path().to_owned()]).prepare(),
            Err(GracefulCancelError::Rejected)
        );
    }

    #[test]
    fn companion_broker_rejects_non_companion_socket_names() {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        let socket_path = directory.path().join("arbitrary.sock");
        let _listener = UnixListener::bind(&socket_path).expect("fixture socket should bind");
        assert!(matches!(
            connect_companion_broker(&verified_broker(format!("unix:{}", socket_path.display()))),
            Err(GracefulCancelError::Rejected)
        ));
    }

    #[test]
    fn companion_broker_rejects_a_peer_with_the_wrong_pid() {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        let broker_directory = directory.path().join("cxc-session");
        fs::create_dir(&broker_directory).expect("broker directory should exist");
        let socket_path = broker_directory.join("broker.sock");
        let _listener = UnixListener::bind(&socket_path).expect("fixture socket should bind");
        assert!(matches!(
            connect_companion_broker(&VerifiedCompanionBroker {
                state: CompanionBrokerState {
                    endpoint: format!("unix:{}", socket_path.display()),
                    pid: std::process::id().saturating_add(1),
                },
                process: companion_process(std::process::id()),
            }),
            Err(GracefulCancelError::Rejected | GracefulCancelError::Unavailable)
        ));
    }

    #[test]
    fn companion_broker_rejects_a_reused_process_identity() {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        let broker_directory = directory.path().join("cxc-session");
        fs::create_dir(&broker_directory).expect("broker directory should exist");
        let socket_path = broker_directory.join("broker.sock");
        let listener = UnixListener::bind(&socket_path).expect("fixture socket should bind");
        let acceptor = thread::spawn(move || listener.accept().expect("connection should arrive"));
        let broker = VerifiedCompanionBroker {
            state: CompanionBrokerState {
                endpoint: format!("unix:{}", socket_path.display()),
                pid: std::process::id(),
            },
            process: companion_process(std::process::id()),
        };

        assert_eq!(
            connect_companion_broker(&broker)
                .expect_err("a stale exact broker identity must be rejected"),
            GracefulCancelError::EvidenceChanged
        );
        acceptor.join().expect("acceptor should finish");
    }

    fn verified_broker(endpoint: String) -> VerifiedCompanionBroker {
        let pid = ProcessId::new(std::process::id()).expect("test PID should validate");
        let process = LinuxProcessSampler::new(1)
            .expect("sampler should initialize")
            .read_identity(pid)
            .expect("test process identity should be readable");
        VerifiedCompanionBroker {
            state: CompanionBrokerState {
                endpoint,
                pid: std::process::id(),
            },
            process,
        }
    }

    #[test]
    fn companion_cancellation_rejects_a_job_pid_that_changed() {
        let directory = companion_state_fixture("running", 43, true);
        let canceller = CompanionGracefulCanceller::new(vec![directory.path().to_owned()]);
        canceller.prepare().expect("fixture should prepare");
        let expected = companion_process(42);
        assert_eq!(
            canceller.request_cancel(companion_child(&expected)),
            Err(GracefulCancelError::EvidenceChanged)
        );
    }

    #[test]
    fn companion_cancellation_rejects_a_fresh_terminal_job() {
        let directory = companion_state_fixture("completed", 42, false);
        let canceller = CompanionGracefulCanceller::new(vec![directory.path().to_owned()]);
        canceller.prepare().expect("fixture should prepare");
        let expected = companion_process(42);
        assert_eq!(
            canceller.request_cancel(companion_child(&expected)),
            Err(GracefulCancelError::EvidenceChanged)
        );
    }

    #[test]
    fn companion_without_an_authoritative_transport_is_unsupported() {
        let directory = companion_state_fixture("running", 42, false);
        let canceller = CompanionGracefulCanceller::new(vec![directory.path().to_owned()]);
        canceller
            .prepare()
            .expect("complete job evidence should prepare without a broker");
        let expected = companion_process(42);
        assert_eq!(
            canceller.request_cancel(companion_child(&expected)),
            Ok(GracefulCancelSupport::Unsupported)
        );
    }

    #[test]
    fn companion_stale_broker_allows_signal_fallback() {
        let directory = companion_state_fixture("running", 42, true);
        fs::write(
            directory.path().join("broker.json"),
            serde_json::to_vec(&serde_json::json!({
                "endpoint": "/invalid-unix-endpoint",
                "pid": u32::MAX
            }))
            .expect("broker should serialize"),
        )
        .expect("stale broker should be written");
        let canceller = CompanionGracefulCanceller::new(vec![directory.path().to_owned()]);
        canceller
            .prepare()
            .expect("stale broker transport must not block signal fallback");
        let expected = companion_process(42);
        assert_eq!(
            canceller.request_cancel(companion_child(&expected)),
            Ok(GracefulCancelSupport::Unsupported)
        );
    }

    #[test]
    fn companion_absent_target_is_contrary_evidence() {
        let directory = companion_state_fixture("running", 42, false);
        let canceller = CompanionGracefulCanceller::new(vec![directory.path().to_owned()]);
        canceller
            .prepare()
            .expect("complete job evidence should prepare without a broker");
        let expected = companion_process(42);
        let mut child = companion_child(&expected);
        child.native_id = "missing-job";
        assert_eq!(
            canceller.request_cancel(child),
            Err(GracefulCancelError::EvidenceChanged)
        );
    }

    #[test]
    fn companion_cancellation_rejects_scan_budget_uncertainty() {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        for index in 0..=COMPANION_SCAN_ENTRIES {
            fs::create_dir(directory.path().join(format!("entry-{index}")))
                .expect("fixture directory should be created");
        }
        assert_eq!(
            CompanionGracefulCanceller::new(vec![directory.path().to_owned()]).prepare(),
            Err(GracefulCancelError::Unavailable)
        );
    }

    #[test]
    fn companion_cancellation_bounds_aggregate_state_content() {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        for index in 0..5 {
            let workspace = directory.path().join(format!("workspace-{index}"));
            fs::create_dir(&workspace).expect("workspace should exist");
            let mut summary = serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "jobs": []
            }))
            .expect("summary should serialize");
            summary.resize(watchdog_companion::MAX_SUMMARY_BYTES, b' ');
            fs::write(workspace.join("state.json"), summary)
                .expect("bounded summary should be written");
        }

        assert_eq!(
            CompanionGracefulCanceller::new(vec![directory.path().to_owned()]).prepare(),
            Err(GracefulCancelError::Unavailable)
        );
    }

    #[test]
    fn companion_cancellation_refreshes_reloaded_capability_roots() {
        let initial = tempfile::tempdir().expect("initial root should exist");
        let reloaded = companion_state_fixture("running", 42, true);
        let canceller = CompanionGracefulCanceller::new(vec![initial.path().to_owned()]);
        canceller.prepare().expect("initial root should scan");
        assert!(
            canceller
                .targets
                .read()
                .expect("target cache should be readable")
                .is_empty()
        );

        canceller.configure_roots(&[reloaded.path().to_owned()]);
        canceller.prepare().expect("reloaded root should scan");
        assert!(
            canceller
                .targets
                .read()
                .expect("target cache should be readable")
                .contains_key("companion-job")
        );
    }

    fn companion_state_fixture(status: &str, job_pid: u32, with_broker: bool) -> tempfile::TempDir {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        fs::write(
            directory.path().join("state.json"),
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "jobs": [{
                    "id": "companion-job",
                    "workspaceRoot": "/work/tree",
                    "sessionId": "claude-main",
                    "status": status,
                    "phase": status,
                    "pid": job_pid,
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "updatedAt": "now"
                }]
            }))
            .expect("summary should serialize"),
        )
        .expect("summary should be written");
        if with_broker {
            fs::write(
                directory.path().join("broker.json"),
                serde_json::to_vec(&serde_json::json!({
                    "endpoint": "/invalid-unix-endpoint",
                    "pid": std::process::id()
                }))
                .expect("broker should serialize"),
            )
            .expect("broker should be written");
        }
        directory
    }

    fn companion_process(pid: u32) -> ProcessIdentity {
        ProcessIdentity::new(
            ProcessId::new(pid).expect("fixture PID should validate"),
            7,
            BoundedText::new("executable", "/usr/bin/codex").expect("executable should validate"),
        )
    }

    fn companion_child(expected_process: &ProcessIdentity) -> VerifiedChild<'_> {
        VerifiedChild {
            child: watchdog_domain::ChildSessionId::from(watchdog_domain::SessionId::from_native(
                &NativeSessionKey::new(RuntimeKind::CodexCompanion, "companion-job")
                    .expect("native identity should validate"),
            )),
            runtime: RuntimeKind::CodexCompanion,
            native_id: "companion-job",
            expected_process,
        }
    }

    #[derive(Debug)]
    struct FakeFreshSampler;

    impl FreshProcessSampler for FakeFreshSampler {
        fn read_identity(&self, expected: &ProcessIdentity) -> FreshProcessEvidence {
            FreshProcessEvidence::Present(expected.clone())
        }
    }

    #[derive(Debug)]
    struct MissingFreshSampler;

    impl FreshProcessSampler for MissingFreshSampler {
        fn read_identity(&self, _expected: &ProcessIdentity) -> FreshProcessEvidence {
            FreshProcessEvidence::Exited
        }
    }

    #[derive(Debug)]
    struct ChangedFreshSampler;

    impl FreshProcessSampler for ChangedFreshSampler {
        fn read_identity(&self, expected: &ProcessIdentity) -> FreshProcessEvidence {
            FreshProcessEvidence::Present(ProcessIdentity::new(
                expected.pid(),
                expected.start_time_ticks().saturating_add(1),
                BoundedText::new("executable", expected.executable())
                    .expect("fixture executable should validate"),
            ))
        }
    }

    async fn api_with_stalled_child(
        store: &WatchdogStore,
        clock: &Arc<FakeClock>,
    ) -> (AgentApi, watchdog_domain::ChildSessionId) {
        let api = AgentApi::with_policy(store.clone(), clock.clone(), ReducerPolicy::default())
            .await
            .expect("API should initialize");
        let transport =
            TransportKey::new("termination-monitor").expect("transport should validate");
        let main = api
            .register_session(
                &transport,
                RegisterSession {
                    runtime: RuntimeKind::ClaudeCode,
                    native_id: "monitor-main".to_owned(),
                    kind: SessionKind::Main,
                    parent: None,
                    event_key: "register-main".to_owned(),
                },
            )
            .await
            .expect("main should register");
        let child = api
            .register_session(
                &transport,
                RegisterSession {
                    runtime: RuntimeKind::ClaudeCode,
                    native_id: "monitor-child".to_owned(),
                    kind: SessionKind::Child,
                    parent: Some(main.session.session_id()),
                    event_key: "register-child".to_owned(),
                },
            )
            .await
            .expect("child should register");
        let process = ProcessIdentity::new(
            ProcessId::new(42).expect("PID should validate"),
            7,
            BoundedText::new("executable", "/usr/bin/claude").expect("executable should validate"),
        );
        let native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "monitor-child")
            .expect("native identity should validate");
        api.ingest_native_observation(
            ObservationEnvelope::new(
                ObservationId::from_native(RuntimeKind::ClaudeCode, "process", "monitor-child")
                    .expect("observation ID should validate"),
                native,
                clock.now(),
                ObservationSource::new(
                    AdapterIdentity::new(RuntimeKind::ClaudeCode, "test")
                        .expect("adapter should validate"),
                    "test:process",
                    EvidenceTrust::Authoritative,
                    None,
                )
                .expect("source should validate"),
                ObservationPayload::ProcessIdentity(process),
            )
            .expect("observation should validate"),
        )
        .await
        .expect("process identity should persist");
        clock.advance(DurationMs::new(15 * 60_000));
        api.reconcile_timers()
            .await
            .expect("stall threshold should reconcile");
        clock.advance(DurationMs::new(60 * 60_000));
        let SessionIdentity::Child(child_id) = child.session else {
            panic!("child registration must return child identity");
        };
        (api, child_id)
    }

    #[tokio::test]
    async fn monitor_starts_only_child_saga_after_fresh_health_and_process_gates_pass() {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        let retained = directory.keep();
        let store = WatchdogStore::open(&retained.join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(WallTimeMs::new(0), 0)));
        let (api, child_id) = api_with_stalled_child(&store, &clock).await;

        let health = HealthService::new(clock.clone());
        for component in [
            ComponentId::Store,
            ComponentId::ProcessSampler,
            ComponentId::Watcher,
            ComponentId::FilesystemReconciliation,
            ComponentId::ObservationQueue,
        ] {
            health.record(component, ComponentStatus::Healthy, None);
        }
        health.record(
            ComponentId::Adapter(RuntimeKind::ClaudeCode),
            ComponentStatus::Degraded,
            Some("test degradation"),
        );
        let engine = TerminationEngine::new(
            store.clone(),
            api.event_sequence(),
            Arc::new(LinuxProcessControl::new()),
            Arc::new(NoGracefulCanceller),
            TerminationConfig::default(),
        );
        let monitor = TerminationMonitor::with_parts(
            store.clone(),
            clock.clone(),
            health.clone(),
            Arc::new(FakeFreshSampler),
            engine.clone(),
            DurationMs::new(60 * 60_000),
        );

        assert_monitor_suspended(&monitor, &store, child_id).await;

        health.record(
            ComponentId::Adapter(RuntimeKind::ClaudeCode),
            ComponentStatus::Healthy,
            None,
        );
        health.record(
            ComponentId::Watcher,
            ComponentStatus::Degraded,
            Some("filesystem events were lost"),
        );
        assert_monitor_suspended(&monitor, &store, child_id).await;

        health.record(ComponentId::Watcher, ComponentStatus::Healthy, None);
        health.record_scoped(
            ComponentId::ObservationQueue,
            ComponentStatus::Degraded,
            HealthScope::Session(SessionIdentity::Child(child_id)),
            Some("session admission queue saturated"),
        );
        assert_monitor_suspended(&monitor, &store, child_id).await;

        health.record_scoped(
            ComponentId::ObservationQueue,
            ComponentStatus::Healthy,
            HealthScope::Session(SessionIdentity::Child(child_id)),
            None,
        );
        let changed_process = TerminationMonitor::with_parts(
            store.clone(),
            clock,
            health,
            Arc::new(ChangedFreshSampler),
            engine,
            DurationMs::new(60 * 60_000),
        );
        assert_monitor_suspended(&changed_process, &store, child_id).await;

        let started = monitor
            .reconcile()
            .await
            .expect("healthy reconciliation should succeed");
        assert_eq!(started.changed_sagas(), 1);
        assert_eq!(
            store
                .termination_saga(child_id)
                .await
                .expect("saga query should succeed")
                .expect("child saga should start")
                .stage,
            TerminationStage::WarningGrace
        );
    }

    async fn assert_monitor_suspended(
        monitor: &TerminationMonitor,
        store: &WatchdogStore,
        child_id: watchdog_domain::ChildSessionId,
    ) {
        let report = monitor
            .reconcile()
            .await
            .expect("unhealthy reconciliation should fail closed");
        assert_eq!(report.evaluated_children(), 1);
        assert_eq!(report.changed_sagas(), 0);
        assert!(
            store
                .termination_saga(child_id)
                .await
                .expect("saga query should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn fresh_process_exit_completes_the_production_monitor_saga() {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        let retained = directory.keep();
        let store = WatchdogStore::open(&retained.join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(WallTimeMs::new(0), 0)));
        let (api, child_id) = api_with_stalled_child(&store, &clock).await;
        let health = HealthService::new(clock.clone());
        for component in [
            ComponentId::Store,
            ComponentId::ProcessSampler,
            ComponentId::Watcher,
            ComponentId::FilesystemReconciliation,
            ComponentId::ObservationQueue,
            ComponentId::Adapter(RuntimeKind::ClaudeCode),
        ] {
            health.record(component, ComponentStatus::Healthy, None);
        }
        let engine = TerminationEngine::new(
            store.clone(),
            api.event_sequence(),
            Arc::new(LinuxProcessControl::new()),
            Arc::new(NoGracefulCanceller),
            TerminationConfig::default(),
        );
        let running = TerminationMonitor::with_parts(
            store.clone(),
            clock.clone(),
            health.clone(),
            Arc::new(FakeFreshSampler),
            engine.clone(),
            DurationMs::new(60 * 60_000),
        );
        assert_eq!(
            running
                .reconcile()
                .await
                .expect("warning should start")
                .changed_sagas(),
            1
        );

        let exited = TerminationMonitor::with_parts(
            store.clone(),
            clock,
            health,
            Arc::new(MissingFreshSampler),
            engine,
            DurationMs::new(60 * 60_000),
        );
        assert_eq!(
            exited
                .reconcile()
                .await
                .expect("definite process exit should reconcile")
                .changed_sagas(),
            1
        );
        assert_eq!(
            store
                .termination_saga(child_id)
                .await
                .expect("saga query should succeed")
                .expect("saga should exist")
                .stage,
            TerminationStage::Completed
        );
    }

    #[tokio::test]
    async fn fresh_process_identity_change_aborts_the_production_monitor_saga() {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        let retained = directory.keep();
        let store = WatchdogStore::open(&retained.join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(WallTimeMs::new(0), 0)));
        let (api, child_id) = api_with_stalled_child(&store, &clock).await;
        let health = HealthService::new(clock.clone());
        for component in [
            ComponentId::Store,
            ComponentId::ProcessSampler,
            ComponentId::Watcher,
            ComponentId::FilesystemReconciliation,
            ComponentId::ObservationQueue,
            ComponentId::Adapter(RuntimeKind::ClaudeCode),
        ] {
            health.record(component, ComponentStatus::Healthy, None);
        }
        let engine = TerminationEngine::new(
            store.clone(),
            api.event_sequence(),
            Arc::new(LinuxProcessControl::new()),
            Arc::new(NoGracefulCanceller),
            TerminationConfig::default(),
        );
        let running = TerminationMonitor::with_parts(
            store.clone(),
            clock.clone(),
            health.clone(),
            Arc::new(FakeFreshSampler),
            engine.clone(),
            DurationMs::new(60 * 60_000),
        );
        running.reconcile().await.expect("warning should start");

        let changed = TerminationMonitor::with_parts(
            store.clone(),
            clock,
            health,
            Arc::new(ChangedFreshSampler),
            engine,
            DurationMs::new(60 * 60_000),
        );
        assert_eq!(
            changed
                .reconcile()
                .await
                .expect("identity change should reconcile")
                .changed_sagas(),
            1
        );
        let saga = store
            .termination_saga(child_id)
            .await
            .expect("saga query should succeed")
            .expect("saga should exist");
        assert_eq!(saga.stage, TerminationStage::Aborted);
        assert_eq!(
            saga.last_outcome,
            Some(TerminationActionOutcome::SafetyAborted)
        );
        assert!(
            saga.safety
                .blockers
                .contains(&TerminationBlocker::ProcessIdentityChanged)
        );
    }
}
