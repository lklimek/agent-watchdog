use std::{
    collections::{BTreeSet, HashSet},
    fmt,
    sync::Arc,
};

use thiserror::Error;
use watchdog_domain::{
    DetailedState, DomainEvent, DomainEventKind, DurationMs, EvidenceTrust, MainSessionId,
    ProcessIdentity, RuntimeKind, SessionKind, TerminationActionOutcome, TerminationBlocker,
    TerminationCandidate, TerminationComponent, TerminationFacts, TerminationHealth,
    TerminationStage, TimePoint, WallTimeMs, assess_termination,
};
use watchdog_process::{LinuxProcessControl, LinuxProcessSampler, ProcessControl, ProcessSignal};
use watchdog_runtime::{ComponentId, ComponentStatus, CoordinatorError, EventSequence};
use watchdog_store::{
    OutboxDestination, StoreError, TerminationAdvance, TerminationSafetyRecord,
    TerminationSagaRecord, WatchdogStore,
};

use crate::{AgentApi, HealthService};

const TEN_MINUTES_MS: u64 = 10 * 60_000;
const MAX_CHILD_SESSIONS: u32 = 1_000;
const MAX_RELATIONS_PER_ROOT: u32 = 1_000;

trait FreshProcessSampler: fmt::Debug + Send + Sync {
    fn read_identity(&self, expected: &ProcessIdentity) -> Option<ProcessIdentity>;
}

impl FreshProcessSampler for LinuxProcessSampler {
    fn read_identity(&self, expected: &ProcessIdentity) -> Option<ProcessIdentity> {
        LinuxProcessSampler::read_identity(self, expected.pid()).ok()
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

    pub(crate) const fn update_policy(
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
            let fresh_process = snapshot
                .process_identity()
                .and_then(|expected| self.sampler.read_identity(expected))
                .filter(|fresh| snapshot.process_identity() == Some(fresh));
            let facts = TerminationFacts {
                snapshot,
                runtime: child.native.runtime(),
                trustworthy_relation: trustworthy.contains(&child_id),
                active_operation: matches!(
                    snapshot.state(),
                    DetailedState::Running | DetailedState::WaitingForTool
                ),
                fresh_process: fresh_process.as_ref(),
                health: self.termination_health(child.native.runtime()),
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
                    child_exited: matches!(
                        snapshot.state(),
                        DetailedState::Completed | DetailedState::Cancelled
                    ),
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

    fn termination_health(&self, runtime: RuntimeKind) -> TerminationHealth {
        let mut health = TerminationHealth::healthy();
        for (component, termination_component) in [
            (ComponentId::Store, TerminationComponent::Store),
            (ComponentId::Watcher, TerminationComponent::Queue),
            (
                ComponentId::FilesystemReconciliation,
                TerminationComponent::Queue,
            ),
            (ComponentId::Adapter(runtime), TerminationComponent::Adapter),
            (ComponentId::ProcessSampler, TerminationComponent::Process),
        ] {
            if self.health.component_status(component) != Some(ComponentStatus::Healthy) {
                health = health.with_unhealthy(termination_component);
            }
        }
        health
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
}

/// Injectable supported-runtime cancellation dispatcher.
pub trait GracefulCanceller: fmt::Debug + Send + Sync {
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
        if existing
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
                let result = self.graceful.request_cancel(VerifiedChild {
                    child,
                    runtime: context.facts.runtime,
                    native_id: context.native_id,
                });
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
                self.persist(
                    context,
                    existing,
                    TerminationStage::Sigterm,
                    None,
                    TerminationActionOutcome::AutomationDisabled,
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
        if existing.safety.process.as_ref() == context.facts.fresh_process {
            return Ok(None);
        }
        if context.facts.fresh_process.is_none() {
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
    if matches!(
        stage,
        TerminationStage::WarningGrace | TerminationStage::Aborted
    ) {
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
    use watchdog_domain::{
        AdapterIdentity, BoundedText, Clock as _, NativeSessionKey, ObservationEnvelope,
        ObservationId, ObservationPayload, ObservationSource, ProcessId, ReducerPolicy,
        SessionIdentity, TimePoint,
    };
    use watchdog_testkit::FakeClock;

    use super::*;
    use crate::{RegisterSession, TransportKey};

    #[derive(Debug)]
    struct FakeFreshSampler;

    impl FreshProcessSampler for FakeFreshSampler {
        fn read_identity(&self, expected: &ProcessIdentity) -> Option<ProcessIdentity> {
            Some(expected.clone())
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
            clock,
            health.clone(),
            Arc::new(FakeFreshSampler),
            engine,
            DurationMs::new(60 * 60_000),
        );

        let suspended = monitor
            .reconcile()
            .await
            .expect("degraded reconciliation should fail closed");
        assert_eq!(suspended.evaluated_children(), 1);
        assert_eq!(suspended.changed_sagas(), 0);
        assert!(
            store
                .termination_saga(child_id)
                .await
                .expect("saga query should succeed")
                .is_none()
        );

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
        let watcher_suspended = monitor
            .reconcile()
            .await
            .expect("watcher degradation should fail closed");
        assert_eq!(watcher_suspended.changed_sagas(), 0);
        assert!(
            store
                .termination_saga(child_id)
                .await
                .expect("saga query should succeed")
                .is_none()
        );

        health.record(ComponentId::Watcher, ComponentStatus::Healthy, None);
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
}
