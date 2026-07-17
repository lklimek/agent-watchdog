use std::{collections::BTreeSet, fmt, sync::Arc};

use thiserror::Error;
use watchdog_domain::{
    DetailedState, DomainEvent, DomainEventKind, DurationMs, MainSessionId, RuntimeKind,
    TerminationActionOutcome, TerminationBlocker, TerminationCandidate, TerminationFacts,
    TerminationStage, TimePoint, WallTimeMs, assess_termination,
};
use watchdog_process::{ProcessControl, ProcessSignal};
use watchdog_runtime::{CoordinatorError, EventSequence};
use watchdog_store::{
    OutboxDestination, StoreError, TerminationAdvance, TerminationSafetyRecord,
    TerminationSagaRecord, WatchdogStore,
};

const TEN_MINUTES_MS: u64 = 10 * 60_000;

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
