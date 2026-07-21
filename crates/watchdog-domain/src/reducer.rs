use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    BoundedText, CompatibilityWarning, DeadlineCommand, DetailedState, DomainEventKind, DurationMs,
    EvidenceTrust, MainSessionId, ObservationEnvelope, ObservationId, ObservationPayload,
    ProcessIdentity, SessionIdentity, TimePoint, WallTimeMs,
};

/// Pure reducer thresholds independent from runtime-specific adapters.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ReducerPolicy {
    suspect_after: DurationMs,
    stall_after: DurationMs,
    reminder_every: DurationMs,
}

impl ReducerPolicy {
    /// Construct pure stall and reminder thresholds.
    ///
    /// # Errors
    ///
    /// Returns [`ReducerPolicyError`] for an inverted suspect/stall order or a
    /// zero reminder cadence.
    pub const fn new(
        suspect_after: DurationMs,
        stall_after: DurationMs,
        reminder_every: DurationMs,
    ) -> Result<Self, ReducerPolicyError> {
        if suspect_after.value() > stall_after.value() {
            return Err(ReducerPolicyError::SuspectAfterStall);
        }
        if reminder_every.value() == 0 {
            return Err(ReducerPolicyError::ZeroReminder);
        }
        Ok(Self {
            suspect_after,
            stall_after,
            reminder_every,
        })
    }
}

impl<'de> Deserialize<'de> for ReducerPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawPolicy {
            suspect_after: DurationMs,
            stall_after: DurationMs,
            reminder_every: DurationMs,
        }

        let raw = RawPolicy::deserialize(deserializer)?;
        Self::new(raw.suspect_after, raw.stall_after, raw.reminder_every).map_err(de::Error::custom)
    }
}

impl Default for ReducerPolicy {
    fn default() -> Self {
        Self {
            suspect_after: DurationMs::new(5 * 60_000),
            stall_after: DurationMs::new(15 * 60_000),
            reminder_every: DurationMs::new(5 * 60_000),
        }
    }
}

/// Invalid reducer timer policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReducerPolicyError {
    /// Corroboration cannot begin after the stall transition.
    #[error("Suspect threshold must not exceed stall threshold")]
    SuspectAfterStall,
    /// Zero cadence would emit a reminder on every scheduler tick.
    #[error("Reminder cadence must be positive")]
    ZeroReminder,
}

/// Durable reducer state for one normalized session.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionSnapshot {
    session: SessionIdentity,
    root: MainSessionId,
    revision: u64,
    state: DetailedState,
    updated_at: TimePoint,
    last_activity: TimePoint,
    last_trusted_transition: Option<TimePoint>,
    explicit_deadline: Option<WallTimeMs>,
    pause_started_monotonic_ms: Option<u64>,
    paused_elapsed_ms: u64,
    suspect_emitted: bool,
    stalled_since_monotonic_ms: Option<u64>,
    last_reminder_monotonic_ms: Option<u64>,
    last_observation_id: Option<ObservationId>,
    last_observation_monotonic_ms: Option<u64>,
    process_identity: Option<ProcessIdentity>,
    last_progress_summary: Option<BoundedText<2_048>>,
    compatibility_warning: Option<CompatibilityWarning>,
    source_conflict: bool,
    state_before_conflict: Option<DetailedState>,
    reconciliation_required: bool,
}

impl SessionSnapshot {
    /// Start a session snapshot at one coherent clock sample.
    #[must_use]
    pub const fn new(session: SessionIdentity, root: MainSessionId, now: TimePoint) -> Self {
        Self {
            session,
            root,
            revision: 0,
            state: DetailedState::Starting,
            updated_at: now,
            last_activity: now,
            last_trusted_transition: None,
            explicit_deadline: None,
            pause_started_monotonic_ms: None,
            paused_elapsed_ms: 0,
            suspect_emitted: false,
            stalled_since_monotonic_ms: None,
            last_reminder_monotonic_ms: None,
            last_observation_id: None,
            last_observation_monotonic_ms: None,
            process_identity: None,
            last_progress_summary: None,
            compatibility_warning: None,
            source_conflict: false,
            state_before_conflict: None,
            reconciliation_required: false,
        }
    }

    /// Role-preserving session identity.
    #[must_use]
    pub const fn session(&self) -> SessionIdentity {
        self.session
    }

    /// Main tree containing this session.
    #[must_use]
    pub const fn root(&self) -> MainSessionId {
        self.root
    }

    /// Monotonic reducer revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Current detailed state.
    #[must_use]
    pub const fn state(&self) -> DetailedState {
        self.state
    }

    /// Last accepted reducer input time.
    #[must_use]
    pub const fn updated_at(&self) -> TimePoint {
        self.updated_at
    }

    /// Last trustworthy progress time by any accepted signal.
    #[must_use]
    pub const fn last_activity(&self) -> TimePoint {
        self.last_activity
    }

    /// Most recent trustworthy state or progress transition.
    #[must_use]
    pub const fn last_trusted_transition(&self) -> Option<TimePoint> {
        self.last_trusted_transition
    }

    /// Observation that most recently advanced this snapshot, when present.
    #[must_use]
    pub const fn last_observation_id(&self) -> Option<ObservationId> {
        self.last_observation_id
    }

    /// Whether material sources currently disagree.
    #[must_use]
    pub const fn source_conflict(&self) -> bool {
        self.source_conflict
    }

    /// Current actionable adapter compatibility warning.
    #[must_use]
    pub const fn compatibility_warning(&self) -> Option<&CompatibilityWarning> {
        self.compatibility_warning.as_ref()
    }

    /// Freshest associated process identity; identity alone is not activity.
    #[must_use]
    pub const fn process_identity(&self) -> Option<&ProcessIdentity> {
        self.process_identity.as_ref()
    }

    /// Freshest bounded progress summary without native transcript content.
    #[must_use]
    pub fn last_progress_summary(&self) -> Option<&str> {
        self.last_progress_summary.as_ref().map(BoundedText::as_str)
    }

    /// Parent-provided wall deadline, when active.
    #[must_use]
    pub const fn explicit_deadline(&self) -> Option<WallTimeMs> {
        self.explicit_deadline
    }

    /// Whether stall and termination accounting is paused.
    #[must_use]
    pub const fn timers_paused(&self) -> bool {
        self.pause_started_monotonic_ms.is_some()
    }

    /// Whether post-restart evidence must be refreshed before decisions.
    #[must_use]
    pub const fn reconciliation_required(&self) -> bool {
        self.reconciliation_required
    }

    /// Monotonic instant at which the current continuous stall began.
    #[must_use]
    pub const fn stalled_since_monotonic_ms(&self) -> Option<u64> {
        self.stalled_since_monotonic_ms
    }
}

/// One pure input to the session reducer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReducerInput {
    /// Typed adapter or MCP observation.
    Observation(ObservationEnvelope),
    /// Monotonic scheduler evaluation.
    Tick(TimePoint),
    /// Process restarted; persisted wall deadlines are not immediately trusted.
    Restarted(TimePoint),
    /// Fresh adapter/process reconciliation completed.
    Reconciled(TimePoint),
}

/// New state and zero or more durable event payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReducerOutput {
    snapshot: SessionSnapshot,
    events: Vec<DomainEventKind>,
}

impl ReducerOutput {
    /// Resulting snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> &SessionSnapshot {
        &self.snapshot
    }

    /// Ordered domain event payloads.
    #[must_use]
    pub fn events(&self) -> &[DomainEventKind] {
        &self.events
    }

    /// Consume the output and retain its snapshot for the next reduction.
    #[must_use]
    pub fn into_snapshot(self) -> SessionSnapshot {
        self.snapshot
    }
}

/// Apply one deterministic reducer input.
#[must_use]
pub fn reduce(
    mut snapshot: SessionSnapshot,
    input: ReducerInput,
    policy: ReducerPolicy,
) -> ReducerOutput {
    let prior = snapshot.clone();
    let mut events = Vec::new();
    match input {
        ReducerInput::Observation(observation) => {
            apply_observation(&mut snapshot, &observation, &mut events);
        }
        ReducerInput::Tick(now) => apply_tick(&mut snapshot, now, policy, &mut events),
        ReducerInput::Restarted(now) => {
            snapshot.reconciliation_required = true;
            snapshot.last_observation_id = None;
            snapshot.last_observation_monotonic_ms = None;
            snapshot.last_activity = now;
            snapshot.paused_elapsed_ms = 0;
            snapshot.pause_started_monotonic_ms = None;
            snapshot.updated_at = now;
            events.push(DomainEventKind::ReconciliationRequired);
        }
        ReducerInput::Reconciled(now) => {
            snapshot.reconciliation_required = false;
            snapshot.updated_at = now;
        }
    }
    if snapshot != prior {
        snapshot.revision = prior.revision.saturating_add(1);
    }
    ReducerOutput { snapshot, events }
}

fn apply_observation(
    snapshot: &mut SessionSnapshot,
    observation: &ObservationEnvelope,
    events: &mut Vec<DomainEventKind>,
) {
    if snapshot.last_observation_id == Some(observation.observation_id())
        || snapshot
            .last_observation_monotonic_ms
            .is_some_and(|last| observation.observed_at().monotonic_ms() < last)
    {
        return;
    }
    snapshot.last_observation_id = Some(observation.observation_id());
    snapshot.last_observation_monotonic_ms = Some(observation.observed_at().monotonic_ms());
    snapshot.updated_at = observation.observed_at();
    if observation.source().trust() == EvidenceTrust::Uncertain {
        return;
    }
    if observation_clears_reconciliation(observation.payload()) {
        snapshot.reconciliation_required = false;
    }

    match observation.payload() {
        ObservationPayload::NativeState(state) => {
            let prior_state = snapshot.state;
            if prior_state == DetailedState::WaitingForUser
                && *state != DetailedState::WaitingForUser
            {
                resume_clock(snapshot, observation.observed_at());
            } else if prior_state != DetailedState::WaitingForUser
                && *state == DetailedState::WaitingForUser
            {
                pause_clock(snapshot, observation.observed_at());
            }
            let state_changed = transition(snapshot, *state, observation.observed_at(), events);
            if matches!(
                state,
                DetailedState::Starting
                    | DetailedState::Running
                    | DetailedState::WaitingForAgent
                    | DetailedState::WaitingForTool
            ) {
                snapshot.last_activity = observation.observed_at();
                snapshot.paused_elapsed_ms = 0;
                snapshot.suspect_emitted = false;
            }
            if state_changed
                && matches!(
                    state,
                    DetailedState::Stalled | DetailedState::Failed | DetailedState::Disappeared
                )
            {
                events.push(DomainEventKind::AlertDue);
            }
        }
        ObservationPayload::Progress(summary) => {
            apply_progress(snapshot, summary, observation.observed_at(), events);
        }
        ObservationPayload::ProcessIdentity(identity) => {
            snapshot.process_identity = Some(identity.clone());
        }
        ObservationPayload::Deadline(command) => {
            apply_deadline(snapshot, *command, observation.observed_at());
            events.push(DomainEventKind::DeadlineChanged);
        }
        ObservationPayload::IntentionalWait => {
            apply_intentional_wait(snapshot, observation.observed_at(), events);
        }
        ObservationPayload::Compatibility(warning) => {
            apply_compatibility_warning(snapshot, warning, events);
        }
        ObservationPayload::CompatibilityResolved => {
            if snapshot.compatibility_warning.take().is_some() {
                events.push(DomainEventKind::CompatibilityChanged);
            }
        }
        ObservationPayload::SourceConflict(_) => {
            if !snapshot.source_conflict {
                snapshot.source_conflict = true;
                snapshot.state_before_conflict = Some(snapshot.state);
                events.push(DomainEventKind::ConflictChanged { active: true });
            }
            transition(
                snapshot,
                DetailedState::Unknown,
                observation.observed_at(),
                events,
            );
        }
        ObservationPayload::SourceConflictResolved => {
            if snapshot.source_conflict {
                snapshot.source_conflict = false;
                events.push(DomainEventKind::ConflictChanged { active: false });
                let restored = snapshot
                    .state_before_conflict
                    .take()
                    .unwrap_or(DetailedState::Unknown);
                transition(snapshot, restored, observation.observed_at(), events);
            }
        }
        ObservationPayload::SchedulerTick => {}
    }
}

fn apply_compatibility_warning(
    snapshot: &mut SessionSnapshot,
    incoming: &CompatibilityWarning,
    events: &mut Vec<DomainEventKind>,
) {
    let existing = snapshot.compatibility_warning.as_ref();
    if existing != Some(incoming)
        && !existing.is_some_and(|warning| {
            warning.has_detected_version() && !incoming.has_detected_version()
        })
    {
        snapshot.compatibility_warning = Some(incoming.clone());
        events.push(DomainEventKind::CompatibilityChanged);
    }
}

fn observation_clears_reconciliation(payload: &ObservationPayload) -> bool {
    !matches!(
        payload,
        ObservationPayload::Compatibility(_)
            | ObservationPayload::CompatibilityResolved
            | ObservationPayload::SchedulerTick
    )
}

fn apply_tick(
    snapshot: &mut SessionSnapshot,
    now: TimePoint,
    policy: ReducerPolicy,
    events: &mut Vec<DomainEventKind>,
) {
    if snapshot.reconciliation_required
        || snapshot.pause_started_monotonic_ms.is_some()
        || !snapshot.state.stall_timer_runs()
    {
        return;
    }
    let deadline_expired = snapshot
        .explicit_deadline
        .is_some_and(|deadline| now.wall_time() >= deadline);
    let elapsed = active_elapsed(snapshot, now);
    let uses_fallback = snapshot.explicit_deadline.is_none();
    if uses_fallback && elapsed >= policy.suspect_after.value() && !snapshot.suspect_emitted {
        snapshot.suspect_emitted = true;
        events.push(DomainEventKind::Suspect);
    }
    if (deadline_expired || (uses_fallback && elapsed >= policy.stall_after.value()))
        && snapshot.state != DetailedState::Stalled
    {
        transition(snapshot, DetailedState::Stalled, now, events);
        snapshot.stalled_since_monotonic_ms = Some(now.monotonic_ms());
        snapshot.last_reminder_monotonic_ms = Some(now.monotonic_ms());
        events.push(DomainEventKind::AlertDue);
    } else if snapshot.state == DetailedState::Stalled
        && snapshot.last_reminder_monotonic_ms.is_some_and(|last| {
            now.monotonic_ms().saturating_sub(last) >= policy.reminder_every.value()
        })
    {
        snapshot.last_reminder_monotonic_ms = Some(now.monotonic_ms());
        snapshot.updated_at = now;
        events.push(DomainEventKind::ReminderDue);
    }
}

fn apply_deadline(snapshot: &mut SessionSnapshot, command: DeadlineCommand, now: TimePoint) {
    match command {
        DeadlineCommand::Set(deadline) => snapshot.explicit_deadline = Some(deadline),
        DeadlineCommand::Pause => pause_clock(snapshot, now),
        DeadlineCommand::Resume => resume_clock(snapshot, now),
        DeadlineCommand::Clear => snapshot.explicit_deadline = None,
    }
}

fn apply_intentional_wait(
    snapshot: &mut SessionSnapshot,
    now: TimePoint,
    events: &mut Vec<DomainEventKind>,
) {
    transition(snapshot, DetailedState::WaitingForAgent, now, events);
    snapshot.last_activity = now;
    snapshot.paused_elapsed_ms = 0;
    snapshot.suspect_emitted = false;
    snapshot.pause_started_monotonic_ms = Some(now.monotonic_ms());
    events.push(DomainEventKind::DeadlineChanged);
}

fn apply_progress(
    snapshot: &mut SessionSnapshot,
    summary: &BoundedText<2_048>,
    now: TimePoint,
    events: &mut Vec<DomainEventKind>,
) {
    snapshot.last_progress_summary = Some(summary.clone());
    if matches!(
        snapshot.state,
        DetailedState::WaitingForAgent
            | DetailedState::WaitingForTool
            | DetailedState::WaitingForUser
            | DetailedState::Idle
            | DetailedState::Stalled
    ) {
        resume_clock(snapshot, now);
        transition(snapshot, DetailedState::Running, now, events);
    }
    record_activity(snapshot, now, events);
}

fn pause_clock(snapshot: &mut SessionSnapshot, now: TimePoint) {
    if snapshot.pause_started_monotonic_ms.is_none() {
        snapshot.pause_started_monotonic_ms = Some(now.monotonic_ms());
    }
}

fn resume_clock(snapshot: &mut SessionSnapshot, now: TimePoint) {
    if let Some(started) = snapshot.pause_started_monotonic_ms.take() {
        snapshot.paused_elapsed_ms = snapshot
            .paused_elapsed_ms
            .saturating_add(now.monotonic_ms().saturating_sub(started));
    }
}

fn record_activity(
    snapshot: &mut SessionSnapshot,
    now: TimePoint,
    events: &mut Vec<DomainEventKind>,
) {
    snapshot.last_activity = now;
    snapshot.paused_elapsed_ms = 0;
    snapshot.suspect_emitted = false;
    snapshot.stalled_since_monotonic_ms = None;
    snapshot.last_reminder_monotonic_ms = None;
    if snapshot.state == DetailedState::Stalled {
        transition(snapshot, DetailedState::Running, now, events);
    }
}

fn transition(
    snapshot: &mut SessionSnapshot,
    state: DetailedState,
    now: TimePoint,
    events: &mut Vec<DomainEventKind>,
) -> bool {
    if snapshot.state == state {
        return false;
    }
    let from = snapshot.state;
    snapshot.state = state;
    if state == DetailedState::Stalled {
        snapshot.stalled_since_monotonic_ms = Some(now.monotonic_ms());
        snapshot.last_reminder_monotonic_ms = Some(now.monotonic_ms());
    } else if from == DetailedState::Stalled {
        snapshot.stalled_since_monotonic_ms = None;
        snapshot.last_reminder_monotonic_ms = None;
    }
    snapshot.updated_at = now;
    snapshot.last_trusted_transition = Some(now);
    events.push(DomainEventKind::StateChanged { from, to: state });
    true
}

fn active_elapsed(snapshot: &SessionSnapshot, now: TimePoint) -> u64 {
    now.monotonic_ms()
        .saturating_sub(snapshot.last_activity.monotonic_ms())
        .saturating_sub(snapshot.paused_elapsed_ms)
}

/// Apply hierarchy safety rules without allowing child stalls to alter the main state.
#[must_use]
pub fn aggregate_main_state(main: DetailedState, children: &[DetailedState]) -> DetailedState {
    if matches!(
        main,
        DetailedState::Completed | DetailedState::Failed | DetailedState::Cancelled
    ) && children.iter().any(|child| {
        !matches!(
            child,
            DetailedState::Completed
                | DetailedState::Failed
                | DetailedState::Cancelled
                | DetailedState::Disappeared
        )
    }) {
        DetailedState::Unknown
    } else {
        main
    }
}
