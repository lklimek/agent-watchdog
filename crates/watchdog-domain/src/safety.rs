use std::{collections::BTreeSet, path::Path};

use serde::{Deserialize, Serialize};

use crate::{
    ChildSessionId, DetailedState, DurationMs, ProcessIdentity, RuntimeKind, SessionIdentity,
    SessionSnapshot, TimePoint,
};

/// A child identity admitted to the separate termination safety pipeline.
///
/// Main identities cannot construct this type:
///
/// ```compile_fail
/// use uuid::Uuid;
/// use watchdog_domain::{MainSessionId, SessionId, TerminationCandidate};
/// let main = MainSessionId::from(SessionId::from_uuid(Uuid::nil()));
/// let _candidate = TerminationCandidate::new(main);
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TerminationCandidate(ChildSessionId);

impl TerminationCandidate {
    /// Admit a child session to later runtime and evidence gates.
    #[must_use]
    pub const fn new(session_id: ChildSessionId) -> Self {
        Self(session_id)
    }

    /// Return the admitted child identity.
    #[must_use]
    pub const fn session_id(self) -> ChildSessionId {
        self.0
    }
}

/// Persisted stage in the conservative child-termination saga.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationStage {
    /// Parent warning and grace period.
    WarningGrace,
    /// Runtime-native cancellation requested.
    GracefulCancel,
    /// Verified `SIGTERM` sent.
    Sigterm,
    /// Verified `SIGKILL` sent.
    Sigkill,
    /// Child exited and no further action is allowed.
    Completed,
    /// A safety gate failed and escalation is permanently stopped.
    Aborted,
}

/// Bounded audit result for the most recent termination-saga action.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationActionOutcome {
    /// Parent warning and its grace deadline were scheduled.
    WarningScheduled,
    /// Runtime accepted a native graceful cancellation request.
    GracefulRequested,
    /// Runtime has no supported native graceful cancellation operation.
    GracefulUnsupported,
    /// Runtime-native graceful cancellation failed without mutating state files.
    GracefulFailed,
    /// A freshly verified pidfd delivered the stage's literal signal.
    SignalSent,
    /// Fresh evidence proved the child exited.
    ChildExited,
    /// Contrary evidence or a safety failure permanently stopped escalation.
    SafetyAborted,
    /// Global automation configuration stopped escalation.
    AutomationDisabled,
    /// SIGKILL policy stopped escalation after a verified SIGTERM.
    SigkillDisabled,
}

impl TerminationStage {
    /// Stable persistence name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WarningGrace => "warning_grace",
            Self::GracefulCancel => "graceful_cancel",
            Self::Sigterm => "sigterm",
            Self::Sigkill => "sigkill",
            Self::Completed => "completed",
            Self::Aborted => "aborted",
        }
    }
}

/// One required child-termination safety gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationGate {
    /// Session and candidate identify the same child.
    TrustworthyChild,
    /// The child has remained continuously stalled for the policy duration.
    ContinuousStall,
    /// No material source conflict exists.
    NoSourceConflict,
    /// No active long-running operation exists.
    NoActiveOperation,
    /// Session is not waiting for user input.
    NotWaitingForUser,
    /// Parent deadline and extensions permit escalation.
    DeadlineAllows,
    /// Parent has not intentionally paused timing.
    TimersRunning,
    /// Restart reconciliation has completed.
    Reconciled,
    /// Store, queue, adapter, and process evidence are all healthy.
    ComponentsHealthy,
    /// A fresh process identity is available.
    ProcessVerified,
    /// The fresh executable matches the declared runtime.
    RuntimeMatches,
}

/// A reason destructive automation must remain suspended.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationBlocker {
    /// Candidate and snapshot identities do not match or are not a child.
    UntrustedChild,
    /// Session is not continuously stalled for the required duration.
    InsufficientStall,
    /// Material sources disagree.
    SourceConflict,
    /// A correlated long-running operation remains active.
    ActiveOperation,
    /// Session requires user input.
    WaitingForUser,
    /// A parent deadline extension remains active.
    DeadlineExtension,
    /// Timer accounting is intentionally paused.
    TimersPaused,
    /// Fresh post-restart reconciliation has not completed.
    ReconciliationRequired,
    /// At least one required component is degraded or failed.
    ComponentUnhealthy,
    /// No fresh process identity is available.
    MissingProcess,
    /// The fresh executable does not match the runtime.
    RuntimeMismatch,
    /// PID, start time, or executable changed after the warning decision.
    ProcessIdentityChanged,
    /// Fresh pidfd verification or literal signal delivery failed.
    ProcessControlFailure,
}

/// Component whose degraded state suspends destructive automation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminationComponent {
    /// Durable storage.
    Store,
    /// Per-session evidence queue.
    Queue,
    /// Owning runtime adapter.
    Adapter,
    /// Process sampling and pidfd control.
    Process,
}

/// Compact external-health facts supplied to the pure termination gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TerminationHealth(u8);

impl TerminationHealth {
    /// Construct an all-healthy component set.
    #[must_use]
    pub const fn healthy() -> Self {
        Self(0)
    }

    /// Mark one component unhealthy without exposing boolean combinations.
    #[must_use]
    pub const fn with_unhealthy(mut self, component: TerminationComponent) -> Self {
        self.0 |= match component {
            TerminationComponent::Store => 1,
            TerminationComponent::Queue => 1 << 1,
            TerminationComponent::Adapter => 1 << 2,
            TerminationComponent::Process => 1 << 3,
        };
        self
    }

    /// Whether every destructive-automation prerequisite is healthy.
    #[must_use]
    pub const fn all_healthy(self) -> bool {
        self.0 == 0
    }
}

/// Complete immutable input to the pure termination gate.
#[derive(Clone, Copy, Debug)]
pub struct TerminationFacts<'a> {
    /// Current normalized child snapshot.
    pub snapshot: &'a SessionSnapshot,
    /// Declared runtime used to check the executable identity.
    pub runtime: RuntimeKind,
    /// Whether exact/corroborated hierarchy evidence selected the parent.
    pub trustworthy_relation: bool,
    /// Whether a correlated long-running operation is still progressing.
    pub active_operation: bool,
    /// Fresh process identity sampled for this decision point.
    pub fresh_process: Option<&'a ProcessIdentity>,
    /// Required component health.
    pub health: TerminationHealth,
    /// One coherent clock sample.
    pub now: TimePoint,
    /// Continuous stalled duration required before warning.
    pub terminate_after_stalled: DurationMs,
}

/// Pure termination-gate result retaining both proof and blockers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminationAssessment {
    passed: BTreeSet<TerminationGate>,
    blockers: BTreeSet<TerminationBlocker>,
}

impl TerminationAssessment {
    /// Gates proven by this immutable evidence snapshot.
    #[must_use]
    pub const fn passed(&self) -> &BTreeSet<TerminationGate> {
        &self.passed
    }

    /// Reasons escalation remains prohibited.
    #[must_use]
    pub const fn blockers(&self) -> &BTreeSet<TerminationBlocker> {
        &self.blockers
    }

    /// Whether every required gate passed simultaneously.
    #[must_use]
    pub fn eligible(&self) -> bool {
        self.blockers.is_empty()
    }
}

/// Evaluate every destructive-automation gate without side effects.
#[must_use]
pub fn assess_termination(
    candidate: TerminationCandidate,
    facts: TerminationFacts<'_>,
) -> TerminationAssessment {
    let mut passed = BTreeSet::new();
    let mut blockers = BTreeSet::new();

    gate(
        matches!(facts.snapshot.session(), SessionIdentity::Child(child) if child == candidate.session_id())
            && facts.trustworthy_relation,
        TerminationGate::TrustworthyChild,
        TerminationBlocker::UntrustedChild,
        &mut passed,
        &mut blockers,
    );
    let stalled_long_enough = facts.snapshot.state() == DetailedState::Stalled
        && facts
            .snapshot
            .stalled_since_monotonic_ms()
            .is_some_and(|since| {
                facts.now.monotonic_ms().saturating_sub(since)
                    >= facts.terminate_after_stalled.value()
            });
    gate(
        stalled_long_enough,
        TerminationGate::ContinuousStall,
        TerminationBlocker::InsufficientStall,
        &mut passed,
        &mut blockers,
    );
    gate(
        !facts.snapshot.source_conflict(),
        TerminationGate::NoSourceConflict,
        TerminationBlocker::SourceConflict,
        &mut passed,
        &mut blockers,
    );
    gate(
        !facts.active_operation,
        TerminationGate::NoActiveOperation,
        TerminationBlocker::ActiveOperation,
        &mut passed,
        &mut blockers,
    );
    gate(
        facts.snapshot.state() != DetailedState::WaitingForUser,
        TerminationGate::NotWaitingForUser,
        TerminationBlocker::WaitingForUser,
        &mut passed,
        &mut blockers,
    );
    gate(
        facts
            .snapshot
            .explicit_deadline()
            .is_none_or(|deadline| deadline <= facts.now.wall_time()),
        TerminationGate::DeadlineAllows,
        TerminationBlocker::DeadlineExtension,
        &mut passed,
        &mut blockers,
    );
    gate(
        !facts.snapshot.timers_paused(),
        TerminationGate::TimersRunning,
        TerminationBlocker::TimersPaused,
        &mut passed,
        &mut blockers,
    );
    gate(
        !facts.snapshot.reconciliation_required(),
        TerminationGate::Reconciled,
        TerminationBlocker::ReconciliationRequired,
        &mut passed,
        &mut blockers,
    );
    gate(
        facts.health.all_healthy(),
        TerminationGate::ComponentsHealthy,
        TerminationBlocker::ComponentUnhealthy,
        &mut passed,
        &mut blockers,
    );
    gate(
        facts.fresh_process.is_some(),
        TerminationGate::ProcessVerified,
        TerminationBlocker::MissingProcess,
        &mut passed,
        &mut blockers,
    );
    if let Some(process) = facts.fresh_process {
        gate(
            executable_matches_runtime(facts.runtime, process.executable()),
            TerminationGate::RuntimeMatches,
            TerminationBlocker::RuntimeMismatch,
            &mut passed,
            &mut blockers,
        );
    }

    TerminationAssessment { passed, blockers }
}

/// Conservatively match a canonical executable basename to its runtime.
#[must_use]
pub fn executable_matches_runtime(runtime: RuntimeKind, executable: &str) -> bool {
    let Some(name) = Path::new(executable)
        .file_name()
        .and_then(|name| name.to_str())
    else {
        return false;
    };
    match runtime {
        RuntimeKind::ClaudeCode => matches!(name, "claude" | "claude-code"),
        RuntimeKind::CodexCli => name == "codex",
        RuntimeKind::CodexCompanion => matches!(name, "codex" | "codex-companion"),
        RuntimeKind::OpenCode => name == "opencode",
    }
}

fn gate(
    condition: bool,
    success: TerminationGate,
    failure: TerminationBlocker,
    passed: &mut BTreeSet<TerminationGate>,
    blockers: &mut BTreeSet<TerminationBlocker>,
) {
    if condition {
        passed.insert(success);
    } else {
        blockers.insert(failure);
    }
}
