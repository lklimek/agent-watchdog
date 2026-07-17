//! Compile-time and runtime child-only safety contracts.

use uuid::Uuid;
use watchdog_domain::{
    AdapterIdentity, BoundedText, ChildSessionId, DeadlineCommand, DetailedState, DurationMs,
    EvidenceTrust, MainSessionId, NativeSessionKey, ObservationEnvelope, ObservationId,
    ObservationPayload, ObservationSource, ProcessId, ProcessIdentity, ReducerInput, ReducerPolicy,
    RuntimeKind, SessionId, SessionIdentity, SessionSnapshot, TerminationBlocker,
    TerminationCandidate, TerminationComponent, TerminationFacts, TerminationHealth, TimePoint,
    WallTimeMs, assess_termination, executable_matches_runtime, reduce,
};

const MINUTE: u64 = 60_000;

fn time(minutes: u64) -> TimePoint {
    TimePoint::new(
        WallTimeMs::new(i64::try_from(minutes * MINUTE).expect("fixture time should fit")),
        minutes * MINUTE,
    )
}

fn ids() -> (MainSessionId, ChildSessionId) {
    (
        MainSessionId::from(SessionId::from_uuid(Uuid::from_u128(1))),
        ChildSessionId::from(SessionId::from_uuid(Uuid::from_u128(2))),
    )
}

fn stalled_snapshot() -> SessionSnapshot {
    let (root, child) = ids();
    reduce(
        SessionSnapshot::new(SessionIdentity::Child(child), root, time(0)),
        ReducerInput::Tick(time(15)),
        ReducerPolicy::default(),
    )
    .into_snapshot()
}

fn process_identity(executable: &str) -> ProcessIdentity {
    ProcessIdentity::new(
        ProcessId::new(42).expect("fixture PID should be valid"),
        100,
        BoundedText::new("executable", executable).expect("executable should be bounded"),
    )
}

fn observation(sequence: u64, payload: ObservationPayload) -> ObservationEnvelope {
    ObservationEnvelope::new(
        ObservationId::from_native(RuntimeKind::ClaudeCode, "safety", sequence.to_string())
            .expect("observation ID should be valid"),
        NativeSessionKey::new(RuntimeKind::ClaudeCode, "child-1").expect("subject should be valid"),
        time(16 + sequence),
        ObservationSource::new(
            AdapterIdentity::new(RuntimeKind::ClaudeCode, "test").expect("adapter should be valid"),
            "safety-test",
            EvidenceTrust::Authoritative,
            None,
        )
        .expect("source should be valid"),
        payload,
    )
    .expect("observation should be valid")
}

fn facts<'a>(
    snapshot: &'a SessionSnapshot,
    fresh_process: Option<&'a ProcessIdentity>,
) -> TerminationFacts<'a> {
    TerminationFacts {
        snapshot,
        runtime: RuntimeKind::ClaudeCode,
        trustworthy_relation: true,
        active_operation: false,
        fresh_process,
        health: TerminationHealth::healthy(),
        now: time(75),
        terminate_after_stalled: DurationMs::new(60 * MINUTE),
    }
}

#[test]
fn termination_candidate_requires_a_child_id() {
    let raw = SessionId::from_uuid(Uuid::from_u128(7));
    let child = ChildSessionId::from(raw);
    let main = MainSessionId::from(raw);

    assert_eq!(TerminationCandidate::new(child).session_id(), child);
    assert_ne!(format!("{child:?}"), format!("{main:?}"));
}

#[test]
fn all_gates_must_pass_simultaneously() {
    let snapshot = stalled_snapshot();
    let process = process_identity("/usr/bin/claude");
    let (_, child) = ids();

    let assessment = assess_termination(
        TerminationCandidate::new(child),
        facts(&snapshot, Some(&process)),
    );

    assert!(assessment.eligible());
    assert_eq!(assessment.passed().len(), 11);
    assert!(assessment.blockers().is_empty());
}

#[test]
fn each_external_precondition_independently_blocks_termination() {
    let snapshot = stalled_snapshot();
    let process = process_identity("/usr/bin/claude");
    let (_, child) = ids();
    let candidate = TerminationCandidate::new(child);

    let mut cases = Vec::new();
    let mut case = facts(&snapshot, Some(&process));
    case.trustworthy_relation = false;
    cases.push((case, TerminationBlocker::UntrustedChild));
    let mut case = facts(&snapshot, Some(&process));
    case.now = time(74);
    cases.push((case, TerminationBlocker::InsufficientStall));
    let mut case = facts(&snapshot, Some(&process));
    case.active_operation = true;
    cases.push((case, TerminationBlocker::ActiveOperation));
    let mut case = facts(&snapshot, Some(&process));
    case.health = case.health.with_unhealthy(TerminationComponent::Queue);
    cases.push((case, TerminationBlocker::ComponentUnhealthy));
    cases.push((facts(&snapshot, None), TerminationBlocker::MissingProcess));
    let wrong_process = process_identity("/usr/bin/python3");
    cases.push((
        facts(&snapshot, Some(&wrong_process)),
        TerminationBlocker::RuntimeMismatch,
    ));

    for (facts, expected) in cases {
        let assessment = assess_termination(candidate, facts);
        assert!(!assessment.eligible());
        assert!(
            assessment.blockers().contains(&expected),
            "missing blocker {expected:?}: {:?}",
            assessment.blockers()
        );
    }
}

#[test]
fn parent_wait_conflict_and_restart_state_each_suspend_termination() {
    let baseline = stalled_snapshot();
    let process = process_identity("/usr/bin/claude");
    let (_, child) = ids();
    let candidate = TerminationCandidate::new(child);

    let deadline = reduce(
        baseline.clone(),
        ReducerInput::Observation(observation(
            1,
            ObservationPayload::Deadline(DeadlineCommand::Set(time(100).wall_time())),
        )),
        ReducerPolicy::default(),
    )
    .into_snapshot();
    let paused = reduce(
        baseline.clone(),
        ReducerInput::Observation(observation(
            2,
            ObservationPayload::Deadline(DeadlineCommand::Pause),
        )),
        ReducerPolicy::default(),
    )
    .into_snapshot();
    let conflicted = reduce(
        baseline.clone(),
        ReducerInput::Observation(observation(
            3,
            ObservationPayload::SourceConflict(
                BoundedText::new("conflict", "native sources disagree")
                    .expect("reason should be valid"),
            ),
        )),
        ReducerPolicy::default(),
    )
    .into_snapshot();
    let waiting = reduce(
        baseline.clone(),
        ReducerInput::Observation(observation(
            4,
            ObservationPayload::NativeState(DetailedState::WaitingForUser),
        )),
        ReducerPolicy::default(),
    )
    .into_snapshot();
    let restarted = reduce(
        baseline,
        ReducerInput::Restarted(time(17)),
        ReducerPolicy::default(),
    )
    .into_snapshot();

    for (snapshot, expected) in [
        (deadline, TerminationBlocker::DeadlineExtension),
        (paused, TerminationBlocker::TimersPaused),
        (conflicted, TerminationBlocker::SourceConflict),
        (waiting, TerminationBlocker::WaitingForUser),
        (restarted, TerminationBlocker::ReconciliationRequired),
    ] {
        let assessment = assess_termination(candidate, facts(&snapshot, Some(&process)));
        assert!(!assessment.eligible());
        assert!(assessment.blockers().contains(&expected));
    }
}

#[test]
fn executable_matching_is_explicit_and_conservative() {
    assert!(executable_matches_runtime(
        RuntimeKind::ClaudeCode,
        "/opt/bin/claude"
    ));
    assert!(executable_matches_runtime(
        RuntimeKind::CodexCompanion,
        "/usr/local/bin/codex"
    ));
    assert!(!executable_matches_runtime(
        RuntimeKind::ClaudeCode,
        "/usr/bin/codex"
    ));
    assert!(!executable_matches_runtime(
        RuntimeKind::CodexCli,
        "/usr/bin/cargo"
    ));
}
