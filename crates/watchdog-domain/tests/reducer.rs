//! Deterministic session reduction, deadlines, and reminder contracts.

use watchdog_domain::{
    AdapterIdentity, BoundedText, ChildSessionId, DeadlineCommand, DetailedState, DomainEventKind,
    EvidenceTrust, MainSessionId, NativeSessionKey, ObservationEnvelope, ObservationId,
    ObservationPayload, ObservationSource, ProcessId, ProcessIdentity, ReducerInput, ReducerPolicy,
    RuntimeKind, SessionId, SessionIdentity, SessionSnapshot, TimePoint, WallTimeMs,
    aggregate_main_state, reduce,
};

const MINUTE: u64 = 60_000;

fn time(minutes: u64) -> TimePoint {
    TimePoint::new(
        WallTimeMs::new(i64::try_from(minutes * MINUTE).expect("fixture time should fit")),
        minutes * MINUTE,
    )
}

fn identity(value: u128) -> SessionIdentity {
    SessionIdentity::Child(ChildSessionId::from(SessionId::from_uuid(
        uuid::Uuid::from_u128(value),
    )))
}

fn root() -> MainSessionId {
    MainSessionId::from(SessionId::from_uuid(uuid::Uuid::from_u128(1)))
}

fn source(trust: EvidenceTrust) -> ObservationSource {
    ObservationSource::new(
        AdapterIdentity::new(RuntimeKind::ClaudeCode, "test").expect("adapter should be valid"),
        "synthetic",
        trust,
        None,
    )
    .expect("source should be valid")
}

fn observation(sequence: u64, at: TimePoint, payload: ObservationPayload) -> ObservationEnvelope {
    ObservationEnvelope::new(
        ObservationId::from_native(
            RuntimeKind::ClaudeCode,
            "reducer-test",
            sequence.to_string(),
        )
        .expect("observation ID should be valid"),
        NativeSessionKey::new(RuntimeKind::ClaudeCode, "child-1").expect("subject should be valid"),
        at,
        source(EvidenceTrust::Authoritative),
        payload,
    )
    .expect("observation should be valid")
}

fn initial() -> SessionSnapshot {
    SessionSnapshot::new(identity(2), root(), time(0))
}

fn has_state_change(events: &[DomainEventKind], from: DetailedState, to: DetailedState) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            DomainEventKind::StateChanged {
                from: actual_from,
                to: actual_to,
            } if *actual_from == from && *actual_to == to
        )
    })
}

#[test]
fn inactivity_emits_suspect_then_one_stalled_transition() {
    let policy = ReducerPolicy::default();
    let suspect = reduce(initial(), ReducerInput::Tick(time(5)), policy);
    assert!(suspect.events().contains(&DomainEventKind::Suspect));
    assert_eq!(suspect.snapshot().state(), DetailedState::Starting);

    let stalled = reduce(
        suspect.into_snapshot(),
        ReducerInput::Tick(time(15)),
        policy,
    );
    assert!(has_state_change(
        stalled.events(),
        DetailedState::Starting,
        DetailedState::Stalled,
    ));
    assert_eq!(stalled.snapshot().state(), DetailedState::Stalled);

    let duplicate = reduce(
        stalled.into_snapshot(),
        ReducerInput::Tick(time(16)),
        policy,
    );
    assert!(!has_state_change(
        duplicate.events(),
        DetailedState::Stalled,
        DetailedState::Stalled,
    ));
}

#[test]
fn waiting_for_user_never_stalls_at_any_threshold() {
    let waiting = reduce(
        initial(),
        ReducerInput::Observation(observation(
            1,
            time(1),
            ObservationPayload::NativeState(DetailedState::WaitingForUser),
        )),
        ReducerPolicy::default(),
    );
    let much_later = reduce(
        waiting.into_snapshot(),
        ReducerInput::Tick(time(10_000)),
        ReducerPolicy::default(),
    );

    assert_eq!(much_later.snapshot().state(), DetailedState::WaitingForUser);
    assert!(much_later.events().is_empty());
}

#[test]
fn deadline_changes_override_fallback_and_are_auditable() {
    let set = reduce(
        initial(),
        ReducerInput::Observation(observation(
            1,
            time(1),
            ObservationPayload::Deadline(DeadlineCommand::Set(WallTimeMs::new(60 * 60_000))),
        )),
        ReducerPolicy::default(),
    );
    assert!(set.events().contains(&DomainEventKind::DeadlineChanged));

    let before_deadline = reduce(
        set.into_snapshot(),
        ReducerInput::Tick(time(30)),
        ReducerPolicy::default(),
    );
    assert_ne!(before_deadline.snapshot().state(), DetailedState::Stalled);

    let shorten = reduce(
        before_deadline.into_snapshot(),
        ReducerInput::Observation(observation(
            2,
            time(31),
            ObservationPayload::Deadline(DeadlineCommand::Set(WallTimeMs::new(30 * 60_000))),
        )),
        ReducerPolicy::default(),
    );
    let expired = reduce(
        shorten.into_snapshot(),
        ReducerInput::Tick(time(31)),
        ReducerPolicy::default(),
    );
    assert_eq!(expired.snapshot().state(), DetailedState::Stalled);
}

#[test]
fn pause_and_resume_do_not_leak_elapsed_time() {
    let paused = reduce(
        initial(),
        ReducerInput::Observation(observation(
            1,
            time(4),
            ObservationPayload::Deadline(DeadlineCommand::Pause),
        )),
        ReducerPolicy::default(),
    );
    let resumed = reduce(
        paused.into_snapshot(),
        ReducerInput::Observation(observation(
            2,
            time(100),
            ObservationPayload::Deadline(DeadlineCommand::Resume),
        )),
        ReducerPolicy::default(),
    );
    let ten_active_minutes = reduce(
        resumed.into_snapshot(),
        ReducerInput::Tick(time(106)),
        ReducerPolicy::default(),
    );

    assert_ne!(
        ten_active_minutes.snapshot().state(),
        DetailedState::Stalled
    );
}

#[test]
fn native_progress_resets_stall_clock_without_mcp_heartbeat() {
    let near_stall = reduce(
        initial(),
        ReducerInput::Tick(time(14)),
        ReducerPolicy::default(),
    );
    let progress = reduce(
        near_stall.into_snapshot(),
        ReducerInput::Observation(observation(
            1,
            time(14),
            ObservationPayload::Progress(
                BoundedText::new("progress", "cargo test still consuming CPU")
                    .expect("progress should be valid"),
            ),
        )),
        ReducerPolicy::default(),
    );
    let later = reduce(
        progress.into_snapshot(),
        ReducerInput::Tick(time(20)),
        ReducerPolicy::default(),
    );

    assert_ne!(later.snapshot().state(), DetailedState::Stalled);
}

#[test]
fn process_identity_without_a_delta_is_not_activity() {
    let associated = reduce(
        initial(),
        ReducerInput::Observation(observation(
            1,
            time(14),
            ObservationPayload::ProcessIdentity(ProcessIdentity::new(
                ProcessId::new(42).expect("PID should be valid"),
                7,
                BoundedText::new("executable", "/usr/bin/claude")
                    .expect("executable should be valid"),
            )),
        )),
        ReducerPolicy::default(),
    );
    assert!(associated.snapshot().process_identity().is_some());

    let stalled = reduce(
        associated.into_snapshot(),
        ReducerInput::Tick(time(15)),
        ReducerPolicy::default(),
    );
    assert_eq!(stalled.snapshot().state(), DetailedState::Stalled);
}

#[test]
fn authoritative_disappearance_alerts_without_waiting_for_stall() {
    let output = reduce(
        initial(),
        ReducerInput::Observation(observation(
            1,
            time(1),
            ObservationPayload::NativeState(DetailedState::Disappeared),
        )),
        ReducerPolicy::default(),
    );

    assert!(has_state_change(
        output.events(),
        DetailedState::Starting,
        DetailedState::Disappeared,
    ));
    assert!(output.events().contains(&DomainEventKind::AlertDue));

    let repeated = reduce(
        output.into_snapshot(),
        ReducerInput::Observation(observation(
            2,
            time(2),
            ObservationPayload::NativeState(DetailedState::Disappeared),
        )),
        ReducerPolicy::default(),
    );
    assert!(repeated.events().is_empty());
}

#[test]
fn unresolved_stall_reminds_every_five_minutes_only() {
    let stalled = reduce(
        initial(),
        ReducerInput::Tick(time(15)),
        ReducerPolicy::default(),
    );
    let early = reduce(
        stalled.into_snapshot(),
        ReducerInput::Tick(time(19)),
        ReducerPolicy::default(),
    );
    assert!(!early.events().contains(&DomainEventKind::ReminderDue));
    let due = reduce(
        early.into_snapshot(),
        ReducerInput::Tick(time(20)),
        ReducerPolicy::default(),
    );
    assert!(due.events().contains(&DomainEventKind::ReminderDue));
    let duplicate = reduce(
        due.into_snapshot(),
        ReducerInput::Tick(time(20)),
        ReducerPolicy::default(),
    );
    assert!(!duplicate.events().contains(&DomainEventKind::ReminderDue));
}

#[test]
fn duplicate_and_out_of_order_observations_are_idempotent() {
    let running = observation(
        1,
        time(10),
        ObservationPayload::NativeState(DetailedState::Running),
    );
    let first = reduce(
        initial(),
        ReducerInput::Observation(running.clone()),
        ReducerPolicy::default(),
    );
    let revision = first.snapshot().revision();
    let duplicate = reduce(
        first.into_snapshot(),
        ReducerInput::Observation(running),
        ReducerPolicy::default(),
    );
    assert_eq!(duplicate.snapshot().revision(), revision);
    assert!(duplicate.events().is_empty());

    let stale = reduce(
        duplicate.into_snapshot(),
        ReducerInput::Observation(observation(
            2,
            time(9),
            ObservationPayload::NativeState(DetailedState::Failed),
        )),
        ReducerPolicy::default(),
    );
    assert_eq!(stale.snapshot().state(), DetailedState::Running);
    assert!(stale.events().is_empty());
}

#[test]
fn restart_with_expired_deadline_requires_fresh_reconciliation() {
    let deadline = reduce(
        initial(),
        ReducerInput::Observation(observation(
            1,
            time(1),
            ObservationPayload::Deadline(DeadlineCommand::Set(WallTimeMs::new(2 * 60_000))),
        )),
        ReducerPolicy::default(),
    );
    let restarted = reduce(
        deadline.into_snapshot(),
        ReducerInput::Restarted(time(100)),
        ReducerPolicy::default(),
    );
    let blocked = reduce(
        restarted.into_snapshot(),
        ReducerInput::Tick(time(101)),
        ReducerPolicy::default(),
    );
    assert_ne!(blocked.snapshot().state(), DetailedState::Stalled);
    assert!(blocked.snapshot().reconciliation_required());

    let reconciled = reduce(
        blocked.into_snapshot(),
        ReducerInput::Reconciled(time(102)),
        ReducerPolicy::default(),
    );
    let expired = reduce(
        reconciled.into_snapshot(),
        ReducerInput::Tick(time(102)),
        ReducerPolicy::default(),
    );
    assert_eq!(expired.snapshot().state(), DetailedState::Stalled);
}

#[test]
fn main_child_aggregation_never_hides_active_children_or_infects_parent_state() {
    assert_eq!(
        aggregate_main_state(DetailedState::Completed, &[DetailedState::Running]),
        DetailedState::Unknown,
    );
    assert_eq!(
        aggregate_main_state(DetailedState::Running, &[DetailedState::Stalled]),
        DetailedState::Running,
    );
}

#[test]
fn source_conflict_becomes_unknown_then_restores_only_on_explicit_resolution() {
    let running = reduce(
        initial(),
        ReducerInput::Observation(observation(
            1,
            time(1),
            ObservationPayload::NativeState(DetailedState::Running),
        )),
        ReducerPolicy::default(),
    );
    let conflicted = reduce(
        running.into_snapshot(),
        ReducerInput::Observation(observation(
            2,
            time(2),
            ObservationPayload::SourceConflict(
                BoundedText::new(
                    "conflict",
                    "native state disagrees with filesystem evidence",
                )
                .expect("conflict should be valid"),
            ),
        )),
        ReducerPolicy::default(),
    );
    assert_eq!(conflicted.snapshot().state(), DetailedState::Unknown);
    assert!(conflicted.snapshot().source_conflict());

    let resolved = reduce(
        conflicted.into_snapshot(),
        ReducerInput::Observation(observation(
            3,
            time(3),
            ObservationPayload::SourceConflictResolved,
        )),
        ReducerPolicy::default(),
    );
    assert_eq!(resolved.snapshot().state(), DetailedState::Running);
    assert!(!resolved.snapshot().source_conflict());
    assert!(
        resolved
            .events()
            .contains(&DomainEventKind::ConflictChanged { active: false })
    );
}

#[test]
fn causally_independent_compatibility_and_progress_inputs_converge() {
    let progress = observation(
        1,
        time(5),
        ObservationPayload::Progress(
            BoundedText::new("progress", "bounded progress").expect("progress should be valid"),
        ),
    );
    let warning = observation(
        2,
        time(5),
        ObservationPayload::Compatibility(
            watchdog_domain::CompatibilityWarning::new(
                watchdog_domain::WarningKind::Upgrade,
                "upgrade adapter",
            )
            .expect("warning should be valid"),
        ),
    );
    let left = reduce(
        reduce(
            initial(),
            ReducerInput::Observation(progress.clone()),
            ReducerPolicy::default(),
        )
        .into_snapshot(),
        ReducerInput::Observation(warning.clone()),
        ReducerPolicy::default(),
    );
    let right = reduce(
        reduce(
            initial(),
            ReducerInput::Observation(warning),
            ReducerPolicy::default(),
        )
        .into_snapshot(),
        ReducerInput::Observation(progress),
        ReducerPolicy::default(),
    );

    assert_eq!(left.snapshot().state(), right.snapshot().state());
    assert_eq!(
        left.snapshot().last_activity(),
        right.snapshot().last_activity()
    );
    assert_eq!(
        left.snapshot().source_conflict(),
        right.snapshot().source_conflict()
    );
    assert_eq!(
        left.snapshot()
            .compatibility_warning()
            .map(watchdog_domain::CompatibilityWarning::badge),
        Some("UPGRADE")
    );

    let cleared = reduce(
        left.into_snapshot(),
        ReducerInput::Observation(observation(
            3,
            time(6),
            ObservationPayload::CompatibilityResolved,
        )),
        ReducerPolicy::default(),
    );
    assert!(cleared.snapshot().compatibility_warning().is_none());
    assert!(
        cleared
            .events()
            .contains(&DomainEventKind::CompatibilityChanged)
    );
}
