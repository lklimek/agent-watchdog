//! Evidence, policy, process, observation, and event contracts.

use watchdog_domain::{
    AdapterIdentity, BoundedText, Capability, CapabilitySet, ChildSessionId, CompatibilityWarning,
    Confidence, DeadlinePolicy, DetailedState, DomainEvent, DomainEventKind, DurationMs, EventId,
    EvidenceTrust, MainSessionId, NativeSessionKey, ObservationEnvelope, ObservationId,
    ObservationPayload, ObservationSource, ProcessId, ProcessIdentity, RuntimeKind, SessionId,
    SessionIdentity, SessionKind, TimePoint, WallTimeMs, WarningKind,
};

fn session_id(value: u128) -> SessionId {
    SessionId::from_uuid(uuid::Uuid::from_u128(value))
}

#[test]
fn confidence_is_bounded_to_basis_points() {
    assert_eq!(
        Confidence::new(10_000)
            .expect("100% is valid")
            .basis_points(),
        10_000
    );
    assert!(Confidence::new(10_001).is_err());
}

#[test]
fn deadline_policy_rejects_zero_stalled_termination_delay() {
    let error = DeadlinePolicy::new(
        DurationMs::new(15 * 60_000),
        DurationMs::new(0),
        DurationMs::new(10 * 60_000),
    )
    .expect_err("termination requires a long child-only delay after stall");

    assert_eq!(
        error.to_string(),
        "Stalled termination delay must be positive"
    );
}

#[test]
fn default_deadlines_use_fifteen_minute_stall_and_one_hour_stalled_delay() {
    let policy = DeadlinePolicy::default();

    assert_eq!(policy.stall_after(), DurationMs::new(15 * 60_000));
    assert_eq!(
        policy.terminate_after_stalled(),
        DurationMs::new(60 * 60_000)
    );
    assert_eq!(policy.graceful_signal_grace(), DurationMs::new(10 * 60_000));
}

#[test]
fn process_identity_rejects_pid_zero() {
    assert!(ProcessId::new(0).is_err());
    let pid = ProcessId::new(42).expect("positive PID is valid");
    let executable =
        BoundedText::new("executable", "/usr/bin/claude").expect("fixture should be valid");

    let identity = ProcessIdentity::new(pid, 99, executable);

    assert_eq!(identity.pid(), pid);
    assert_eq!(identity.start_time_ticks(), 99);
}

#[test]
fn observation_preserves_typed_provenance_and_bounded_payload() {
    let native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "session-7")
        .expect("fixture should be valid");
    let source = ObservationSource::new(
        AdapterIdentity::new(RuntimeKind::ClaudeCode, "2.1.212")
            .expect("adapter identity should be valid"),
        "hook:subagent-stop",
        EvidenceTrust::Authoritative,
        Some(Confidence::new(10_000).expect("confidence should be valid")),
    )
    .expect("source should be valid");
    let payload = ObservationPayload::NativeState(DetailedState::Running);
    let observation_id = ObservationId::from_native(RuntimeKind::ClaudeCode, "hook", "event-9")
        .expect("identity should be valid");

    let envelope = ObservationEnvelope::new(
        observation_id,
        native.clone(),
        TimePoint::new(WallTimeMs::new(1_000), 500),
        source,
        payload.clone(),
    )
    .expect("matching runtime provenance should be valid");

    assert_eq!(envelope.subject(), &native);
    assert_eq!(envelope.payload(), &payload);
    assert_eq!(envelope.source().trust(), EvidenceTrust::Authoritative);
}

#[test]
fn observation_rejects_cross_runtime_provenance() {
    let subject = NativeSessionKey::new(RuntimeKind::ClaudeCode, "session-7")
        .expect("fixture should be valid");
    let source = ObservationSource::new(
        AdapterIdentity::new(RuntimeKind::CodexCli, "0.144.5")
            .expect("adapter identity should be valid"),
        "app-server",
        EvidenceTrust::Authoritative,
        None,
    )
    .expect("source should be valid");
    let observation_id = ObservationId::from_native(RuntimeKind::CodexCli, "event", "7")
        .expect("identity should be valid");

    let error = ObservationEnvelope::new(
        observation_id,
        subject,
        TimePoint::new(WallTimeMs::new(1_000), 500),
        source,
        ObservationPayload::NativeState(DetailedState::Running),
    )
    .expect_err("an adapter cannot emit another runtime's native subject");

    assert!(error.to_string().contains("runtime does not match"));
}

#[test]
fn observation_deserialization_rejects_cross_runtime_provenance() {
    let json = serde_json::json!({
        "observation_id": ObservationId::from_native(RuntimeKind::CodexCli, "event", "7")
            .expect("identity should be valid"),
        "subject": NativeSessionKey::new(RuntimeKind::ClaudeCode, "session-7")
            .expect("fixture should be valid"),
        "observed_at": TimePoint::new(WallTimeMs::new(1_000), 500),
        "source": ObservationSource::new(
            AdapterIdentity::new(RuntimeKind::CodexCli, "0.144.5")
                .expect("adapter identity should be valid"),
            "app-server",
            EvidenceTrust::Authoritative,
            None,
        )
        .expect("source should be valid"),
        "payload": ObservationPayload::NativeState(DetailedState::Running),
    });

    let error = serde_json::from_value::<ObservationEnvelope>(json)
        .expect_err("deserialization must preserve cross-field invariants");

    assert!(error.to_string().contains("runtime does not match"));
}

#[test]
fn upgrade_warning_has_actionable_single_word_badge() {
    let warning = CompatibilityWarning::new(WarningKind::Upgrade, "Update Agent Watchdog")
        .expect("warning should be valid");

    assert_eq!(warning.badge(), "UPGRADE");
    assert_eq!(warning.message(), "Update Agent Watchdog");
}

#[test]
fn session_identity_keeps_main_and_child_roles_separate() {
    let main = SessionIdentity::Main(MainSessionId::from(session_id(1)));
    let child = SessionIdentity::Child(ChildSessionId::from(session_id(2)));

    assert_eq!(main.kind(), SessionKind::Main);
    assert_eq!(child.kind(), SessionKind::Child);
    assert_ne!(main.session_id(), child.session_id());
}

#[test]
fn capability_model_reserves_future_opencode_without_schema_change() {
    let capabilities = CapabilitySet::new([
        Capability::ExactHierarchy,
        Capability::NativeEvents,
        Capability::GracefulCancel,
        Capability::PushHint,
    ]);

    assert_eq!(RuntimeKind::OpenCode.as_str(), "opencode");
    assert!(capabilities.contains(Capability::NativeEvents));
    assert!(!capabilities.contains(Capability::ForceSignal));
}

#[test]
fn event_is_ordered_and_scoped_to_main_tree() {
    let root = MainSessionId::from(session_id(1));
    let subject = SessionIdentity::Child(ChildSessionId::from(session_id(2)));
    let event = DomainEvent::new(
        EventId::new(17),
        root,
        subject,
        WallTimeMs::new(2_000),
        DomainEventKind::StateChanged {
            from: DetailedState::Running,
            to: DetailedState::Stalled,
        },
    );

    assert_eq!(event.id().value(), 17);
    assert_eq!(event.root(), root);
}
