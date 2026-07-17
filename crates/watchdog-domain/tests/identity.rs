//! Identity namespace and input-bound contracts.

use watchdog_domain::{
    BoundedText, DomainInputError, NativeSessionKey, ObservationId, RuntimeKind, SessionId,
};

#[test]
fn repeated_native_id_is_namespaced_by_runtime() {
    let claude = NativeSessionKey::new(RuntimeKind::ClaudeCode, "same-native-id")
        .expect("fixture should be valid");
    let codex = NativeSessionKey::new(RuntimeKind::CodexCli, "same-native-id")
        .expect("fixture should be valid");

    assert_ne!(
        SessionId::from_native(&claude),
        SessionId::from_native(&codex)
    );
}

#[test]
fn native_identity_and_observation_ids_are_stable() {
    let native = NativeSessionKey::new(RuntimeKind::CodexCompanion, "job-42")
        .expect("fixture should be valid");

    assert_eq!(
        SessionId::from_native(&native),
        SessionId::from_native(&native)
    );
    assert_eq!(
        ObservationId::from_native(RuntimeKind::CodexCompanion, "state", "event-7")
            .expect("fixture should be valid"),
        ObservationId::from_native(RuntimeKind::CodexCompanion, "state", "event-7")
            .expect("fixture should be valid")
    );
}

#[test]
fn bounded_text_rejects_oversized_input_without_echoing_it() {
    let secret_marker = "do-not-echo";
    let input = format!("{secret_marker}{}", "x".repeat(32));

    let error = BoundedText::<16>::new("summary", input).expect_err("input exceeds byte limit");

    assert_eq!(
        error,
        DomainInputError::TooLong {
            field: "summary",
            max_bytes: 16,
            actual_bytes: 43,
        }
    );
    assert!(!error.to_string().contains(secret_marker));
}

#[test]
fn empty_native_id_is_rejected() {
    let error = NativeSessionKey::new(RuntimeKind::ClaudeCode, "")
        .expect_err("native identity must not be empty");

    assert_eq!(error, DomainInputError::Empty { field: "native_id" });
}

#[test]
fn empty_observation_identity_parts_are_rejected() {
    let error = ObservationId::from_native(RuntimeKind::ClaudeCode, "hook", "")
        .expect_err("native event identity must not be empty");

    assert_eq!(
        error,
        DomainInputError::Empty {
            field: "native_event_id"
        }
    );
}
