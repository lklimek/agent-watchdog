//! Deserialization preserves every constructor invariant.

use watchdog_domain::{
    AdapterIdentity, BoundedText, CompatibilityWarning, Confidence, DeadlinePolicy,
    NativeSessionKey, ProcessId,
};

#[test]
fn bounded_and_nonempty_invariants_cannot_be_bypassed_by_json() {
    assert!(serde_json::from_str::<BoundedText<4>>(r#""12345""#).is_err());
    assert!(
        serde_json::from_str::<NativeSessionKey>(r#"{"runtime":"claude_code","native_id":""}"#)
            .is_err()
    );
    assert!(
        serde_json::from_str::<AdapterIdentity>(r#"{"runtime":"codex_cli","version":""}"#).is_err()
    );
    assert!(
        serde_json::from_str::<CompatibilityWarning>(r#"{"kind":"upgrade","message":""}"#).is_err()
    );
}

#[test]
fn legacy_version_specific_warning_recovers_detected_version() {
    let warning = serde_json::from_str::<CompatibilityWarning>(
        r#"{"kind":"upgrade","message":"Update Agent Watchdog's Claude adapter; detected Claude Code 2.2.0, tested with Claude Code 2.1.214"}"#,
    )
    .expect("legacy warning should deserialize");

    assert_eq!(warning.detected_version(), Some("2.2.0"));
}

#[test]
fn numeric_invariants_cannot_be_bypassed_by_json() {
    assert!(serde_json::from_str::<Confidence>("10001").is_err());
    assert!(serde_json::from_str::<ProcessId>("0").is_err());
    assert!(
        serde_json::from_str::<DeadlinePolicy>(
            r#"{"stall_after":900000,"terminate_after_stalled":0,"graceful_signal_grace":600000}"#
        )
        .is_err()
    );
}
