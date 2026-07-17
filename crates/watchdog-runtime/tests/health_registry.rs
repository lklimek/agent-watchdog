//! Component health containment and safety suspension contracts.

use watchdog_domain::{ChildSessionId, NativeSessionKey, RuntimeKind, SessionId, SessionIdentity};
use watchdog_runtime::{
    ComponentHealth, ComponentId, ComponentStatus, HealthRegistry, HealthScope,
};

fn child(runtime: RuntimeKind, native_id: &str) -> SessionIdentity {
    let native = NativeSessionKey::new(runtime, native_id).expect("native key should be valid");
    SessionIdentity::Child(ChildSessionId::from(SessionId::from_native(&native)))
}

#[test]
fn adapter_failure_is_contained_but_suspends_its_runtime() {
    let mut registry = HealthRegistry::default();
    registry.record(ComponentHealth::new(
        ComponentId::Adapter(RuntimeKind::ClaudeCode),
        ComponentStatus::Failed,
        HealthScope::Runtime(RuntimeKind::ClaudeCode),
    ));

    assert!(registry.is_ready());
    assert!(!registry.destructive_automation_allowed(
        RuntimeKind::ClaudeCode,
        child(RuntimeKind::ClaudeCode, "one")
    ));
    assert!(registry.destructive_automation_allowed(
        RuntimeKind::CodexCli,
        child(RuntimeKind::CodexCli, "two")
    ));
}

#[test]
fn critical_failure_fails_readiness_and_degradation_suspends_automation() {
    let mut registry = HealthRegistry::default();
    registry.record(ComponentHealth::new(
        ComponentId::ProcessSampler,
        ComponentStatus::Degraded,
        HealthScope::Global,
    ));
    assert!(registry.is_ready());
    assert!(!registry.destructive_automation_allowed(
        RuntimeKind::CodexCli,
        child(RuntimeKind::CodexCli, "one")
    ));

    registry.record(ComponentHealth::new(
        ComponentId::ProcessSampler,
        ComponentStatus::Failed,
        HealthScope::Global,
    ));
    assert!(!registry.is_ready());
}

#[test]
fn queue_saturation_is_limited_to_the_affected_session() {
    let affected = child(RuntimeKind::ClaudeCode, "affected");
    let healthy = child(RuntimeKind::ClaudeCode, "healthy");
    let mut registry = HealthRegistry::default();
    registry.record(ComponentHealth::new(
        ComponentId::ObservationQueue,
        ComponentStatus::Degraded,
        HealthScope::Session(affected),
    ));

    assert!(!registry.destructive_automation_allowed(RuntimeKind::ClaudeCode, affected));
    assert!(registry.destructive_automation_allowed(RuntimeKind::ClaudeCode, healthy));
}
