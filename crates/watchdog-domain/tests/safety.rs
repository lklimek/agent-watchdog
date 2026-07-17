//! Compile-time and runtime child-only safety contracts.

use uuid::Uuid;
use watchdog_domain::{ChildSessionId, MainSessionId, SessionId, TerminationCandidate};

#[test]
fn termination_candidate_requires_a_child_id() {
    let raw = SessionId::from_uuid(Uuid::from_u128(7));
    let child = ChildSessionId::from(raw);
    let main = MainSessionId::from(raw);

    assert_eq!(TerminationCandidate::new(child).session_id(), child);
    assert_ne!(format!("{child:?}"), format!("{main:?}"));
}
