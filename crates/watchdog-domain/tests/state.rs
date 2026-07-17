//! Detailed-state policy and compact projection contracts.

use watchdog_domain::{CompactState, DetailedState};

#[test]
fn every_detailed_state_has_the_expected_compact_projection() {
    let cases = [
        (DetailedState::Starting, CompactState::Active),
        (DetailedState::Running, CompactState::Active),
        (DetailedState::WaitingForAgent, CompactState::Waiting),
        (DetailedState::WaitingForTool, CompactState::Waiting),
        (DetailedState::WaitingForUser, CompactState::Waiting),
        (DetailedState::Idle, CompactState::Idle),
        (DetailedState::Stalled, CompactState::Stalled),
        (DetailedState::Completed, CompactState::Finished),
        (DetailedState::Failed, CompactState::Failed),
        (DetailedState::Cancelled, CompactState::Finished),
        (DetailedState::Disappeared, CompactState::Failed),
        (DetailedState::Unknown, CompactState::Unknown),
    ];

    for (detailed, expected) in cases {
        assert_eq!(detailed.compact(), expected, "projection for {detailed:?}");
    }
}

#[test]
fn waiting_for_user_never_allows_stall_or_termination_timers() {
    assert!(!DetailedState::WaitingForUser.stall_timer_runs());
    assert!(!DetailedState::WaitingForUser.termination_timer_runs());
    assert!(DetailedState::Running.stall_timer_runs());
}
