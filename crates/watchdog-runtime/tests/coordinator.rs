#![cfg(target_os = "linux")]
//! Per-session admission and transactional coordination contracts.

use std::sync::Arc;

use watchdog_domain::{
    AdapterIdentity, DetailedState, EvidenceTrust, MainSessionId, NativeSessionKey,
    ObservationEnvelope, ObservationId, ObservationPayload, ObservationSource, ReducerPolicy,
    RuntimeKind, SessionId, SessionIdentity, SessionSnapshot, TimePoint, WallTimeMs,
};
use watchdog_runtime::{
    AdmissionError, EventSequence, ObservationClass, SessionCoordinator, SessionQueue,
};
use watchdog_store::{ApplyResult, OutboxDestination, WatchdogStore};

#[test]
fn activity_coalesces_but_terminal_admission_is_preserved() {
    let mut queue = SessionQueue::new(2).expect("queue capacity should be valid");
    queue
        .try_push(ObservationClass::Activity, "activity-1")
        .expect("first activity should fit");
    queue
        .try_push(ObservationClass::Activity, "activity-2")
        .expect("new activity should coalesce");
    queue
        .try_push(ObservationClass::Durable, "terminal")
        .expect("terminal evidence should retain admission capacity");

    assert_eq!(queue.len(), 2);
    assert_eq!(queue.pop(), Some("activity-2"));
    assert_eq!(queue.pop(), Some("terminal"));
}

#[test]
fn full_durable_queue_backpressures_without_losing_the_item() {
    let mut queue = SessionQueue::new(1).expect("queue capacity should be valid");
    queue
        .try_push(ObservationClass::Durable, "waiting-user")
        .expect("first durable event should fit");
    let error = queue
        .try_push(ObservationClass::Durable, "failed")
        .expect_err("durable evidence must be returned to the producer");

    assert_eq!(error, AdmissionError::Backpressure("failed"));
    assert!(queue.is_degraded());
    assert_eq!(queue.pop(), Some("waiting-user"));
}

#[tokio::test]
async fn coordinator_commits_observation_rich_snapshot_and_events_atomically() {
    let directory = tempfile::tempdir().expect("temporary database directory should exist");
    let store = WatchdogStore::open(&directory.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "coordinator-child")
        .expect("native key should be valid");
    let identity = SessionIdentity::Child(watchdog_domain::ChildSessionId::from(
        SessionId::from_native(&native),
    ));
    let root_native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "coordinator-main")
        .expect("root native key should be valid");
    let root = MainSessionId::from(SessionId::from_native(&root_native));
    let snapshot = SessionSnapshot::new(identity, root, TimePoint::new(WallTimeMs::new(0), 0));
    let mut coordinator = SessionCoordinator::new(
        store.clone(),
        snapshot,
        ReducerPolicy::default(),
        Arc::new(EventSequence::new(1)),
        [OutboxDestination::ParentInbox],
    );
    let observation = fixture_observation(native, "running-1", DetailedState::Running);

    assert_eq!(
        coordinator
            .apply_observation(observation.clone())
            .await
            .expect("reduction should commit"),
        ApplyResult::Applied,
    );
    let stored = store
        .snapshot(identity)
        .await
        .expect("snapshot query should succeed")
        .expect("snapshot should exist");
    assert_eq!(stored.state(), DetailedState::Running);
    assert_eq!(
        stored
            .reducer_snapshot()
            .expect("rich reducer state should persist")
            .state(),
        DetailedState::Running,
    );
    assert_eq!(store.counts().await.expect("counts should load").events, 1);

    assert_eq!(
        coordinator
            .apply_observation(observation)
            .await
            .expect("duplicate should be harmless"),
        ApplyResult::Duplicate,
    );
    assert_eq!(store.counts().await.expect("counts should load").events, 1);
}

#[tokio::test]
async fn scheduler_tick_commits_stall_once_and_duplicate_tick_is_idempotent() {
    let directory = tempfile::tempdir().expect("temporary database directory should exist");
    let store = WatchdogStore::open(&directory.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "scheduled-child")
        .expect("native key should be valid");
    let identity = SessionIdentity::Child(watchdog_domain::ChildSessionId::from(
        SessionId::from_native(&native),
    ));
    let root_native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "scheduled-main")
        .expect("root native key should be valid");
    let snapshot = SessionSnapshot::new(
        identity,
        MainSessionId::from(SessionId::from_native(&root_native)),
        TimePoint::new(WallTimeMs::new(0), 0),
    );
    let mut coordinator = SessionCoordinator::new(
        store.clone(),
        snapshot,
        ReducerPolicy::default(),
        Arc::new(EventSequence::new(1)),
        [OutboxDestination::ParentInbox],
    );
    let tick = fixture_tick(native, "stall-15m", 15 * 60_000);

    assert_eq!(
        coordinator
            .apply_tick(tick.clone())
            .await
            .expect("stall tick should commit"),
        ApplyResult::Applied,
    );
    assert_eq!(coordinator.snapshot().state(), DetailedState::Stalled);
    let event_count = store.counts().await.expect("counts should load").events;
    assert_eq!(event_count, 3);

    assert_eq!(
        coordinator
            .apply_tick(tick)
            .await
            .expect("duplicate tick should be harmless"),
        ApplyResult::Duplicate,
    );
    assert_eq!(
        store.counts().await.expect("counts should load").events,
        event_count
    );
}

#[tokio::test]
async fn event_sequence_resumes_after_the_highest_durable_event() {
    let directory = tempfile::tempdir().expect("temporary database directory should exist");
    let store = WatchdogStore::open(&directory.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "restart-child")
        .expect("native key should be valid");
    let identity = SessionIdentity::Child(watchdog_domain::ChildSessionId::from(
        SessionId::from_native(&native),
    ));
    let root_native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "restart-main")
        .expect("root native key should be valid");
    let snapshot = SessionSnapshot::new(
        identity,
        MainSessionId::from(SessionId::from_native(&root_native)),
        TimePoint::new(WallTimeMs::new(0), 0),
    );
    let mut first = SessionCoordinator::new(
        store.clone(),
        snapshot,
        ReducerPolicy::default(),
        Arc::new(EventSequence::new(1)),
        [OutboxDestination::ParentInbox],
    );
    first
        .apply_observation(fixture_observation(
            native.clone(),
            "running-before-restart",
            DetailedState::Running,
        ))
        .await
        .expect("first transition should commit");
    let persisted = store
        .snapshot(identity)
        .await
        .expect("snapshot should load")
        .and_then(|value| value.reducer_snapshot().cloned())
        .expect("rich snapshot should survive restart");

    let sequence = Arc::new(
        EventSequence::from_store(&store)
            .await
            .expect("sequence should resume"),
    );
    let mut resumed = SessionCoordinator::new(
        store.clone(),
        persisted,
        ReducerPolicy::default(),
        sequence,
        [OutboxDestination::ParentInbox],
    );
    resumed
        .apply_observation(fixture_observation(
            native,
            "failed-after-restart",
            DetailedState::Failed,
        ))
        .await
        .expect("post-restart IDs must not conflict");

    assert_eq!(store.counts().await.expect("counts should load").events, 3);
}

fn fixture_observation(
    native: NativeSessionKey,
    key: &str,
    state: DetailedState,
) -> ObservationEnvelope {
    ObservationEnvelope::new(
        ObservationId::from_native(RuntimeKind::ClaudeCode, "coordinator-test", key)
            .expect("observation ID should be valid"),
        native,
        TimePoint::new(WallTimeMs::new(1_000), 1_000),
        ObservationSource::new(
            AdapterIdentity::new(RuntimeKind::ClaudeCode, "test").expect("adapter should be valid"),
            "synthetic",
            EvidenceTrust::Authoritative,
            None,
        )
        .expect("source should be valid"),
        ObservationPayload::NativeState(state),
    )
    .expect("observation should be valid")
}

fn fixture_tick(native: NativeSessionKey, key: &str, milliseconds: u64) -> ObservationEnvelope {
    ObservationEnvelope::new(
        ObservationId::from_native(RuntimeKind::ClaudeCode, "scheduler", key)
            .expect("observation ID should be valid"),
        native,
        TimePoint::new(
            WallTimeMs::new(i64::try_from(milliseconds).expect("fixture time should fit")),
            milliseconds,
        ),
        ObservationSource::new(
            AdapterIdentity::new(RuntimeKind::ClaudeCode, "scheduler-test")
                .expect("adapter should be valid"),
            "scheduler",
            EvidenceTrust::Authoritative,
            None,
        )
        .expect("source should be valid"),
        ObservationPayload::SchedulerTick,
    )
    .expect("tick should be valid")
}
