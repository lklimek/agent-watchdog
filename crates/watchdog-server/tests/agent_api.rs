//! Scoped durable parent-agent API acceptance tests.

use std::sync::Arc;

use watchdog_domain::{RuntimeKind, SessionId, SessionKind, TimePoint, WallTimeMs};
use watchdog_server::{AgentApi, AgentApiError, CompletionOutcome, RegisterSession, TransportKey};
use watchdog_store::WatchdogStore;
use watchdog_testkit::FakeClock;

async fn api_fixture() -> (AgentApi, WatchdogStore, Arc<FakeClock>) {
    let directory = tempfile::tempdir().expect("temporary directory should exist");
    let path = directory.path().join("watchdog.db");
    // Keep the directory alive for the duration of the test process. The store
    // owns open handles and each test uses a unique temp path.
    let _retained = directory.keep();
    let store = WatchdogStore::open(&path)
        .await
        .expect("database should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(10_000),
        5_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("agent API should initialize");
    (api, store, clock)
}

async fn register_main(
    api: &AgentApi,
    transport: &TransportKey,
    native_id: &str,
    event_key: &str,
) -> SessionId {
    api.register_session(
        transport,
        RegisterSession {
            runtime: RuntimeKind::ClaudeCode,
            native_id: native_id.to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: event_key.to_owned(),
        },
    )
    .await
    .expect("main should register")
    .session
    .session_id()
}

#[tokio::test]
async fn transport_binds_once_and_cross_tree_access_fails() {
    let (api, _store, _clock) = api_fixture().await;
    let transport_a = TransportKey::new("transport-a").expect("transport should be valid");
    let transport_b = TransportKey::new("transport-b").expect("transport should be valid");
    let main_a = register_main(&api, &transport_a, "main-a", "register-main-a").await;
    let main_b = register_main(&api, &transport_b, "main-b", "register-main-b").await;

    let rebind = api.bind_discovered_main(&transport_a, main_b).await;
    assert!(matches!(rebind, Err(AgentApiError::TransportAlreadyBound)));
    let cross_tree = api.get_session(&transport_a, main_b).await;
    assert!(matches!(cross_tree, Err(AgentApiError::CrossTreeAccess)));
    assert!(api.get_session(&transport_a, main_a).await.is_ok());
}

#[tokio::test]
async fn child_registration_progress_and_completion_are_idempotent_and_scoped() {
    let (api, _store, _clock) = api_fixture().await;
    let transport = TransportKey::new("transport-main").expect("transport should be valid");
    let main = register_main(&api, &transport, "main", "register-main").await;
    let child = api
        .register_session(
            &transport,
            RegisterSession {
                runtime: RuntimeKind::CodexCompanion,
                native_id: "task-1".to_owned(),
                kind: SessionKind::Child,
                parent: Some(main),
                event_key: "register-child".to_owned(),
            },
        )
        .await
        .expect("child should register");
    let child_id = child.session.session_id();

    let progressed = api
        .report_progress(
            &transport,
            child_id,
            "progress-1",
            "running cargo test".to_owned(),
            Some("verification".to_owned()),
        )
        .await
        .expect("progress should commit");
    let duplicate = api
        .report_progress(
            &transport,
            child_id,
            "progress-1",
            "running cargo test".to_owned(),
            Some("verification".to_owned()),
        )
        .await
        .expect("duplicate should be harmless");
    assert_eq!(
        progressed.snapshot.revision(),
        duplicate.snapshot.revision()
    );
    assert_eq!(
        duplicate.snapshot.last_progress_summary(),
        Some("verification: running cargo test")
    );

    let completed = api
        .complete_session(
            &transport,
            child_id,
            "complete-1",
            CompletionOutcome::Completed,
        )
        .await
        .expect("completion should commit");
    assert_eq!(
        completed.snapshot.state(),
        watchdog_domain::DetailedState::Completed
    );
    assert_eq!(
        api.list_sessions(&transport)
            .await
            .expect("tree should list")
            .len(),
        2
    );
}

#[tokio::test]
async fn durable_event_cursor_survives_api_and_transport_restart() {
    let (api, store, clock) = api_fixture().await;
    let first_transport = TransportKey::new("transport-first").expect("transport should be valid");
    let main = register_main(&api, &first_transport, "main-cursor", "register-main").await;
    api.complete_session(
        &first_transport,
        main,
        "main-completed",
        CompletionOutcome::Completed,
    )
    .await
    .expect("completion should commit");

    let page = api
        .list_events(&first_transport, None, 10)
        .await
        .expect("event should be durable");
    assert!(!page.events.is_empty());
    assert!(
        page.events
            .iter()
            .all(|event| event.session.session.session_id() == main)
    );
    assert_eq!(
        page.events
            .last()
            .expect("completion event should exist")
            .session
            .snapshot
            .state(),
        watchdog_domain::DetailedState::Completed
    );
    let confirmed = page.next_cursor;
    let acknowledged = api
        .list_events(&first_transport, Some(confirmed), 10)
        .await
        .expect("cursor confirmation should persist");
    assert!(acknowledged.events.is_empty());

    let restarted = AgentApi::new(store, clock)
        .await
        .expect("API should restart from store");
    let second_transport =
        TransportKey::new("transport-second").expect("transport should be valid");
    restarted
        .bind_discovered_main(&second_transport, main)
        .await
        .expect("autodiscovered main should bind after restart");
    let after_restart = restarted
        .list_events(&second_transport, None, 10)
        .await
        .expect("durable cursor should load");
    assert_eq!(after_restart.after, confirmed);
    assert!(after_restart.events.is_empty());
}
