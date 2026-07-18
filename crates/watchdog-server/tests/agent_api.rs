//! Scoped durable parent-agent API acceptance tests.

use std::sync::Arc;

use watchdog_domain::{
    AdapterIdentity, Clock, DetailedState, DurationMs, EvidenceTrust, NativeSessionKey,
    ObservationEnvelope, ObservationId, ObservationPayload, ObservationSource, RuntimeKind,
    SessionId, SessionKind, TimePoint, WallTimeMs,
};
use watchdog_server::{
    AgentApi, AgentApiError, CompletionOutcome, DiscoveredSession, RegisterSession, TransportKey,
};
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
    let conflicting_retry = api
        .report_progress(
            &transport,
            child_id,
            "progress-1",
            "running a different command".to_owned(),
            Some("verification".to_owned()),
        )
        .await;
    assert!(
        conflicting_retry.is_err(),
        "reusing an event key with a different payload must remain an error"
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
async fn native_discovery_is_idempotent_persists_metadata_and_remains_mcp_bindable() {
    let (api, store, clock) = api_fixture().await;
    let main = api
        .discover_session(DiscoveredSession {
            runtime: RuntimeKind::CodexCli,
            native_id: "discovered-main".to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: "codex-state:discovered-main".to_owned(),
            title: Some("Native title".to_owned()),
            startup_directory: Some("/work/repository".to_owned()),
        })
        .await
        .expect("main should be auto-discovered");
    let child_request = DiscoveredSession {
        runtime: RuntimeKind::CodexCli,
        native_id: "discovered-child".to_owned(),
        kind: SessionKind::Child,
        parent: Some(main.session.session_id()),
        event_key: "codex-state:discovered-child".to_owned(),
        title: Some("Native child".to_owned()),
        startup_directory: Some("/work/repository-child".to_owned()),
    };
    let child = api
        .discover_session(child_request.clone())
        .await
        .expect("child should be auto-discovered");
    let duplicate = api
        .discover_session(child_request)
        .await
        .expect("repeated discovery should be harmless");
    assert_eq!(child.snapshot.revision(), duplicate.snapshot.revision());

    clock.advance(DurationMs::new(1_000));
    let restarted = AgentApi::new(store.clone(), clock)
        .await
        .expect("API should restart from store");
    let rediscovered_main = restarted
        .discover_session(DiscoveredSession {
            runtime: RuntimeKind::CodexCli,
            native_id: "discovered-main".to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: "codex-state:discovered-main".to_owned(),
            title: Some("Native title".to_owned()),
            startup_directory: Some("/work/repository".to_owned()),
        })
        .await
        .expect("main rediscovery after restart should be harmless");
    restarted
        .discover_session(DiscoveredSession {
            runtime: RuntimeKind::CodexCli,
            native_id: "discovered-child".to_owned(),
            kind: SessionKind::Child,
            parent: Some(rediscovered_main.session.session_id()),
            event_key: "codex-state:discovered-child".to_owned(),
            title: Some("Native child".to_owned()),
            startup_directory: Some("/work/repository-child".to_owned()),
        })
        .await
        .expect("child rediscovery after restart should be harmless");

    let metadata = store
        .session_metadata(child.session)
        .await
        .expect("metadata query should succeed")
        .expect("metadata should exist");
    assert_eq!(metadata.title(), Some("Native child"));
    assert_eq!(metadata.startup_directory(), Some("/work/repository-child"));

    let transport = TransportKey::new("discovered-parent-mcp").expect("valid transport");
    api.bind_discovered_main(&transport, main.session.session_id())
        .await
        .expect("MCP should bind an auto-discovered main");
    assert_eq!(
        api.list_sessions(&transport)
            .await
            .expect("discovered tree should list")
            .len(),
        2
    );
}

#[tokio::test]
async fn native_observation_ingestion_preserves_provenance_and_retry_idempotency() {
    let (api, store, clock) = api_fixture().await;
    let native = NativeSessionKey::new(RuntimeKind::CodexCompanion, "native-job")
        .expect("native identity should be valid");
    api.discover_session(DiscoveredSession {
        runtime: native.runtime(),
        native_id: native.native_id().to_owned(),
        kind: SessionKind::Main,
        parent: None,
        event_key: "companion:discover:native-job".to_owned(),
        title: Some("Native job".to_owned()),
        startup_directory: Some("/work/repository".to_owned()),
    })
    .await
    .expect("session should be discovered before evidence arrives");

    let observation_id =
        ObservationId::from_native(RuntimeKind::CodexCompanion, "state", "native-job:running")
            .expect("observation identity should be valid");
    let observation = native_state_observation(
        &native,
        observation_id,
        clock.now(),
        DetailedState::WaitingForTool,
    );
    let applied = api
        .ingest_native_observation(observation)
        .await
        .expect("native evidence should apply");
    assert_eq!(applied.snapshot.state(), DetailedState::WaitingForTool);

    clock.advance(DurationMs::new(1_000));
    let restarted = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should restart");
    let duplicate = restarted
        .ingest_native_observation(native_state_observation(
            &native,
            observation_id,
            clock.now(),
            DetailedState::WaitingForTool,
        ))
        .await
        .expect("same native event should remain idempotent after restart");
    assert_eq!(duplicate.snapshot.revision(), applied.snapshot.revision());

    let stored = store
        .observation(observation_id)
        .await
        .expect("observation query should succeed")
        .expect("native observation should be durable");
    assert_eq!(stored.source().adapter().version(), "1.0.6");
    assert_eq!(stored.source().trust(), EvidenceTrust::Authoritative);
}

#[tokio::test]
async fn restart_reconciliation_resets_process_local_monotonic_ordering() {
    let (api, store, clock) = api_fixture().await;
    let native = NativeSessionKey::new(RuntimeKind::CodexCompanion, "restart-job")
        .expect("native identity should be valid");
    api.discover_session(DiscoveredSession {
        runtime: native.runtime(),
        native_id: native.native_id().to_owned(),
        kind: SessionKind::Main,
        parent: None,
        event_key: "restart-job:discovered".to_owned(),
        title: None,
        startup_directory: None,
    })
    .await
    .expect("session should be discovered");
    let first_id =
        ObservationId::from_native(RuntimeKind::CodexCompanion, "state", "restart-job:first")
            .expect("observation identity should be valid");
    api.ingest_native_observation(native_state_observation(
        &native,
        first_id,
        TimePoint::new(WallTimeMs::new(20_000), 700_000),
        DetailedState::Running,
    ))
    .await
    .expect("pre-restart evidence should apply");

    clock.set(TimePoint::new(WallTimeMs::new(30_000), 5));
    let restarted = AgentApi::new(store, clock.clone())
        .await
        .expect("API should restart");
    restarted
        .mark_restarted()
        .await
        .expect("all retained sessions should be marked for reconciliation");
    let fresh_id =
        ObservationId::from_native(RuntimeKind::CodexCompanion, "state", "restart-job:fresh")
            .expect("observation identity should be valid");
    let reconciled = restarted
        .ingest_native_observation(native_state_observation(
            &native,
            fresh_id,
            clock.now(),
            DetailedState::Running,
        ))
        .await
        .expect("fresh low-monotonic evidence should apply after restart");
    assert!(!reconciled.snapshot.reconciliation_required());
    assert!(reconciled.snapshot.revision() >= 4);
}

fn native_state_observation(
    native: &NativeSessionKey,
    id: ObservationId,
    observed_at: TimePoint,
    state: DetailedState,
) -> ObservationEnvelope {
    ObservationEnvelope::new(
        id,
        native.clone(),
        observed_at,
        ObservationSource::new(
            AdapterIdentity::new(RuntimeKind::CodexCompanion, "1.0.6")
                .expect("adapter should be valid"),
            "state:summary",
            EvidenceTrust::Authoritative,
            None,
        )
        .expect("source should be valid"),
        ObservationPayload::NativeState(state),
    )
    .expect("observation should be valid")
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
