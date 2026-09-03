//! Scoped durable parent-agent API acceptance tests.

use std::{fs, path::Path, process::Command, sync::Arc};

use sqlx::sqlite::SqliteConnectOptions;
use watchdog_companion::CompanionParser;
use watchdog_domain::{
    AdapterIdentity, BoundedText, Clock, CorrelationBasis, DeadlineCommand, DetailedState,
    DomainEventKind, DurationMs, EvidenceTrust, MainSessionId, NativeSessionKey,
    ObservationEnvelope, ObservationId, ObservationPayload, ObservationSource, ProcessId,
    ProcessIdentity, RuntimeKind, SessionId, SessionKind, TimePoint, WallTimeMs,
};
use watchdog_server::{
    AgentApi, AgentApiError, AgentEventView, CompletionOutcome, DiscoveredSession, RegisterSession,
    RepositoryMetadata, TransportKey, WaitingKind,
};
use watchdog_store::{
    ActivityEvidence, ActivitySampleRecord, AdapterHealthRecord, AdapterHealthStatus,
    OutboxDestination, WatchdogStore,
};
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

async fn register_child(
    api: &AgentApi,
    transport: &TransportKey,
    parent: SessionId,
    native_id: &str,
    event_key: &str,
) -> SessionId {
    api.register_session(
        transport,
        RegisterSession {
            runtime: RuntimeKind::CodexCompanion,
            native_id: native_id.to_owned(),
            kind: SessionKind::Child,
            parent: Some(parent),
            event_key: event_key.to_owned(),
        },
    )
    .await
    .expect("child should register")
    .session
    .session_id()
}

#[tokio::test]
async fn session_views_include_server_time_and_latest_evidence_provenance() {
    let (api, _store, clock) = api_fixture().await;
    let transport = TransportKey::new("response-contract").expect("transport should be valid");

    let registered = api
        .register_session(
            &transport,
            RegisterSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: "response-main".to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "register-response-main".to_owned(),
            },
        )
        .await
        .expect("main should register");

    assert_eq!(registered.server_time, clock.now().wall_time());
    let provenance = registered
        .provenance
        .expect("the latest accepted observation should retain provenance");
    assert_eq!(provenance.adapter().runtime(), RuntimeKind::ClaudeCode);
    assert_eq!(provenance.fingerprint(), "mcp:register_session");
}

#[tokio::test]
async fn register_delegation_updates_relation_and_rejects_main_as_child() {
    let (api, _store, _clock) = api_fixture().await;
    let transport = TransportKey::new("delegation-tool").expect("transport should be valid");
    let main = register_main(&api, &transport, "delegation-main", "register-main").await;
    let child = register_child(&api, &transport, main, "delegation-child", "register-child").await;

    let delegated = api
        .register_delegation(&transport, main, child, "delegate-child", None)
        .await
        .expect("child delegation should succeed");
    assert_eq!(delegated.session.session_id(), child);
    let tree = api
        .session_tree(&transport)
        .await
        .expect("session tree should load");
    assert!(tree.relations.iter().any(|relation| {
        relation.selected
            && relation.child.session_id() == child
            && relation.parent.session_id() == main
            && relation
                .provenance
                .fingerprint()
                .starts_with("mcp:register_delegation:")
    }));
    let deadline = WallTimeMs::new(90_000);
    let delegated_with_deadline = api
        .register_delegation(
            &transport,
            main,
            child,
            "delegate-child-with-deadline",
            Some(DeadlineCommand::Set(deadline)),
        )
        .await
        .expect("child delegation with a deadline should succeed");
    assert_eq!(
        delegated_with_deadline.snapshot.explicit_deadline(),
        Some(deadline)
    );

    let rejected = api
        .register_delegation(&transport, main, main, "delegate-main", None)
        .await;
    assert!(matches!(rejected, Err(AgentApiError::ChildSessionRequired)));
}

#[tokio::test]
async fn update_deadline_supports_set_pause_resume_and_clear() {
    let (api, _store, _clock) = api_fixture().await;
    let transport = TransportKey::new("deadline-tool").expect("transport should be valid");
    let main = register_main(&api, &transport, "deadline-main", "register-main").await;
    let child = register_child(&api, &transport, main, "deadline-child", "register-child").await;
    let deadline = WallTimeMs::new(60_000);

    let set = api
        .update_deadline(
            &transport,
            child,
            "deadline-set",
            DeadlineCommand::Set(deadline),
        )
        .await
        .expect("deadline should set");
    assert_eq!(set.snapshot.explicit_deadline(), Some(deadline));

    let paused = api
        .update_deadline(&transport, child, "deadline-pause", DeadlineCommand::Pause)
        .await
        .expect("deadline timers should pause");
    assert!(paused.snapshot.timers_paused());

    let resumed = api
        .update_deadline(
            &transport,
            child,
            "deadline-resume",
            DeadlineCommand::Resume,
        )
        .await
        .expect("deadline timers should resume");
    assert!(!resumed.snapshot.timers_paused());

    let cleared = api
        .update_deadline(&transport, child, "deadline-clear", DeadlineCommand::Clear)
        .await
        .expect("deadline should clear");
    assert_eq!(cleared.snapshot.explicit_deadline(), None);
}

#[tokio::test]
async fn session_tree_returns_bound_sessions_and_selected_relations() {
    let (api, _store, _clock) = api_fixture().await;
    let transport = TransportKey::new("tree-tool").expect("transport should be valid");
    let main = register_main(&api, &transport, "tree-main", "register-main").await;
    let child = register_child(&api, &transport, main, "tree-child", "register-child").await;

    let tree = api
        .session_tree(&transport)
        .await
        .expect("session tree should load");

    assert_eq!(tree.root.session_id(), main);
    assert_eq!(tree.sessions.len(), 2);
    assert!(tree.relations.iter().any(|relation| {
        relation.selected
            && relation.child.session_id() == child
            && relation.parent.session_id() == main
    }));
}

#[tokio::test]
async fn watchdog_health_returns_each_persisted_runtime_adapter() {
    let (api, store, clock) = api_fixture().await;
    let transport = TransportKey::new("health-tool").expect("transport should be valid");
    register_main(&api, &transport, "health-main", "register-main").await;
    for runtime in [
        RuntimeKind::ClaudeCode,
        RuntimeKind::CodexCli,
        RuntimeKind::CodexCompanion,
    ] {
        store
            .save_adapter_health(&AdapterHealthRecord {
                adapter: AdapterIdentity::new(runtime, "test").expect("adapter should be valid"),
                status: AdapterHealthStatus::Healthy,
                last_success: Some(WallTimeMs::new(10_000)),
                last_error: None,
                affected_scope: None,
                message: None,
            })
            .await
            .expect("adapter health should persist");
    }

    let health = api
        .health(&transport)
        .await
        .expect("watchdog health should load");

    assert_eq!(health.server_time, clock.now().wall_time());
    assert!(health.store_wal);
    assert!(health.store_foreign_keys);
    assert_eq!(health.adapters.len(), 3);
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
async fn a_spawned_agent_registers_itself_as_a_child_on_its_own_transport() {
    let (api, _store, _clock) = api_fixture().await;
    let coordinator =
        TransportKey::new("self-register-coordinator").expect("transport should be valid");
    let spawned = TransportKey::new("self-register-spawned").expect("transport should be valid");
    let main = register_main(
        &api,
        &coordinator,
        "self-register-main",
        "register-self-main",
    )
    .await;

    let child = api
        .register_session(
            &spawned,
            RegisterSession {
                runtime: RuntimeKind::CodexCompanion,
                native_id: "self-register-child".to_owned(),
                kind: SessionKind::Child,
                parent: Some(main),
                event_key: "register-self-child".to_owned(),
            },
        )
        .await
        .expect("a spawned agent should register itself against its parent session id");

    assert_eq!(child.root, MainSessionId::from(main));
    let observed = api
        .get_session(&spawned, child.session.session_id())
        .await
        .expect("child self-registration should bind the child's own transport");
    assert_eq!(observed.session, child.session);
    assert!(
        api.get_session(&coordinator, child.session.session_id())
            .await
            .is_ok(),
        "the coordinator keeps its own scope over the child it spawned"
    );
}

#[tokio::test]
async fn a_nested_child_registers_itself_against_its_actual_child_parent() {
    let (api, store, _clock) = api_fixture().await;
    let coordinator = TransportKey::new("nested-coordinator").expect("transport should be valid");
    let child_transport = TransportKey::new("nested-child").expect("transport should be valid");
    let grandchild_transport =
        TransportKey::new("nested-grandchild").expect("transport should be valid");
    let main = register_main(&api, &coordinator, "nested-main", "register-nested-main").await;
    let child = register_child(
        &api,
        &child_transport,
        main,
        "nested-parent-child",
        "register-nested-parent-child",
    )
    .await;

    let grandchild = api
        .register_session(
            &grandchild_transport,
            RegisterSession {
                runtime: RuntimeKind::CodexCompanion,
                native_id: "nested-grandchild".to_owned(),
                kind: SessionKind::Child,
                parent: Some(child),
                event_key: "register-nested-grandchild".to_owned(),
            },
        )
        .await
        .expect("a nested child should register against its actual child parent");

    assert_eq!(grandchild.root, MainSessionId::from(main));
    assert!(
        api.get_session(&grandchild_transport, child).await.is_ok(),
        "the nested child's transport should bind to the complete root tree"
    );
    let relations = store
        .relations_for_root(MainSessionId::from(main), 10)
        .await
        .expect("relations should load");
    assert!(relations.iter().any(|relation| {
        relation.selected
            && relation.child.session_id() == grandchild.session.session_id()
            && relation.parent.session_id() == child
            && relation.root == MainSessionId::from(main)
    }));

    let foreign_transport = TransportKey::new("nested-foreign").expect("transport should be valid");
    register_main(
        &api,
        &foreign_transport,
        "nested-foreign-main",
        "register-nested-foreign-main",
    )
    .await;
    let rejected = api
        .register_session(
            &foreign_transport,
            RegisterSession {
                runtime: RuntimeKind::CodexCompanion,
                native_id: "nested-cross-tree-grandchild".to_owned(),
                kind: SessionKind::Child,
                parent: Some(child),
                event_key: "register-nested-cross-tree-grandchild".to_owned(),
            },
        )
        .await;
    assert!(matches!(rejected, Err(AgentApiError::CrossTreeAccess)));
}

#[tokio::test]
async fn an_exact_registration_retry_repairs_a_relation_after_partial_persistence() {
    let directory = tempfile::tempdir().expect("temporary directory should exist");
    let path = directory.path().join("watchdog.db");
    let store = WatchdogStore::open(&path)
        .await
        .expect("database should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(10_000),
        5_000,
    )));
    let api = AgentApi::new(store.clone(), clock)
        .await
        .expect("agent API should initialize");
    let coordinator =
        TransportKey::new("retry-repair-coordinator").expect("transport should be valid");
    let child_transport =
        TransportKey::new("retry-repair-child").expect("transport should be valid");
    let main = register_main(
        &api,
        &coordinator,
        "retry-repair-main",
        "register-retry-repair-main",
    )
    .await;
    let pool = sqlx::SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(false),
    )
    .await
    .expect("fault injection connection should open");
    sqlx::query(
        "CREATE TRIGGER fail_relation_insert BEFORE INSERT ON session_relations \
         BEGIN SELECT RAISE(FAIL, 'injected relation failure'); END",
    )
    .execute(&pool)
    .await
    .expect("fault injection trigger should install");
    let request = RegisterSession {
        runtime: RuntimeKind::CodexCompanion,
        native_id: "retry-repair-child".to_owned(),
        kind: SessionKind::Child,
        parent: Some(main),
        event_key: "register-retry-repair-child".to_owned(),
    };

    assert!(
        matches!(
            api.register_session(&child_transport, request.clone())
                .await,
            Err(AgentApiError::Store(watchdog_store::StoreError::Sqlx(_)))
        ),
        "the injected relation failure should surface after observation persistence"
    );
    assert!(
        matches!(
            api.get_session(&child_transport, main).await,
            Err(AgentApiError::TransportNotBound)
        ),
        "a partial persistence failure must leave the child transport unauthorized"
    );
    sqlx::query("DROP TRIGGER fail_relation_insert")
        .execute(&pool)
        .await
        .expect("fault injection trigger should be removed");

    let retried = api
        .register_session(&child_transport, request)
        .await
        .expect("the exact retry should repair the relation and bind the transport");
    let relations = store
        .relations_for_root(MainSessionId::from(main), 10)
        .await
        .expect("relations should load");
    assert!(relations.iter().any(|relation| {
        relation.selected
            && relation.child.session_id() == retried.session.session_id()
            && relation.parent.session_id() == main
    }));
}

#[tokio::test]
async fn a_delayed_exact_registration_retry_does_not_restore_an_older_parent() {
    let (api, store, clock) = api_fixture().await;
    let coordinator =
        TransportKey::new("delayed-retry-coordinator").expect("transport should be valid");
    let alternate_parent_transport =
        TransportKey::new("delayed-retry-parent").expect("transport should be valid");
    let child_transport =
        TransportKey::new("delayed-retry-child").expect("transport should be valid");
    let main = register_main(
        &api,
        &coordinator,
        "delayed-retry-main",
        "register-delayed-retry-main",
    )
    .await;
    let alternate_parent = register_child(
        &api,
        &alternate_parent_transport,
        main,
        "delayed-retry-alternate-parent",
        "register-delayed-retry-alternate-parent",
    )
    .await;
    let original = RegisterSession {
        runtime: RuntimeKind::CodexCompanion,
        native_id: "delayed-retry-target".to_owned(),
        kind: SessionKind::Child,
        parent: Some(main),
        event_key: "register-delayed-retry-target-original".to_owned(),
    };
    let child = api
        .register_session(&child_transport, original.clone())
        .await
        .expect("the original parent should register")
        .session
        .session_id();
    clock.advance(DurationMs::new(1_000));
    api.register_session(
        &child_transport,
        RegisterSession {
            runtime: RuntimeKind::CodexCompanion,
            native_id: "delayed-retry-target".to_owned(),
            kind: SessionKind::Child,
            parent: Some(alternate_parent),
            event_key: "register-delayed-retry-target-new-parent".to_owned(),
        },
    )
    .await
    .expect("a newer event should select the new parent");
    clock.advance(DurationMs::new(1_000));

    api.register_session(&child_transport, original)
        .await
        .expect("an exact retry of the older event should be an idempotent success");

    let selected = store
        .relations_for_root(MainSessionId::from(main), 10)
        .await
        .expect("relations should load")
        .into_iter()
        .find(|relation| relation.selected && relation.child.session_id() == child)
        .expect("the child should retain one selected parent");
    assert_eq!(selected.parent.session_id(), alternate_parent);
}

#[tokio::test]
async fn a_registration_event_cannot_be_reused_for_a_different_parent() {
    let (api, store, _clock) = api_fixture().await;
    let coordinator =
        TransportKey::new("event-reuse-coordinator").expect("transport should be valid");
    let alternate_parent_transport =
        TransportKey::new("event-reuse-parent").expect("transport should be valid");
    let child_transport =
        TransportKey::new("event-reuse-child").expect("transport should be valid");
    let main = register_main(
        &api,
        &coordinator,
        "event-reuse-main",
        "register-event-reuse-main",
    )
    .await;
    let alternate_parent = register_child(
        &api,
        &alternate_parent_transport,
        main,
        "event-reuse-alternate-parent",
        "register-event-reuse-alternate-parent",
    )
    .await;
    let event_key = "register-event-reuse-target";
    let child = register_child(
        &api,
        &child_transport,
        main,
        "event-reuse-target",
        event_key,
    )
    .await;

    let conflicting = api
        .register_session(
            &child_transport,
            RegisterSession {
                runtime: RuntimeKind::CodexCompanion,
                native_id: "event-reuse-target".to_owned(),
                kind: SessionKind::Child,
                parent: Some(alternate_parent),
                event_key: event_key.to_owned(),
            },
        )
        .await;
    assert!(matches!(
        conflicting,
        Err(AgentApiError::RelationEventConflict)
    ));

    let selected = store
        .relations_for_root(MainSessionId::from(main), 10)
        .await
        .expect("relations should load")
        .into_iter()
        .find(|relation| relation.selected && relation.child.session_id() == child)
        .expect("the child should retain one selected parent");
    assert_eq!(selected.parent.session_id(), main);
}

#[tokio::test]
async fn a_coordinator_registered_child_binds_its_own_transport_by_re_registering() {
    let (api, _store, _clock) = api_fixture().await;
    let coordinator = TransportKey::new("rebind-coordinator").expect("transport should be valid");
    let spawned = TransportKey::new("rebind-spawned").expect("transport should be valid");
    let main = register_main(&api, &coordinator, "rebind-main", "register-rebind-main").await;
    let child = register_child(
        &api,
        &coordinator,
        main,
        "rebind-child",
        "register-rebind-child",
    )
    .await;

    let re_registered = api
        .register_session(
            &spawned,
            RegisterSession {
                runtime: RuntimeKind::CodexCompanion,
                native_id: "rebind-child".to_owned(),
                kind: SessionKind::Child,
                parent: Some(main),
                event_key: "re-register-rebind-child".to_owned(),
            },
        )
        .await
        .expect("an already registered child should re-register from its own transport");

    assert_eq!(re_registered.session.session_id(), child);
    assert!(
        api.get_session(&spawned, child).await.is_ok(),
        "re-registration should bind the child's own transport"
    );
}

#[tokio::test]
async fn a_bound_transport_cannot_register_a_child_into_another_tree() {
    let (api, store, _clock) = api_fixture().await;
    let transport_a = TransportKey::new("child-cross-tree-a").expect("transport should be valid");
    let transport_b = TransportKey::new("child-cross-tree-b").expect("transport should be valid");
    let main_a = register_main(&api, &transport_a, "child-cross-a", "register-cross-a").await;
    register_main(&api, &transport_b, "child-cross-b", "register-cross-b").await;
    let native = NativeSessionKey::new(RuntimeKind::CodexCompanion, "child-cross-target")
        .expect("child identity should validate");

    let rejected = api
        .register_session(
            &transport_b,
            RegisterSession {
                runtime: native.runtime(),
                native_id: native.native_id().to_owned(),
                kind: SessionKind::Child,
                parent: Some(main_a),
                event_key: "register-cross-child".to_owned(),
            },
        )
        .await;

    assert!(matches!(rejected, Err(AgentApiError::CrossTreeAccess)));
    assert!(
        store
            .session_by_id(SessionId::from_native(&native))
            .await
            .expect("child lookup should succeed")
            .is_none(),
        "a rejected cross-tree child must not be persisted"
    );
}

#[tokio::test]
async fn a_rejected_child_registration_leaves_its_transport_unbound() {
    let (api, _store, _clock) = api_fixture().await;
    let coordinator = TransportKey::new("conflict-coordinator").expect("transport should be valid");
    let claimant = TransportKey::new("conflict-claimant").expect("transport should be valid");
    let spawned = TransportKey::new("conflict-spawned").expect("transport should be valid");
    let main = register_main(
        &api,
        &coordinator,
        "conflict-main",
        "register-conflict-main",
    )
    .await;
    register_main(
        &api,
        &claimant,
        "conflict-native",
        "register-conflict-claimed",
    )
    .await;

    let rejected = api
        .register_session(
            &spawned,
            RegisterSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: "conflict-native".to_owned(),
                kind: SessionKind::Child,
                parent: Some(main),
                event_key: "register-conflict-child".to_owned(),
            },
        )
        .await;

    assert!(matches!(
        rejected,
        Err(AgentApiError::SessionIdentityConflict)
    ));
    assert!(
        matches!(
            api.get_session(&spawned, main).await,
            Err(AgentApiError::TransportNotBound)
        ),
        "a rejected child registration must not grant scope over the parent tree"
    );
}

#[tokio::test]
async fn rejected_main_rebinds_do_not_mutate_other_trees() {
    let (api, store, clock) = api_fixture().await;
    let transport = TransportKey::new("bound-transport").expect("transport should validate");
    register_main(&api, &transport, "bound-main", "register-bound-main").await;

    let direct_native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "rejected-direct-main")
        .expect("direct identity should validate");
    let direct = api
        .register_session(
            &transport,
            RegisterSession {
                runtime: direct_native.runtime(),
                native_id: direct_native.native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "reject-direct-main".to_owned(),
            },
        )
        .await;
    assert!(matches!(direct, Err(AgentApiError::TransportAlreadyBound)));
    assert!(
        store
            .session_by_id(SessionId::from_native(&direct_native))
            .await
            .expect("direct target lookup should succeed")
            .is_none(),
        "rejected direct registration must not create another tree"
    );

    let canonical = api
        .discover_session(DiscoveredSession {
            runtime: RuntimeKind::ClaudeCode,
            native_id: "other-canonical-main".to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: "discover-other-canonical".to_owned(),
            adapter_version: "test".to_owned(),
            evidence_source: "test:other-canonical".to_owned(),
            title: None,
            startup_directory: None,
        })
        .await
        .expect("other canonical main should be discovered");
    let alias = NativeSessionKey::new(RuntimeKind::ClaudeCode, "other-wrapper-main")
        .expect("wrapper alias should validate");
    store
        .save_discovery_alias(
            &alias,
            canonical.session.session_id(),
            clock.now().wall_time(),
        )
        .await
        .expect("wrapper alias should persist");
    let before = store
        .snapshot(canonical.session)
        .await
        .expect("canonical snapshot should load")
        .expect("canonical snapshot should exist");

    let aliased = api
        .register_session(
            &transport,
            RegisterSession {
                runtime: alias.runtime(),
                native_id: alias.native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "reject-aliased-main".to_owned(),
            },
        )
        .await;
    assert!(matches!(aliased, Err(AgentApiError::TransportAlreadyBound)));
    assert_eq!(
        store
            .snapshot(canonical.session)
            .await
            .expect("canonical snapshot should reload")
            .expect("canonical snapshot should remain"),
        before,
        "rejected aliased registration must not mutate another tree"
    );
}

#[tokio::test]
async fn mcp_main_registration_resolves_discovery_alias_before_child_retry() {
    let (api, store, clock) = api_fixture().await;
    let canonical = api
        .discover_session(DiscoveredSession {
            runtime: RuntimeKind::ClaudeCode,
            native_id: "canonical-main".to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: "discover-canonical-main".to_owned(),
            adapter_version: "test".to_owned(),
            evidence_source: "test:canonical-main".to_owned(),
            title: None,
            startup_directory: None,
        })
        .await
        .expect("canonical main should be discovered");
    let alias = NativeSessionKey::new(RuntimeKind::ClaudeCode, "wrapper-main")
        .expect("wrapper alias should validate");
    store
        .save_discovery_alias(
            &alias,
            canonical.session.session_id(),
            clock.now().wall_time(),
        )
        .await
        .expect("wrapper alias should persist");
    let discovered_alias_main = api
        .discover_session(DiscoveredSession {
            runtime: alias.runtime(),
            native_id: alias.native_id().to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: "discover-wrapper-main".to_owned(),
            adapter_version: "test".to_owned(),
            evidence_source: "test:wrapper-main".to_owned(),
            title: None,
            startup_directory: None,
        })
        .await
        .expect("known wrapper alias should enrich the canonical main");
    assert_eq!(discovered_alias_main.session, canonical.session);
    let discovered_child = api
        .discover_session(DiscoveredSession {
            runtime: RuntimeKind::CodexCompanion,
            native_id: "companion-job".to_owned(),
            kind: SessionKind::Child,
            parent: Some(canonical.session.session_id()),
            event_key: "discover-companion-job".to_owned(),
            adapter_version: "1.0.6".to_owned(),
            evidence_source: "companion:state-summary".to_owned(),
            title: None,
            startup_directory: None,
        })
        .await
        .expect("Companion child should be discovered under the canonical main");

    let transport = TransportKey::new("wrapper-transport").expect("transport should validate");
    let registered_main = api
        .register_session(
            &transport,
            RegisterSession {
                runtime: alias.runtime(),
                native_id: alias.native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "register-wrapper-main".to_owned(),
            },
        )
        .await
        .expect("known wrapper alias should bind the canonical main");
    assert_eq!(registered_main.session, canonical.session);

    let registered_child = api
        .register_session(
            &transport,
            RegisterSession {
                runtime: RuntimeKind::CodexCompanion,
                native_id: "companion-job".to_owned(),
                kind: SessionKind::Child,
                parent: Some(registered_main.session.session_id()),
                event_key: "register-companion-job".to_owned(),
            },
        )
        .await
        .expect("discovered Companion child should accept exact MCP registration");
    assert_eq!(registered_child.session, discovered_child.session);
    assert_eq!(registered_child.root, canonical.root);
}

#[tokio::test]
async fn main_registration_self_heals_when_its_alias_target_finished() {
    let (api, store, clock) = api_fixture().await;
    let canonical = api
        .discover_session(DiscoveredSession {
            runtime: RuntimeKind::ClaudeCode,
            native_id: "finished-canonical".to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: "discover-finished-canonical".to_owned(),
            adapter_version: "test".to_owned(),
            evidence_source: "test:finished-canonical".to_owned(),
            title: None,
            startup_directory: None,
        })
        .await
        .expect("canonical main should be discovered");
    let canonical_native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "finished-canonical")
        .expect("canonical identity should validate");
    api.ingest_native_observation(
        ObservationEnvelope::new(
            ObservationId::from_native(RuntimeKind::ClaudeCode, "test", "finish-canonical")
                .expect("observation identity should validate"),
            canonical_native,
            clock.now(),
            ObservationSource::new(
                AdapterIdentity::new(RuntimeKind::ClaudeCode, "2.1.212")
                    .expect("adapter should be valid"),
                "hook:stop",
                EvidenceTrust::Authoritative,
                None,
            )
            .expect("source should be valid"),
            ObservationPayload::NativeState(DetailedState::Completed),
        )
        .expect("observation should be valid"),
    )
    .await
    .expect("canonical main should finish");
    let alias = NativeSessionKey::new(RuntimeKind::ClaudeCode, "live-wrapper-main")
        .expect("wrapper alias should validate");
    store
        .save_discovery_alias(
            &alias,
            canonical.session.session_id(),
            clock.now().wall_time(),
        )
        .await
        .expect("stale alias should persist");
    let before = store
        .snapshot(canonical.session)
        .await
        .expect("canonical snapshot should load")
        .expect("canonical snapshot should exist");

    let transport = TransportKey::new("live-wrapper-transport").expect("transport should validate");
    let registered = api
        .register_session(
            &transport,
            RegisterSession {
                runtime: alias.runtime(),
                native_id: alias.native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "register-live-wrapper".to_owned(),
            },
        )
        .await
        .expect("a live caller must not be redirected onto a finished session");

    assert_eq!(
        registered.native_id,
        alias.native_id(),
        "registration keeps the caller's own identity when the alias target already finished"
    );
    assert_eq!(
        registered.root.session_id(),
        SessionId::from_native(&alias),
        "the self-healed registration owns its own tree"
    );
    assert_ne!(registered.snapshot.state(), DetailedState::Completed);
    assert_eq!(
        store
            .snapshot(canonical.session)
            .await
            .expect("canonical snapshot should reload")
            .expect("canonical snapshot should remain"),
        before,
        "self-healing must not mutate the finished tree"
    );
    assert!(
        !store
            .discovery_aliases(RuntimeKind::ClaudeCode, 10)
            .await
            .expect("aliases should load")
            .iter()
            .any(|(key, _)| *key == alias),
        "the stale alias is discarded so the next registration resolves directly"
    );
}

#[tokio::test]
async fn main_registration_self_heals_when_its_alias_target_is_not_a_main_session() {
    let (api, store, clock) = api_fixture().await;
    let transport = TransportKey::new("canonical-transport").expect("transport should validate");
    let canonical_main = register_main(&api, &transport, "child-alias-main", "register-main").await;
    let child = api
        .register_session(
            &transport,
            RegisterSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: "aliased-child-job".to_owned(),
                kind: SessionKind::Child,
                parent: Some(canonical_main),
                event_key: "register-aliased-child".to_owned(),
            },
        )
        .await
        .expect("child should register");
    let alias = NativeSessionKey::new(RuntimeKind::ClaudeCode, "child-aliased-wrapper")
        .expect("wrapper alias should validate");
    store
        .save_discovery_alias(&alias, child.session.session_id(), clock.now().wall_time())
        .await
        .expect("corrupt alias should persist");

    let wrapper_transport =
        TransportKey::new("child-aliased-transport").expect("transport should validate");
    let registered = api
        .register_session(
            &wrapper_transport,
            RegisterSession {
                runtime: alias.runtime(),
                native_id: alias.native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "register-child-aliased-wrapper".to_owned(),
            },
        )
        .await
        .expect("an alias pointing at a non-main session must not brick registration");

    assert_eq!(registered.native_id, alias.native_id());
    assert_eq!(registered.root.session_id(), SessionId::from_native(&alias));
    assert_eq!(
        store
            .session_by_id(child.session.session_id())
            .await
            .expect("child lookup should succeed")
            .expect("child should remain")
            .root,
        child.root,
        "self-healing must not re-root the mistargeted session"
    );
    assert!(
        !store
            .discovery_aliases(RuntimeKind::ClaudeCode, 10)
            .await
            .expect("aliases should load")
            .iter()
            .any(|(key, _)| *key == alias),
        "the corrupt alias is discarded"
    );
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
async fn progress_returns_a_waiting_session_to_running_and_resumes_timers() {
    let (api, _store, _clock) = api_fixture().await;
    let transport = TransportKey::new("progress-resumes").expect("transport should be valid");
    let main = register_main(&api, &transport, "progress-main", "register-main").await;

    let waiting = api
        .report_waiting(&transport, main, "wait-for-agent", WaitingKind::Intentional)
        .await
        .expect("intentional wait should commit");
    assert_eq!(waiting.snapshot.state(), DetailedState::WaitingForAgent);
    assert!(waiting.snapshot.timers_paused());

    let running = api
        .report_progress(
            &transport,
            main,
            "work-resumed",
            "processing the child result".to_owned(),
            None,
        )
        .await
        .expect("progress should commit");

    assert_eq!(running.snapshot.state(), DetailedState::Running);
    assert!(!running.snapshot.timers_paused());
}

#[tokio::test]
async fn intentional_wait_commits_state_and_timer_pause_in_one_observation() {
    let (api, _store, _clock) = api_fixture().await;
    let transport = TransportKey::new("intentional-wait").expect("transport should be valid");
    let main = register_main(&api, &transport, "waiting-main", "register-main").await;
    let before = api
        .get_session(&transport, main)
        .await
        .expect("registered session should load");

    let waiting = api
        .report_waiting(
            &transport,
            main,
            "intentional-wait-1",
            WaitingKind::Intentional,
        )
        .await
        .expect("intentional wait should commit atomically");

    assert_eq!(waiting.snapshot.state(), DetailedState::WaitingForAgent);
    assert!(waiting.snapshot.timers_paused());
    assert_eq!(waiting.snapshot.revision(), before.snapshot.revision() + 1);
}

#[tokio::test]
async fn intentional_wait_uses_a_new_idempotency_namespace_after_legacy_waiting_evidence() {
    let (api, _store, _clock) = api_fixture().await;
    let transport = TransportKey::new("intentional-wait-upgrade").expect("transport should work");
    let main = register_main(&api, &transport, "waiting-upgrade-main", "register-main").await;
    api.report_waiting(&transport, main, "shared-event", WaitingKind::Agent)
        .await
        .expect("legacy waiting evidence should persist");

    let intentional = api
        .report_waiting(&transport, main, "shared-event", WaitingKind::Intentional)
        .await
        .expect("intentional wait should not conflict with the legacy namespace");

    assert!(intentional.snapshot.timers_paused());
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
            adapter_version: "0.144.5".to_owned(),
            evidence_source: "codex:state-db".to_owned(),
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
        adapter_version: "0.144.5".to_owned(),
        evidence_source: "codex:state-db".to_owned(),
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
            adapter_version: "0.144.5".to_owned(),
            evidence_source: "codex:state-db".to_owned(),
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
            adapter_version: "0.144.5".to_owned(),
            evidence_source: "codex:state-db".to_owned(),
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
    assert_native_discovery_provenance(&store, main.root, &child).await;

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
async fn repository_enrichment_can_clear_a_stale_pull_request() {
    let (api, store, _) = api_fixture().await;
    let session = api
        .discover_session(DiscoveredSession {
            runtime: RuntimeKind::CodexCli,
            native_id: "repository-main".to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: "repository-main".to_owned(),
            adapter_version: "0.144.5".to_owned(),
            evidence_source: "codex:state-db".to_owned(),
            title: Some("Repository session".to_owned()),
            startup_directory: Some("/work/repository".to_owned()),
        })
        .await
        .expect("session should be discovered");
    api.enrich_repository_metadata(
        session.session,
        RepositoryMetadata {
            remote: Some("https://github.com/lklimek/agent-watchdog.git".to_owned()),
            branch: Some("feat/metadata".to_owned()),
            pull_request_number: Some(42),
            pull_request_url: Some("https://github.com/lklimek/agent-watchdog/pull/42".to_owned()),
            replace_pull_request: true,
        },
    )
    .await
    .expect("repository metadata should persist");
    api.enrich_repository_metadata(
        session.session,
        RepositoryMetadata {
            remote: None,
            branch: None,
            replace_pull_request: true,
            ..RepositoryMetadata::default()
        },
    )
    .await
    .expect("stale pull request should clear");

    let metadata = store
        .session_metadata(session.session)
        .await
        .expect("metadata should query")
        .expect("metadata should exist");
    assert_eq!(metadata.branch(), Some("feat/metadata"));
    assert_eq!(metadata.pull_request_number(), None);
    assert_eq!(metadata.pull_request_url(), None);
}

async fn assert_native_discovery_provenance(
    store: &WatchdogStore,
    root: watchdog_domain::MainSessionId,
    child: &watchdog_server::SessionView,
) {
    let discovery_id = ObservationId::from_native(
        RuntimeKind::CodexCli,
        "native-discovery",
        format!(
            "{}:codex-state:discovered-child",
            child.session.session_id()
        ),
    )
    .expect("discovery observation ID should validate");
    let discovery = store
        .observation(discovery_id)
        .await
        .expect("discovery observation should load")
        .expect("native discovery observation should persist");
    assert_eq!(discovery.source().adapter().version(), "0.144.5");
    assert_eq!(discovery.source().fingerprint(), "codex:state-db");
    assert!(!discovery.source().fingerprint().starts_with("mcp:"));
    let relations = store
        .relations_for_root(root, 10)
        .await
        .expect("relations should load");
    let selected = relations
        .iter()
        .find(|relation| relation.child.session_id() == child.session.session_id())
        .expect("native child relation should persist");
    assert_eq!(selected.provenance.adapter().version(), "0.144.5");
    assert_eq!(
        selected.provenance.fingerprint(),
        "codex:state-db:relation:codex-state:discovered-child"
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
        adapter_version: "1.0.6".to_owned(),
        evidence_source: "companion:state-summary".to_owned(),
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
        adapter_version: "1.0.6".to_owned(),
        evidence_source: "companion:state-summary".to_owned(),
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

#[tokio::test]
async fn scheduler_reconciles_all_sessions_without_persisting_noop_ticks() {
    let (api, store, clock) = api_fixture().await;
    let transport = TransportKey::new("timer-main").expect("transport should be valid");
    let main = register_main(&api, &transport, "timer-main", "register-main").await;
    api.register_session(
        &transport,
        RegisterSession {
            runtime: RuntimeKind::ClaudeCode,
            native_id: "timer-child".to_owned(),
            kind: SessionKind::Child,
            parent: Some(main),
            event_key: "register-child".to_owned(),
        },
    )
    .await
    .expect("child should register");
    let before = store.counts().await.expect("store counts should load");

    let no_change = api
        .reconcile_timers()
        .await
        .expect("early scheduler reconciliation should succeed");
    assert_eq!(no_change.evaluated_sessions(), 2);
    assert_eq!(no_change.changed_sessions(), 0);
    assert_eq!(
        store.counts().await.expect("store counts should load"),
        before,
        "pre-threshold ticks must not grow the durable ledger"
    );

    clock.advance(DurationMs::new(5 * 60_000));
    let suspect = api
        .reconcile_timers()
        .await
        .expect("suspect threshold should reconcile");
    assert_eq!(suspect.changed_sessions(), 2);
    let page = api
        .list_events(&transport, Some(0), 100)
        .await
        .expect("suspect events should list");
    assert_eq!(
        page.events
            .iter()
            .filter(|event| matches!(event.event.kind(), DomainEventKind::Suspect))
            .count(),
        2
    );

    let after_suspect = store.counts().await.expect("store counts should load");
    let repeated = api
        .reconcile_timers()
        .await
        .expect("same clock tick should be harmless");
    assert_eq!(repeated.changed_sessions(), 0);
    assert_eq!(
        store.counts().await.expect("store counts should load"),
        after_suspect
    );

    clock.advance(DurationMs::new(10 * 60_000));
    let stalled = api
        .reconcile_timers()
        .await
        .expect("stall threshold should reconcile");
    assert_eq!(stalled.changed_sessions(), 2);
    let sessions = api
        .list_sessions(&transport)
        .await
        .expect("sessions should list");
    assert!(
        sessions
            .iter()
            .all(|session| session.snapshot.state() == DetailedState::Stalled)
    );
    for destination in [
        OutboxDestination::Browser,
        OutboxDestination::HomeAssistant,
        OutboxDestination::Webhook,
    ] {
        let pending = store
            .pending_outbox_for(destination, 10)
            .await
            .expect("human outbox should load");
        assert_eq!(pending.len(), 1, "only the main alert should route");
        let event: watchdog_domain::DomainEvent = serde_json::from_slice(pending[0].payload())
            .expect("outbox payload should be a domain event");
        assert_eq!(event.subject().session_id(), main);
        assert!(matches!(event.kind(), DomainEventKind::AlertDue));
    }
}

#[tokio::test]
async fn parent_alert_event_contains_complete_bounded_diagnostics() {
    let (api, store, clock) = api_fixture().await;
    let transport = TransportKey::new("diagnostic-main").expect("transport should be valid");
    let main = register_main(&api, &transport, "diagnostic-main", "register-main").await;
    let child_id = prepare_diagnostic_child(&api, &store, &clock, &transport, main).await;

    clock.advance(DurationMs::new(15 * 60_000));
    api.reconcile_timers()
        .await
        .expect("alert threshold should reconcile");
    let page = api
        .list_events(&transport, Some(0), 100)
        .await
        .expect("parent events should list");
    let alert = page
        .events
        .iter()
        .find(|view| {
            view.event.subject().session_id() == child_id
                && matches!(view.event.kind(), DomainEventKind::AlertDue)
        })
        .expect("child alert should exist");
    assert_complete_diagnostics(alert);
}

#[tokio::test]
async fn runtime_absence_with_a_later_commit_reports_an_uncertain_outcome() {
    const STALE_STATUS_EPOCH: u64 = 1_784_715_604;
    const COMMIT_EPOCH: u64 = 1_784_718_759;

    let repository = tempfile::tempdir().expect("repository fixture should exist");
    initialize_repository(repository.path());
    let (api, _store, clock) = api_fixture().await;
    let transport =
        TransportKey::new("companion-runtime-absence").expect("transport should be valid");
    let main = register_main(
        &api,
        &transport,
        "companion-runtime-absence-main",
        "register-main",
    )
    .await;
    let child = register_child(
        &api,
        &transport,
        main,
        "task-mrvxk4m5-heu75i",
        "register-child",
    )
    .await;
    let parser = CompanionParser::new("1.0.6").expect("version should be valid");
    let detail = parser
        .parse_detail(
            &serde_json::to_vec(&serde_json::json!({
                "id": "task-mrvxk4m5-heu75i",
                "sessionId": "companion-runtime-absence-main",
                "workspaceRoot": repository.path(),
                "status": "running",
                "phase": "investigating",
                "pid": 871_478,
                "updatedAt": "2026-07-22T10:20:04Z",
                "completedAt": null
            }))
            .expect("job fixture should serialize"),
        )
        .expect("stale running detail should parse");
    let reconciled = parser
        .reconcile(None, Some(&detail))
        .expect("detail-only job should reconcile");
    api.ingest_native_observation(
        parser
            .observation(&reconciled, "stale-running-status", clock.now())
            .expect("stale status observation should be valid"),
    )
    .await
    .expect("stale running status should apply");

    create_deliverable_commit(repository.path());
    let commit_epoch = latest_commit_epoch(repository.path());
    assert_eq!(commit_epoch, COMMIT_EPOCH);
    assert!(commit_epoch > STALE_STATUS_EPOCH);
    clock.advance(DurationMs::new((COMMIT_EPOCH - STALE_STATUS_EPOCH) * 1_000));

    let native = NativeSessionKey::new(RuntimeKind::CodexCompanion, "task-mrvxk4m5-heu75i")
        .expect("native identity should be valid");
    api.ingest_native_observation(native_state_observation(
        &native,
        ObservationId::from_native(RuntimeKind::CodexCompanion, "runtime", "runtime-absent")
            .expect("observation ID should be valid"),
        clock.now(),
        DetailedState::Disappeared,
    ))
    .await
    .expect("verified runtime absence should apply");

    let page = api
        .list_events(&transport, Some(0), 100)
        .await
        .expect("parent events should list");
    let alert = page
        .events
        .iter()
        .find(|view| {
            view.event.subject().session_id() == child
                && matches!(view.event.kind(), DomainEventKind::AlertDue)
        })
        .expect("runtime absence must still alert");
    assert_eq!(alert.session.snapshot.state(), DetailedState::Disappeared);
    assert!(alert.diagnostics.outcome_uncertain);
    assert!(alert.diagnostics.suggested_checks.iter().any(|check| {
        check
            == "Inspect the exact target branch and worktree for commits or changes newer than \
                the last trusted activity before treating runtime absence as failure"
    }));
}

/// Latest child diagnostics assembled from the durable parent inbox.
async fn child_diagnostics(
    api: &AgentApi,
    transport: &TransportKey,
    child: SessionId,
) -> watchdog_server::AgentDiagnosticView {
    api.list_events(transport, Some(0), 100)
        .await
        .expect("parent events should list")
        .events
        .iter()
        .rev()
        .find(|view| view.event.subject().session_id() == child)
        .expect("the child should have durable events")
        .diagnostics
        .clone()
}

#[tokio::test]
async fn established_child_outcomes_are_not_reported_uncertain() {
    let (api, _store, clock) = api_fixture().await;
    let transport = TransportKey::new("established-outcome").expect("transport should be valid");
    let main = register_main(&api, &transport, "established-main", "register-main").await;
    let child = register_child(
        &api,
        &transport,
        main,
        "established-child",
        "register-child",
    )
    .await;
    let native = NativeSessionKey::new(RuntimeKind::CodexCompanion, "established-child")
        .expect("native identity should be valid");

    api.ingest_native_observation(native_state_observation(
        &native,
        ObservationId::from_native(RuntimeKind::CodexCompanion, "runtime", "running")
            .expect("observation ID should be valid"),
        clock.now(),
        DetailedState::Running,
    ))
    .await
    .expect("authoritative running state should apply");
    let running = child_diagnostics(&api, &transport, child).await;
    assert!(
        !running.outcome_uncertain,
        "a plainly running child has no unresolved outcome to flag"
    );

    api.complete_session(
        &transport,
        child,
        "complete-established-child",
        CompletionOutcome::Completed,
    )
    .await
    .expect("child should complete");
    let completed = child_diagnostics(&api, &transport, child).await;
    assert!(
        !completed.outcome_uncertain,
        "a normally completed child has an established outcome"
    );
}

#[tokio::test]
async fn terminal_state_after_an_unresolved_source_conflict_stays_uncertain() {
    let (api, _store, clock) = api_fixture().await;
    let transport = TransportKey::new("conflicted-outcome").expect("transport should be valid");
    let main = register_main(&api, &transport, "conflicted-main", "register-main").await;
    let child = register_child(&api, &transport, main, "conflicted-child", "register-child").await;
    let native = NativeSessionKey::new(RuntimeKind::CodexCompanion, "conflicted-child")
        .expect("native identity should be valid");

    api.ingest_native_observation(
        ObservationEnvelope::new(
            ObservationId::from_native(RuntimeKind::CodexCompanion, "runtime", "conflict")
                .expect("observation ID should be valid"),
            native.clone(),
            clock.now(),
            ObservationSource::new(
                AdapterIdentity::new(RuntimeKind::CodexCompanion, "1.0.6")
                    .expect("adapter should be valid"),
                "state:identity-conflict",
                EvidenceTrust::Authoritative,
                None,
            )
            .expect("source should be valid"),
            ObservationPayload::SourceConflict(
                BoundedText::new("source_conflict", "runtime and agent sources disagree")
                    .expect("conflict text should be valid"),
            ),
        )
        .expect("conflict observation should be valid"),
    )
    .await
    .expect("source conflict should apply");

    api.ingest_native_observation(native_state_observation(
        &native,
        ObservationId::from_native(RuntimeKind::CodexCompanion, "runtime", "failed")
            .expect("observation ID should be valid"),
        clock.now(),
        DetailedState::Failed,
    ))
    .await
    .expect("terminal state should apply over an unresolved conflict");

    let diagnostics = child_diagnostics(&api, &transport, child).await;
    assert!(
        !diagnostics.source_conflicts.is_empty(),
        "the unresolved conflict must remain visible"
    );
    assert!(
        diagnostics.outcome_uncertain,
        "a terminal state reached while sources still disagree is not an established outcome"
    );
}

async fn prepare_diagnostic_child(
    api: &AgentApi,
    store: &WatchdogStore,
    clock: &Arc<FakeClock>,
    transport: &TransportKey,
    main: SessionId,
) -> SessionId {
    let child = api
        .register_session(
            transport,
            RegisterSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: "diagnostic-child".to_owned(),
                kind: SessionKind::Child,
                parent: Some(main),
                event_key: "register-child".to_owned(),
            },
        )
        .await
        .expect("child should register");
    let child_id = child.session.session_id();
    api.report_progress(
        transport,
        child_id,
        "long-operation",
        "running the slow suite".to_owned(),
        Some("cargo test".to_owned()),
    )
    .await
    .expect("operation should report");
    let native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "diagnostic-child")
        .expect("native identity should be valid");
    let process = ProcessIdentity::new(
        ProcessId::new(4_242).expect("PID should be valid"),
        99,
        BoundedText::new("executable", "/usr/bin/claude").expect("executable should be valid"),
    );
    api.ingest_native_observation(
        ObservationEnvelope::new(
            ObservationId::from_native(RuntimeKind::ClaudeCode, "process", "diagnostic-child")
                .expect("observation ID should be valid"),
            native,
            clock.now(),
            ObservationSource::new(
                AdapterIdentity::new(RuntimeKind::ClaudeCode, "linux-procfs-v1")
                    .expect("adapter should be valid"),
                "process:identity",
                EvidenceTrust::Corroborating,
                None,
            )
            .expect("source should be valid"),
            ObservationPayload::ProcessIdentity(process),
        )
        .expect("observation should be valid"),
    )
    .await
    .expect("process identity should apply");
    store
        .save_latest_activity(&ActivitySampleRecord {
            session: child.session,
            observed_at: clock.now().wall_time(),
            evidence: ActivityEvidence::ProcessCpu {
                user_ticks: 0,
                system_ticks: 0,
                child_user_ticks: 0,
                child_system_ticks: 0,
            },
        })
        .await
        .expect("fresh process check should persist");
    child_id
}

fn assert_complete_diagnostics(alert: &AgentEventView) {
    assert_eq!(
        alert
            .diagnostics
            .process_identity
            .as_ref()
            .expect("PID diagnostics should exist")
            .pid()
            .value(),
        4_242
    );
    assert_eq!(alert.diagnostics.process_activity.len(), 1);
    let activity_source = alert
        .diagnostics
        .process_activity_provenance
        .as_ref()
        .expect("process provenance should exist");
    assert_eq!(activity_source.adapter().version(), "linux-procfs-v1");
    assert_eq!(activity_source.fingerprint(), "process:tree-delta");
    assert_eq!(
        alert.diagnostics.active_operation.as_deref(),
        Some("cargo test: running the slow suite")
    );
    assert!(
        alert
            .diagnostics
            .signal_times
            .last_trusted_transition
            .is_some()
    );
    assert!(
        alert
            .diagnostics
            .signal_times
            .latest_process_sample
            .is_some()
    );
    assert!(alert.diagnostics.source_conflicts.is_empty());
    let correlation = alert
        .diagnostics
        .correlation
        .as_ref()
        .expect("selected parent correlation should exist");
    assert_eq!(correlation.basis, CorrelationBasis::McpRegistration);
    assert!(correlation.evidence.starts_with("mcp:register_delegation:"));
    assert!(!alert.diagnostics.suggested_checks.is_empty());
}

fn initialize_repository(repository: &Path) {
    run_git(
        repository,
        &["init", "-b", "chore/bump-platform-pin-pr3968"],
    );
    run_git(repository, &["config", "user.name", "Codex"]);
    run_git(
        repository,
        &["config", "user.email", "codex@example.invalid"],
    );
}

fn create_deliverable_commit(repository: &Path) {
    fs::write(repository.join("deliverable.txt"), "completed work\n")
        .expect("deliverable fixture should write");
    run_git(repository, &["add", "deliverable.txt"]);
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args([
            "commit",
            "-m",
            "fix(wallet-backend): reap persisted FVK row on wallet removal",
        ])
        .env("GIT_AUTHOR_DATE", "2026-07-22T11:12:39Z")
        .env("GIT_COMMITTER_DATE", "2026-07-22T11:12:39Z")
        .output()
        .expect("git commit should run");
    assert!(
        output.status.success(),
        "git commit failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn latest_commit_epoch(repository: &Path) -> u64 {
    String::from_utf8(run_git(repository, &["log", "-1", "--format=%ct"]).stdout)
        .expect("commit timestamp should be UTF-8")
        .trim()
        .parse()
        .expect("commit timestamp should be numeric")
}

fn run_git(repository: &Path, arguments: &[&str]) -> std::process::Output {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .expect("git command should run");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
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

#[tokio::test]
async fn durable_event_cursor_never_advances_beyond_the_latest_committed_event() {
    let (api, store, clock) = api_fixture().await;
    let first_transport =
        TransportKey::new("cursor-clamp-first").expect("transport should be valid");
    let main = register_main(&api, &first_transport, "cursor-clamp-main", "register-main").await;
    let latest_before = store
        .latest_event_id()
        .await
        .expect("latest event ID should load")
        .value();

    api.list_events(&first_transport, Some(latest_before + 1_000), 10)
        .await
        .expect("oversized acknowledgement should be safely clamped");
    let root = store
        .session_by_id(main)
        .await
        .expect("session lookup should succeed")
        .expect("main should exist")
        .root;
    assert_eq!(
        store
            .inbox_offset(root)
            .await
            .expect("inbox offset should load")
            .expect("acknowledgement should persist")
            .last_event_id
            .value(),
        latest_before
    );

    api.complete_session(
        &first_transport,
        main,
        "completion-after-clamp",
        CompletionOutcome::Completed,
    )
    .await
    .expect("new event should commit");
    let restarted = AgentApi::new(store, clock)
        .await
        .expect("API should restart from store");
    let second_transport =
        TransportKey::new("cursor-clamp-second").expect("transport should be valid");
    restarted
        .bind_discovered_main(&second_transport, main)
        .await
        .expect("main should bind after restart");

    let page = restarted
        .list_events(&second_transport, None, 10)
        .await
        .expect("events after the clamped acknowledgement should remain deliverable");
    assert!(!page.events.is_empty());
    assert!(page.next_cursor > latest_before);
}

#[tokio::test]
async fn oversized_acknowledgement_cannot_skip_another_roots_undelivered_events() {
    let (api, _store, _clock) = api_fixture().await;
    let first_transport =
        TransportKey::new("cursor-root-first").expect("transport should be valid");
    let second_transport =
        TransportKey::new("cursor-root-second").expect("transport should be valid");
    let first = register_main(
        &api,
        &first_transport,
        "cursor-first-main",
        "register-first-main",
    )
    .await;
    api.complete_session(
        &first_transport,
        first,
        "complete-first-main",
        CompletionOutcome::Completed,
    )
    .await
    .expect("the first root should have one undelivered event");
    register_main(
        &api,
        &second_transport,
        "cursor-second-main",
        "register-second-main",
    )
    .await;

    let page = api
        .list_events(&first_transport, Some(u64::MAX), 10)
        .await
        .expect("oversized acknowledgement should be clamped to delivered events");

    assert_eq!(page.after, 0);
    assert!(
        page.events
            .iter()
            .any(|event| event.session.session.session_id() == first),
        "the first root's undelivered event must remain visible"
    );
}

#[tokio::test]
async fn failed_event_page_assembly_does_not_advance_the_delivered_ceiling() {
    let directory = tempfile::tempdir().expect("temporary directory should exist");
    let path = directory.path().join("watchdog.db");
    let store = WatchdogStore::open(&path)
        .await
        .expect("database should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(10_000),
        5_000,
    )));
    let api = AgentApi::new(store.clone(), clock)
        .await
        .expect("agent API should initialize");
    let transport = TransportKey::new("cursor-failed-page").expect("transport should be valid");
    let main = register_main(&api, &transport, "cursor-failed-main", "register-main").await;
    api.complete_session(
        &transport,
        main,
        "complete-main",
        CompletionOutcome::Completed,
    )
    .await
    .expect("the root should have an event");
    let pool = sqlx::SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(false),
    )
    .await
    .expect("a corruption fixture connection should open");
    sqlx::query("UPDATE session_snapshots SET snapshot_json = X'7B' WHERE session_id = ?")
        .bind(main.to_string())
        .execute(&pool)
        .await
        .expect("the snapshot fixture should become unreadable");

    assert!(api.list_events(&transport, None, 10).await.is_err());
    assert_eq!(
        store
            .inbox_offset(MainSessionId::from(main))
            .await
            .expect("inbox lookup should work"),
        None,
        "a page the caller never received must not become acknowledgeable"
    );
}
