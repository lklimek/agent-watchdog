//! Read-only dashboard projection and HTTP acceptance tests.

use std::sync::Arc;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::StreamExt as _;
use tokio::time::{Duration, timeout};
use tower::ServiceExt as _;
use watchdog_domain::{
    AdapterIdentity, CompactState, DetailedState, DomainEvent, DomainEventKind, EventId,
    EvidenceTrust, MainSessionId, NativeSessionKey, ObservationEnvelope, ObservationId,
    ObservationPayload, ObservationSource, RuntimeKind, SessionId, SessionIdentity, SessionKind,
    TimePoint, WallTimeMs,
};
use watchdog_server::{
    BasicAuthenticator, DashboardOutboxDispatcher, DashboardQuery, DashboardScope,
    DashboardService, DashboardSort, dashboard_router,
};
use watchdog_store::{
    AdapterHealthRecord, AdapterHealthStatus, ApplyObservation, OutboxDestination,
    SessionMetadataRecord, SnapshotUpdate, WatchdogStore,
};
use watchdog_testkit::FakeClock;

struct DashboardFixture {
    store: WatchdogStore,
    service: DashboardService,
}

struct SessionSeed<'a> {
    native_id: &'a str,
    kind: SessionKind,
    root: Option<MainSessionId>,
    state: DetailedState,
    event_id: u64,
    directory: Option<&'a str>,
    title: Option<&'a str>,
}

impl DashboardFixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory should exist");
        let path = directory.path().join("watchdog.db");
        let _retained = directory.keep();
        let store = WatchdogStore::open(&path)
            .await
            .expect("database should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(
            WallTimeMs::new(60_000),
            60_000,
        )));
        let service = DashboardService::new(store.clone(), clock);
        Self { store, service }
    }

    async fn seed(&self, seed: SessionSeed<'_>) -> SessionIdentity {
        let SessionSeed {
            native_id,
            kind,
            root,
            state,
            event_id,
            directory,
            title,
        } = seed;
        let native = NativeSessionKey::new(RuntimeKind::ClaudeCode, native_id)
            .expect("native identity should be valid");
        let session_id = SessionId::from_native(&native);
        let session = match kind {
            SessionKind::Main => SessionIdentity::Main(MainSessionId::from(session_id)),
            SessionKind::Child => {
                SessionIdentity::Child(watchdog_domain::ChildSessionId::from(session_id))
            }
        };
        let root = root.unwrap_or_else(|| MainSessionId::from(session_id));
        let observed_at = WallTimeMs::new(10_000 + i64::try_from(event_id).expect("small ID"));
        let observation = ObservationEnvelope::new(
            ObservationId::from_native(RuntimeKind::ClaudeCode, "dashboard-test", native_id)
                .expect("observation identity should be valid"),
            native,
            TimePoint::new(observed_at, 10_000 + event_id),
            ObservationSource::new(
                AdapterIdentity::new(RuntimeKind::ClaudeCode, "test")
                    .expect("adapter identity should be valid"),
                "dashboard-fixture",
                EvidenceTrust::Authoritative,
                None,
            )
            .expect("source should be valid"),
            ObservationPayload::NativeState(state),
        )
        .expect("observation should be valid");
        let snapshot = SnapshotUpdate::new(session, root, event_id, state, observed_at)
            .expect("snapshot should be valid");
        let event = DomainEvent::new(
            EventId::new(event_id),
            root,
            session,
            observed_at,
            DomainEventKind::StateChanged {
                from: DetailedState::Starting,
                to: state,
            },
        );
        self.store
            .apply_observation(&ApplyObservation::new(
                observation,
                snapshot,
                vec![event],
                [OutboxDestination::Sse],
            ))
            .await
            .expect("fixture should persist");
        self.store
            .save_session_metadata(
                &SessionMetadataRecord::new(
                    session,
                    title.map(ToOwned::to_owned),
                    directory.map(ToOwned::to_owned),
                    None,
                    Some(format!("branch/{native_id}")),
                    None,
                    None,
                    observed_at,
                )
                .expect("metadata should be valid"),
            )
            .await
            .expect("metadata should persist");
        session
    }

    async fn seed_projection_scenario(&self) {
        let waiting = self
            .seed(SessionSeed {
                native_id: "waiting-main",
                kind: SessionKind::Main,
                root: None,
                state: DetailedState::WaitingForUser,
                event_id: 1,
                directory: Some("/z/waiting"),
                title: Some("Waiting session"),
            })
            .await;
        let waiting_root = MainSessionId::from(waiting.session_id());
        for seed in [
            SessionSeed {
                native_id: "active-child",
                kind: SessionKind::Child,
                root: Some(waiting_root),
                state: DetailedState::Running,
                event_id: 2,
                directory: None,
                title: None,
            },
            SessionSeed {
                native_id: "stalled-child",
                kind: SessionKind::Child,
                root: Some(waiting_root),
                state: DetailedState::Stalled,
                event_id: 3,
                directory: None,
                title: None,
            },
            SessionSeed {
                native_id: "finished-child",
                kind: SessionKind::Child,
                root: Some(waiting_root),
                state: DetailedState::Completed,
                event_id: 4,
                directory: None,
                title: None,
            },
            SessionSeed {
                native_id: "active-main",
                kind: SessionKind::Main,
                root: None,
                state: DetailedState::Running,
                event_id: 5,
                directory: Some("/m/active"),
                title: Some("Active session"),
            },
            SessionSeed {
                native_id: "idle-main",
                kind: SessionKind::Main,
                root: None,
                state: DetailedState::Idle,
                event_id: 6,
                directory: Some("/a/idle"),
                title: None,
            },
            SessionSeed {
                native_id: "completed-main",
                kind: SessionKind::Main,
                root: None,
                state: DetailedState::Completed,
                event_id: 7,
                directory: Some("/c/completed"),
                title: Some("Completed session"),
            },
        ] {
            self.seed(seed).await;
        }
    }

    fn router(&self) -> Router {
        dashboard_router(
            self.service.clone(),
            BasicAuthenticator::new("watchdog", "secret").expect("auth should be valid"),
        )
    }
}

#[tokio::test]
async fn authenticated_root_redirects_to_dashboard() {
    let fixture = DashboardFixture::new().await;
    let router = fixture.router();

    let unauthorized = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let authorization = format!("Basic {}", STANDARD.encode("watchdog:secret"));
    let authorized = router
        .oneshot(
            Request::builder()
                .uri("/")
                .header(header::AUTHORIZATION, authorization)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    assert_eq!(authorized.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(authorized.headers()[header::LOCATION], "/ui");
    assert_eq!(authorized.headers()[header::CACHE_CONTROL], "no-store");
}

#[tokio::test]
async fn projection_filters_sorts_and_aggregates_main_session_cards() {
    let fixture = DashboardFixture::new().await;
    fixture.seed_projection_scenario().await;

    let active = fixture
        .service
        .snapshot(DashboardQuery::default())
        .await
        .expect("snapshot should render");
    assert_eq!(
        active
            .sessions
            .iter()
            .map(|card| card.title.as_str())
            .collect::<Vec<_>>(),
        ["Waiting session", "idle", "Active session"]
    );
    assert_eq!(active.sessions[0].child_counts[&CompactState::Active], 1);
    assert_eq!(active.sessions[0].child_counts[&CompactState::Stalled], 1);
    assert_eq!(active.sessions[0].child_counts[&CompactState::Finished], 1);

    let directory = fixture
        .service
        .snapshot(DashboardQuery {
            scope: DashboardScope::Active,
            sort: DashboardSort::Directory,
        })
        .await
        .expect("directory snapshot should render");
    assert_eq!(
        directory
            .sessions
            .iter()
            .map(|card| card.startup_directory.as_str())
            .collect::<Vec<_>>(),
        ["/a/idle", "/m/active", "/z/waiting"]
    );

    let all = fixture
        .service
        .snapshot(DashboardQuery {
            scope: DashboardScope::All,
            sort: DashboardSort::Attention,
        })
        .await
        .expect("all-session snapshot should render");
    assert_eq!(all.sessions.len(), 4);
    assert_eq!(all.revision, 7);
}

#[tokio::test]
async fn dashboard_authentication_escaping_headers_and_read_only_routes_fail_closed() {
    let fixture = DashboardFixture::new().await;
    fixture
        .seed(SessionSeed {
            native_id: "hostile-main",
            kind: SessionKind::Main,
            root: None,
            state: DetailedState::Running,
            event_id: 1,
            directory: Some("/safe/worktree"),
            title: Some("<script>alert('watchdog')</script>"),
        })
        .await;
    let router = fixture.router();

    let unauthorized = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ui")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        unauthorized.headers()[header::WWW_AUTHENTICATE],
        "Basic realm=\"Agent Watchdog\", charset=\"UTF-8\""
    );
    assert_eq!(unauthorized.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        unauthorized.headers()[header::X_CONTENT_TYPE_OPTIONS],
        "nosniff"
    );
    let unauthorized_body = to_bytes(unauthorized.into_body(), 16_384)
        .await
        .expect("body should be bounded");
    assert!(!String::from_utf8_lossy(&unauthorized_body).contains("hostile-main"));

    let authorization = format!("Basic {}", STANDARD.encode("watchdog:secret"));
    let authorized = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ui")
                .header(header::AUTHORIZATION, &authorization)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    assert_eq!(authorized.status(), StatusCode::OK);
    assert!(
        authorized
            .headers()
            .contains_key(header::CONTENT_SECURITY_POLICY)
    );
    let body = String::from_utf8(
        to_bytes(authorized.into_body(), 1_048_576)
            .await
            .expect("body should be bounded")
            .to_vec(),
    )
    .expect("dashboard should be UTF-8");
    assert!(body.contains("&lt;script&gt;alert('watchdog')&lt;/script&gt;"));
    assert!(!body.contains("<script>alert('watchdog')</script>"));

    let mutation = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/sessions")
                .header(header::AUTHORIZATION, authorization)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    assert_eq!(mutation.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn page_warnings_name_each_degraded_runtime() {
    let fixture = DashboardFixture::new().await;
    for runtime in [RuntimeKind::ClaudeCode, RuntimeKind::CodexCli] {
        fixture
            .store
            .save_adapter_health(&AdapterHealthRecord {
                adapter: AdapterIdentity::new(runtime, "test").expect("adapter should be valid"),
                status: AdapterHealthStatus::Degraded,
                last_success: None,
                last_error: Some(WallTimeMs::new(10_000)),
                affected_scope: None,
                message: Some(
                    watchdog_domain::BoundedText::new(
                        "message",
                        "Some runtime records could not be reconciled safely",
                    )
                    .expect("message should be valid"),
                ),
            })
            .await
            .expect("health should persist");
    }
    let authorization = format!("Basic {}", STANDARD.encode("watchdog:secret"));
    let response = fixture
        .router()
        .oneshot(
            Request::builder()
                .uri("/ui")
                .header(header::AUTHORIZATION, authorization)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    let body = String::from_utf8(
        to_bytes(response.into_body(), 1_048_576)
            .await
            .expect("body should be bounded")
            .to_vec(),
    )
    .expect("dashboard should be UTF-8");

    assert!(body.contains("Claude Code — <strong>DEGRADED</strong>"));
    assert!(body.contains("Codex CLI — <strong>DEGRADED</strong>"));
}

#[tokio::test]
async fn lagging_sse_client_is_told_to_resynchronize() {
    let fixture = DashboardFixture::new().await;
    let authorization = format!("Basic {}", STANDARD.encode("watchdog:secret"));
    let response = fixture
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/v1/events")
                .header(header::AUTHORIZATION, authorization)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();

    for _ in 0..129 {
        fixture
            .service
            .publish()
            .await
            .expect("snapshot publication should succeed");
    }

    let initial = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("initial SSE frame should arrive")
        .expect("stream should remain open")
        .expect("initial frame should be readable");
    assert!(String::from_utf8_lossy(&initial).contains("event: snapshot"));
    let lagged = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("lag response should arrive")
        .expect("stream should remain open")
        .expect("lag frame should be readable");
    assert!(String::from_utf8_lossy(&lagged).contains("event: resync_required"));
}

#[tokio::test]
async fn durable_sse_outbox_publishes_current_snapshot_and_acknowledges_delivery() {
    let fixture = DashboardFixture::new().await;
    let authorization = format!("Basic {}", STANDARD.encode("watchdog:secret"));
    let response = fixture
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/v1/events")
                .header(header::AUTHORIZATION, authorization)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    let mut stream = response.into_body().into_data_stream();
    let _initial = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("initial SSE frame should arrive")
        .expect("stream should remain open")
        .expect("initial frame should be readable");

    fixture
        .seed(SessionSeed {
            native_id: "new-main",
            kind: SessionKind::Main,
            root: None,
            state: DetailedState::WaitingForUser,
            event_id: 1,
            directory: Some("/work/new-main"),
            title: Some("New main"),
        })
        .await;
    let dispatcher = DashboardOutboxDispatcher::new(
        fixture.store.clone(),
        fixture.service.clone(),
        Arc::new(FakeClock::new(TimePoint::new(
            WallTimeMs::new(61_000),
            61_000,
        ))),
    );
    assert_eq!(
        dispatcher
            .deliver_pending(10)
            .await
            .expect("delivery should succeed"),
        1
    );

    let update = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("published snapshot should arrive")
        .expect("stream should remain open")
        .expect("snapshot frame should be readable");
    let update = String::from_utf8_lossy(&update);
    assert!(update.contains("event: snapshot"));
    assert!(update.contains("New main"));
    assert!(
        fixture
            .store
            .pending_outbox_for(OutboxDestination::Sse, 10)
            .await
            .expect("outbox should load")
            .is_empty()
    );
}
