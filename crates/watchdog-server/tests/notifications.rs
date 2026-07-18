//! Concise one-attempt human notification acceptance tests.

use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::IntoResponse,
    routing::post,
};
use serde_json::{Value, json};
use watchdog_domain::{RuntimeKind, SessionKind, TimePoint, WallTimeMs};
use watchdog_server::{
    AgentApi, HumanNotification, HumanNotifier, HumanOutboxDispatcher, NotificationConfigError,
    NotificationEndpoints, RegisterSession, TransportKey, WaitingKind, WebhookEndpoint,
};
use watchdog_store::{NotificationChannel, NotificationOutcome, WatchdogStore};
use watchdog_testkit::FakeClock;

async fn notifier_fixture() -> (WatchdogStore, Arc<FakeClock>, watchdog_domain::EventId) {
    let directory = tempfile::tempdir().expect("temporary directory should exist");
    let path = directory.path().join("watchdog.db");
    let _retained = directory.keep();
    let store = WatchdogStore::open(&path)
        .await
        .expect("database should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(10_000),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("agent API should initialize");
    let transport = TransportKey::new("notification-main").expect("transport should be valid");
    let main = api
        .register_session(
            &transport,
            RegisterSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: "notification-main".to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "register-main".to_owned(),
            },
        )
        .await
        .expect("main should register");
    api.report_waiting(
        &transport,
        main.session.session_id(),
        "waiting-user",
        WaitingKind::User,
    )
    .await
    .expect("waiting event should persist");
    let event_id = store.latest_event_id().await.expect("event ID should load");
    (store, clock, event_id)
}

#[tokio::test]
async fn generic_webhook_receives_only_concise_human_fields_and_records_attempt() {
    let received = Arc::new(Mutex::new(Vec::<Value>::new()));
    let app = Router::new()
        .route(
            "/notify",
            post(
                |State(received): State<Arc<Mutex<Vec<Value>>>>, Json(body): Json<Value>| async move {
                    received
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(body);
                    StatusCode::NO_CONTENT
                },
            ),
        )
        .with_state(received.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("address should be available");
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let (store, clock, event_id) = notifier_fixture().await;
    let notifier = HumanNotifier::new(
        store.clone(),
        clock,
        NotificationEndpoints::new(
            None,
            Some(
                WebhookEndpoint::new(format!("http://{address}/notify"))
                    .expect("endpoint should be valid"),
            ),
        ),
    )
    .expect("notifier should initialize");
    let event = HumanNotification::new(
        "waiting for user",
        "Agent Watchdog architecture",
        "/home/ubuntu/git/agent-watchdog",
    )
    .expect("notification should be valid");

    let deliveries = notifier
        .deliver(event_id, &event)
        .await
        .expect("delivery should be recorded");

    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].channel, NotificationChannel::Webhook);
    assert_eq!(deliveries[0].outcome, NotificationOutcome::Delivered);
    assert_eq!(
        *received
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        [json!({
            "issue": "waiting for user",
            "title": "Agent Watchdog architecture",
            "startup_directory": "/home/ubuntu/git/agent-watchdog"
        })]
    );
    let attempts = store
        .notification_attempts(event_id, 10)
        .await
        .expect("attempts should load");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].channel, NotificationChannel::Webhook);
    assert_eq!(attempts[0].outcome, NotificationOutcome::Delivered);
    server.abort();
}

#[tokio::test]
async fn durable_dispatcher_delivers_each_human_channel_once_and_acknowledges_it() {
    let received = Arc::new(Mutex::new(Vec::<Value>::new()));
    let app = Router::new()
        .route(
            "/notify",
            post(
                |State(received): State<Arc<Mutex<Vec<Value>>>>, Json(body): Json<Value>| async move {
                    received
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(body);
                    StatusCode::NO_CONTENT
                },
            ),
        )
        .with_state(received.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("address should be available");
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let (store, clock, _) = notifier_fixture().await;
    let notifier = HumanNotifier::new(
        store.clone(),
        clock.clone(),
        NotificationEndpoints::new(
            None,
            Some(
                WebhookEndpoint::new(format!("http://{address}/notify"))
                    .expect("endpoint should be valid"),
            ),
        ),
    )
    .expect("notifier should initialize");
    let dispatcher = HumanOutboxDispatcher::new(store.clone(), clock, notifier);

    assert_eq!(
        dispatcher
            .deliver_pending(10)
            .await
            .expect("pending notification should deliver"),
        2,
        "configured webhook and disabled Home Assistant rows are both terminal"
    );
    assert_eq!(
        dispatcher
            .deliver_pending(10)
            .await
            .expect("second dispatch should be empty"),
        0
    );
    assert_eq!(
        received
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        1
    );
    server.abort();
}

#[derive(Clone, Default)]
struct RedirectCounts {
    start: Arc<Mutex<u32>>,
    target: Arc<Mutex<u32>>,
}

#[tokio::test]
async fn webhook_redirect_is_not_followed_or_retried() {
    let counts = RedirectCounts::default();
    let app = Router::new()
        .route(
            "/start",
            post(|State(counts): State<RedirectCounts>| async move {
                *counts
                    .start
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    [(header::LOCATION, HeaderValue::from_static("/target"))],
                )
                    .into_response()
            }),
        )
        .route(
            "/target",
            post(|State(counts): State<RedirectCounts>| async move {
                *counts
                    .target
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
                StatusCode::NO_CONTENT.into_response()
            }),
        )
        .with_state(counts.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("address should be available");
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let (store, clock, event_id) = notifier_fixture().await;
    let notifier = HumanNotifier::new(
        store,
        clock,
        NotificationEndpoints::new(
            None,
            Some(
                WebhookEndpoint::new(format!("http://{address}/start"))
                    .expect("endpoint should be valid"),
            ),
        ),
    )
    .expect("notifier should initialize");
    let event = HumanNotification::new("stalled", "Session", "/work/session")
        .expect("notification should be valid");

    let deliveries = notifier
        .deliver(event_id, &event)
        .await
        .expect("delivery should be recorded");

    assert_eq!(deliveries[0].outcome, NotificationOutcome::Failed);
    assert_eq!(
        *counts
            .start
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        1
    );
    assert_eq!(
        *counts
            .target
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        0
    );
    server.abort();
}

#[test]
fn webhook_endpoint_is_bounded_scheme_restricted_and_redacted() {
    assert_eq!(
        WebhookEndpoint::new("file:///etc/passwd").expect_err("file URLs must fail"),
        NotificationConfigError::UnsupportedEndpoint
    );
    assert_eq!(
        WebhookEndpoint::new("https://user:password@example.com/hook")
            .expect_err("userinfo must fail"),
        NotificationConfigError::EndpointUserinfo
    );
    assert_eq!(
        WebhookEndpoint::new(format!("https://example.com/{}", "x".repeat(4_096)))
            .expect_err("oversized URLs must fail"),
        NotificationConfigError::EndpointTooLong
    );
    let endpoint = WebhookEndpoint::new("https://example.com/secret-token")
        .expect("HTTPS endpoint should be valid");
    assert_eq!(format!("{endpoint:?}"), "WebhookEndpoint([REDACTED])");
}
