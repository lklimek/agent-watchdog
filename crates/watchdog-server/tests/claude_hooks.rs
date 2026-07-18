//! Authenticated official Claude lifecycle-hook ingestion acceptance tests.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use tower::ServiceExt as _;
use watchdog_domain::{DetailedState, SessionKind, TimePoint, WallTimeMs};
use watchdog_server::{AgentApi, BearerAuthenticator, ClaudeHookService, claude_hook_router};
use watchdog_store::WatchdogStore;
use watchdog_testkit::FakeClock;

#[tokio::test]
async fn bearer_hook_ingestion_creates_exact_hierarchy_without_retaining_body_content() {
    let fixture = tempfile::tempdir().expect("fixture should exist");
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(10_000),
        5_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let service = ClaudeHookService::new(api, clock);
    let router = claude_hook_router(
        service,
        BearerAuthenticator::new("hook-secret").expect("token should be valid"),
    );

    assert_hook_http_boundaries(&router).await;
    let main = br#"{"session_id":"main-1","cwd":"/private/repository","hook_event_name":"SessionStart","transcript_path":"/private/transcript","last_assistant_message":"SECRET_HOOK_BODY"}"#;
    assert_eq!(
        send_hook(&router, Some("Bearer hook-secret"), main).await,
        StatusCode::NO_CONTENT
    );
    let after_main = store.counts().await.expect("counts should query");
    assert_eq!(
        send_hook(&router, Some("Bearer hook-secret"), main).await,
        StatusCode::NO_CONTENT,
        "native retries should remain idempotent"
    );
    assert_eq!(
        store.counts().await.expect("counts should query"),
        after_main
    );
    let child = br#"{"session_id":"main-1","agent_id":"child-1","agent_type":"security-reviewer","cwd":"/private/repository","hook_event_name":"SubagentStart","prompt":"SECRET_CHILD_PROMPT"}"#;
    assert_eq!(
        send_hook(&router, Some("Bearer hook-secret"), child).await,
        StatusCode::NO_CONTENT
    );

    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(mains.len(), 1);
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].root, mains[0].root);
    let relations = store
        .relations_for_root(mains[0].root, 10)
        .await
        .expect("relations should query");
    assert_eq!(relations.len(), 1);
    assert!(relations[0].selected);
    let child_snapshot = store
        .snapshot(children[0].session)
        .await
        .expect("snapshot should query")
        .expect("snapshot should exist");
    let child_snapshot = child_snapshot
        .reducer_snapshot()
        .expect("reducer snapshot should exist");
    assert_eq!(child_snapshot.state(), DetailedState::Starting);
    assert!(!format!("{child_snapshot:?}").contains("SECRET"));
    let metadata = store
        .session_metadata(children[0].session)
        .await
        .expect("metadata should query")
        .expect("metadata should exist");
    assert_eq!(metadata.title(), Some("security-reviewer"));
    assert_eq!(metadata.startup_directory(), None);
    assert!(
        store
            .counts()
            .await
            .expect("counts should query")
            .observations
            > after_main.observations
    );
}

async fn assert_hook_http_boundaries(router: &axum::Router) {
    let valid = br#"{"session_id":"main-1","hook_event_name":"SessionStart"}"#;
    assert_eq!(
        send_hook(router, None, valid).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send_hook(router, Some("Bearer wrong-secret"), valid).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send_hook(router, Some("Bearer hook-secret"), b"{malformed").await,
        StatusCode::BAD_REQUEST
    );
    let oversized = vec![b'x'; watchdog_claude::MAX_HOOK_BYTES + 1];
    assert_eq!(
        send_hook(router, Some("Bearer hook-secret"), &oversized).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

async fn send_hook(
    router: &axum::Router,
    authorization: Option<&str>,
    payload: &[u8],
) -> StatusCode {
    let mut request = Request::post("/hooks/claude")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_vec()))
        .expect("request should build");
    if let Some(authorization) = authorization {
        request.headers_mut().insert(
            header::AUTHORIZATION,
            authorization.parse().expect("authorization should parse"),
        );
    }
    router
        .clone()
        .oneshot(request)
        .await
        .expect("router should respond")
        .status()
}
