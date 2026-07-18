//! Authenticated official Codex lifecycle-hook ingestion acceptance tests.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use tower::ServiceExt as _;
use watchdog_domain::{Clock, DetailedState, SessionKind, TimePoint, WallTimeMs};
use watchdog_server::{AgentApi, BearerAuthenticator, CodexHookService, codex_hook_router};
use watchdog_store::WatchdogStore;

#[tokio::test]
async fn bearer_hook_ingestion_creates_exact_codex_hierarchy_without_retaining_body_content() {
    let fixture = tempfile::tempdir().expect("fixture should exist");
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(AdvancingClock::default());
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let service = CodexHookService::new(api, clock);
    let router = codex_hook_router(
        service,
        BearerAuthenticator::new("hook-secret").expect("token should be valid"),
    );

    assert_hook_http_boundaries(&router).await;
    let main = br#"{"session_id":"main-1","cwd":"/private/repository","hook_event_name":"SessionStart","transcript_path":"/private/transcript","source":"startup"}"#;
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
    let child = br#"{"session_id":"main-1","agent_id":"child-1","agent_type":"reviewer","cwd":"/private/repository","hook_event_name":"SubagentStart","turn_id":"turn-1","permission_mode":"default","systemMessage":"SECRET_CHILD_PROMPT"}"#;
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
    let child_snapshot_record = store
        .snapshot(children[0].session)
        .await
        .expect("snapshot should query")
        .expect("snapshot should exist");
    let child_snapshot = child_snapshot_record
        .reducer_snapshot()
        .expect("reducer snapshot should exist");
    assert_eq!(child_snapshot.state(), DetailedState::Starting);
    assert!(!format!("{child_snapshot:?}").contains("SECRET"));
    let metadata = store
        .session_metadata(children[0].session)
        .await
        .expect("metadata should query")
        .expect("metadata should exist");
    assert_eq!(metadata.title(), Some("reviewer"));
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

#[derive(Debug, Default)]
struct AdvancingClock {
    next: AtomicU64,
}

impl Clock for AdvancingClock {
    fn now(&self) -> TimePoint {
        let sequence = self.next.fetch_add(1, Ordering::Relaxed);
        TimePoint::new(
            WallTimeMs::new(10_000 + i64::try_from(sequence).expect("test time should fit")),
            5_000 + sequence,
        )
    }
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
    let oversized = vec![b'x'; watchdog_codex::MAX_HOOK_BYTES + 1];
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
    let mut request = Request::post("/hooks/codex")
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
