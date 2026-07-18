//! HTTP health and readiness behavior.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use tower::ServiceExt as _;
use watchdog_domain::{TimePoint, WallTimeMs};
use watchdog_runtime::{ComponentId, ComponentStatus};
use watchdog_server::{BasicAuthenticator, HealthService, health_router};
use watchdog_testkit::FakeClock;

#[tokio::test]
async fn liveness_is_minimal_while_detailed_health_is_authenticated() {
    let health = HealthService::new(Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(100),
        100,
    ))));
    let router = health_router(
        health,
        BasicAuthenticator::new("operator", "password").expect("valid auth"),
    );

    let live = router
        .clone()
        .oneshot(
            Request::get("/health/live")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(live.status(), StatusCode::OK);
    assert_eq!(
        axum::body::to_bytes(live.into_body(), 16)
            .await
            .expect("body"),
        "ok"
    );

    let unauthorized = router
        .oneshot(
            Request::get("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn critical_failure_fails_readiness_but_isolated_adapter_failure_is_degraded() {
    let clock = Arc::new(FakeClock::new(TimePoint::new(WallTimeMs::new(100), 100)));
    let health = HealthService::new(clock);
    health.record(
        ComponentId::Adapter(watchdog_domain::RuntimeKind::ClaudeCode),
        ComponentStatus::Failed,
        Some("Adapter schema changed"),
    );
    let router = health_router(
        health.clone(),
        BasicAuthenticator::new("operator", "password").expect("valid auth"),
    );
    assert_eq!(authorized(&router).await, StatusCode::OK);

    health.record(
        ComponentId::Store,
        ComponentStatus::Failed,
        Some("Database unavailable"),
    );
    assert_eq!(authorized(&router).await, StatusCode::SERVICE_UNAVAILABLE);
}

async fn authorized(router: &axum::Router) -> StatusCode {
    let authorization = format!("Basic {}", STANDARD.encode("operator:password"));
    router
        .clone()
        .oneshot(
            Request::get("/health")
                .header(axum::http::header::AUTHORIZATION, authorization)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response")
        .status()
}
