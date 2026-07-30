//! HTTP health and readiness behavior.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt as _;
use watchdog_domain::{TimePoint, WallTimeMs};
use watchdog_runtime::{ComponentId, ComponentStatus};
use watchdog_server::{HealthService, health_router};
use watchdog_testkit::FakeClock;

#[tokio::test]
async fn liveness_is_minimal_while_detailed_health_reports_components() {
    let health = HealthService::new(Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(100),
        100,
    ))));
    health.record(ComponentId::Store, ComponentStatus::Healthy, None);
    let router = health_router(health);

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

    let detailed = router
        .oneshot(
            Request::get("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(detailed.status(), StatusCode::OK);
    let body = axum::body::to_bytes(detailed.into_body(), 16_384)
        .await
        .expect("body");
    assert!(String::from_utf8_lossy(&body).contains("\"store\""));
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
    let router = health_router(health.clone());
    assert_eq!(status(&router).await, StatusCode::OK);

    health.record(
        ComponentId::Store,
        ComponentStatus::Failed,
        Some("Database unavailable"),
    );
    assert_eq!(status(&router).await, StatusCode::SERVICE_UNAVAILABLE);
}

async fn status(router: &axum::Router) -> StatusCode {
    router
        .clone()
        .oneshot(
            Request::get("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response")
        .status()
}
