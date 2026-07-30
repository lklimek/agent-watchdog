use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::{Query, Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Redirect, Response, Sse, sse::Event},
    routing::get,
};
use futures::{Stream, StreamExt as _, stream};
use maud::Markup;
use tokio::sync::broadcast;

use super::{
    model::{
        DashboardError, DashboardQuery, DashboardScope, DashboardService, DashboardSnapshot,
        DashboardSort,
    },
    render,
};

const CSS: &str = include_str!("../../../../web/app.css");
const JAVASCRIPT: &str = include_str!("../../../../web/dist/app.js");

/// Build dashboard, read-only JSON, assets, and SSE routes.
///
/// The routes rely on the reverse proxy for authentication; the application
/// container publishes no port of its own.
pub fn dashboard_router(service: DashboardService) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/ui", get(ui))
        .route("/ui/assets/app.css", get(css))
        .route("/ui/assets/app.js", get(javascript))
        .route("/api/v1/sessions", get(sessions))
        .route("/api/v1/events", get(events))
        .with_state(service)
        .layer(middleware::from_fn(security_headers))
}

async fn root() -> Redirect {
    Redirect::temporary("/ui")
}

async fn ui(
    State(service): State<DashboardService>,
    Query(query): Query<DashboardQuery>,
) -> Result<Markup, DashboardHttpError> {
    Ok(render::dashboard(&service.snapshot(query).await?))
}

async fn sessions(
    State(service): State<DashboardService>,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<DashboardSnapshot>, DashboardHttpError> {
    Ok(Json(service.snapshot(query).await?))
}

async fn events(
    State(service): State<DashboardService>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, DashboardHttpError> {
    // Subscribe before reading the snapshot: a concurrent publish may duplicate
    // a revision, but cannot be missed between snapshot and subscription.
    let receiver = service.subscribe();
    let initial = Arc::new(
        service
            .snapshot(DashboardQuery {
                scope: DashboardScope::All,
                sort: DashboardSort::Attention,
            })
            .await?,
    );
    let initial_event = snapshot_event(&initial)?;
    let updates = stream::unfold(receiver, |mut receiver| async move {
        let event = match receiver.recv().await {
            Ok(snapshot) => snapshot_event(&snapshot)
                .unwrap_or_else(|_| Event::default().event("resync_required")),
            Err(broadcast::error::RecvError::Lagged(_)) => {
                Event::default().event("resync_required")
            }
            Err(broadcast::error::RecvError::Closed) => return None,
        };
        Some((Ok(event), receiver))
    });
    Ok(
        Sse::new(stream::once(async { Ok(initial_event) }).chain(updates)).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        ),
    )
}

async fn css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], CSS)
}

async fn javascript() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        JAVASCRIPT,
    )
}

async fn security_headers(request: Request<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn snapshot_event(snapshot: &DashboardSnapshot) -> Result<Event, DashboardError> {
    Ok(Event::default()
        .event("snapshot")
        .id(snapshot.revision.to_string())
        .data(serde_json::to_string(snapshot)?))
}

#[derive(Debug)]
struct DashboardHttpError(DashboardError);

impl From<DashboardError> for DashboardHttpError {
    fn from(error: DashboardError) -> Self {
        Self(error)
    }
}

impl IntoResponse for DashboardHttpError {
    fn into_response(self) -> Response {
        tracing::error!(
            event = "dashboard.request_failed",
            error = %self.0,
            "Dashboard request could not be served"
        );
        (StatusCode::INTERNAL_SERVER_ERROR, "Dashboard unavailable").into_response()
    }
}
