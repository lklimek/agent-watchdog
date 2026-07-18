use std::sync::Arc;

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use uuid::Uuid;
use watchdog_domain::{Clock, RuntimeKind, SessionId, SessionKind};

use crate::{AgentApi, BearerAuthenticator, DiscoveredSession};

/// Authenticated, bounded ingestion service for official Claude lifecycle hooks.
#[derive(Clone)]
pub struct ClaudeHookService {
    api: AgentApi,
    clock: Arc<dyn Clock>,
}

impl ClaudeHookService {
    /// Construct hook ingestion over the shared durable coordinator.
    #[must_use]
    pub fn new(api: AgentApi, clock: Arc<dyn Clock>) -> Self {
        Self { api, clock }
    }

    async fn ingest(&self, payload: &[u8]) -> Result<(), ClaudeHookHttpError> {
        let parser = watchdog_claude::ClaudeHookParser::new(watchdog_claude::TESTED_CLAUDE_VERSION)
            .map_err(|_| ClaudeHookHttpError::Unavailable)?;
        let event_key = Uuid::new_v5(&hook_namespace(), payload)
            .simple()
            .to_string();
        let evidence = parser
            .parse_hook(payload, &event_key, self.clock.now())
            .map_err(|_| ClaudeHookHttpError::InvalidPayload)?;
        let parent = if let Some(parent) = evidence.parent() {
            let parent_id = SessionId::from_native(parent);
            self.api
                .discover_session(DiscoveredSession {
                    runtime: RuntimeKind::ClaudeCode,
                    native_id: parent.native_id().to_owned(),
                    kind: SessionKind::Main,
                    parent: None,
                    event_key: format!("claude-hook-parent:{parent_id}"),
                    adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION.to_owned(),
                    evidence_source: "claude:official-hook-parent".to_owned(),
                    title: None,
                    startup_directory: None,
                })
                .await
                .map_err(|_| ClaudeHookHttpError::Unavailable)?;
            Some(parent_id)
        } else {
            None
        };
        let subject_id = SessionId::from_native(evidence.subject());
        self.api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: evidence.subject().native_id().to_owned(),
                kind: evidence.kind(),
                parent,
                event_key: format!("claude-hook-session:{subject_id}"),
                adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION.to_owned(),
                evidence_source: "claude:official-hook".to_owned(),
                title: evidence.title().map(ToOwned::to_owned),
                // Hook paths are untrusted host paths. Automatic discovery or
                // MCP may add them only after capability validation.
                startup_directory: None,
            })
            .await
            .map_err(|_| ClaudeHookHttpError::Unavailable)?;
        self.api
            .ingest_native_observation(evidence.observation().clone())
            .await
            .map_err(|_| ClaudeHookHttpError::Unavailable)?;
        Ok(())
    }
}

impl std::fmt::Debug for ClaudeHookService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClaudeHookService")
            .finish_non_exhaustive()
    }
}

/// Build the Bearer-authenticated official Claude hook endpoint.
pub fn claude_hook_router(
    service: ClaudeHookService,
    authenticator: BearerAuthenticator,
) -> Router {
    Router::new()
        .route("/hooks/claude", post(ingest_claude_hook))
        .with_state(service)
        .layer(DefaultBodyLimit::max(watchdog_claude::MAX_HOOK_BYTES))
        .layer(middleware::from_fn(
            move |request: Request<Body>, next: Next| {
                let authenticator = authenticator.clone();
                async move { bearer_auth(request, next, &authenticator).await }
            },
        ))
}

async fn ingest_claude_hook(
    State(service): State<ClaudeHookService>,
    payload: Bytes,
) -> Result<StatusCode, ClaudeHookHttpError> {
    service.ingest(&payload).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn bearer_auth(
    request: Request<Body>,
    next: Next,
    authenticator: &BearerAuthenticator,
) -> Response {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .is_some_and(|value| authenticator.authorize(Some(value.as_bytes())));
    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}

fn hook_namespace() -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        b"https://github.com/lklimek/agent-watchdog/hooks/claude",
    )
}

#[derive(Clone, Copy, Debug)]
enum ClaudeHookHttpError {
    InvalidPayload,
    Unavailable,
}

impl IntoResponse for ClaudeHookHttpError {
    fn into_response(self) -> Response {
        match self {
            Self::InvalidPayload => StatusCode::BAD_REQUEST,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
        .into_response()
    }
}
