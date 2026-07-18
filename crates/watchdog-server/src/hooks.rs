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
use watchdog_domain::{
    Clock, NativeSessionKey, ObservationEnvelope, RuntimeKind, SessionId, SessionKind,
};

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

    async fn ingest(&self, payload: &[u8]) -> Result<(), HookHttpError> {
        let parser = watchdog_claude::ClaudeHookParser::new(watchdog_claude::TESTED_CLAUDE_VERSION)
            .map_err(|_| HookHttpError::Unavailable)?;
        let event_key = Uuid::new_v5(&claude_hook_namespace(), payload)
            .simple()
            .to_string();
        let evidence = parser
            .parse_hook(payload, &event_key, self.clock.now())
            .map_err(|_| HookHttpError::InvalidPayload)?;
        ingest_evidence(
            &self.api,
            self.clock.as_ref(),
            HookIngestion {
                runtime: RuntimeKind::ClaudeCode,
                adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION,
                source_name: "claude:official-hook",
                event_prefix: "claude-hook",
                subject: evidence.subject().clone(),
                parent: evidence.parent().cloned(),
                kind: evidence.kind(),
                observation: evidence.observation().clone(),
                title: evidence.title().map(ToOwned::to_owned),
            },
        )
        .await
    }
}

impl std::fmt::Debug for ClaudeHookService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClaudeHookService")
            .finish_non_exhaustive()
    }
}

/// Authenticated, bounded ingestion service for official Codex lifecycle hooks.
#[derive(Clone)]
pub struct CodexHookService {
    api: AgentApi,
    clock: Arc<dyn Clock>,
}

impl CodexHookService {
    /// Construct Codex hook ingestion over the shared durable coordinator.
    #[must_use]
    pub fn new(api: AgentApi, clock: Arc<dyn Clock>) -> Self {
        Self { api, clock }
    }

    async fn ingest(&self, payload: &[u8]) -> Result<(), HookHttpError> {
        let parser = watchdog_codex::CodexHookParser::new(watchdog_codex::TESTED_CODEX_VERSION)
            .map_err(|_| HookHttpError::Unavailable)?;
        let event_key = Uuid::new_v5(&codex_hook_namespace(), payload)
            .simple()
            .to_string();
        let evidence = parser
            .parse_hook(payload, &event_key, self.clock.now())
            .map_err(|_| HookHttpError::InvalidPayload)?;
        ingest_evidence(
            &self.api,
            self.clock.as_ref(),
            HookIngestion {
                runtime: RuntimeKind::CodexCli,
                adapter_version: watchdog_codex::TESTED_CODEX_VERSION,
                source_name: "codex:official-hook",
                event_prefix: "codex-hook",
                subject: evidence.subject().clone(),
                parent: evidence.parent().cloned(),
                kind: evidence.kind(),
                observation: evidence.observation().clone(),
                title: evidence.title().map(ToOwned::to_owned),
            },
        )
        .await
    }
}

impl std::fmt::Debug for CodexHookService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodexHookService")
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

/// Build the Bearer-authenticated official Codex hook endpoint.
pub fn codex_hook_router(service: CodexHookService, authenticator: BearerAuthenticator) -> Router {
    Router::new()
        .route("/hooks/codex", post(ingest_codex_hook))
        .with_state(service)
        .layer(DefaultBodyLimit::max(watchdog_codex::MAX_HOOK_BYTES))
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
) -> Result<StatusCode, HookHttpError> {
    service.ingest(&payload).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_codex_hook(
    State(service): State<CodexHookService>,
    payload: Bytes,
) -> Result<StatusCode, HookHttpError> {
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

fn claude_hook_namespace() -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        b"https://github.com/lklimek/agent-watchdog/hooks/claude",
    )
}

fn codex_hook_namespace() -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        b"https://github.com/lklimek/agent-watchdog/hooks/codex",
    )
}

struct HookIngestion {
    runtime: RuntimeKind,
    adapter_version: &'static str,
    source_name: &'static str,
    event_prefix: &'static str,
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    kind: SessionKind,
    observation: ObservationEnvelope,
    title: Option<String>,
}

async fn ingest_evidence(
    api: &AgentApi,
    clock: &dyn Clock,
    evidence: HookIngestion,
) -> Result<(), HookHttpError> {
    let parent = if let Some(parent) = &evidence.parent {
        let parent_id = SessionId::from_native(parent);
        api.discover_session(DiscoveredSession {
            runtime: evidence.runtime,
            native_id: parent.native_id().to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: format!("{}-parent:{parent_id}", evidence.event_prefix),
            adapter_version: evidence.adapter_version.to_owned(),
            evidence_source: format!("{}-parent", evidence.source_name),
            title: None,
            startup_directory: None,
        })
        .await
        .map_err(|_| HookHttpError::Unavailable)?;
        Some(parent_id)
    } else {
        None
    };
    let subject_id = SessionId::from_native(&evidence.subject);
    api.discover_session(DiscoveredSession {
        runtime: evidence.runtime,
        native_id: evidence.subject.native_id().to_owned(),
        kind: evidence.kind,
        parent,
        event_key: format!("{}-session:{subject_id}", evidence.event_prefix),
        adapter_version: evidence.adapter_version.to_owned(),
        evidence_source: evidence.source_name.to_owned(),
        title: evidence.title,
        // Hook paths are untrusted host paths. Automatic discovery or MCP may
        // add them only after capability validation.
        startup_directory: None,
    })
    .await
    .map_err(|_| HookHttpError::Unavailable)?;
    // Discovery commits Starting first, so stamp native state afterwards with
    // a fresh monotonic value.
    let native_state = ObservationEnvelope::new(
        evidence.observation.observation_id(),
        evidence.subject,
        clock.now(),
        evidence.observation.source().clone(),
        evidence.observation.payload().clone(),
    )
    .map_err(|_| HookHttpError::Unavailable)?;
    api.ingest_native_observation(native_state)
        .await
        .map_err(|_| HookHttpError::Unavailable)?;
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum HookHttpError {
    InvalidPayload,
    Unavailable,
}

impl IntoResponse for HookHttpError {
    fn into_response(self) -> Response {
        match self {
            Self::InvalidPayload => StatusCode::BAD_REQUEST,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
        .into_response()
    }
}
