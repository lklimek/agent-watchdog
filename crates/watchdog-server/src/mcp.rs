use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
    middleware,
    middleware::Next,
    response::{IntoResponse, Response},
};
use futures::Stream;
use rmcp::{
    Json, RoleServer, ServerHandler,
    handler::server::wrapper::Parameters,
    model::{
        CallToolResult, ClientJsonRpcMessage, ContentBlock, ErrorCode, Implementation,
        ProtocolVersion, ServerCapabilities, ServerInfo, ServerJsonRpcMessage,
    },
    schemars,
    service::RequestContext,
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        session::{
            RestoreOutcome, ServerSseMessage, SessionId as McpSessionId, SessionManager,
            local::{LocalSessionManager, LocalSessionManagerError},
        },
        tower::{StreamableHttpServerConfig, StreamableHttpService},
    },
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::Mutex, time::Instant};
use watchdog_domain::{
    DeadlineCommand, DetailedState, RuntimeKind, SessionId, SessionKind, WallTimeMs,
};

use crate::{
    AgentApi, AgentApiError, AgentHealthView, BearerAuthenticator, CompletionOutcome, EventPage,
    RegisterSession, RegisteredWatchPathView, SessionTreeView, SessionView, TransportKey,
    WaitingKind,
    agent_api::{MAX_TREE_SESSIONS, McpSessionGauge},
};

const DEFAULT_EVENT_PAGE_SIZE: u32 = 100;
/// `Display` text of [`WatchdogSessionManagerError::Capacity`], matched to turn
/// rmcp's fixed HTTP 500 into a retryable answer. Locked to the variant by test.
const CAPACITY_MARKER: &str = "MCP session capacity is exhausted";
const CAPACITY_RETRY_AFTER_SECONDS: &str = "5";
const MAX_MCP_ERROR_BODY_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransportScope(McpSessionId);

/// Bounds the MCP endpoint applies to transport admission and request reads.
///
/// Every bound is read once at startup: rmcp bakes the idle timeout into each
/// session worker when the session is created, so none is `SIGHUP`-reloadable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpLimits {
    max_sessions: usize,
    idle_ttl: Duration,
    request_body_timeout: Duration,
}

impl McpLimits {
    /// Concurrent authenticated transports admitted when unconfigured.
    pub const DEFAULT_MAX_SESSIONS: usize = 64;
    /// Idle period after which an abandoned transport is reclaimed, unconfigured.
    pub const DEFAULT_IDLE_TTL: Duration = Duration::from_hours(48);
    /// Patience for one stalled request body when unconfigured.
    pub const DEFAULT_REQUEST_BODY_TIMEOUT: Duration = Duration::from_secs(30);

    /// Validate one set of MCP endpoint bounds.
    ///
    /// # Errors
    ///
    /// Returns [`McpLimitsError`] when any bound is zero. A zero cap would admit
    /// nothing and leave eviction with no slot to free, a zero idle TTL would
    /// expire every transport immediately, and a zero body timeout would reject
    /// every request before its first byte.
    pub const fn new(
        max_sessions: usize,
        idle_ttl: Duration,
        request_body_timeout: Duration,
    ) -> Result<Self, McpLimitsError> {
        if max_sessions == 0 {
            return Err(McpLimitsError::EmptyCapacity);
        }
        if idle_ttl.is_zero() {
            return Err(McpLimitsError::ZeroIdleTtl);
        }
        if request_body_timeout.is_zero() {
            return Err(McpLimitsError::ZeroRequestBodyTimeout);
        }
        Ok(Self {
            max_sessions,
            idle_ttl,
            request_body_timeout,
        })
    }

    /// Concurrent authenticated transports this policy admits.
    #[must_use]
    pub const fn max_sessions(self) -> usize {
        self.max_sessions
    }

    /// Idle period after which rmcp reclaims an abandoned transport.
    #[must_use]
    pub const fn idle_ttl(self) -> Duration {
        self.idle_ttl
    }

    /// Time one request body has to arrive in full before it is refused.
    #[must_use]
    pub const fn request_body_timeout(self) -> Duration {
        self.request_body_timeout
    }
}

impl Default for McpLimits {
    fn default() -> Self {
        Self {
            max_sessions: Self::DEFAULT_MAX_SESSIONS,
            idle_ttl: Self::DEFAULT_IDLE_TTL,
            request_body_timeout: Self::DEFAULT_REQUEST_BODY_TIMEOUT,
        }
    }
}

/// Rejected MCP endpoint bounds.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum McpLimitsError {
    /// `max_sessions` was zero.
    #[error("MCP max_sessions must be at least 1")]
    EmptyCapacity,
    /// `idle_ttl_seconds` was zero.
    #[error("MCP idle_ttl_seconds must be at least 1")]
    ZeroIdleTtl,
    /// `request_body_timeout_seconds` was zero.
    #[error("MCP request_body_timeout_seconds must be at least 1")]
    ZeroRequestBodyTimeout,
}

/// rmcp session manager that injects the opaque transport identity into every
/// request context for application-level main-session scoping.
///
/// Sessions survive normal long idle periods and abandoned transports expire on
/// the configured idle TTL. At capacity, admission evicts the longest-idle
/// transport instead of refusing, so a stale binding never outranks a live one.
#[derive(Debug)]
pub struct WatchdogSessionManager {
    inner: LocalSessionManager,
    limits: McpLimits,
    creation: Mutex<()>,
    activity: std::sync::RwLock<HashMap<McpSessionId, Instant>>,
    occupancy: Arc<McpSessionGauge>,
    api: Option<AgentApi>,
}

/// Failure while managing one stateful MCP transport.
#[derive(Debug, Error)]
pub enum WatchdogSessionManagerError {
    /// rmcp rejected the requested session operation.
    #[error(transparent)]
    Local(#[from] LocalSessionManagerError),
    /// The bounded authenticated session pool is full and offered no evictable
    /// transport. Unreachable while `max_sessions` is at least 1.
    #[error("MCP session capacity is exhausted")]
    Capacity,
}

impl WatchdogSessionManager {
    /// Construct an rmcp session manager that releases application scope when
    /// a transport closes, expires, or is evicted under admission pressure.
    #[must_use]
    pub fn new(api: AgentApi, limits: McpLimits) -> Self {
        Self::build(Some(api), limits)
    }

    #[cfg(test)]
    fn without_application_scope(limits: McpLimits) -> Self {
        Self::build(None, limits)
    }

    fn build(api: Option<AgentApi>, limits: McpLimits) -> Self {
        let mut inner = LocalSessionManager::default();
        inner.session_config.keep_alive = Some(limits.idle_ttl);
        Self {
            inner,
            limits,
            creation: Mutex::new(()),
            activity: std::sync::RwLock::new(HashMap::new()),
            occupancy: Arc::new(McpSessionGauge::new(limits.max_sessions)),
            api,
        }
    }

    /// Current admission occupancy against this manager's configured cap.
    #[must_use]
    pub fn occupancy(&self) -> crate::McpSessionOccupancy {
        self.occupancy.view()
    }

    /// Shared counters published to `get_watchdog_health`.
    fn gauge(&self) -> Arc<McpSessionGauge> {
        Arc::clone(&self.occupancy)
    }

    fn scoped(id: &McpSessionId, mut message: ClientJsonRpcMessage) -> ClientJsonRpcMessage {
        message.insert_extension(TransportScope(id.clone()));
        message
    }

    fn touch(&self, id: &McpSessionId) {
        self.activity
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.clone(), Instant::now());
    }

    fn forget(&self, id: &McpSessionId) {
        self.activity
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
    }

    async fn publish_occupancy(&self) {
        self.occupancy
            .set_admitted(self.inner.sessions.read().await.len());
    }

    /// Longest-idle live transport, preferring one with no recorded activity.
    async fn longest_idle(&self) -> Option<McpSessionId> {
        let live = self
            .inner
            .sessions
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let activity = self
            .activity
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.into_iter().min_by_key(|id| activity.get(id).copied())
    }

    /// Ensure one free admission slot, evicting the longest-idle transport when
    /// already at capacity. Callers must hold the creation mutex.
    async fn reserve_slot(
        &self,
        restoring: Option<&McpSessionId>,
    ) -> Result<(), WatchdogSessionManagerError> {
        let occupancy = {
            let sessions = self.inner.sessions.read().await;
            if restoring.is_some_and(|id| sessions.contains_key(id)) {
                return Ok(());
            }
            sessions.len()
        };
        if occupancy < self.limits.max_sessions {
            return Ok(());
        }
        let Some(evicted) = self.longest_idle().await else {
            return Err(WatchdogSessionManagerError::Capacity);
        };
        tracing::warn!(
            event = "mcp.session_evicted",
            session = %evicted,
            occupancy,
            capacity = self.limits.max_sessions,
            "Evicted the longest-idle MCP transport to admit a new one; raise [mcp] max_sessions in watchdog.toml if this recurs"
        );
        self.close_session(&evicted).await?;
        self.occupancy.record_eviction();
        Ok(())
    }
}

impl SessionManager for WatchdogSessionManager {
    type Error = WatchdogSessionManagerError;
    type Transport = <LocalSessionManager as SessionManager>::Transport;

    async fn create_session(&self) -> Result<(McpSessionId, Self::Transport), Self::Error> {
        let _creation = self.creation.lock().await;
        self.reserve_slot(None).await?;
        let (id, transport) = self.inner.create_session().await?;
        self.touch(&id);
        self.publish_occupancy().await;
        Ok((id, transport))
    }

    async fn initialize_session(
        &self,
        id: &McpSessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        let response = self
            .inner
            .initialize_session(id, Self::scoped(id, message))
            .await?;
        self.touch(id);
        Ok(response)
    }

    async fn has_session(&self, id: &McpSessionId) -> Result<bool, Self::Error> {
        Ok(self.inner.has_session(id).await?)
    }

    async fn close_session(&self, id: &McpSessionId) -> Result<(), Self::Error> {
        // rmcp treats an unknown ID as a silent success, so release the
        // application scope only for a transport that actually existed, and on
        // the inner error path too — nothing else ever releases it.
        let existed = self.inner.has_session(id).await?;
        let outcome = self.inner.close_session(id).await;
        if existed {
            self.forget(id);
            if let Some(api) = &self.api
                && let Ok(transport) = TransportKey::new(id.to_string())
            {
                api.release_transport_scope(&transport);
            }
        }
        self.publish_occupancy().await;
        Ok(outcome?)
    }

    async fn create_stream(
        &self,
        id: &McpSessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let stream = self
            .inner
            .create_stream(id, Self::scoped(id, message))
            .await?;
        self.touch(id);
        Ok(stream)
    }

    async fn accept_message(
        &self,
        id: &McpSessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        self.inner
            .accept_message(id, Self::scoped(id, message))
            .await?;
        self.touch(id);
        Ok(())
    }

    async fn create_standalone_stream(
        &self,
        id: &McpSessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let stream = self.inner.create_standalone_stream(id).await?;
        self.touch(id);
        Ok(stream)
    }

    async fn resume(
        &self,
        id: &McpSessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let stream = self.inner.resume(id, last_event_id).await?;
        self.touch(id);
        Ok(stream)
    }

    async fn restore_session(
        &self,
        id: McpSessionId,
    ) -> Result<RestoreOutcome<Self::Transport>, Self::Error> {
        let _creation = self.creation.lock().await;
        self.reserve_slot(Some(&id)).await?;
        let outcome = self.inner.restore_session(id.clone()).await?;
        if !matches!(outcome, RestoreOutcome::NotSupported) {
            self.touch(&id);
            self.publish_occupancy().await;
        }
        Ok(outcome)
    }
}

/// Create the stateful Streamable HTTP MCP service and its occupancy gauge.
fn mcp_http_service(
    api: AgentApi,
    limits: McpLimits,
) -> StreamableHttpService<WatchdogMcpService, WatchdogSessionManager> {
    let manager = Arc::new(WatchdogSessionManager::new(api.clone(), limits));
    api.configure_mcp_sessions(manager.gauge());
    StreamableHttpService::new(
        move || Ok(WatchdogMcpService::new(api.clone())),
        manager,
        StreamableHttpServerConfig::default(),
    )
}

/// Build the `/mcp` Streamable HTTP route behind strict shared-token auth.
///
/// Authentication runs before rmcp parses or allocates protocol state, and a
/// request body that never arrives is refused before rmcp waits on it forever.
/// Failure responses are deliberately fixed and never reflect credential input.
pub fn mcp_router(api: AgentApi, authenticator: BearerAuthenticator, limits: McpLimits) -> Router {
    let service = mcp_http_service(api, limits);
    let request_body_timeout = limits.request_body_timeout();
    Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn(
            move |request: Request<Body>, next: Next| {
                bounded_request_body(request, next, request_body_timeout)
            },
        ))
        .layer(middleware::from_fn(
            move |request: Request<Body>, next: Next| {
                let authenticator = authenticator.clone();
                async move {
                    let authorized = request
                        .headers()
                        .get(header::AUTHORIZATION)
                        .is_some_and(|value| authenticator.authorize(Some(value.as_bytes())));
                    if !authorized {
                        tracing::warn!(
                            event = "auth.rejected",
                            route = "/mcp",
                            "MCP bearer credential rejected"
                        );
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    retryable_capacity_response(next.run(request).await).await
                }
            },
        ))
}

/// Bound how long the endpoint waits for one MCP request body.
///
/// rmcp collects the whole POST body before any session lookup or logging and
/// never bounds that read, so a client stalling mid-send parks the handler with
/// nothing recorded anywhere. Request bodies only: a server-push stream is a
/// response body and stays long-lived.
async fn bounded_request_body(request: Request<Body>, next: Next, timeout: Duration) -> Response {
    if request.method() != Method::POST {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    match tokio::time::timeout(timeout, axum::body::to_bytes(body, usize::MAX)).await {
        Ok(Ok(bytes)) => {
            next.run(Request::from_parts(parts, Body::from(bytes)))
                .await
        }
        Ok(Err(error)) => {
            tracing::warn!(
                event = "mcp.request_body_unreadable",
                %error,
                "MCP request body failed mid-read; the client connection broke before the body completed"
            );
            (StatusCode::BAD_REQUEST, "MCP request body was unreadable").into_response()
        }
        Err(_elapsed) => {
            tracing::warn!(
                event = "mcp.request_body_timeout",
                timeout_seconds = timeout.as_secs(),
                "MCP request body did not arrive in full within the configured bound; raise [mcp] request_body_timeout_seconds in watchdog.toml if legitimate clients need longer"
            );
            (
                StatusCode::REQUEST_TIMEOUT,
                "MCP request body timed out before it was complete",
            )
                .into_response()
        }
    }
}

/// Re-answer capacity exhaustion as retryable.
///
/// rmcp maps every `SessionManager` error to a fixed HTTP 500 with no hook, so
/// the only place to distinguish a bounded-resource refusal from a server defect
/// is on the way out.
async fn retryable_capacity_response(response: Response) -> Response {
    if response.status() != StatusCode::INTERNAL_SERVER_ERROR {
        return response;
    }
    let (parts, body) = response.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, MAX_MCP_ERROR_BODY_BYTES).await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if !String::from_utf8_lossy(&bytes).contains(CAPACITY_MARKER) {
        return Response::from_parts(parts, Body::from(bytes));
    }
    tracing::warn!(
        event = "mcp.capacity_exhausted",
        "MCP admission found no evictable transport; answering 503"
    );
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, CAPACITY_RETRY_AFTER_SECONDS)],
        Body::from(bytes),
    )
        .into_response()
}

/// MCP protocol façade over the scoped durable agent API.
#[derive(Clone, Debug)]
pub struct WatchdogMcpService {
    api: AgentApi,
}

impl WatchdogMcpService {
    /// Names of every tool this service registers with the MCP router.
    ///
    /// Exposed so contract tests can check documentation against the actual
    /// tool surface instead of a second hand-maintained list.
    #[must_use]
    pub fn registered_tool_names() -> Vec<String> {
        Self::tool_router()
            .list_all()
            .into_iter()
            .map(|tool| tool.name.into_owned())
            .collect()
    }
}

#[tool_router]
impl WatchdogMcpService {
    /// Construct one per-transport protocol handler over shared application state.
    #[must_use]
    pub fn new(api: AgentApi) -> Self {
        Self { api }
    }

    #[tool(
        description = "Register or enrich a session. Use kind=main to start and bind this transport to a new immutable session tree; kind=child names an existing parent_session_id and binds this transport directly to that parent's tree, including from a fresh transport. Supported runtimes: claude_code, codex_cli, codex_companion"
    )]
    async fn register_session(
        &self,
        Parameters(params): Parameters<RegisterSessionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SessionView>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        let kind = params.kind.into();
        let parent = params
            .parent_session_id
            .as_deref()
            .map(parse_session_id)
            .transpose()?;
        structured_json_result(
            self.api
                .register_session(
                    &transport,
                    RegisterSession {
                        runtime: params.runtime.into(),
                        native_id: params.native_id,
                        kind,
                        parent,
                        event_key: params.event_key,
                    },
                )
                .await,
        )
    }

    #[tool(
        description = "Record an exact relation between two existing sessions in the bound tree and optionally set the child's expected check-in time"
    )]
    async fn register_delegation(
        &self,
        Parameters(params): Parameters<RegisterDelegationParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SessionView>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        let deadline = params
            .deadline_ms
            .map(|value| DeadlineCommand::Set(WallTimeMs::new(value)));
        structured_json_result(
            self.api
                .register_delegation(
                    &transport,
                    parse_session_id(&params.parent_session_id)?,
                    parse_session_id(&params.child_session_id)?,
                    &params.event_key,
                    deadline,
                )
                .await,
        )
    }

    #[tool(
        description = "Watch one existing directory beneath a configured worktree prefix for an in-tree session"
    )]
    async fn register_watch_path(
        &self,
        Parameters(params): Parameters<RegisterWatchPathParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<RegisteredWatchPathView>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        structured_json_result(
            self.api
                .register_watch_path(
                    &transport,
                    parse_session_id(&params.session_id)?,
                    &params.event_key,
                    &params.path,
                )
                .await,
        )
    }

    #[tool(description = "Record bounded progress text for one session in the bound tree")]
    async fn report_progress(
        &self,
        Parameters(params): Parameters<ProgressParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SessionView>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        structured_json_result(
            self.api
                .report_progress(
                    &transport,
                    parse_session_id(&params.session_id)?,
                    &params.event_key,
                    params.summary,
                    params.operation,
                )
                .await,
        )
    }

    #[tool(
        description = "Mark an in-tree session as waiting for an agent, tool, user, or intentional pause; intentional pauses also pause timers"
    )]
    async fn report_waiting(
        &self,
        Parameters(params): Parameters<WaitingParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SessionView>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        structured_json_result(
            self.api
                .report_waiting(
                    &transport,
                    parse_session_id(&params.session_id)?,
                    &params.event_key,
                    params.waiting_for.into(),
                )
                .await,
        )
    }

    #[tool(description = "Record a completed, failed, or cancelled terminal outcome")]
    async fn complete_session(
        &self,
        Parameters(params): Parameters<CompleteParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SessionView>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        structured_json_result(
            self.api
                .complete_session(
                    &transport,
                    parse_session_id(&params.session_id)?,
                    &params.event_key,
                    params.outcome.into(),
                )
                .await,
        )
    }

    #[tool(
        description = "Set an absolute expected check-in time, pause or resume timer accounting, or clear the explicit deadline"
    )]
    async fn update_deadline(
        &self,
        Parameters(params): Parameters<DeadlineParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SessionView>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        let command = params.action.command(params.deadline_ms)?;
        structured_json_result(
            self.api
                .update_deadline(
                    &transport,
                    parse_session_id(&params.session_id)?,
                    &params.event_key,
                    command,
                )
                .await,
        )
    }

    #[tool(description = "Read one normalized session in the bound tree with agent diagnostics")]
    async fn get_session(
        &self,
        Parameters(params): Parameters<SessionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SessionView>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        structured_json_result(
            self.api
                .get_session(&transport, parse_session_id(&params.session_id)?)
                .await,
        )
    }

    #[tool(
        description = "List up to 1000 sessions in the bound tree with optional normalized-state filters"
    )]
    async fn list_sessions(
        &self,
        Parameters(params): Parameters<ListSessionsParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        let states = params
            .states
            .unwrap_or_default()
            .into_iter()
            .map(Into::into)
            .collect::<Vec<DetailedState>>();
        let limit = params.limit.unwrap_or(MAX_TREE_SESSIONS);
        if limit == 0 || limit > MAX_TREE_SESSIONS {
            return Err(invalid_params(format!(
                "limit must be between 1 and {MAX_TREE_SESSIONS}"
            )));
        }
        let mut sessions = self
            .api
            .list_sessions(&transport)
            .await
            .map_err(api_error)?;
        if !states.is_empty() {
            sessions.retain(|view| states.contains(&view.snapshot.state()));
        }
        sessions.truncate(limit as usize);
        success(&sessions)
    }

    #[tool(description = "Read the complete bound hierarchy and retained relation evidence")]
    async fn get_session_tree(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SessionTreeView>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        structured_json_result(self.api.session_tree(&transport).await)
    }

    #[tool(
        description = "Read up to 500 durable tree events; after acknowledges a previously processed next_cursor before reading later events"
    )]
    async fn list_events(
        &self,
        Parameters(params): Parameters<ListEventsParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<EventPage>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        structured_json_result(
            self.api
                .list_events(
                    &transport,
                    params.after,
                    params.limit.unwrap_or(DEFAULT_EVENT_PAGE_SIZE),
                )
                .await,
        )
    }

    #[tool(description = "Read Watchdog storage and runtime-adapter health for the bound tree")]
    async fn get_watchdog_health(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<AgentHealthView>, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        structured_json_result(self.api.health(&transport).await)
    }
}

#[tool_handler]
impl ServerHandler for WatchdogMcpService {
    fn get_info(&self) -> ServerInfo {
        let mut implementation = Implementation::default();
        "agent-watchdog".clone_into(&mut implementation.name);
        implementation.title = Some("Agent Watchdog".to_owned());
        env!("CARGO_PKG_VERSION").clone_into(&mut implementation.version);
        implementation.description =
            Some("Monitoring subagents in multi-agent orchestration sessions".to_owned());
        implementation.website_url = Some("https://github.com/lklimek/agent-watchdog".to_owned());
        let mut info = ServerInfo::default();
        info.protocol_version = ProtocolVersion::LATEST;
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info = implementation;
        info.instructions = Some(
            "Choose the first registration by role. A coordinator starts or reconnects by calling register_session with kind=main. A child handed an in-tree parent_session_id starts or reconnects by calling register_session with kind=child and that parent, without registering a main first. For every mutation, generate a fresh event_key and reuse it only when retrying the identical mutation. Treat list_events as a durable inbox: process the returned events, then pass that page's next_cursor as after to acknowledge them."
                .to_owned(),
        );
        info
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RegisterSessionParams {
    /// Runtime namespace containing the native session identifier.
    runtime: RegisterSessionRuntime,
    /// Runtime-native session, job, or thread identifier; not a Watchdog UUID.
    native_id: String,
    /// Role of this session in the monitored hierarchy.
    kind: SessionKindParam,
    /// Watchdog session UUID returned by registration. Required when kind is child; omit for main.
    parent_session_id: Option<String>,
    /// Idempotency key; reuse only to retry the identical mutation.
    event_key: String,
}

#[derive(Clone, Copy, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RegisterSessionRuntime {
    ClaudeCode,
    CodexCli,
    CodexCompanion,
}

#[derive(Clone, Copy, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SessionKindParam {
    Main,
    Child,
}

impl From<SessionKindParam> for SessionKind {
    fn from(kind: SessionKindParam) -> Self {
        match kind {
            SessionKindParam::Main => Self::Main,
            SessionKindParam::Child => Self::Child,
        }
    }
}

impl From<RegisterSessionRuntime> for RuntimeKind {
    fn from(runtime: RegisterSessionRuntime) -> Self {
        match runtime {
            RegisterSessionRuntime::ClaudeCode => Self::ClaudeCode,
            RegisterSessionRuntime::CodexCli => Self::CodexCli,
            RegisterSessionRuntime::CodexCompanion => Self::CodexCompanion,
        }
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RegisterDelegationParams {
    /// Watchdog UUID of an existing parent session in the bound tree.
    parent_session_id: String,
    /// Watchdog UUID of an existing child session in the bound tree.
    child_session_id: String,
    /// Idempotency key; reuse only to retry the identical mutation.
    event_key: String,
    /// Optional absolute Unix epoch time in milliseconds for the child's expected check-in.
    deadline_ms: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RegisterWatchPathParams {
    /// Watchdog session UUID returned by registration.
    session_id: String,
    /// Idempotency key; reuse only to retry the identical mutation.
    event_key: String,
    /// Existing directory beneath a configured worktree prefix.
    path: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ProgressParams {
    /// Watchdog session UUID returned by registration.
    session_id: String,
    /// Idempotency key; reuse only to retry the identical mutation.
    event_key: String,
    /// Bounded human-readable description of current progress.
    summary: String,
    /// Optional free-text operation label included with the progress summary.
    operation: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct WaitingParams {
    /// Watchdog session UUID returned by registration.
    session_id: String,
    /// Idempotency key; reuse only to retry the identical mutation.
    event_key: String,
    /// Reason the session is waiting.
    waiting_for: WaitingKindParam,
}

#[derive(Clone, Copy, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum WaitingKindParam {
    Agent,
    Tool,
    User,
    Intentional,
}

impl From<WaitingKindParam> for WaitingKind {
    fn from(kind: WaitingKindParam) -> Self {
        match kind {
            WaitingKindParam::Agent => Self::Agent,
            WaitingKindParam::Tool => Self::Tool,
            WaitingKindParam::User => Self::User,
            WaitingKindParam::Intentional => Self::Intentional,
        }
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct CompleteParams {
    /// Watchdog session UUID returned by registration.
    session_id: String,
    /// Idempotency key; reuse only to retry the identical mutation.
    event_key: String,
    /// Terminal result to record.
    outcome: CompletionOutcomeParam,
}

#[derive(Clone, Copy, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum CompletionOutcomeParam {
    Completed,
    Failed,
    Cancelled,
}

impl From<CompletionOutcomeParam> for CompletionOutcome {
    fn from(outcome: CompletionOutcomeParam) -> Self {
        match outcome {
            CompletionOutcomeParam::Completed => Self::Completed,
            CompletionOutcomeParam::Failed => Self::Failed,
            CompletionOutcomeParam::Cancelled => Self::Cancelled,
        }
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DeadlineParams {
    /// Watchdog session UUID returned by registration.
    session_id: String,
    /// Idempotency key; reuse only to retry the identical mutation.
    event_key: String,
    /// Deadline operation to apply.
    action: DeadlineActionParam,
    /// Absolute Unix epoch time in milliseconds; required only for action=set.
    deadline_ms: Option<i64>,
}

#[derive(Clone, Copy, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum DeadlineActionParam {
    Set,
    Pause,
    Resume,
    Clear,
}

impl DeadlineActionParam {
    fn command(self, deadline_ms: Option<i64>) -> Result<DeadlineCommand, rmcp::ErrorData> {
        match self {
            Self::Set => deadline_ms
                .map(|value| DeadlineCommand::Set(WallTimeMs::new(value)))
                .ok_or_else(|| invalid_params("set requires deadline_ms")),
            Self::Pause => Ok(DeadlineCommand::Pause),
            Self::Resume => Ok(DeadlineCommand::Resume),
            Self::Clear => Ok(DeadlineCommand::Clear),
        }
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SessionParams {
    /// Watchdog session UUID returned by registration.
    session_id: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ListSessionsParams {
    /// Normalized states to include; omit or pass [] for all states.
    states: Option<Vec<SessionStateParam>>,
    /// Maximum results, from 1 through 1000; defaults to 1000.
    limit: Option<u32>,
}

#[derive(Clone, Copy, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SessionStateParam {
    Starting,
    Running,
    WaitingForAgent,
    WaitingForTool,
    WaitingForUser,
    Idle,
    Stalled,
    Completed,
    Failed,
    Cancelled,
    Disappeared,
    Unknown,
}

impl From<SessionStateParam> for DetailedState {
    fn from(state: SessionStateParam) -> Self {
        match state {
            SessionStateParam::Starting => Self::Starting,
            SessionStateParam::Running => Self::Running,
            SessionStateParam::WaitingForAgent => Self::WaitingForAgent,
            SessionStateParam::WaitingForTool => Self::WaitingForTool,
            SessionStateParam::WaitingForUser => Self::WaitingForUser,
            SessionStateParam::Idle => Self::Idle,
            SessionStateParam::Stalled => Self::Stalled,
            SessionStateParam::Completed => Self::Completed,
            SessionStateParam::Failed => Self::Failed,
            SessionStateParam::Cancelled => Self::Cancelled,
            SessionStateParam::Disappeared => Self::Disappeared,
            SessionStateParam::Unknown => Self::Unknown,
        }
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ListEventsParams {
    /// Previously processed `next_cursor` to acknowledge; omit to resume the durable cursor.
    after: Option<u64>,
    /// Maximum events, from 1 through 500; defaults to 100.
    limit: Option<u32>,
}

fn transport_key(context: &RequestContext<RoleServer>) -> Result<TransportKey, rmcp::ErrorData> {
    let scope = context
        .extensions
        .get::<TransportScope>()
        .ok_or_else(|| invalid_params("MCP transport scope is unavailable"))?;
    TransportKey::new(scope.0.to_string())
        .map_err(|_| invalid_params("MCP transport ID is invalid"))
}

fn parse_session_id(value: &str) -> Result<SessionId, rmcp::ErrorData> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| invalid_params("session_id must be a UUID"))
}

fn structured_json_result<T>(result: Result<T, AgentApiError>) -> Result<Json<T>, rmcp::ErrorData> {
    result.map(Json).map_err(api_error)
}

fn success<T: Serialize>(value: &T) -> Result<CallToolResult, rmcp::ErrorData> {
    let body = serde_json::to_string(value).map_err(|_| internal_error())?;
    Ok(CallToolResult::success(vec![ContentBlock::text(body)]))
}

fn api_error(error: AgentApiError) -> rmcp::ErrorData {
    match error {
        AgentApiError::Store(_)
        | AgentApiError::Coordinator(_)
        | AgentApiError::ReducerSnapshotUnavailable
        | AgentApiError::TransportBindingCapacityExhausted
        | AgentApiError::WatchPathUnavailable => internal_error(),
        other => invalid_params(other.to_string()),
    }
}

fn invalid_params(message: impl Into<String>) -> rmcp::ErrorData {
    rmcp::ErrorData::new(ErrorCode::INVALID_PARAMS, message.into(), None)
}

fn internal_error() -> rmcp::ErrorData {
    rmcp::ErrorData::new(
        ErrorCode::INTERNAL_ERROR,
        "Agent Watchdog internal persistence failure".to_owned(),
        None,
    )
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rmcp::model::ErrorCode;
    use rmcp::transport::streamable_http_server::session::{
        SessionId as McpSessionId, SessionManager,
    };
    use tokio::sync::Barrier;
    use watchdog_domain::{DeadlineCommand, WallTimeMs};

    use super::{
        AgentApiError, Body, CAPACITY_MARKER, CAPACITY_RETRY_AFTER_SECONDS, DeadlineActionParam,
        IntoResponse as _, McpLimits, McpLimitsError, RestoreOutcome, StatusCode,
        WatchdogSessionManager, WatchdogSessionManagerError, api_error, header,
        retryable_capacity_response,
    };

    #[test]
    fn limits_reject_bounds_that_would_admit_retain_or_read_nothing() {
        assert_eq!(
            McpLimits::new(
                0,
                Duration::from_hours(1),
                McpLimits::DEFAULT_REQUEST_BODY_TIMEOUT
            ),
            Err(McpLimitsError::EmptyCapacity)
        );
        assert_eq!(
            McpLimits::new(1, Duration::ZERO, McpLimits::DEFAULT_REQUEST_BODY_TIMEOUT),
            Err(McpLimitsError::ZeroIdleTtl)
        );
        assert_eq!(
            McpLimits::new(1, Duration::from_hours(1), Duration::ZERO),
            Err(McpLimitsError::ZeroRequestBodyTimeout)
        );
        let defaults = McpLimits::default();
        assert_eq!(defaults.max_sessions(), 64);
        assert_eq!(defaults.idle_ttl(), Duration::from_hours(48));
        assert_eq!(defaults.request_body_timeout(), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn capacity_exhaustion_is_answered_as_retryable_and_other_failures_are_untouched() {
        // The rewrite keys off the error's own Display text, so a rename that
        // did not update the marker would silently stop mapping to 503.
        assert!(
            WatchdogSessionManagerError::Capacity
                .to_string()
                .contains(CAPACITY_MARKER)
        );

        let exhausted = retryable_capacity_response(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Body::from(format!(
                    "Encounter an error when create session: {CAPACITY_MARKER}"
                )),
            )
                .into_response(),
        )
        .await;
        assert_eq!(exhausted.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            exhausted
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some(CAPACITY_RETRY_AFTER_SECONDS)
        );

        let defect = retryable_capacity_response(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Body::from("Encounter an error when create session: disk failure"),
            )
                .into_response(),
        )
        .await;
        assert_eq!(
            defect.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "a genuine server defect must stay a 500"
        );

        let success = retryable_capacity_response(StatusCode::OK.into_response()).await;
        assert_eq!(success.status(), StatusCode::OK);
    }

    #[test]
    fn unavailable_server_state_is_not_reported_as_invalid_caller_input() {
        for error in [
            AgentApiError::ReducerSnapshotUnavailable,
            AgentApiError::WatchPathUnavailable,
        ] {
            assert_eq!(api_error(error).code, ErrorCode::INTERNAL_ERROR);
        }
    }

    #[test]
    fn deadline_tool_actions_parse_to_their_domain_commands() {
        assert!(matches!(
            DeadlineActionParam::Set.command(Some(42)),
            Ok(DeadlineCommand::Set(value)) if value == WallTimeMs::new(42)
        ));
        assert!(matches!(
            DeadlineActionParam::Pause.command(None),
            Ok(DeadlineCommand::Pause)
        ));
        assert!(matches!(
            DeadlineActionParam::Resume.command(None),
            Ok(DeadlineCommand::Resume)
        ));
        assert!(matches!(
            DeadlineActionParam::Clear.command(None),
            Ok(DeadlineCommand::Clear)
        ));
        assert!(DeadlineActionParam::Set.command(None).is_err());
    }

    #[test]
    fn mcp_session_idle_ttl_is_long_and_finite() {
        let manager = WatchdogSessionManager::without_application_scope(McpLimits::default());

        assert_eq!(
            manager.inner.session_config.keep_alive,
            Some(Duration::from_hours(48))
        );
    }

    type Admission = Result<
        (
            McpSessionId,
            <WatchdogSessionManager as SessionManager>::Transport,
        ),
        WatchdogSessionManagerError,
    >;

    fn spawn_concurrent_admissions(
        manager: &Arc<WatchdogSessionManager>,
    ) -> (Arc<Barrier>, Vec<tokio::task::JoinHandle<Admission>>) {
        let attempts = McpLimits::DEFAULT_MAX_SESSIONS * 2;
        let barrier = Arc::new(Barrier::new(attempts + 1));
        let mut tasks = Vec::with_capacity(attempts);
        for index in 0..attempts {
            let manager = Arc::clone(manager);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                let id: McpSessionId = format!("concurrent-restore-{index}").into();
                barrier.wait().await;
                if index % 2 == 0 {
                    match manager.restore_session(id.clone()).await {
                        Ok(RestoreOutcome::Restored(transport)) => Ok((id, transport)),
                        Ok(RestoreOutcome::AlreadyPresent) => {
                            panic!("unique restore id was already present")
                        }
                        Ok(_) => panic!("unexpected restore outcome"),
                        Err(error) => Err(error),
                    }
                } else {
                    manager.create_session().await
                }
            }));
        }
        (barrier, tasks)
    }

    async fn collect_admissions(
        tasks: Vec<tokio::task::JoinHandle<Admission>>,
    ) -> Vec<(
        McpSessionId,
        <WatchdogSessionManager as SessionManager>::Transport,
    )> {
        let attempts = tasks.len();
        let mut admitted = Vec::with_capacity(attempts);
        for task in tasks {
            match task.await.expect("admission task should not panic") {
                Ok(session) => admitted.push(session),
                Err(error) => panic!("eviction should admit every caller, got: {error}"),
            }
        }
        assert_eq!(
            admitted.len(),
            attempts,
            "eviction admits every caller instead of refusing at the cap"
        );
        admitted
    }

    async fn close_admitted(
        manager: &WatchdogSessionManager,
        admitted: Vec<(
            McpSessionId,
            <WatchdogSessionManager as SessionManager>::Transport,
        )>,
    ) {
        for (session, _transport) in admitted {
            manager
                .close_session(&session)
                .await
                .expect("admitted session should close");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mcp_session_admission_is_atomic_under_concurrent_create_and_restore() {
        let manager = Arc::new(WatchdogSessionManager::without_application_scope(
            McpLimits::default(),
        ));

        let creation = manager.creation.lock().await;
        let (barrier, tasks) = spawn_concurrent_admissions(&manager);
        barrier.wait().await;
        tokio::task::yield_now().await;
        assert!(
            manager.inner.sessions.read().await.is_empty(),
            "scheduled creation tasks must wait behind the admission mutex"
        );
        drop(creation);
        let admitted = collect_admissions(tasks).await;
        // Serialized admission is what keeps the map at the cap: any interleaving
        // that skipped the mutex would leave more live sessions than capacity.
        let occupancy = manager.occupancy();
        assert_eq!(occupancy.admitted, McpLimits::DEFAULT_MAX_SESSIONS);
        assert_eq!(
            manager.inner.sessions.read().await.len(),
            McpLimits::DEFAULT_MAX_SESSIONS
        );
        assert_eq!(
            occupancy.evicted,
            (admitted.len() - McpLimits::DEFAULT_MAX_SESSIONS) as u64
        );
        close_admitted(&manager, admitted).await;
    }

    #[tokio::test(start_paused = true)]
    async fn mcp_session_admission_evicts_the_longest_idle_session_at_capacity() {
        const CAPACITY: usize = 4;

        let limits = McpLimits::new(
            CAPACITY,
            McpLimits::DEFAULT_IDLE_TTL,
            McpLimits::DEFAULT_REQUEST_BODY_TIMEOUT,
        )
        .expect("test limits should validate");
        let manager = WatchdogSessionManager::without_application_scope(limits);
        let mut sessions = Vec::with_capacity(CAPACITY);
        for _ in 0..CAPACITY {
            sessions.push(
                manager
                    .create_session()
                    .await
                    .expect("session within capacity should be admitted"),
            );
            // Separate each admission so "longest idle" is unambiguous.
            tokio::time::advance(Duration::from_millis(1)).await;
        }
        assert_eq!(manager.occupancy().admitted, CAPACITY);

        let (replacement, _transport) = manager
            .create_session()
            .await
            .expect("admission at capacity should evict rather than refuse");
        let occupancy = manager.occupancy();
        assert_eq!(occupancy.admitted, CAPACITY, "the cap still holds");
        assert_eq!(occupancy.capacity, CAPACITY);
        assert_eq!(occupancy.evicted, 1);
        assert!(
            !manager
                .has_session(&sessions[0].0)
                .await
                .expect("session lookup should succeed"),
            "the longest-idle session is the one evicted"
        );
        for (session, _transport) in &sessions[1..] {
            assert!(
                manager
                    .has_session(session)
                    .await
                    .expect("session lookup should succeed"),
                "more recently active sessions must survive"
            );
        }
        assert!(matches!(
            manager.restore_session(sessions[1].0.clone()).await,
            Ok(RestoreOutcome::AlreadyPresent)
        ));
        assert_eq!(
            manager.occupancy().evicted,
            1,
            "restoring a session already present must not evict"
        );
        manager
            .close_session(&replacement)
            .await
            .expect("replacement should close");

        for (session, _transport) in sessions {
            manager
                .close_session(&session)
                .await
                .expect("explicit close should release capacity");
        }
        let (session, _transport) = manager
            .create_session()
            .await
            .expect("capacity should be reusable after close");
        manager
            .close_session(&session)
            .await
            .expect("replacement session should close");
    }
}
