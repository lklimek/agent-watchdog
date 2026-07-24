use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::Request,
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
use watchdog_domain::{
    DeadlineCommand, DetailedState, RuntimeKind, SessionId, SessionKind, WallTimeMs,
};

use crate::{
    AgentApi, AgentApiError, AgentHealthView, BearerAuthenticator, CompletionOutcome, EventPage,
    RegisterSession, RegisteredWatchPathView, SessionTreeView, SessionView, TransportKey,
    WaitingKind, agent_api::MAX_TREE_SESSIONS,
};

const DEFAULT_EVENT_PAGE_SIZE: u32 = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransportScope(McpSessionId);

/// rmcp session manager that injects the opaque transport identity into every
/// request context for application-level main-session scoping.
#[derive(Debug, Default)]
pub struct WatchdogSessionManager {
    inner: LocalSessionManager,
}

impl WatchdogSessionManager {
    fn scoped(id: &McpSessionId, mut message: ClientJsonRpcMessage) -> ClientJsonRpcMessage {
        message.insert_extension(TransportScope(id.clone()));
        message
    }
}

impl SessionManager for WatchdogSessionManager {
    type Error = LocalSessionManagerError;
    type Transport = <LocalSessionManager as SessionManager>::Transport;

    async fn create_session(&self) -> Result<(McpSessionId, Self::Transport), Self::Error> {
        self.inner.create_session().await
    }

    async fn initialize_session(
        &self,
        id: &McpSessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        self.inner
            .initialize_session(id, Self::scoped(id, message))
            .await
    }

    async fn has_session(&self, id: &McpSessionId) -> Result<bool, Self::Error> {
        self.inner.has_session(id).await
    }

    async fn close_session(&self, id: &McpSessionId) -> Result<(), Self::Error> {
        self.inner.close_session(id).await
    }

    async fn create_stream(
        &self,
        id: &McpSessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner
            .create_stream(id, Self::scoped(id, message))
            .await
    }

    async fn accept_message(
        &self,
        id: &McpSessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        self.inner
            .accept_message(id, Self::scoped(id, message))
            .await
    }

    async fn create_standalone_stream(
        &self,
        id: &McpSessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner.create_standalone_stream(id).await
    }

    async fn resume(
        &self,
        id: &McpSessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner.resume(id, last_event_id).await
    }

    async fn restore_session(
        &self,
        id: McpSessionId,
    ) -> Result<RestoreOutcome<Self::Transport>, Self::Error> {
        self.inner.restore_session(id).await
    }
}

/// Create the stateful Streamable HTTP MCP service.
#[must_use]
fn mcp_http_service(
    api: AgentApi,
) -> StreamableHttpService<WatchdogMcpService, WatchdogSessionManager> {
    let manager = Arc::new(WatchdogSessionManager::default());
    StreamableHttpService::new(
        move || Ok(WatchdogMcpService::new(api.clone())),
        manager,
        StreamableHttpServerConfig::default(),
    )
}

/// Build the `/mcp` Streamable HTTP route behind strict shared-token auth.
///
/// Authentication runs before rmcp parses or allocates protocol state. Failure
/// responses are deliberately fixed and never reflect credential input.
pub fn mcp_router(api: AgentApi, authenticator: BearerAuthenticator) -> Router {
    let service = mcp_http_service(api);
    Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn(
            move |request: Request<Body>, next: Next| {
                let authenticator = authenticator.clone();
                async move {
                    let authorized = request
                        .headers()
                        .get(axum::http::header::AUTHORIZATION)
                        .is_some_and(|value| authenticator.authorize(Some(value.as_bytes())));
                    if !authorized {
                        tracing::warn!(
                            event = "auth.rejected",
                            route = "/mcp",
                            "MCP bearer credential rejected"
                        );
                        return axum::http::StatusCode::UNAUTHORIZED.into_response();
                    }
                    let response: Response = next.run(request).await;
                    response
                }
            },
        ))
}

/// MCP protocol façade over the scoped durable agent API.
#[derive(Clone, Debug)]
pub struct WatchdogMcpService {
    api: AgentApi,
}

#[tool_router]
impl WatchdogMcpService {
    /// Construct one per-transport protocol handler over shared application state.
    #[must_use]
    pub fn new(api: AgentApi) -> Self {
        Self { api }
    }

    #[tool(
        description = "Register or enrich a session. Register kind=main first to bind this transport to one immutable session tree; kind=child requires an in-tree parent. Supported runtimes: claude_code, codex_cli, codex_companion"
    )]
    async fn register_session(
        &self,
        Parameters(params): Parameters<RegisterSessionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        let kind = params.kind.into();
        let parent = params
            .parent_session_id
            .as_deref()
            .map(parse_session_id)
            .transpose()?;
        json_result(
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
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        let deadline = params
            .deadline_ms
            .map(|value| DeadlineCommand::Set(WallTimeMs::new(value)));
        json_result(
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
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        json_result(
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
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        json_result(
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
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        json_result(
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
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        let command = params.action.command(params.deadline_ms)?;
        json_result(
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
            "Register the coordinating main session first to bind this transport to one tree. For every mutation, generate a fresh event_key and reuse it only when retrying the identical mutation. Treat list_events as a durable inbox: process the returned events, then pass that page's next_cursor as after to acknowledge them."
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

fn json_result<T: Serialize>(
    result: Result<T, AgentApiError>,
) -> Result<CallToolResult, rmcp::ErrorData> {
    success(&result.map_err(api_error)?)
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
    use rmcp::model::ErrorCode;
    use watchdog_domain::{DeadlineCommand, WallTimeMs};

    use super::{AgentApiError, DeadlineActionParam, api_error};

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
}
