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
    RoleServer, ServerHandler,
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
    AgentApi, AgentApiError, BearerAuthenticator, CompletionOutcome, RegisterSession, TransportKey,
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
        description = "Register or enrich a session and bind a main session to this MCP transport"
    )]
    async fn register_session(
        &self,
        Parameters(params): Parameters<RegisterSessionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        let runtime = parse_runtime(&params.runtime)?;
        let kind = parse_kind(&params.kind)?;
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
                        runtime,
                        native_id: params.native_id,
                        kind,
                        parent,
                        event_key: params.event_key,
                    },
                )
                .await,
        )
    }

    #[tool(description = "Record an exact parent-child delegation and optional expected check-in")]
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
        description = "Register one additional allowlisted filesystem path for an in-scope session"
    )]
    async fn register_watch_path(
        &self,
        Parameters(params): Parameters<RegisterWatchPathParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        json_result(
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

    #[tool(description = "Report bounded event-driven progress for one in-scope session")]
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

    #[tool(description = "Report waiting for an agent, tool, user, or intentional pause")]
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
                    parse_waiting(&params.waiting_for)?,
                )
                .await,
        )
    }

    #[tool(description = "Report a successful, failed, or cancelled terminal outcome")]
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
                    parse_outcome(&params.outcome)?,
                )
                .await,
        )
    }

    #[tool(description = "Set, pause, resume, or clear a session check-in deadline")]
    async fn update_deadline(
        &self,
        Parameters(params): Parameters<DeadlineParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        let command = parse_deadline(&params.action, params.deadline_ms)?;
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

    #[tool(description = "Read one normalized in-scope session with agent diagnostics")]
    async fn get_session(
        &self,
        Parameters(params): Parameters<SessionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        json_result(
            self.api
                .get_session(&transport, parse_session_id(&params.session_id)?)
                .await,
        )
    }

    #[tool(description = "List sessions in the bound tree with optional normalized-state filters")]
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
            .map(|value| parse_state(&value))
            .collect::<Result<Vec<_>, _>>()?;
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
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        json_result(self.api.session_tree(&transport).await)
    }

    #[tool(description = "Read durable parent events after a caller-confirmed cursor")]
    async fn list_events(
        &self,
        Parameters(params): Parameters<ListEventsParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        json_result(
            self.api
                .list_events(
                    &transport,
                    params.after,
                    params.limit.unwrap_or(DEFAULT_EVENT_PAGE_SIZE),
                )
                .await,
        )
    }

    #[tool(description = "Read Watchdog storage and runtime-adapter health warnings")]
    async fn get_watchdog_health(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let transport = transport_key(&context)?;
        json_result(self.api.health(&transport).await)
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
            "Register or bind the coordinating main session first. Durable list_events delivery remains authoritative."
                .to_owned(),
        );
        info
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RegisterSessionParams {
    runtime: String,
    native_id: String,
    kind: String,
    parent_session_id: Option<String>,
    event_key: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RegisterDelegationParams {
    parent_session_id: String,
    child_session_id: String,
    event_key: String,
    deadline_ms: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RegisterWatchPathParams {
    session_id: String,
    event_key: String,
    path: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ProgressParams {
    session_id: String,
    event_key: String,
    summary: String,
    operation: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct WaitingParams {
    session_id: String,
    event_key: String,
    waiting_for: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct CompleteParams {
    session_id: String,
    event_key: String,
    outcome: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DeadlineParams {
    session_id: String,
    event_key: String,
    action: String,
    deadline_ms: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SessionParams {
    session_id: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ListSessionsParams {
    states: Option<Vec<String>>,
    limit: Option<u32>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ListEventsParams {
    after: Option<u64>,
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

fn parse_runtime(value: &str) -> Result<RuntimeKind, rmcp::ErrorData> {
    match value {
        "claude_code" => Ok(RuntimeKind::ClaudeCode),
        "codex_cli" => Ok(RuntimeKind::CodexCli),
        "codex_companion" => Ok(RuntimeKind::CodexCompanion),
        _ => Err(invalid_params("runtime is unsupported in v1")),
    }
}

fn parse_kind(value: &str) -> Result<SessionKind, rmcp::ErrorData> {
    match value {
        "main" => Ok(SessionKind::Main),
        "child" => Ok(SessionKind::Child),
        _ => Err(invalid_params("kind must be main or child")),
    }
}

fn parse_waiting(value: &str) -> Result<WaitingKind, rmcp::ErrorData> {
    match value {
        "agent" => Ok(WaitingKind::Agent),
        "tool" => Ok(WaitingKind::Tool),
        "user" => Ok(WaitingKind::User),
        "intentional" => Ok(WaitingKind::Intentional),
        _ => Err(invalid_params(
            "waiting_for must be agent, tool, user, or intentional",
        )),
    }
}

fn parse_outcome(value: &str) -> Result<CompletionOutcome, rmcp::ErrorData> {
    match value {
        "completed" => Ok(CompletionOutcome::Completed),
        "failed" => Ok(CompletionOutcome::Failed),
        "cancelled" => Ok(CompletionOutcome::Cancelled),
        _ => Err(invalid_params(
            "outcome must be completed, failed, or cancelled",
        )),
    }
}

fn parse_deadline(
    action: &str,
    deadline_ms: Option<i64>,
) -> Result<DeadlineCommand, rmcp::ErrorData> {
    match action {
        "set" => deadline_ms
            .map(|value| DeadlineCommand::Set(WallTimeMs::new(value)))
            .ok_or_else(|| invalid_params("set requires deadline_ms")),
        "pause" => Ok(DeadlineCommand::Pause),
        "resume" => Ok(DeadlineCommand::Resume),
        "clear" => Ok(DeadlineCommand::Clear),
        _ => Err(invalid_params(
            "action must be set, pause, resume, or clear",
        )),
    }
}

fn parse_state(value: &str) -> Result<DetailedState, rmcp::ErrorData> {
    match value {
        "starting" => Ok(DetailedState::Starting),
        "running" => Ok(DetailedState::Running),
        "waiting_for_agent" => Ok(DetailedState::WaitingForAgent),
        "waiting_for_tool" => Ok(DetailedState::WaitingForTool),
        "waiting_for_user" => Ok(DetailedState::WaitingForUser),
        "idle" => Ok(DetailedState::Idle),
        "stalled" => Ok(DetailedState::Stalled),
        "completed" => Ok(DetailedState::Completed),
        "failed" => Ok(DetailedState::Failed),
        "cancelled" => Ok(DetailedState::Cancelled),
        "disappeared" => Ok(DetailedState::Disappeared),
        "unknown" => Ok(DetailedState::Unknown),
        _ => Err(invalid_params("states contains an unsupported value")),
    }
}

fn json_result<T: Serialize>(
    result: Result<T, AgentApiError>,
) -> Result<CallToolResult, rmcp::ErrorData> {
    success(&result.map_err(api_error)?)
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

    use super::{AgentApiError, api_error, parse_deadline};

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
            parse_deadline("set", Some(42)),
            Ok(DeadlineCommand::Set(value)) if value == WallTimeMs::new(42)
        ));
        assert!(matches!(
            parse_deadline("pause", None),
            Ok(DeadlineCommand::Pause)
        ));
        assert!(matches!(
            parse_deadline("resume", None),
            Ok(DeadlineCommand::Resume)
        ));
        assert!(matches!(
            parse_deadline("clear", None),
            Ok(DeadlineCommand::Clear)
        ));
        assert!(parse_deadline("set", None).is_err());
    }
}
