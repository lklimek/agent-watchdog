//! rmcp transport identity, tool surface, and scope integration tests.

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use futures::StreamExt;
use rmcp::{
    ServiceExt,
    model::ClientJsonRpcMessage,
    transport::streamable_http_server::{
        session::{SessionId as McpSessionId, SessionManager},
        tower::{StreamableHttpServerConfig, StreamableHttpService},
    },
};
use serde_json::{Value, json};
use tower::ServiceExt as _;
use watchdog_domain::{NativeSessionKey, RuntimeKind, SessionId, TimePoint, WallTimeMs};
use watchdog_server::{
    AgentApi, AgentApiError, BearerAuthenticator, McpLimits, TransportKey, WatchdogMcpService,
    WatchdogSessionManager, mcp_router,
};
use watchdog_store::WatchdogStore;
use watchdog_testkit::FakeClock;

fn message(value: Value) -> ClientJsonRpcMessage {
    serde_json::from_value(value).expect("JSON-RPC fixture should deserialize")
}

fn initialize_request() -> ClientJsonRpcMessage {
    message(json!({
        "jsonrpc":"2.0",
        "id":1,
        "method":"initialize",
        "params":{
            "protocolVersion":"2025-03-26",
            "capabilities":{},
            "clientInfo":{"name":"watchdog-test","version":"0.0.0"}
        }
    }))
}

async fn response(
    manager: &WatchdogSessionManager,
    session: &rmcp::transport::streamable_http_server::session::SessionId,
    request: ClientJsonRpcMessage,
) -> Value {
    let mut stream = manager
        .create_stream(session, request)
        .await
        .expect("request stream should open");
    while let Some(event) = stream.next().await {
        if let Some(message) = event.message {
            return serde_json::to_value(message.as_ref())
                .expect("server message should serialize");
        }
    }
    panic!("request stream ended without a JSON-RPC response")
}

fn http_json_rpc_body(body: &[u8]) -> Value {
    if let Ok(value) = serde_json::from_slice(body) {
        return value;
    }
    let text = std::str::from_utf8(body).expect("MCP response body should be UTF-8");
    text.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .find_map(|data| serde_json::from_str(data).ok())
        .unwrap_or_else(|| panic!("MCP response body should contain JSON-RPC data: {text}"))
}

async fn test_api() -> AgentApi {
    let directory = tempfile::tempdir().expect("temporary directory should exist");
    let path = directory.path().join("watchdog.db");
    let _retained = directory.keep();
    let store = WatchdogStore::open(&path)
        .await
        .expect("database should open");
    AgentApi::new(
        store,
        Arc::new(FakeClock::new(TimePoint::new(
            WallTimeMs::new(10_000),
            5_000,
        ))),
    )
    .await
    .expect("agent API should initialize")
}

async fn initialize_http_session(router: &Router) -> McpSessionId {
    let response = router
        .clone()
        .oneshot(
            Request::post("/mcp")
                .header("host", "localhost")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    serde_json::to_vec(&initialize_request())
                        .expect("initialize request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("router is infallible");
    assert_eq!(response.status(), StatusCode::OK);
    let session = response
        .headers()
        .get("mcp-session-id")
        .expect("initialize response should carry a session ID")
        .to_str()
        .expect("session header should be text")
        .to_owned()
        .into();
    axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("initialize response should be bounded");
    session
}

async fn register_http_main(router: &Router, session: &McpSessionId) {
    let response = router
        .clone()
        .oneshot(
            Request::post("/mcp")
                .header("host", "localhost")
                .header("mcp-session-id", session.to_string())
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    serde_json::to_vec(&message(json!({
                        "jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                            "name":"register_session","arguments":{
                                "runtime":"claude_code","native_id":"expiring-main","kind":"main",
                                "event_key":"register-expiring-main"
                            }
                        }
                    })))
                    .expect("registration request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("router is infallible");
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("registration response should be bounded");
    assert!(
        http_json_rpc_body(&body).get("error").is_none(),
        "{}",
        String::from_utf8_lossy(&body)
    );
}

async fn register_structured_child(
    manager: &WatchdogSessionManager,
    session: &McpSessionId,
    parent: SessionId,
) -> SessionId {
    let registered = response(
        manager,
        session,
        message(json!({
            "jsonrpc":"2.0","id":4,"method":"tools/call","params":{
                "name":"register_session","arguments":{
                    "runtime":"codex_cli","native_id":"structured-child","kind":"child",
                    "parent_session_id": parent.to_string(),
                    "event_key":"register-structured-child"
                }
            }
        })),
    )
    .await;
    assert!(registered.get("error").is_none(), "{registered}");
    SessionId::from_native(
        &NativeSessionKey::new(RuntimeKind::CodexCli, "structured-child")
            .expect("native ID should be valid"),
    )
}

async fn register_main_named(router: &Router, session: &McpSessionId, native_id: &str) {
    let response = post_mcp(
        router,
        None,
        Some(session),
        &message(json!({
            "jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                "name":"register_session","arguments":{
                    "runtime":"claude_code","native_id":native_id,"kind":"main",
                    "event_key":format!("register-{native_id}")
                }
            }
        })),
    )
    .await;
    assert!(response.get("error").is_none(), "{response}");
}

async fn read_session_tree(router: &Router, session: &McpSessionId) {
    let response = post_mcp(
        router,
        None,
        Some(session),
        &message(json!({
            "jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                "name":"get_session_tree","arguments":{}
            }
        })),
    )
    .await;
    assert!(response.get("error").is_none(), "{response}");
}

async fn initialize_authenticated_session(router: &Router, token: &str) -> McpSessionId {
    let response = mcp_request(router, Some(token), None, &initialize_request())
        .await
        .expect("router is infallible");
    assert_eq!(response.status(), StatusCode::OK);
    let session = response
        .headers()
        .get("mcp-session-id")
        .expect("initialize response should carry a session ID")
        .to_str()
        .expect("session header should be text")
        .to_owned()
        .into();
    axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("initialize response should be bounded");
    session
}

async fn authenticated_tool_call(
    router: &Router,
    token: &str,
    session: &McpSessionId,
    id: u32,
    tool: &str,
    arguments: Value,
) -> Value {
    post_mcp(
        router,
        Some(token),
        Some(session),
        &message(json!({
            "jsonrpc":"2.0","id":id,"method":"tools/call","params":{
                "name":tool,"arguments":arguments
            }
        })),
    )
    .await
}

async fn post_mcp(
    router: &Router,
    token: Option<&str>,
    session: Option<&McpSessionId>,
    request: &ClientJsonRpcMessage,
) -> Value {
    let response = mcp_request(router, token, session, request)
        .await
        .expect("router is infallible");
    let body = axum::body::to_bytes(response.into_body(), 256 * 1024)
        .await
        .expect("MCP response should be bounded");
    http_json_rpc_body(&body)
}

async fn mcp_request(
    router: &Router,
    token: Option<&str>,
    session: Option<&McpSessionId>,
    request: &ClientJsonRpcMessage,
) -> Result<axum::response::Response, std::convert::Infallible> {
    let mut builder = Request::post("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(session) = session {
        builder = builder.header("mcp-session-id", session.to_string());
    }
    router
        .clone()
        .oneshot(
            builder
                .body(Body::from(
                    serde_json::to_vec(request).expect("MCP request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
}

fn listed_tool<'a>(listed: &'a Value, name: &str) -> &'a Value {
    listed["result"]["tools"]
        .as_array()
        .expect("tools should be an array")
        .iter()
        .find(|tool| tool["name"] == name)
        .expect("expected tool should be listed")
}

fn assert_register_session_contract(listed: &Value) {
    let register_session = listed_tool(listed, "register_session");
    let advertised_runtimes =
        register_session["inputSchema"]["$defs"]["RegisterSessionRuntime"]["enum"]
            .as_array()
            .expect("register_session runtime should be an enum");
    assert_eq!(
        advertised_runtimes,
        &["claude_code", "codex_cli", "codex_companion"],
        "only supported v1 runtimes should be advertised"
    );

    let event_key_description =
        register_session["inputSchema"]["properties"]["event_key"]["description"]
            .as_str()
            .unwrap_or_default();
    let event_key_description_lower = event_key_description.to_ascii_lowercase();
    assert!(
        event_key_description_lower.contains("idempotency")
            && event_key_description_lower.contains("identical mutation"),
        "event_key retry semantics should be discoverable: {event_key_description}"
    );
    let parent_description =
        register_session["inputSchema"]["properties"]["parent_session_id"]["description"]
            .as_str()
            .unwrap_or_default();
    assert!(
        parent_description.contains("Required when kind is child"),
        "conditional parent requirement should be discoverable: {parent_description}"
    );
}

fn assert_finite_inputs_are_enums(listed: &Value) {
    for (tool_name, definition, expected) in [
        (
            "register_session",
            "SessionKindParam",
            &["main", "child"][..],
        ),
        (
            "report_waiting",
            "WaitingKindParam",
            &["agent", "tool", "user", "intentional"][..],
        ),
        (
            "complete_session",
            "CompletionOutcomeParam",
            &["completed", "failed", "cancelled"][..],
        ),
        (
            "update_deadline",
            "DeadlineActionParam",
            &["set", "pause", "resume", "clear"][..],
        ),
        (
            "list_sessions",
            "SessionStateParam",
            &[
                "starting",
                "running",
                "waiting_for_agent",
                "waiting_for_tool",
                "waiting_for_user",
                "idle",
                "stalled",
                "completed",
                "failed",
                "cancelled",
                "disappeared",
                "unknown",
            ][..],
        ),
    ] {
        let tool = listed_tool(listed, tool_name);
        assert_eq!(
            tool["inputSchema"]["$defs"][definition]["enum"]
                .as_array()
                .expect("finite input should be an enum"),
            expected,
            "{tool_name} should advertise all accepted {definition} values"
        );
    }
}

fn assert_event_cursor_contract(listed: &Value) {
    let list_events = listed_tool(listed, "list_events");
    let after_description = list_events["inputSchema"]["properties"]["after"]["description"]
        .as_str()
        .unwrap_or_default();
    assert!(
        after_description.contains("acknowledge") && after_description.contains("next_cursor"),
        "durable cursor semantics should be discoverable: {after_description}"
    );
}

fn assert_structured_result_matches_schema(listed: &Value, tool_name: &str, called: &Value) {
    let schema = &listed_tool(listed, tool_name)["outputSchema"];
    assert!(
        schema.is_object(),
        "{tool_name} should advertise an output schema: {schema}"
    );
    let structured = &called["result"]["structuredContent"];
    assert!(
        structured.is_object(),
        "{tool_name} should return structured content: {called}"
    );
    let text = called["result"]["content"]
        .as_array()
        .and_then(|content| content.first())
        .and_then(|content| content["text"].as_str())
        .expect("legacy text content should remain available");
    let legacy: Value = serde_json::from_str(text).expect("legacy text content should remain JSON");
    assert_eq!(
        &legacy, structured,
        "{tool_name} text and structured content should agree"
    );
    if let Err(error) = jsonschema::draft202012::validate(schema, structured) {
        panic!("{tool_name} structured content should match output schema: {error}");
    }
}

#[tokio::test]
async fn http_mcp_route_requires_the_configured_bearer_token() {
    let router = mcp_router(
        test_api().await,
        BearerAuthenticator::new("correct-secret").expect("token should be valid"),
        McpLimits::default(),
    );
    for authorization in [None, Some("Bearer wrong-secret")] {
        let mut request = Request::post("/mcp")
            .header("host", "localhost")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .body(Body::from(
                serde_json::to_vec(&initialize_request())
                    .expect("initialize request should serialize"),
            ))
            .expect("request should build");
        if let Some(value) = authorization {
            request
                .headers_mut()
                .insert("authorization", value.parse().expect("header should parse"));
        }
        let response = router
            .clone()
            .oneshot(request)
            .await
            .expect("router is infallible");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    let response = router
        .clone()
        .oneshot(
            Request::post("/mcp")
                .header("host", "localhost")
                .header("authorization", "Bearer correct-secret")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    serde_json::to_vec(&initialize_request())
                        .expect("initialize request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("router is infallible");
    let status = response.status();
    let session_id = response
        .headers()
        .get("mcp-session-id")
        .expect("initialize response should carry a session ID")
        .clone();
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("response body should be bounded");
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));

    let listed_response = router
        .oneshot(
            Request::post("/mcp")
                .header("host", "localhost")
                .header("authorization", "Bearer correct-secret")
                .header("mcp-session-id", session_id)
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    serde_json::to_vec(&message(
                        json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
                    ))
                    .expect("tools/list request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("router is infallible");
    let listed_status = listed_response.status();
    let listed_body = axum::body::to_bytes(listed_response.into_body(), 256 * 1024)
        .await
        .expect("tools/list response should be bounded");
    assert_eq!(
        listed_status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&listed_body)
    );
    let listed = http_json_rpc_body(&listed_body);
    for expected in [
        "register_session",
        "register_delegation",
        "get_session",
        "get_session_tree",
        "list_events",
        "get_watchdog_health",
    ] {
        listed_tool(&listed, expected);
    }
}

#[tokio::test]
async fn mcp_initialize_instructions_distinguish_coordinator_and_child_registration() {
    let router = mcp_router(
        test_api().await,
        BearerAuthenticator::new("instruction-secret").expect("token should be valid"),
        McpLimits::default(),
    );
    let response = mcp_request(
        &router,
        Some("instruction-secret"),
        None,
        &initialize_request(),
    )
    .await
    .expect("router is infallible");
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("initialize response should be bounded");
    let initialized = http_json_rpc_body(&body);
    let instructions = initialized["result"]["instructions"]
        .as_str()
        .expect("initialize response should include instructions");

    for required in [
        "coordinator",
        "kind=main",
        "child",
        "kind=child",
        "parent_session_id",
        "reconnect",
    ] {
        assert!(
            instructions.contains(required),
            "server instructions should explain role-aware registration using `{required}`: {instructions}"
        );
    }
}

#[tokio::test]
async fn mcp_binding_survives_transport_idle_periods() {
    let api = test_api().await;
    let manager = Arc::new(WatchdogSessionManager::new(
        api.clone(),
        McpLimits::default(),
    ));
    let (session, transport) = manager
        .create_session()
        .await
        .expect("rmcp session should be created");
    let server = WatchdogMcpService::new(api);
    let service = tokio::spawn(async move { server.serve(transport).await });
    manager
        .initialize_session(&session, initialize_request())
        .await
        .expect("session should initialize");

    let registered = response(
        &manager,
        &session,
        message(json!({
            "jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                "name":"register_session","arguments":{
                    "runtime":"claude_code","native_id":"idle-main","kind":"main",
                    "event_key":"register-idle-main"
                }
            }
        })),
    )
    .await;
    assert!(registered.get("error").is_none(), "{registered}");

    tokio::time::pause();
    tokio::time::advance(Duration::from_mins(40)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();

    let tree = response(
        &manager,
        &session,
        message(json!({
            "jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                "name":"get_session_tree","arguments":{}
            }
        })),
    )
    .await;
    assert!(tree.get("error").is_none(), "{tree}");

    manager
        .close_session(&session)
        .await
        .expect("session should close");
    let running = service
        .await
        .expect("server task should join")
        .expect("server should initialize");
    running.waiting().await.expect("closed server should stop");
}

#[tokio::test]
async fn mcp_idle_expiry_reclaims_capacity_and_transport_scope() {
    let api = test_api().await;
    let manager = Arc::new(WatchdogSessionManager::new(
        api.clone(),
        McpLimits::default(),
    ));
    let factory_api = api.clone();
    let service = StreamableHttpService::new(
        move || Ok(WatchdogMcpService::new(factory_api.clone())),
        Arc::clone(&manager),
        StreamableHttpServerConfig::default(),
    );
    let router = Router::new().nest_service("/mcp", service);
    let session = initialize_http_session(&router).await;
    register_http_main(&router, &session).await;
    let transport = TransportKey::new(session.to_string()).expect("transport should validate");
    api.session_tree(&transport)
        .await
        .expect("registration should bind the transport");

    let mut fillers = Vec::with_capacity(63);
    for _ in 0..63 {
        fillers.push(
            manager
                .create_session()
                .await
                .expect("filler session should fit within capacity"),
        );
    }
    let full = manager.occupancy();
    assert_eq!(
        full.admitted, full.capacity,
        "the pool should be full before idle expiry"
    );
    assert_eq!(
        full.evicted, 0,
        "a full pool nobody pushed on must not evict anything"
    );

    tokio::time::pause();
    tokio::time::advance(Duration::from_hours(48) + Duration::from_secs(1)).await;
    for _ in 0..32 {
        if !manager
            .has_session(&session)
            .await
            .expect("session lookup should succeed")
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        !manager
            .has_session(&session)
            .await
            .expect("session lookup should succeed"),
        "HTTP worker idle expiry should remove its manager entry"
    );
    assert!(matches!(
        api.session_tree(&transport).await,
        Err(AgentApiError::TransportNotBound)
    ));

    let replacement = manager
        .create_session()
        .await
        .expect("idle expiry should release admission capacity");
    for (session, _transport) in fillers.into_iter().chain([replacement]) {
        manager
            .close_session(&session)
            .await
            .expect("test session should close");
    }
}

#[tokio::test]
async fn mcp_admission_at_capacity_evicts_the_longest_idle_transport() {
    const CAPACITY: usize = 3;

    let api = test_api().await;
    let limits = McpLimits::new(
        CAPACITY,
        Duration::from_hours(48),
        McpLimits::DEFAULT_REQUEST_BODY_TIMEOUT,
    )
    .expect("test limits should validate");
    let manager = Arc::new(WatchdogSessionManager::new(api.clone(), limits));
    let factory_api = api.clone();
    let service = StreamableHttpService::new(
        move || Ok(WatchdogMcpService::new(factory_api.clone())),
        Arc::clone(&manager),
        StreamableHttpServerConfig::default(),
    );
    let router = Router::new().nest_service("/mcp", service);

    // Fill capacity with bound main sessions, oldest first, then re-touch every
    // session except the first so the first is unambiguously longest-idle.
    let mut sessions = Vec::with_capacity(CAPACITY);
    for index in 0..CAPACITY {
        let session = initialize_http_session(&router).await;
        register_main_named(&router, &session, &format!("evictable-main-{index}")).await;
        sessions.push(session);
    }
    tokio::time::pause();
    for session in sessions.iter().skip(1) {
        tokio::time::advance(Duration::from_mins(1)).await;
        read_session_tree(&router, session).await;
    }
    tokio::time::resume();

    let transports = sessions
        .iter()
        .map(|session| TransportKey::new(session.to_string()).expect("transport should validate"))
        .collect::<Vec<_>>();
    for transport in &transports {
        api.session_tree(transport)
            .await
            .expect("every admitted transport should be bound before eviction");
    }

    let admitted = initialize_http_session(&router).await;

    let occupancy = manager.occupancy();
    assert_eq!(occupancy.capacity, CAPACITY);
    assert_eq!(
        occupancy.admitted, CAPACITY,
        "eviction must free exactly one slot, not grow the pool"
    );
    assert_eq!(
        occupancy.evicted, 1,
        "exactly one transport should be evicted"
    );
    assert!(
        !manager
            .has_session(&sessions[0])
            .await
            .expect("session lookup should succeed"),
        "the longest-idle transport should be the evicted one"
    );
    assert!(
        matches!(
            api.session_tree(&transports[0]).await,
            Err(AgentApiError::TransportNotBound)
        ),
        "eviction must release the application scope, matching idle expiry"
    );
    for transport in &transports[1..] {
        api.session_tree(transport)
            .await
            .expect("more recently active transports must keep their scope");
    }
    assert!(
        manager
            .has_session(&admitted)
            .await
            .expect("session lookup should succeed"),
        "the new transport should have been admitted, not refused"
    );
}

#[tokio::test]
async fn mcp_router_publishes_session_occupancy_in_agent_health() {
    let api = test_api().await;
    let router = mcp_router(
        api,
        BearerAuthenticator::new("occupancy-secret").expect("token should be valid"),
        McpLimits::default(),
    );
    let session = initialize_authenticated_session(&router, "occupancy-secret").await;
    let registered = authenticated_tool_call(
        &router,
        "occupancy-secret",
        &session,
        2,
        "register_session",
        json!({
            "runtime":"claude_code","native_id":"occupancy-main","kind":"main",
            "event_key":"register-occupancy-main"
        }),
    )
    .await;
    assert!(registered.get("error").is_none(), "{registered}");

    let health = authenticated_tool_call(
        &router,
        "occupancy-secret",
        &session,
        3,
        "get_watchdog_health",
        json!({}),
    )
    .await;
    let occupancy = &health["result"]["structuredContent"]["mcp_sessions"];
    assert_eq!(
        occupancy["admitted"], 1,
        "production wiring should publish live occupancy: {health}"
    );
    assert_eq!(
        occupancy["capacity"],
        McpLimits::DEFAULT_MAX_SESSIONS,
        "{health}"
    );
    assert_eq!(occupancy["evicted"], 0, "{health}");
}

#[tokio::test]
async fn real_rmcp_transport_exposes_all_tools_and_rejects_cross_tree_target() {
    let api = test_api().await;
    let manager = Arc::new(WatchdogSessionManager::new(
        api.clone(),
        McpLimits::default(),
    ));
    let mut sessions = Vec::new();
    let mut services = Vec::new();
    for _ in 0..2 {
        let (session, transport) = manager
            .create_session()
            .await
            .expect("rmcp session should be created");
        let server = WatchdogMcpService::new(api.clone());
        services.push(tokio::spawn(async move { server.serve(transport).await }));
        manager
            .initialize_session(&session, initialize_request())
            .await
            .expect("session should initialize");
        sessions.push(session);
    }

    let listed = response(
        &manager,
        &sessions[0],
        message(json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}})),
    )
    .await;
    let names = listed["result"]["tools"]
        .as_array()
        .expect("tools should be an array")
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect::<Vec<_>>();
    for expected in [
        "register_session",
        "register_delegation",
        "register_watch_path",
        "report_progress",
        "report_waiting",
        "complete_session",
        "update_deadline",
        "get_session",
        "list_sessions",
        "get_session_tree",
        "list_events",
        "get_watchdog_health",
    ] {
        assert!(names.contains(&expected), "missing MCP tool {expected}");
    }

    assert_register_session_contract(&listed);
    assert_finite_inputs_are_enums(&listed);
    assert_event_cursor_contract(&listed);

    for (index, native_id) in ["main-a", "main-b"].into_iter().enumerate() {
        let registered = response(
            &manager,
            &sessions[index],
            message(json!({
                "jsonrpc":"2.0","id":10 + index,"method":"tools/call","params":{
                    "name":"register_session","arguments":{
                        "runtime":"claude_code","native_id":native_id,"kind":"main",
                        "event_key":format!("register-{native_id}")
                    }
                }
            })),
        )
        .await;
        assert!(registered.get("error").is_none(), "{registered}");
    }

    let main_b = SessionId::from_native(
        &NativeSessionKey::new(RuntimeKind::ClaudeCode, "main-b")
            .expect("native ID should be valid"),
    );
    let denied = response(
        &manager,
        &sessions[0],
        message(json!({
            "jsonrpc":"2.0","id":20,"method":"tools/call","params":{
                "name":"get_session","arguments":{"session_id":main_b.to_string()}
            }
        })),
    )
    .await;
    assert_eq!(denied["error"]["code"], -32602);
    assert!(
        denied["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("outside"))
    );

    for session in &sessions {
        manager
            .close_session(session)
            .await
            .expect("session should close");
    }
    for service in services {
        let running = service
            .await
            .expect("server task should join")
            .expect("server should initialize");
        running.waiting().await.expect("closed server should stop");
    }
}

/// Object-shaped mutating tools, ordered so each call is legal for the child's
/// current state.
///
/// `register_watch_path` is absent because it needs configured worktree roots
/// this transport fixture deliberately lacks; its response conformance is
/// asserted in `watch_paths.rs`, where that capability wiring exists.
fn structured_tool_calls(main: SessionId, child: SessionId) -> Vec<(u32, &'static str, Value)> {
    vec![
        (
            5,
            "register_delegation",
            json!({
                "parent_session_id": main.to_string(),
                "child_session_id": child.to_string(),
                "event_key": "structured-delegation"
            }),
        ),
        (
            7,
            "report_progress",
            json!({
                "session_id": child.to_string(),
                "event_key": "structured-progress",
                "summary": "structured output check"
            }),
        ),
        (
            8,
            "report_waiting",
            json!({
                "session_id": child.to_string(),
                "event_key": "structured-waiting",
                "waiting_for": "tool"
            }),
        ),
        (
            9,
            "update_deadline",
            json!({
                "session_id": child.to_string(),
                "event_key": "structured-deadline",
                "action": "set",
                "deadline_ms": 20_000
            }),
        ),
        (
            10,
            "complete_session",
            json!({
                "session_id": child.to_string(),
                "event_key": "structured-completion",
                "outcome": "completed"
            }),
        ),
        (11, "get_session", json!({"session_id": main.to_string()})),
        (12, "get_session_tree", json!({})),
        (13, "list_events", json!({"limit": 10})),
        (14, "get_watchdog_health", json!({})),
    ]
}

#[tokio::test]
async fn structured_output_schemas_validate_live_tool_results() {
    let api = test_api().await;
    let manager = Arc::new(WatchdogSessionManager::new(
        api.clone(),
        McpLimits::default(),
    ));
    let (session, transport) = manager
        .create_session()
        .await
        .expect("rmcp session should be created");
    let server = WatchdogMcpService::new(api);
    let service = tokio::spawn(async move { server.serve(transport).await });
    manager
        .initialize_session(&session, initialize_request())
        .await
        .expect("session should initialize");

    let listed = response(
        &manager,
        &session,
        message(json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}})),
    )
    .await;
    // Every tool returning an object-shaped payload advertises a schema, so an
    // MCP client sees the identical SessionView treated identically everywhere.
    for tool_name in [
        "register_session",
        "register_delegation",
        "register_watch_path",
        "report_progress",
        "report_waiting",
        "complete_session",
        "update_deadline",
        "get_session",
        "get_session_tree",
        "list_events",
        "get_watchdog_health",
    ] {
        assert!(
            listed_tool(&listed, tool_name)["outputSchema"].is_object(),
            "{tool_name} should advertise an output schema"
        );
    }

    let registered = response(
        &manager,
        &session,
        message(json!({
            "jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                "name":"register_session","arguments":{
                    "runtime":"claude_code","native_id":"structured-main","kind":"main",
                    "event_key":"register-structured-main"
                }
            }
        })),
    )
    .await;
    assert!(registered.get("error").is_none(), "{registered}");
    assert_structured_result_matches_schema(&listed, "register_session", &registered);
    let main = SessionId::from_native(
        &NativeSessionKey::new(RuntimeKind::ClaudeCode, "structured-main")
            .expect("native ID should be valid"),
    );
    let child = register_structured_child(&manager, &session, main).await;

    for (request_id, tool_name, arguments) in structured_tool_calls(main, child) {
        let called = response(
            &manager,
            &session,
            message(json!({
                "jsonrpc":"2.0","id":request_id,"method":"tools/call","params":{
                    "name":tool_name,"arguments":arguments
                }
            })),
        )
        .await;
        assert!(called.get("error").is_none(), "{tool_name}: {called}");
        assert_structured_result_matches_schema(&listed, tool_name, &called);
    }

    manager
        .close_session(&session)
        .await
        .expect("session should close");
    let running = service
        .await
        .expect("server task should join")
        .expect("server should initialize");
    running.waiting().await.expect("closed server should stop");
}

/// Generous bound standing in for "the endpoint never answered at all".
const STALL_DETECTION_BOUND: Duration = Duration::from_mins(10);

/// Collects one test's `tracing` output so a log contract can be asserted.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn text(&self) -> String {
        String::from_utf8_lossy(
            &self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
        .into_owned()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl tracing_subscriber::fmt::MakeWriter<'_> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}

/// A client that announces a body and then never sends it.
fn stalled_body() -> Body {
    Body::from_stream(futures::stream::pending::<
        Result<axum::body::Bytes, std::io::Error>,
    >())
}

#[tokio::test]
async fn mcp_post_with_a_stalled_request_body_is_bounded_and_logged() {
    let logs = CapturedLogs::default();
    let _capture = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish(),
    );
    let router = mcp_router(
        test_api().await,
        BearerAuthenticator::new("stall-secret").expect("token should be valid"),
        McpLimits::default(),
    );

    // Virtual time only once the store is open: a clock paused any earlier
    // trips sqlx's pool-acquisition timeout before it can connect.
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let response = tokio::time::timeout(
        STALL_DETECTION_BOUND,
        router.oneshot(
            Request::post("/mcp")
                .header("host", "localhost")
                .header("authorization", "Bearer stall-secret")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(stalled_body())
                .expect("request should build"),
        ),
    )
    .await
    .expect("a stalled request body must never park the MCP endpoint indefinitely")
    .expect("router is infallible");

    assert_eq!(
        response.status(),
        StatusCode::REQUEST_TIMEOUT,
        "a stalled body deserves an explicit answer, not a dropped connection"
    );
    // Virtual time, so the only slack is the timer wheel's millisecond tick.
    let bound = McpLimits::default().request_body_timeout();
    let elapsed = started.elapsed();
    assert!(
        (bound..bound + Duration::from_secs(1)).contains(&elapsed),
        "the answer must arrive on the configured bound of {bound:?}, not merely eventually: {elapsed:?}"
    );
    let captured = logs.text();
    assert!(
        captured.contains("mcp.request_body_timeout"),
        "the timeout must leave a greppable server-side trace: {captured}"
    );
}

#[tokio::test]
async fn mcp_stalled_request_body_is_refused_on_credentials_before_it_is_read() {
    let router = mcp_router(
        test_api().await,
        BearerAuthenticator::new("order-secret").expect("token should be valid"),
        McpLimits::default(),
    );

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let response = tokio::time::timeout(
        STALL_DETECTION_BOUND,
        router.oneshot(
            Request::post("/mcp")
                .header("host", "localhost")
                .header("authorization", "Bearer wrong-secret")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(stalled_body())
                .expect("request should build"),
        ),
    )
    .await
    .expect("an unauthenticated stalled body must not be waited on at all")
    .expect("router is infallible");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    // Authentication is the outer layer, so a rejected caller never gets to
    // occupy the endpoint for the length of the body bound.
    assert_eq!(
        started.elapsed(),
        Duration::ZERO,
        "credentials must be checked on headers alone, before any body is read"
    );
}

#[tokio::test]
async fn mcp_server_push_stream_outlives_the_request_body_bound() {
    let router = mcp_router(
        test_api().await,
        BearerAuthenticator::new("push-secret").expect("token should be valid"),
        McpLimits::default(),
    );
    let session = initialize_authenticated_session(&router, "push-secret").await;
    tokio::time::pause();

    let response = router
        .oneshot(
            Request::get("/mcp")
                .header("host", "localhost")
                .header("authorization", "Bearer push-secret")
                .header("mcp-session-id", session.to_string())
                .header("accept", "text/event-stream")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router is infallible");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the standalone server-push stream should open"
    );
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream"),
        "the guarded response must actually be the long-lived SSE stream"
    );

    let idle = McpLimits::default().request_body_timeout() * 5;
    let mut frames = response.into_body().into_data_stream();
    match tokio::time::timeout(idle, frames.next()).await {
        Err(_) | Ok(Some(Ok(_))) => {}
        Ok(other) => panic!(
            "a long-lived server-push stream must not be cut by the request-body bound: {other:?}"
        ),
    }
}
