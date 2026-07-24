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
    AgentApi, AgentApiError, BearerAuthenticator, TransportKey, WatchdogMcpService,
    WatchdogSessionManager, WatchdogSessionManagerError, mcp_router,
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
async fn mcp_binding_survives_transport_idle_periods() {
    let api = test_api().await;
    let manager = Arc::new(WatchdogSessionManager::new(api.clone()));
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
    let manager = Arc::new(WatchdogSessionManager::new(api.clone()));
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
    assert!(matches!(
        manager.create_session().await,
        Err(WatchdogSessionManagerError::Capacity)
    ));

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
async fn real_rmcp_transport_exposes_all_tools_and_rejects_cross_tree_target() {
    let api = test_api().await;
    let manager = Arc::new(WatchdogSessionManager::new(api.clone()));
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

#[tokio::test]
async fn structured_output_schemas_validate_live_tool_results() {
    let api = test_api().await;
    let manager = Arc::new(WatchdogSessionManager::default());
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
    for tool_name in [
        "get_session",
        "get_session_tree",
        "register_watch_path",
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
    let main = SessionId::from_native(
        &NativeSessionKey::new(RuntimeKind::ClaudeCode, "structured-main")
            .expect("native ID should be valid"),
    );

    for (request_id, tool_name, arguments) in [
        (4, "get_session", json!({"session_id": main.to_string()})),
        (5, "get_session_tree", json!({})),
        (6, "list_events", json!({"limit": 10})),
        (7, "get_watchdog_health", json!({})),
    ] {
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
