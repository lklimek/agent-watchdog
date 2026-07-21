//! rmcp transport identity, tool surface, and scope integration tests.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use futures::StreamExt;
use rmcp::{
    ServiceExt, model::ClientJsonRpcMessage,
    transport::streamable_http_server::session::SessionManager,
};
use serde_json::{Value, json};
use tower::ServiceExt as _;
use watchdog_domain::{NativeSessionKey, RuntimeKind, SessionId, TimePoint, WallTimeMs};
use watchdog_server::{
    AgentApi, BearerAuthenticator, WatchdogMcpService, WatchdogSessionManager, mcp_router,
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
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("response body should be bounded");
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
}

#[tokio::test]
async fn real_rmcp_transport_exposes_all_tools_and_rejects_cross_tree_target() {
    let api = test_api().await;
    let manager = Arc::new(WatchdogSessionManager::default());
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
