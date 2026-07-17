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
