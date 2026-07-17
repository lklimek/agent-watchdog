use std::{collections::HashMap, sync::Arc};

use futures::Stream;
use rmcp::{
    RoleServer, ServerHandler, ServiceExt,
    model::{
        ClientJsonRpcMessage, InitializeRequestParams, InitializeResult, ServerCapabilities,
        ServerJsonRpcMessage,
    },
    service::RequestContext,
    transport::streamable_http_server::session::{
        RestoreOutcome, ServerSseMessage, SessionId, SessionManager,
        local::{LocalSessionManager, LocalSessionManagerError},
    },
};
use tokio::sync::{Mutex, RwLock};

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransportScope(SessionId);

#[derive(Debug, Default)]
struct InjectingSessionManager {
    inner: LocalSessionManager,
}

impl InjectingSessionManager {
    fn scoped(id: &SessionId, mut message: ClientJsonRpcMessage) -> ClientJsonRpcMessage {
        message.insert_extension(TransportScope(id.clone()));
        message
    }
}

impl SessionManager for InjectingSessionManager {
    type Error = LocalSessionManagerError;
    type Transport = <LocalSessionManager as SessionManager>::Transport;

    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        self.inner.create_session().await
    }

    async fn initialize_session(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        self.inner
            .initialize_session(id, Self::scoped(id, message))
            .await
    }

    async fn has_session(&self, id: &SessionId) -> Result<bool, Self::Error> {
        self.inner.has_session(id).await
    }

    async fn close_session(&self, id: &SessionId) -> Result<(), Self::Error> {
        self.inner.close_session(id).await
    }

    async fn create_stream(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner
            .create_stream(id, Self::scoped(id, message))
            .await
    }

    async fn accept_message(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        self.inner
            .accept_message(id, Self::scoped(id, message))
            .await
    }

    async fn create_standalone_stream(
        &self,
        id: &SessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner.create_standalone_stream(id).await
    }

    async fn resume(
        &self,
        id: &SessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner.resume(id, last_event_id).await
    }

    async fn restore_session(
        &self,
        id: SessionId,
    ) -> Result<RestoreOutcome<Self::Transport>, Self::Error> {
        self.inner.restore_session(id).await
    }
}

#[derive(Clone, Debug)]
struct ScopeCapturingServer {
    seen: Arc<Mutex<Vec<SessionId>>>,
}

impl ServerHandler for ScopeCapturingServer {
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, rmcp::ErrorData> {
        context.peer.set_peer_info(request);
        let scope = context
            .extensions
            .get::<TransportScope>()
            .expect("custom manager must inject the rmcp transport ID");
        self.seen.lock().await.push(scope.0.clone());
        Ok(InitializeResult::new(ServerCapabilities::default()))
    }
}

#[derive(Debug, Default)]
struct ApplicationScopes {
    roots: RwLock<HashMap<SessionId, &'static str>>,
    parents: HashMap<&'static str, &'static str>,
    inbox: RwLock<HashMap<&'static str, Vec<(u64, &'static str)>>>,
}

impl ApplicationScopes {
    async fn bind_once(&self, transport: SessionId, root: &'static str) -> bool {
        let mut roots = self.roots.write().await;
        if let Some(bound) = roots.get(&transport) {
            *bound == root
        } else {
            roots.insert(transport, root);
            true
        }
    }

    async fn can_read(&self, transport: &SessionId, target: &str) -> bool {
        let roots = self.roots.read().await;
        let Some(root) = roots.get(transport) else {
            return false;
        };
        target == *root
            || self
                .parents
                .get(target)
                .is_some_and(|parent| parent == root)
    }

    async fn append(&self, root: &'static str, sequence: u64, event: &'static str) {
        self.inbox
            .write()
            .await
            .entry(root)
            .or_default()
            .push((sequence, event));
    }

    async fn after(&self, root: &str, cursor: u64) -> Vec<(u64, &'static str)> {
        self.inbox
            .read()
            .await
            .get(root)
            .into_iter()
            .flatten()
            .filter(|(sequence, _)| *sequence > cursor)
            .copied()
            .collect()
    }
}

fn initialize_request() -> ClientJsonRpcMessage {
    serde_json::from_str(
        r#"{
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": { "name": "watchdog-spike", "version": "0.0.0" }
            }
        }"#,
    )
    .expect("initialize fixture should deserialize")
}

#[tokio::test]
async fn rmcp_transport_id_reaches_initialize_context_and_scopes_durable_inbox() {
    let manager = Arc::new(InjectingSessionManager::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut session_ids = Vec::new();
    let mut services = Vec::new();

    for _ in 0..2 {
        let (session_id, transport) = manager
            .create_session()
            .await
            .expect("rmcp session should be created");
        let server = ScopeCapturingServer {
            seen: Arc::clone(&seen),
        };
        let service = tokio::spawn(async move { server.serve(transport).await });
        manager
            .initialize_session(&session_id, initialize_request())
            .await
            .expect("rmcp session should initialize");
        session_ids.push(session_id);
        services.push(service);
    }

    assert_eq!(*seen.lock().await, session_ids);

    let scopes = ApplicationScopes {
        parents: HashMap::from([("child-a", "main-a"), ("child-b", "main-b")]),
        ..ApplicationScopes::default()
    };
    assert!(scopes.bind_once(session_ids[0].clone(), "main-a").await);
    assert!(scopes.bind_once(session_ids[1].clone(), "main-b").await);
    assert!(!scopes.bind_once(session_ids[0].clone(), "main-b").await);
    assert!(scopes.can_read(&session_ids[0], "child-a").await);
    assert!(!scopes.can_read(&session_ids[0], "child-b").await);

    scopes.append("main-a", 1, "started").await;
    scopes.append("main-a", 2, "stalled").await;
    assert_eq!(scopes.after("main-a", 1).await, vec![(2, "stalled")]);

    for session_id in &session_ids {
        manager
            .close_session(session_id)
            .await
            .expect("rmcp session should close");
    }
    for service in services {
        let running = service
            .await
            .expect("server task should join")
            .expect("server should initialize");
        running
            .waiting()
            .await
            .expect("closed server should stop cleanly");
    }
}
