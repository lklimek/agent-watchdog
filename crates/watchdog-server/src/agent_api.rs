use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use serde::Serialize;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock as AsyncRwLock};
use watchdog_domain::{
    AdapterIdentity, BoundedText, ChildSessionId, Clock, DeadlineCommand, DetailedState,
    DomainEvent, DomainInputError, EvidenceTrust, MainSessionId, NativeSessionKey,
    ObservationEnvelope, ObservationError, ObservationId, ObservationPayload, ObservationSource,
    ReducerPolicy, RuntimeKind, SessionId, SessionIdentity, SessionKind, SessionSnapshot,
    TimePoint,
};
use watchdog_runtime::{CoordinatorError, EventSequence, SessionCoordinator};
use watchdog_store::{
    AdapterHealthRecord, ApplyResult, InboxOffsetRecord, OutboxDestination, RelationRecord,
    SessionMetadataRecord, StoreError, StoredSessionRecord, WatchdogStore,
};

const MAX_TREE_SESSIONS: u32 = 1_000;
const MAX_EVENTS: u32 = 500;

/// Bounded opaque MCP transport identity used only for application scoping.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TransportKey(BoundedText<256>);

impl TransportKey {
    /// Validate one rmcp transport identity.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] for empty or oversized values.
    pub fn new(value: impl Into<String>) -> Result<Self, DomainInputError> {
        let value = BoundedText::new("transport_id", value)?;
        if value.is_empty() {
            return Err(DomainInputError::Empty {
                field: "transport_id",
            });
        }
        Ok(Self(value))
    }
}

/// Runtime-native registration used to enrich automatic discovery or create an
/// in-scope session before native evidence arrives.
#[derive(Clone, Debug)]
pub struct RegisterSession {
    /// Runtime namespace.
    pub runtime: RuntimeKind,
    /// Runtime-native session/job/thread identifier.
    pub native_id: String,
    /// Main or child role.
    pub kind: SessionKind,
    /// Existing parent for a child registration.
    pub parent: Option<SessionId>,
    /// Caller-provided idempotency key.
    pub event_key: String,
}

/// Runtime-native session discovered without MCP or optional hook registration.
#[derive(Clone, Debug)]
pub struct DiscoveredSession {
    /// Runtime namespace.
    pub runtime: RuntimeKind,
    /// Runtime-native session/job/thread identifier.
    pub native_id: String,
    /// Main or child role.
    pub kind: SessionKind,
    /// Existing native parent identity for a child discovery.
    pub parent: Option<SessionId>,
    /// Adapter-provided deterministic idempotency key.
    pub event_key: String,
    /// Bounded native title, when available.
    pub title: Option<String>,
    /// Capability-validated startup directory, when available.
    pub startup_directory: Option<String>,
}

/// Agent-reported waiting class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitingKind {
    /// Waiting for a delegated child.
    Agent,
    /// Waiting for a tool or external operation.
    Tool,
    /// Waiting for human input or approval.
    User,
    /// Intentional wait that also pauses timers.
    Intentional,
}

/// Agent-reported terminal outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionOutcome {
    /// Successful completion.
    Completed,
    /// Terminal failure.
    Failed,
    /// Intentional cancellation.
    Cancelled,
}

/// Agent-facing current session state and diagnostic evidence.
#[derive(Clone, Debug, Serialize)]
pub struct SessionView {
    /// Role-preserving runtime-neutral identity.
    pub session: SessionIdentity,
    /// Main-session tree identity.
    pub root: MainSessionId,
    /// Runtime namespace.
    pub runtime: RuntimeKind,
    /// Runtime-native identity for agent diagnostics.
    pub native_id: String,
    /// Complete reducer snapshot, including PID, warning, conflicts, and timers.
    pub snapshot: SessionSnapshot,
}

/// Durable transition paired with current detailed diagnostics for its subject.
#[derive(Clone, Debug, Serialize)]
pub struct AgentEventView {
    /// Ordered durable transition metadata.
    pub event: DomainEvent,
    /// Current full session evidence, including PID, CPU, conflicts, and warning.
    pub session: SessionView,
}

/// Durable parent event page independent from MCP transport replay.
#[derive(Clone, Debug, Serialize)]
pub struct EventPage {
    /// Exclusive cursor used for this query.
    pub after: u64,
    /// Highest returned durable event ID, or `after` for an empty page.
    pub next_cursor: u64,
    /// Ordered durable events.
    pub events: Vec<AgentEventView>,
}

/// Agent-facing hierarchy with current sessions and retained relation evidence.
#[derive(Clone, Debug, Serialize)]
pub struct SessionTreeView {
    /// Bound main-session tree.
    pub root: MainSessionId,
    /// Current session projections.
    pub sessions: Vec<SessionView>,
    /// Current and superseded bounded relation evidence.
    pub relations: Vec<RelationRecord>,
}

/// Agent-facing health needed to diagnose monitoring coverage.
#[derive(Clone, Debug, Serialize)]
pub struct AgentHealthView {
    /// Whether the database uses WAL journaling.
    pub store_wal: bool,
    /// Whether foreign-key enforcement is active.
    pub store_foreign_keys: bool,
    /// Applied schema version.
    pub schema_version: i64,
    /// Per-runtime persisted health, including actionable warning messages.
    pub adapters: Vec<AdapterHealthRecord>,
}

#[derive(Debug, Default)]
struct ScopeRegistry {
    roots: RwLock<HashMap<TransportKey, MainSessionId>>,
}

impl ScopeRegistry {
    fn bind_once(&self, transport: TransportKey, root: MainSessionId) -> Result<(), AgentApiError> {
        let mut roots = self
            .roots
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match roots.get(&transport) {
            Some(bound) if *bound == root => Ok(()),
            Some(_) => Err(AgentApiError::TransportAlreadyBound),
            None => {
                roots.insert(transport, root);
                Ok(())
            }
        }
    }

    fn root(&self, transport: &TransportKey) -> Result<MainSessionId, AgentApiError> {
        self.roots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(transport)
            .copied()
            .ok_or(AgentApiError::TransportNotBound)
    }
}

struct AgentApiInner {
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
    event_sequence: Arc<EventSequence>,
    policy: RwLock<ReducerPolicy>,
    scopes: ScopeRegistry,
    lanes: AsyncRwLock<HashMap<SessionId, Arc<Mutex<SessionCoordinator>>>>,
}

/// Scoped application service underlying all MCP tools.
#[derive(Clone)]
pub struct AgentApi {
    inner: Arc<AgentApiInner>,
}

impl std::fmt::Debug for AgentApi {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("AgentApi").finish_non_exhaustive()
    }
}

impl AgentApi {
    /// Construct the agent API and resume durable event allocation.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] when the store cannot provide a safe event
    /// sequence.
    pub async fn new(store: WatchdogStore, clock: Arc<dyn Clock>) -> Result<Self, AgentApiError> {
        Self::with_policy(store, clock, ReducerPolicy::default()).await
    }

    /// Construct the agent API with an explicit immutable policy snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] when the store cannot provide a safe event
    /// sequence.
    pub async fn with_policy(
        store: WatchdogStore,
        clock: Arc<dyn Clock>,
        policy: ReducerPolicy,
    ) -> Result<Self, AgentApiError> {
        let event_sequence = Arc::new(EventSequence::from_store(&store).await?);
        Ok(Self {
            inner: Arc::new(AgentApiInner {
                store,
                clock,
                event_sequence,
                policy: RwLock::new(policy),
                scopes: ScopeRegistry::default(),
                lanes: AsyncRwLock::new(HashMap::new()),
            }),
        })
    }

    /// Apply reloaded reducer thresholds to existing and future session lanes.
    pub async fn update_policy(&self, policy: ReducerPolicy) {
        *self
            .inner
            .policy
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = policy;
        let lanes = self
            .inner
            .lanes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for lane in lanes {
            lane.lock().await.set_policy(policy);
        }
    }

    /// Persist a restart boundary for every retained session before native
    /// reconciliation or timer-driven work resumes.
    ///
    /// Linux monotonic timestamps are process-local and cannot be ordered
    /// across a server restart. This clears the retained monotonic cursor and
    /// marks each session as requiring fresh evidence.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] when retained sessions cannot be listed or a
    /// restart marker cannot be committed atomically.
    pub async fn mark_restarted(&self) -> Result<(), AgentApiError> {
        let mut sessions = self
            .inner
            .store
            .sessions_by_kind(SessionKind::Main, MAX_TREE_SESSIONS)
            .await?;
        sessions.extend(
            self.inner
                .store
                .sessions_by_kind(SessionKind::Child, MAX_TREE_SESSIONS)
                .await?,
        );
        let now = self.inner.clock.now();
        for record in sessions {
            let observation = restart_observation(&record, now)?;
            let lane = self.lane(&record, now).await?;
            lane.lock().await.apply_restarted(observation).await?;
        }
        Ok(())
    }

    /// Persist one automatically discovered session independently from MCP
    /// registration while retaining the ability for a parent agent to bind the
    /// resulting main tree later.
    ///
    /// Adapter callers must capability-validate any path before supplying it as
    /// operator-facing metadata. Repeated discoveries with the same event key
    /// are idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for invalid identity/metadata, a missing child
    /// parent, conflicting hierarchy, or persistence failure.
    pub async fn discover_session(
        &self,
        request: DiscoveredSession,
    ) -> Result<SessionView, AgentApiError> {
        let native = NativeSessionKey::new(request.runtime, request.native_id.clone())?;
        let session_id = SessionId::from_native(&native);
        let root = match request.kind {
            SessionKind::Main => MainSessionId::from(session_id),
            SessionKind::Child => {
                let parent_id = request.parent.ok_or(AgentApiError::MissingParent)?;
                self.inner
                    .store
                    .session_by_id(parent_id)
                    .await?
                    .ok_or(AgentApiError::SessionNotFound)?
                    .root
            }
        };
        let transport = discovery_transport(root)?;
        self.inner.scopes.bind_once(transport.clone(), root)?;
        let view = self
            .register_session(
                &transport,
                RegisterSession {
                    runtime: request.runtime,
                    native_id: request.native_id,
                    kind: request.kind,
                    parent: request.parent,
                    event_key: request.event_key,
                },
            )
            .await?;

        let existing = self.inner.store.session_metadata(view.session).await?;
        let metadata = SessionMetadataRecord::new(
            view.session,
            request.title.or_else(|| {
                existing
                    .as_ref()
                    .and_then(SessionMetadataRecord::title)
                    .map(ToOwned::to_owned)
            }),
            request.startup_directory.or_else(|| {
                existing
                    .as_ref()
                    .and_then(SessionMetadataRecord::startup_directory)
                    .map(ToOwned::to_owned)
            }),
            existing
                .as_ref()
                .and_then(SessionMetadataRecord::repository_remote)
                .map(ToOwned::to_owned),
            existing
                .as_ref()
                .and_then(SessionMetadataRecord::branch)
                .map(ToOwned::to_owned),
            existing
                .as_ref()
                .and_then(SessionMetadataRecord::pull_request_number),
            existing
                .as_ref()
                .and_then(SessionMetadataRecord::pull_request_url)
                .map(ToOwned::to_owned),
            self.inner.clock.now().wall_time(),
        )?;
        self.inner.store.save_session_metadata(&metadata).await?;
        Ok(view)
    }

    /// Apply one provenance-preserving runtime observation to an already
    /// discovered native session.
    ///
    /// Retries reuse the timestamp already committed for the same observation
    /// identity. Subject, source, and payload must still match exactly, so an
    /// adapter cannot reuse an event key for materially different evidence.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] when the session is absent, its stable native
    /// identity conflicts, or reducer/persistence application fails.
    pub async fn ingest_native_observation(
        &self,
        observation: ObservationEnvelope,
    ) -> Result<SessionView, AgentApiError> {
        let session_id = SessionId::from_native(observation.subject());
        let record = self
            .inner
            .store
            .session_by_id(session_id)
            .await?
            .ok_or(AgentApiError::SessionNotFound)?;
        if record.native != *observation.subject() {
            return Err(AgentApiError::SessionIdentityConflict);
        }
        self.apply_observation(&record, observation).await?;
        self.view_for_record(&record).await
    }

    /// Register/enrich a session and bind a main registration's transport once.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for invalid identity, cross-tree access,
    /// rebinding, role conflict, or persistence failure.
    pub async fn register_session(
        &self,
        transport: &TransportKey,
        request: RegisterSession,
    ) -> Result<SessionView, AgentApiError> {
        validate_event_key(&request.event_key)?;
        let native = NativeSessionKey::new(request.runtime, request.native_id)?;
        let session_id = SessionId::from_native(&native);
        let now = self.inner.clock.now();
        let (session, root, parent) = match request.kind {
            SessionKind::Main => {
                let root = MainSessionId::from(session_id);
                (SessionIdentity::Main(root), root, None)
            }
            SessionKind::Child => {
                let parent_id = request.parent.ok_or(AgentApiError::MissingParent)?;
                let parent = self.resolve_scoped(transport, parent_id).await?;
                (
                    SessionIdentity::Child(ChildSessionId::from(session_id)),
                    parent.root,
                    Some(parent),
                )
            }
        };
        let record = StoredSessionRecord {
            session,
            root,
            native,
        };
        if let Some(existing) = self.inner.store.session_by_id(session_id).await?
            && existing != record
        {
            return Err(AgentApiError::SessionIdentityConflict);
        }
        let result = self
            .apply_payload(
                &record,
                "register_session",
                &request.event_key,
                ObservationPayload::Progress(BoundedText::new(
                    "progress",
                    "Session registered with Agent Watchdog",
                )?),
                now,
            )
            .await?;
        if let Some(parent) = parent
            && result == ApplyResult::Applied
        {
            self.save_relation(&record, &parent, &request.event_key, now)
                .await?;
        }
        if request.kind == SessionKind::Main {
            self.inner.scopes.bind_once(transport.clone(), root)?;
        }
        self.view_for_record(&record).await
    }

    /// Bind a transport to an already auto-discovered main session.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] if the target is absent/not-main or the
    /// transport was previously bound to another tree.
    pub async fn bind_discovered_main(
        &self,
        transport: &TransportKey,
        session_id: SessionId,
    ) -> Result<SessionView, AgentApiError> {
        let record = self
            .inner
            .store
            .session_by_id(session_id)
            .await?
            .ok_or(AgentApiError::SessionNotFound)?;
        let SessionIdentity::Main(main) = record.session else {
            return Err(AgentApiError::MainSessionRequired);
        };
        if main != record.root {
            return Err(AgentApiError::SessionIdentityConflict);
        }
        self.inner
            .scopes
            .bind_once(transport.clone(), record.root)?;
        self.view_for_record(&record).await
    }

    /// Record bounded progress for one in-scope session.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for scope, bounds, identity, or persistence
    /// failure.
    pub async fn report_progress(
        &self,
        transport: &TransportKey,
        session_id: SessionId,
        event_key: &str,
        summary: String,
        operation: Option<String>,
    ) -> Result<SessionView, AgentApiError> {
        validate_event_key(event_key)?;
        let summary = progress_text(&summary, operation.as_deref())?;
        self.mutate_scoped(
            transport,
            session_id,
            "report_progress",
            event_key,
            ObservationPayload::Progress(summary),
        )
        .await
    }

    /// Register or replace an exact in-scope delegation relation and optional
    /// expected check-in deadline.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for scope, role, bounds, identity, or
    /// persistence failure.
    pub async fn register_delegation(
        &self,
        transport: &TransportKey,
        parent_id: SessionId,
        child_id: SessionId,
        event_key: &str,
        deadline: Option<DeadlineCommand>,
    ) -> Result<SessionView, AgentApiError> {
        validate_event_key(event_key)?;
        let parent = self.resolve_scoped(transport, parent_id).await?;
        let child = self.resolve_scoped(transport, child_id).await?;
        if !matches!(child.session, SessionIdentity::Child(_)) {
            return Err(AgentApiError::ChildSessionRequired);
        }
        self.save_relation(&child, &parent, event_key, self.inner.clock.now())
            .await?;
        if let Some(command) = deadline {
            return self
                .mutate_scoped(
                    transport,
                    child_id,
                    "register_delegation_deadline",
                    event_key,
                    ObservationPayload::Deadline(command),
                )
                .await;
        }
        self.view_for_record(&child).await
    }

    /// Report an in-scope waiting state. Intentional waiting also pauses timers.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for scope, identity, or persistence failure.
    pub async fn report_waiting(
        &self,
        transport: &TransportKey,
        session_id: SessionId,
        event_key: &str,
        kind: WaitingKind,
    ) -> Result<SessionView, AgentApiError> {
        validate_event_key(event_key)?;
        let state = match kind {
            WaitingKind::Agent | WaitingKind::Intentional => DetailedState::WaitingForAgent,
            WaitingKind::Tool => DetailedState::WaitingForTool,
            WaitingKind::User => DetailedState::WaitingForUser,
        };
        let view = self
            .mutate_scoped(
                transport,
                session_id,
                "report_waiting",
                event_key,
                ObservationPayload::NativeState(state),
            )
            .await?;
        if kind == WaitingKind::Intentional {
            self.mutate_scoped(
                transport,
                session_id,
                "report_waiting_pause",
                &format!("{event_key}:pause"),
                ObservationPayload::Deadline(DeadlineCommand::Pause),
            )
            .await
        } else {
            Ok(view)
        }
    }

    /// Report a terminal outcome for one in-scope session.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for scope, identity, or persistence failure.
    pub async fn complete_session(
        &self,
        transport: &TransportKey,
        session_id: SessionId,
        event_key: &str,
        outcome: CompletionOutcome,
    ) -> Result<SessionView, AgentApiError> {
        validate_event_key(event_key)?;
        let state = match outcome {
            CompletionOutcome::Completed => DetailedState::Completed,
            CompletionOutcome::Failed => DetailedState::Failed,
            CompletionOutcome::Cancelled => DetailedState::Cancelled,
        };
        self.mutate_scoped(
            transport,
            session_id,
            "complete_session",
            event_key,
            ObservationPayload::NativeState(state),
        )
        .await
    }

    /// Change a deadline for one in-scope session.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for scope, identity, or persistence failure.
    pub async fn update_deadline(
        &self,
        transport: &TransportKey,
        session_id: SessionId,
        event_key: &str,
        command: DeadlineCommand,
    ) -> Result<SessionView, AgentApiError> {
        validate_event_key(event_key)?;
        self.mutate_scoped(
            transport,
            session_id,
            "update_deadline",
            event_key,
            ObservationPayload::Deadline(command),
        )
        .await
    }

    /// Read one in-scope session.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for scope, missing state, or persistence
    /// failure.
    pub async fn get_session(
        &self,
        transport: &TransportKey,
        session_id: SessionId,
    ) -> Result<SessionView, AgentApiError> {
        let record = self.resolve_scoped(transport, session_id).await?;
        self.view_for_record(&record).await
    }

    /// List every bounded session in the transport's main tree.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for unbound transports, missing snapshots, or
    /// persistence failure.
    pub async fn list_sessions(
        &self,
        transport: &TransportKey,
    ) -> Result<Vec<SessionView>, AgentApiError> {
        let root = self.inner.scopes.root(transport)?;
        let records = self
            .inner
            .store
            .sessions_for_root(root, MAX_TREE_SESSIONS)
            .await?;
        let mut views = Vec::with_capacity(records.len());
        for record in records {
            views.push(self.view_for_record(&record).await?);
        }
        Ok(views)
    }

    /// Read the bound hierarchy and retained correlation evidence.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for unbound transports, missing snapshots, or
    /// persistence failure.
    pub async fn session_tree(
        &self,
        transport: &TransportKey,
    ) -> Result<SessionTreeView, AgentApiError> {
        let root = self.inner.scopes.root(transport)?;
        let sessions = self.list_sessions(transport).await?;
        let relations = self
            .inner
            .store
            .relations_for_root(root, MAX_TREE_SESSIONS)
            .await?;
        Ok(SessionTreeView {
            root,
            sessions,
            relations,
        })
    }

    /// Read database and adapter health relevant to agent diagnostics.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for unbound transports or persistence failure.
    pub async fn health(&self, transport: &TransportKey) -> Result<AgentHealthView, AgentApiError> {
        self.inner.scopes.root(transport)?;
        let store = self.inner.store.health().await?;
        let mut adapters = Vec::new();
        for runtime in [
            RuntimeKind::ClaudeCode,
            RuntimeKind::CodexCli,
            RuntimeKind::CodexCompanion,
        ] {
            if let Some(health) = self.inner.store.adapter_health(runtime).await? {
                adapters.push(health);
            }
        }
        Ok(AgentHealthView {
            store_wal: store.journal_mode == "wal",
            store_foreign_keys: store.foreign_keys,
            schema_version: store.schema_version,
            adapters,
        })
    }

    /// Read durable events after a caller-confirmed cursor.
    ///
    /// Passing `after` also advances the stored acknowledgement monotonically;
    /// the returned `next_cursor` is not stored until a later call confirms it.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for unbound transports, invalid limits, cursor
    /// persistence, or event read failure.
    pub async fn list_events(
        &self,
        transport: &TransportKey,
        after: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, AgentApiError> {
        if limit == 0 || limit > MAX_EVENTS {
            return Err(AgentApiError::InvalidLimit);
        }
        let root = self.inner.scopes.root(transport)?;
        let durable = self.inner.store.inbox_offset(root).await?;
        let cursor =
            after.unwrap_or_else(|| durable.map_or(0, |offset| offset.last_event_id.value()));
        if let Some(confirmed) = after {
            self.inner
                .store
                .save_inbox_offset(InboxOffsetRecord {
                    parent: root,
                    last_event_id: watchdog_domain::EventId::new(confirmed),
                    updated_at: self.inner.clock.now().wall_time(),
                })
                .await?;
        }
        let domain_events = self
            .inner
            .store
            .events_after(root, watchdog_domain::EventId::new(cursor), limit)
            .await?;
        let next_cursor = domain_events
            .last()
            .map_or(cursor, |event| event.id().value());
        let mut events = Vec::with_capacity(domain_events.len());
        for event in domain_events {
            let record = self
                .inner
                .store
                .session_by_id(event.subject().session_id())
                .await?
                .ok_or(AgentApiError::SessionNotFound)?;
            events.push(AgentEventView {
                session: self.view_for_record(&record).await?,
                event,
            });
        }
        Ok(EventPage {
            after: cursor,
            next_cursor,
            events,
        })
    }

    async fn mutate_scoped(
        &self,
        transport: &TransportKey,
        session_id: SessionId,
        operation: &str,
        event_key: &str,
        payload: ObservationPayload,
    ) -> Result<SessionView, AgentApiError> {
        let record = self.resolve_scoped(transport, session_id).await?;
        self.apply_payload(
            &record,
            operation,
            event_key,
            payload,
            self.inner.clock.now(),
        )
        .await?;
        self.view_for_record(&record).await
    }

    async fn apply_payload(
        &self,
        record: &StoredSessionRecord,
        operation: &str,
        event_key: &str,
        payload: ObservationPayload,
        now: TimePoint,
    ) -> Result<ApplyResult, AgentApiError> {
        let observation = mcp_observation(record, operation, event_key, payload, now)?;
        self.apply_observation(record, observation).await
    }

    async fn apply_observation(
        &self,
        record: &StoredSessionRecord,
        mut observation: ObservationEnvelope,
    ) -> Result<ApplyResult, AgentApiError> {
        let lane = self.lane(record, observation.observed_at()).await?;
        let mut lane = lane.lock().await;
        if let Some(existing) = self
            .inner
            .store
            .observation(observation.observation_id())
            .await?
        {
            // Ingestion timestamps are assigned while an event is observed,
            // rather than being part of the caller's idempotency contract.
            // Reuse the committed timestamp for byte-for-byte comparison while
            // preserving conflict detection for source or payload changes.
            observation = ObservationEnvelope::new(
                observation.observation_id(),
                observation.subject().clone(),
                existing.observed_at(),
                observation.source().clone(),
                observation.payload().clone(),
            )?;
        }
        let result = lane.apply_observation(observation).await?;
        Ok(result)
    }

    async fn lane(
        &self,
        record: &StoredSessionRecord,
        now: TimePoint,
    ) -> Result<Arc<Mutex<SessionCoordinator>>, AgentApiError> {
        if let Some(lane) = self
            .inner
            .lanes
            .read()
            .await
            .get(&record.session.session_id())
            .cloned()
        {
            return Ok(lane);
        }
        let snapshot = match self.inner.store.snapshot(record.session).await? {
            Some(stored) => stored
                .reducer_snapshot()
                .cloned()
                .ok_or(AgentApiError::ReducerSnapshotUnavailable)?,
            None => SessionSnapshot::new(record.session, record.root, now),
        };
        let lane = Arc::new(Mutex::new(SessionCoordinator::new(
            self.inner.store.clone(),
            snapshot,
            *self
                .inner
                .policy
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Arc::clone(&self.inner.event_sequence),
            [OutboxDestination::ParentInbox, OutboxDestination::Sse],
        )));
        let mut lanes = self.inner.lanes.write().await;
        Ok(lanes
            .entry(record.session.session_id())
            .or_insert_with(|| Arc::clone(&lane))
            .clone())
    }

    async fn resolve_scoped(
        &self,
        transport: &TransportKey,
        session_id: SessionId,
    ) -> Result<StoredSessionRecord, AgentApiError> {
        let root = self.inner.scopes.root(transport)?;
        let record = self
            .inner
            .store
            .session_by_id(session_id)
            .await?
            .ok_or(AgentApiError::SessionNotFound)?;
        if record.root != root {
            return Err(AgentApiError::CrossTreeAccess);
        }
        Ok(record)
    }

    async fn view_for_record(
        &self,
        record: &StoredSessionRecord,
    ) -> Result<SessionView, AgentApiError> {
        let stored = self
            .inner
            .store
            .snapshot(record.session)
            .await?
            .ok_or(AgentApiError::SessionNotFound)?;
        let snapshot = stored
            .reducer_snapshot()
            .cloned()
            .ok_or(AgentApiError::ReducerSnapshotUnavailable)?;
        Ok(SessionView {
            session: record.session,
            root: record.root,
            runtime: record.native.runtime(),
            native_id: record.native.native_id().to_owned(),
            snapshot,
        })
    }

    async fn save_relation(
        &self,
        child: &StoredSessionRecord,
        parent: &StoredSessionRecord,
        event_key: &str,
        now: TimePoint,
    ) -> Result<(), AgentApiError> {
        let SessionIdentity::Child(child_id) = child.session else {
            return Err(AgentApiError::ChildSessionRequired);
        };
        let source = ObservationSource::new(
            AdapterIdentity::new(child.native.runtime(), "mcp-v1")?,
            format!("mcp:register_delegation:{event_key}"),
            EvidenceTrust::Authoritative,
            None,
        )?;
        self.inner
            .store
            .select_relation(&RelationRecord {
                child: child_id,
                parent: parent.session,
                root: child.root,
                selected: true,
                provenance: source,
                valid_from: now.wall_time(),
                valid_until: None,
            })
            .await?;
        Ok(())
    }
}

fn discovery_transport(root: MainSessionId) -> Result<TransportKey, DomainInputError> {
    TransportKey::new(format!("autodiscovery:{}", root.session_id()))
}

fn restart_observation(
    record: &StoredSessionRecord,
    now: TimePoint,
) -> Result<ObservationEnvelope, AgentApiError> {
    let event_key = format!(
        "{}:{}:{}",
        record.session.session_id(),
        now.wall_time().value(),
        now.monotonic_ms()
    );
    Ok(ObservationEnvelope::new(
        ObservationId::from_native(record.native.runtime(), "server-restart", event_key)?,
        record.native.clone(),
        now,
        ObservationSource::new(
            AdapterIdentity::new(record.native.runtime(), "agent-watchdog-restart-v1")?,
            "server:restart",
            EvidenceTrust::Authoritative,
            None,
        )?,
        ObservationPayload::SchedulerTick,
    )?)
}

fn mcp_observation(
    record: &StoredSessionRecord,
    operation: &str,
    event_key: &str,
    payload: ObservationPayload,
    now: TimePoint,
) -> Result<ObservationEnvelope, AgentApiError> {
    let source = ObservationSource::new(
        AdapterIdentity::new(record.native.runtime(), "mcp-v1")?,
        format!("mcp:{operation}"),
        EvidenceTrust::Authoritative,
        None,
    )?;
    let native_event_id = format!("{}:{event_key}", record.session.session_id());
    Ok(ObservationEnvelope::new(
        ObservationId::from_native(
            record.native.runtime(),
            format!("mcp:{operation}"),
            native_event_id,
        )?,
        record.native.clone(),
        now,
        source,
        payload,
    )?)
}

fn progress_text(
    summary: &str,
    operation: Option<&str>,
) -> Result<BoundedText<2_048>, AgentApiError> {
    if summary.is_empty() {
        return Err(DomainInputError::Empty { field: "summary" }.into());
    }
    let value = operation.filter(|value| !value.is_empty()).map_or_else(
        || summary.to_owned(),
        |operation| format!("{operation}: {summary}"),
    );
    Ok(BoundedText::new("progress", value)?)
}

fn validate_event_key(event_key: &str) -> Result<(), AgentApiError> {
    let key = BoundedText::<256>::new("event_key", event_key)?;
    if key.is_empty() {
        return Err(DomainInputError::Empty { field: "event_key" }.into());
    }
    Ok(())
}

/// Scoped API failure without credentials or native payload content.
#[derive(Debug, Error)]
pub enum AgentApiError {
    /// Transport must register/bind a main session first.
    #[error("MCP transport is not bound to a main session")]
    TransportNotBound,
    /// A transport cannot change its bound main session.
    #[error("MCP transport is already bound to another main session")]
    TransportAlreadyBound,
    /// Target exists outside the transport's bound tree.
    #[error("MCP target is outside the bound main-session tree")]
    CrossTreeAccess,
    /// Referenced session does not exist.
    #[error("MCP target session does not exist")]
    SessionNotFound,
    /// Existing stable identity disagrees with registration.
    #[error("MCP registration conflicts with the stored session identity")]
    SessionIdentityConflict,
    /// Child registration omitted its parent.
    #[error("MCP child registration requires an existing parent")]
    MissingParent,
    /// Operation accepts a main session only.
    #[error("MCP operation requires a main session")]
    MainSessionRequired,
    /// Operation accepts a child session only.
    #[error("MCP operation requires a child session")]
    ChildSessionRequired,
    /// Requested list bound is invalid.
    #[error("MCP list limit is invalid")]
    InvalidLimit,
    /// Durable legacy projection lacks restartable reducer state.
    #[error("MCP session reducer state is unavailable until reconciliation")]
    ReducerSnapshotUnavailable,
    /// Bounded input failed domain validation.
    #[error(transparent)]
    Domain(#[from] DomainInputError),
    /// Observation subject and runtime did not agree.
    #[error(transparent)]
    Observation(#[from] ObservationError),
    /// Transactional coordinator failed.
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    /// Durable store failed.
    #[error(transparent)]
    Store(#[from] StoreError),
}
