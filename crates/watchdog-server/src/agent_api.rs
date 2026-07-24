use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, RwLock},
};

use serde::Serialize;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock as AsyncRwLock, oneshot};
use watchdog_domain::{
    AdapterIdentity, BoundedText, ChildSessionId, Clock, CorrelationBasis, DeadlineCommand,
    DetailedState, DomainEvent, DomainInputError, EvidenceTrust, MainSessionId, NativeSessionKey,
    ObservationEnvelope, ObservationError, ObservationId, ObservationPayload, ObservationSource,
    ProcessIdentity, ReducerPolicy, RuntimeKind, SessionId, SessionIdentity, SessionKind,
    SessionSnapshot, TimePoint, WallTimeMs,
};
use watchdog_runtime::{
    AdmissionError, CoordinatorError, EventSequence, HealthScope, ObservationClass,
    SessionCoordinator, SessionQueue,
};
use watchdog_store::{
    ActivityEvidence, ActivitySampleRecord, AdapterHealthRecord, ApplyResult,
    DiscoveryAliasResolution, InboxOffsetRecord, OutboxDestination, RelationRecord,
    SessionMetadataRecord, StoreError, StoredSessionRecord, WatchdogStore,
};

pub(crate) const MAX_TREE_SESSIONS: u32 = 1_000;
const MAX_EVENTS: u32 = 500;
const OBSERVATION_QUEUE_CAPACITY: usize = 64;
// Retain a small recent sample without inflating every event response.
const DIAGNOSTIC_ACTIVITY_SAMPLES: u32 = 8;

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
    /// Tested native adapter version that produced the discovery evidence.
    pub adapter_version: String,
    /// Bounded provenance fingerprint naming the native discovery surface.
    pub evidence_source: String,
    /// Bounded native title, when available.
    pub title: Option<String>,
    /// Capability-validated startup directory, when available.
    pub startup_directory: Option<String>,
}

/// Best-effort repository metadata derived from a trusted native source.
#[derive(Clone, Debug, Default)]
pub struct RepositoryMetadata {
    /// Repository remote retained for later GitHub enrichment.
    pub remote: Option<String>,
    /// Current native branch.
    pub branch: Option<String>,
    /// Enriched open pull-request number.
    pub pull_request_number: Option<u64>,
    /// Locally validated pull-request URL.
    pub pull_request_url: Option<String>,
    /// Replace (and therefore allow clearing) stored pull-request fields.
    pub replace_pull_request: bool,
}

#[derive(Clone, Debug)]
enum RegistrationProvenance {
    Mcp,
    Native {
        adapter_version: String,
        evidence_source: String,
    },
}

impl RegistrationProvenance {
    const fn correlation_basis(&self) -> CorrelationBasis {
        match self {
            Self::Mcp => CorrelationBasis::McpRegistration,
            Self::Native { .. } => CorrelationBasis::ExactNative,
        }
    }
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
    /// Server wall time at which this response view was assembled.
    pub server_time: WallTimeMs,
    /// Role-preserving runtime-neutral identity.
    pub session: SessionIdentity,
    /// Main-session tree identity.
    pub root: MainSessionId,
    /// Runtime namespace.
    pub runtime: RuntimeKind,
    /// Runtime-native identity for agent diagnostics.
    pub native_id: String,
    /// Source of the latest observation that advanced the reducer snapshot.
    pub provenance: Option<ObservationSource>,
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
    /// Bounded parent-facing evidence assembled without transcript retrieval.
    pub diagnostics: AgentDiagnosticView,
}

/// Freshest bounded evidence needed to investigate a child alert.
#[derive(Clone, Debug, Serialize)]
pub struct AgentDiagnosticView {
    /// Latest verified PID identity, including PID-reuse defenses.
    pub process_identity: Option<ProcessIdentity>,
    /// Latest bounded process deltas and their observation times.
    pub process_activity: Vec<ActivitySampleRecord>,
    /// Provenance shared by the retained process delta samples.
    pub process_activity_provenance: Option<ObservationSource>,
    /// Trusted signal timestamps used by the current decision.
    pub signal_times: AgentSignalTimes,
    /// Latest bounded progress or active-operation summary.
    pub active_operation: Option<String>,
    /// Actionable material source-conflict summaries.
    pub source_conflicts: Vec<String>,
    /// Selected parent relation and its retained evidence.
    pub correlation: Option<AgentCorrelationView>,
    /// Bounded deterministic checks the parent can perform next.
    pub suggested_checks: Vec<String>,
}

/// Trusted timestamps included in parent diagnostics.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct AgentSignalTimes {
    /// Latest accepted reducer input.
    pub updated_at: TimePoint,
    /// Latest trustworthy progress signal.
    pub last_activity: TimePoint,
    /// Latest trustworthy state/progress transition, when present.
    pub last_trusted_transition: Option<TimePoint>,
    /// Latest retained process delta sample, when present.
    pub latest_process_sample: Option<WallTimeMs>,
}

/// Selected hierarchy evidence rendered explicitly for parent agents.
#[derive(Clone, Debug, Serialize)]
pub struct AgentCorrelationView {
    /// Strongest basis represented by the selected relation.
    pub basis: CorrelationBasis,
    /// Bounded source fingerprint supporting the selection.
    pub evidence: String,
    /// Trust assigned to the relation source.
    pub trust: EvidenceTrust,
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

/// Server-timestamped result of registering one capability-validated path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RegisteredWatchPathView {
    /// Server wall time used for response correlation.
    pub server_time: WallTimeMs,
    /// Durable registration, including ownership and caller provenance.
    pub registration: watchdog_store::RegisteredWatchPathRecord,
}

/// Agent-facing health needed to diagnose monitoring coverage.
#[derive(Clone, Debug, Serialize)]
pub struct AgentHealthView {
    /// Server wall time used for client-relative display and correlation.
    pub server_time: WallTimeMs,
    /// Whether the database uses WAL journaling.
    pub store_wal: bool,
    /// Whether foreign-key enforcement is active.
    pub store_foreign_keys: bool,
    /// Applied schema version.
    pub schema_version: i64,
    /// Per-runtime persisted health, including actionable warning messages.
    pub adapters: Vec<AdapterHealthRecord>,
}

/// Bounded result of one timer-reconciliation pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimerReconciliationReport {
    evaluated_sessions: usize,
    changed_sessions: usize,
}

impl TimerReconciliationReport {
    /// Number of retained sessions evaluated in this pass.
    #[must_use]
    pub const fn evaluated_sessions(self) -> usize {
        self.evaluated_sessions
    }

    /// Number of sessions whose durable timer state changed.
    #[must_use]
    pub const fn changed_sessions(self) -> usize {
        self.changed_sessions
    }
}

#[derive(Debug, Default)]
struct ScopeRegistry {
    roots: RwLock<HashMap<TransportKey, MainSessionId>>,
}

#[derive(Debug)]
struct SessionAdmission<T> {
    queue: SessionQueue<T>,
    draining: bool,
}

impl<T> SessionAdmission<T> {
    fn new(capacity: usize) -> Self {
        Self {
            queue: SessionQueue::new(capacity)
                .unwrap_or_else(|_| unreachable!("observation queue capacity is positive")),
            draining: false,
        }
    }

    fn push(&mut self, class: ObservationClass, value: T) -> Result<Option<T>, AdmissionError<T>> {
        self.queue.try_push_replacing(class, value)
    }

    fn pop(&mut self) -> Option<T> {
        self.queue.pop()
    }

    #[cfg(test)]
    fn is_degraded(&self) -> bool {
        self.queue.is_degraded()
    }

    fn reconcile_rejected(&mut self, durable_uncertainty_remaining: bool) -> bool {
        if durable_uncertainty_remaining || !self.queue.is_degraded() {
            return false;
        }
        self.queue.mark_reconciled();
        true
    }
}

#[derive(Debug)]
struct PendingObservation {
    observation: ObservationEnvelope,
    completion: oneshot::Sender<Result<ApplyResult, AgentApiError>>,
}

#[derive(Debug)]
struct SessionLane {
    record: StoredSessionRecord,
    coordinator: Mutex<SessionCoordinator>,
    admission: Mutex<SessionAdmission<PendingObservation>>,
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

    fn release(&self, transport: &TransportKey) {
        self.roots
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(transport);
    }
}

struct AgentApiInner {
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
    event_sequence: Arc<EventSequence>,
    policy: RwLock<ReducerPolicy>,
    scopes: ScopeRegistry,
    lanes: AsyncRwLock<HashMap<SessionId, Arc<SessionLane>>>,
    health: RwLock<Option<crate::HealthService>>,
    queue_uncertain: RwLock<BTreeSet<SessionIdentity>>,
    queue_health_transition: Mutex<()>,
    watch_paths: RwLock<Option<crate::watch_paths::WatchPathRegistry>>,
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
    pub(crate) fn event_sequence(&self) -> Arc<EventSequence> {
        Arc::clone(&self.inner.event_sequence)
    }

    pub(crate) fn release_transport_scope(&self, transport: &TransportKey) {
        self.inner.scopes.release(transport);
    }

    pub(crate) async fn discovered_session_identity(
        &self,
        session_id: SessionId,
    ) -> Result<Option<SessionIdentity>, AgentApiError> {
        Ok(self
            .inner
            .store
            .session_by_id(session_id)
            .await?
            .map(|record| record.session))
    }

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
        let queue_uncertain = store
            .observation_queue_uncertain_sessions()
            .await?
            .into_iter()
            .collect();
        Ok(Self {
            inner: Arc::new(AgentApiInner {
                store,
                clock,
                event_sequence,
                policy: RwLock::new(policy),
                scopes: ScopeRegistry::default(),
                lanes: AsyncRwLock::new(HashMap::new()),
                health: RwLock::new(None),
                queue_uncertain: RwLock::new(queue_uncertain),
                queue_health_transition: Mutex::new(()),
                watch_paths: RwLock::new(None),
            }),
        })
    }

    pub(crate) fn configure_watch_paths(&self, registry: crate::watch_paths::WatchPathRegistry) {
        *self
            .inner
            .watch_paths
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(registry);
    }

    pub(crate) fn configure_health(&self, health: crate::HealthService) {
        *self
            .inner
            .health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(health);
        for session in self
            .inner
            .queue_uncertain
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .copied()
        {
            self.record_queue_health(
                session,
                watchdog_runtime::ComponentStatus::Degraded,
                Some("Durable observation admission requires an exact retry"),
            );
        }
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
            lane.coordinator.lock().await.set_policy(policy);
        }
    }

    /// Register one additional capability-validated path for an in-scope session.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] for unbound/cross-tree targets, invalid paths,
    /// unavailable watcher registration, or persistence failure.
    pub async fn register_watch_path(
        &self,
        transport: &TransportKey,
        session_id: SessionId,
        event_key: &str,
        native_path: &str,
    ) -> Result<RegisteredWatchPathView, AgentApiError> {
        validate_event_key(event_key)?;
        let session = self.resolve_scoped(transport, session_id).await?;
        let registry = self
            .inner
            .watch_paths
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or(AgentApiError::WatchPathUnavailable)?;
        let registration = registry
            .register(session.session, session.root, event_key, native_path)
            .await
            .map_err(|error| match error {
                crate::watch_paths::WatchPathError::Rejected => AgentApiError::WatchPathRejected,
                crate::watch_paths::WatchPathError::Capacity
                | crate::watch_paths::WatchPathError::State => AgentApiError::WatchPathUnavailable,
                crate::watch_paths::WatchPathError::Store(source) => AgentApiError::Store(source),
            })?;
        Ok(RegisteredWatchPathView {
            server_time: self.inner.clock.now().wall_time(),
            registration,
        })
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
            lane.coordinator
                .lock()
                .await
                .apply_restarted(observation)
                .await?;
        }
        Ok(())
    }

    /// Clear the restart gate after an adapter has re-observed a current native
    /// session without claiming that the observation itself is progress.
    pub(crate) async fn mark_native_reconciled(
        &self,
        session_id: SessionId,
        adapter_version: &str,
        evidence_source: &str,
    ) -> Result<(), AgentApiError> {
        let record = self
            .inner
            .store
            .session_by_id(session_id)
            .await?
            .ok_or(AgentApiError::SessionNotFound)?;
        let now = self.inner.clock.now();
        let observation =
            reconciliation_observation(&record, now, adapter_version, evidence_source)?;
        let lane = self.lane(&record, now).await?;
        lane.coordinator
            .lock()
            .await
            .apply_reconciled(observation)
            .await?;
        Ok(())
    }

    /// Evaluate suspect, stall, and reminder timers for every retained session.
    ///
    /// No-op evaluations are kept entirely in memory and do not grow the
    /// observation ledger. Main sessions are included here; the separate
    /// termination API remains child-only by construction.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] when retained sessions cannot be listed or a
    /// due transition cannot be committed atomically.
    pub async fn reconcile_timers(&self) -> Result<TimerReconciliationReport, AgentApiError> {
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
        let mut changed_sessions = 0;
        for record in &sessions {
            let observation = scheduler_observation(record, now)?;
            let lane = self.lane(record, now).await?;
            if lane
                .coordinator
                .lock()
                .await
                .apply_tick(observation)
                .await?
                == ApplyResult::Applied
            {
                changed_sessions += 1;
            }
        }
        Ok(TimerReconciliationReport {
            evaluated_sessions: sessions.len(),
            changed_sessions,
        })
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
        if request.kind == SessionKind::Child {
            self.inner.scopes.bind_once(transport.clone(), root)?;
        }
        let provenance = RegistrationProvenance::Native {
            adapter_version: request.adapter_version,
            evidence_source: request.evidence_source,
        };
        let view = self
            .register_session_with_provenance(
                &transport,
                RegisterSession {
                    runtime: request.runtime,
                    native_id: request.native_id,
                    kind: request.kind,
                    parent: request.parent,
                    event_key: request.event_key,
                },
                provenance,
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

    /// Merge repository, branch, and optional pull-request metadata without
    /// changing runtime state or discovery provenance.
    ///
    /// # Errors
    ///
    /// Returns [`AgentApiError`] when the session metadata is absent, fields
    /// exceed their bounds, or persistence fails.
    pub async fn enrich_repository_metadata(
        &self,
        session: SessionIdentity,
        repository: RepositoryMetadata,
    ) -> Result<(), AgentApiError> {
        let existing = self
            .inner
            .store
            .session_metadata(session)
            .await?
            .ok_or(AgentApiError::SessionNotFound)?;
        let metadata = SessionMetadataRecord::new(
            session,
            existing.title().map(ToOwned::to_owned),
            existing.startup_directory().map(ToOwned::to_owned),
            repository
                .remote
                .or_else(|| existing.repository_remote().map(ToOwned::to_owned)),
            repository
                .branch
                .or_else(|| existing.branch().map(ToOwned::to_owned)),
            if repository.replace_pull_request {
                repository.pull_request_number
            } else {
                repository
                    .pull_request_number
                    .or(existing.pull_request_number())
            },
            if repository.replace_pull_request {
                repository.pull_request_url
            } else {
                repository
                    .pull_request_url
                    .or_else(|| existing.pull_request_url().map(ToOwned::to_owned))
            },
            self.inner.clock.now().wall_time(),
        )?;
        self.inner.store.save_session_metadata(&metadata).await?;
        Ok(())
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
        self.register_session_with_provenance(transport, request, RegistrationProvenance::Mcp)
            .await
    }

    async fn register_session_with_provenance(
        &self,
        transport: &TransportKey,
        request: RegisterSession,
        provenance: RegistrationProvenance,
    ) -> Result<SessionView, AgentApiError> {
        validate_event_key(&request.event_key)?;
        let native = NativeSessionKey::new(request.runtime, request.native_id)?;
        let session_id = SessionId::from_native(&native);
        let now = self.inner.clock.now();
        // Native identity and restorable transport IDs are caller-asserted. Alias
        // leasing therefore relies on the documented single-tenant bearer boundary.
        let alias_lease = self.inner.store.lease_discovery_alias(&native).await?;
        if request.kind == SessionKind::Main
            && let Some(record) = self
                .resolve_aliased_main(transport, alias_lease.resolution())
                .await?
        {
            drop(alias_lease);
            let observation =
                registration_observation(&record, &request.event_key, now, &provenance)?;
            self.apply_observation(&record, observation).await?;
            return self.view_for_record(&record).await;
        }
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
        if request.kind == SessionKind::Main {
            self.inner.scopes.bind_once(transport.clone(), root)?;
        }
        drop(alias_lease);
        let observation = registration_observation(&record, &request.event_key, now, &provenance)?;
        let result = self.apply_observation(&record, observation).await?;
        if let Some(parent) = parent
            && result == ApplyResult::Applied
        {
            self.save_relation(&record, &parent, &request.event_key, now, &provenance)
                .await?;
        }
        self.view_for_record(&record).await
    }

    async fn resolve_aliased_main(
        &self,
        transport: &TransportKey,
        resolution: DiscoveryAliasResolution,
    ) -> Result<Option<StoredSessionRecord>, AgentApiError> {
        let canonical = match resolution {
            DiscoveryAliasResolution::Absent => return Ok(None),
            DiscoveryAliasResolution::Unique(canonical) => canonical,
            DiscoveryAliasResolution::Ambiguous => {
                return Err(AgentApiError::SessionIdentityConflict);
            }
        };
        let record = self
            .inner
            .store
            .session_by_id(canonical)
            .await?
            .ok_or(AgentApiError::SessionIdentityConflict)?;
        let SessionIdentity::Main(main) = record.session else {
            return Err(AgentApiError::SessionIdentityConflict);
        };
        if main != record.root {
            return Err(AgentApiError::SessionIdentityConflict);
        }
        self.inner
            .scopes
            .bind_once(transport.clone(), record.root)?;
        Ok(Some(record))
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
        self.save_relation(
            &child,
            &parent,
            event_key,
            self.inner.clock.now(),
            &RegistrationProvenance::Mcp,
        )
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
        let (operation, payload) = match kind {
            WaitingKind::Agent => (
                "report_waiting",
                ObservationPayload::NativeState(DetailedState::WaitingForAgent),
            ),
            WaitingKind::Tool => (
                "report_waiting",
                ObservationPayload::NativeState(DetailedState::WaitingForTool),
            ),
            WaitingKind::User => (
                "report_waiting",
                ObservationPayload::NativeState(DetailedState::WaitingForUser),
            ),
            WaitingKind::Intentional => (
                "report_intentional_wait",
                ObservationPayload::IntentionalWait,
            ),
        };
        self.mutate_scoped(transport, session_id, operation, event_key, payload)
            .await
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
            server_time: self.inner.clock.now().wall_time(),
            store_wal: store.journal_mode == "wal",
            store_foreign_keys: store.foreign_keys,
            schema_version: store.schema_version,
            adapters,
        })
    }

    /// Read durable events after a caller-confirmed cursor.
    ///
    /// Passing `after` advances the stored acknowledgement monotonically. A
    /// successfully assembled page also stores `next_cursor` as the highest
    /// cursor that this root may acknowledge on a later call.
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
        let stored_cursor = durable.map_or(0, |offset| offset.last_event_id.value());
        let delivered_cursor = durable.map_or(0, |offset| offset.last_delivered_event_id.value());
        let cursor = after.map_or(stored_cursor, |confirmed| {
            confirmed.min(delivered_cursor).max(stored_cursor)
        });
        let domain_events = self
            .inner
            .store
            .events_after(root, watchdog_domain::EventId::new(cursor), limit)
            .await?;
        let next_cursor = domain_events
            .last()
            .map_or(cursor, |event| event.id().value());
        let mut events = Vec::with_capacity(domain_events.len());
        let mut views = HashMap::<SessionId, (SessionView, AgentDiagnosticView)>::new();
        let correlations = self.selected_correlations(root).await?;
        for event in domain_events {
            let subject = event.subject().session_id();
            let (session, diagnostics) = if let Some(cached) = views.get(&subject) {
                cached.clone()
            } else {
                let record = self
                    .inner
                    .store
                    .session_by_id(subject)
                    .await?
                    .ok_or(AgentApiError::SessionNotFound)?;
                let session = self.view_for_record(&record).await?;
                let correlation = match record.session {
                    SessionIdentity::Main(_) => None,
                    SessionIdentity::Child(child) => correlations.get(&child).cloned(),
                };
                let diagnostics = self
                    .diagnostics_for(&record, &session.snapshot, correlation)
                    .await?;
                views.insert(subject, (session.clone(), diagnostics.clone()));
                (session, diagnostics)
            };
            events.push(AgentEventView {
                event,
                session,
                diagnostics,
            });
        }
        let page = EventPage {
            after: cursor,
            next_cursor,
            events,
        };
        self.inner
            .store
            .save_inbox_offset(InboxOffsetRecord {
                parent: root,
                last_event_id: watchdog_domain::EventId::new(cursor),
                last_delivered_event_id: watchdog_domain::EventId::new(
                    delivered_cursor.max(next_cursor),
                ),
                updated_at: self.inner.clock.now().wall_time(),
            })
            .await?;
        Ok(page)
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
        observation: ObservationEnvelope,
    ) -> Result<ApplyResult, AgentApiError> {
        let lane = self.lane(record, observation.observed_at()).await?;
        let (completion, receiver) = oneshot::channel();
        let pending = PendingObservation {
            observation,
            completion,
        };
        let mut admission = lane.admission.lock().await;
        let class = observation_class(pending.observation.payload());
        let replaced = match admission.push(class, pending) {
            Ok(replaced) => replaced,
            Err(AdmissionError::Backpressure(pending)) => {
                self.persist_queue_rejection(record, &pending.observation)
                    .await?;
                self.record_queue_health(
                    record.session,
                    watchdog_runtime::ComponentStatus::Degraded,
                    Some("Durable observation admission queue is full"),
                );
                return Err(AgentApiError::ObservationBackpressure);
            }
            Err(AdmissionError::ActivitySaturated(pending)) => {
                self.persist_queue_rejection(record, &pending.observation)
                    .await?;
                self.record_queue_health(
                    record.session,
                    watchdog_runtime::ComponentStatus::Degraded,
                    Some("Activity observation admission queue is full"),
                );
                return Err(AgentApiError::ObservationActivitySaturated);
            }
        };
        if let Some(replaced) = replaced {
            let _ = replaced
                .completion
                .send(Err(AgentApiError::ObservationCoalesced));
        }
        let start_drainer = !admission.draining;
        admission.draining = true;
        drop(admission);
        if start_drainer {
            let api = self.clone();
            let lane = Arc::clone(&lane);
            tokio::spawn(async move { api.drain_observations(lane).await });
        }
        receiver
            .await
            .map_err(|_| AgentApiError::ObservationQueueStopped)?
    }

    async fn drain_observations(&self, lane: Arc<SessionLane>) {
        loop {
            let pending = {
                let mut admission = lane.admission.lock().await;
                let Some(pending) = admission.pop() else {
                    admission.draining = false;
                    return;
                };
                pending
            };
            let observation_id = pending.observation.observation_id();
            let mut result = self
                .apply_queued_observation(&lane, pending.observation)
                .await;
            if result.is_ok()
                && let Err(error) = self
                    .reconcile_rejected_observation(&lane, observation_id)
                    .await
            {
                result = Err(error);
            }
            let _ = pending.completion.send(result);
        }
    }

    async fn reconcile_rejected_observation(
        &self,
        lane: &SessionLane,
        observation_id: ObservationId,
    ) -> Result<(), AgentApiError> {
        let _transition = self.inner.queue_health_transition.lock().await;
        let uncertain = self
            .inner
            .queue_uncertain
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&lane.record.session);
        let mut durable_uncertainty_remaining = uncertain;
        if uncertain {
            let remaining = self
                .inner
                .store
                .clear_observation_queue_rejection(observation_id, lane.record.session)
                .await?;
            durable_uncertainty_remaining = remaining;
            if !remaining {
                self.inner
                    .queue_uncertain
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&lane.record.session);
                self.record_queue_health(
                    lane.record.session,
                    watchdog_runtime::ComponentStatus::Healthy,
                    None,
                );
            }
        }
        let mut admission = lane.admission.lock().await;
        if !admission.reconcile_rejected(durable_uncertainty_remaining) {
            return Ok(());
        }
        self.record_queue_health(
            lane.record.session,
            watchdog_runtime::ComponentStatus::Healthy,
            None,
        );
        Ok(())
    }

    async fn persist_queue_rejection(
        &self,
        record: &StoredSessionRecord,
        observation: &ObservationEnvelope,
    ) -> Result<(), AgentApiError> {
        let _transition = self.inner.queue_health_transition.lock().await;
        self.inner
            .queue_uncertain
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(record.session);
        self.record_queue_health(
            record.session,
            watchdog_runtime::ComponentStatus::Degraded,
            Some("Durable observation admission requires an exact retry"),
        );
        self.inner
            .store
            .save_observation_queue_rejection(
                observation.observation_id(),
                record.session,
                observation.observed_at().wall_time(),
            )
            .await?;
        Ok(())
    }

    async fn apply_queued_observation(
        &self,
        lane: &SessionLane,
        mut observation: ObservationEnvelope,
    ) -> Result<ApplyResult, AgentApiError> {
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
        let result = lane
            .coordinator
            .lock()
            .await
            .apply_observation(observation)
            .await?;
        Ok(result)
    }

    fn record_queue_health(
        &self,
        session: SessionIdentity,
        status: watchdog_runtime::ComponentStatus,
        message: Option<&str>,
    ) {
        if let Some(health) = self
            .inner
            .health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            health.record_scoped(
                watchdog_runtime::ComponentId::ObservationQueue,
                status,
                HealthScope::Session(session),
                message,
            );
        }
    }

    async fn lane(
        &self,
        record: &StoredSessionRecord,
        now: TimePoint,
    ) -> Result<Arc<SessionLane>, AgentApiError> {
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
        let lane = Arc::new(SessionLane {
            record: record.clone(),
            coordinator: Mutex::new(SessionCoordinator::new(
                self.inner.store.clone(),
                snapshot,
                *self
                    .inner
                    .policy
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                Arc::clone(&self.inner.event_sequence),
                [OutboxDestination::ParentInbox, OutboxDestination::Sse],
            )),
            admission: Mutex::new(SessionAdmission::new(OBSERVATION_QUEUE_CAPACITY)),
        });
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
        let provenance = match snapshot.last_observation_id() {
            Some(observation_id) => self
                .inner
                .store
                .observation(observation_id)
                .await?
                .map(|observation| observation.source().clone()),
            None => None,
        };
        Ok(SessionView {
            server_time: self.inner.clock.now().wall_time(),
            session: record.session,
            root: record.root,
            runtime: record.native.runtime(),
            native_id: record.native.native_id().to_owned(),
            provenance,
            snapshot,
        })
    }

    async fn diagnostics_for(
        &self,
        record: &StoredSessionRecord,
        snapshot: &SessionSnapshot,
        correlation: Option<AgentCorrelationView>,
    ) -> Result<AgentDiagnosticView, AgentApiError> {
        let process_activity = self
            .inner
            .store
            .recent_activity(record.session, DIAGNOSTIC_ACTIVITY_SAMPLES)
            .await?
            .into_iter()
            .filter(|sample| {
                matches!(
                    sample.evidence,
                    ActivityEvidence::ProcessCpu { .. } | ActivityEvidence::ProcessIo { .. }
                )
            })
            .collect::<Vec<_>>();
        let process_activity_provenance = if process_activity.is_empty() {
            None
        } else {
            Some(ObservationSource::new(
                AdapterIdentity::new(record.native.runtime(), "linux-procfs-v1")?,
                "process:tree-delta",
                EvidenceTrust::Corroborating,
                None,
            )?)
        };
        let source_conflicts = snapshot
            .source_conflict()
            .then(|| "Authoritative runtime and agent sources currently disagree".to_owned());
        let suggested_checks = suggested_checks(snapshot, &process_activity);
        Ok(AgentDiagnosticView {
            process_identity: snapshot.process_identity().cloned(),
            process_activity_provenance,
            signal_times: AgentSignalTimes {
                updated_at: snapshot.updated_at(),
                last_activity: snapshot.last_activity(),
                last_trusted_transition: snapshot.last_trusted_transition(),
                latest_process_sample: process_activity.first().map(|sample| sample.observed_at),
            },
            active_operation: snapshot.last_progress_summary().map(ToOwned::to_owned),
            source_conflicts: source_conflicts.into_iter().collect(),
            correlation,
            suggested_checks,
            process_activity,
        })
    }

    async fn selected_correlations(
        &self,
        root: MainSessionId,
    ) -> Result<HashMap<ChildSessionId, AgentCorrelationView>, AgentApiError> {
        let relations = self
            .inner
            .store
            .relations_for_root(root, MAX_TREE_SESSIONS)
            .await?
            .into_iter()
            .filter(|relation| relation.selected)
            .map(|relation| {
                (
                    relation.child,
                    AgentCorrelationView {
                        basis: relation.basis,
                        evidence: relation.provenance.fingerprint().to_owned(),
                        trust: relation.provenance.trust(),
                    },
                )
            })
            .collect();
        Ok(relations)
    }

    async fn save_relation(
        &self,
        child: &StoredSessionRecord,
        parent: &StoredSessionRecord,
        event_key: &str,
        now: TimePoint,
        provenance: &RegistrationProvenance,
    ) -> Result<(), AgentApiError> {
        let SessionIdentity::Child(child_id) = child.session else {
            return Err(AgentApiError::ChildSessionRequired);
        };
        let source = registration_source(&child.native, event_key, provenance, true)?;
        self.inner
            .store
            .select_relation(&RelationRecord {
                child: child_id,
                parent: parent.session,
                root: child.root,
                selected: true,
                basis: provenance.correlation_basis(),
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

fn registration_observation(
    record: &StoredSessionRecord,
    event_key: &str,
    now: TimePoint,
    provenance: &RegistrationProvenance,
) -> Result<ObservationEnvelope, AgentApiError> {
    let (namespace, source) = match provenance {
        RegistrationProvenance::Mcp => (
            "mcp:register_session",
            registration_source(&record.native, event_key, provenance, false)?,
        ),
        RegistrationProvenance::Native { .. } => (
            "native-discovery",
            registration_source(&record.native, event_key, provenance, false)?,
        ),
    };
    let native_event_id = format!("{}:{event_key}", record.session.session_id());
    Ok(ObservationEnvelope::new(
        ObservationId::from_native(record.native.runtime(), namespace, native_event_id)?,
        record.native.clone(),
        now,
        source,
        ObservationPayload::Progress(BoundedText::new(
            "progress",
            "Session registered with Agent Watchdog",
        )?),
    )?)
}

fn registration_source(
    native: &NativeSessionKey,
    event_key: &str,
    provenance: &RegistrationProvenance,
    relation: bool,
) -> Result<ObservationSource, DomainInputError> {
    let (version, fingerprint) = match provenance {
        RegistrationProvenance::Mcp => (
            "mcp-v1",
            if relation {
                format!("mcp:register_delegation:{event_key}")
            } else {
                "mcp:register_session".to_owned()
            },
        ),
        RegistrationProvenance::Native {
            adapter_version,
            evidence_source,
        } => (
            adapter_version.as_str(),
            if relation {
                format!("{evidence_source}:relation")
            } else {
                evidence_source.clone()
            },
        ),
    };
    ObservationSource::new(
        AdapterIdentity::new(native.runtime(), version)?,
        fingerprint,
        EvidenceTrust::Authoritative,
        None,
    )
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

fn scheduler_observation(
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
        ObservationId::from_native(record.native.runtime(), "scheduler-tick", event_key)?,
        record.native.clone(),
        now,
        ObservationSource::new(
            AdapterIdentity::new(record.native.runtime(), "agent-watchdog-scheduler-v1")?,
            "scheduler:tick",
            EvidenceTrust::Authoritative,
            None,
        )?,
        ObservationPayload::SchedulerTick,
    )?)
}

fn reconciliation_observation(
    record: &StoredSessionRecord,
    now: TimePoint,
    adapter_version: &str,
    evidence_source: &str,
) -> Result<ObservationEnvelope, AgentApiError> {
    let event_key = format!(
        "{}:{}:{}",
        record.session.session_id(),
        now.wall_time().value(),
        now.monotonic_ms()
    );
    Ok(ObservationEnvelope::new(
        ObservationId::from_native(record.native.runtime(), "native-reconciled", event_key)?,
        record.native.clone(),
        now,
        ObservationSource::new(
            AdapterIdentity::new(record.native.runtime(), adapter_version)?,
            evidence_source,
            EvidenceTrust::Corroborating,
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

fn suggested_checks(
    snapshot: &SessionSnapshot,
    process_activity: &[ActivitySampleRecord],
) -> Vec<String> {
    let mut checks = vec!["Ask the child agent for its current status and active operation".into()];
    if snapshot.process_identity().is_some() {
        checks.push("Verify the reported PID still has the same start time and executable".into());
    } else {
        checks.push("Confirm that the child process is still running and discoverable".into());
    }
    if process_activity.iter().any(|sample| {
        matches!(
            sample.evidence,
            ActivityEvidence::ProcessCpu {
                user_ticks: 0,
                system_ticks: 0,
                child_user_ticks: 0,
                child_system_ticks: 0,
            }
        )
    }) {
        checks
            .push("Inspect whether the child is blocked on a tool or long-running command".into());
    }
    if snapshot.source_conflict() {
        checks.push("Reconcile the conflicting runtime and agent status sources".into());
    }
    checks
}

fn validate_event_key(event_key: &str) -> Result<(), AgentApiError> {
    let key = BoundedText::<256>::new("event_key", event_key)?;
    if key.is_empty() {
        return Err(DomainInputError::Empty { field: "event_key" }.into());
    }
    Ok(())
}

const fn observation_class(payload: &ObservationPayload) -> ObservationClass {
    if matches!(payload, ObservationPayload::Progress(_)) {
        ObservationClass::Activity
    } else {
        ObservationClass::Durable
    }
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
    /// The supplied path is not a concrete allowlisted worktree directory.
    #[error("MCP watch path is outside the configured worktree roots")]
    WatchPathRejected,
    /// Watch-path registration is not available in this server process.
    #[error("MCP watch path registration is unavailable")]
    WatchPathUnavailable,
    /// Durable observation admission requires producer retry.
    #[error("Session observation queue is full; retry durable evidence")]
    ObservationBackpressure,
    /// Activity admission could not find a safe coalescing slot.
    #[error("Session activity queue is saturated; reconcile before retrying")]
    ObservationActivitySaturated,
    /// The session queue worker stopped before reporting an outcome.
    #[error("Session observation queue worker stopped")]
    ObservationQueueStopped,
    /// Newer activity displaced this coalescible observation before persistence.
    #[error("Session activity observation was coalesced by newer evidence")]
    ObservationCoalesced,
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

#[cfg(test)]
mod admission_tests {
    use std::{sync::Arc, time::Duration};

    use watchdog_domain::{
        Clock as _, DurationMs, MainSessionId, NativeSessionKey, RuntimeKind, SessionId,
        SessionIdentity, SessionKind, TimePoint, WallTimeMs,
    };
    use watchdog_runtime::{AdmissionError, ComponentId, ComponentStatus, ObservationClass};
    use watchdog_store::WatchdogStore;
    use watchdog_testkit::FakeClock;

    use super::{
        AgentApi, AgentApiError, RegisterSession, SessionAdmission, TransportKey, WaitingKind,
    };
    use crate::HealthService;

    #[test]
    fn production_lane_coalesces_activity_and_backpressures_durable_evidence() {
        let mut admission = SessionAdmission::new(2);
        assert_eq!(
            admission
                .push(ObservationClass::Activity, "activity-1")
                .expect("first activity should fit"),
            None
        );
        assert_eq!(
            admission
                .push(ObservationClass::Activity, "activity-2")
                .expect("new activity should coalesce"),
            Some("activity-1")
        );
        admission
            .push(ObservationClass::Durable, "waiting-user")
            .expect("durable evidence should use reserved capacity");
        assert_eq!(
            admission.push(ObservationClass::Durable, "failed"),
            Err(AdmissionError::Backpressure("failed"))
        );
        assert!(admission.is_degraded());
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the production queue contract is clearer as one end-to-end scenario"
    )]
    async fn live_agent_lane_bounds_coalesced_waiters_and_stays_degraded_until_reconciliation() {
        let directory = tempfile::tempdir().expect("fixture directory should exist");
        let store = WatchdogStore::open(&directory.path().join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(WallTimeMs::new(0), 0)));
        let api = AgentApi::new(store.clone(), clock.clone())
            .await
            .expect("API should initialize");
        let health = healthy_service(clock.clone());
        api.configure_health(health.clone());
        let transport = TransportKey::new("queue-test").expect("transport should validate");
        let session = api
            .register_session(
                &transport,
                RegisterSession {
                    runtime: RuntimeKind::ClaudeCode,
                    native_id: "queue-main".to_owned(),
                    kind: SessionKind::Main,
                    parent: None,
                    event_key: "register-main".to_owned(),
                },
            )
            .await
            .expect("session should register")
            .session;
        let record = store
            .session_by_id(session.session_id())
            .await
            .expect("session lookup should work")
            .expect("session should exist");
        let lane = api
            .lane(&record, clock.now())
            .await
            .expect("lane should exist");
        *lane.admission.lock().await = SessionAdmission::new(1);
        let coordinator = lane.coordinator.lock().await;

        let blocker = spawn_wait(
            api.clone(),
            transport.clone(),
            session.session_id(),
            "blocker",
        );
        wait_for_queue_len(&lane, 0).await;
        let old_activity = spawn_progress(
            api.clone(),
            transport.clone(),
            session.session_id(),
            "activity-1",
        );
        wait_for_queue_len(&lane, 1).await;
        let latest_activity = spawn_progress(
            api.clone(),
            transport.clone(),
            session.session_id(),
            "activity-2",
        );
        let old_activity_result = tokio::time::timeout(Duration::from_secs(1), old_activity)
            .await
            .expect("a displaced progress caller must not be retained without bound")
            .expect("old activity task should finish");
        assert!(matches!(
            api.report_waiting(
                &transport,
                session.session_id(),
                "rejected-durable",
                WaitingKind::User,
            )
            .await,
            Err(AgentApiError::ObservationBackpressure)
        ));
        assert!(!health.destructive_automation_allowed(RuntimeKind::ClaudeCode, session));
        let restarted = AgentApi::new(store.clone(), clock.clone())
            .await
            .expect("restarted API should initialize");
        let restarted_health = healthy_service(clock.clone());
        restarted.configure_health(restarted_health.clone());
        assert!(
            !restarted_health.destructive_automation_allowed(RuntimeKind::ClaudeCode, session),
            "durable admission uncertainty must survive a process restart"
        );
        let unaffected = SessionIdentity::Main(MainSessionId::from(SessionId::from_native(
            &NativeSessionKey::new(RuntimeKind::ClaudeCode, "queue-unaffected")
                .expect("unaffected identity should validate"),
        )));
        assert!(
            restarted_health.destructive_automation_allowed(RuntimeKind::ClaudeCode, unaffected),
            "recovered queue uncertainty must remain session-scoped"
        );

        drop(coordinator);
        blocker
            .await
            .expect("blocker task should finish")
            .expect("blocker observation should persist");
        assert!(matches!(
            old_activity_result,
            Err(AgentApiError::ObservationCoalesced)
        ));
        latest_activity
            .await
            .expect("latest activity task should finish")
            .expect("latest activity should persist");
        assert!(
            !health.destructive_automation_allowed(RuntimeKind::ClaudeCode, session),
            "draining accepted work cannot reconcile rejected durable evidence"
        );

        clock.advance(DurationMs::new(1));
        api.mark_native_reconciled(session.session_id(), "test", "test:reconcile")
            .await
            .expect("authoritative reconciliation should succeed");
        assert!(
            !health.destructive_automation_allowed(RuntimeKind::ClaudeCode, session),
            "unrelated native evidence cannot reconstruct a rejected MCP command"
        );
        api.report_waiting(
            &transport,
            session.session_id(),
            "rejected-durable",
            WaitingKind::User,
        )
        .await
        .expect("retrying the rejected durable observation should persist");
        assert!(health.destructive_automation_allowed(RuntimeKind::ClaudeCode, session));
    }

    fn healthy_service(clock: Arc<FakeClock>) -> HealthService {
        let health = HealthService::new(clock);
        for component in [
            ComponentId::Store,
            ComponentId::Watcher,
            ComponentId::FilesystemReconciliation,
            ComponentId::ObservationQueue,
            ComponentId::ProcessSampler,
            ComponentId::Adapter(RuntimeKind::ClaudeCode),
        ] {
            health.record(component, ComponentStatus::Healthy, None);
        }
        health
    }

    async fn wait_for_queue_len(lane: &super::SessionLane, expected: usize) {
        // Wall-clock polling lets contended background tasks make progress
        // without assuming a fixed number of cooperative yields is sufficient.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let admission = lane.admission.lock().await;
            if admission.draining && admission.queue.len() == expected {
                return;
            }
            drop(admission);
            assert!(
                tokio::time::Instant::now() < deadline,
                "queue did not reach expected length"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    fn spawn_wait(
        api: AgentApi,
        transport: TransportKey,
        session: watchdog_domain::SessionId,
        event_key: &'static str,
    ) -> tokio::task::JoinHandle<Result<super::SessionView, AgentApiError>> {
        tokio::spawn(async move {
            api.report_waiting(&transport, session, event_key, WaitingKind::Agent)
                .await
        })
    }

    fn spawn_progress(
        api: AgentApi,
        transport: TransportKey,
        session: watchdog_domain::SessionId,
        event_key: &'static str,
    ) -> tokio::task::JoinHandle<Result<super::SessionView, AgentApiError>> {
        tokio::spawn(async move {
            api.report_progress(&transport, session, event_key, event_key.to_owned(), None)
                .await
        })
    }
}
