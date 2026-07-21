//! Transactional `SQLite` persistence for Agent Watchdog.

mod records;
mod repositories;

pub use records::{
    ActivityEvidence, ActivitySampleRecord, AdapterHealthRecord, AdapterHealthStatus,
    DeadlineRecord, FileCursorRecord, InboxOffsetRecord, NotificationAttemptRecord,
    NotificationChannel, NotificationOutcome, RecordInputError, RegisteredWatchPathRecord,
    RelationRecord, SessionMetadataRecord, StoredSessionRecord, TerminationSafetyRecord,
    TerminationSagaRecord,
};
pub use watchdog_domain::{TerminationGate, TerminationStage};

use std::{path::Path, str::FromStr, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::to_vec;
use sqlx::{
    Row, SqlitePool,
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use thiserror::Error;
use watchdog_domain::{
    CompactState, DetailedState, DomainEvent, EventId, MainSessionId, ObservationEnvelope,
    SessionIdentity, SessionSnapshot, WallTimeMs,
};

static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");
const MAX_RECORD_BYTES: usize = 16_384;

/// One reducer snapshot update stored with an observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SnapshotUpdate {
    session: SessionIdentity,
    root: MainSessionId,
    revision: u64,
    state: DetailedState,
    updated_at: WallTimeMs,
    #[serde(skip_serializing_if = "Option::is_none")]
    reducer_snapshot: Option<SessionSnapshot>,
}

impl<'de> Deserialize<'de> for SnapshotUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawSnapshotUpdate {
            session: SessionIdentity,
            root: MainSessionId,
            revision: u64,
            state: DetailedState,
            updated_at: WallTimeMs,
            #[serde(default)]
            reducer_snapshot: Option<SessionSnapshot>,
        }

        let raw = RawSnapshotUpdate::deserialize(deserializer)?;
        let mut snapshot = Self::new(
            raw.session,
            raw.root,
            raw.revision,
            raw.state,
            raw.updated_at,
        )
        .map_err(de::Error::custom)?;
        if let Some(reducer_snapshot) = raw.reducer_snapshot {
            if reducer_snapshot.session() != snapshot.session
                || reducer_snapshot.root() != snapshot.root
                || reducer_snapshot.revision() != snapshot.revision
                || reducer_snapshot.state() != snapshot.state
                || reducer_snapshot.updated_at().wall_time() != snapshot.updated_at
            {
                return Err(de::Error::custom(
                    "Reducer snapshot metadata does not match",
                ));
            }
            snapshot.reducer_snapshot = Some(reducer_snapshot);
        }
        Ok(snapshot)
    }
}

impl SnapshotUpdate {
    /// Construct a positive-revision snapshot update.
    ///
    /// # Errors
    ///
    /// Returns [`StoreInputError`] when revision is zero or does not fit `SQLite`.
    pub fn new(
        session: SessionIdentity,
        root: MainSessionId,
        revision: u64,
        state: DetailedState,
        updated_at: WallTimeMs,
    ) -> Result<Self, StoreInputError> {
        positive_sqlite_integer("snapshot revision", revision)?;
        Ok(Self {
            session,
            root,
            revision,
            state,
            updated_at,
            reducer_snapshot: None,
        })
    }

    /// Construct a store projection that retains the complete reducer state.
    ///
    /// # Errors
    ///
    /// Returns [`StoreInputError`] when the reducer revision is zero or cannot
    /// fit `SQLite`.
    pub fn from_reducer(snapshot: &SessionSnapshot) -> Result<Self, StoreInputError> {
        let mut update = Self::new(
            snapshot.session(),
            snapshot.root(),
            snapshot.revision(),
            snapshot.state(),
            snapshot.updated_at().wall_time(),
        )?;
        update.reducer_snapshot = Some(snapshot.clone());
        Ok(update)
    }

    /// Role-preserving session identity.
    #[must_use]
    pub const fn session(&self) -> SessionIdentity {
        self.session
    }

    /// Main-session tree containing the snapshot.
    #[must_use]
    pub const fn root(&self) -> MainSessionId {
        self.root
    }

    /// Monotonically increasing reducer revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Detailed normalized session state.
    #[must_use]
    pub const fn state(&self) -> DetailedState {
        self.state
    }

    /// Persistable snapshot update time.
    #[must_use]
    pub const fn updated_at(&self) -> WallTimeMs {
        self.updated_at
    }

    /// Complete pure reducer state, when produced by the coordinator.
    #[must_use]
    pub const fn reducer_snapshot(&self) -> Option<&SessionSnapshot> {
        self.reducer_snapshot.as_ref()
    }
}

/// Durable delivery fan-out target.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OutboxDestination {
    /// Parent agent's durable MCP inbox.
    ParentInbox,
    /// Live server-sent event consumers.
    Sse,
    /// Browser notification center.
    Browser,
    /// Home Assistant webhook.
    HomeAssistant,
    /// Generic operator webhook.
    Webhook,
}

impl OutboxDestination {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ParentInbox => "parent_inbox",
            Self::Sse => "sse",
            Self::Browser => "browser",
            Self::HomeAssistant => "home_assistant",
            Self::Webhook => "webhook",
        }
    }
}

/// Atomic persistence input produced by the coordinator.
#[derive(Clone, Debug)]
pub struct ApplyObservation {
    observation: ObservationEnvelope,
    snapshot: SnapshotUpdate,
    events: Vec<RoutedEvent>,
}

/// One durable event paired with its exact fan-out destinations.
#[derive(Clone, Debug)]
pub struct RoutedEvent {
    event: DomainEvent,
    destinations: Vec<OutboxDestination>,
}

impl RoutedEvent {
    /// Pair one event with its bounded delivery routes.
    #[must_use]
    pub fn new(
        event: DomainEvent,
        destinations: impl IntoIterator<Item = OutboxDestination>,
    ) -> Self {
        Self {
            event,
            destinations: destinations.into_iter().collect(),
        }
    }
}

/// Atomic child-termination saga transition and delivery fan-out.
#[derive(Clone, Debug)]
pub struct TerminationAdvance {
    saga: TerminationSagaRecord,
    event: DomainEvent,
    destinations: Vec<OutboxDestination>,
}

impl TerminationAdvance {
    /// Construct one atomic saga/event persistence unit.
    #[must_use]
    pub fn new(
        saga: TerminationSagaRecord,
        event: DomainEvent,
        destinations: impl IntoIterator<Item = OutboxDestination>,
    ) -> Self {
        Self {
            saga,
            event,
            destinations: destinations.into_iter().collect(),
        }
    }
}

impl ApplyObservation {
    /// Construct one atomic reducer/store unit.
    #[must_use]
    pub fn new(
        observation: ObservationEnvelope,
        snapshot: SnapshotUpdate,
        events: Vec<DomainEvent>,
        destinations: impl IntoIterator<Item = OutboxDestination>,
    ) -> Self {
        let destinations = destinations.into_iter().collect::<Vec<_>>();
        Self {
            observation,
            snapshot,
            events: events
                .into_iter()
                .map(|event| RoutedEvent::new(event, destinations.iter().copied()))
                .collect(),
        }
    }

    /// Construct one atomic reducer/store unit with event-specific routes.
    #[must_use]
    pub fn routed(
        observation: ObservationEnvelope,
        snapshot: SnapshotUpdate,
        events: impl IntoIterator<Item = RoutedEvent>,
    ) -> Self {
        Self {
            observation,
            snapshot,
            events: events.into_iter().collect(),
        }
    }
}

/// Whether an observation mutated durable state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyResult {
    /// The observation and all derived records committed.
    Applied,
    /// The observation identity had already committed.
    Duplicate,
    /// A transient internal evaluation produced no durable state change.
    NoChange,
}

/// `SQLite` settings and migration status used by readiness checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreHealth {
    /// Active journal mode.
    pub journal_mode: String,
    /// Whether the inspected connection enforces foreign keys.
    pub foreign_keys: bool,
    /// Highest successfully applied embedded migration.
    pub schema_version: i64,
    /// Number of application tables from the embedded schema.
    pub application_table_count: i64,
}

/// Counts used by storage acceptance tests and administrative diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreCounts {
    /// Stored idempotent observations.
    pub observations: i64,
    /// Current session snapshots.
    pub snapshots: i64,
    /// Durable domain events.
    pub events: i64,
    /// Delivery fan-out records.
    pub outbox: i64,
}

/// One undelivered outbox entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingOutboxEntry {
    outbox_id: i64,
    event_id: EventId,
    destination: String,
    payload: Vec<u8>,
}

impl PendingOutboxEntry {
    /// Durable event represented by this delivery.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Stable fan-out target name.
    #[must_use]
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Bounded serialized event payload.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Store-assigned delivery identity.
    #[must_use]
    pub const fn outbox_id(&self) -> i64 {
        self.outbox_id
    }
}

/// Open migrated Agent Watchdog database.
#[derive(Clone, Debug)]
pub struct WatchdogStore {
    pub(crate) pool: SqlitePool,
}

impl WatchdogStore {
    /// Open a file-backed database, configure safety pragmas, and migrate it.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when options, connection, or migrations fail.
    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::from_str("sqlite:")?
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    /// Load the first unallocated durable event identity for process startup.
    ///
    /// This must be called once before session coordinator lanes are exposed;
    /// all lanes then share one in-process allocator.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt values, exhaustion, or `SQLite`
    /// failure.
    pub async fn first_unallocated_event_id(&self) -> Result<u64, StoreError> {
        let highest: Option<i64> =
            sqlx::query_scalar("SELECT MAX(event_id) FROM state_transitions")
                .fetch_one(&self.pool)
                .await?;
        let highest = highest.unwrap_or(0);
        if highest < 0 {
            return Err(StoreError::CorruptValue("negative event ID"));
        }
        let next = highest
            .checked_add(1)
            .ok_or(StoreInputError::IntegerRange { field: "event ID" })?;
        u64::try_from(next).map_err(|_| StoreError::CorruptValue("negative event ID"))
    }

    /// Load the highest committed durable event ID, or zero for an empty store.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt values or `SQLite` failure.
    pub async fn latest_event_id(&self) -> Result<EventId, StoreError> {
        let highest: Option<i64> =
            sqlx::query_scalar("SELECT MAX(event_id) FROM state_transitions")
                .fetch_one(&self.pool)
                .await?;
        let highest = highest.unwrap_or(0);
        Ok(EventId::new(u64::try_from(highest).map_err(|_| {
            StoreError::CorruptValue("negative event ID")
        })?))
    }

    /// Atomically persist an observation, snapshot, events, and delivery rows.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid cross-record identity, stale revisions,
    /// oversized serialization, or any `SQLite` failure. Failed transactions leave
    /// no partial records visible.
    pub async fn apply_observation(
        &self,
        apply: &ApplyObservation,
    ) -> Result<ApplyResult, StoreError> {
        validate_apply(apply)?;
        let mut transaction = self.pool.begin().await?;
        if !insert_observation(&mut transaction, &apply.observation).await? {
            transaction.commit().await?;
            return Ok(ApplyResult::Duplicate);
        }
        upsert_session(&mut transaction, apply).await?;
        upsert_snapshot(&mut transaction, &apply.snapshot).await?;
        insert_routed_events(&mut transaction, &apply.events).await?;
        transaction.commit().await?;
        Ok(ApplyResult::Applied)
    }

    /// Atomically persist one monotonic termination saga revision, its durable
    /// event, and delivery fan-out.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for cross-session identity, stale/invalid
    /// revisions, oversized records, event conflicts, or `SQLite` failure.
    pub async fn apply_termination_advance(
        &self,
        advance: &TerminationAdvance,
    ) -> Result<(), StoreError> {
        if advance.event.subject() != SessionIdentity::Child(advance.saga.child) {
            return Err(StoreError::IdentityMismatch);
        }
        let revision = positive_sqlite_integer("termination saga revision", advance.saga.revision)?;
        let payload = bounded_json(&advance.saga, "termination saga")?;
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "INSERT INTO termination_sagas \
             (child_session_id, stage, revision, next_action_at_ms, safety_json) \
             VALUES (?, ?, ?, ?, ?) ON CONFLICT(child_session_id) DO UPDATE SET \
             stage = excluded.stage, revision = excluded.revision, \
             next_action_at_ms = excluded.next_action_at_ms, safety_json = excluded.safety_json \
             WHERE excluded.revision > termination_sagas.revision",
        )
        .bind(advance.saga.child.session_id().to_string())
        .bind(advance.saga.stage.as_str())
        .bind(revision)
        .bind(
            advance
                .saga
                .next_action_at
                .map(watchdog_domain::WallTimeMs::value),
        )
        .bind(payload)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::StaleSnapshot);
        }
        insert_events(
            &mut transaction,
            std::slice::from_ref(&advance.event),
            &advance.destinations,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Read undelivered entries in stable store order.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the limit is invalid or `SQLite` fails.
    pub async fn pending_outbox(&self, limit: u32) -> Result<Vec<PendingOutboxEntry>, StoreError> {
        self.pending_outbox_matching(None, limit).await
    }

    /// Read undelivered entries for one fan-out destination in stable order.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the limit is invalid or `SQLite` fails.
    pub async fn pending_outbox_for(
        &self,
        destination: OutboxDestination,
        limit: u32,
    ) -> Result<Vec<PendingOutboxEntry>, StoreError> {
        self.pending_outbox_matching(Some(destination), limit).await
    }

    async fn pending_outbox_matching(
        &self,
        destination: Option<OutboxDestination>,
        limit: u32,
    ) -> Result<Vec<PendingOutboxEntry>, StoreError> {
        if limit == 0 {
            return Err(StoreInputError::ZeroLimit.into());
        }
        let destination = destination.map(OutboxDestination::as_str);
        let rows = sqlx::query(
            "SELECT outbox_id, event_id, destination, payload_json FROM outbox \
             WHERE delivered_at_ms IS NULL AND (? IS NULL OR destination = ?) \
             ORDER BY outbox_id LIMIT ?",
        )
        .bind(destination)
        .bind(destination)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let raw_event_id: i64 = row.try_get("event_id")?;
                Ok(PendingOutboxEntry {
                    outbox_id: row.try_get("outbox_id")?,
                    event_id: EventId::new(
                        u64::try_from(raw_event_id)
                            .map_err(|_| StoreError::CorruptValue("negative event ID"))?,
                    ),
                    destination: row.try_get("destination")?,
                    payload: row.try_get("payload_json")?,
                })
            })
            .collect()
    }

    /// Mark one pending delivery complete exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when `SQLite` cannot update the row.
    pub async fn acknowledge_outbox(
        &self,
        outbox_id: i64,
        delivered_at: WallTimeMs,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "UPDATE outbox SET delivered_at_ms = ? \
             WHERE outbox_id = ? AND delivered_at_ms IS NULL",
        )
        .bind(delivered_at.value())
        .bind(outbox_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Delete only Agent Watchdog application data while retaining migrations.
    ///
    /// This operation issues only fixed SQL against embedded table names. It
    /// never accepts or follows a native runtime path.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the transactional wipe fails.
    pub async fn wipe_watchdog_data(&self) -> Result<(), StoreError> {
        let mut transaction = self.pool.begin().await?;
        for statement in [
            "DELETE FROM notification_attempts",
            "DELETE FROM inbox_offsets",
            "DELETE FROM outbox",
            "DELETE FROM termination_sagas",
            "DELETE FROM deadlines",
            "DELETE FROM file_cursors",
            "DELETE FROM activity_samples",
            "DELETE FROM observation_queue_rejections",
            "DELETE FROM observation_queue_rejection_overflow",
            "DELETE FROM registered_watch_paths",
            "DELETE FROM state_transitions",
            "DELETE FROM session_snapshots",
            "DELETE FROM observations",
            "DELETE FROM session_relations",
            "DELETE FROM adapter_health",
            "DELETE FROM sessions",
            "DELETE FROM sqlite_sequence WHERE name IN ('activity_samples', 'outbox', 'notification_attempts')",
        ] {
            sqlx::query(statement).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Return readiness-relevant database settings and migration version.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when `SQLite` cannot answer a health query.
    pub async fn health(&self) -> Result<StoreHealth, StoreError> {
        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&self.pool)
            .await?;
        let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&self.pool)
            .await?;
        let schema_version: Option<i64> =
            sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations WHERE success = 1")
                .fetch_one(&self.pool)
                .await?;
        let application_table_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_schema \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name != '_sqlx_migrations'",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(StoreHealth {
            journal_mode,
            foreign_keys: foreign_keys == 1,
            schema_version: schema_version.unwrap_or(0),
            application_table_count,
        })
    }

    /// Return core record counts for diagnostics.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when `SQLite` cannot answer the query.
    pub async fn counts(&self) -> Result<StoreCounts, StoreError> {
        let row = sqlx::query(
            "SELECT \
             (SELECT COUNT(*) FROM observations) AS observations, \
             (SELECT COUNT(*) FROM session_snapshots) AS snapshots, \
             (SELECT COUNT(*) FROM state_transitions) AS events, \
             (SELECT COUNT(*) FROM outbox) AS outbox",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(StoreCounts {
            observations: row.try_get("observations")?,
            snapshots: row.try_get("snapshots")?,
            events: row.try_get("events")?,
            outbox: row.try_get("outbox")?,
        })
    }
}

/// Invalid typed input at the `SQLite` boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StoreInputError {
    /// A positive ordered value was zero.
    #[error("{field} must be positive")]
    Zero {
        /// Stable field description without input content.
        field: &'static str,
    },
    /// A value cannot be represented by `SQLite`'s signed integer.
    #[error("{field} exceeds SQLite integer range")]
    IntegerRange {
        /// Stable field description without input content.
        field: &'static str,
    },
    /// An outbox query requested no rows.
    #[error("Outbox limit must be positive")]
    ZeroLimit,
}

/// Persistence or boundary validation failure.
#[derive(Debug, Error)]
pub enum StoreError {
    /// `SQLx` connection, statement, or transaction failure.
    #[error("SQLite operation failed")]
    Sqlx(#[from] sqlx::Error),
    /// Embedded migration failure.
    #[error("SQLite migration failed")]
    Migration(#[from] sqlx::migrate::MigrateError),
    /// Invalid typed store input.
    #[error(transparent)]
    Input(#[from] StoreInputError),
    /// A serialized bounded record exceeded its storage cap.
    #[error("Serialized {record} exceeds {MAX_RECORD_BYTES} bytes")]
    RecordTooLarge {
        /// Stable record kind without content.
        record: &'static str,
    },
    /// Serialization of a typed bounded record failed.
    #[error("Typed record serialization failed")]
    Serialization(#[from] serde_json::Error),
    /// A new observation attempted to apply a non-increasing revision.
    #[error("Snapshot revision must increase")]
    StaleSnapshot,
    /// One idempotency key was reused for materially different content.
    #[error("Observation identity was reused with different content")]
    ObservationIdentityConflict,
    /// One watch-path event key was reused for materially different content.
    #[error("Watch-path event identity was reused with different content")]
    WatchPathIdentityConflict,
    /// Cross-record identities do not describe one session tree.
    #[error("Observation, snapshot, and event identities do not agree")]
    IdentityMismatch,
    /// A database value violates a schema invariant.
    #[error("Database contains invalid value: {0}")]
    CorruptValue(&'static str),
}

async fn insert_observation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    observation: &ObservationEnvelope,
) -> Result<bool, StoreError> {
    let observation_json = bounded_json(observation, "observation")?;
    let result = sqlx::query(
        "INSERT OR IGNORE INTO observations \
         (observation_id, runtime, native_id, observed_at_ms, monotonic_ms, envelope_json) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(observation.observation_id().to_string())
    .bind(observation.subject().runtime().as_str())
    .bind(observation.subject().native_id())
    .bind(observation.observed_at().wall_time().value())
    .bind(sqlite_integer(
        "observation monotonic time",
        observation.observed_at().monotonic_ms(),
    )?)
    .bind(&observation_json)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() == 1 {
        return Ok(true);
    }
    let stored: Vec<u8> =
        sqlx::query_scalar("SELECT envelope_json FROM observations WHERE observation_id = ?")
            .bind(observation.observation_id().to_string())
            .fetch_one(&mut **transaction)
            .await?;
    if stored != observation_json {
        return Err(StoreError::ObservationIdentityConflict);
    }
    Ok(false)
}

async fn upsert_session(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    apply: &ApplyObservation,
) -> Result<(), StoreError> {
    let session_id = apply.snapshot.session.session_id().to_string();
    let root_id = apply.snapshot.root.session_id().to_string();
    sqlx::query(
        "INSERT INTO sessions \
         (session_id, kind, root_session_id, runtime, native_id, created_at_ms, updated_at_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(session_id) DO UPDATE SET \
         root_session_id = excluded.root_session_id, runtime = excluded.runtime, \
         native_id = excluded.native_id, updated_at_ms = excluded.updated_at_ms",
    )
    .bind(session_id)
    .bind(session_kind(apply.snapshot.session))
    .bind(root_id)
    .bind(apply.observation.subject().runtime().as_str())
    .bind(apply.observation.subject().native_id())
    .bind(apply.snapshot.updated_at.value())
    .bind(apply.snapshot.updated_at.value())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn upsert_snapshot(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    snapshot: &SnapshotUpdate,
) -> Result<(), StoreError> {
    let snapshot_json = bounded_json(snapshot, "snapshot")?;
    let result = sqlx::query(
        "INSERT INTO session_snapshots \
         (session_id, root_session_id, revision, detailed_state, compact_state, updated_at_ms, snapshot_json) \
         VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(session_id) DO UPDATE SET \
         root_session_id = excluded.root_session_id, revision = excluded.revision, \
         detailed_state = excluded.detailed_state, compact_state = excluded.compact_state, \
         updated_at_ms = excluded.updated_at_ms, snapshot_json = excluded.snapshot_json \
         WHERE excluded.revision > session_snapshots.revision",
    )
    .bind(snapshot.session.session_id().to_string())
    .bind(snapshot.root.session_id().to_string())
    .bind(sqlite_integer("snapshot revision", snapshot.revision)?)
    .bind(detailed_state_name(snapshot.state))
    .bind(compact_state_name(snapshot.state.compact()))
    .bind(snapshot.updated_at.value())
    .bind(snapshot_json)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(StoreError::StaleSnapshot);
    }
    Ok(())
}

async fn insert_events(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    events: &[DomainEvent],
    destinations: &[OutboxDestination],
) -> Result<(), StoreError> {
    for event in events {
        let event_json = bounded_json(event, "event")?;
        let event_id = sqlite_integer("event ID", event.id().value())?;
        sqlx::query(
            "INSERT INTO state_transitions \
             (event_id, root_session_id, subject_session_id, occurred_at_ms, event_json) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(event_id)
        .bind(event.root().session_id().to_string())
        .bind(event.subject().session_id().to_string())
        .bind(event.occurred_at().value())
        .bind(&event_json)
        .execute(&mut **transaction)
        .await?;
        for destination in destinations {
            sqlx::query(
                "INSERT INTO outbox (event_id, destination, payload_json) VALUES (?, ?, ?)",
            )
            .bind(event_id)
            .bind(destination.as_str())
            .bind(&event_json)
            .execute(&mut **transaction)
            .await?;
        }
    }
    Ok(())
}

async fn insert_routed_events(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    events: &[RoutedEvent],
) -> Result<(), StoreError> {
    for routed in events {
        insert_events(
            transaction,
            std::slice::from_ref(&routed.event),
            &routed.destinations,
        )
        .await?;
    }
    Ok(())
}

fn validate_apply(apply: &ApplyObservation) -> Result<(), StoreError> {
    let observed_session = watchdog_domain::SessionId::from_native(apply.observation.subject());
    if observed_session != apply.snapshot.session.session_id()
        || apply.events.iter().any(|event| {
            event.event.root() != apply.snapshot.root
                || event.event.subject() != apply.snapshot.session
        })
    {
        return Err(StoreError::IdentityMismatch);
    }
    Ok(())
}

fn bounded_json<T: serde::Serialize>(
    value: &T,
    record: &'static str,
) -> Result<Vec<u8>, StoreError> {
    let serialized = to_vec(value)?;
    if serialized.len() > MAX_RECORD_BYTES {
        return Err(StoreError::RecordTooLarge { record });
    }
    Ok(serialized)
}

fn sqlite_integer(field: &'static str, value: u64) -> Result<i64, StoreInputError> {
    i64::try_from(value).map_err(|_| StoreInputError::IntegerRange { field })
}

fn positive_sqlite_integer(field: &'static str, value: u64) -> Result<i64, StoreInputError> {
    if value == 0 {
        return Err(StoreInputError::Zero { field });
    }
    sqlite_integer(field, value)
}

const fn session_kind(session: SessionIdentity) -> &'static str {
    match session {
        SessionIdentity::Main(_) => "main",
        SessionIdentity::Child(_) => "child",
    }
}

const fn detailed_state_name(state: DetailedState) -> &'static str {
    match state {
        DetailedState::Starting => "starting",
        DetailedState::Running => "running",
        DetailedState::WaitingForAgent => "waiting_for_agent",
        DetailedState::WaitingForTool => "waiting_for_tool",
        DetailedState::WaitingForUser => "waiting_for_user",
        DetailedState::Idle => "idle",
        DetailedState::Stalled => "stalled",
        DetailedState::Completed => "completed",
        DetailedState::Failed => "failed",
        DetailedState::Cancelled => "cancelled",
        DetailedState::Disappeared => "disappeared",
        DetailedState::Unknown => "unknown",
    }
}

const fn compact_state_name(state: CompactState) -> &'static str {
    match state {
        CompactState::Active => "active",
        CompactState::Waiting => "waiting",
        CompactState::Idle => "idle",
        CompactState::Stalled => "stalled",
        CompactState::Finished => "finished",
        CompactState::Failed => "failed",
        CompactState::Unknown => "unknown",
    }
}
