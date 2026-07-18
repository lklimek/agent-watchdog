use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;
use watchdog_domain::{
    AdapterIdentity, BoundedText, ChildSessionId, CorrelationBasis, EventId, MainSessionId,
    ObservationId, ObservationSource, ProcessIdentity, SessionIdentity, TerminationActionOutcome,
    TerminationBlocker, TerminationGate, TerminationStage, WallTimeMs,
};

/// Stable native identity and tree placement for one stored session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredSessionRecord {
    /// Role-preserving Watchdog identity.
    pub session: SessionIdentity,
    /// Owning main-session tree.
    pub root: MainSessionId,
    /// Runtime-qualified native identity.
    pub native: watchdog_domain::NativeSessionKey,
}

/// Bounded operator-facing session metadata retained separately from state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionMetadataRecord {
    session: SessionIdentity,
    title: Option<BoundedText<512>>,
    startup_directory: Option<BoundedText<4_096>>,
    repository_remote: Option<BoundedText<2_048>>,
    branch: Option<BoundedText<512>>,
    pull_request_number: Option<u64>,
    pull_request_url: Option<BoundedText<2_048>>,
    updated_at: WallTimeMs,
}

impl SessionMetadataRecord {
    /// Construct bounded metadata after path capability validation and optional
    /// GitHub enrichment.
    ///
    /// # Errors
    ///
    /// Returns [`watchdog_domain::DomainInputError`] for an oversized field.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: SessionIdentity,
        title: Option<String>,
        startup_directory: Option<String>,
        repository_remote: Option<String>,
        branch: Option<String>,
        pull_request_number: Option<u64>,
        pull_request_url: Option<String>,
        updated_at: WallTimeMs,
    ) -> Result<Self, watchdog_domain::DomainInputError> {
        Ok(Self {
            session,
            title: optional_bounded("session_title", title)?,
            startup_directory: optional_bounded("startup_directory", startup_directory)?,
            repository_remote: optional_bounded("repository_remote", repository_remote)?,
            branch: optional_bounded("branch", branch)?,
            pull_request_number,
            pull_request_url: optional_bounded("pull_request_url", pull_request_url)?,
            updated_at,
        })
    }

    /// Session receiving this metadata.
    #[must_use]
    pub const fn session(&self) -> SessionIdentity {
        self.session
    }

    /// Native title, when available.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_ref().map(BoundedText::as_str)
    }

    /// Directory in which the user started the runtime command.
    #[must_use]
    pub fn startup_directory(&self) -> Option<&str> {
        self.startup_directory.as_ref().map(BoundedText::as_str)
    }

    /// Git repository remote used for optional PR enrichment.
    #[must_use]
    pub fn repository_remote(&self) -> Option<&str> {
        self.repository_remote.as_ref().map(BoundedText::as_str)
    }

    /// Current Git branch, when detected.
    #[must_use]
    pub fn branch(&self) -> Option<&str> {
        self.branch.as_ref().map(BoundedText::as_str)
    }

    /// Enriched GitHub pull request number.
    #[must_use]
    pub const fn pull_request_number(&self) -> Option<u64> {
        self.pull_request_number
    }

    /// Validated GitHub pull request URL.
    #[must_use]
    pub fn pull_request_url(&self) -> Option<&str> {
        self.pull_request_url.as_ref().map(BoundedText::as_str)
    }

    /// Freshest metadata observation time.
    #[must_use]
    pub const fn updated_at(&self) -> WallTimeMs {
        self.updated_at
    }
}

/// Durable capability-validated native path registered by one scoped agent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegisteredWatchPathRecord {
    session: SessionIdentity,
    root: MainSessionId,
    event_key: BoundedText<512>,
    native_path: BoundedText<4_096>,
    registered_at: WallTimeMs,
}

impl RegisteredWatchPathRecord {
    /// Construct one bounded watch-path registration.
    ///
    /// # Errors
    ///
    /// Returns [`watchdog_domain::DomainInputError`] for empty or oversized text.
    pub fn new(
        session: SessionIdentity,
        root: MainSessionId,
        event_key: impl Into<String>,
        native_path: impl Into<String>,
        registered_at: WallTimeMs,
    ) -> Result<Self, watchdog_domain::DomainInputError> {
        let event_key = BoundedText::new("watch_path_event_key", event_key)?;
        let native_path = BoundedText::new("registered_watch_path", native_path)?;
        if event_key.is_empty() {
            return Err(watchdog_domain::DomainInputError::Empty {
                field: "watch_path_event_key",
            });
        }
        if native_path.is_empty() {
            return Err(watchdog_domain::DomainInputError::Empty {
                field: "registered_watch_path",
            });
        }
        Ok(Self {
            session,
            root,
            event_key,
            native_path,
            registered_at,
        })
    }

    /// Session that owns this registration.
    #[must_use]
    pub const fn session(&self) -> SessionIdentity {
        self.session
    }

    /// Main tree containing the session.
    #[must_use]
    pub const fn root(&self) -> MainSessionId {
        self.root
    }

    /// Caller idempotency key.
    #[must_use]
    pub fn event_key(&self) -> &str {
        self.event_key.as_str()
    }

    /// Capability-validated runtime-native path.
    #[must_use]
    pub fn native_path(&self) -> &str {
        self.native_path.as_str()
    }

    /// Durable registration time.
    #[must_use]
    pub const fn registered_at(&self) -> WallTimeMs {
        self.registered_at
    }
}

fn optional_bounded<const N: usize>(
    field: &'static str,
    value: Option<String>,
) -> Result<Option<BoundedText<N>>, watchdog_domain::DomainInputError> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| BoundedText::new(field, value))
        .transpose()
}

/// Selected or candidate session hierarchy relation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RelationRecord {
    /// Child whose parent is being correlated.
    pub child: ChildSessionId,
    /// Candidate or selected parent.
    pub parent: SessionIdentity,
    /// Main-session tree containing the relation.
    pub root: MainSessionId,
    /// Whether this candidate currently owns the relation.
    pub selected: bool,
    /// Strongest bounded correlation basis represented by the evidence.
    #[serde(default = "legacy_relation_basis")]
    pub basis: CorrelationBasis,
    /// Evidence supporting the relation.
    pub provenance: ObservationSource,
    /// Inclusive validity start.
    pub valid_from: WallTimeMs,
    /// Exclusive validity end, when superseded.
    pub valid_until: Option<WallTimeMs>,
}

const fn legacy_relation_basis() -> CorrelationBasis {
    CorrelationBasis::WeakHeuristic
}

/// Attributable activity evidence retained for restart diagnostics.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActivitySampleRecord {
    /// Session receiving the activity.
    pub session: SessionIdentity,
    /// Persistable observation time.
    pub observed_at: WallTimeMs,
    /// Typed bounded activity evidence.
    pub evidence: ActivityEvidence,
}

/// Activity signals that can corroborate progress without transcript storage.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ActivityEvidence {
    /// Runtime-native progress event.
    NativeProgress,
    /// Attributable worktree change identified by a bounded target key.
    Filesystem {
        /// Watch-service target identity, never file content.
        target: BoundedText<512>,
    },
    /// Linux process-tree CPU deltas, including waited-for children.
    ProcessCpu {
        /// User-mode ticks.
        user_ticks: u64,
        /// Kernel-mode ticks.
        system_ticks: u64,
        /// Waited-for child user-mode ticks.
        child_user_ticks: u64,
        /// Waited-for child kernel-mode ticks.
        child_system_ticks: u64,
    },
    /// Linux process I/O deltas.
    ProcessIo {
        /// Bytes read since the prior sample.
        read_bytes: u64,
        /// Bytes written since the prior sample.
        written_bytes: u64,
    },
}

impl ActivityEvidence {
    /// Stable database discriminator.
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::NativeProgress => "native_progress",
            Self::Filesystem { .. } => "filesystem",
            Self::ProcessCpu { .. } => "process_cpu",
            Self::ProcessIo { .. } => "process_io",
        }
    }
}

/// Incremental parser cursor with file identity and complete-record boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FileCursorRecord {
    /// Capability-scoped stable path key.
    path_key: BoundedText<4_096>,
    /// Device identity from fresh metadata.
    device_id: u64,
    /// Inode identity from fresh metadata.
    inode: u64,
    /// Next byte to inspect.
    byte_offset: u64,
    /// End of the last fully parsed record.
    complete_record_offset: u64,
    /// Parser compatibility version.
    parser_version: BoundedText<128>,
    /// Last idempotent observation emitted from this cursor.
    last_observation_id: Option<ObservationId>,
}

impl<'de> Deserialize<'de> for FileCursorRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawFileCursorRecord {
            path_key: BoundedText<4_096>,
            device_id: u64,
            inode: u64,
            byte_offset: u64,
            complete_record_offset: u64,
            parser_version: BoundedText<128>,
            last_observation_id: Option<ObservationId>,
        }

        let raw = RawFileCursorRecord::deserialize(deserializer)?;
        Self::new(
            raw.path_key,
            raw.device_id,
            raw.inode,
            raw.byte_offset,
            raw.complete_record_offset,
            raw.parser_version,
            raw.last_observation_id,
        )
        .map_err(de::Error::custom)
    }
}

impl FileCursorRecord {
    /// Validate offsets and construct a cursor.
    ///
    /// # Errors
    ///
    /// Returns [`RecordInputError`] when a complete boundary exceeds the read
    /// offset.
    pub fn new(
        path_key: BoundedText<4_096>,
        device_id: u64,
        inode: u64,
        byte_offset: u64,
        complete_record_offset: u64,
        parser_version: BoundedText<128>,
        last_observation_id: Option<ObservationId>,
    ) -> Result<Self, RecordInputError> {
        if complete_record_offset > byte_offset {
            return Err(RecordInputError::CompleteOffsetAfterReadOffset);
        }
        Ok(Self {
            path_key,
            device_id,
            inode,
            byte_offset,
            complete_record_offset,
            parser_version,
            last_observation_id,
        })
    }

    /// Capability-scoped stable path key.
    #[must_use]
    pub const fn path_key(&self) -> &BoundedText<4_096> {
        &self.path_key
    }

    /// Device identity from fresh metadata.
    #[must_use]
    pub const fn device_id(&self) -> u64 {
        self.device_id
    }

    /// Inode identity from fresh metadata.
    #[must_use]
    pub const fn inode(&self) -> u64 {
        self.inode
    }

    /// Next byte to inspect.
    #[must_use]
    pub const fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    /// End of the last fully parsed record.
    #[must_use]
    pub const fn complete_record_offset(&self) -> u64 {
        self.complete_record_offset
    }

    /// Parser compatibility version.
    #[must_use]
    pub const fn parser_version(&self) -> &BoundedText<128> {
        &self.parser_version
    }

    /// Last idempotent observation emitted from this cursor.
    #[must_use]
    pub const fn last_observation_id(&self) -> Option<ObservationId> {
        self.last_observation_id
    }
}

/// Parent-provided deadline state retained across restart.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeadlineRecord {
    /// Session whose expected check-in is overridden.
    pub session: SessionIdentity,
    /// Expected check-in wall time.
    pub deadline: Option<WallTimeMs>,
    /// Time at which timer accounting paused.
    pub paused_at: Option<WallTimeMs>,
    /// Source that last changed the deadline.
    pub provenance: ObservationSource,
}

/// Safety evidence frozen at a termination-saga decision point.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TerminationSafetyRecord {
    /// Gates proven at this decision point.
    pub passed_gates: BTreeSet<TerminationGate>,
    /// Gates that suspended or aborted this decision point.
    #[serde(default)]
    pub blockers: BTreeSet<TerminationBlocker>,
    /// Fresh verified process identity when OS signalling may follow.
    pub process: Option<ProcessIdentity>,
}

/// Restartable child-only termination saga.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TerminationSagaRecord {
    /// Child admitted to the safety pipeline.
    pub child: ChildSessionId,
    /// Current conservative stage.
    pub stage: TerminationStage,
    /// Monotonic persisted saga revision.
    pub revision: u64,
    /// Next wall-clock wake-up, subject to fresh reconciliation.
    pub next_action_at: Option<WallTimeMs>,
    /// Safety evidence for the current stage.
    pub safety: TerminationSafetyRecord,
    /// Bounded result of the action that produced the current revision.
    #[serde(default)]
    pub last_outcome: Option<TerminationActionOutcome>,
}

/// Parent's durable MCP inbox cursor.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InboxOffsetRecord {
    /// Parent main session.
    pub parent: MainSessionId,
    /// Last event acknowledged by the parent.
    pub last_event_id: EventId,
    /// Persistable acknowledgement time.
    pub updated_at: WallTimeMs,
}

/// Runtime adapter component health class.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterHealthStatus {
    /// Adapter evidence is current and recognized.
    Healthy,
    /// Adapter continues best effort with actionable warning.
    Degraded,
    /// Adapter cannot currently produce evidence.
    Failed,
}

impl AdapterHealthStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Failed => "failed",
        }
    }
}

/// Persisted adapter health without unbounded native error content.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdapterHealthRecord {
    /// Runtime and tested version.
    pub adapter: AdapterIdentity,
    /// Current component health.
    pub status: AdapterHealthStatus,
    /// Last successful evidence time.
    pub last_success: Option<WallTimeMs>,
    /// Last failed attempt time.
    pub last_error: Option<WallTimeMs>,
    /// Bounded affected-scope description.
    pub affected_scope: Option<BoundedText<2_048>>,
    /// Bounded actionable warning or error.
    pub message: Option<BoundedText<2_048>>,
}

/// One-shot human notification channel.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationChannel {
    /// Home Assistant webhook.
    HomeAssistant,
    /// Generic webhook.
    Webhook,
    /// Browser notification.
    Browser,
}

impl NotificationChannel {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::HomeAssistant => "home_assistant",
            Self::Webhook => "webhook",
            Self::Browser => "browser",
        }
    }
}

/// One-shot notification delivery result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationOutcome {
    /// Channel accepted the notification.
    Delivered,
    /// Attempt returned or produced an error.
    Failed,
    /// Attempt exceeded its bounded total timeout.
    TimedOut,
}

impl NotificationOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Delivered => "delivered",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
        }
    }
}

/// Auditable single notification attempt with bounded diagnostic message.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NotificationAttemptRecord {
    /// Domain event that caused the human notification.
    pub event_id: EventId,
    /// Attempted channel.
    pub channel: NotificationChannel,
    /// Persistable attempt time.
    pub attempted_at: WallTimeMs,
    /// Terminal one-shot outcome.
    pub outcome: NotificationOutcome,
    /// Bounded diagnostic without response bodies or secrets.
    pub message: Option<BoundedText<2_048>>,
}

/// Invalid restart-record construction.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RecordInputError {
    /// Complete-record boundary cannot be beyond the read offset.
    #[error("Complete record offset exceeds read offset")]
    CompleteOffsetAfterReadOffset,
}
