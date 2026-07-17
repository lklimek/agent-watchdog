use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;
use watchdog_domain::{
    AdapterIdentity, BoundedText, ChildSessionId, EventId, MainSessionId, ObservationId,
    ObservationSource, ProcessIdentity, SessionIdentity, WallTimeMs,
};

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
    /// Evidence supporting the relation.
    pub provenance: ObservationSource,
    /// Inclusive validity start.
    pub valid_from: WallTimeMs,
    /// Exclusive validity end, when superseded.
    pub valid_until: Option<WallTimeMs>,
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

/// Persisted child-only termination stage.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationStage {
    /// Parent warning and grace period.
    WarningGrace,
    /// Runtime-native cancellation requested.
    GracefulCancel,
    /// Verified `SIGTERM` sent.
    Sigterm,
    /// Verified `SIGKILL` sent.
    Sigkill,
    /// Child exited and no further action is allowed.
    Completed,
    /// Safety gate failed or recovery cancelled escalation.
    Aborted,
}

impl TerminationStage {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::WarningGrace => "warning_grace",
            Self::GracefulCancel => "graceful_cancel",
            Self::Sigterm => "sigterm",
            Self::Sigkill => "sigkill",
            Self::Completed => "completed",
            Self::Aborted => "aborted",
        }
    }
}

/// One required child-termination safety gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationGate {
    /// Session is authoritatively classified as a child.
    TrustworthyChild,
    /// No material source conflict exists.
    NoSourceConflict,
    /// No active long-running operation exists.
    NoActiveOperation,
    /// Session is not waiting for user input.
    NotWaitingForUser,
    /// Parent deadline and extensions permit escalation.
    DeadlineAllows,
}

/// Safety evidence frozen at a termination-saga decision point.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TerminationSafetyRecord {
    /// Gates proven at this decision point.
    pub passed_gates: BTreeSet<TerminationGate>,
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
