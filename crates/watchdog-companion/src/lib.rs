//! Codex Companion observation adapter.

use std::{
    fmt,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use thiserror::Error;
use watchdog_domain::{
    AdapterIdentity, BoundedText, CompatibilityWarning, DetailedState, DomainInputError,
    EvidenceTrust, NativeSessionKey, ObservationEnvelope, ObservationError, ObservationId,
    ObservationPayload, ObservationSource, RuntimeKind, TimePoint, WarningKind,
};

/// Largest accepted per-workspace summary.
pub const MAX_SUMMARY_BYTES: usize = 2 * 1_024 * 1_024;
/// Largest accepted individual detail file. Unknown result fields are streamed
/// through Serde and discarded rather than materialized.
pub const MAX_DETAIL_BYTES: usize = 8 * 1_024 * 1_024;
const MAX_JOBS: usize = 50;
const STATE_VERSION: u32 = 1;
const TESTED_VERSION: &str = "1.0.6";

/// Parser and reconciler for current Codex Companion persisted state.
#[derive(Clone, Debug)]
pub struct CompanionParser {
    adapter: AdapterIdentity,
}

impl CompanionParser {
    /// Construct an adapter for the observed plugin version.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] when the version is empty or oversized.
    pub fn new(version: impl Into<String>) -> Result<Self, DomainInputError> {
        Ok(Self {
            adapter: AdapterIdentity::new(RuntimeKind::CodexCompanion, version)?,
        })
    }

    /// Parse one bounded per-workspace `state.json` independently of details.
    ///
    /// # Errors
    ///
    /// Returns [`CompanionParseError`] on size, schema, version, or domain
    /// violations without echoing native content.
    pub fn parse_summary(&self, input: &[u8]) -> Result<CompanionSnapshot, CompanionParseError> {
        check_size(input, MAX_SUMMARY_BYTES)?;
        let raw: RawSummary =
            serde_json::from_slice(input).map_err(|_| CompanionParseError::Malformed)?;
        if raw.version != Some(STATE_VERSION) {
            return Err(CompanionParseError::UnsupportedVersion);
        }
        if raw.jobs.len() > MAX_JOBS {
            return Err(CompanionParseError::TooManyJobs {
                actual: raw.jobs.len(),
                maximum: MAX_JOBS,
            });
        }
        let jobs = raw
            .jobs
            .into_iter()
            .map(|raw| normalize_job(raw, CompanionSource::Summary))
            .collect::<Result<_, _>>()?;
        Ok(CompanionSnapshot { jobs })
    }

    /// Parse one bounded `jobs/<id>.json` independently of its summary.
    ///
    /// Large result/rendered/request fields are ignored during deserialization.
    ///
    /// # Errors
    ///
    /// Returns [`CompanionParseError`] on size, schema, state, PID, or domain
    /// violations without echoing native content.
    pub fn parse_detail(&self, input: &[u8]) -> Result<CompanionJob, CompanionParseError> {
        check_size(input, MAX_DETAIL_BYTES)?;
        let raw: RawJob =
            serde_json::from_slice(input).map_err(|_| CompanionParseError::Malformed)?;
        normalize_job(raw, CompanionSource::Detail)
    }

    /// Reconcile non-atomic summary/detail records conservatively.
    ///
    /// Missing either side is normal during write ordering and native pruning.
    /// Conflicting active/terminal or terminal states become `Unknown` until a
    /// later read agrees, preventing false completion or recovery.
    ///
    /// # Errors
    ///
    /// Returns [`CompanionParseError`] when no record exists or IDs disagree.
    pub fn reconcile(
        &self,
        summary: Option<&CompanionJob>,
        detail: Option<&CompanionJob>,
    ) -> Result<ReconciledCompanionJob, CompanionParseError> {
        let (job, source, consistency) = match (summary, detail) {
            (Some(summary), Some(detail)) => {
                if summary.subject != detail.subject {
                    return Err(CompanionParseError::SubjectMismatch);
                }
                let mut merged = merge_jobs(summary, detail);
                let parent_conflicts = summary.parent.is_some()
                    && detail.parent.is_some()
                    && summary.parent != detail.parent;
                let consistency = if summary.state == detail.state
                    && summary.workspace_root == detail.workspace_root
                    && !parent_conflicts
                {
                    CompanionConsistency::Consistent
                } else {
                    merged.state = DetailedState::Unknown;
                    CompanionConsistency::Conflicted
                };
                (merged, CompanionSource::Both, consistency)
            }
            (Some(summary), None) => (
                summary.clone(),
                CompanionSource::Summary,
                CompanionConsistency::SingleSource,
            ),
            (None, Some(detail)) => (
                detail.clone(),
                CompanionSource::Detail,
                CompanionConsistency::SingleSource,
            ),
            (None, None) => return Err(CompanionParseError::MissingRecord),
        };
        Ok(ReconciledCompanionJob {
            job,
            source,
            consistency,
        })
    }

    /// Construct a typed reducer observation from reconciled state.
    ///
    /// # Errors
    ///
    /// Returns [`CompanionParseError`] if bounded observation construction
    /// fails.
    pub fn observation(
        &self,
        job: &ReconciledCompanionJob,
        event_key: &str,
        observed_at: TimePoint,
    ) -> Result<ObservationEnvelope, CompanionParseError> {
        let trust = if job.consistency == CompanionConsistency::Conflicted {
            EvidenceTrust::Uncertain
        } else {
            EvidenceTrust::Authoritative
        };
        let source = ObservationSource::new(
            self.adapter.clone(),
            match job.source {
                CompanionSource::Summary => "state:summary",
                CompanionSource::Detail => "state:detail",
                CompanionSource::Both => "state:summary+detail",
            },
            trust,
            None,
        )?;
        Ok(ObservationEnvelope::new(
            ObservationId::from_native(RuntimeKind::CodexCompanion, "state", event_key)?,
            job.job.subject.clone(),
            observed_at,
            source,
            ObservationPayload::NativeState(job.job.state),
        )?)
    }

    /// Record an append to a capability-validated registered job log without
    /// parsing or retaining native log content.
    ///
    /// # Errors
    ///
    /// Returns [`CompanionParseError`] when the subject belongs to another
    /// runtime or bounded observation construction fails.
    pub fn log_activity(
        &self,
        subject: &NativeSessionKey,
        event_key: &str,
        observed_at: TimePoint,
    ) -> Result<ObservationEnvelope, CompanionParseError> {
        if subject.runtime() != RuntimeKind::CodexCompanion {
            return Err(CompanionParseError::SubjectMismatch);
        }
        let source = ObservationSource::new(
            self.adapter.clone(),
            "log:append",
            EvidenceTrust::Corroborating,
            None,
        )?;
        Ok(ObservationEnvelope::new(
            ObservationId::from_native(RuntimeKind::CodexCompanion, "log", event_key)?,
            subject.clone(),
            observed_at,
            source,
            ObservationPayload::Progress(BoundedText::new(
                "progress",
                "Codex Companion log activity",
            )?),
        )?)
    }
}

/// One independently parsed per-workspace summary.
#[derive(Debug)]
pub struct CompanionSnapshot {
    jobs: Vec<CompanionJob>,
}

impl CompanionSnapshot {
    /// All retained jobs, including active and terminal records.
    #[must_use]
    pub fn jobs(&self) -> &[CompanionJob] {
        &self.jobs
    }
}

/// One typed Companion job record.
#[derive(Clone)]
pub struct CompanionJob {
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    workspace_root: PathBuf,
    title: Option<BoundedText<512>>,
    state: DetailedState,
    phase: Option<BoundedText<128>>,
    pid: Option<u32>,
    thread_id: Option<BoundedText<256>>,
    turn_id: Option<BoundedText<256>>,
    updated_at: Option<BoundedText<128>>,
    terminal_error: Option<BoundedText<2_048>>,
    source: CompanionSource,
}

impl CompanionJob {
    /// Native Companion job ID.
    #[must_use]
    pub const fn subject(&self) -> &NativeSessionKey {
        &self.subject
    }

    /// Exact owning Claude session when Companion persisted it.
    #[must_use]
    pub const fn parent(&self) -> Option<&NativeSessionKey> {
        self.parent.as_ref()
    }

    /// Untrusted job workspace for later capability validation.
    #[must_use]
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Native job title.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_ref().map(BoundedText::as_str)
    }

    /// Normalized job state.
    #[must_use]
    pub const fn state(&self) -> DetailedState {
        self.state
    }

    /// Current native execution phase.
    #[must_use]
    pub fn phase(&self) -> Option<&str> {
        self.phase.as_ref().map(BoundedText::as_str)
    }

    /// Launcher/worker PID. It is not a process identity until freshly sampled.
    #[must_use]
    pub const fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// Bounded native terminal diagnostic, intentionally omitted from Debug.
    #[must_use]
    pub fn terminal_error(&self) -> Option<&str> {
        self.terminal_error.as_ref().map(BoundedText::as_str)
    }

    /// Current record source.
    #[must_use]
    pub const fn source(&self) -> CompanionSource {
        self.source
    }

    /// Supported graceful interrupt identity when both current fields exist.
    #[must_use]
    pub fn graceful_cancellation(&self) -> Option<CompanionCancellation<'_>> {
        Some(CompanionCancellation {
            thread_id: self.thread_id.as_ref()?.as_str(),
            turn_id: self.turn_id.as_ref()?.as_str(),
        })
    }
}

impl fmt::Debug for CompanionJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompanionJob")
            .field("subject", &self.subject)
            .field("parent", &self.parent)
            .field("state", &self.state)
            .field("phase", &self.phase)
            .field("pid", &self.pid)
            .field("has_thread", &self.thread_id.is_some())
            .field("has_turn", &self.turn_id.is_some())
            .field("updated_at", &self.updated_at)
            .field("has_terminal_error", &self.terminal_error.is_some())
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

/// Reconciled view of independently written native files.
pub struct ReconciledCompanionJob {
    job: CompanionJob,
    source: CompanionSource,
    consistency: CompanionConsistency,
}

impl ReconciledCompanionJob {
    /// Native job ID.
    #[must_use]
    pub const fn subject(&self) -> &NativeSessionKey {
        self.job.subject()
    }

    /// Exact owning Claude session when persisted.
    #[must_use]
    pub const fn parent(&self) -> Option<&NativeSessionKey> {
        self.job.parent()
    }

    /// Conservatively reconciled state.
    #[must_use]
    pub const fn state(&self) -> DetailedState {
        self.job.state()
    }

    /// Source files participating in the current view.
    #[must_use]
    pub const fn source(&self) -> CompanionSource {
        self.source
    }

    /// Whether independently written records agree.
    #[must_use]
    pub const fn consistency(&self) -> CompanionConsistency {
        self.consistency
    }

    /// Full typed job metadata.
    #[must_use]
    pub const fn job(&self) -> &CompanionJob {
        &self.job
    }
}

impl fmt::Debug for ReconciledCompanionJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReconciledCompanionJob")
            .field("job", &self.job)
            .field("source", &self.source)
            .field("consistency", &self.consistency)
            .finish()
    }
}

/// Native records used for the current view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompanionSource {
    /// Per-workspace summary only.
    Summary,
    /// Individual job detail only.
    Detail,
    /// Both summary and detail.
    Both,
}

/// Agreement between independently written native records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompanionConsistency {
    /// Both records report the same normalized state.
    Consistent,
    /// Only one record currently exists, including normal pruning windows.
    SingleSource,
    /// Records disagree; state is forced to unknown.
    Conflicted,
}

/// Current app-server turn identity for Companion's supported interrupt path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompanionCancellation<'a> {
    thread_id: &'a str,
    turn_id: &'a str,
}

impl<'a> CompanionCancellation<'a> {
    /// Native Codex thread ID.
    #[must_use]
    pub const fn thread_id(self) -> &'a str {
        self.thread_id
    }

    /// Native active turn ID.
    #[must_use]
    pub const fn turn_id(self) -> &'a str {
        self.turn_id
    }
}

#[derive(Deserialize)]
struct RawSummary {
    version: Option<u32>,
    #[serde(default)]
    jobs: Vec<RawJob>,
}

#[derive(Deserialize)]
struct RawJob {
    id: Option<String>,
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    #[serde(rename = "workspaceRoot")]
    workspace_root: Option<String>,
    title: Option<String>,
    status: Option<String>,
    phase: Option<String>,
    pid: Option<i64>,
    #[serde(rename = "threadId")]
    thread_id: Option<String>,
    #[serde(rename = "turnId")]
    turn_id: Option<String>,
    #[serde(rename = "updatedAt")]
    updated_at: Option<String>,
    #[serde(rename = "errorMessage")]
    error_message: Option<String>,
}

fn normalize_job(
    raw: RawJob,
    source: CompanionSource,
) -> Result<CompanionJob, CompanionParseError> {
    let id = required(raw.id.as_deref(), "id")?;
    let workspace_root = required(raw.workspace_root.as_deref(), "workspaceRoot")?;
    let status = required(raw.status.as_deref(), "status")?;
    Ok(CompanionJob {
        subject: NativeSessionKey::new(RuntimeKind::CodexCompanion, id)?,
        parent: raw
            .session_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(|value| NativeSessionKey::new(RuntimeKind::ClaudeCode, value))
            .transpose()?,
        workspace_root: PathBuf::from(workspace_root),
        title: optional_text(raw.title, "title")?,
        state: normalize_state(status)?,
        phase: optional_text(raw.phase, "phase")?,
        pid: raw
            .pid
            .map(|pid| u32::try_from(pid).map_err(|_| CompanionParseError::InvalidPid))
            .transpose()?
            .filter(|pid| *pid > 0),
        thread_id: optional_text(raw.thread_id, "threadId")?,
        turn_id: optional_text(raw.turn_id, "turnId")?,
        updated_at: optional_text(raw.updated_at, "updatedAt")?,
        terminal_error: optional_text(raw.error_message, "errorMessage")?,
        source,
    })
}

fn merge_jobs(summary: &CompanionJob, detail: &CompanionJob) -> CompanionJob {
    CompanionJob {
        subject: detail.subject.clone(),
        parent: detail.parent.clone().or_else(|| summary.parent.clone()),
        workspace_root: detail.workspace_root.clone(),
        title: detail.title.clone().or_else(|| summary.title.clone()),
        state: detail.state,
        phase: detail.phase.clone().or_else(|| summary.phase.clone()),
        pid: detail.pid.or(summary.pid),
        thread_id: detail
            .thread_id
            .clone()
            .or_else(|| summary.thread_id.clone()),
        turn_id: detail.turn_id.clone().or_else(|| summary.turn_id.clone()),
        updated_at: summary
            .updated_at
            .clone()
            .or_else(|| detail.updated_at.clone()),
        terminal_error: detail
            .terminal_error
            .clone()
            .or_else(|| summary.terminal_error.clone()),
        source: CompanionSource::Both,
    }
}

fn normalize_state(status: &str) -> Result<DetailedState, CompanionParseError> {
    match status {
        "queued" => Ok(DetailedState::Starting),
        "running" => Ok(DetailedState::Running),
        "completed" | "cancelled" => Ok(DetailedState::Completed),
        "failed" => Ok(DetailedState::Failed),
        _ => Err(CompanionParseError::UnsupportedState),
    }
}

fn check_size(input: &[u8], maximum: usize) -> Result<(), CompanionParseError> {
    if input.len() > maximum {
        Err(CompanionParseError::InputTooLarge {
            actual_bytes: input.len(),
            max_bytes: maximum,
        })
    } else {
        Ok(())
    }
}

fn required<'a>(
    value: Option<&'a str>,
    field: &'static str,
) -> Result<&'a str, CompanionParseError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or(CompanionParseError::MissingField(field))
}

fn optional_text<const N: usize>(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<BoundedText<N>>, CompanionParseError> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| BoundedText::new(field, value).map_err(CompanionParseError::from))
        .transpose()
}

/// Bounded adapter failure without native result or error content.
#[derive(Debug, Error)]
pub enum CompanionParseError {
    /// Native file exceeded its parser-specific cap.
    #[error("Codex Companion input exceeds its byte limit")]
    InputTooLarge {
        /// Observed bytes.
        actual_bytes: usize,
        /// Enforced byte cap.
        max_bytes: usize,
    },
    /// JSON or current schema shape was malformed.
    #[error("Codex Companion input is malformed")]
    Malformed,
    /// Summary state version is not supported.
    #[error("Codex Companion state version is unsupported")]
    UnsupportedVersion,
    /// Required field was missing or empty.
    #[error("Codex Companion input is missing required field {0}")]
    MissingField(&'static str),
    /// Native status cannot be safely normalized.
    #[error("Codex Companion job state is unsupported")]
    UnsupportedState,
    /// Native PID is outside the Linux process ID domain.
    #[error("Codex Companion job PID is invalid")]
    InvalidPid,
    /// Summary exceeded the native retention contract.
    #[error("Codex Companion summary exceeds the job limit")]
    TooManyJobs {
        /// Observed job count.
        actual: usize,
        /// Enforced native maximum.
        maximum: usize,
    },
    /// Summary/detail IDs do not identify the same job.
    #[error("Codex Companion summary and detail IDs conflict")]
    SubjectMismatch,
    /// Reconciliation was requested without either record.
    #[error("Codex Companion job has no current record")]
    MissingRecord,
    /// Bounded domain construction failed.
    #[error(transparent)]
    Domain(#[from] DomainInputError),
    /// Observation subject and provenance did not agree.
    #[error(transparent)]
    Observation(#[from] ObservationError),
}

impl CompanionParseError {
    /// Actionable compatibility warning for affected sessions/adapter health.
    #[must_use]
    pub fn compatibility_warning(&self) -> CompatibilityWarning {
        CompatibilityWarning::new(
            WarningKind::Upgrade,
            format!(
                "Update Agent Watchdog's Codex Companion adapter; tested with plugin {TESTED_VERSION}"
            ),
        )
        .unwrap_or_else(|_| unreachable!("static compatibility warning is bounded"))
    }
}
