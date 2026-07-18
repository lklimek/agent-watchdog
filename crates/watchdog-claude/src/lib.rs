//! Claude Code observation adapter.

use std::{
    fmt,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use thiserror::Error;
use watchdog_domain::{
    AdapterIdentity, BoundedText, CompatibilityWarning, DetailedState, DomainInputError,
    EvidenceTrust, NativeSessionKey, ObservationEnvelope, ObservationError, ObservationId,
    ObservationPayload, ObservationSource, RuntimeKind, SessionKind, TimePoint, WarningKind,
};

/// Largest accepted official hook payload.
pub const MAX_HOOK_BYTES: usize = 64 * 1_024;
/// Largest accepted team configuration snapshot.
pub const MAX_TEAM_CONFIG_BYTES: usize = 1_024 * 1_024;
const MAX_TEAM_MEMBERS: usize = 256;
/// Claude Code version used for the current adapter compatibility fixtures.
pub const TESTED_CLAUDE_VERSION: &str = "2.1.212";

/// Parser for official Claude Code lifecycle hook input.
#[derive(Clone, Debug)]
pub struct ClaudeHookParser {
    adapter: AdapterIdentity,
}

impl ClaudeHookParser {
    /// Construct a parser for the observed native version.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] when the version is empty or oversized.
    pub fn new(version: impl Into<String>) -> Result<Self, DomainInputError> {
        Ok(Self {
            adapter: AdapterIdentity::new(RuntimeKind::ClaudeCode, version)?,
        })
    }

    /// Normalize one bounded official hook event.
    ///
    /// # Errors
    ///
    /// Returns [`ClaudeParseError`] without echoing native content when the
    /// event is oversized, malformed, incomplete, or unsupported.
    pub fn parse_hook(
        &self,
        input: &[u8],
        event_key: &str,
        observed_at: TimePoint,
    ) -> Result<ClaudeHookEvidence, ClaudeParseError> {
        if input.len() > MAX_HOOK_BYTES {
            return Err(ClaudeParseError::InputTooLarge {
                actual_bytes: input.len(),
                max_bytes: MAX_HOOK_BYTES,
            });
        }
        let raw: RawHook =
            serde_json::from_slice(input).map_err(|_| ClaudeParseError::Malformed)?;
        let parent = native_key(required(raw.session_id.as_deref(), "session_id")?)?;
        let event_name = required(raw.hook_event_name.as_deref(), "hook_event_name")?;
        let (subject, parent, kind, state, transcript_path) = match event_name {
            "SessionStart" => (
                parent.clone(),
                None,
                SessionKind::Main,
                DetailedState::Running,
                raw.transcript_path.as_deref(),
            ),
            "SubagentStart" => (
                native_key(required(raw.agent_id.as_deref(), "agent_id")?)?,
                Some(parent),
                SessionKind::Child,
                DetailedState::Starting,
                raw.transcript_path.as_deref(),
            ),
            "SubagentStop" => (
                native_key(required(raw.agent_id.as_deref(), "agent_id")?)?,
                Some(parent),
                SessionKind::Child,
                DetailedState::Completed,
                raw.agent_transcript_path
                    .as_deref()
                    .or(raw.transcript_path.as_deref()),
            ),
            "Stop" => (
                parent.clone(),
                None,
                SessionKind::Main,
                DetailedState::Idle,
                raw.transcript_path.as_deref(),
            ),
            "StopFailure" => (
                parent.clone(),
                None,
                SessionKind::Main,
                DetailedState::Failed,
                raw.transcript_path.as_deref(),
            ),
            "SessionEnd" => (
                parent.clone(),
                None,
                SessionKind::Main,
                session_end_state(raw.reason.as_deref()),
                raw.transcript_path.as_deref(),
            ),
            "Notification" if raw.notification_type.as_deref() == Some("permission_prompt") => (
                parent.clone(),
                None,
                SessionKind::Main,
                DetailedState::WaitingForUser,
                raw.transcript_path.as_deref(),
            ),
            _ => return Err(ClaudeParseError::UnsupportedEvent),
        };
        let source = ObservationSource::new(
            self.adapter.clone(),
            format!("hook:{event_name}"),
            EvidenceTrust::Authoritative,
            None,
        )?;
        let observation = ObservationEnvelope::new(
            ObservationId::from_native(RuntimeKind::ClaudeCode, "hook", event_key)?,
            subject.clone(),
            observed_at,
            source,
            ObservationPayload::NativeState(state),
        )?;
        let title = raw
            .agent_type
            .filter(|value| !value.is_empty())
            .map(|value| BoundedText::new("agent_type", value))
            .transpose()?;
        Ok(ClaudeHookEvidence {
            subject,
            parent,
            kind,
            observation,
            cwd: path(raw.cwd),
            transcript_path: transcript_path.map(PathBuf::from),
            title,
        })
    }
}

/// Normalized hook evidence; paths are available only for later capability checks.
pub struct ClaudeHookEvidence {
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    kind: SessionKind,
    observation: ObservationEnvelope,
    cwd: Option<PathBuf>,
    transcript_path: Option<PathBuf>,
    title: Option<BoundedText<256>>,
}

impl ClaudeHookEvidence {
    /// Native subject.
    #[must_use]
    pub const fn subject(&self) -> &NativeSessionKey {
        &self.subject
    }

    /// Exact native parent for a child hook.
    #[must_use]
    pub const fn parent(&self) -> Option<&NativeSessionKey> {
        self.parent.as_ref()
    }

    /// Main or child classification supplied by the event contract.
    #[must_use]
    pub const fn kind(&self) -> SessionKind {
        self.kind
    }

    /// Typed reducer observation.
    #[must_use]
    pub const fn observation(&self) -> &ObservationEnvelope {
        &self.observation
    }

    /// Untrusted startup directory to validate against capability roots.
    #[must_use]
    pub fn cwd(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }

    /// Untrusted transcript target to validate before registration.
    #[must_use]
    pub fn transcript_path(&self) -> Option<&Path> {
        self.transcript_path.as_deref()
    }

    /// Native agent type suitable for a child title.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_ref().map(BoundedText::as_str)
    }
}

impl fmt::Debug for ClaudeHookEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaudeHookEvidence")
            .field("subject", &self.subject)
            .field("parent", &self.parent)
            .field("kind", &self.kind)
            .field("has_cwd", &self.cwd.is_some())
            .field("has_transcript", &self.transcript_path.is_some())
            .field("title", &self.title)
            .finish_non_exhaustive()
    }
}

/// Active Claude team discovered from one exact team configuration.
pub struct ClaudeTeam {
    lead: NativeSessionKey,
    lead_cwd: Option<PathBuf>,
    members: Vec<ClaudeTeamMember>,
}

impl ClaudeTeam {
    /// Exact lead session.
    #[must_use]
    pub const fn lead(&self) -> &NativeSessionKey {
        &self.lead
    }

    /// Untrusted lead startup directory for capability validation.
    #[must_use]
    pub fn lead_cwd(&self) -> Option<&Path> {
        self.lead_cwd.as_deref()
    }

    /// Active non-lead members only.
    #[must_use]
    pub fn members(&self) -> &[ClaudeTeamMember] {
        &self.members
    }
}

impl fmt::Debug for ClaudeTeam {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaudeTeam")
            .field("lead", &self.lead)
            .field("has_lead_cwd", &self.lead_cwd.is_some())
            .field("member_count", &self.members.len())
            .finish()
    }
}

/// One active Claude team member.
pub struct ClaudeTeamMember {
    subject: NativeSessionKey,
    name: BoundedText<256>,
    agent_type: Option<BoundedText<256>>,
    cwd: Option<PathBuf>,
}

impl ClaudeTeamMember {
    /// Native child identity.
    #[must_use]
    pub const fn subject(&self) -> &NativeSessionKey {
        &self.subject
    }

    /// Native member name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Native member type.
    #[must_use]
    pub fn agent_type(&self) -> Option<&str> {
        self.agent_type.as_ref().map(BoundedText::as_str)
    }

    /// Untrusted member directory for capability validation.
    #[must_use]
    pub fn cwd(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }
}

impl fmt::Debug for ClaudeTeamMember {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaudeTeamMember")
            .field("subject", &self.subject)
            .field("name", &self.name)
            .field("agent_type", &self.agent_type)
            .field("has_cwd", &self.cwd.is_some())
            .finish()
    }
}

/// Parse one bounded exact team config without choosing a newest team.
///
/// # Errors
///
/// Returns [`ClaudeParseError`] for partial, oversized, or unsupported data.
pub fn parse_team_config(input: &[u8]) -> Result<ClaudeTeam, ClaudeParseError> {
    if input.len() > MAX_TEAM_CONFIG_BYTES {
        return Err(ClaudeParseError::InputTooLarge {
            actual_bytes: input.len(),
            max_bytes: MAX_TEAM_CONFIG_BYTES,
        });
    }
    let raw: RawTeam = serde_json::from_slice(input).map_err(|_| ClaudeParseError::Malformed)?;
    let lead = native_key(required(raw.lead_session_id.as_deref(), "leadSessionId")?)?;
    if raw.members.len() > MAX_TEAM_MEMBERS {
        return Err(ClaudeParseError::TooManyMembers {
            actual: raw.members.len(),
            maximum: MAX_TEAM_MEMBERS,
        });
    }
    let mut lead_cwd = None;
    let mut members = Vec::new();
    for member in raw.members {
        if member.agent_type.as_deref() == Some("team-lead") {
            lead_cwd = path(member.cwd);
            continue;
        }
        if !member.is_active {
            continue;
        }
        let subject = native_key(required(member.agent_id.as_deref(), "agentId")?)?;
        let name_value = required(member.name.as_deref(), "name")?;
        let name = BoundedText::new("name", name_value)?;
        let agent_type = member
            .agent_type
            .filter(|value| !value.is_empty())
            .map(|value| BoundedText::new("agent_type", value))
            .transpose()?;
        members.push(ClaudeTeamMember {
            subject,
            name,
            agent_type,
            cwd: path(member.cwd),
        });
    }
    Ok(ClaudeTeam {
        lead,
        lead_cwd,
        members,
    })
}

/// Metadata-only signal from one complete incremental transcript record.
pub struct ClaudeTranscriptSignal {
    session_id: Option<BoundedText<512>>,
    agent_id: Option<BoundedText<512>>,
    cwd: Option<PathBuf>,
    activity: bool,
}

impl ClaudeTranscriptSignal {
    /// Main native session ID when the record includes it.
    #[must_use]
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_ref().map(BoundedText::as_str)
    }

    /// Child native agent ID when the record includes it.
    #[must_use]
    pub fn agent_id(&self) -> Option<&str> {
        self.agent_id.as_ref().map(BoundedText::as_str)
    }

    /// Untrusted record directory for capability validation.
    #[must_use]
    pub fn cwd(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }

    /// Whether this recognized record proves bounded transcript activity.
    #[must_use]
    pub const fn is_activity(&self) -> bool {
        self.activity
    }
}

impl fmt::Debug for ClaudeTranscriptSignal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaudeTranscriptSignal")
            .field("session_id", &self.session_id)
            .field("agent_id", &self.agent_id)
            .field("has_cwd", &self.cwd.is_some())
            .field("activity", &self.activity)
            .finish()
    }
}

/// Parse one complete JSONL record without retaining message or tool content.
///
/// # Errors
///
/// Returns [`ClaudeParseError`] for oversized, malformed, or unrecognized
/// records so the caller can preserve its last trusted state.
pub fn parse_transcript_record(input: &[u8]) -> Result<ClaudeTranscriptSignal, ClaudeParseError> {
    if input.len() > MAX_HOOK_BYTES {
        return Err(ClaudeParseError::InputTooLarge {
            actual_bytes: input.len(),
            max_bytes: MAX_HOOK_BYTES,
        });
    }
    let raw: RawTranscript =
        serde_json::from_slice(input).map_err(|_| ClaudeParseError::Malformed)?;
    let record_type = required(raw.record_type.as_deref(), "type")?;
    if !matches!(
        record_type,
        "assistant" | "user" | "progress" | "system" | "queue-operation"
    ) {
        return Err(ClaudeParseError::UnsupportedRecord);
    }
    Ok(ClaudeTranscriptSignal {
        session_id: raw
            .session_id
            .filter(|value| !value.is_empty())
            .map(|value| BoundedText::new("session_id", value))
            .transpose()?,
        agent_id: raw
            .agent_id
            .filter(|value| !value.is_empty())
            .map(|value| BoundedText::new("agent_id", value))
            .transpose()?,
        cwd: path(raw.cwd),
        activity: true,
    })
}

/// Metadata-only projection of a Claude team task record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeTaskSignal {
    owner: BoundedText<256>,
    title: Option<BoundedText<512>>,
    state: DetailedState,
}

impl ClaudeTaskSignal {
    /// Team member name owning the task.
    #[must_use]
    pub fn owner(&self) -> &str {
        self.owner.as_str()
    }

    /// Bounded task subject suitable for display.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_ref().map(BoundedText::as_str)
    }

    /// Normalized current task state.
    #[must_use]
    pub const fn state(&self) -> DetailedState {
        self.state
    }
}

/// Parse one bounded team task status.
///
/// # Errors
///
/// Returns [`ClaudeParseError`] for partial or unknown task records.
pub fn parse_task_record(input: &[u8]) -> Result<ClaudeTaskSignal, ClaudeParseError> {
    if input.len() > MAX_HOOK_BYTES {
        return Err(ClaudeParseError::InputTooLarge {
            actual_bytes: input.len(),
            max_bytes: MAX_HOOK_BYTES,
        });
    }
    let raw: RawTask = serde_json::from_slice(input).map_err(|_| ClaudeParseError::Malformed)?;
    let state = match required(raw.status.as_deref(), "status")? {
        "pending" => DetailedState::Starting,
        "in_progress" => DetailedState::Running,
        "completed" => DetailedState::Completed,
        "failed" => DetailedState::Failed,
        "cancelled" => DetailedState::Cancelled,
        _ => return Err(ClaudeParseError::UnsupportedState),
    };
    let owner = BoundedText::new("owner", required(raw.owner.as_deref(), "owner")?)?;
    let title = raw
        .subject
        .filter(|value| !value.is_empty())
        .map(|value| BoundedText::new("subject", value))
        .transpose()?;
    Ok(ClaudeTaskSignal {
        owner,
        title,
        state,
    })
}

/// Bounded parser failure without raw hook/config content.
#[derive(Debug, Error)]
pub enum ClaudeParseError {
    /// Input exceeded its parser-specific cap.
    #[error("Claude adapter input exceeds its byte limit")]
    InputTooLarge {
        /// Observed byte length.
        actual_bytes: usize,
        /// Enforced byte limit.
        max_bytes: usize,
    },
    /// JSON was partial or malformed.
    #[error("Claude adapter input is malformed")]
    Malformed,
    /// A required current-schema field was absent or empty.
    #[error("Claude adapter input is missing required field {0}")]
    MissingField(&'static str),
    /// Hook kind is not yet safe to normalize.
    #[error("Claude adapter event is unsupported")]
    UnsupportedEvent,
    /// Transcript record kind is not safe to treat as activity.
    #[error("Claude transcript record is unsupported")]
    UnsupportedRecord,
    /// Native task status cannot be normalized safely.
    #[error("Claude task state is unsupported")]
    UnsupportedState,
    /// Team member count exceeded the bounded scan contract.
    #[error("Claude team exceeds the member limit")]
    TooManyMembers {
        /// Observed member count.
        actual: usize,
        /// Enforced member count.
        maximum: usize,
    },
    /// Bounded domain construction failed.
    #[error(transparent)]
    Domain(#[from] DomainInputError),
    /// Observation subject and provenance did not agree.
    #[error(transparent)]
    Observation(#[from] ObservationError),
}

impl ClaudeParseError {
    /// Actionable compatibility warning for affected sessions/adapter health.
    #[must_use]
    pub fn compatibility_warning(&self) -> CompatibilityWarning {
        CompatibilityWarning::new(
            WarningKind::Upgrade,
            format!(
                "Update Agent Watchdog's Claude adapter; tested with Claude Code {TESTED_CLAUDE_VERSION}"
            ),
        )
        .unwrap_or_else(|_| unreachable!("static compatibility warning is bounded"))
    }
}

#[derive(Deserialize)]
struct RawHook {
    session_id: Option<String>,
    transcript_path: Option<String>,
    cwd: Option<String>,
    hook_event_name: Option<String>,
    agent_id: Option<String>,
    agent_type: Option<String>,
    agent_transcript_path: Option<String>,
    notification_type: Option<String>,
    reason: Option<String>,
}

#[derive(Deserialize)]
struct RawTeam {
    #[serde(rename = "leadSessionId")]
    lead_session_id: Option<String>,
    #[serde(default)]
    members: Vec<RawMember>,
}

#[derive(Deserialize)]
struct RawMember {
    name: Option<String>,
    cwd: Option<String>,
    #[serde(rename = "agentId")]
    agent_id: Option<String>,
    #[serde(rename = "agentType")]
    agent_type: Option<String>,
    #[serde(rename = "isActive", default)]
    is_active: bool,
}

#[derive(Deserialize)]
struct RawTranscript {
    #[serde(rename = "type")]
    record_type: Option<String>,
    #[serde(rename = "sessionId", alias = "session_id")]
    session_id: Option<String>,
    #[serde(rename = "agentId", alias = "agent_id")]
    agent_id: Option<String>,
    cwd: Option<String>,
}

#[derive(Deserialize)]
struct RawTask {
    subject: Option<String>,
    status: Option<String>,
    owner: Option<String>,
}

fn required<'a>(value: Option<&'a str>, field: &'static str) -> Result<&'a str, ClaudeParseError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or(ClaudeParseError::MissingField(field))
}

fn native_key(value: &str) -> Result<NativeSessionKey, DomainInputError> {
    NativeSessionKey::new(RuntimeKind::ClaudeCode, value)
}

fn path(value: Option<String>) -> Option<PathBuf> {
    value.filter(|value| !value.is_empty()).map(PathBuf::from)
}

fn session_end_state(reason: Option<&str>) -> DetailedState {
    match reason {
        Some("clear" | "resume") => DetailedState::Idle,
        Some("bypass_permissions_disabled") => DetailedState::Cancelled,
        _ => DetailedState::Completed,
    }
}
