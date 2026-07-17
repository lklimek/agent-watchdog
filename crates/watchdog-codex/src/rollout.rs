use std::{
    fmt,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_json::Value;
use watchdog_domain::{
    AdapterIdentity, BoundedText, DetailedState, DomainInputError, EvidenceTrust, NativeSessionKey,
    ObservationEnvelope, ObservationId, ObservationPayload, ObservationSource, RuntimeKind,
    SessionKind, TimePoint,
};

use crate::CodexParseError;

/// Largest rollout record parsed for native metadata or typed activity.
pub const MAX_ROLLOUT_RECORD_BYTES: usize = 1_024 * 1_024;

/// Metadata-only parser for current Codex rollout JSONL records.
#[derive(Clone, Debug)]
pub struct CodexRolloutParser {
    adapter: AdapterIdentity,
}

impl CodexRolloutParser {
    /// Construct a parser for the observed Codex CLI version.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] when the version is empty or oversized.
    pub fn new(version: impl Into<String>) -> Result<Self, DomainInputError> {
        Ok(Self {
            adapter: AdapterIdentity::new(RuntimeKind::CodexCli, version)?,
        })
    }

    /// Normalize one bounded rollout record without retaining transcript content.
    ///
    /// Non-metadata records require the caller's capability-validated file
    /// association. A session metadata record establishes and verifies that
    /// association itself.
    ///
    /// # Errors
    ///
    /// Returns [`CodexParseError`] when input is oversized, malformed,
    /// unsupported, or cannot be associated with an exact Codex thread.
    pub fn parse_record(
        &self,
        input: &[u8],
        known_subject: Option<&NativeSessionKey>,
        event_key: &str,
        observed_at: TimePoint,
    ) -> Result<CodexRolloutEvidence, CodexParseError> {
        if input.len() > MAX_ROLLOUT_RECORD_BYTES {
            return Err(CodexParseError::InputTooLarge {
                actual_bytes: input.len(),
                max_bytes: MAX_ROLLOUT_RECORD_BYTES,
            });
        }
        let raw: RawRolloutRecord =
            serde_json::from_slice(input).map_err(|_| CodexParseError::Malformed)?;
        let record_type = raw
            .record_type
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or(CodexParseError::MissingField("type"))?;

        let normalized = if record_type == "session_meta" {
            normalize_metadata(raw.payload, known_subject)?
        } else if is_activity_record(record_type) {
            normalize_activity(known_subject)?
        } else {
            return Err(CodexParseError::UnsupportedEvent);
        };

        let source = ObservationSource::new(
            self.adapter.clone(),
            format!("rollout:{record_type}"),
            EvidenceTrust::Corroborating,
            None,
        )?;
        let payload = if record_type == "session_meta" {
            ObservationPayload::NativeState(DetailedState::Starting)
        } else {
            ObservationPayload::Progress(BoundedText::new("progress", "Codex rollout activity")?)
        };
        let observation = ObservationEnvelope::new(
            ObservationId::from_native(RuntimeKind::CodexCli, "rollout", event_key)?,
            normalized.subject.clone(),
            observed_at,
            source,
            payload,
        )?;

        Ok(CodexRolloutEvidence {
            subject: normalized.subject,
            parent: normalized.parent,
            classification: normalized.classification,
            observation,
            cwd: normalized.cwd,
            title: normalized.title,
        })
    }
}

/// Typed rollout evidence with no transcript content retained.
pub struct CodexRolloutEvidence {
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    classification: Option<SessionKind>,
    observation: ObservationEnvelope,
    cwd: Option<PathBuf>,
    title: Option<BoundedText<512>>,
}

impl CodexRolloutEvidence {
    /// Exact native thread subject.
    #[must_use]
    pub const fn subject(&self) -> &NativeSessionKey {
        &self.subject
    }

    /// Exact native parent from session metadata.
    #[must_use]
    pub const fn parent(&self) -> Option<&NativeSessionKey> {
        self.parent.as_ref()
    }

    /// Exact role when the record establishes it.
    #[must_use]
    pub const fn kind(&self) -> Option<SessionKind> {
        self.classification
    }

    /// Typed reducer observation.
    #[must_use]
    pub const fn observation(&self) -> &ObservationEnvelope {
        &self.observation
    }

    /// Untrusted startup directory for capability validation.
    #[must_use]
    pub fn cwd(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }

    /// Optional native agent nickname or role.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_ref().map(BoundedText::as_str)
    }
}

impl fmt::Debug for CodexRolloutEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexRolloutEvidence")
            .field("subject", &self.subject)
            .field("parent", &self.parent)
            .field("classification", &self.classification)
            .field("has_cwd", &self.cwd.is_some())
            .field("title", &self.title)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct RawRolloutRecord {
    #[serde(rename = "type")]
    record_type: Option<String>,
    #[serde(default)]
    payload: Value,
}

#[derive(Deserialize)]
struct RawSessionMeta {
    id: Option<String>,
    parent_thread_id: Option<String>,
    cwd: Option<String>,
    agent_nickname: Option<String>,
    #[serde(default, alias = "agent_type")]
    agent_role: Option<String>,
    #[serde(default)]
    source: Value,
}

struct NormalizedRollout {
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    classification: Option<SessionKind>,
    cwd: Option<PathBuf>,
    title: Option<BoundedText<512>>,
}

fn normalize_metadata(
    payload: Value,
    known_subject: Option<&NativeSessionKey>,
) -> Result<NormalizedRollout, CodexParseError> {
    let raw: RawSessionMeta =
        serde_json::from_value(payload).map_err(|_| CodexParseError::Malformed)?;
    let id = raw
        .id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or(CodexParseError::MissingField("payload.id"))?;
    let subject = native_key(id)?;
    if known_subject.is_some_and(|known| known != &subject) {
        return Err(CodexParseError::SubjectMismatch);
    }
    let nested_parent = raw
        .source
        .get("subagent")
        .and_then(|value| value.get("thread_spawn"))
        .and_then(|value| value.get("parent_thread_id"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    let parent = raw
        .parent_thread_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .or(nested_parent)
        .map(native_key)
        .transpose()?;
    let is_subagent = parent.is_some() || raw.source.get("subagent").is_some();
    let title = raw
        .agent_nickname
        .filter(|value| !value.is_empty())
        .or_else(|| raw.agent_role.filter(|value| !value.is_empty()))
        .map(|value| BoundedText::new("agent_title", value))
        .transpose()?;
    Ok(NormalizedRollout {
        subject,
        parent,
        classification: Some(if is_subagent {
            SessionKind::Child
        } else {
            SessionKind::Main
        }),
        cwd: raw.cwd.filter(|value| !value.is_empty()).map(PathBuf::from),
        title,
    })
}

fn normalize_activity(
    known_subject: Option<&NativeSessionKey>,
) -> Result<NormalizedRollout, CodexParseError> {
    let subject = known_subject
        .filter(|subject| subject.runtime() == RuntimeKind::CodexCli)
        .cloned()
        .ok_or(CodexParseError::MissingSubject)?;
    Ok(NormalizedRollout {
        subject,
        parent: None,
        classification: None,
        cwd: None,
        title: None,
    })
}

fn is_activity_record(record_type: &str) -> bool {
    matches!(
        record_type,
        "response_item"
            | "inter_agent_communication"
            | "inter_agent_communication_metadata"
            | "compacted"
            | "turn_context"
            | "world_state"
            | "event_msg"
    )
}

fn native_key(value: &str) -> Result<NativeSessionKey, DomainInputError> {
    NativeSessionKey::new(RuntimeKind::CodexCli, value)
}
