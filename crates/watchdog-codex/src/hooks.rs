use std::{
    fmt,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use watchdog_domain::{
    AdapterIdentity, BoundedText, DetailedState, DomainInputError, EvidenceTrust, NativeSessionKey,
    ObservationEnvelope, ObservationId, ObservationPayload, ObservationSource, RuntimeKind,
    SessionKind, TimePoint,
};

use crate::CodexParseError;

/// Largest accepted official Codex hook input.
pub const MAX_HOOK_BYTES: usize = 64 * 1_024;

/// Parser for official Codex lifecycle hook input.
#[derive(Clone, Debug)]
pub struct CodexHookParser {
    adapter: AdapterIdentity,
}

impl CodexHookParser {
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

    /// Normalize one bounded official lifecycle hook event.
    ///
    /// # Errors
    ///
    /// Returns [`CodexParseError`] without native message content when input is
    /// oversized, malformed, incomplete, or unsupported.
    pub fn parse_hook(
        &self,
        input: &[u8],
        event_key: &str,
        observed_at: TimePoint,
    ) -> Result<CodexHookEvidence, CodexParseError> {
        if input.len() > MAX_HOOK_BYTES {
            return Err(CodexParseError::InputTooLarge {
                actual_bytes: input.len(),
                max_bytes: MAX_HOOK_BYTES,
            });
        }
        let raw: RawHook = serde_json::from_slice(input).map_err(|_| CodexParseError::Malformed)?;
        let parent = native_key(required(raw.session_id.as_deref(), "session_id")?)?;
        let event_name = required(raw.hook_event_name.as_deref(), "hook_event_name")?;
        let (subject, parent, kind, state, transcript_path) = match event_name {
            "SessionStart" => (
                parent.clone(),
                None,
                SessionKind::Main,
                DetailedState::Running,
                raw.transcript_path,
            ),
            "SubagentStart" => (
                native_key(required(raw.agent_id.as_deref(), "agent_id")?)?,
                Some(parent),
                SessionKind::Child,
                DetailedState::Starting,
                raw.transcript_path,
            ),
            "SubagentStop" => (
                native_key(required(raw.agent_id.as_deref(), "agent_id")?)?,
                Some(parent),
                SessionKind::Child,
                DetailedState::Completed,
                raw.agent_transcript_path.or(raw.transcript_path),
            ),
            "Stop" => (
                parent.clone(),
                None,
                SessionKind::Main,
                DetailedState::Idle,
                raw.transcript_path,
            ),
            _ => return Err(CodexParseError::UnsupportedEvent),
        };
        let source = ObservationSource::new(
            self.adapter.clone(),
            format!("hook:{event_name}"),
            EvidenceTrust::Authoritative,
            None,
        )?;
        let observation = ObservationEnvelope::new(
            ObservationId::from_native(RuntimeKind::CodexCli, "hook", event_key)?,
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
        Ok(CodexHookEvidence {
            subject,
            parent,
            kind,
            observation,
            cwd: raw.cwd.filter(|value| !value.is_empty()).map(PathBuf::from),
            transcript_path: transcript_path
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            title,
        })
    }
}

/// Normalized official hook evidence with native text bodies discarded.
pub struct CodexHookEvidence {
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    kind: SessionKind,
    observation: ObservationEnvelope,
    cwd: Option<PathBuf>,
    transcript_path: Option<PathBuf>,
    title: Option<BoundedText<256>>,
}

impl CodexHookEvidence {
    /// Native subject.
    #[must_use]
    pub const fn subject(&self) -> &NativeSessionKey {
        &self.subject
    }

    /// Exact native parent for child events.
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

    /// Untrusted startup directory for capability validation.
    #[must_use]
    pub fn cwd(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }

    /// Untrusted transcript target for capability validation.
    #[must_use]
    pub fn transcript_path(&self) -> Option<&Path> {
        self.transcript_path.as_deref()
    }

    /// Native subagent type suitable for display.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_ref().map(BoundedText::as_str)
    }
}

impl fmt::Debug for CodexHookEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexHookEvidence")
            .field("subject", &self.subject)
            .field("parent", &self.parent)
            .field("kind", &self.kind)
            .field("has_cwd", &self.cwd.is_some())
            .field("has_transcript", &self.transcript_path.is_some())
            .field("title", &self.title)
            .finish_non_exhaustive()
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
}

fn required<'a>(value: Option<&'a str>, field: &'static str) -> Result<&'a str, CodexParseError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or(CodexParseError::MissingField(field))
}

fn native_key(value: &str) -> Result<NativeSessionKey, DomainInputError> {
    NativeSessionKey::new(RuntimeKind::CodexCli, value)
}
