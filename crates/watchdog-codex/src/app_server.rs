use std::{
    fmt,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use watchdog_domain::{
    AdapterIdentity, BoundedText, CompatibilityWarning, DetailedState, DomainInputError,
    EvidenceTrust, NativeSessionKey, ObservationEnvelope, ObservationError, ObservationId,
    ObservationPayload, ObservationSource, RuntimeKind, SessionKind, TimePoint, WarningKind,
};

/// Largest accepted app-server JSONL/WebSocket message.
pub const MAX_APP_SERVER_BYTES: usize = 256 * 1_024;
const TESTED_VERSION: &str = "0.144.5";

/// Parser for generated-schema Codex app-server notifications.
#[derive(Clone, Debug)]
pub struct CodexAppServerParser {
    adapter: AdapterIdentity,
}

impl CodexAppServerParser {
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

    /// Normalize one bounded server notification.
    ///
    /// # Errors
    ///
    /// Returns [`CodexParseError`] without native payload content when the
    /// current generated-schema subset cannot be parsed safely.
    pub fn parse_notification(
        &self,
        input: &[u8],
        event_key: &str,
        observed_at: TimePoint,
    ) -> Result<CodexEventEvidence, CodexParseError> {
        if input.len() > MAX_APP_SERVER_BYTES {
            return Err(CodexParseError::InputTooLarge {
                actual_bytes: input.len(),
                max_bytes: MAX_APP_SERVER_BYTES,
            });
        }
        let raw: RawNotification =
            serde_json::from_slice(input).map_err(|_| CodexParseError::Malformed)?;
        let method = raw
            .method
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or(CodexParseError::MissingField("method"))?;
        let normalized = normalize(method, raw.params)?;
        let subject = native_key(&normalized.thread_id)?;
        let parent = normalized.parent.as_deref().map(native_key).transpose()?;
        let source = ObservationSource::new(
            self.adapter.clone(),
            format!("app-server:{method}"),
            EvidenceTrust::Authoritative,
            None,
        )?;
        let payload = normalized.state.map_or_else(
            || {
                BoundedText::new("progress", "Codex app-server item activity")
                    .map(ObservationPayload::Progress)
            },
            |state| Ok(ObservationPayload::NativeState(state)),
        )?;
        let observation = ObservationEnvelope::new(
            ObservationId::from_native(RuntimeKind::CodexCli, "app-server", event_key)?,
            subject.clone(),
            observed_at,
            source,
            payload,
        )?;
        let classification = normalized
            .classification
            .or_else(|| parent.as_ref().map(|_| SessionKind::Child));
        Ok(CodexEventEvidence {
            subject,
            parent,
            classification,
            observation,
            cwd: normalized.cwd.map(PathBuf::from),
            title: normalized
                .title
                .filter(|value| !value.is_empty())
                .map(|value| BoundedText::new("thread_title", value))
                .transpose()?,
        })
    }
}

/// Typed app-server evidence; native paths remain untrusted until capability checked.
pub struct CodexEventEvidence {
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    classification: Option<SessionKind>,
    observation: ObservationEnvelope,
    cwd: Option<PathBuf>,
    title: Option<BoundedText<512>>,
}

impl CodexEventEvidence {
    /// Native thread subject.
    #[must_use]
    pub const fn subject(&self) -> &NativeSessionKey {
        &self.subject
    }

    /// Exact native parent when app-server supplied it.
    #[must_use]
    pub const fn parent(&self) -> Option<&NativeSessionKey> {
        self.parent.as_ref()
    }

    /// Exact role when the event establishes it.
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

    /// Native thread name suitable for display.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_ref().map(BoundedText::as_str)
    }
}

impl fmt::Debug for CodexEventEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexEventEvidence")
            .field("subject", &self.subject)
            .field("parent", &self.parent)
            .field("classification", &self.classification)
            .field("has_cwd", &self.cwd.is_some())
            .field("title", &self.title)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct RawNotification {
    method: Option<String>,
    #[serde(default)]
    params: Value,
}

struct Normalized {
    thread_id: String,
    parent: Option<String>,
    classification: Option<SessionKind>,
    state: Option<DetailedState>,
    cwd: Option<String>,
    title: Option<String>,
}

fn normalize(method: &str, params: Value) -> Result<Normalized, CodexParseError> {
    match method {
        "thread/started" => {
            let raw: ThreadStarted =
                serde_json::from_value(params).map_err(|_| CodexParseError::Malformed)?;
            let parent = raw.thread.parent_thread_id;
            let classification = Some(if parent.is_some() {
                SessionKind::Child
            } else {
                SessionKind::Main
            });
            Ok(Normalized {
                thread_id: nonempty(raw.thread.id, "thread.id")?,
                parent,
                classification,
                state: Some(thread_state(&raw.thread.status)?),
                cwd: raw.thread.cwd,
                title: raw.thread.name,
            })
        }
        "thread/status/changed" => {
            let raw: ThreadStatusChanged =
                serde_json::from_value(params).map_err(|_| CodexParseError::Malformed)?;
            Ok(Normalized {
                thread_id: nonempty(raw.thread_id, "threadId")?,
                parent: None,
                classification: None,
                state: Some(thread_state(&raw.status)?),
                cwd: None,
                title: None,
            })
        }
        "turn/started" => simple_thread(params, DetailedState::Running),
        "turn/completed" => {
            let raw: TurnCompleted =
                serde_json::from_value(params).map_err(|_| CodexParseError::Malformed)?;
            let state = match raw.turn.status.as_str() {
                "completed" | "interrupted" => DetailedState::Idle,
                "failed" => DetailedState::Failed,
                "inProgress" => DetailedState::Running,
                _ => return Err(CodexParseError::UnsupportedState),
            };
            Ok(Normalized {
                thread_id: nonempty(raw.thread_id, "threadId")?,
                parent: None,
                classification: None,
                state: Some(state),
                cwd: None,
                title: None,
            })
        }
        // This is an app-server listener unload, not native task completion.
        "thread/closed" => simple_thread(params, DetailedState::Idle),
        "thread/deleted" => simple_thread(params, DetailedState::Disappeared),
        "item/started"
        | "item/completed"
        | "item/commandExecution/outputDelta"
        | "item/mcpToolCall/progress" => progress_thread(params),
        _ => Err(CodexParseError::UnsupportedEvent),
    }
}

fn simple_thread(params: Value, state: DetailedState) -> Result<Normalized, CodexParseError> {
    let raw: ThreadOnly = serde_json::from_value(params).map_err(|_| CodexParseError::Malformed)?;
    Ok(Normalized {
        thread_id: nonempty(raw.thread_id, "threadId")?,
        parent: None,
        classification: None,
        state: Some(state),
        cwd: None,
        title: None,
    })
}

fn progress_thread(params: Value) -> Result<Normalized, CodexParseError> {
    let raw: ThreadOnly = serde_json::from_value(params).map_err(|_| CodexParseError::Malformed)?;
    Ok(Normalized {
        thread_id: nonempty(raw.thread_id, "threadId")?,
        parent: None,
        classification: None,
        state: None,
        cwd: None,
        title: None,
    })
}

fn thread_state(status: &ThreadStatus) -> Result<DetailedState, CodexParseError> {
    match status.status_type.as_str() {
        "notLoaded" => Ok(DetailedState::Unknown),
        "idle" => Ok(DetailedState::Idle),
        "systemError" => Ok(DetailedState::Failed),
        "active"
            if status.active_flags.iter().any(|flag| {
                matches!(flag.as_str(), "waitingOnApproval" | "waitingOnUserInput")
            }) =>
        {
            Ok(DetailedState::WaitingForUser)
        }
        "active" => Ok(DetailedState::Running),
        _ => Err(CodexParseError::UnsupportedState),
    }
}

#[derive(Deserialize)]
struct ThreadStarted {
    thread: ThreadValue,
}

#[derive(Deserialize)]
struct ThreadValue {
    id: String,
    #[serde(rename = "parentThreadId")]
    parent_thread_id: Option<String>,
    cwd: Option<String>,
    name: Option<String>,
    status: ThreadStatus,
}

#[derive(Deserialize)]
struct ThreadStatusChanged {
    #[serde(rename = "threadId")]
    thread_id: String,
    status: ThreadStatus,
}

#[derive(Deserialize)]
struct ThreadStatus {
    #[serde(rename = "type")]
    status_type: String,
    #[serde(rename = "activeFlags", default)]
    active_flags: Vec<String>,
}

#[derive(Deserialize)]
struct TurnCompleted {
    #[serde(rename = "threadId")]
    thread_id: String,
    turn: TurnValue,
}

#[derive(Deserialize)]
struct TurnValue {
    status: String,
}

#[derive(Deserialize)]
struct ThreadOnly {
    #[serde(rename = "threadId")]
    thread_id: String,
}

fn nonempty(value: String, field: &'static str) -> Result<String, CodexParseError> {
    if value.is_empty() {
        Err(CodexParseError::MissingField(field))
    } else {
        Ok(value)
    }
}

fn native_key(value: &str) -> Result<NativeSessionKey, DomainInputError> {
    NativeSessionKey::new(RuntimeKind::CodexCli, value)
}

/// Bounded app-server parser failure without native content.
#[derive(Debug, Error)]
pub enum CodexParseError {
    /// Input exceeded the app-server message cap.
    #[error("Codex app-server input exceeds its byte limit")]
    InputTooLarge {
        /// Observed byte length.
        actual_bytes: usize,
        /// Enforced byte limit.
        max_bytes: usize,
    },
    /// JSON or a required current-schema shape was malformed.
    #[error("Codex app-server input is malformed")]
    Malformed,
    /// Required current-schema field was empty or absent.
    #[error("Codex app-server input is missing required field {0}")]
    MissingField(&'static str),
    /// Notification method is not safe to normalize.
    #[error("Codex app-server event is unsupported")]
    UnsupportedEvent,
    /// Runtime status is not recognized.
    #[error("Codex app-server state is unsupported")]
    UnsupportedState,
    /// A rollout activity record had no exact file-to-thread association.
    #[error("Codex rollout activity has no exact thread association")]
    MissingSubject,
    /// Rollout metadata disagreed with its registered file association.
    #[error("Codex rollout metadata thread association conflicts")]
    SubjectMismatch,
    /// Bounded domain construction failed.
    #[error(transparent)]
    Domain(#[from] DomainInputError),
    /// Observation subject and source did not agree.
    #[error(transparent)]
    Observation(#[from] ObservationError),
}

impl CodexParseError {
    /// Actionable warning for affected sessions and adapter health.
    #[must_use]
    pub fn compatibility_warning(&self) -> CompatibilityWarning {
        CompatibilityWarning::new(
            WarningKind::Upgrade,
            format!(
                "Update Agent Watchdog's Codex adapter; tested with Codex CLI {TESTED_VERSION}"
            ),
        )
        .unwrap_or_else(|_| unreachable!("static compatibility warning is bounded"))
    }
}
