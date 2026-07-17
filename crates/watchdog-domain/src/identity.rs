use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de};
use uuid::Uuid;

use crate::{BoundedText, DomainInputError};

const MAX_NATIVE_ID_BYTES: usize = 512;
const MAX_OBSERVATION_PART_BYTES: usize = 512;

/// Runtime namespace for native identifiers and adapter behavior.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeKind {
    /// Anthropic Claude Code.
    ClaudeCode,
    /// `OpenAI` native Codex CLI.
    CodexCli,
    /// The Codex Companion Claude plugin.
    CodexCompanion,
    /// Reserved runtime identity for a future `OpenCode` adapter.
    OpenCode,
}

impl RuntimeKind {
    /// Stable serialized runtime namespace.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClaudeCode => "claude_code",
            Self::CodexCli => "codex_cli",
            Self::CodexCompanion => "codex_companion",
            Self::OpenCode => "opencode",
        }
    }
}

/// A runtime-qualified, bounded native session identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NativeSessionKey {
    runtime: RuntimeKind,
    native_id: BoundedText<MAX_NATIVE_ID_BYTES>,
}

impl<'de> Deserialize<'de> for NativeSessionKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawNativeSessionKey {
            runtime: RuntimeKind,
            native_id: String,
        }

        let raw = RawNativeSessionKey::deserialize(deserializer)?;
        Self::new(raw.runtime, raw.native_id).map_err(de::Error::custom)
    }
}

impl NativeSessionKey {
    /// Construct a non-empty runtime-qualified native identifier.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] when the native identifier is empty or
    /// exceeds its byte budget.
    pub fn new(
        runtime: RuntimeKind,
        native_id: impl Into<String>,
    ) -> Result<Self, DomainInputError> {
        let native_id = BoundedText::new("native_id", native_id)?;
        if native_id.is_empty() {
            return Err(DomainInputError::Empty { field: "native_id" });
        }
        Ok(Self { runtime, native_id })
    }

    /// Runtime namespace for this native identifier.
    #[must_use]
    pub const fn runtime(&self) -> RuntimeKind {
        self.runtime
    }

    /// Runtime-provided identifier without its namespace.
    #[must_use]
    pub fn native_id(&self) -> &str {
        self.native_id.as_str()
    }
}

macro_rules! uuid_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Construct the identifier from a UUID.
            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// Return the underlying UUID.
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_id!(
    SessionId,
    "Stable Watchdog identity for a retained session."
);
uuid_id!(
    ObservationId,
    "Stable idempotency identity for one observation."
);

impl SessionId {
    /// Derive a stable `UUIDv5` identity from a runtime-qualified native identity.
    #[must_use]
    pub fn from_native(native: &NativeSessionKey) -> Self {
        Self(uuid_v5_parts(
            &Uuid::NAMESPACE_URL,
            &[
                native.runtime.as_str().as_bytes(),
                native.native_id.as_bytes(),
            ],
        ))
    }
}

impl ObservationId {
    /// Derive a stable `UUIDv5` identity from bounded native event parts.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] when a source or event identifier exceeds
    /// the per-part byte budget.
    pub fn from_native(
        runtime: RuntimeKind,
        source: impl Into<String>,
        native_event_id: impl Into<String>,
    ) -> Result<Self, DomainInputError> {
        let source = BoundedText::<MAX_OBSERVATION_PART_BYTES>::new("source", source)?;
        let native_event_id =
            BoundedText::<MAX_OBSERVATION_PART_BYTES>::new("native_event_id", native_event_id)?;
        if source.is_empty() {
            return Err(DomainInputError::Empty { field: "source" });
        }
        if native_event_id.is_empty() {
            return Err(DomainInputError::Empty {
                field: "native_event_id",
            });
        }
        let namespace = uuid_v5_parts(&Uuid::NAMESPACE_URL, &[b"agent-watchdog", b"observations"]);
        Ok(Self(uuid_v5_parts(
            &namespace,
            &[
                runtime.as_str().as_bytes(),
                source.as_bytes(),
                native_event_id.as_bytes(),
            ],
        )))
    }
}

/// Type-safe identity for a main session.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct MainSessionId(SessionId);

impl From<SessionId> for MainSessionId {
    fn from(value: SessionId) -> Self {
        Self(value)
    }
}

impl MainSessionId {
    /// Return the general session identity.
    #[must_use]
    pub const fn session_id(self) -> SessionId {
        self.0
    }
}

/// Type-safe identity for a child session.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ChildSessionId(SessionId);

impl From<SessionId> for ChildSessionId {
    fn from(value: SessionId) -> Self {
        Self(value)
    }
}

impl ChildSessionId {
    /// Return the general session identity.
    #[must_use]
    pub const fn session_id(self) -> SessionId {
        self.0
    }
}

fn uuid_v5_parts(namespace: &Uuid, parts: &[&[u8]]) -> Uuid {
    let capacity = parts.iter().fold(0_usize, |total, part| {
        total.saturating_add(8_usize.saturating_add(part.len()))
    });
    let mut name = Vec::with_capacity(capacity);
    for part in parts {
        name.extend_from_slice(&u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
        name.extend_from_slice(part);
    }
    Uuid::new_v5(namespace, &name)
}
