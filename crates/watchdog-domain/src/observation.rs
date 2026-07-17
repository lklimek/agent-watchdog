use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    BoundedText, CompatibilityWarning, DeadlineCommand, DetailedState, NativeSessionKey,
    ObservationId, ObservationSource, ProcessIdentity, TimePoint,
};

/// Bounded typed fact emitted by adapters or MCP callers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ObservationPayload {
    /// Runtime-native lifecycle state.
    NativeState(DetailedState),
    /// Process identity associated with the session.
    ProcessIdentity(ProcessIdentity),
    /// Bounded progress summary without transcript content.
    Progress(BoundedText<2_048>),
    /// Parent-requested expected check-in change.
    Deadline(DeadlineCommand),
    /// Runtime compatibility degradation.
    Compatibility(CompatibilityWarning),
    /// Bounded explanation of materially conflicting sources.
    SourceConflict(BoundedText<2_048>),
}

/// Idempotent observation with typed subject, provenance, time, and payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ObservationEnvelope {
    observation_id: ObservationId,
    subject: NativeSessionKey,
    observed_at: TimePoint,
    source: ObservationSource,
    payload: ObservationPayload,
}

impl<'de> Deserialize<'de> for ObservationEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawObservationEnvelope {
            observation_id: ObservationId,
            subject: NativeSessionKey,
            observed_at: TimePoint,
            source: ObservationSource,
            payload: ObservationPayload,
        }

        let raw = RawObservationEnvelope::deserialize(deserializer)?;
        Self::new(
            raw.observation_id,
            raw.subject,
            raw.observed_at,
            raw.source,
            raw.payload,
        )
        .map_err(de::Error::custom)
    }
}

/// Cross-field observation validation failure without native content.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObservationError {
    /// Adapter runtime and native subject namespace differ.
    #[error("Adapter runtime does not match subject runtime")]
    RuntimeMismatch,
}

impl ObservationEnvelope {
    /// Construct a validated observation from bounded domain parts.
    ///
    /// # Errors
    ///
    /// Returns [`ObservationError`] when adapter and subject runtimes differ.
    pub fn new(
        observation_id: ObservationId,
        subject: NativeSessionKey,
        observed_at: TimePoint,
        source: ObservationSource,
        payload: ObservationPayload,
    ) -> Result<Self, ObservationError> {
        if source.adapter().runtime() != subject.runtime() {
            return Err(ObservationError::RuntimeMismatch);
        }
        Ok(Self {
            observation_id,
            subject,
            observed_at,
            source,
            payload,
        })
    }

    /// Stable idempotency identity.
    #[must_use]
    pub const fn observation_id(&self) -> ObservationId {
        self.observation_id
    }

    /// Runtime-qualified native subject.
    #[must_use]
    pub const fn subject(&self) -> &NativeSessionKey {
        &self.subject
    }

    /// Coherent wall and monotonic observation time.
    #[must_use]
    pub const fn observed_at(&self) -> TimePoint {
        self.observed_at
    }

    /// Source provenance and trust.
    #[must_use]
    pub const fn source(&self) -> &ObservationSource {
        &self.source
    }

    /// Typed bounded fact.
    #[must_use]
    pub const fn payload(&self) -> &ObservationPayload {
        &self.payload
    }
}
