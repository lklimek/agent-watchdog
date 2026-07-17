use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{BoundedText, DomainInputError, RuntimeKind};

const MAX_ADAPTER_VERSION_BYTES: usize = 128;
const MAX_SOURCE_FINGERPRINT_BYTES: usize = 512;

/// Trust assigned to one evidence source before deterministic reduction.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceTrust {
    /// Runtime contract or explicit agent report owns this fact.
    Authoritative,
    /// Independent evidence supports but cannot own the fact.
    Corroborating,
    /// Evidence participates only in scored correlation.
    Heuristic,
    /// Source is degraded, conflicting, or otherwise unsafe to trust.
    Uncertain,
}

/// Confidence in basis points from zero through ten thousand.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Confidence(u16);

impl Confidence {
    /// Validate a basis-point confidence value.
    ///
    /// # Errors
    ///
    /// Returns [`ConfidenceError`] for values above 10,000.
    pub const fn new(basis_points: u16) -> Result<Self, ConfidenceError> {
        if basis_points > 10_000 {
            return Err(ConfidenceError { basis_points });
        }
        Ok(Self(basis_points))
    }

    /// Return confidence in basis points.
    #[must_use]
    pub const fn basis_points(self) -> u16 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Confidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let basis_points = u16::deserialize(deserializer)?;
        Self::new(basis_points).map_err(de::Error::custom)
    }
}

/// Invalid confidence that contains no source content.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Confidence {basis_points} exceeds 10000 basis points")]
pub struct ConfidenceError {
    basis_points: u16,
}

/// Runtime adapter name and tested native version.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct AdapterIdentity {
    runtime: RuntimeKind,
    version: BoundedText<MAX_ADAPTER_VERSION_BYTES>,
}

impl<'de> Deserialize<'de> for AdapterIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawAdapterIdentity {
            runtime: RuntimeKind,
            version: String,
        }

        let raw = RawAdapterIdentity::deserialize(deserializer)?;
        Self::new(raw.runtime, raw.version).map_err(de::Error::custom)
    }
}

impl AdapterIdentity {
    /// Construct an adapter identity with a non-empty bounded version.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] when the version is empty or oversized.
    pub fn new(runtime: RuntimeKind, version: impl Into<String>) -> Result<Self, DomainInputError> {
        let version = BoundedText::new("adapter_version", version)?;
        if version.is_empty() {
            return Err(DomainInputError::Empty {
                field: "adapter_version",
            });
        }
        Ok(Self { runtime, version })
    }

    /// Runtime implemented by the adapter.
    #[must_use]
    pub const fn runtime(&self) -> RuntimeKind {
        self.runtime
    }

    /// Native version observed by the adapter.
    #[must_use]
    pub fn version(&self) -> &str {
        self.version.as_str()
    }
}

/// Provenance attached to every observation envelope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObservationSource {
    adapter: AdapterIdentity,
    fingerprint: BoundedText<MAX_SOURCE_FINGERPRINT_BYTES>,
    trust: EvidenceTrust,
    confidence: Option<Confidence>,
}

impl ObservationSource {
    /// Construct bounded source provenance.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] when the source fingerprint is oversized.
    pub fn new(
        adapter: AdapterIdentity,
        fingerprint: impl Into<String>,
        trust: EvidenceTrust,
        confidence: Option<Confidence>,
    ) -> Result<Self, DomainInputError> {
        Ok(Self {
            adapter,
            fingerprint: BoundedText::new("source_fingerprint", fingerprint)?,
            trust,
            confidence,
        })
    }

    /// Adapter that emitted the observation.
    #[must_use]
    pub const fn adapter(&self) -> &AdapterIdentity {
        &self.adapter
    }

    /// Stable native source-location fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> &str {
        self.fingerprint.as_str()
    }

    /// Trust class used by the reducer.
    #[must_use]
    pub const fn trust(&self) -> EvidenceTrust {
        self.trust
    }

    /// Optional heuristic confidence.
    #[must_use]
    pub const fn confidence(&self) -> Option<Confidence> {
        self.confidence
    }
}
