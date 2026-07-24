use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{BoundedText, DomainInputError, MAX_ADAPTER_VERSION_BYTES, WallTimeMs};

const MAX_WARNING_MESSAGE_BYTES: usize = 1_024;

/// Non-negative duration represented without a wall-clock dependency.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct DurationMs(u64);

impl DurationMs {
    /// Construct a duration from milliseconds.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return milliseconds.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Validated default timer thresholds for a session class.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DeadlinePolicy {
    stall_after: DurationMs,
    terminate_after_stalled: DurationMs,
    graceful_signal_grace: DurationMs,
}

impl<'de> Deserialize<'de> for DeadlinePolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawDeadlinePolicy {
            stall_after: DurationMs,
            terminate_after_stalled: DurationMs,
            graceful_signal_grace: DurationMs,
        }

        let raw = RawDeadlinePolicy::deserialize(deserializer)?;
        Self::new(
            raw.stall_after,
            raw.terminate_after_stalled,
            raw.graceful_signal_grace,
        )
        .map_err(de::Error::custom)
    }
}

impl DeadlinePolicy {
    /// Construct stall, child-only termination, and signal-grace thresholds.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyError`] when either post-stall termination delay is zero.
    pub const fn new(
        stall_after: DurationMs,
        terminate_after_stalled: DurationMs,
        graceful_signal_grace: DurationMs,
    ) -> Result<Self, PolicyError> {
        if terminate_after_stalled.0 == 0 {
            return Err(PolicyError::ZeroStalledTerminationDelay);
        }
        if graceful_signal_grace.0 == 0 {
            return Err(PolicyError::ZeroGrace);
        }
        Ok(Self {
            stall_after,
            terminate_after_stalled,
            graceful_signal_grace,
        })
    }

    /// Inactivity before a session becomes stalled.
    #[must_use]
    pub const fn stall_after(self) -> DurationMs {
        self.stall_after
    }

    /// Stalled duration before a child becomes termination-eligible.
    #[must_use]
    pub const fn terminate_after_stalled(self) -> DurationMs {
        self.terminate_after_stalled
    }

    /// Grace between graceful and forceful termination stages.
    #[must_use]
    pub const fn graceful_signal_grace(self) -> DurationMs {
        self.graceful_signal_grace
    }
}

impl Default for DeadlinePolicy {
    fn default() -> Self {
        Self {
            stall_after: DurationMs::new(15 * 60_000),
            terminate_after_stalled: DurationMs::new(60 * 60_000),
            graceful_signal_grace: DurationMs::new(10 * 60_000),
        }
    }
}

/// Invalid timer policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PolicyError {
    /// A stalled child could become eligible without an observation delay.
    #[error("Stalled termination delay must be positive")]
    ZeroStalledTerminationDelay,
    /// The graceful termination stage had no observation grace.
    #[error("Graceful signal grace must be positive")]
    ZeroGrace,
}

/// Parent-requested change to one session's expected check-in deadline.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeadlineCommand {
    /// Replace the expected check-in wall time.
    Set(WallTimeMs),
    /// Pause progress and termination timers.
    Pause,
    /// Resume timers without counting paused elapsed time.
    Resume,
    /// Remove the explicit expected check-in override.
    Clear,
}

/// Actionable compatibility warning category.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WarningKind {
    /// Agent Watchdog needs an adapter update for this native version.
    Upgrade,
    /// Monitoring continues with incomplete evidence.
    Degraded,
}

/// Bounded compatibility warning returned to agents and the UI.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
pub struct CompatibilityWarning {
    kind: WarningKind,
    message: BoundedText<MAX_WARNING_MESSAGE_BYTES>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detected_version: Option<BoundedText<MAX_ADAPTER_VERSION_BYTES>>,
}

impl<'de> Deserialize<'de> for CompatibilityWarning {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawCompatibilityWarning {
            kind: WarningKind,
            message: String,
            #[serde(default)]
            detected_version: Option<String>,
        }

        let raw = RawCompatibilityWarning::deserialize(deserializer)?;
        let detected_version = raw
            .detected_version
            .or_else(|| legacy_detected_version(&raw.message).map(std::borrow::ToOwned::to_owned));
        match detected_version {
            Some(version) => Self::new_with_detected_version(raw.kind, raw.message, version),
            None => Self::new(raw.kind, raw.message),
        }
        .map_err(de::Error::custom)
    }
}

impl CompatibilityWarning {
    /// Construct a non-empty compatibility warning.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] when the message is empty or oversized.
    pub fn new(kind: WarningKind, message: impl Into<String>) -> Result<Self, DomainInputError> {
        let message = BoundedText::new("warning", message)?;
        if message.is_empty() {
            return Err(DomainInputError::Empty { field: "warning" });
        }
        Ok(Self {
            kind,
            message,
            detected_version: None,
        })
    }

    /// Construct a warning backed by a bounded detected native version.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] when the message or version is empty or oversized.
    pub fn new_with_detected_version(
        kind: WarningKind,
        message: impl Into<String>,
        detected_version: impl Into<String>,
    ) -> Result<Self, DomainInputError> {
        let mut warning = Self::new(kind, message)?;
        let detected_version = BoundedText::new("detected_version", detected_version)?;
        if detected_version.is_empty() {
            return Err(DomainInputError::Empty {
                field: "detected_version",
            });
        }
        warning.detected_version = Some(detected_version);
        Ok(warning)
    }

    /// Single-word actionable UI badge.
    #[must_use]
    pub const fn badge(&self) -> &'static str {
        match self.kind {
            WarningKind::Upgrade => "UPGRADE",
            WarningKind::Degraded => "DEGRADED",
        }
    }

    /// Warning category.
    #[must_use]
    pub const fn kind(&self) -> WarningKind {
        self.kind
    }

    /// Human- and agent-readable bounded warning.
    #[must_use]
    pub fn message(&self) -> &str {
        self.message.as_str()
    }

    /// Detected native version supporting this warning, when available.
    #[must_use]
    pub fn detected_version(&self) -> Option<&str> {
        self.detected_version.as_ref().map(BoundedText::as_str)
    }

    /// Whether this warning includes detected-version evidence.
    #[must_use]
    pub const fn has_detected_version(&self) -> bool {
        self.detected_version.is_some()
    }
}

fn legacy_detected_version(message: &str) -> Option<&str> {
    ["detected Claude Code ", "detected Codex CLI "]
        .into_iter()
        .find_map(|prefix| {
            message
                .split_once(prefix)
                .and_then(|(_, suffix)| suffix.split_once(", tested with "))
                .map(|(version, _)| version)
                .filter(|version| !version.is_empty() && version.len() <= MAX_ADAPTER_VERSION_BYTES)
        })
}
