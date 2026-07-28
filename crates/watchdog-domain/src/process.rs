use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::BoundedText;

/// Positive host process identifier.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, schemars::JsonSchema,
)]
#[serde(transparent)]
pub struct ProcessId(u32);

impl ProcessId {
    /// Construct a positive process identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessIdError`] for PID zero.
    pub const fn new(value: u32) -> Result<Self, ProcessIdError> {
        if value == 0 {
            return Err(ProcessIdError);
        }
        Ok(Self(value))
    }

    /// Return the numeric process identifier.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl<'de> Deserialize<'de> for ProcessId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// PID zero cannot identify a host process.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Process ID must be positive")]
pub struct ProcessIdError;

/// Fresh facts required to prevent PID-reuse mistakes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, schemars::JsonSchema)]
pub struct ProcessIdentity {
    pid: ProcessId,
    start_time_ticks: u64,
    executable: BoundedText<512>,
}

impl ProcessIdentity {
    /// Construct an identity from already validated procfs facts.
    #[must_use]
    pub const fn new(pid: ProcessId, start_time_ticks: u64, executable: BoundedText<512>) -> Self {
        Self {
            pid,
            start_time_ticks,
            executable,
        }
    }

    /// Host process identifier.
    #[must_use]
    pub const fn pid(&self) -> ProcessId {
        self.pid
    }

    /// Kernel start time used to detect PID reuse.
    #[must_use]
    pub const fn start_time_ticks(&self) -> u64 {
        self.start_time_ticks
    }

    /// Canonical executable identity selected by the process adapter.
    #[must_use]
    pub fn executable(&self) -> &str {
        self.executable.as_str()
    }
}
