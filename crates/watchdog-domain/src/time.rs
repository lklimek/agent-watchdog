use serde::{Deserialize, Serialize};

use crate::SessionId;

/// Milliseconds since the Unix epoch for persistence and external presentation.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    schemars::JsonSchema,
)]
#[serde(transparent)]
pub struct WallTimeMs(i64);

impl WallTimeMs {
    /// Construct a wall-clock timestamp from Unix milliseconds.
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Return Unix milliseconds.
    #[must_use]
    pub const fn value(self) -> i64 {
        self.0
    }
}

/// One coherent wall and boot-relative time sample.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, schemars::JsonSchema)]
pub struct TimePoint {
    wall_time: WallTimeMs,
    monotonic_ms: u64,
}

impl TimePoint {
    /// Construct one coherent time sample.
    #[must_use]
    pub const fn new(wall_time: WallTimeMs, monotonic_ms: u64) -> Self {
        Self {
            wall_time,
            monotonic_ms,
        }
    }

    /// Persistable wall time.
    #[must_use]
    pub const fn wall_time(self) -> WallTimeMs {
        self.wall_time
    }

    /// Boot-relative elapsed milliseconds used for live timers.
    #[must_use]
    pub const fn monotonic_ms(self) -> u64 {
        self.monotonic_ms
    }
}

/// Time source injected into pure policy and reducer code.
pub trait Clock: Send + Sync {
    /// Return one coherent current time sample.
    fn now(&self) -> TimePoint;
}

/// Source of `UUIDv4` identities for heuristic-only discoveries.
pub trait IdFactory: Send + Sync {
    /// Create a new non-native session identity.
    fn new_session_id(&self) -> SessionId;
}
