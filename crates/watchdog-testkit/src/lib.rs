//! Deterministic test support for Agent Watchdog crates.

use std::sync::{
    Mutex, MutexGuard,
    atomic::{AtomicU64, Ordering},
};

use uuid::Uuid;
use watchdog_domain::{Clock, DurationMs, IdFactory, SessionId, TimePoint, WallTimeMs};

/// Thread-safe manually advanced clock for reducer and policy tests.
#[derive(Debug)]
pub struct FakeClock {
    current: Mutex<TimePoint>,
}

impl FakeClock {
    /// Construct a clock at an explicit coherent time sample.
    #[must_use]
    pub const fn new(current: TimePoint) -> Self {
        Self {
            current: Mutex::new(current),
        }
    }

    /// Advance both wall and monotonic components by the same duration.
    pub fn advance(&self, duration: DurationMs) {
        let mut current = self.lock();
        let wall_delta = i64::try_from(duration.value()).unwrap_or(i64::MAX);
        *current = TimePoint::new(
            WallTimeMs::new(current.wall_time().value().saturating_add(wall_delta)),
            current.monotonic_ms().saturating_add(duration.value()),
        );
    }

    /// Replace the current coherent sample.
    pub fn set(&self, current: TimePoint) {
        *self.lock() = current;
    }

    fn lock(&self) -> MutexGuard<'_, TimePoint> {
        match self.current.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl Clock for FakeClock {
    fn now(&self) -> TimePoint {
        *self.lock()
    }
}

/// Reproducible `UUIDv5` sequence for tests and fixtures.
#[derive(Debug)]
pub struct DeterministicIdFactory {
    namespace: Uuid,
    sequence: AtomicU64,
}

impl DeterministicIdFactory {
    /// Construct a sequence from a test-selected namespace UUID.
    #[must_use]
    pub const fn new(namespace: Uuid) -> Self {
        Self {
            namespace,
            sequence: AtomicU64::new(0),
        }
    }
}

impl IdFactory for DeterministicIdFactory {
    fn new_session_id(&self) -> SessionId {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        SessionId::from_uuid(Uuid::new_v5(&self.namespace, &sequence.to_be_bytes()))
    }
}
