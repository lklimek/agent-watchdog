use std::time::{Instant, SystemTime, UNIX_EPOCH};

use watchdog_domain::{Clock, TimePoint, WallTimeMs};

/// Production wall and monotonic clock sampled as one bounded time point.
#[derive(Debug)]
pub struct SystemClock {
    started: Instant,
}

impl SystemClock {
    /// Start a process-relative monotonic epoch.
    #[must_use]
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> TimePoint {
        let wall_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u128, |elapsed| elapsed.as_millis());
        let wall_ms = i64::try_from(wall_ms).unwrap_or(i64::MAX);
        let monotonic_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        TimePoint::new(WallTimeMs::new(wall_ms), monotonic_ms)
    }
}
