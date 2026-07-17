//! Deterministic clock and identity test-support contracts.

use uuid::Uuid;
use watchdog_domain::{Clock, DurationMs, IdFactory, TimePoint, WallTimeMs};
use watchdog_testkit::{DeterministicIdFactory, FakeClock};

#[test]
fn fake_clock_advances_wall_and_monotonic_time_together() {
    let clock = FakeClock::new(TimePoint::new(WallTimeMs::new(1_000), 500));

    clock.advance(DurationMs::new(250));

    assert_eq!(clock.now(), TimePoint::new(WallTimeMs::new(1_250), 750));
}

#[test]
fn seeded_id_factory_is_reproducible_and_unique_per_sequence() {
    let seed = Uuid::from_u128(42);
    let first = DeterministicIdFactory::new(seed);
    let replay = DeterministicIdFactory::new(seed);

    let id_one = first.new_session_id();
    let id_two = first.new_session_id();

    assert_ne!(id_one, id_two);
    assert_eq!(id_one, replay.new_session_id());
    assert_eq!(id_two, replay.new_session_id());
}
