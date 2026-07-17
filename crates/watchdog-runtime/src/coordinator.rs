use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use thiserror::Error;
use watchdog_domain::{
    DomainEvent, EventId, ObservationEnvelope, ReducerInput, ReducerPolicy, SessionSnapshot, reduce,
};
use watchdog_store::{
    ApplyObservation, ApplyResult, OutboxDestination, SnapshotUpdate, StoreError, WatchdogStore,
};

/// Process-wide monotonic event identity source shared by session coordinators.
#[derive(Debug)]
pub struct EventSequence {
    next: AtomicU64,
}

impl EventSequence {
    /// Construct a sequence at the first unallocated durable event ID.
    #[must_use]
    pub const fn new(first: u64) -> Self {
        Self {
            next: AtomicU64::new(first),
        }
    }

    /// Resume allocation after the highest transactionally stored event.
    ///
    /// Call this once during startup before sharing the sequence with session
    /// lanes.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinatorError`] when the store cannot provide a safe next
    /// identity.
    pub async fn from_store(store: &WatchdogStore) -> Result<Self, CoordinatorError> {
        Ok(Self::new(store.first_unallocated_event_id().await?))
    }

    fn next(&self) -> Result<EventId, CoordinatorError> {
        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map(EventId::new)
            .map_err(|_| CoordinatorError::EventSequenceExhausted)
    }
}

/// Coordinator persistence or identity-allocation failure.
#[derive(Debug, Error)]
pub enum CoordinatorError {
    /// Transactional persistence failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// No unique durable integer event identity remains.
    #[error("Durable event sequence exhausted")]
    EventSequenceExhausted,
    /// Observation payload was submitted through the wrong coordinator path.
    #[error("Observation payload does not match coordinator operation")]
    InvalidInput,
}

/// One serial reducer lane for a session; callers use separate lanes concurrently.
#[derive(Debug)]
pub struct SessionCoordinator {
    store: WatchdogStore,
    snapshot: SessionSnapshot,
    policy: ReducerPolicy,
    event_sequence: Arc<EventSequence>,
    destinations: Vec<OutboxDestination>,
}

impl SessionCoordinator {
    /// Construct one session lane around a shared store and event sequence.
    #[must_use]
    pub fn new(
        store: WatchdogStore,
        snapshot: SessionSnapshot,
        policy: ReducerPolicy,
        event_sequence: Arc<EventSequence>,
        destinations: impl IntoIterator<Item = OutboxDestination>,
    ) -> Self {
        Self {
            store,
            snapshot,
            policy,
            event_sequence,
            destinations: destinations.into_iter().collect(),
        }
    }

    /// Reduce and atomically persist one observation, snapshot, event set, and outbox fan-out.
    ///
    /// The in-memory snapshot advances only after the store transaction commits.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinatorError`] when event allocation or persistence fails.
    pub async fn apply_observation(
        &mut self,
        observation: ObservationEnvelope,
    ) -> Result<ApplyResult, CoordinatorError> {
        if matches!(
            observation.payload(),
            watchdog_domain::ObservationPayload::SchedulerTick
        ) {
            return Err(CoordinatorError::InvalidInput);
        }
        let input = ReducerInput::Observation(observation.clone());
        self.apply_input(observation, input).await
    }

    /// Evaluate and atomically persist one idempotent scheduler tick.
    ///
    /// # Errors
    ///
    /// Returns [`CoordinatorError`] for non-tick payloads, event allocation, or
    /// persistence failure.
    pub async fn apply_tick(
        &mut self,
        observation: ObservationEnvelope,
    ) -> Result<ApplyResult, CoordinatorError> {
        if !matches!(
            observation.payload(),
            watchdog_domain::ObservationPayload::SchedulerTick
        ) {
            return Err(CoordinatorError::InvalidInput);
        }
        let input = ReducerInput::Tick(observation.observed_at());
        self.apply_input(observation, input).await
    }

    async fn apply_input(
        &mut self,
        observation: ObservationEnvelope,
        input: ReducerInput,
    ) -> Result<ApplyResult, CoordinatorError> {
        let output = reduce(self.snapshot.clone(), input, self.policy);
        let events = output
            .events()
            .iter()
            .cloned()
            .map(|kind| {
                Ok(DomainEvent::new(
                    self.event_sequence.next()?,
                    output.snapshot().root(),
                    output.snapshot().session(),
                    observation.observed_at().wall_time(),
                    kind,
                ))
            })
            .collect::<Result<Vec<_>, CoordinatorError>>()?;
        let snapshot_update =
            SnapshotUpdate::from_reducer(output.snapshot()).map_err(StoreError::from)?;
        let apply = ApplyObservation::new(
            observation,
            snapshot_update,
            events,
            self.destinations.iter().copied(),
        );
        let result = self.store.apply_observation(&apply).await?;
        if result == ApplyResult::Applied {
            self.snapshot = output.into_snapshot();
        }
        Ok(result)
    }

    /// Current committed in-memory reducer state.
    #[must_use]
    pub const fn snapshot(&self) -> &SessionSnapshot {
        &self.snapshot
    }
}
