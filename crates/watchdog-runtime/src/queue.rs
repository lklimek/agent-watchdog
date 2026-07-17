use std::{collections::VecDeque, error::Error, fmt};

/// Admission class used by evidence-aware backpressure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationClass {
    /// Repeated progress evidence may coalesce to its newest value.
    Activity,
    /// Terminal, waiting, conflict, disappearance, and safety evidence.
    Durable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueueEntry<T> {
    class: ObservationClass,
    value: T,
}

/// Producer-visible admission failure that returns the unaccepted item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionError<T> {
    /// Durable evidence requires producer backpressure and must be retried.
    Backpressure(T),
    /// No coalescible slot exists; activity loss makes the scope uncertain.
    ActivitySaturated(T),
}

impl<T> fmt::Display for AdmissionError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backpressure(_) => formatter.write_str("durable observation queue is full"),
            Self::ActivitySaturated(_) => formatter.write_str("activity observation queue is full"),
        }
    }
}

impl<T: fmt::Debug> Error for AdmissionError<T> {}

/// A queue cannot enforce a finite bound with zero capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueCapacityError;

impl fmt::Display for QueueCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("session queue capacity must be positive")
    }
}

impl Error for QueueCapacityError {}

/// Bounded per-session queue that coalesces activity and backpressures durable evidence.
#[derive(Clone, Debug)]
pub struct SessionQueue<T> {
    capacity: usize,
    entries: VecDeque<QueueEntry<T>>,
    degraded: bool,
}

impl<T> SessionQueue<T> {
    /// Construct a finite queue.
    ///
    /// # Errors
    ///
    /// Returns [`QueueCapacityError`] when `capacity` is zero.
    pub fn new(capacity: usize) -> Result<Self, QueueCapacityError> {
        if capacity == 0 {
            return Err(QueueCapacityError);
        }
        Ok(Self {
            capacity,
            entries: VecDeque::with_capacity(capacity),
            degraded: false,
        })
    }

    /// Admit an item or return ownership to the producer without silent loss.
    ///
    /// # Errors
    ///
    /// Returns backpressure for durable evidence, or saturation for activity
    /// when the queue contains no coalescible activity slot.
    pub fn try_push(&mut self, class: ObservationClass, value: T) -> Result<(), AdmissionError<T>> {
        if class == ObservationClass::Activity
            && let Some(existing) = self
                .entries
                .iter_mut()
                .rev()
                .find(|entry| entry.class == ObservationClass::Activity)
        {
            existing.value = value;
            return Ok(());
        }
        if self.entries.len() == self.capacity {
            self.degraded = true;
            return Err(match class {
                ObservationClass::Activity => AdmissionError::ActivitySaturated(value),
                ObservationClass::Durable => AdmissionError::Backpressure(value),
            });
        }
        self.entries.push_back(QueueEntry { class, value });
        Ok(())
    }

    /// Remove the oldest accepted value.
    pub fn pop(&mut self) -> Option<T> {
        self.entries.pop_front().map(|entry| entry.value)
    }

    /// Number of accepted entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the queue contains no accepted entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether any admission saturated and destructive automation must pause.
    #[must_use]
    pub const fn is_degraded(&self) -> bool {
        self.degraded
    }

    /// Clear degradation after a successful bounded reconciliation.
    pub const fn mark_reconciled(&mut self) {
        self.degraded = false;
    }
}
