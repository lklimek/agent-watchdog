use serde::{Deserialize, Serialize};

use crate::{DetailedState, MainSessionId, SessionIdentity, WallTimeMs};

/// Durable monotonically ordered event identifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct EventId(u64);

impl EventId {
    /// Construct an event identifier assigned by the durable store.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the durable sequence value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Agent- and human-visible domain transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DomainEventKind {
    /// Normalized session state changed.
    StateChanged {
        /// State before the transition.
        from: DetailedState,
        /// State after the transition.
        to: DetailedState,
    },
    /// Adapter compatibility warning changed.
    CompatibilityChanged,
    /// Session hierarchy changed after better evidence.
    HierarchyChanged,
    /// Session crossed the internal corroboration threshold.
    Suspect,
    /// A fresh-check alert should be persisted and delivered.
    AlertDue,
    /// An unresolved alert reached its reminder cadence.
    ReminderDue,
    /// A parent changed the expected check-in policy.
    DeadlineChanged,
    /// Material source conflict started or resolved.
    ConflictChanged {
        /// Whether a conflict is now active.
        active: bool,
    },
    /// Restart recovery requires fresh trustworthy evidence.
    ReconciliationRequired,
}

/// Durable event scoped to one main-session tree.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DomainEvent {
    id: EventId,
    root: MainSessionId,
    subject: SessionIdentity,
    occurred_at: WallTimeMs,
    kind: DomainEventKind,
}

impl DomainEvent {
    /// Construct a durable scoped domain event.
    #[must_use]
    pub const fn new(
        id: EventId,
        root: MainSessionId,
        subject: SessionIdentity,
        occurred_at: WallTimeMs,
        kind: DomainEventKind,
    ) -> Self {
        Self {
            id,
            root,
            subject,
            occurred_at,
            kind,
        }
    }

    /// Durable sequence identity.
    #[must_use]
    pub const fn id(&self) -> EventId {
        self.id
    }

    /// Main-session tree receiving this event.
    #[must_use]
    pub const fn root(&self) -> MainSessionId {
        self.root
    }

    /// Session that changed.
    #[must_use]
    pub const fn subject(&self) -> SessionIdentity {
        self.subject
    }

    /// Persistable transition time.
    #[must_use]
    pub const fn occurred_at(&self) -> WallTimeMs {
        self.occurred_at
    }

    /// Typed event payload.
    #[must_use]
    pub const fn kind(&self) -> &DomainEventKind {
        &self.kind
    }
}
