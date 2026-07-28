use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{ChildSessionId, MainSessionId, SessionId};

/// Role of a session within the monitored hierarchy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    /// User-started root that automatic termination can never accept.
    Main,
    /// Delegated child monitored for progress and optional termination.
    Child,
}

/// Role-preserving session identity for generic observations and events.
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
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum SessionIdentity {
    /// Main-session identity.
    Main(MainSessionId),
    /// Child-session identity.
    Child(ChildSessionId),
}

impl SessionIdentity {
    /// Role represented by this identity.
    #[must_use]
    pub const fn kind(self) -> SessionKind {
        match self {
            Self::Main(_) => SessionKind::Main,
            Self::Child(_) => SessionKind::Child,
        }
    }

    /// General session identity without erasing the stored role.
    #[must_use]
    pub const fn session_id(self) -> SessionId {
        match self {
            Self::Main(id) => id.session_id(),
            Self::Child(id) => id.session_id(),
        }
    }
}

/// One adapter capability attached to a discovered session.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// Runtime provides exact parent/child relations.
    ExactHierarchy,
    /// Runtime provides event-driven native lifecycle state.
    NativeEvents,
    /// Correlated process activity is available.
    ProcessEvidence,
    /// Runtime exposes a graceful child cancellation operation.
    GracefulCancel,
    /// Verified host signalling is available for this child type.
    ForceSignal,
    /// Runtime may accept best-effort event hints from the server.
    PushHint,
}

/// Extensible set of adapter capabilities.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct CapabilitySet(BTreeSet<Capability>);

impl CapabilitySet {
    /// Construct a deduplicated capability set.
    #[must_use]
    pub fn new(values: impl IntoIterator<Item = Capability>) -> Self {
        Self(values.into_iter().collect())
    }

    /// Return whether one capability is present.
    #[must_use]
    pub fn contains(&self, capability: Capability) -> bool {
        self.0.contains(&capability)
    }

    /// Iterate over capabilities in deterministic order.
    pub fn iter(&self) -> impl Iterator<Item = Capability> + '_ {
        self.0.iter().copied()
    }
}
