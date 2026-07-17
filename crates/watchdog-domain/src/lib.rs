//! Pure domain contracts for Agent Watchdog.

mod event;
mod evidence;
mod identity;
mod input;
mod observation;
mod policy;
mod process;
mod safety;
mod secret;
mod session;
mod state;
mod time;

pub use event::{DomainEvent, DomainEventKind, EventId};
pub use evidence::{
    AdapterIdentity, Confidence, ConfidenceError, EvidenceTrust, ObservationSource,
};
pub use identity::{
    ChildSessionId, MainSessionId, NativeSessionKey, ObservationId, RuntimeKind, SessionId,
};
pub use input::{BoundedText, DomainInputError};
pub use observation::{ObservationEnvelope, ObservationError, ObservationPayload};
pub use policy::{
    CompatibilityWarning, DeadlineCommand, DeadlinePolicy, DurationMs, PolicyError, WarningKind,
};
pub use process::{ProcessId, ProcessIdError, ProcessIdentity};
pub use safety::TerminationCandidate;
pub use secret::SecretText;
pub use session::{Capability, CapabilitySet, SessionIdentity, SessionKind};
pub use state::{CompactState, DetailedState};
pub use time::{Clock, IdFactory, TimePoint, WallTimeMs};
