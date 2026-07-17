//! Pure domain contracts for Agent Watchdog.

mod correlation;
mod event;
mod evidence;
mod identity;
mod input;
mod observation;
mod policy;
mod process;
mod reducer;
mod safety;
mod secret;
mod session;
mod state;
mod time;

pub use correlation::{
    CorrelationBasis, CorrelationCandidate, CorrelationDecision, CorrelationPolicy,
    CorrelationPolicyError, CorrelationSelection, correlate,
};
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
pub use reducer::{
    ReducerInput, ReducerOutput, ReducerPolicy, ReducerPolicyError, SessionSnapshot,
    aggregate_main_state, reduce,
};
pub use safety::{
    TerminationActionOutcome, TerminationAssessment, TerminationBlocker, TerminationCandidate,
    TerminationComponent, TerminationFacts, TerminationGate, TerminationHealth, TerminationStage,
    assess_termination, executable_matches_runtime,
};
pub use secret::SecretText;
pub use session::{Capability, CapabilitySet, SessionIdentity, SessionKind};
pub use state::{CompactState, DetailedState};
pub use time::{Clock, IdFactory, TimePoint, WallTimeMs};
