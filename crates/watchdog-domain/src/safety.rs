use crate::ChildSessionId;

/// A child identity admitted to the separate termination safety pipeline.
///
/// Main identities cannot construct this type:
///
/// ```compile_fail
/// use uuid::Uuid;
/// use watchdog_domain::{MainSessionId, SessionId, TerminationCandidate};
/// let main = MainSessionId::from(SessionId::from_uuid(Uuid::nil()));
/// let _candidate = TerminationCandidate::new(main);
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TerminationCandidate(ChildSessionId);

impl TerminationCandidate {
    /// Admit a child session to later runtime and evidence gates.
    #[must_use]
    pub const fn new(session_id: ChildSessionId) -> Self {
        Self(session_id)
    }

    /// Return the admitted child identity.
    #[must_use]
    pub const fn session_id(self) -> ChildSessionId {
        self.0
    }
}
