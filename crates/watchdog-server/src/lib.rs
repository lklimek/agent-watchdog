//! Agent Watchdog composition root, protocols, and web experience.

mod agent_api;

pub use agent_api::{
    AgentApi, AgentApiError, CompletionOutcome, EventPage, RegisterSession, SessionView,
    TransportKey, WaitingKind,
};
