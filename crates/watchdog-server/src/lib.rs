//! Agent Watchdog composition root, protocols, and web experience.

mod agent_api;
mod auth;
mod mcp;
#[cfg(target_os = "linux")]
mod termination;

pub use agent_api::{
    AgentApi, AgentApiError, AgentEventView, AgentHealthView, CompletionOutcome, EventPage,
    RegisterSession, SessionTreeView, SessionView, TransportKey, WaitingKind,
};
pub use auth::{BearerAuthError, BearerAuthenticator, MAX_AUTHORIZATION_BYTES};
pub use mcp::{WatchdogMcpService, WatchdogSessionManager, mcp_router};
#[cfg(target_os = "linux")]
pub use termination::{
    GracefulCancelError, GracefulCancelSupport, GracefulCanceller, NoGracefulCanceller,
    TerminationConfig, TerminationConfigError, TerminationContext, TerminationEngine,
    TerminationEngineError, TerminationStatus, VerifiedChild,
};
