//! Agent Watchdog composition root, protocols, and web experience.

mod agent_api;
mod auth;
mod clock;
mod config;
mod dashboard;
mod discovery;
mod filesystem_activity;
mod github;
mod health;
mod mcp;
mod notifications;
#[cfg(target_os = "linux")]
mod process_monitor;
mod server;
#[cfg(target_os = "linux")]
mod termination;

pub use agent_api::{
    AgentApi, AgentApiError, AgentEventView, AgentHealthView, CompletionOutcome, DiscoveredSession,
    EventPage, RegisterSession, RepositoryMetadata, SessionTreeView, SessionView,
    TimerReconciliationReport, TransportKey, WaitingKind,
};
pub use auth::{
    BasicAuthError, BasicAuthenticator, BearerAuthError, BearerAuthenticator,
    MAX_AUTHORIZATION_BYTES,
};
pub use clock::SystemClock;
pub use dashboard::{
    DashboardCard, DashboardError, DashboardOutboxDispatcher, DashboardOutboxError, DashboardQuery,
    DashboardScope, DashboardService, DashboardSnapshot, DashboardSort, DashboardWarning,
    dashboard_router,
};
pub use discovery::{
    ClaudeDiscovery, ClaudeDiscoveryReport, CodexDiscovery, CodexDiscoveryReport,
    CompanionDiscovery, CompanionDiscoveryReport, PathMappingError, RuntimeDiscoveryReport,
    WorktreePathMapping,
};
pub use filesystem_activity::{
    FilesystemActivityError, FilesystemActivityReconciler, FilesystemActivityReport,
};
pub use github::{GitHubConfigError, GitHubEnricher, GitHubEnrichment};
pub use health::{HealthComponentView, HealthLevel, HealthService, HealthSnapshot, health_router};
pub use mcp::{WatchdogMcpService, WatchdogSessionManager, mcp_router};
pub use notifications::{
    HumanNotification, HumanNotifier, HumanOutboxDispatcher, NotificationConfigError,
    NotificationDelivery, NotificationDeliveryError, NotificationEndpoints, WebhookEndpoint,
};
pub use server::{ServerError, healthcheck_from_environment, init_tracing, run_from_environment};
#[cfg(target_os = "linux")]
pub use termination::{
    GracefulCancelError, GracefulCancelSupport, GracefulCanceller, NoGracefulCanceller,
    TerminationConfig, TerminationConfigError, TerminationContext, TerminationEngine,
    TerminationEngineError, TerminationStatus, VerifiedChild,
};
