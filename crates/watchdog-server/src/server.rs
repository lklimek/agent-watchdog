use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsString,
    io::{Read as _, Write as _},
    net::{IpAddr, SocketAddr, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::Router;
use thiserror::Error;
use tokio::{
    sync::{Notify, mpsc},
    task::JoinHandle,
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _};
use watchdog_domain::{AdapterIdentity, BoundedText, Clock as _, RuntimeKind, SecretText};
use watchdog_runtime::{
    CapabilityRoot, ComponentId, ComponentStatus, DirectoryScanner, ScanBudget, ScanUncertainty,
    WatchService, WatchSignal, WatchTargetId,
};
use watchdog_store::{AdapterHealthRecord, AdapterHealthStatus, WatchdogStore};

use crate::config::{ConfigManager, RuntimeConfig};
#[cfg(target_os = "linux")]
use crate::process_monitor::ProcessMonitor;
#[cfg(target_os = "linux")]
use crate::termination::TerminationMonitor;
use crate::watch_paths::WatchPathRegistry;
use crate::{
    AgentApi, BasicAuthenticator, BearerAuthenticator, ClaudeDiscovery, ClaudeHookService,
    CodexDiscovery, CodexHookService, CompanionDiscovery, DashboardOutboxDispatcher,
    DashboardService, DiscoveryAliasRegistry, FilesystemActivityReconciler, GitHubEnricher,
    HealthService, HumanNotifier, HumanOutboxDispatcher, NotificationEndpoints, RepositoryMetadata,
    RuntimeDiscoveryReport, SystemClock, TerminationConfig, WebhookEndpoint, claude_hook_router,
    codex_hook_router, dashboard_router, health_router, mcp_router,
};

const MAX_ENV_PATH_BYTES: usize = 4_096;
const WATCH_QUEUE_CAPACITY: usize = 4_096;
const WATCH_TARGET_LIMIT: usize = 4_096;
const WATCH_SCAN_DEPTH: usize = 8;
const WATCH_SCAN_ENTRY_LIMIT: usize = 65_536;
const FILESYSTEM_ACTIVITY_QUEUE_CAPACITY: usize = 64;
const MAX_GITHUB_SESSIONS: u32 = 1_000;
const GITHUB_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(30);
const DASHBOARD_DELIVERY_LIMIT: u32 = 256;
const NOTIFICATION_DELIVERY_LIMIT: u32 = 128;
const PERIODIC_RECONCILIATION: Duration = Duration::from_mins(5);
const DISCOVERY_EVENT_COALESCE: Duration = Duration::from_secs(1);
const TIMER_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(5);
const CLAUDE_DISCOVERY_BIT: u8 = 1 << 0;
const CODEX_DISCOVERY_BIT: u8 = 1 << 1;
const COMPANION_DISCOVERY_BIT: u8 = 1 << 2;
const ALL_DISCOVERY_BITS: u8 = CLAUDE_DISCOVERY_BIT | CODEX_DISCOVERY_BIT | COMPANION_DISCOVERY_BIT;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DiscoveryRequest(u8);

impl DiscoveryRequest {
    const fn none() -> Self {
        Self(0)
    }

    const fn all() -> Self {
        Self(ALL_DISCOVERY_BITS)
    }

    const fn for_runtime(runtime: RuntimeKind) -> Self {
        match runtime {
            RuntimeKind::ClaudeCode => Self(CLAUDE_DISCOVERY_BIT),
            RuntimeKind::CodexCli => Self(CODEX_DISCOVERY_BIT),
            RuntimeKind::CodexCompanion => Self(COMPANION_DISCOVERY_BIT),
            RuntimeKind::OpenCode => Self::none(),
        }
    }

    const fn includes(self, runtime: RuntimeKind) -> bool {
        self.0 & Self::for_runtime(runtime).0 != 0
    }

    const fn is_empty(self) -> bool {
        self.0 == 0
    }

    fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

#[derive(Clone, Default)]
struct DiscoveryScheduler {
    pending: Arc<AtomicU8>,
    wake: Arc<Notify>,
}

impl DiscoveryScheduler {
    fn request(&self, request: DiscoveryRequest) {
        if !request.is_empty() {
            self.pending.fetch_or(request.0, Ordering::AcqRel);
            self.wake.notify_one();
        }
    }

    fn take(&self) -> DiscoveryRequest {
        DiscoveryRequest(self.pending.swap(0, Ordering::AcqRel))
    }
}

/// Initialize structured JSON tracing using `RUST_LOG` or a safe info default.
///
/// # Errors
///
/// Returns [`ServerError`] for an invalid filter or an already installed global
/// subscriber.
pub fn init_tracing() -> Result<(), ServerError> {
    let filter = match env::var("RUST_LOG") {
        Ok(value) => EnvFilter::try_new(value).map_err(|_| ServerError::Logging)?,
        Err(env::VarError::NotPresent) => {
            EnvFilter::try_new("watchdog_server=info").map_err(|_| ServerError::Logging)?
        }
        Err(env::VarError::NotUnicode(_)) => return Err(ServerError::Logging),
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .try_init()
        .map_err(|_| ServerError::Logging)
}

/// Load environment and TOML configuration, then run the composed HTTP server.
///
/// # Errors
///
/// Returns [`ServerError`] when bootstrap validation, critical initialization,
/// binding, or serving fails. Isolated watcher/enrichment failures degrade health.
pub async fn run_from_environment() -> Result<(), ServerError> {
    let bootstrap = BootstrapConfig::from_environment()?;
    run(bootstrap).await
}

/// Probe the local unauthenticated liveness route for container health checks.
///
/// # Errors
///
/// Returns [`ServerError::Healthcheck`] when the configured listen address is
/// invalid, the local socket is unavailable, or the response is not HTTP 200.
pub fn healthcheck_from_environment() -> Result<(), ServerError> {
    let address = env::var("WATCHDOG_LISTEN_ADDRESS")
        .map_err(|_| ServerError::Healthcheck)?
        .parse()
        .map_err(|_| ServerError::Healthcheck)?;
    check_liveness(address)
}

fn check_liveness(address: SocketAddr) -> Result<(), ServerError> {
    let loopback = match address.ip() {
        IpAddr::V4(_) => IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        IpAddr::V6(_) => IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
    };
    let address = SocketAddr::new(loopback, address.port());
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))
        .map_err(|_| ServerError::Healthcheck)?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|_| ServerError::Healthcheck)?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|_| ServerError::Healthcheck)?;
    stream
        .write_all(b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .map_err(|_| ServerError::Healthcheck)?;
    let mut response = [0_u8; 64];
    let size = stream
        .read(&mut response)
        .map_err(|_| ServerError::Healthcheck)?;
    if response[..size].starts_with(b"HTTP/1.1 200 ")
        || response[..size].starts_with(b"HTTP/1.0 200 ")
    {
        Ok(())
    } else {
        Err(ServerError::Healthcheck)
    }
}

fn load_server_config(path: &Path) -> Result<ConfigManager, ServerError> {
    ConfigManager::load(path).map_err(|error| {
        tracing::error!(
            event = "configuration.load_failed",
            error = %error,
            error_variant = ?error,
            "Server configuration could not be loaded"
        );
        ServerError::Configuration
    })
}

async fn run(bootstrap: BootstrapConfig) -> Result<(), ServerError> {
    let config = load_server_config(&bootstrap.config_path)?;
    let current = config.current();
    let clock = Arc::new(SystemClock::new());
    let store = WatchdogStore::open(&bootstrap.database_path).await?;
    let watch_paths = initialize_watch_paths(&store, &clock, &config).await?;
    let health = HealthService::new(Arc::clone(&clock) as Arc<_>);

    let api = initialize_agent_api(&store, &clock, &current).await?;
    record_initial_health(&health, &current);
    configure_agent_api(&api, &health, &watch_paths);

    #[cfg(target_os = "linux")]
    let linux_monitors = initialize_linux_monitors(&api, &store, &clock, &health, &current)?;

    let dashboard = DashboardService::new(store.clone(), Arc::clone(&clock) as Arc<_>);
    let dispatcher = DashboardOutboxDispatcher::new(
        store.clone(),
        dashboard.clone(),
        Arc::clone(&clock) as Arc<_>,
    );
    let notification_dispatcher = initialize_notification_dispatcher(
        &store,
        &clock,
        bootstrap.notification_endpoints.clone(),
    )?;
    health.record(ComponentId::Notifications, ComponentStatus::Healthy, None);
    let github = initialize_github(&bootstrap, &current, &clock)?;

    let router = application_router(
        health.clone(),
        dashboard,
        api.clone(),
        Arc::clone(&clock),
        bootstrap.basic_auth.clone(),
        bootstrap.bearer_auth.clone(),
    );

    let discovery = DiscoveryScheduler::default();
    let filesystem_uncertainty_generation = Arc::new(AtomicU64::new(0));
    let discovery_worker = start_discovery(
        api.clone(),
        DiscoveryWorkerContext {
            config: config.clone(),
            store: store.clone(),
            clock: Arc::clone(&clock),
            health: health.clone(),
            requested: discovery.clone(),
            filesystem_uncertainty_generation: Arc::clone(&filesystem_uncertainty_generation),
            watch_paths: watch_paths.clone(),
        },
    );
    let watcher_stop = Arc::new(AtomicBool::new(false));
    let (filesystem_activity_tx, filesystem_activity_worker) =
        initialize_filesystem_activity(&api, &store, &clock, &health);
    let github_worker =
        github.map(|enricher| spawn_github_worker(enricher, api.clone(), store.clone()));
    let watcher = spawn_watcher_supervisor(
        config.clone(),
        health.clone(),
        Arc::clone(&watcher_stop),
        discovery,
        filesystem_activity_tx,
        filesystem_uncertainty_generation,
        watch_paths,
    );
    let dashboard_worker = spawn_dashboard_worker(dispatcher, health.clone());
    let notification_worker = spawn_notification_worker(notification_dispatcher, health.clone());
    #[cfg(target_os = "linux")]
    let timer_worker = spawn_timer_worker(api.clone(), health.clone(), linux_monitors.process);
    #[cfg(not(target_os = "linux"))]
    let timer_worker = spawn_timer_worker(api.clone(), health.clone());
    #[cfg(target_os = "linux")]
    let termination_worker =
        spawn_termination_worker(linux_monitors.termination, config.clone(), health.clone());
    let reload_worker = spawn_reload_worker(config, api, health.clone())?;

    let listener = tokio::net::TcpListener::bind(bootstrap.listen_address)
        .await
        .map_err(|_| ServerError::Bind)?;
    tracing::info!(
        event = "server.started",
        listen_address = %bootstrap.listen_address,
        "Agent Watchdog server started"
    );
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|_| ServerError::Serve);

    watcher_stop.store(true, Ordering::Release);
    discovery_worker.abort();
    dashboard_worker.abort();
    notification_worker.abort();
    timer_worker.abort();
    filesystem_activity_worker.abort();
    if let Some(worker) = github_worker {
        worker.abort();
    }
    #[cfg(target_os = "linux")]
    termination_worker.abort();
    reload_worker.abort();
    let _ = watcher.await;
    tracing::info!(event = "server.stopped", "Agent Watchdog server stopped");
    result
}

fn configure_agent_api(api: &AgentApi, health: &HealthService, watch_paths: &WatchPathRegistry) {
    api.configure_health(health.clone());
    api.configure_watch_paths(watch_paths.clone());
}

fn record_initial_health(health: &HealthService, config: &RuntimeConfig) {
    health.record(ComponentId::Store, ComponentStatus::Healthy, None);
    health.record(ComponentId::Authorization, ComponentStatus::Healthy, None);
    health.record(ComponentId::Reducer, ComponentStatus::Healthy, None);
    health.record(
        ComponentId::ObservationQueue,
        ComponentStatus::Healthy,
        None,
    );
    record_adapter_health(health, config);
}

fn application_router(
    health: HealthService,
    dashboard: DashboardService,
    api: AgentApi,
    clock: Arc<SystemClock>,
    basic_auth: BasicAuthenticator,
    bearer_auth: BearerAuthenticator,
) -> Router {
    Router::new()
        .merge(health_router(health, basic_auth.clone()))
        .merge(dashboard_router(dashboard, basic_auth))
        .merge(claude_hook_router(
            ClaudeHookService::new(api.clone(), Arc::clone(&clock) as Arc<_>),
            bearer_auth.clone(),
        ))
        .merge(codex_hook_router(
            CodexHookService::new(api.clone(), clock),
            bearer_auth.clone(),
        ))
        .merge(mcp_router(api, bearer_auth))
}

fn initialize_github(
    bootstrap: &BootstrapConfig,
    config: &RuntimeConfig,
    clock: &Arc<SystemClock>,
) -> Result<Option<GitHubEnricher>, ServerError> {
    if !config.github_enabled() {
        return Ok(None);
    }
    GitHubEnricher::new(
        Arc::clone(clock) as Arc<_>,
        bootstrap
            .github_token
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret),
    )
    .map(Some)
    .map_err(|_| ServerError::GitHubConfiguration)
}

fn initialize_filesystem_activity(
    api: &AgentApi,
    store: &WatchdogStore,
    clock: &Arc<SystemClock>,
    health: &HealthService,
) -> (mpsc::Sender<Vec<PathBuf>>, JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(FILESYSTEM_ACTIVITY_QUEUE_CAPACITY);
    let reconciler =
        FilesystemActivityReconciler::new(api.clone(), store.clone(), Arc::clone(clock) as Arc<_>);
    (
        sender,
        spawn_filesystem_activity_worker(reconciler, health.clone(), receiver),
    )
}

fn spawn_github_worker(
    enricher: GitHubEnricher,
    api: AgentApi,
    store: WatchdogStore,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(GITHUB_RECONCILIATION_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Ok((enriched, warnings)) =
                reconcile_github_metadata(&enricher, &api, &store).await
            {
                tracing::debug!(
                    event = "github.reconciled",
                    enriched_sessions = enriched,
                    warnings,
                    "GitHub pull-request metadata was reconciled"
                );
            } else {
                tracing::warn!(
                    event = "github.reconcile_failed",
                    "GitHub metadata could not be loaded; branch fallback remains available"
                );
            }
        }
    })
}

async fn reconcile_github_metadata(
    enricher: &GitHubEnricher,
    api: &AgentApi,
    store: &WatchdogStore,
) -> Result<(u32, u32), ()> {
    let sessions = store
        .sessions_by_kind(watchdog_domain::SessionKind::Main, MAX_GITHUB_SESSIONS)
        .await
        .map_err(|_| ())?;
    let mut enriched_sessions = 0_u32;
    let mut warnings = 0_u32;
    for session in sessions {
        let metadata = store
            .session_metadata(session.session)
            .await
            .map_err(|_| ())?;
        let Some(metadata) = metadata else {
            warnings = warnings.saturating_add(1);
            continue;
        };
        let (Some(remote), Some(branch)) = (metadata.repository_remote(), metadata.branch()) else {
            continue;
        };
        let Ok(github) = enricher.enrich(remote, branch).await else {
            warnings = warnings.saturating_add(1);
            continue;
        };
        let repository = RepositoryMetadata {
            remote: Some(remote.to_owned()),
            branch: Some(github.branch().to_owned()),
            pull_request_number: github.pull_request_number(),
            pull_request_url: github.pull_request_url().map(ToOwned::to_owned),
            replace_pull_request: true,
        };
        if api
            .enrich_repository_metadata(session.session, repository)
            .await
            .is_err()
        {
            warnings = warnings.saturating_add(1);
        } else {
            enriched_sessions = enriched_sessions.saturating_add(1);
        }
    }
    Ok((enriched_sessions, warnings))
}

#[cfg(target_os = "linux")]
struct LinuxMonitors {
    process: ProcessMonitor,
    termination: TerminationMonitor,
}

#[cfg(target_os = "linux")]
fn initialize_linux_monitors(
    api: &AgentApi,
    store: &WatchdogStore,
    clock: &Arc<SystemClock>,
    health: &HealthService,
    config: &RuntimeConfig,
) -> Result<LinuxMonitors, ServerError> {
    let termination_config = TerminationConfig::new(
        config.automation_enabled(),
        config.sigkill_enabled(),
        config.warning_grace(),
        config.action_grace(),
    )
    .map_err(|_| ServerError::Configuration)?;
    let process = initialize_process_monitor(api, store, clock, health)?;
    let termination = TerminationMonitor::new(
        api,
        store.clone(),
        Arc::clone(clock) as Arc<_>,
        health.clone(),
        termination_config,
        config.deadline_policy().terminate_after_stalled(),
    )
    .map_err(|_| ServerError::ProcessSampler)?;
    health.record(
        ComponentId::TerminationAutomation,
        ComponentStatus::Healthy,
        None,
    );
    Ok(LinuxMonitors {
        process,
        termination,
    })
}

#[cfg(target_os = "linux")]
fn spawn_termination_worker(
    mut monitor: TerminationMonitor,
    config: ConfigManager,
    health: HealthService,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TIMER_RECONCILIATION_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let current = config.current();
            let Ok(termination_config) = TerminationConfig::new(
                current.automation_enabled(),
                current.sigkill_enabled(),
                current.warning_grace(),
                current.action_grace(),
            ) else {
                health.record(
                    ComponentId::TerminationAutomation,
                    ComponentStatus::Degraded,
                    Some("Child termination configuration is invalid"),
                );
                continue;
            };
            monitor.update_policy(
                termination_config,
                current.deadline_policy().terminate_after_stalled(),
            );
            if let Ok(report) = monitor.reconcile().await {
                health.record(
                    ComponentId::TerminationAutomation,
                    ComponentStatus::Healthy,
                    None,
                );
                if report.changed_sagas() > 0 {
                    tracing::info!(
                        event = "termination.reconciled",
                        evaluated_children = report.evaluated_children(),
                        changed_sagas = report.changed_sagas(),
                        "Child termination reconciliation completed"
                    );
                }
            } else {
                health.record(
                    ComponentId::TerminationAutomation,
                    ComponentStatus::Degraded,
                    Some("Child termination reconciliation is suspended"),
                );
                tracing::error!(
                    event = "termination.reconcile_failed",
                    "Child termination reconciliation failed safely"
                );
            }
        }
    })
}

fn initialize_notification_dispatcher(
    store: &WatchdogStore,
    clock: &Arc<SystemClock>,
    endpoints: NotificationEndpoints,
) -> Result<HumanOutboxDispatcher, ServerError> {
    let notifier = HumanNotifier::new(store.clone(), Arc::clone(clock) as Arc<_>, endpoints)
        .map_err(|_| ServerError::NotificationConfiguration)?;
    Ok(HumanOutboxDispatcher::new(
        store.clone(),
        Arc::clone(clock) as Arc<_>,
        notifier,
    ))
}

fn spawn_notification_worker(
    dispatcher: HumanOutboxDispatcher,
    health: HealthService,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if dispatcher
                .deliver_pending(NOTIFICATION_DELIVERY_LIMIT)
                .await
                .is_ok()
            {
                health.record(ComponentId::Notifications, ComponentStatus::Healthy, None);
            } else {
                health.record(
                    ComponentId::Notifications,
                    ComponentStatus::Degraded,
                    Some("Human notification delivery is degraded"),
                );
                tracing::warn!(
                    event = "notifications.delivery_failed",
                    "Human notification delivery failed"
                );
            }
        }
    })
}

fn spawn_timer_worker(
    api: AgentApi,
    health: HealthService,
    #[cfg(target_os = "linux")] process_monitor: ProcessMonitor,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TIMER_RECONCILIATION_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            #[cfg(target_os = "linux")]
            reconcile_process_before_timers(&process_monitor, &health).await;
            if let Ok(report) = api.reconcile_timers().await {
                health.record(ComponentId::Reducer, ComponentStatus::Healthy, None);
                if report.changed_sessions() > 0 {
                    tracing::info!(
                        event = "scheduler.reconciled",
                        evaluated_sessions = report.evaluated_sessions(),
                        changed_sessions = report.changed_sessions(),
                        "Session timer reconciliation completed"
                    );
                }
            } else {
                health.record(
                    ComponentId::Reducer,
                    ComponentStatus::Failed,
                    Some("Session timer reconciliation failed"),
                );
                tracing::error!(
                    event = "scheduler.reconcile_failed",
                    "Session timer reconciliation failed"
                );
            }
        }
    })
}

async fn initialize_agent_api(
    store: &WatchdogStore,
    clock: &Arc<SystemClock>,
    config: &RuntimeConfig,
) -> Result<AgentApi, ServerError> {
    let api = AgentApi::with_policy(
        store.clone(),
        Arc::clone(clock) as Arc<_>,
        config.reducer_policy(),
    )
    .await?;
    api.mark_restarted().await?;
    Ok(api)
}

async fn initialize_watch_paths(
    store: &WatchdogStore,
    clock: &Arc<SystemClock>,
    config: &ConfigManager,
) -> Result<WatchPathRegistry, ServerError> {
    WatchPathRegistry::load(store.clone(), Arc::clone(clock) as Arc<_>, config.clone())
        .await
        .map_err(|_| ServerError::WatchPathRegistry)
}

#[cfg(target_os = "linux")]
fn initialize_process_monitor(
    api: &AgentApi,
    store: &WatchdogStore,
    clock: &Arc<SystemClock>,
    health: &HealthService,
) -> Result<ProcessMonitor, ServerError> {
    let monitor = ProcessMonitor::new(api.clone(), store.clone(), Arc::clone(clock) as Arc<_>)
        .map_err(|_| ServerError::ProcessSampler)?;
    health.record(ComponentId::ProcessSampler, ComponentStatus::Healthy, None);
    Ok(monitor)
}

#[cfg(target_os = "linux")]
async fn reconcile_process_before_timers(monitor: &ProcessMonitor, health: &HealthService) {
    match monitor.reconcile().await {
        Ok(report) if report.warning_count() == 0 => {
            health.record(ComponentId::ProcessSampler, ComponentStatus::Healthy, None);
            if report.progressed_sessions() > 0 {
                tracing::info!(
                    event = "process.reconciled",
                    monitored_sessions = report.monitored_sessions(),
                    progressed_sessions = report.progressed_sessions(),
                    warnings = 0,
                    "Process activity reconciliation completed"
                );
            }
        }
        Ok(report) => {
            health.record(
                ComponentId::ProcessSampler,
                ComponentStatus::Degraded,
                Some("Some verified process trees could not be sampled safely"),
            );
            tracing::warn!(
                event = "process.reconciled",
                monitored_sessions = report.monitored_sessions(),
                progressed_sessions = report.progressed_sessions(),
                warnings = report.warning_count(),
                "Process activity reconciliation was incomplete"
            );
        }
        Err(_) => {
            health.record(
                ComponentId::ProcessSampler,
                ComponentStatus::Failed,
                Some("Process activity reconciliation failed"),
            );
            tracing::error!(
                event = "process.reconcile_failed",
                "Process activity reconciliation failed"
            );
        }
    }
}

struct DiscoveryWorkerContext {
    config: ConfigManager,
    store: WatchdogStore,
    clock: Arc<SystemClock>,
    health: HealthService,
    requested: DiscoveryScheduler,
    filesystem_uncertainty_generation: Arc<AtomicU64>,
    watch_paths: WatchPathRegistry,
}

fn start_discovery(api: AgentApi, context: DiscoveryWorkerContext) -> JoinHandle<()> {
    let aliases = DiscoveryAliasRegistry::default();
    let discoveries = RuntimeDiscoveries {
        claude: ClaudeDiscovery::with_alias_registry(
            api.clone(),
            context.store.clone(),
            context.clock.clone(),
            aliases.clone(),
        ),
        codex: CodexDiscovery::new(api.clone(), context.store.clone(), context.clock.clone()),
        companion: CompanionDiscovery::with_alias_registry(
            api,
            context.store.clone(),
            context.clock.clone(),
            aliases,
        ),
    };
    spawn_discovery_worker(discoveries, context)
}

struct RuntimeDiscoveries {
    claude: ClaudeDiscovery,
    codex: CodexDiscovery,
    companion: CompanionDiscovery,
}

fn spawn_discovery_worker(
    discoveries: RuntimeDiscoveries,
    context: DiscoveryWorkerContext,
) -> JoinHandle<()> {
    let DiscoveryWorkerContext {
        config,
        store,
        clock,
        health,
        requested,
        filesystem_uncertainty_generation,
        watch_paths,
    } = context;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PERIODIC_RECONCILIATION);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let request = tokio::select! {
                _ = interval.tick() => {
                    let _ = requested.take();
                    DiscoveryRequest::all()
                }
                () = requested.wake.notified() => {
                    tokio::time::sleep(DISCOVERY_EVENT_COALESCE).await;
                    requested.take()
                },
            };
            if request.is_empty() {
                continue;
            }
            let reconciliation_generation =
                filesystem_uncertainty_generation.load(Ordering::Acquire);
            let current = config.current();
            if request.includes(RuntimeKind::ClaudeCode) && current.adapters().claude() {
                let report = discoveries
                    .claude
                    .reconcile(
                        current.claude_roots(),
                        current.runtime_path_mappings(RuntimeKind::ClaudeCode),
                        current.worktree_mappings(),
                    )
                    .await;
                record_discovery_health(
                    &store,
                    &clock,
                    &health,
                    RuntimeKind::ClaudeCode,
                    watchdog_claude::TESTED_CLAUDE_VERSION,
                    report,
                )
                .await;
            }
            if request.includes(RuntimeKind::CodexCli) && current.adapters().codex() {
                let report = discoveries
                    .codex
                    .reconcile(
                        current.codex_roots(),
                        current.runtime_path_mappings(RuntimeKind::CodexCli),
                        current.worktree_mappings(),
                    )
                    .await;
                record_discovery_health(
                    &store,
                    &clock,
                    &health,
                    RuntimeKind::CodexCli,
                    watchdog_codex::TESTED_CODEX_VERSION,
                    report,
                )
                .await;
            }
            if request.includes(RuntimeKind::CodexCompanion) && current.adapters().companion() {
                let report = discoveries
                    .companion
                    .reconcile(current.companion_roots(), current.worktree_mappings())
                    .await;
                record_discovery_health(
                    &store,
                    &clock,
                    &health,
                    RuntimeKind::CodexCompanion,
                    watchdog_companion::TESTED_COMPANION_VERSION,
                    report,
                )
                .await;
            }
            if watch_paths.refresh_active().await.is_err() {
                health.record(
                    ComponentId::Watcher,
                    ComponentStatus::Degraded,
                    Some("Active worktree watch paths could not be refreshed"),
                );
                tracing::warn!(
                    event = "watcher.active_paths_refresh_failed",
                    "Active worktree watch paths could not be refreshed"
                );
            }
            mark_filesystem_reconciliation_complete(
                &health,
                &filesystem_uncertainty_generation,
                reconciliation_generation,
            );
        }
    })
}

fn mark_filesystem_reconciliation_complete(
    health: &HealthService,
    uncertainty_generation: &AtomicU64,
    reconciliation_generation: u64,
) {
    if uncertainty_generation.load(Ordering::Acquire) == reconciliation_generation {
        health.record(
            ComponentId::FilesystemReconciliation,
            ComponentStatus::Healthy,
            None,
        );
    }
}

async fn record_discovery_health(
    store: &WatchdogStore,
    clock: &SystemClock,
    health: &HealthService,
    runtime: RuntimeKind,
    tested_version: &'static str,
    report: RuntimeDiscoveryReport,
) {
    let now = clock.now().wall_time();
    let (status, component, message) = if report.is_degraded() {
        (
            AdapterHealthStatus::Degraded,
            ComponentStatus::Degraded,
            Some("Some runtime records could not be reconciled safely"),
        )
    } else {
        (AdapterHealthStatus::Healthy, ComponentStatus::Healthy, None)
    };
    health.record(ComponentId::Adapter(runtime), component, message);
    let Ok(adapter) = AdapterIdentity::new(runtime, tested_version) else {
        return;
    };
    let record = AdapterHealthRecord {
        adapter,
        status,
        last_success: Some(now),
        last_error: report.is_degraded().then_some(now),
        affected_scope: if report.is_degraded() {
            BoundedText::new("affected_scope", "configured runtime roots").ok()
        } else {
            None
        },
        message: message
            .and_then(|message| BoundedText::new("adapter_health_message", message).ok()),
    };
    if store.save_adapter_health(&record).await.is_err() {
        health.record(
            ComponentId::Adapter(runtime),
            ComponentStatus::Degraded,
            Some("Runtime discovery health could not be persisted"),
        );
        tracing::warn!(
            event = "adapter.health_persist_failed",
            runtime = runtime.as_str(),
            "Adapter health persistence failed"
        );
    } else {
        tracing::debug!(
            event = "adapter.reconciled",
            runtime = runtime.as_str(),
            main_sessions = report.main_sessions(),
            child_sessions = report.child_sessions(),
            warnings = report.warning_count(),
            "Runtime adapter reconciliation completed"
        );
    }
}

fn record_adapter_health(health: &HealthService, config: &RuntimeConfig) {
    for (enabled, runtime) in [
        (config.adapters().claude(), RuntimeKind::ClaudeCode),
        (config.adapters().codex(), RuntimeKind::CodexCli),
        (config.adapters().companion(), RuntimeKind::CodexCompanion),
    ] {
        if enabled {
            health.record(
                ComponentId::Adapter(runtime),
                ComponentStatus::Degraded,
                Some("Adapter discovery is awaiting its first reconciliation"),
            );
        }
    }
}

fn spawn_dashboard_worker(
    dispatcher: DashboardOutboxDispatcher,
    health: HealthService,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            match dispatcher.deliver_pending(DASHBOARD_DELIVERY_LIMIT).await {
                Ok(_) => health.record(
                    ComponentId::DashboardDelivery,
                    ComponentStatus::Healthy,
                    None,
                ),
                Err(error) => {
                    tracing::warn!(
                        event = "dashboard.delivery_failed",
                        error = %error,
                        "Dashboard event delivery failed"
                    );
                    health.record(
                        ComponentId::DashboardDelivery,
                        ComponentStatus::Degraded,
                        Some("Dashboard delivery is degraded"),
                    );
                }
            }
        }
    })
}

fn spawn_reload_worker(
    config: ConfigManager,
    api: AgentApi,
    health: HealthService,
) -> Result<JoinHandle<()>, ServerError> {
    #[cfg(unix)]
    {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
            .map_err(|_| ServerError::Signal)?;
        Ok(tokio::spawn(async move {
            while signal.recv().await.is_some() {
                if let Ok(candidate) = config.reload() {
                    api.update_policy(candidate.reducer_policy()).await;
                    record_adapter_health(&health, &candidate);
                    health.record_configuration_warning(None);
                    tracing::info!(event = "config.reload_succeeded", "Configuration reloaded");
                } else {
                    health.record_configuration_warning(config.last_reload_error().as_deref());
                    tracing::warn!(
                        event = "config.reload_rejected",
                        "Configuration reload rejected"
                    );
                }
            }
        }))
    }
    #[cfg(not(unix))]
    {
        let _ = (config, api, health);
        Ok(tokio::spawn(std::future::pending()))
    }
}

fn spawn_watcher_supervisor(
    config: ConfigManager,
    health: HealthService,
    stop: Arc<AtomicBool>,
    discovery: DiscoveryScheduler,
    filesystem_activity: mpsc::Sender<Vec<PathBuf>>,
    filesystem_uncertainty_generation: Arc<AtomicU64>,
    watch_paths: WatchPathRegistry,
) -> JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let mut applied = config.current();
        let mut applied_watch_generation = watch_paths.generation();
        let mut watcher = build_watcher(&applied, &health, &watch_paths);
        discovery.request(DiscoveryRequest::all());
        while !stop.load(Ordering::Acquire) {
            let candidate = config.current();
            let watch_generation = watch_paths.generation();
            if !Arc::ptr_eq(&applied, &candidate) || applied_watch_generation != watch_generation {
                watcher = build_watcher(&candidate, &health, &watch_paths);
                applied = candidate;
                applied_watch_generation = watch_generation;
                discovery.request(DiscoveryRequest::all());
            }
            if let Some(signal) = watcher
                .as_ref()
                .and_then(|registry| registry.service.next_signal())
            {
                match signal {
                    WatchSignal::Targets(targets) => {
                        reconcile_watch_targets(
                            watcher.as_ref(),
                            &targets,
                            &health,
                            &discovery,
                            &filesystem_activity,
                            &filesystem_uncertainty_generation,
                        );
                    }
                    WatchSignal::TopologyChanged(targets) => {
                        reconcile_watch_targets(
                            watcher.as_ref(),
                            &targets,
                            &health,
                            &discovery,
                            &filesystem_activity,
                            &filesystem_uncertainty_generation,
                        );
                        watcher = build_watcher(&applied, &health, &watch_paths);
                    }
                    WatchSignal::ReconcileAll(_) => {
                        filesystem_uncertainty_generation.fetch_add(1, Ordering::AcqRel);
                        health.record(
                            ComponentId::FilesystemReconciliation,
                            ComponentStatus::Degraded,
                            Some("Filesystem events were lost; reconciliation is required"),
                        );
                        discovery.request(DiscoveryRequest::all());
                    }
                }
            }
            std::thread::park_timeout(Duration::from_millis(100));
        }
    })
}

fn reconcile_watch_targets(
    watcher: Option<&WatchRegistry>,
    targets: &[WatchTargetId],
    health: &HealthService,
    discovery: &DiscoveryScheduler,
    filesystem_activity: &mpsc::Sender<Vec<PathBuf>>,
    filesystem_uncertainty_generation: &AtomicU64,
) {
    let actions = watcher.map_or_else(WatchTargetActions::default, |registry| {
        registry.routes.actions(targets)
    });
    discovery.request(actions.discovery);
    if !actions.worktree_paths.is_empty()
        && filesystem_activity
            .try_send(actions.worktree_paths)
            .is_err()
    {
        filesystem_uncertainty_generation.fetch_add(1, Ordering::AcqRel);
        health.record(
            ComponentId::FilesystemReconciliation,
            ComponentStatus::Degraded,
            Some("Filesystem activity queue is saturated"),
        );
    }
}

fn spawn_filesystem_activity_worker(
    reconciler: FilesystemActivityReconciler,
    health: HealthService,
    mut requests: mpsc::Receiver<Vec<PathBuf>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(paths) = requests.recv().await {
            match reconciler.reconcile(&paths).await {
                Ok(report) if report.warnings() == 0 => {
                    if report.attributed() > 0 {
                        tracing::debug!(
                            event = "filesystem_activity.reconciled",
                            attributed_sessions = report.attributed(),
                            ambiguous_paths = report.ambiguous(),
                            unowned_paths = report.unowned(),
                            "Worktree activity was reconciled"
                        );
                    }
                }
                Ok(report) => {
                    health.record(
                        ComponentId::Watcher,
                        ComponentStatus::Degraded,
                        Some("Some filesystem activity could not be attributed safely"),
                    );
                    tracing::warn!(
                        event = "filesystem_activity.incomplete",
                        attributed_sessions = report.attributed(),
                        ambiguous_paths = report.ambiguous(),
                        unowned_paths = report.unowned(),
                        warnings = report.warnings(),
                        "Worktree activity reconciliation was incomplete"
                    );
                }
                Err(_) => {
                    health.record(
                        ComponentId::Watcher,
                        ComponentStatus::Degraded,
                        Some("Filesystem activity ownership could not be loaded"),
                    );
                    tracing::warn!(
                        event = "filesystem_activity.failed",
                        "Worktree activity reconciliation failed"
                    );
                }
            }
        }
    })
}

#[derive(Default)]
struct WatchTargetRoutes {
    runtimes: BTreeMap<WatchTargetId, RuntimeKind>,
    worktree_paths: BTreeMap<WatchTargetId, PathBuf>,
}

impl WatchTargetRoutes {
    fn actions(&self, targets: &[WatchTargetId]) -> WatchTargetActions {
        let mut discovery = DiscoveryRequest::none();
        let mut worktree_paths = BTreeSet::new();
        for target in targets {
            if let Some(runtime) = self.runtimes.get(target) {
                discovery.insert(DiscoveryRequest::for_runtime(*runtime));
            }
            if let Some(path) = self.worktree_paths.get(target) {
                worktree_paths.insert(path.clone());
            }
        }
        WatchTargetActions {
            discovery,
            worktree_paths: worktree_paths.into_iter().collect(),
        }
    }
}

#[derive(Default)]
struct WatchTargetActions {
    discovery: DiscoveryRequest,
    worktree_paths: Vec<PathBuf>,
}

struct WatchRegistry {
    service: WatchService,
    routes: WatchTargetRoutes,
}

fn build_watcher(
    config: &RuntimeConfig,
    health: &HealthService,
    registered: &WatchPathRegistry,
) -> Option<WatchRegistry> {
    build_watcher_with_limit(config, health, registered, WATCH_TARGET_LIMIT)
}

fn build_watcher_with_limit(
    config: &RuntimeConfig,
    health: &HealthService,
    registered: &WatchPathRegistry,
    target_limit: usize,
) -> Option<WatchRegistry> {
    let Ok(mut watcher) = WatchService::new(WATCH_QUEUE_CAPACITY) else {
        health.record(
            ComponentId::Watcher,
            ComponentStatus::Degraded,
            Some("Filesystem watch backend is unavailable"),
        );
        return None;
    };
    let Ok(scan_budget) =
        ScanBudget::new(WATCH_SCAN_DEPTH, WATCH_SCAN_ENTRY_LIMIT, 4 * 1_024 * 1_024)
    else {
        health.record(
            ComponentId::Watcher,
            ComponentStatus::Degraded,
            Some("Filesystem watch target budget is invalid"),
        );
        return None;
    };
    let scanner = DirectoryScanner::new(scan_budget);
    let mut next_target = 1_u64;
    let mut routes = WatchTargetRoutes::default();
    let mut degraded = false;
    let runtime_roots = [
        (
            config.adapters().claude(),
            RuntimeKind::ClaudeCode,
            config.claude_roots(),
        ),
        (
            config.adapters().codex(),
            RuntimeKind::CodexCli,
            config.codex_roots(),
        ),
        (
            config.adapters().companion(),
            RuntimeKind::CodexCompanion,
            config.companion_roots(),
        ),
    ];
    for descendants_only in [false, true] {
        for (enabled, runtime, roots) in runtime_roots {
            if !enabled {
                continue;
            }
            add_runtime_watch_paths(
                &mut watcher,
                config,
                runtime,
                roots,
                &scanner,
                target_limit,
                descendants_only,
                &mut next_target,
                &mut routes,
                &mut degraded,
            );
        }
    }
    add_exact_worktree_watch_paths(
        &mut watcher,
        config,
        registered,
        &scanner,
        target_limit,
        &mut next_target,
        &mut routes,
        &mut degraded,
    );
    if degraded {
        health.record(
            ComponentId::Watcher,
            ComponentStatus::Degraded,
            Some("Some configured filesystem directories could not be watched within bounds"),
        );
    } else {
        health.record(ComponentId::Watcher, ComponentStatus::Healthy, None);
    }
    Some(WatchRegistry {
        service: watcher,
        routes,
    })
}

#[allow(clippy::too_many_arguments)]
fn add_runtime_watch_paths(
    watcher: &mut WatchService,
    config: &RuntimeConfig,
    runtime: RuntimeKind,
    roots: &[PathBuf],
    scanner: &DirectoryScanner,
    target_limit: usize,
    descendants_only: bool,
    next_target: &mut u64,
    routes: &mut WatchTargetRoutes,
    degraded: &mut bool,
) {
    let exclusions = config.exclusions();
    for configured_root in roots
        .iter()
        .filter(|root| !exclusions.iter().any(|excluded| root.starts_with(excluded)))
    {
        let Ok(root) = CapabilityRoot::new(configured_root.clone()) else {
            *degraded = true;
            log_runtime_watch_issue(runtime, "capability_root_rejected");
            continue;
        };
        let candidates = if descendants_only {
            let scan = scanner.scan(&root, Path::new("."));
            if let Ok(scan) = scan {
                if let Some(uncertainty) = scan.uncertainty() {
                    *degraded = true;
                    log_runtime_watch_issue(runtime, watch_uncertainty_code(uncertainty));
                }
                scan.directories().to_vec()
            } else {
                *degraded = true;
                log_runtime_watch_issue(runtime, "scan_failed");
                Vec::new()
            }
        } else {
            vec![root.path().to_owned()]
        };
        for directory in candidates.into_iter().filter(|directory| {
            !exclusions
                .iter()
                .any(|excluded| directory.starts_with(excluded))
        }) {
            if target_budget_exhausted(*next_target, target_limit) {
                *degraded = true;
                log_runtime_watch_issue(runtime, "target_budget");
                break;
            }
            let Ok(relative) = directory.strip_prefix(root.path()) else {
                *degraded = true;
                continue;
            };
            let target = WatchTargetId::new(*next_target);
            *next_target = (*next_target).saturating_add(1);
            let Ok(target) = target else {
                *degraded = true;
                continue;
            };
            if watcher.add_target(target, &root, relative).is_err() {
                *degraded = true;
                log_runtime_watch_issue(runtime, "backend_rejected_target");
                continue;
            }
            routes.runtimes.insert(target, runtime);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn add_exact_worktree_watch_paths(
    watcher: &mut WatchService,
    config: &RuntimeConfig,
    registered: &WatchPathRegistry,
    scanner: &DirectoryScanner,
    target_limit: usize,
    next_target: &mut u64,
    routes: &mut WatchTargetRoutes,
    degraded: &mut bool,
) {
    let Ok((paths, incomplete)) = registered.projected_paths(config) else {
        *degraded = true;
        log_worktree_watch_issue("registry_unavailable");
        return;
    };
    *degraded |= incomplete;
    if incomplete {
        log_worktree_watch_issue("projection_incomplete");
    }
    for projected in paths {
        if target_budget_exhausted(*next_target, target_limit) {
            *degraded = true;
            log_worktree_watch_issue("target_budget");
            break;
        }
        let directory = projected.mounted_path();
        let Some(mapping) = config
            .worktree_mappings()
            .iter()
            .filter(|mapping| directory.starts_with(mapping.mounted_root()))
            .max_by_key(|mapping| mapping.mounted_root().components().count())
        else {
            *degraded = true;
            log_worktree_watch_issue("mapping_unavailable");
            continue;
        };
        let Ok(root) = CapabilityRoot::new(mapping.mounted_root()) else {
            *degraded = true;
            log_worktree_watch_issue("capability_root_rejected");
            continue;
        };
        let Ok(relative) = directory.strip_prefix(root.path()) else {
            *degraded = true;
            continue;
        };
        let scan = scanner.scan(&root, relative);
        let directories = if let Ok(scan) = scan {
            if let Some(uncertainty) = scan.uncertainty() {
                *degraded = true;
                log_worktree_watch_issue(watch_uncertainty_code(uncertainty));
            }
            scan.directories().to_vec()
        } else {
            *degraded = true;
            log_worktree_watch_issue("scan_failed");
            Vec::new()
        };
        let candidates = std::iter::once(directory.to_path_buf()).chain(directories);
        for candidate in candidates.filter(|candidate| {
            !config
                .exclusions()
                .iter()
                .any(|excluded| candidate.starts_with(excluded))
        }) {
            if target_budget_exhausted(*next_target, target_limit) {
                *degraded = true;
                log_worktree_watch_issue("target_budget");
                break;
            }
            let Ok(relative) = candidate.strip_prefix(root.path()) else {
                *degraded = true;
                continue;
            };
            let Ok(target) = WatchTargetId::new(*next_target) else {
                *degraded = true;
                continue;
            };
            *next_target = (*next_target).saturating_add(1);
            if watcher.add_target(target, &root, relative).is_err() {
                *degraded = true;
                log_worktree_watch_issue("backend_rejected_target");
                continue;
            }
            let native_path = if candidate == directory {
                Some(PathBuf::from(projected.native_path()))
            } else {
                native_worktree_path(&candidate, config.worktree_mappings())
            };
            if let Some(native_path) = native_path {
                routes.worktree_paths.insert(target, native_path);
            } else {
                *degraded = true;
            }
        }
    }
}

fn target_budget_exhausted(next_target: u64, target_limit: usize) -> bool {
    usize::try_from(next_target).map_or(true, |target| target > target_limit)
}

fn log_runtime_watch_issue(runtime: RuntimeKind, reason: &'static str) {
    tracing::warn!(
        event = "watcher.coverage_incomplete",
        scope = "runtime",
        runtime = runtime.as_str(),
        reason,
        "Runtime watch coverage is incomplete"
    );
}

fn log_worktree_watch_issue(reason: &'static str) {
    tracing::warn!(
        event = "watcher.coverage_incomplete",
        scope = "worktree",
        reason,
        "Exact worktree watch coverage is incomplete"
    );
}

const fn watch_uncertainty_code(uncertainty: ScanUncertainty) -> &'static str {
    match uncertainty {
        ScanUncertainty::DepthBudget => "depth_budget",
        ScanUncertainty::EntryBudget => "entry_budget",
        ScanUncertainty::PathByteBudget => "path_byte_budget",
        ScanUncertainty::TimeBudget => "time_budget",
        ScanUncertainty::PathRace => "path_race",
    }
}

fn native_worktree_path(
    mounted_path: &Path,
    mappings: &[crate::WorktreePathMapping],
) -> Option<PathBuf> {
    mappings
        .iter()
        .filter_map(|mapping| {
            let relative = mounted_path.strip_prefix(mapping.mounted_root()).ok()?;
            Some((
                mapping.mounted_root().components().count(),
                mapping.native_root().join(relative),
            ))
        })
        .max_by_key(|(specificity, _)| *specificity)
        .map(|(_, path)| path)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = async {
            if let Ok(mut signal) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                signal.recv().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            () = terminate => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[derive(Clone)]
struct BootstrapConfig {
    config_path: PathBuf,
    database_path: PathBuf,
    listen_address: SocketAddr,
    basic_auth: BasicAuthenticator,
    bearer_auth: BearerAuthenticator,
    github_token: Option<SecretText>,
    notification_endpoints: NotificationEndpoints,
}

impl std::fmt::Debug for BootstrapConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BootstrapConfig")
            .field("listen_address", &self.listen_address)
            .finish_non_exhaustive()
    }
}

impl BootstrapConfig {
    fn from_environment() -> Result<Self, ServerError> {
        Self::from_getter(|name| env::var_os(name))
    }

    fn from_getter(mut get: impl FnMut(&str) -> Option<OsString>) -> Result<Self, ServerError> {
        let config_path = required_path(&mut get, "WATCHDOG_CONFIG_PATH")?;
        let database_path = required_path(&mut get, "WATCHDOG_DATABASE_PATH")?;
        let listen_address = required(&mut get, "WATCHDOG_LISTEN_ADDRESS")?
            .parse()
            .map_err(|_| ServerError::InvalidEnvironment("WATCHDOG_LISTEN_ADDRESS"))?;
        let username = required(&mut get, "WATCHDOG_BASIC_USERNAME")?;
        let password = required(&mut get, "WATCHDOG_BASIC_PASSWORD")?;
        let bearer = required(&mut get, "WATCHDOG_BEARER_TOKEN")?;
        let basic_auth = BasicAuthenticator::new(&username, &password)
            .map_err(|_| ServerError::InvalidEnvironment("WATCHDOG_BASIC_USERNAME/PASSWORD"))?;
        let bearer_auth = BearerAuthenticator::new(bearer)
            .map_err(|_| ServerError::InvalidEnvironment("WATCHDOG_BEARER_TOKEN"))?;
        let github_token = optional(&mut get, "WATCHDOG_GITHUB_TOKEN")?.map(SecretText::new);
        let home_assistant = optional(&mut get, "WATCHDOG_HOME_ASSISTANT_WEBHOOK")?
            .map(WebhookEndpoint::new)
            .transpose()
            .map_err(|_| ServerError::InvalidEnvironment("WATCHDOG_HOME_ASSISTANT_WEBHOOK"))?;
        let webhook = optional(&mut get, "WATCHDOG_WEBHOOK")?
            .map(WebhookEndpoint::new)
            .transpose()
            .map_err(|_| ServerError::InvalidEnvironment("WATCHDOG_WEBHOOK"))?;
        Ok(Self {
            config_path,
            database_path,
            listen_address,
            basic_auth,
            bearer_auth,
            github_token,
            notification_endpoints: NotificationEndpoints::new(home_assistant, webhook),
        })
    }
}

fn required_path(
    get: &mut impl FnMut(&str) -> Option<OsString>,
    name: &'static str,
) -> Result<PathBuf, ServerError> {
    let value = required(get, name)?;
    if value.len() > MAX_ENV_PATH_BYTES {
        return Err(ServerError::InvalidEnvironment(name));
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(ServerError::InvalidEnvironment(name));
    }
    Ok(path)
}

fn required(
    get: &mut impl FnMut(&str) -> Option<OsString>,
    name: &'static str,
) -> Result<String, ServerError> {
    get(name)
        .ok_or(ServerError::MissingEnvironment(name))?
        .into_string()
        .map_err(|_| ServerError::InvalidEnvironment(name))
}

fn optional(
    get: &mut impl FnMut(&str) -> Option<OsString>,
    name: &'static str,
) -> Result<Option<String>, ServerError> {
    get(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| ServerError::InvalidEnvironment(name))
        })
        .transpose()
        .map(|value| value.filter(|value| !value.is_empty()))
}

/// Bounded process bootstrap or serving failure.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Required bootstrap variable is absent.
    #[error("Required environment variable {0} is missing")]
    MissingEnvironment(&'static str),
    /// Bootstrap variable is malformed; its value is never reflected.
    #[error("Environment variable {0} is invalid")]
    InvalidEnvironment(&'static str),
    /// Mounted TOML could not be loaded or validated.
    #[error("Server configuration is invalid")]
    Configuration,
    /// Structured logging could not initialize.
    #[error("Structured logging initialization failed")]
    Logging,
    /// `SQLite` initialization or operation failed.
    #[error(transparent)]
    Store(#[from] watchdog_store::StoreError),
    /// Durable reducer/API initialization failed.
    #[error(transparent)]
    AgentApi(#[from] crate::AgentApiError),
    /// Linux process sampling could not initialize.
    #[error("Linux process sampler initialization failed")]
    ProcessSampler,
    /// Webhook client configuration failed.
    #[error("Human notification configuration is invalid")]
    NotificationConfiguration,
    /// Durable agent watch-path state could not be restored safely.
    #[error("Registered watch-path state is unavailable")]
    WatchPathRegistry,
    /// GitHub enrichment client configuration failed.
    #[error("GitHub enrichment configuration is invalid")]
    GitHubConfiguration,
    /// OS signal subscription failed.
    #[error("Shutdown or reload signal initialization failed")]
    Signal,
    /// Listen socket could not bind.
    #[error("HTTP listen socket could not bind")]
    Bind,
    /// HTTP serving failed.
    #[error("HTTP server failed")]
    Serve,
    /// Local liveness health check failed.
    #[error("Local liveness health check failed")]
    Healthcheck,
}

impl ServerError {
    /// Stable bounded code suitable for structured logs.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::MissingEnvironment(_) => "missing_environment",
            Self::InvalidEnvironment(_) => "invalid_environment",
            Self::Configuration => "configuration",
            Self::Logging => "logging",
            Self::Store(_) => "store",
            Self::AgentApi(_) => "agent_api",
            Self::ProcessSampler => "process_sampler",
            Self::NotificationConfiguration => "notification_configuration",
            Self::WatchPathRegistry => "watch_path_registry",
            Self::GitHubConfiguration => "github_configuration",
            Self::Signal => "signal",
            Self::Bind => "bind",
            Self::Serve => "serve",
            Self::Healthcheck => "healthcheck",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        ffi::OsString,
        io::{Read as _, Write as _},
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        thread,
    };

    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use watchdog_domain::{RuntimeKind, TimePoint, WallTimeMs};
    use watchdog_runtime::{ComponentId, ComponentStatus};
    use watchdog_testkit::FakeClock;

    use super::{
        BootstrapConfig, ConfigManager, DiscoveryRequest, WatchTargetRoutes,
        build_watcher_with_limit, check_liveness, mark_filesystem_reconciliation_complete,
        native_worktree_path,
    };
    use crate::{HealthService, watch_paths::WatchPathRegistry};

    #[test]
    fn internal_healthcheck_accepts_only_a_successful_liveness_response() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener");
        let address = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("healthcheck connection");
            let mut request = [0_u8; 256];
            let size = stream.read(&mut request).expect("healthcheck request");
            assert!(request[..size].starts_with(b"GET /health/live HTTP/1.1\r\n"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .expect("healthcheck response");
        });

        check_liveness(address).expect("successful liveness response");
        server.join().expect("healthcheck server");
    }

    #[test]
    fn bootstrap_snapshots_environment_and_redacts_secrets() {
        let mut values = HashMap::from([
            ("WATCHDOG_CONFIG_PATH", "/etc/agent-watchdog/watchdog.toml"),
            (
                "WATCHDOG_DATABASE_PATH",
                "/var/lib/agent-watchdog/watchdog.db",
            ),
            ("WATCHDOG_LISTEN_ADDRESS", "0.0.0.0:8080"),
            ("WATCHDOG_BASIC_USERNAME", "operator"),
            ("WATCHDOG_BASIC_PASSWORD", "browser-secret"),
            ("WATCHDOG_BEARER_TOKEN", "agent-secret"),
            ("WATCHDOG_GITHUB_TOKEN", "github-secret"),
        ]);
        let bootstrap = BootstrapConfig::from_getter(|name| {
            values.get(name).map(|value| OsString::from(*value))
        })
        .expect("valid environment");
        values.insert("WATCHDOG_BASIC_PASSWORD", "changed-secret");

        let header = format!("Basic {}", STANDARD.encode("operator:browser-secret"));
        assert!(bootstrap.basic_auth.authorize(Some(header.as_bytes())));
        let debug = format!("{bootstrap:?}");
        for secret in ["browser-secret", "agent-secret", "github-secret"] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn invalid_secret_configuration_never_reflects_secret_value() {
        let secret = "secret\nvalue";
        let values = HashMap::from([
            ("WATCHDOG_CONFIG_PATH", "/etc/agent-watchdog/watchdog.toml"),
            (
                "WATCHDOG_DATABASE_PATH",
                "/var/lib/agent-watchdog/watchdog.db",
            ),
            ("WATCHDOG_LISTEN_ADDRESS", "0.0.0.0:8080"),
            ("WATCHDOG_BASIC_USERNAME", "operator"),
            ("WATCHDOG_BASIC_PASSWORD", "password"),
            ("WATCHDOG_BEARER_TOKEN", secret),
        ]);
        let result = BootstrapConfig::from_getter(|name| {
            values.get(name).map(|value| OsString::from(*value))
        });
        let error = result.expect_err("invalid token").to_string();
        assert!(!error.contains(secret));
    }

    #[test]
    fn watched_container_directory_projects_back_to_native_worktree_path() {
        let mounted = tempfile::tempdir().expect("mounted root should exist");
        let nested = mounted.path().join("repo/src");
        std::fs::create_dir_all(&nested).expect("nested directory should exist");
        let mapping = crate::WorktreePathMapping::new("/host/repositories", mounted.path())
            .expect("mapping should be valid");

        assert_eq!(
            native_worktree_path(&nested, &[mapping]),
            Some(std::path::PathBuf::from("/host/repositories/repo/src"))
        );
    }

    #[test]
    fn newer_filesystem_uncertainty_cannot_be_cleared_by_an_older_reconciliation() {
        let health = HealthService::new(Arc::new(FakeClock::new(TimePoint::new(
            WallTimeMs::new(100),
            100,
        ))));
        let generation = AtomicU64::new(1);
        health.record(
            ComponentId::FilesystemReconciliation,
            ComponentStatus::Degraded,
            Some("filesystem events were lost"),
        );

        generation.store(2, Ordering::Release);
        mark_filesystem_reconciliation_complete(&health, &generation, 1);
        assert_eq!(
            health.component_status(ComponentId::FilesystemReconciliation),
            Some(ComponentStatus::Degraded)
        );

        mark_filesystem_reconciliation_complete(&health, &generation, 2);
        assert_eq!(
            health.component_status(ComponentId::FilesystemReconciliation),
            Some(ComponentStatus::Healthy)
        );
    }

    #[test]
    fn watch_targets_route_only_to_the_affected_runtime_or_worktree() {
        let codex = watchdog_runtime::WatchTargetId::new(1).expect("valid Codex target");
        let worktree = watchdog_runtime::WatchTargetId::new(2).expect("valid worktree target");
        let routes = WatchTargetRoutes {
            runtimes: BTreeMap::from([(codex, RuntimeKind::CodexCli)]),
            worktree_paths: BTreeMap::from([(
                worktree,
                std::path::PathBuf::from("/host/repository"),
            )]),
        };

        let codex_actions = routes.actions(&[codex]);
        assert_eq!(
            codex_actions.discovery,
            DiscoveryRequest::for_runtime(RuntimeKind::CodexCli)
        );
        assert!(codex_actions.worktree_paths.is_empty());

        let worktree_actions = routes.actions(&[worktree]);
        assert_eq!(worktree_actions.discovery, DiscoveryRequest::none());
        assert_eq!(
            worktree_actions.worktree_paths,
            vec![std::path::PathBuf::from("/host/repository")]
        );
    }

    #[test]
    fn discovery_scheduler_coalesces_runtime_requests_without_losing_a_runtime() {
        let scheduler = super::DiscoveryScheduler::default();
        scheduler.request(DiscoveryRequest::for_runtime(RuntimeKind::ClaudeCode));
        scheduler.request(DiscoveryRequest::for_runtime(RuntimeKind::CodexCli));

        let request = scheduler.take();
        assert!(request.includes(RuntimeKind::ClaudeCode));
        assert!(request.includes(RuntimeKind::CodexCli));
        assert!(!request.includes(RuntimeKind::CodexCompanion));
        assert_eq!(scheduler.take(), DiscoveryRequest::none());
    }

    #[tokio::test]
    async fn broad_worktree_allowlist_cannot_starve_runtime_watch_roots() {
        let fixture = tempfile::tempdir().expect("fixture root should exist");
        let worktrees = fixture.path().join("worktrees");
        let claude = fixture.path().join("claude");
        let codex = fixture.path().join("codex");
        let companion = fixture.path().join("companion");
        for path in [&worktrees, &claude, &codex, &companion] {
            std::fs::create_dir(path).expect("watch root should exist");
        }
        for index in 0..16 {
            std::fs::create_dir(worktrees.join(format!("repository-{index}")))
                .expect("broad worktree fixture should exist");
            std::fs::write(claude.join(format!("transcript-{index}.jsonl")), "fixture")
                .expect("runtime file fixture should exist");
        }
        let config_path = fixture.path().join("watchdog.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"[paths]
allowed_worktree_roots = ["{}"]
native_worktree_roots = ["/host/repositories"]
claude_roots = ["{}"]
codex_roots = ["{}"]
companion_roots = ["{}"]
exclusions = []

[adapters]
claude = true
codex = true
companion = true

[github]
enabled = false

[termination]
automation_enabled = false
sigkill_enabled = false
"#,
                worktrees.display(),
                claude.display(),
                codex.display(),
                companion.display(),
            ),
        )
        .expect("config should write");
        let config = ConfigManager::load(&config_path).expect("config should load");
        let store = watchdog_store::WatchdogStore::open(&fixture.path().join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(WallTimeMs::new(100), 100)));
        let watch_paths = WatchPathRegistry::load(store, clock.clone(), config.clone())
            .await
            .expect("watch paths should load");
        let health = HealthService::new(clock);

        let watcher = build_watcher_with_limit(&config.current(), &health, &watch_paths, 3)
            .expect("runtime roots should fit exactly");
        assert_eq!(watcher.routes.runtimes.len(), 3);
        assert_eq!(
            watcher
                .routes
                .runtimes
                .values()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from([
                RuntimeKind::ClaudeCode,
                RuntimeKind::CodexCli,
                RuntimeKind::CodexCompanion,
            ])
        );
        assert!(watcher.routes.worktree_paths.is_empty());
        assert_eq!(
            health.component_status(ComponentId::Watcher),
            Some(ComponentStatus::Healthy)
        );
    }
}
