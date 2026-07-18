use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    io::{Read as _, Write as _},
    net::{IpAddr, SocketAddr, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::Router;
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _};
use watchdog_domain::{AdapterIdentity, BoundedText, Clock as _, RuntimeKind, SecretText};
use watchdog_runtime::{
    CapabilityRoot, ComponentId, ComponentStatus, DirectoryScanner, ScanBudget, WatchService,
    WatchSignal, WatchTargetId,
};
use watchdog_store::{AdapterHealthRecord, AdapterHealthStatus, WatchdogStore};

use crate::config::{ConfigManager, RuntimeConfig};
#[cfg(target_os = "linux")]
use crate::process_monitor::ProcessMonitor;
#[cfg(target_os = "linux")]
use crate::termination::TerminationMonitor;
use crate::{
    AgentApi, BasicAuthenticator, BearerAuthenticator, ClaudeDiscovery, ClaudeHookService,
    CodexDiscovery, CompanionDiscovery, DashboardOutboxDispatcher, DashboardService,
    FilesystemActivityReconciler, GitHubEnricher, HealthService, HumanNotifier,
    HumanOutboxDispatcher, NotificationEndpoints, RepositoryMetadata, RuntimeDiscoveryReport,
    SystemClock, TerminationConfig, WebhookEndpoint, claude_hook_router, dashboard_router,
    health_router, mcp_router,
};

const MAX_ENV_PATH_BYTES: usize = 4_096;
const WATCH_QUEUE_CAPACITY: usize = 4_096;
const WATCH_TARGET_LIMIT: usize = 4_096;
const FILESYSTEM_ACTIVITY_QUEUE_CAPACITY: usize = 64;
const MAX_GITHUB_SESSIONS: u32 = 1_000;
const GITHUB_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(30);
const DASHBOARD_DELIVERY_LIMIT: u32 = 256;
const NOTIFICATION_DELIVERY_LIMIT: u32 = 128;
const PERIODIC_RECONCILIATION: Duration = Duration::from_mins(5);
const TIMER_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(target_os = "linux")]
const PROCESS_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

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

async fn run(bootstrap: BootstrapConfig) -> Result<(), ServerError> {
    let config =
        ConfigManager::load(&bootstrap.config_path).map_err(|_| ServerError::Configuration)?;
    let current = config.current();
    let clock = Arc::new(SystemClock::new());
    let store = WatchdogStore::open(&bootstrap.database_path).await?;
    let health = HealthService::new(Arc::clone(&clock) as Arc<_>);
    health.record(ComponentId::Store, ComponentStatus::Healthy, None);
    health.record(ComponentId::Authorization, ComponentStatus::Healthy, None);

    let api = initialize_agent_api(&store, &clock, &current).await?;
    health.record(ComponentId::Reducer, ComponentStatus::Healthy, None);
    record_adapter_health(&health, &current);

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

    let router = Router::new()
        .merge(health_router(health.clone(), bootstrap.basic_auth.clone()))
        .merge(dashboard_router(dashboard, bootstrap.basic_auth.clone()))
        .merge(claude_hook_router(
            ClaudeHookService::new(api.clone(), Arc::clone(&clock) as Arc<_>),
            bootstrap.bearer_auth.clone(),
        ))
        .merge(mcp_router(api.clone(), bootstrap.bearer_auth.clone()));

    let (reconcile_tx, reconcile_rx) = mpsc::channel(1);
    let discovery_worker = start_discovery(
        config.clone(),
        api.clone(),
        store.clone(),
        Arc::clone(&clock),
        health.clone(),
        reconcile_rx,
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
        reconcile_tx,
        filesystem_activity_tx,
    );
    let dashboard_worker = spawn_dashboard_worker(dispatcher, health.clone());
    let notification_worker = spawn_notification_worker(notification_dispatcher, health.clone());
    let timer_worker = spawn_timer_worker(api.clone(), health.clone());
    #[cfg(target_os = "linux")]
    let process_worker = spawn_process_worker(linux_monitors.process, health.clone());
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
    process_worker.abort();
    #[cfg(target_os = "linux")]
    termination_worker.abort();
    reload_worker.abort();
    let _ = watcher.await;
    tracing::info!(event = "server.stopped", "Agent Watchdog server stopped");
    result
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

fn spawn_timer_worker(api: AgentApi, health: HealthService) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TIMER_RECONCILIATION_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
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
fn spawn_process_worker(monitor: ProcessMonitor, health: HealthService) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PROCESS_SAMPLE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
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
    })
}

fn start_discovery(
    config: ConfigManager,
    api: AgentApi,
    store: WatchdogStore,
    clock: Arc<SystemClock>,
    health: HealthService,
    requested: mpsc::Receiver<()>,
) -> JoinHandle<()> {
    let discoveries = RuntimeDiscoveries {
        claude: ClaudeDiscovery::new(api.clone(), store.clone(), clock.clone()),
        codex: CodexDiscovery::new(api.clone(), store.clone(), clock.clone()),
        companion: CompanionDiscovery::new(api, store.clone(), clock.clone()),
    };
    spawn_discovery_worker(config, discoveries, store, clock, health, requested)
}

struct RuntimeDiscoveries {
    claude: ClaudeDiscovery,
    codex: CodexDiscovery,
    companion: CompanionDiscovery,
}

fn spawn_discovery_worker(
    config: ConfigManager,
    discoveries: RuntimeDiscoveries,
    store: WatchdogStore,
    clock: Arc<SystemClock>,
    health: HealthService,
    mut requested: mpsc::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PERIODIC_RECONCILIATION);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                signal = requested.recv() => {
                    if signal.is_none() {
                        return;
                    }
                }
            }
            let current = config.current();
            if current.adapters().claude() {
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
            if current.adapters().codex() {
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
            if current.adapters().companion() {
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
        }
    })
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
        tracing::info!(
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
                    ComponentId::ObservationQueue,
                    ComponentStatus::Healthy,
                    None,
                ),
                Err(_) => health.record(
                    ComponentId::ObservationQueue,
                    ComponentStatus::Degraded,
                    Some("Dashboard delivery is degraded"),
                ),
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
    reconcile: mpsc::Sender<()>,
    filesystem_activity: mpsc::Sender<Vec<PathBuf>>,
) -> JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let mut applied = config.current();
        let mut watcher = build_watcher(&applied, &health);
        let _ = reconcile.try_send(());
        while !stop.load(Ordering::Acquire) {
            let candidate = config.current();
            if !Arc::ptr_eq(&applied, &candidate) {
                watcher = build_watcher(&candidate, &health);
                applied = candidate;
                let _ = reconcile.try_send(());
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
                            &reconcile,
                            &filesystem_activity,
                        );
                    }
                    WatchSignal::TopologyChanged(targets) => {
                        reconcile_watch_targets(
                            watcher.as_ref(),
                            &targets,
                            &health,
                            &reconcile,
                            &filesystem_activity,
                        );
                        watcher = build_watcher(&applied, &health);
                    }
                    WatchSignal::ReconcileAll(_) => {
                        health.record(
                            ComponentId::Watcher,
                            ComponentStatus::Degraded,
                            Some("Filesystem events were lost; reconciliation is required"),
                        );
                        let _ = reconcile.try_send(());
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
    reconcile: &mpsc::Sender<()>,
    filesystem_activity: &mpsc::Sender<Vec<PathBuf>>,
) {
    health.record(ComponentId::Watcher, ComponentStatus::Healthy, None);
    let _ = reconcile.try_send(());
    let paths = watcher.map_or_else(Vec::new, |registry| {
        targets
            .iter()
            .filter_map(|target| registry.worktree_paths.get(target).cloned())
            .collect()
    });
    if !paths.is_empty() && filesystem_activity.try_send(paths).is_err() {
        health.record(
            ComponentId::Watcher,
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

struct WatchRegistry {
    service: WatchService,
    worktree_paths: BTreeMap<WatchTargetId, PathBuf>,
}

fn build_watcher(config: &RuntimeConfig, health: &HealthService) -> Option<WatchRegistry> {
    let Ok(mut watcher) = WatchService::new(WATCH_QUEUE_CAPACITY) else {
        health.record(
            ComponentId::Watcher,
            ComponentStatus::Degraded,
            Some("Filesystem watch backend is unavailable"),
        );
        return None;
    };
    let mut roots = Vec::new();
    roots.extend(
        config
            .allowed_worktree_roots()
            .iter()
            .cloned()
            .map(|root| (root, true)),
    );
    roots.extend(
        config
            .claude_roots()
            .iter()
            .chain(config.codex_roots())
            .chain(config.companion_roots())
            .cloned()
            .map(|root| (root, false)),
    );
    let exclusions = config.exclusions();
    let Ok(scan_budget) = ScanBudget::new(4, WATCH_TARGET_LIMIT, 4 * 1_024 * 1_024) else {
        return None;
    };
    let scanner = DirectoryScanner::new(scan_budget);
    let mut next_target = 1_u64;
    let mut degraded = false;
    let mut worktree_paths = BTreeMap::new();
    for (configured_root, is_worktree) in roots
        .into_iter()
        .filter(|(root, _)| !exclusions.iter().any(|excluded| root.starts_with(excluded)))
    {
        let Ok(root) = CapabilityRoot::new(configured_root) else {
            degraded = true;
            continue;
        };
        let scan = scanner.scan(&root, Path::new("."));
        let directories = if let Ok(scan) = scan {
            degraded |= scan.uncertainty().is_some();
            scan.directories().to_vec()
        } else {
            degraded = true;
            Vec::new()
        };
        let candidates = std::iter::once(root.path().to_owned()).chain(directories);
        for directory in candidates.filter(|directory| {
            !exclusions
                .iter()
                .any(|excluded| directory.starts_with(excluded))
        }) {
            if usize::try_from(next_target).map_or(true, |target| target > WATCH_TARGET_LIMIT) {
                degraded = true;
                break;
            }
            let Ok(relative) = directory.strip_prefix(root.path()) else {
                degraded = true;
                continue;
            };
            let target = WatchTargetId::new(next_target);
            next_target = next_target.saturating_add(1);
            let Ok(target) = target else {
                degraded = true;
                continue;
            };
            if watcher.add_target(target, &root, relative).is_err() {
                degraded = true;
                continue;
            }
            if is_worktree
                && let Some(native_path) =
                    native_worktree_path(&directory, config.worktree_mappings())
            {
                worktree_paths.insert(target, native_path);
            }
        }
    }
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
        worktree_paths,
    })
}

fn native_worktree_path(
    mounted_path: &Path,
    mappings: &[crate::WorktreePathMapping],
) -> Option<PathBuf> {
    mappings.iter().find_map(|mapping| {
        let relative = mounted_path.strip_prefix(mapping.mounted_root()).ok()?;
        Some(mapping.native_root().join(relative))
    })
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
        collections::HashMap,
        ffi::OsString,
        io::{Read as _, Write as _},
        net::TcpListener,
        thread,
    };

    use base64::{Engine as _, engine::general_purpose::STANDARD};

    use super::{BootstrapConfig, check_liveness, native_worktree_path};

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
}
