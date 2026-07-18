use std::{
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
use crate::{
    AgentApi, BasicAuthenticator, BearerAuthenticator, ClaudeTeamDiscovery, CompanionDiscovery,
    DashboardOutboxDispatcher, DashboardService, GitHubEnricher, HealthService, HumanNotifier,
    NotificationEndpoints, RuntimeDiscoveryReport, SystemClock, TerminationConfig, WebhookEndpoint,
    dashboard_router, health_router, mcp_router,
};

const MAX_ENV_PATH_BYTES: usize = 4_096;
const WATCH_QUEUE_CAPACITY: usize = 4_096;
const WATCH_TARGET_LIMIT: usize = 4_096;
const DASHBOARD_DELIVERY_LIMIT: u32 = 256;
const PERIODIC_RECONCILIATION: Duration = Duration::from_mins(5);

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

    let api = AgentApi::with_policy(
        store.clone(),
        Arc::clone(&clock) as Arc<_>,
        current.reducer_policy(),
    )
    .await?;
    health.record(ComponentId::Reducer, ComponentStatus::Healthy, None);
    record_adapter_health(&health, &current);

    #[cfg(target_os = "linux")]
    let _termination_config = TerminationConfig::new(
        current.automation_enabled(),
        current.sigkill_enabled(),
        current.warning_grace(),
        current.action_grace(),
    )
    .map_err(|_| ServerError::Configuration)?;
    let _deadline_policy = current.deadline_policy();

    #[cfg(target_os = "linux")]
    {
        watchdog_process::LinuxProcessSampler::new(32_768)
            .map_err(|_| ServerError::ProcessSampler)?;
        health.record(ComponentId::ProcessSampler, ComponentStatus::Healthy, None);
    }

    let dashboard = DashboardService::new(store.clone(), Arc::clone(&clock) as Arc<_>);
    let dispatcher = DashboardOutboxDispatcher::new(
        store.clone(),
        dashboard.clone(),
        Arc::clone(&clock) as Arc<_>,
    );
    let _notifier = HumanNotifier::new(
        store.clone(),
        Arc::clone(&clock) as Arc<_>,
        bootstrap.notification_endpoints.clone(),
    )
    .map_err(|_| ServerError::NotificationConfiguration)?;
    let _github = if current.github_enabled() {
        Some(
            GitHubEnricher::new(
                Arc::clone(&clock) as Arc<_>,
                bootstrap
                    .github_token
                    .as_ref()
                    .map(secrecy::ExposeSecret::expose_secret),
            )
            .map_err(|_| ServerError::GitHubConfiguration)?,
        )
    } else {
        None
    };

    let router = Router::new()
        .merge(health_router(health.clone(), bootstrap.basic_auth.clone()))
        .merge(dashboard_router(dashboard, bootstrap.basic_auth.clone()))
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
    let watcher = spawn_watcher_supervisor(
        config.clone(),
        health.clone(),
        Arc::clone(&watcher_stop),
        reconcile_tx,
    );
    let dashboard_worker = spawn_dashboard_worker(dispatcher, health.clone());
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
    reload_worker.abort();
    let _ = watcher.await;
    tracing::info!(event = "server.stopped", "Agent Watchdog server stopped");
    result
}

fn start_discovery(
    config: ConfigManager,
    api: AgentApi,
    store: WatchdogStore,
    clock: Arc<SystemClock>,
    health: HealthService,
    requested: mpsc::Receiver<()>,
) -> JoinHandle<()> {
    spawn_discovery_worker(
        config,
        ClaudeTeamDiscovery::new(api.clone()),
        CompanionDiscovery::new(api, clock.clone()),
        store,
        clock,
        health,
        requested,
    )
}

fn spawn_discovery_worker(
    config: ConfigManager,
    claude: ClaudeTeamDiscovery,
    companion: CompanionDiscovery,
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
                let report = claude
                    .reconcile(current.claude_roots(), current.worktree_mappings())
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
            if current.adapters().companion() {
                let report = companion
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
            if let Some(signal) = watcher.as_ref().and_then(WatchService::next_signal) {
                match signal {
                    WatchSignal::Targets(_) => {
                        health.record(ComponentId::Watcher, ComponentStatus::Healthy, None);
                        let _ = reconcile.try_send(());
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

fn build_watcher(config: &RuntimeConfig, health: &HealthService) -> Option<WatchService> {
    let Ok(mut watcher) = WatchService::new(WATCH_QUEUE_CAPACITY) else {
        health.record(
            ComponentId::Watcher,
            ComponentStatus::Degraded,
            Some("Filesystem watch backend is unavailable"),
        );
        return None;
    };
    let mut roots = Vec::new();
    roots.extend_from_slice(config.allowed_worktree_roots());
    roots.extend_from_slice(config.claude_roots());
    roots.extend_from_slice(config.codex_roots());
    roots.extend_from_slice(config.companion_roots());
    let exclusions = config.exclusions();
    let Ok(scan_budget) = ScanBudget::new(4, WATCH_TARGET_LIMIT, 4 * 1_024 * 1_024) else {
        return None;
    };
    let scanner = DirectoryScanner::new(scan_budget);
    let mut next_target = 1_u64;
    let mut degraded = false;
    for configured_root in roots
        .into_iter()
        .filter(|root| !exclusions.iter().any(|excluded| root.starts_with(excluded)))
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
            if target
                .ok()
                .is_none_or(|target| watcher.add_target(target, &root, relative).is_err())
            {
                degraded = true;
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
    Some(watcher)
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

    use super::{BootstrapConfig, check_liveness};

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
}
