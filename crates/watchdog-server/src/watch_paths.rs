use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use thiserror::Error;
use watchdog_domain::{BoundedText, Clock, MainSessionId, SessionId, SessionIdentity};
use watchdog_store::{RegisteredWatchPathRecord, StoreError, WatchdogStore};

use crate::config::{ConfigManager, RuntimeConfig};

const MAX_REGISTERED_WATCH_PATHS: u32 = 4_096;
const MAX_ACTIVE_WATCH_PATHS: u32 = 4_096;

/// Durable agent-registered paths shared with the filesystem watcher.
#[derive(Clone)]
pub(crate) struct WatchPathRegistry {
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
    config: ConfigManager,
    records: Arc<RwLock<BTreeMap<(SessionId, String), RegisteredWatchPathRecord>>>,
    active: Arc<RwLock<BTreeMap<SessionId, String>>>,
    registration: Arc<tokio::sync::Mutex<()>>,
    generation: Arc<AtomicU64>,
}

impl WatchPathRegistry {
    pub(crate) async fn load(
        store: WatchdogStore,
        clock: Arc<dyn Clock>,
        config: ConfigManager,
    ) -> Result<Self, WatchPathError> {
        let records = store
            .registered_watch_paths(MAX_REGISTERED_WATCH_PATHS.saturating_add(1))
            .await?;
        if records.len() > MAX_REGISTERED_WATCH_PATHS as usize {
            return Err(WatchPathError::Capacity);
        }
        let records = records
            .into_iter()
            .map(|record| {
                (
                    (
                        record.session().session_id(),
                        record.native_path().to_owned(),
                    ),
                    record,
                )
            })
            .collect();
        Ok(Self {
            store,
            clock,
            config,
            records: Arc::new(RwLock::new(records)),
            active: Arc::new(RwLock::new(BTreeMap::new())),
            registration: Arc::new(tokio::sync::Mutex::new(())),
            generation: Arc::new(AtomicU64::new(1)),
        })
    }

    pub(crate) async fn register(
        &self,
        session: SessionIdentity,
        root: MainSessionId,
        event_key: &str,
        native_path: &str,
    ) -> Result<RegisteredWatchPathRecord, WatchPathError> {
        let _registration = self.registration.lock().await;
        let native_path = BoundedText::<4_096>::new("registered_watch_path", native_path)
            .map_err(|_| WatchPathError::Rejected)?;
        if native_path.is_empty() {
            return Err(WatchPathError::Rejected);
        }
        let native_path = Path::new(native_path.as_str());
        if !concrete_native_path(native_path) {
            return Err(WatchPathError::Rejected);
        }
        let config = self.config.current();
        let (native_path, mounted_path) = project_path(native_path, &config)?;
        if !mounted_path.is_dir()
            || config
                .exclusions()
                .iter()
                .any(|excluded| mounted_path.starts_with(excluded))
        {
            return Err(WatchPathError::Rejected);
        }
        let record = RegisteredWatchPathRecord::new(
            session,
            root,
            event_key,
            native_path,
            self.clock.now().wall_time(),
        )
        .map_err(|_| WatchPathError::Rejected)?;
        let key = (
            record.session().session_id(),
            record.native_path().to_owned(),
        );
        {
            let records = self.records.read().map_err(|_| WatchPathError::State)?;
            if let Some(existing) = records.get(&key) {
                return if existing.event_key() == event_key {
                    Ok(existing.clone())
                } else {
                    Err(WatchPathError::Store(
                        StoreError::WatchPathAlreadyRegistered,
                    ))
                };
            }
            if records.len() >= MAX_REGISTERED_WATCH_PATHS as usize {
                return Err(WatchPathError::Capacity);
            }
        }
        let inserted = self.store.save_registered_watch_path(&record).await?;
        if inserted {
            let mut records = self.records.write().map_err(|_| WatchPathError::State)?;
            if records.len() >= MAX_REGISTERED_WATCH_PATHS as usize {
                return Err(WatchPathError::Capacity);
            }
            records.insert(key, record.clone());
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
        Ok(record)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) async fn refresh_active(&self) -> Result<(), WatchPathError> {
        let sessions = self
            .store
            .active_child_metadata(MAX_ACTIVE_WATCH_PATHS.saturating_add(1))
            .await?;
        if sessions.len() > MAX_ACTIVE_WATCH_PATHS as usize {
            return Err(WatchPathError::Capacity);
        }
        let mut candidates = BTreeMap::new();
        for metadata in sessions {
            let Some(path) = metadata.startup_directory() else {
                continue;
            };
            if concrete_native_path(Path::new(path)) {
                candidates.insert(metadata.session().session_id(), path.to_owned());
            }
        }
        let mut ownership_counts = BTreeMap::<String, usize>::new();
        for path in candidates.values() {
            let count = ownership_counts.entry(path.clone()).or_default();
            *count = count.saturating_add(1);
        }
        let refreshed = candidates
            .into_iter()
            .filter(|(_, path)| ownership_counts.get(path) == Some(&1))
            .collect();
        let mut active = self.active.write().map_err(|_| WatchPathError::State)?;
        if *active != refreshed {
            *active = refreshed;
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
        Ok(())
    }

    pub(crate) fn projected_paths(
        &self,
        config: &RuntimeConfig,
    ) -> Result<(Vec<ProjectedWatchPath>, bool), WatchPathError> {
        let records = self.records.read().map_err(|_| WatchPathError::State)?;
        let active = self.active.read().map_err(|_| WatchPathError::State)?;
        let durable_paths = records
            .values()
            .map(|record| record.native_path().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        let active_paths = active
            .values()
            .filter(|path| !durable_paths.contains(*path))
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let path_count = durable_paths.len().saturating_add(active_paths.len());
        let native_paths = durable_paths.into_iter().chain(active_paths);
        let mut projected = Vec::with_capacity(path_count);
        let mut incomplete = false;
        for native_path in native_paths {
            match project_path(Path::new(&native_path), config) {
                Ok((native_path, mounted_path))
                    if mounted_path.is_dir()
                        && !config
                            .exclusions()
                            .iter()
                            .any(|excluded| mounted_path.starts_with(excluded)) =>
                {
                    projected.push(ProjectedWatchPath {
                        native_path,
                        mounted_path,
                    });
                }
                _ => incomplete = true,
            }
        }
        Ok((projected, incomplete))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProjectedWatchPath {
    native_path: String,
    mounted_path: PathBuf,
}

impl ProjectedWatchPath {
    pub(crate) fn native_path(&self) -> &str {
        &self.native_path
    }

    pub(crate) fn mounted_path(&self) -> &Path {
        &self.mounted_path
    }
}

fn project_path(
    native_path: &Path,
    config: &RuntimeConfig,
) -> Result<(String, PathBuf), WatchPathError> {
    config
        .worktree_mappings()
        .iter()
        .filter_map(|mapping| {
            mapping
                .project_native_directory(native_path)
                .map(|projected| (mapping.native_root().components().count(), projected))
        })
        .max_by_key(|(specificity, _)| *specificity)
        .map(|(_, projected)| projected)
        .ok_or(WatchPathError::Rejected)
}

fn concrete_native_path(path: &Path) -> bool {
    path.is_absolute()
        && path != Path::new("/")
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

impl std::fmt::Debug for WatchPathRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WatchPathRegistry")
            .field("generation", &self.generation())
            .finish_non_exhaustive()
    }
}

/// Stable watch-path registration failure without retaining the rejected path.
#[derive(Debug, Error)]
pub(crate) enum WatchPathError {
    #[error("Registered watch path is outside an allowed worktree root")]
    Rejected,
    #[error("Registered watch path limit was reached")]
    Capacity,
    #[error("Registered watch path state is unavailable")]
    State,
    #[error("Registered watch path persistence failed")]
    Store(#[from] StoreError),
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::symlink};

    use tempfile::TempDir;
    use watchdog_domain::{RuntimeKind, SessionKind, TimePoint, WallTimeMs};
    use watchdog_testkit::FakeClock;

    use super::*;
    use crate::{AgentApi, DiscoveredSession, RegisterSession, TransportKey};

    #[tokio::test]
    async fn scoped_registration_accepts_only_capability_projected_directories_and_restores() {
        let fixture = WatchPathFixture::new();
        let config = ConfigManager::load(&fixture.config_path).expect("config should load");
        let store = WatchdogStore::open(&fixture.root.path().join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(
            WallTimeMs::new(10_000),
            5_000,
        )));
        let registry = WatchPathRegistry::load(store.clone(), clock.clone(), config.clone())
            .await
            .expect("registry should load");
        let api = AgentApi::new(store.clone(), clock.clone())
            .await
            .expect("API should initialize");
        api.configure_watch_paths(registry.clone());
        let main = api
            .discover_session(discovery("main", SessionKind::Main, None))
            .await
            .expect("main should discover");
        let child = api
            .discover_session(discovery(
                "child",
                SessionKind::Child,
                Some(main.session.session_id()),
            ))
            .await
            .expect("child should discover");
        let transport = TransportKey::new("watch-path-test").expect("transport should be valid");
        api.register_session(
            &transport,
            RegisterSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: "main".to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "bind-main".to_owned(),
            },
        )
        .await
        .expect("main should bind transport");

        let accepted = api
            .register_watch_path(
                &transport,
                child.session.session_id(),
                "watch-inside",
                "/host/repositories/inside",
            )
            .await
            .expect("inside path should register");
        assert_eq!(accepted.server_time, clock.now().wall_time());
        assert_eq!(
            accepted.registration.native_path(),
            "/host/repositories/inside"
        );
        assert_eq!(
            api.register_watch_path(
                &transport,
                child.session.session_id(),
                "watch-inside",
                "/host/repositories/inside",
            )
            .await
            .expect("duplicate should be idempotent"),
            accepted
        );
        assert_duplicate_path_rejected(&api, &transport, child.session.session_id()).await;
        for rejected in [
            "/host/repositories/../outside",
            "/host/outside",
            "/host/repositories/escape",
        ] {
            assert!(matches!(
                api.register_watch_path(
                    &transport,
                    child.session.session_id(),
                    &format!("reject:{rejected}"),
                    rejected,
                )
                .await,
                Err(crate::AgentApiError::WatchPathRejected)
            ));
        }
        let (projected, incomplete) = registry
            .projected_paths(&config.current())
            .expect("registry should project");
        assert!(!incomplete);
        assert_eq!(projected.len(), 1);
        assert_eq!(
            projected[0].mounted_path(),
            fixture.worktrees.join("inside")
        );

        let restored = WatchPathRegistry::load(store, clock, config)
            .await
            .expect("registry should restore");
        assert_eq!(restored.records.read().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn active_child_worktree_is_ephemeral_and_removed_at_terminal_state() {
        let fixture = WatchPathFixture::new();
        let config = ConfigManager::load(&fixture.config_path).expect("config should load");
        let store = WatchdogStore::open(&fixture.root.path().join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(
            WallTimeMs::new(10_000),
            5_000,
        )));
        let registry = WatchPathRegistry::load(store.clone(), clock.clone(), config.clone())
            .await
            .expect("registry should load");
        let api = AgentApi::new(store, clock)
            .await
            .expect("API should initialize");
        let main = api
            .discover_session(discovery("main", SessionKind::Main, None))
            .await
            .expect("main should discover");
        let child = api
            .discover_session(discovery_with_directory(
                "child",
                SessionKind::Child,
                Some(main.session.session_id()),
                Some("/host/repositories/active"),
            ))
            .await
            .expect("child should discover");
        let transport = TransportKey::new("active-watch-test").expect("valid transport");
        api.register_session(
            &transport,
            RegisterSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: "main".to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "bind-main".to_owned(),
            },
        )
        .await
        .expect("main should bind transport");

        registry.refresh_active().await.expect("active refresh");
        let (projected, incomplete) = registry
            .projected_paths(&config.current())
            .expect("active path should project");
        assert!(!incomplete);
        assert_eq!(projected.len(), 1);
        assert_eq!(
            projected[0].mounted_path(),
            fixture.worktrees.join("active")
        );

        api.complete_session(
            &transport,
            child.session.session_id(),
            "complete-child",
            crate::CompletionOutcome::Completed,
        )
        .await
        .expect("child should complete");
        registry.refresh_active().await.expect("terminal refresh");
        let (projected, incomplete) = registry
            .projected_paths(&config.current())
            .expect("terminal path should be removed");
        assert!(!incomplete);
        assert!(projected.is_empty());
    }

    #[tokio::test]
    async fn shared_active_worktree_is_excluded_from_automatic_watch_paths() {
        let fixture = WatchPathFixture::new();
        let config = ConfigManager::load(&fixture.config_path).expect("config should load");
        let store = WatchdogStore::open(&fixture.root.path().join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(
            WallTimeMs::new(10_000),
            5_000,
        )));
        let registry = WatchPathRegistry::load(store.clone(), clock.clone(), config.clone())
            .await
            .expect("registry should load");
        let api = AgentApi::new(store, clock)
            .await
            .expect("API should initialize");
        let main = api
            .discover_session(discovery("main", SessionKind::Main, None))
            .await
            .expect("main should discover");
        for native_id in ["child-one", "child-two"] {
            api.discover_session(discovery_with_directory(
                native_id,
                SessionKind::Child,
                Some(main.session.session_id()),
                Some("/host/repositories/active"),
            ))
            .await
            .expect("child should discover");
        }

        registry.refresh_active().await.expect("active refresh");
        let (projected, incomplete) = registry
            .projected_paths(&config.current())
            .expect("shared path should be omitted");
        assert!(!incomplete);
        assert!(projected.is_empty());
    }

    fn discovery(
        native_id: &str,
        kind: SessionKind,
        parent: Option<SessionId>,
    ) -> DiscoveredSession {
        discovery_with_directory(native_id, kind, parent, None)
    }

    fn discovery_with_directory(
        native_id: &str,
        kind: SessionKind,
        parent: Option<SessionId>,
        startup_directory: Option<&str>,
    ) -> DiscoveredSession {
        DiscoveredSession {
            runtime: RuntimeKind::ClaudeCode,
            native_id: native_id.to_owned(),
            kind,
            parent,
            event_key: format!("discover:{native_id}"),
            adapter_version: "test".to_owned(),
            evidence_source: "test:watch-path".to_owned(),
            title: None,
            startup_directory: startup_directory.map(ToOwned::to_owned),
        }
    }

    async fn assert_duplicate_path_rejected(
        api: &AgentApi,
        transport: &TransportKey,
        session_id: SessionId,
    ) {
        assert!(matches!(
            api.register_watch_path(
                transport,
                session_id,
                "watch-inside-new-key",
                "/host/repositories/inside",
            )
            .await,
            Err(crate::AgentApiError::Store(
                StoreError::WatchPathAlreadyRegistered
            ))
        ));
    }

    struct WatchPathFixture {
        root: TempDir,
        config_path: PathBuf,
        worktrees: PathBuf,
    }

    impl WatchPathFixture {
        fn new() -> Self {
            let root = TempDir::new().expect("fixture root should exist");
            let config_path = root.path().join("watchdog.toml");
            let worktrees = root.path().join("worktrees");
            let outside = root.path().join("outside");
            let claude = root.path().join("claude");
            let codex = root.path().join("codex");
            let companion = root.path().join("companion");
            for path in [
                &worktrees,
                &worktrees.join("inside"),
                &worktrees.join("active"),
                &outside,
                &claude,
                &codex,
                &companion,
            ] {
                fs::create_dir(path).expect("fixture directory should exist");
            }
            symlink(&outside, worktrees.join("escape")).expect("escape symlink should exist");
            fs::write(
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
            .expect("config should be written");
            Self {
                root,
                config_path,
                worktrees,
            }
        }
    }
}
