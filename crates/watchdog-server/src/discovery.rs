use std::{
    collections::BTreeSet,
    io::Read as _,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use thiserror::Error;
use watchdog_domain::{Clock, DetailedState, RuntimeKind, SessionId, SessionKind};
use watchdog_runtime::{CapabilityRoot, DirectoryScanner, ScanBudget};

use crate::{AgentApi, DiscoveredSession};

const MAX_SCAN_DEPTH: usize = 4;
const MAX_SCAN_ENTRIES: usize = 2_048;
const MAX_SCAN_PATH_BYTES: usize = 2 * 1_024 * 1_024;
const CODEX_BOOTSTRAP_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_CODEX_THREADS: u32 = 1_000;

/// Explicit projection from a runtime-native host prefix to its read-only
/// mount inside the supported Docker container.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorktreePathMapping {
    native_root: PathBuf,
    mounted_root: PathBuf,
}

impl WorktreePathMapping {
    /// Validate a concrete native prefix and canonical mounted capability root.
    ///
    /// The native prefix need not exist inside the container. The mounted root
    /// must exist and may not be the filesystem root.
    ///
    /// # Errors
    ///
    /// Returns [`PathMappingError`] for a relative, ambiguous, overbroad, or
    /// unavailable mapping.
    pub fn new(
        native_root: impl Into<PathBuf>,
        mounted_root: impl Into<PathBuf>,
    ) -> Result<Self, PathMappingError> {
        let native_root = native_root.into();
        if !is_concrete_absolute(&native_root) {
            return Err(PathMappingError::InvalidNativeRoot);
        }
        let mounted_root = mounted_root
            .into()
            .canonicalize()
            .map_err(|_| PathMappingError::InvalidMountedRoot)?;
        if mounted_root == Path::new("/") || !mounted_root.is_dir() {
            return Err(PathMappingError::InvalidMountedRoot);
        }
        Ok(Self {
            native_root,
            mounted_root,
        })
    }

    /// Runtime-visible host prefix retained for human-facing metadata.
    #[must_use]
    pub fn native_root(&self) -> &Path {
        &self.native_root
    }

    /// Canonical in-container capability root used for safe filesystem access.
    #[must_use]
    pub fn mounted_root(&self) -> &Path {
        &self.mounted_root
    }

    fn validate_native_path(&self, candidate: &Path) -> Option<String> {
        let relative = candidate.strip_prefix(&self.native_root).ok()?;
        if relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return None;
        }
        let projected = self.mounted_root.join(relative).canonicalize().ok()?;
        if !projected.starts_with(&self.mounted_root) {
            return None;
        }
        self.native_root
            .join(relative)
            .to_str()
            .map(ToOwned::to_owned)
    }
}

/// Invalid host-to-container worktree projection.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PathMappingError {
    /// Host prefix is relative, root-wide, or contains ambiguous components.
    #[error("Native worktree prefix is invalid")]
    InvalidNativeRoot,
    /// Container capability root is absent, not a directory, or root-wide.
    #[error("Mounted worktree root is invalid")]
    InvalidMountedRoot,
}

/// Bounded best-effort result of one runtime reconciliation pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeDiscoveryReport {
    main_sessions: u32,
    child_sessions: u32,
    warning_count: u32,
}

impl RuntimeDiscoveryReport {
    /// Unique main sessions successfully reconciled in this pass.
    #[must_use]
    pub const fn main_sessions(self) -> u32 {
        self.main_sessions
    }

    /// Unique active child sessions successfully reconciled in this pass.
    #[must_use]
    pub const fn child_sessions(self) -> u32 {
        self.child_sessions
    }

    /// Bounded inputs or paths that could not be reconciled safely.
    #[must_use]
    pub const fn warning_count(self) -> u32 {
        self.warning_count
    }

    /// Whether any configured scope needs operator attention or a later retry.
    #[must_use]
    pub const fn is_degraded(self) -> bool {
        self.warning_count > 0
    }

    fn warn(&mut self) {
        self.warning_count = self.warning_count.saturating_add(1);
    }
}

/// Claude team reconciliation report.
pub type ClaudeDiscoveryReport = RuntimeDiscoveryReport;

/// Codex Companion reconciliation report.
pub type CompanionDiscoveryReport = RuntimeDiscoveryReport;

/// Native Codex reconciliation report.
pub type CodexDiscoveryReport = RuntimeDiscoveryReport;

/// Automatic Claude team discovery independent from optional MCP/hooks.
#[derive(Clone, Debug)]
pub struct ClaudeTeamDiscovery {
    api: AgentApi,
}

impl ClaudeTeamDiscovery {
    /// Construct discovery over the shared durable ingestion service.
    #[must_use]
    pub const fn new(api: AgentApi) -> Self {
        Self { api }
    }

    /// Scan exact configured Claude roots without following symlinks, parse
    /// current team configs under strict byte/entry bounds, and persist every
    /// valid team even when another configured team is malformed.
    pub async fn reconcile(
        &self,
        claude_roots: &[PathBuf],
        worktree_mappings: &[WorktreePathMapping],
    ) -> ClaudeDiscoveryReport {
        let mut report = RuntimeDiscoveryReport::default();
        let mut mains = BTreeSet::new();
        let mut children = BTreeSet::new();
        let Ok(budget) = ScanBudget::new(MAX_SCAN_DEPTH, MAX_SCAN_ENTRIES, MAX_SCAN_PATH_BYTES)
        else {
            report.warn();
            return report;
        };
        let scanner = DirectoryScanner::new(budget);
        for configured_root in claude_roots {
            let Ok(root) = CapabilityRoot::new(configured_root) else {
                report.warn();
                continue;
            };
            let Ok(scan) = scanner.scan(&root, Path::new(".")) else {
                report.warn();
                continue;
            };
            if scan.uncertainty().is_some() {
                report.warn();
            }
            let candidates =
                std::iter::once(root.path().to_owned()).chain(scan.directories().iter().cloned());
            for directory in candidates {
                let Ok(relative) = directory.strip_prefix(root.path()) else {
                    report.warn();
                    continue;
                };
                let config = relative.join("config.json");
                let bytes = match read_bounded_config(&root, &config) {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => continue,
                    Err(()) => {
                        report.warn();
                        continue;
                    }
                };
                let Ok(team) = watchdog_claude::parse_team_config(&bytes) else {
                    report.warn();
                    continue;
                };
                self.reconcile_team(
                    &team,
                    worktree_mappings,
                    &mut report,
                    &mut mains,
                    &mut children,
                )
                .await;
            }
        }
        report
    }

    async fn reconcile_team(
        &self,
        team: &watchdog_claude::ClaudeTeam,
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) {
        let main_id = SessionId::from_native(team.lead());
        let main_directory = validated_directory(team.lead_cwd(), worktree_mappings, report);
        if let Err(error) = self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: team.lead().native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: discovery_key("claude-team", main_id),
                title: None,
                startup_directory: main_directory,
            })
            .await
        {
            log_reconcile_failure(RuntimeKind::ClaudeCode, "main", &error);
            report.warn();
            return;
        }
        if mains.insert(main_id) {
            report.main_sessions = report.main_sessions.saturating_add(1);
        }
        for member in team.members() {
            let child_id = SessionId::from_native(member.subject());
            let child_directory = validated_directory(member.cwd(), worktree_mappings, report);
            if let Err(error) = self
                .api
                .discover_session(DiscoveredSession {
                    runtime: RuntimeKind::ClaudeCode,
                    native_id: member.subject().native_id().to_owned(),
                    kind: SessionKind::Child,
                    parent: Some(main_id),
                    event_key: discovery_key("claude-team", child_id),
                    title: Some(member.name().to_owned()),
                    startup_directory: child_directory,
                })
                .await
            {
                log_reconcile_failure(RuntimeKind::ClaudeCode, "child", &error);
                report.warn();
            } else if children.insert(child_id) {
                report.child_sessions = report.child_sessions.saturating_add(1);
            }
        }
    }
}

/// Automatic Codex Companion job discovery from current per-workspace state.
#[derive(Clone)]
pub struct CompanionDiscovery {
    api: AgentApi,
    clock: Arc<dyn Clock>,
}

impl CompanionDiscovery {
    /// Construct discovery over the shared durable ingestion service.
    #[must_use]
    pub fn new(api: AgentApi, clock: Arc<dyn Clock>) -> Self {
        Self { api, clock }
    }

    /// Scan bounded workspace summaries, tolerate absent/pruned detail files,
    /// and reconcile valid jobs independently from malformed workspaces.
    pub async fn reconcile(
        &self,
        companion_roots: &[PathBuf],
        worktree_mappings: &[WorktreePathMapping],
    ) -> CompanionDiscoveryReport {
        let mut report = RuntimeDiscoveryReport::default();
        let mut mains = BTreeSet::new();
        let mut children = BTreeSet::new();
        let Ok(parser) =
            watchdog_companion::CompanionParser::new(watchdog_companion::TESTED_COMPANION_VERSION)
        else {
            report.warn();
            return report;
        };
        let Ok(budget) = ScanBudget::new(MAX_SCAN_DEPTH, MAX_SCAN_ENTRIES, MAX_SCAN_PATH_BYTES)
        else {
            report.warn();
            return report;
        };
        let scanner = DirectoryScanner::new(budget);
        for configured_root in companion_roots {
            let Ok(root) = CapabilityRoot::new(configured_root) else {
                report.warn();
                continue;
            };
            let Ok(scan) = scanner.scan(&root, Path::new(".")) else {
                report.warn();
                continue;
            };
            if scan.uncertainty().is_some() {
                report.warn();
            }
            let candidates =
                std::iter::once(root.path().to_owned()).chain(scan.directories().iter().cloned());
            for directory in candidates {
                let Ok(relative) = directory.strip_prefix(root.path()) else {
                    report.warn();
                    continue;
                };
                let summary_path = relative.join("state.json");
                let bytes = match read_bounded_file(
                    &root,
                    &summary_path,
                    watchdog_companion::MAX_SUMMARY_BYTES,
                ) {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => continue,
                    Err(()) => {
                        report.warn();
                        continue;
                    }
                };
                let Ok(snapshot) = parser.parse_summary(&bytes) else {
                    report.warn();
                    continue;
                };
                for job in snapshot.jobs() {
                    self.reconcile_companion_job(
                        &parser,
                        job,
                        worktree_mappings,
                        &mut report,
                        &mut mains,
                        &mut children,
                    )
                    .await;
                }
            }
        }
        report
    }

    async fn reconcile_companion_job(
        &self,
        parser: &watchdog_companion::CompanionParser,
        job: &watchdog_companion::CompanionJob,
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) {
        let Some(parent) = job.parent() else {
            report.warn();
            return;
        };
        let parent_id = SessionId::from_native(parent);
        if let Err(error) = self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: parent.native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: discovery_key("companion-parent", parent_id),
                title: None,
                startup_directory: None,
            })
            .await
        {
            log_reconcile_failure(RuntimeKind::CodexCompanion, "parent", &error);
            report.warn();
            return;
        }
        if mains.insert(parent_id) {
            report.main_sessions = report.main_sessions.saturating_add(1);
        }

        let child_id = SessionId::from_native(job.subject());
        let startup_directory =
            validated_directory(Some(job.workspace_root()), worktree_mappings, report);
        if let Err(error) = self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::CodexCompanion,
                native_id: job.subject().native_id().to_owned(),
                kind: SessionKind::Child,
                parent: Some(parent_id),
                event_key: discovery_key("companion-job", child_id),
                title: job.title().map(ToOwned::to_owned),
                startup_directory,
            })
            .await
        {
            log_reconcile_failure(RuntimeKind::CodexCompanion, "child", &error);
            report.warn();
            return;
        }
        let Ok(reconciled) = parser.reconcile(Some(job), None) else {
            report.warn();
            return;
        };
        let event_key = companion_event_key(child_id, job);
        let Ok(observation) = parser.observation(&reconciled, &event_key, self.clock.now()) else {
            report.warn();
            return;
        };
        if let Err(error) = self.api.ingest_native_observation(observation).await {
            log_reconcile_failure(RuntimeKind::CodexCompanion, "child", &error);
            report.warn();
        } else if children.insert(child_id) {
            report.child_sessions = report.child_sessions.saturating_add(1);
        }
    }
}

impl std::fmt::Debug for CompanionDiscovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompanionDiscovery")
            .finish_non_exhaustive()
    }
}

/// Automatic bounded Codex thread/spawn-edge discovery from read-only state.
#[derive(Clone)]
pub struct CodexDiscovery {
    api: AgentApi,
    clock: Arc<dyn Clock>,
}

impl CodexDiscovery {
    /// Construct discovery over the shared durable ingestion service.
    #[must_use]
    pub fn new(api: AgentApi, clock: Arc<dyn Clock>) -> Self {
        Self { api, clock }
    }

    /// Reconcile recent unarchived threads and exact native spawn edges.
    ///
    /// The bounded recency window is a bootstrap heuristic only: official
    /// events, MCP, and later process correlation may retain or add sessions
    /// outside it without broad historical discovery.
    pub async fn reconcile(
        &self,
        codex_roots: &[PathBuf],
        worktree_mappings: &[WorktreePathMapping],
    ) -> CodexDiscoveryReport {
        let mut report = RuntimeDiscoveryReport::default();
        let mut mains = BTreeSet::new();
        let mut children = BTreeSet::new();
        let cutoff = watchdog_domain::WallTimeMs::new(
            self.clock
                .now()
                .wall_time()
                .value()
                .saturating_sub(CODEX_BOOTSTRAP_WINDOW_MS),
        );
        for configured_root in codex_roots {
            let Ok(root) = CapabilityRoot::new(configured_root) else {
                report.warn();
                continue;
            };
            let database_relative = Path::new("state_5.sqlite");
            let database_path = root.path().join(database_relative);
            match database_path.symlink_metadata() {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => {
                    report.warn();
                    continue;
                }
            }
            if root.open_file(database_relative).is_err() {
                report.warn();
                continue;
            }
            let Ok(database_path) = database_path.canonicalize() else {
                report.warn();
                continue;
            };
            if !database_path.starts_with(root.path()) {
                report.warn();
                continue;
            }
            let Ok(reader) = watchdog_codex::CodexStateReader::open(&database_path).await else {
                report.warn();
                continue;
            };
            let Ok(threads) = reader
                .discover_recent_threads(cutoff, MAX_CODEX_THREADS)
                .await
            else {
                report.warn();
                continue;
            };
            self.reconcile_codex_threads(
                &threads,
                worktree_mappings,
                &mut report,
                &mut mains,
                &mut children,
            )
            .await;
        }
        report
    }

    async fn reconcile_codex_threads(
        &self,
        threads: &[watchdog_codex::CodexThread],
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) {
        for thread in threads
            .iter()
            .filter(|thread| thread.kind() == SessionKind::Main)
        {
            self.reconcile_codex_main(thread, worktree_mappings, report, mains)
                .await;
        }
        for thread in threads
            .iter()
            .filter(|thread| thread.kind() == SessionKind::Child)
        {
            self.reconcile_codex_child(thread, worktree_mappings, report, mains, children)
                .await;
        }
    }

    async fn reconcile_codex_main(
        &self,
        thread: &watchdog_codex::CodexThread,
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
    ) {
        if thread.cli_version() != watchdog_codex::TESTED_CODEX_VERSION {
            report.warn();
        }
        let main_id = SessionId::from_native(thread.subject());
        let startup_directory = validated_directory(Some(thread.cwd()), worktree_mappings, report);
        if let Err(error) = self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::CodexCli,
                native_id: thread.subject().native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: discovery_key("codex-state", main_id),
                title: Some(thread.title().to_owned()),
                startup_directory,
            })
            .await
        {
            log_reconcile_failure(RuntimeKind::CodexCli, "main", &error);
            report.warn();
        } else if mains.insert(main_id) {
            report.main_sessions = report.main_sessions.saturating_add(1);
        }
    }

    async fn reconcile_codex_child(
        &self,
        thread: &watchdog_codex::CodexThread,
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) {
        if thread.cli_version() != watchdog_codex::TESTED_CODEX_VERSION {
            report.warn();
        }
        let Some(parent) = thread.parent() else {
            report.warn();
            return;
        };
        let parent_id = SessionId::from_native(parent);
        if !mains.contains(&parent_id) {
            if let Err(error) = self
                .api
                .discover_session(DiscoveredSession {
                    runtime: RuntimeKind::CodexCli,
                    native_id: parent.native_id().to_owned(),
                    kind: SessionKind::Main,
                    parent: None,
                    event_key: discovery_key("codex-state-parent", parent_id),
                    title: None,
                    startup_directory: None,
                })
                .await
            {
                log_reconcile_failure(RuntimeKind::CodexCli, "parent", &error);
                report.warn();
                return;
            }
            mains.insert(parent_id);
            report.main_sessions = report.main_sessions.saturating_add(1);
        }
        let child_id = SessionId::from_native(thread.subject());
        let startup_directory = validated_directory(Some(thread.cwd()), worktree_mappings, report);
        let title = thread
            .agent_nickname()
            .or_else(|| thread.agent_role())
            .unwrap_or_else(|| thread.title())
            .to_owned();
        if let Err(error) = self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::CodexCli,
                native_id: thread.subject().native_id().to_owned(),
                kind: SessionKind::Child,
                parent: Some(parent_id),
                event_key: discovery_key("codex-state", child_id),
                title: Some(title),
                startup_directory,
            })
            .await
        {
            log_reconcile_failure(RuntimeKind::CodexCli, "child", &error);
            report.warn();
        } else if children.insert(child_id) {
            report.child_sessions = report.child_sessions.saturating_add(1);
        }
    }
}

impl std::fmt::Debug for CodexDiscovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodexDiscovery")
            .finish_non_exhaustive()
    }
}

fn companion_event_key(session: SessionId, job: &watchdog_companion::CompanionJob) -> String {
    format!(
        "companion-state:{session}:{}:{}",
        job.updated_at().unwrap_or("no-native-time"),
        state_key(job.state())
    )
}

const fn state_key(state: DetailedState) -> &'static str {
    match state {
        DetailedState::Starting => "starting",
        DetailedState::Running => "running",
        DetailedState::WaitingForAgent => "waiting-agent",
        DetailedState::WaitingForTool => "waiting-tool",
        DetailedState::WaitingForUser => "waiting-user",
        DetailedState::Idle => "idle",
        DetailedState::Stalled => "stalled",
        DetailedState::Completed => "completed",
        DetailedState::Failed => "failed",
        DetailedState::Cancelled => "cancelled",
        DetailedState::Disappeared => "disappeared",
        DetailedState::Unknown => "unknown",
    }
}

fn log_reconcile_failure(
    runtime: RuntimeKind,
    session_kind: &'static str,
    error: &crate::AgentApiError,
) {
    tracing::warn!(
        event = "adapter.session_reconcile_failed",
        runtime = runtime.as_str(),
        session_kind,
        error = %error,
        "Runtime-native session could not be reconciled"
    );
}

fn discovery_key(source: &str, session: SessionId) -> String {
    format!("{source}:{session}")
}

fn read_bounded_config(root: &CapabilityRoot, relative: &Path) -> Result<Option<Vec<u8>>, ()> {
    read_bounded_file(root, relative, watchdog_claude::MAX_TEAM_CONFIG_BYTES)
}

fn read_bounded_file(
    root: &CapabilityRoot,
    relative: &Path,
    maximum: usize,
) -> Result<Option<Vec<u8>>, ()> {
    let Ok(mut file) = root.open_file(relative) else {
        return Ok(None);
    };
    let limit = u64::try_from(maximum).map_err(|_| ())?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    if bytes.len() > maximum {
        return Err(());
    }
    Ok(Some(bytes))
}

fn validated_directory(
    candidate: Option<&Path>,
    mappings: &[WorktreePathMapping],
    report: &mut RuntimeDiscoveryReport,
) -> Option<String> {
    let candidate = candidate?;
    mappings
        .iter()
        .filter_map(|mapping| {
            mapping
                .validate_native_path(candidate)
                .map(|path| (mapping.native_root.components().count(), path))
        })
        .max_by_key(|(specificity, _)| *specificity)
        .map(|(_, path)| path)
        .or_else(|| {
            report.warn();
            None
        })
}

fn is_concrete_absolute(path: &Path) -> bool {
    path.is_absolute()
        && path != Path::new("/")
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::symlink, path::Path};

    use super::WorktreePathMapping;

    #[test]
    fn worktree_projection_rejects_traversal_and_symlink_escape() {
        let fixture = tempfile::tempdir().expect("fixture root");
        let mounted = fixture.path().join("mounted");
        let outside = fixture.path().join("outside");
        fs::create_dir(&mounted).expect("mounted root");
        fs::create_dir(&outside).expect("outside root");
        symlink(&outside, mounted.join("escape")).expect("escape symlink");
        let mapping = WorktreePathMapping::new("/host/repositories", mounted)
            .expect("mapping should be valid");

        assert!(
            mapping
                .validate_native_path(Path::new("/host/repositories/../secret"))
                .is_none()
        );
        assert!(
            mapping
                .validate_native_path(Path::new("/host/repositories/escape"))
                .is_none()
        );
    }
}
