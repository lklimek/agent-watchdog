use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Read as _,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
};

use thiserror::Error;
#[cfg(target_os = "linux")]
use watchdog_domain::ProcessId;
use watchdog_domain::{
    AdapterIdentity, BoundedText, Clock, DetailedState, EvidenceTrust, NativeSessionKey,
    ObservationEnvelope, ObservationId, ObservationPayload, ObservationSource, RuntimeKind,
    SessionId, SessionKind,
};
use watchdog_runtime::{
    CapabilityRoot, DirectoryScanner, FileCursor, FileIdentity, IncrementalReader, ReadBudget,
    ReadOutcome, ScanBudget,
};
use watchdog_store::{FileCursorRecord, WatchdogStore};

use crate::{AgentApi, DiscoveredSession, GitHubEnricher, RepositoryMetadata};

const MAX_SCAN_DEPTH: usize = 4;
const MAX_SCAN_ENTRIES: usize = 2_048;
const MAX_SCAN_PATH_BYTES: usize = 2 * 1_024 * 1_024;
const CLAUDE_BOOTSTRAP_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
const CLAUDE_TRANSCRIPT_PARSER_VERSION: u32 = 1;
const MAX_CLAUDE_BOOTSTRAP_BATCHES: usize = 1;
const MAX_CLAUDE_TRANSCRIPT_BATCHES: usize = 4;
const MAX_CLAUDE_TRANSCRIPT_BATCH_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_CLAUDE_TRANSCRIPT_RECORDS: usize = 128;
const MAX_CLAUDE_ALIAS_CACHE: usize = 2_048;
const CODEX_BOOTSTRAP_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_CODEX_THREADS: u32 = 1_000;
const CODEX_ROLLOUT_PARSER_VERSION: u32 = 1;
const MAX_CODEX_ROLLOUT_BATCHES: usize = 4;
const MAX_CODEX_ROLLOUT_BATCH_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_CODEX_ROLLOUT_RECORDS: usize = 128;
const COMPANION_LOG_CURSOR_VERSION: u32 = 1;

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
        self.project_relative(relative)?;
        self.native_root
            .join(relative)
            .to_str()
            .map(ToOwned::to_owned)
    }

    fn project_native_path(&self, candidate: &Path) -> Option<PathBuf> {
        let relative = candidate.strip_prefix(&self.native_root).ok()?;
        self.project_relative(relative)
    }

    fn project_relative(&self, relative: &Path) -> Option<PathBuf> {
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
        Some(projected)
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

struct ClaudeTranscriptCandidate {
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    kind: SessionKind,
    transcript_session: NativeSessionKey,
    expected_agent: Option<NativeSessionKey>,
    relative: PathBuf,
    path_key: BoundedText<4_096>,
}

impl ClaudeTranscriptCandidate {
    fn from_file(
        root: &CapabilityRoot,
        file: &Path,
        path_mappings: &[WorktreePathMapping],
    ) -> Option<Self> {
        let relative = file.strip_prefix(root.path()).ok()?.to_path_buf();
        if relative.extension().and_then(std::ffi::OsStr::to_str) != Some("jsonl") {
            return None;
        }
        let components = relative
            .components()
            .filter_map(|component| match component {
                Component::Normal(value) => value.to_str(),
                _ => None,
            })
            .collect::<Vec<_>>();
        if components.is_empty() || components.len() != relative.components().count() {
            return None;
        }
        let stem = relative.file_stem()?.to_str()?;
        let (subject, parent, kind, transcript_session, expected_agent) =
            if components.len() == 4 && components[2] == "subagents" {
                let child = stem.strip_prefix("agent-")?;
                let parent = components[1];
                let subject = NativeSessionKey::new(RuntimeKind::ClaudeCode, child).ok()?;
                let parent = NativeSessionKey::new(RuntimeKind::ClaudeCode, parent).ok()?;
                (
                    subject.clone(),
                    Some(parent.clone()),
                    SessionKind::Child,
                    parent,
                    Some(subject),
                )
            } else if components.len() == 2 {
                let subject = NativeSessionKey::new(RuntimeKind::ClaudeCode, stem).ok()?;
                (subject.clone(), None, SessionKind::Main, subject, None)
            } else {
                return None;
            };
        let mapping = path_mappings
            .iter()
            .find(|mapping| mapping.mounted_root() == root.path())?;
        let native_path = mapping.native_root().join(&relative);
        let native_path = native_path.to_str()?;
        let path_key =
            BoundedText::new("transcript_path_key", format!("claude:{native_path}")).ok()?;
        Some(Self {
            subject,
            parent,
            kind,
            transcript_session,
            expected_agent,
            relative,
            path_key,
        })
    }

    fn accepts(&self, signal: &watchdog_claude::ClaudeTranscriptSignal) -> bool {
        signal
            .session_id()
            .is_none_or(|value| value == self.transcript_session.native_id())
            && self.expected_agent.as_ref().is_none_or(|expected| {
                signal
                    .agent_id()
                    .is_none_or(|value| value == expected.native_id())
            })
    }

    fn bind_team_member(&mut self, alias: &ClaudeTeamTranscriptAlias) {
        self.subject = alias.subject.clone();
        self.parent = Some(alias.parent.clone());
        self.kind = SessionKind::Child;
    }
}

#[derive(Clone)]
struct ClaudeTeamTranscriptAlias {
    subject: NativeSessionKey,
    parent: NativeSessionKey,
    agent_type: String,
    cwd: PathBuf,
}

#[derive(Default)]
struct ClaudeTeamTranscriptAliases(Vec<ClaudeTeamTranscriptAlias>);

impl ClaudeTeamTranscriptAliases {
    fn extend(&mut self, team: &watchdog_claude::ClaudeTeam) {
        self.0.extend(team.members().iter().filter_map(|member| {
            Some(ClaudeTeamTranscriptAlias {
                subject: member.subject().clone(),
                parent: team.lead().clone(),
                agent_type: member.agent_type()?.to_owned(),
                cwd: member.cwd()?.to_path_buf(),
            })
        }));
    }

    fn resolve(&self, bootstrap: &ClaudeTranscriptBootstrap) -> Option<&ClaudeTeamTranscriptAlias> {
        let title = bootstrap.title.as_deref()?;
        let cwd = bootstrap.cwd.as_deref()?;
        let mut candidates = self
            .0
            .iter()
            .filter(|alias| alias.agent_type == title && alias.cwd == cwd);
        let matched = candidates.next()?;
        candidates.next().is_none().then_some(matched)
    }
}

struct ClaudeTaskAggregate {
    subject: NativeSessionKey,
    states: BTreeSet<DetailedState>,
    latest_terminal: Option<(DetailedState, u128)>,
    latest_modified_ns: u128,
    task_count: u32,
}

impl ClaudeTaskAggregate {
    fn new(subject: NativeSessionKey) -> Self {
        Self {
            subject,
            states: BTreeSet::new(),
            latest_terminal: None,
            latest_modified_ns: 0,
            task_count: 0,
        }
    }

    fn observe(&mut self, state: DetailedState, modified_ns: u128) {
        self.states.insert(state);
        if matches!(
            state,
            DetailedState::Completed | DetailedState::Failed | DetailedState::Cancelled
        ) && self
            .latest_terminal
            .is_none_or(|(_, current_ns)| modified_ns >= current_ns)
        {
            self.latest_terminal = Some((state, modified_ns));
        }
        self.latest_modified_ns = self.latest_modified_ns.max(modified_ns);
        self.task_count = self.task_count.saturating_add(1);
    }

    fn state(&self) -> DetailedState {
        if self.states.contains(&DetailedState::Running) {
            DetailedState::Running
        } else if self.states.contains(&DetailedState::Starting) {
            DetailedState::Starting
        } else if let Some((state, _)) = self.latest_terminal {
            state
        } else {
            DetailedState::Unknown
        }
    }
}

#[derive(Default)]
struct ClaudeTranscriptBootstrap {
    cwd: Option<PathBuf>,
    branch: Option<String>,
    title: Option<String>,
    drifted: bool,
}

impl ClaudeTranscriptBootstrap {
    fn merge(
        &mut self,
        candidate: &ClaudeTranscriptCandidate,
        signal: &watchdog_claude::ClaudeTranscriptSignal,
    ) {
        if !candidate.accepts(signal) {
            self.drifted = true;
            return;
        }
        if self.cwd.is_none() {
            self.cwd = signal.cwd().map(Path::to_path_buf);
        }
        if self.branch.is_none() {
            self.branch = signal.git_branch().map(ToOwned::to_owned);
        }
        if self.title.is_none() {
            self.title = signal.agent_setting().map(ToOwned::to_owned);
        }
    }
}

/// Automatic Claude session, subagent, and team discovery independent from optional MCP/hooks.
#[derive(Clone)]
pub struct ClaudeDiscovery {
    api: AgentApi,
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
    alias_cache: Arc<Mutex<HashMap<String, ClaudeTeamTranscriptAlias>>>,
}

impl std::fmt::Debug for ClaudeDiscovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClaudeDiscovery")
            .finish_non_exhaustive()
    }
}

impl ClaudeDiscovery {
    /// Construct discovery over the shared durable ingestion service.
    #[must_use]
    pub fn new(api: AgentApi, store: WatchdogStore, clock: Arc<dyn Clock>) -> Self {
        Self {
            api,
            store,
            clock,
            alias_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Scan exact configured Claude roots without following symlinks, parse
    /// current team configs under strict byte/entry bounds, and persist every
    /// valid team even when another configured team is malformed.
    pub async fn reconcile(
        &self,
        claude_roots: &[PathBuf],
        claude_path_mappings: &[WorktreePathMapping],
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
        let mut scans = Vec::new();
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
            scans.push((root, scan));
        }
        let mut aliases = ClaudeTeamTranscriptAliases::default();
        let mut teams = Vec::new();
        for (root, scan) in &scans {
            let candidates =
                std::iter::once(root.path().to_owned()).chain(scan.directories().iter().cloned());
            for directory in candidates {
                let Ok(relative) = directory.strip_prefix(root.path()) else {
                    report.warn();
                    continue;
                };
                let config = relative.join("config.json");
                let bytes = match read_bounded_config(root, &config) {
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
                aliases.extend(&team);
                self.reconcile_team(
                    &team,
                    worktree_mappings,
                    &mut report,
                    &mut mains,
                    &mut children,
                )
                .await;
                teams.push(team);
            }
        }
        self.reconcile_team_tasks(&scans, &teams, &mut report).await;
        for (root, scan) in &scans {
            for file in scan.files() {
                let Some(candidate) =
                    ClaudeTranscriptCandidate::from_file(root, file, claude_path_mappings)
                else {
                    continue;
                };
                self.reconcile_transcript(
                    root,
                    candidate,
                    &aliases,
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
                adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION.to_owned(),
                evidence_source: "claude:team-config".to_owned(),
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
                    adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION.to_owned(),
                    evidence_source: "claude:team-config".to_owned(),
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

    async fn reconcile_team_tasks(
        &self,
        scans: &[(CapabilityRoot, watchdog_runtime::ScanResult)],
        teams: &[watchdog_claude::ClaudeTeam],
        report: &mut RuntimeDiscoveryReport,
    ) {
        let mut aggregates = BTreeMap::<SessionId, ClaudeTaskAggregate>::new();
        for (root, scan) in scans {
            for file in scan.files() {
                let Some((relative, team_name)) = claude_task_candidate(root, file) else {
                    continue;
                };
                let mut matching_teams = teams
                    .iter()
                    .filter(|team| team.name() == Some(team_name.as_str()));
                let Some(team) = matching_teams.next() else {
                    continue;
                };
                if matching_teams.next().is_some() {
                    report.warn();
                    continue;
                }
                let bytes =
                    match read_bounded_file(root, &relative, watchdog_claude::MAX_HOOK_BYTES) {
                        Ok(Some(bytes)) => bytes,
                        Ok(None) => continue,
                        Err(()) => {
                            report.warn();
                            continue;
                        }
                    };
                let Ok(task) = watchdog_claude::parse_task_record(&bytes) else {
                    report.warn();
                    continue;
                };
                let Some(owner) = task.owner() else {
                    continue;
                };
                let mut matching_members = team
                    .members()
                    .iter()
                    .filter(|member| member.name() == owner);
                let Some(member) = matching_members.next() else {
                    continue;
                };
                if matching_members.next().is_some() {
                    report.warn();
                    continue;
                }
                let Some(modified_ns) = task_modified_ns(root, &relative) else {
                    report.warn();
                    continue;
                };
                let session_id = SessionId::from_native(member.subject());
                aggregates
                    .entry(session_id)
                    .or_insert_with(|| ClaudeTaskAggregate::new(member.subject().clone()))
                    .observe(task.state(), modified_ns);
            }
        }
        for (session_id, aggregate) in aggregates {
            if self
                .ingest_team_task_aggregate(session_id, &aggregate)
                .await
                .is_err()
            {
                report.warn();
            }
        }
    }

    async fn ingest_team_task_aggregate(
        &self,
        session_id: SessionId,
        aggregate: &ClaudeTaskAggregate,
    ) -> Result<(), ()> {
        let state = aggregate.state();
        let event_key = format!(
            "{session_id}:{}:{}:{}",
            state_key(state),
            aggregate.latest_modified_ns,
            aggregate.task_count
        );
        let observation_id =
            ObservationId::from_native(RuntimeKind::ClaudeCode, "team-task", event_key)
                .map_err(|_| ())?;
        if self
            .store
            .observation(observation_id)
            .await
            .map_err(|_| ())?
            .is_some()
        {
            return Ok(());
        }
        let observation = ObservationEnvelope::new(
            observation_id,
            aggregate.subject.clone(),
            self.clock.now(),
            claude_team_task_source()?,
            ObservationPayload::NativeState(state),
        )
        .map_err(|_| ())?;
        self.api
            .ingest_native_observation(observation)
            .await
            .map(|_| ())
            .map_err(|_| ())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the adapter keeps capability and bounded reconciliation scopes explicit"
    )]
    async fn reconcile_transcript(
        &self,
        root: &CapabilityRoot,
        candidate: ClaudeTranscriptCandidate,
        aliases: &ClaudeTeamTranscriptAliases,
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) {
        if !self.transcript_is_recent(root, &candidate, report) {
            return;
        }
        let Ok((candidate, bootstrap)) = self
            .prepare_transcript_candidate(root, candidate, aliases)
            .await
        else {
            report.warn();
            return;
        };
        let Ok(parent) = self
            .ensure_transcript_parent(&candidate, mains, report)
            .await
        else {
            report.warn();
            return;
        };
        let session_id = SessionId::from_native(&candidate.subject);
        let Ok(existing) = self.existing_metadata(session_id).await else {
            report.warn();
            return;
        };
        let title = if existing
            .as_ref()
            .and_then(|metadata| metadata.title())
            .is_some()
        {
            None
        } else if candidate.kind == SessionKind::Child {
            Self::subagent_title(root, &candidate).or(bootstrap.title.clone())
        } else {
            bootstrap.title.clone()
        };
        let startup_directory = if existing
            .as_ref()
            .and_then(|metadata| metadata.startup_directory())
            .is_some()
        {
            None
        } else {
            validated_directory(bootstrap.cwd.as_deref(), worktree_mappings, report)
        };
        let Ok(view) = self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: candidate.subject.native_id().to_owned(),
                kind: candidate.kind,
                parent,
                event_key: discovery_key("claude-transcript", session_id),
                adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION.to_owned(),
                evidence_source: "claude:transcript-path".to_owned(),
                title,
                startup_directory,
            })
            .await
        else {
            report.warn();
            return;
        };
        if self
            .api
            .enrich_repository_metadata(
                view.session,
                RepositoryMetadata {
                    branch: bootstrap.branch,
                    ..RepositoryMetadata::default()
                },
            )
            .await
            .is_err()
        {
            report.warn();
        }
        if bootstrap.drifted {
            report.warn();
            let _ = self
                .emit_claude_compatibility_warning(
                    &candidate,
                    "bootstrap",
                    watchdog_claude::ClaudeParseError::UnsupportedRecord.compatibility_warning(),
                )
                .await;
        }
        self.reconcile_transcript_cursor(root, &candidate, report)
            .await;
        match candidate.kind {
            SessionKind::Main if mains.insert(session_id) => {
                report.main_sessions = report.main_sessions.saturating_add(1);
            }
            SessionKind::Child if children.insert(session_id) => {
                report.child_sessions = report.child_sessions.saturating_add(1);
            }
            SessionKind::Main | SessionKind::Child => {}
        }
    }

    async fn prepare_transcript_candidate(
        &self,
        root: &CapabilityRoot,
        mut candidate: ClaudeTranscriptCandidate,
        aliases: &ClaudeTeamTranscriptAliases,
    ) -> Result<(ClaudeTranscriptCandidate, ClaudeTranscriptBootstrap), ()> {
        let cached_alias = self.cached_alias(&candidate.path_key);
        let cursor = self.store.file_cursor(&candidate.path_key).await;
        let path_session_exists = self
            .store
            .session_by_id(SessionId::from_native(&candidate.transcript_session))
            .await;
        let bootstrap = match (cursor, path_session_exists) {
            (Ok(None), Ok(_)) => Self::bootstrap_transcript(root, &candidate),
            (Ok(Some(_)), Ok(path_session))
                if candidate.kind == SessionKind::Main
                    && cached_alias.is_none()
                    && path_session.is_none()
                    && !aliases.0.is_empty() =>
            {
                Self::bootstrap_transcript(root, &candidate)
            }
            (Ok(Some(_)), Ok(_)) => ClaudeTranscriptBootstrap::default(),
            (Err(_), _) | (_, Err(_)) => return Err(()),
        };
        if candidate.kind == SessionKind::Main {
            if let Some(alias) = cached_alias {
                candidate.bind_team_member(&alias);
            } else if let Some(alias) = aliases.resolve(&bootstrap) {
                candidate.bind_team_member(alias);
                self.cache_alias(&candidate.path_key, alias.clone());
            }
        }
        Ok((candidate, bootstrap))
    }

    async fn ensure_transcript_parent(
        &self,
        candidate: &ClaudeTranscriptCandidate,
        mains: &mut BTreeSet<SessionId>,
        report: &mut RuntimeDiscoveryReport,
    ) -> Result<Option<SessionId>, ()> {
        let Some(parent_native) = candidate.parent.as_ref() else {
            return Ok(None);
        };
        let parent_id = SessionId::from_native(parent_native);
        if mains.contains(&parent_id) {
            return Ok(Some(parent_id));
        }
        self.api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: parent_native.native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: discovery_key("claude-transcript-parent", parent_id),
                adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION.to_owned(),
                evidence_source: "claude:transcript-path".to_owned(),
                title: None,
                startup_directory: None,
            })
            .await
            .map_err(|_| ())?;
        mains.insert(parent_id);
        report.main_sessions = report.main_sessions.saturating_add(1);
        Ok(Some(parent_id))
    }

    async fn existing_metadata(
        &self,
        session_id: SessionId,
    ) -> Result<Option<watchdog_store::SessionMetadataRecord>, ()> {
        let Some(record) = self.store.session_by_id(session_id).await.map_err(|_| ())? else {
            return Ok(None);
        };
        self.store
            .session_metadata(record.session)
            .await
            .map_err(|_| ())
    }

    fn cached_alias(&self, path_key: &BoundedText<4_096>) -> Option<ClaudeTeamTranscriptAlias> {
        self.alias_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(path_key.as_str())
            .cloned()
    }

    fn cache_alias(&self, path_key: &BoundedText<4_096>, alias: ClaudeTeamTranscriptAlias) {
        let mut cache = self
            .alias_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cache.len() >= MAX_CLAUDE_ALIAS_CACHE && !cache.contains_key(path_key.as_str()) {
            cache.clear();
        }
        cache.insert(path_key.as_str().to_owned(), alias);
    }

    fn transcript_is_recent(
        &self,
        root: &CapabilityRoot,
        candidate: &ClaudeTranscriptCandidate,
        report: &mut RuntimeDiscoveryReport,
    ) -> bool {
        let Ok(file) = root.open_file(&candidate.relative) else {
            report.warn();
            return false;
        };
        let Ok(modified) = file.metadata().and_then(|metadata| metadata.modified()) else {
            report.warn();
            return false;
        };
        let Ok(elapsed) = modified.duration_since(UNIX_EPOCH) else {
            report.warn();
            return false;
        };
        let Ok(modified_ms) = i64::try_from(elapsed.as_millis()) else {
            report.warn();
            return false;
        };
        modified_ms
            >= self
                .clock
                .now()
                .wall_time()
                .value()
                .saturating_sub(CLAUDE_BOOTSTRAP_WINDOW_MS)
    }

    fn bootstrap_transcript(
        root: &CapabilityRoot,
        candidate: &ClaudeTranscriptCandidate,
    ) -> ClaudeTranscriptBootstrap {
        let reader = claude_transcript_reader();
        let Ok(mut cursor) =
            reader.cursor_at_start(root, &candidate.relative, CLAUDE_TRANSCRIPT_PARSER_VERSION)
        else {
            return ClaudeTranscriptBootstrap {
                drifted: true,
                ..ClaudeTranscriptBootstrap::default()
            };
        };
        let mut bootstrap = ClaudeTranscriptBootstrap::default();
        for _ in 0..MAX_CLAUDE_BOOTSTRAP_BATCHES {
            let Ok(ReadOutcome::Records(batch)) = reader.read(root, &candidate.relative, &cursor)
            else {
                bootstrap.drifted = true;
                break;
            };
            for record in batch.records() {
                match watchdog_claude::parse_transcript_record(record) {
                    Ok(signal) => bootstrap.merge(candidate, &signal),
                    Err(_) => bootstrap.drifted = true,
                }
            }
            cursor = batch.cursor().clone();
            if !batch.continuation_required() {
                break;
            }
        }
        bootstrap
    }

    fn subagent_title(
        root: &CapabilityRoot,
        candidate: &ClaudeTranscriptCandidate,
    ) -> Option<String> {
        let sidecar = candidate.relative.with_extension("meta.json");
        let Ok(Some(bytes)) = read_bounded_file(root, &sidecar, watchdog_claude::MAX_HOOK_BYTES)
        else {
            return None;
        };
        if let Ok(metadata) = watchdog_claude::parse_subagent_metadata(&bytes) {
            metadata.agent_type().map(ToOwned::to_owned)
        } else {
            None
        }
    }

    async fn reconcile_transcript_cursor(
        &self,
        root: &CapabilityRoot,
        candidate: &ClaudeTranscriptCandidate,
        report: &mut RuntimeDiscoveryReport,
    ) {
        let reader = claude_transcript_reader();
        let Ok(saved) = self.store.file_cursor(&candidate.path_key).await else {
            report.warn();
            return;
        };
        let Some(saved) = saved else {
            match reader.cursor_at_end(root, &candidate.relative, CLAUDE_TRANSCRIPT_PARSER_VERSION)
            {
                Ok(cursor) => {
                    if self
                        .persist_claude_cursor(&candidate.path_key, &cursor, None)
                        .await
                        .is_err()
                    {
                        report.warn();
                    }
                }
                Err(_) => report.warn(),
            }
            return;
        };
        self.tail_claude_transcript(root, candidate, reader, &saved, report)
            .await;
    }

    async fn tail_claude_transcript(
        &self,
        root: &CapabilityRoot,
        candidate: &ClaudeTranscriptCandidate,
        reader: IncrementalReader,
        saved: &FileCursorRecord,
        report: &mut RuntimeDiscoveryReport,
    ) {
        let mut cursor = FileCursor::resume_from_complete(
            FileIdentity::new(saved.device_id(), saved.inode()),
            saved.complete_record_offset(),
            CLAUDE_TRANSCRIPT_PARSER_VERSION,
        );
        let mut last_observation_id = saved.last_observation_id();
        let mut complete_offset = cursor.complete_offset();
        for _ in 0..MAX_CLAUDE_TRANSCRIPT_BATCHES {
            let Ok(outcome) = reader.read(root, &candidate.relative, &cursor) else {
                report.warn();
                return;
            };
            let ReadOutcome::Records(batch) = outcome else {
                report.warn();
                match reader.cursor_at_end(
                    root,
                    &candidate.relative,
                    CLAUDE_TRANSCRIPT_PARSER_VERSION,
                ) {
                    Ok(new_cursor) => {
                        let _ = self
                            .persist_claude_cursor(
                                &candidate.path_key,
                                &new_cursor,
                                last_observation_id,
                            )
                            .await;
                    }
                    Err(_) => report.warn(),
                }
                return;
            };
            for record in batch.records() {
                complete_offset = complete_offset
                    .saturating_add(u64::try_from(record.len()).unwrap_or(u64::MAX))
                    .saturating_add(1);
                let event_key = format!("{}:{complete_offset}", candidate.subject.native_id());
                match self
                    .ingest_claude_transcript_record(candidate, record, &event_key)
                    .await
                {
                    Ok((Some(observation_id), warning)) => {
                        last_observation_id = Some(observation_id);
                        if warning {
                            report.warn();
                        }
                    }
                    Ok((None, warning)) => {
                        if warning {
                            report.warn();
                        }
                    }
                    Err(()) => {
                        report.warn();
                        return;
                    }
                }
            }
            cursor = batch.cursor().clone();
            if self
                .persist_claude_cursor(&candidate.path_key, &cursor, last_observation_id)
                .await
                .is_err()
            {
                report.warn();
                return;
            }
            if !batch.continuation_required() {
                return;
            }
        }
    }

    async fn ingest_claude_transcript_record(
        &self,
        candidate: &ClaudeTranscriptCandidate,
        record: &[u8],
        event_key: &str,
    ) -> Result<(Option<ObservationId>, bool), ()> {
        let signal = match watchdog_claude::parse_transcript_record(record) {
            Ok(signal) => signal,
            Err(error) => {
                return Ok((
                    self.emit_claude_compatibility_warning(
                        candidate,
                        event_key,
                        error.compatibility_warning(),
                    )
                    .await,
                    true,
                ));
            }
        };
        if !candidate.accepts(&signal) {
            let source = claude_transcript_source("transcript:identity-conflict")?;
            let observation_id = ObservationId::from_native(
                RuntimeKind::ClaudeCode,
                "transcript-conflict",
                event_key,
            )
            .map_err(|_| ())?;
            let observation = ObservationEnvelope::new(
                observation_id,
                candidate.subject.clone(),
                self.clock.now(),
                source,
                ObservationPayload::SourceConflict(
                    BoundedText::new(
                        "source_conflict",
                        "Claude transcript identity conflicts with its native path",
                    )
                    .map_err(|_| ())?,
                ),
            )
            .map_err(|_| ())?;
            self.api
                .ingest_native_observation(observation)
                .await
                .map_err(|_| ())?;
            return Ok((Some(observation_id), true));
        }
        if !signal.is_activity() {
            return Ok((None, false));
        }
        let observation_id =
            ObservationId::from_native(RuntimeKind::ClaudeCode, "transcript", event_key)
                .map_err(|_| ())?;
        let observation = ObservationEnvelope::new(
            observation_id,
            candidate.subject.clone(),
            self.clock.now(),
            claude_transcript_source("transcript:jsonl")?,
            ObservationPayload::Progress(
                BoundedText::new("progress", "Claude transcript activity").map_err(|_| ())?,
            ),
        )
        .map_err(|_| ())?;
        self.api
            .ingest_native_observation(observation)
            .await
            .map_err(|_| ())?;
        Ok((Some(observation_id), false))
    }

    async fn persist_claude_cursor(
        &self,
        path_key: &BoundedText<4_096>,
        cursor: &FileCursor,
        last_observation_id: Option<ObservationId>,
    ) -> Result<(), ()> {
        let record = FileCursorRecord::new(
            path_key.clone(),
            cursor.identity().device(),
            cursor.identity().inode(),
            cursor.complete_offset(),
            cursor.complete_offset(),
            BoundedText::new(
                "parser_version",
                format!("claude-transcript-v{}", cursor.parser_version()),
            )
            .map_err(|_| ())?,
            last_observation_id,
        )
        .map_err(|_| ())?;
        self.store.save_file_cursor(&record).await.map_err(|_| ())
    }

    async fn emit_claude_compatibility_warning(
        &self,
        candidate: &ClaudeTranscriptCandidate,
        event_key: &str,
        warning: watchdog_domain::CompatibilityWarning,
    ) -> Option<ObservationId> {
        let observation_id = ObservationId::from_native(
            RuntimeKind::ClaudeCode,
            "transcript-compatibility",
            format!("{}:{event_key}", candidate.subject.native_id()),
        )
        .ok()?;
        let observation = ObservationEnvelope::new(
            observation_id,
            candidate.subject.clone(),
            self.clock.now(),
            claude_transcript_source("transcript:compatibility").ok()?,
            ObservationPayload::Compatibility(warning),
        )
        .ok()?;
        self.api
            .ingest_native_observation(observation)
            .await
            .ok()
            .map(|_| observation_id)
    }
}

fn claude_transcript_reader() -> IncrementalReader {
    IncrementalReader::new(
        ReadBudget::new(
            MAX_CLAUDE_TRANSCRIPT_BATCH_BYTES,
            watchdog_claude::MAX_TRANSCRIPT_RECORD_BYTES,
            MAX_CLAUDE_TRANSCRIPT_RECORDS,
        )
        .unwrap_or_else(|_| unreachable!("static Claude transcript budget is valid")),
    )
}

fn claude_transcript_source(evidence: &'static str) -> Result<ObservationSource, ()> {
    ObservationSource::new(
        AdapterIdentity::new(
            RuntimeKind::ClaudeCode,
            watchdog_claude::TESTED_CLAUDE_VERSION,
        )
        .map_err(|_| ())?,
        evidence,
        EvidenceTrust::Corroborating,
        None,
    )
    .map_err(|_| ())
}

fn claude_team_task_source() -> Result<ObservationSource, ()> {
    ObservationSource::new(
        AdapterIdentity::new(
            RuntimeKind::ClaudeCode,
            watchdog_claude::TESTED_CLAUDE_VERSION,
        )
        .map_err(|_| ())?,
        "team-task:status",
        EvidenceTrust::Corroborating,
        None,
    )
    .map_err(|_| ())
}

fn claude_task_candidate(root: &CapabilityRoot, file: &Path) -> Option<(PathBuf, String)> {
    let relative = file.strip_prefix(root.path()).ok()?.to_path_buf();
    if relative.extension().and_then(std::ffi::OsStr::to_str) != Some("json") {
        return None;
    }
    let components = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>();
    if components.len() != 2 || components.len() != relative.components().count() {
        return None;
    }
    let task_id = relative.file_stem()?.to_str()?;
    if task_id == "config"
        || task_id.is_empty()
        || !task_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return None;
    }
    let team_name = components[0].to_owned();
    Some((relative, team_name))
}

fn task_modified_ns(root: &CapabilityRoot, relative: &Path) -> Option<u128> {
    root.open_file(relative)
        .ok()?
        .metadata()
        .ok()?
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_nanos())
}

/// Automatic Codex Companion job discovery from current per-workspace state.
#[derive(Clone)]
pub struct CompanionDiscovery {
    api: AgentApi,
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
}

impl CompanionDiscovery {
    /// Construct discovery over the shared durable ingestion service.
    #[must_use]
    pub fn new(api: AgentApi, store: WatchdogStore, clock: Arc<dyn Clock>) -> Self {
        Self { api, store, clock }
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
                    let detail = read_companion_detail(&root, relative, &parser, job, &mut report);
                    let Ok(reconciled) = parser.reconcile(Some(job), detail.as_ref()) else {
                        report.warn();
                        continue;
                    };
                    if reconciled.consistency()
                        == watchdog_companion::CompanionConsistency::Conflicted
                    {
                        report.warn();
                    }
                    let reconciled_session = self
                        .reconcile_companion_job(
                            &parser,
                            &reconciled,
                            worktree_mappings,
                            &mut report,
                            &mut mains,
                            &mut children,
                        )
                        .await;
                    if reconciled_session {
                        self.reconcile_companion_log(
                            &root,
                            relative,
                            &parser,
                            reconciled.job(),
                            &mut report,
                        )
                        .await;
                    }
                }
            }
        }
        report
    }

    async fn reconcile_companion_job(
        &self,
        parser: &watchdog_companion::CompanionParser,
        reconciled: &watchdog_companion::ReconciledCompanionJob,
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) -> bool {
        let reconciled_job = reconciled.job();
        let Some(parent) = reconciled.parent() else {
            report.warn();
            return false;
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
                adapter_version: format!(
                    "companion-{}",
                    watchdog_companion::TESTED_COMPANION_VERSION
                ),
                evidence_source: "companion:parent-summary".to_owned(),
                title: None,
                startup_directory: None,
            })
            .await
        {
            log_reconcile_failure(RuntimeKind::CodexCompanion, "parent", &error);
            report.warn();
            return false;
        }
        if mains.insert(parent_id) {
            report.main_sessions = report.main_sessions.saturating_add(1);
        }

        let child_id = SessionId::from_native(reconciled.subject());
        let startup_directory = validated_directory(
            Some(reconciled_job.workspace_root()),
            worktree_mappings,
            report,
        );
        if let Err(error) = self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::CodexCompanion,
                native_id: reconciled.subject().native_id().to_owned(),
                kind: SessionKind::Child,
                parent: Some(parent_id),
                event_key: discovery_key("companion-job", child_id),
                adapter_version: watchdog_companion::TESTED_COMPANION_VERSION.to_owned(),
                evidence_source: "companion:state-summary".to_owned(),
                title: reconciled_job.title().map(ToOwned::to_owned),
                startup_directory,
            })
            .await
        {
            log_reconcile_failure(RuntimeKind::CodexCompanion, "child", &error);
            report.warn();
            return false;
        }
        let event_key = companion_event_key(child_id, reconciled_job);
        let Ok(observation) = parser.observation(reconciled, &event_key, self.clock.now()) else {
            report.warn();
            return false;
        };
        if let Err(error) = self.api.ingest_native_observation(observation).await {
            log_reconcile_failure(RuntimeKind::CodexCompanion, "child", &error);
            report.warn();
            return false;
        }
        #[cfg(target_os = "linux")]
        self.ingest_companion_process(reconciled_job).await;
        if children.insert(child_id) {
            report.child_sessions = report.child_sessions.saturating_add(1);
        }
        true
    }

    async fn reconcile_companion_log(
        &self,
        root: &CapabilityRoot,
        workspace_relative: &Path,
        parser: &watchdog_companion::CompanionParser,
        job: &watchdog_companion::CompanionJob,
        report: &mut RuntimeDiscoveryReport,
    ) {
        let native_id = job.subject().native_id();
        if !safe_native_filename(native_id) {
            report.warn();
            return;
        }
        let relative = workspace_relative
            .join("jobs")
            .join(format!("{native_id}.log"));
        let Ok(path_key) = BoundedText::new(
            "companion_log_path_key",
            format!("companion-log:{}", root.path().join(&relative).display()),
        ) else {
            report.warn();
            return;
        };
        let reader = IncrementalReader::new(
            ReadBudget::new(1, 1, 1)
                .unwrap_or_else(|_| unreachable!("static metadata-only cursor budget is valid")),
        );
        let Ok(current) = reader.cursor_at_end(root, &relative, COMPANION_LOG_CURSOR_VERSION)
        else {
            // Logs are optional and pruned by Companion. Absence is neutral.
            return;
        };
        let Ok(saved) = self.store.file_cursor(&path_key).await else {
            report.warn();
            return;
        };
        let expected_version = format!("companion-log-v{COMPANION_LOG_CURSOR_VERSION}");
        let appended = saved.as_ref().is_some_and(|saved| {
            saved.parser_version().as_str() == expected_version
                && saved.device_id() == current.identity().device()
                && saved.inode() == current.identity().inode()
                && current.read_offset() > saved.byte_offset()
        });
        let last_observation_id = if appended {
            let event_key = format!(
                "{}:{}:{}:{}",
                SessionId::from_native(job.subject()),
                current.identity().device(),
                current.identity().inode(),
                current.read_offset()
            );
            let Ok(observation) = parser.log_activity(job.subject(), &event_key, self.clock.now())
            else {
                report.warn();
                return;
            };
            let observation_id = observation.observation_id();
            if self
                .api
                .ingest_native_observation(observation)
                .await
                .is_err()
            {
                report.warn();
                return;
            }
            Some(observation_id)
        } else if saved.as_ref().is_some_and(|saved| {
            saved.parser_version().as_str() == expected_version
                && saved.device_id() == current.identity().device()
                && saved.inode() == current.identity().inode()
                && saved.byte_offset() == current.read_offset()
        }) {
            return;
        } else {
            None
        };
        let Ok(record) = FileCursorRecord::new(
            path_key,
            current.identity().device(),
            current.identity().inode(),
            current.read_offset(),
            current.read_offset(),
            BoundedText::new("parser_version", expected_version)
                .unwrap_or_else(|_| unreachable!("static parser version is bounded")),
            last_observation_id,
        ) else {
            report.warn();
            return;
        };
        if self.store.save_file_cursor(&record).await.is_err() {
            report.warn();
        }
    }

    #[cfg(target_os = "linux")]
    async fn ingest_companion_process(&self, job: &watchdog_companion::CompanionJob) {
        let Some(pid) = job.pid() else {
            return;
        };
        let Ok(pid) = ProcessId::new(pid) else {
            return;
        };
        let Ok(sampler) = watchdog_process::LinuxProcessSampler::new(1) else {
            return;
        };
        let Ok(identity) = sampler.read_identity(pid) else {
            return;
        };
        let event_key = format!(
            "{}:{}:{}",
            SessionId::from_native(job.subject()),
            identity.pid().value(),
            identity.start_time_ticks()
        );
        let Ok(source) = ObservationSource::new(
            AdapterIdentity::new(
                RuntimeKind::CodexCompanion,
                watchdog_companion::TESTED_COMPANION_VERSION,
            )
            .unwrap_or_else(|_| unreachable!("static adapter identity is bounded")),
            "state:pid",
            EvidenceTrust::Corroborating,
            None,
        ) else {
            return;
        };
        let Ok(observation_id) =
            ObservationId::from_native(RuntimeKind::CodexCompanion, "process-identity", event_key)
        else {
            return;
        };
        let Ok(observation) = ObservationEnvelope::new(
            observation_id,
            job.subject().clone(),
            self.clock.now(),
            source,
            ObservationPayload::ProcessIdentity(identity),
        ) else {
            return;
        };
        let _ = self.api.ingest_native_observation(observation).await;
    }
}

impl std::fmt::Debug for CompanionDiscovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompanionDiscovery")
            .finish_non_exhaustive()
    }
}

fn read_companion_detail(
    root: &CapabilityRoot,
    workspace_relative: &Path,
    parser: &watchdog_companion::CompanionParser,
    summary: &watchdog_companion::CompanionJob,
    report: &mut RuntimeDiscoveryReport,
) -> Option<watchdog_companion::CompanionJob> {
    let native_id = summary.subject().native_id();
    if !safe_native_filename(native_id) {
        report.warn();
        return None;
    }
    let detail_path = workspace_relative
        .join("jobs")
        .join(format!("{native_id}.json"));
    let bytes = match read_bounded_file(root, &detail_path, watchdog_companion::MAX_DETAIL_BYTES) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return None,
        Err(()) => {
            report.warn();
            return None;
        }
    };
    if let Ok(detail) = parser.parse_detail(&bytes) {
        Some(detail)
    } else {
        report.warn();
        None
    }
}

fn safe_native_filename(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Automatic bounded Codex thread/spawn-edge discovery from read-only state.
#[derive(Clone)]
pub struct CodexDiscovery {
    api: AgentApi,
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
}

impl CodexDiscovery {
    /// Construct discovery over the shared durable ingestion service.
    #[must_use]
    pub fn new(api: AgentApi, store: WatchdogStore, clock: Arc<dyn Clock>) -> Self {
        Self { api, store, clock }
    }

    /// Reconcile recent unarchived threads and exact native spawn edges.
    ///
    /// The bounded recency window is a bootstrap heuristic only: official
    /// events, MCP, and later process correlation may retain or add sessions
    /// outside it without broad historical discovery.
    pub async fn reconcile(
        &self,
        codex_roots: &[PathBuf],
        codex_path_mappings: &[WorktreePathMapping],
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
                codex_path_mappings,
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
        codex_path_mappings: &[WorktreePathMapping],
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
        for thread in threads {
            self.reconcile_codex_rollout(thread, codex_path_mappings, report)
                .await;
        }
    }

    async fn reconcile_codex_rollout(
        &self,
        thread: &watchdog_codex::CodexThread,
        path_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
    ) {
        let Some((root, relative, path_key)) =
            projected_runtime_file(thread.rollout_path(), path_mappings)
        else {
            report.warn();
            return;
        };
        let Ok(path_key) = BoundedText::new("rollout_path_key", path_key) else {
            report.warn();
            return;
        };
        let reader = IncrementalReader::new(
            ReadBudget::new(
                MAX_CODEX_ROLLOUT_BATCH_BYTES,
                watchdog_codex::MAX_ROLLOUT_RECORD_BYTES,
                MAX_CODEX_ROLLOUT_RECORDS,
            )
            .unwrap_or_else(|_| unreachable!("static Codex rollout budget is valid")),
        );
        let Ok(parser) = watchdog_codex::CodexRolloutParser::new(thread.cli_version()) else {
            report.warn();
            return;
        };
        let Ok(saved) = self.store.file_cursor(&path_key).await else {
            report.warn();
            return;
        };
        let Some(saved) = saved else {
            match reader.cursor_at_end(&root, &relative, CODEX_ROLLOUT_PARSER_VERSION) {
                Ok(cursor) => {
                    if self
                        .persist_codex_cursor(&path_key, &cursor, None)
                        .await
                        .is_err()
                    {
                        report.warn();
                    }
                }
                Err(_) => report.warn(),
            }
            return;
        };
        self.tail_codex_rollout(
            thread, report, &root, &relative, &path_key, reader, &parser, &saved,
        )
        .await;
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the arguments are the explicit bounded source capabilities and durable cursor"
    )]
    async fn tail_codex_rollout(
        &self,
        thread: &watchdog_codex::CodexThread,
        report: &mut RuntimeDiscoveryReport,
        root: &CapabilityRoot,
        relative: &Path,
        path_key: &BoundedText<4_096>,
        reader: IncrementalReader,
        parser: &watchdog_codex::CodexRolloutParser,
        saved: &FileCursorRecord,
    ) {
        let mut cursor = FileCursor::resume_from_complete(
            FileIdentity::new(saved.device_id(), saved.inode()),
            saved.complete_record_offset(),
            CODEX_ROLLOUT_PARSER_VERSION,
        );
        let mut last_observation_id = saved.last_observation_id();
        let mut complete_offset = cursor.complete_offset();
        for _ in 0..MAX_CODEX_ROLLOUT_BATCHES {
            let Ok(outcome) = reader.read(root, relative, &cursor) else {
                report.warn();
                return;
            };
            let ReadOutcome::Records(batch) = outcome else {
                report.warn();
                match reader.cursor_at_end(root, relative, CODEX_ROLLOUT_PARSER_VERSION) {
                    Ok(new_cursor) => {
                        let _ = self
                            .persist_codex_cursor(path_key, &new_cursor, last_observation_id)
                            .await;
                    }
                    Err(_) => report.warn(),
                }
                return;
            };
            for record in batch.records() {
                complete_offset = complete_offset
                    .saturating_add(u64::try_from(record.len()).unwrap_or(u64::MAX))
                    .saturating_add(1);
                let event_key = format!("{}:{complete_offset}", thread.subject().native_id());
                let result = self
                    .ingest_codex_rollout_record(thread, parser, record, &event_key, report)
                    .await;
                let Ok(observation_id) = result else {
                    report.warn();
                    return;
                };
                if let Some(observation_id) = observation_id {
                    last_observation_id = Some(observation_id);
                }
            }
            cursor = batch.cursor().clone();
            if self
                .persist_codex_cursor(path_key, &cursor, last_observation_id)
                .await
                .is_err()
            {
                report.warn();
                return;
            }
            if !batch.continuation_required() {
                return;
            }
        }
    }

    async fn ingest_codex_rollout_record(
        &self,
        thread: &watchdog_codex::CodexThread,
        parser: &watchdog_codex::CodexRolloutParser,
        record: &[u8],
        event_key: &str,
        report: &mut RuntimeDiscoveryReport,
    ) -> Result<Option<ObservationId>, ()> {
        match parser.parse_record(record, Some(thread.subject()), event_key, self.clock.now()) {
            Ok(evidence) => {
                let observation_id = evidence.observation().observation_id();
                self.api
                    .ingest_native_observation(evidence.observation().clone())
                    .await
                    .map_err(|_| ())?;
                Ok(Some(observation_id))
            }
            Err(error) => {
                report.warn();
                Ok(self
                    .emit_codex_compatibility_warning(
                        thread,
                        event_key,
                        error.compatibility_warning(),
                    )
                    .await)
            }
        }
    }

    async fn persist_codex_cursor(
        &self,
        path_key: &BoundedText<4_096>,
        cursor: &FileCursor,
        last_observation_id: Option<ObservationId>,
    ) -> Result<(), ()> {
        let parser_version = BoundedText::new(
            "parser_version",
            format!("codex-rollout-v{}", cursor.parser_version()),
        )
        .map_err(|_| ())?;
        let record = FileCursorRecord::new(
            path_key.clone(),
            cursor.identity().device(),
            cursor.identity().inode(),
            cursor.complete_offset(),
            cursor.complete_offset(),
            parser_version,
            last_observation_id,
        )
        .map_err(|_| ())?;
        self.store.save_file_cursor(&record).await.map_err(|_| ())
    }

    async fn emit_codex_compatibility_warning(
        &self,
        thread: &watchdog_codex::CodexThread,
        event_key: &str,
        warning: watchdog_domain::CompatibilityWarning,
    ) -> Option<ObservationId> {
        let source = ObservationSource::new(
            AdapterIdentity::new(RuntimeKind::CodexCli, thread.cli_version()).ok()?,
            "rollout:compatibility",
            EvidenceTrust::Corroborating,
            None,
        )
        .ok()?;
        let observation_id =
            ObservationId::from_native(RuntimeKind::CodexCli, "rollout-compatibility", event_key)
                .ok()?;
        let observation = ObservationEnvelope::new(
            observation_id,
            thread.subject().clone(),
            self.clock.now(),
            source,
            ObservationPayload::Compatibility(warning),
        )
        .ok()?;
        self.api
            .ingest_native_observation(observation)
            .await
            .ok()
            .map(|_| observation_id)
    }

    async fn reconcile_codex_main(
        &self,
        thread: &watchdog_codex::CodexThread,
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
    ) {
        let main_id = SessionId::from_native(thread.subject());
        let startup_directory = validated_directory(Some(thread.cwd()), worktree_mappings, report);
        let Ok(view) = self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::CodexCli,
                native_id: thread.subject().native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: discovery_key("codex-state", main_id),
                adapter_version: watchdog_codex::TESTED_CODEX_VERSION.to_owned(),
                evidence_source: "codex:state-db".to_owned(),
                title: Some(thread.title().to_owned()),
                startup_directory,
            })
            .await
        else {
            report.warn();
            return;
        };
        if self
            .api
            .enrich_repository_metadata(
                view.session,
                RepositoryMetadata {
                    remote: thread
                        .git_origin_url()
                        .and_then(GitHubEnricher::canonical_remote),
                    branch: thread.git_branch().map(ToOwned::to_owned),
                    ..RepositoryMetadata::default()
                },
            )
            .await
            .is_err()
        {
            report.warn();
        }
        if mains.insert(main_id) {
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
                    adapter_version: watchdog_codex::TESTED_CODEX_VERSION.to_owned(),
                    evidence_source: "codex:state-db".to_owned(),
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
        let Ok(view) = self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::CodexCli,
                native_id: thread.subject().native_id().to_owned(),
                kind: SessionKind::Child,
                parent: Some(parent_id),
                event_key: discovery_key("codex-state", child_id),
                adapter_version: watchdog_codex::TESTED_CODEX_VERSION.to_owned(),
                evidence_source: "codex:state-db".to_owned(),
                title: Some(title),
                startup_directory,
            })
            .await
        else {
            report.warn();
            return;
        };
        if self
            .api
            .enrich_repository_metadata(
                view.session,
                RepositoryMetadata {
                    remote: thread
                        .git_origin_url()
                        .and_then(GitHubEnricher::canonical_remote),
                    branch: thread.git_branch().map(ToOwned::to_owned),
                    ..RepositoryMetadata::default()
                },
            )
            .await
            .is_err()
        {
            report.warn();
        }
        if children.insert(child_id) {
            report.child_sessions = report.child_sessions.saturating_add(1);
        }
    }
}

fn projected_runtime_file(
    native_path: &Path,
    mappings: &[WorktreePathMapping],
) -> Option<(CapabilityRoot, PathBuf, String)> {
    for mapping in mappings {
        let Ok(relative) = native_path.strip_prefix(mapping.native_root()) else {
            continue;
        };
        if mapping.project_native_path(native_path).is_none() {
            continue;
        }
        let Ok(root) = CapabilityRoot::new(mapping.mounted_root()) else {
            continue;
        };
        let path_key = format!("codex:{}", native_path.display());
        return Some((root, relative.to_path_buf(), path_key));
    }
    None
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
