use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    hash::Hash,
    io::{Read as _, Seek as _, SeekFrom},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::UNIX_EPOCH,
};

use thiserror::Error;
#[cfg(target_os = "linux")]
use watchdog_domain::ProcessId;
use watchdog_domain::{
    AdapterIdentity, BoundedText, Clock, DetailedState, DomainInputError, EvidenceTrust,
    NativeSessionKey, ObservationEnvelope, ObservationId, ObservationPayload, ObservationSource,
    RuntimeKind, SessionId, SessionIdentity, SessionKind, TimePoint,
};
use watchdog_runtime::{
    CapabilityRoot, DirectoryScanner, FileCursor, FileIdentity, IncrementalReader, ReadBudget,
    ReadOutcome, ScanBudget, ScanOrder,
};
use watchdog_store::{
    DiscoveryAliasResolution, FileCursorRecord, RecordInputError, StoreError, WatchdogStore,
};

use crate::{AgentApi, DiscoveredSession, GitHubEnricher, RepositoryMetadata, WorktreePathMapping};

const MAX_SCAN_DEPTH: usize = 4;
const MAX_SCAN_ENTRIES: usize = 2_048;
const MAX_SCAN_PATH_BYTES: usize = 2 * 1_024 * 1_024;
const CLAUDE_BOOTSTRAP_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
const CLAUDE_TRANSCRIPT_PARSER_VERSION: u32 = 1;
const MAX_CLAUDE_BOOTSTRAP_BATCHES: usize = 1;
const MAX_CLAUDE_TRANSCRIPT_BATCHES: usize = 4;
const MAX_CLAUDE_TRANSCRIPT_BATCH_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_CLAUDE_TRANSCRIPT_RECORDS: usize = 128;
const MAX_DISCOVERY_ALIAS_CACHE: usize = 2_048;
const MAX_CLAUDE_TRANSCRIPT_ALIAS_CACHE: usize = 2_048;
const MAX_CLAUDE_TRANSCRIPT_VERSION_CACHE: usize = 2_048;
const MAX_CLAUDE_SESSIONS: u32 = 1_000;
const CODEX_BOOTSTRAP_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_CODEX_THREADS: u32 = 1_000;
const CODEX_VERSION_UNKNOWN: &str = "unknown";
const CODEX_ROLLOUT_PARSER_VERSION: u32 = 1;
const MAX_CODEX_ROLLOUT_BATCHES: usize = 4;
const MAX_CODEX_ROLLOUT_BATCH_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_CODEX_ROLLOUT_RECORDS: usize = 128;
const MAX_CODEX_BOOTSTRAP_TAIL_BYTES: usize = 1_024 * 1_024;
const MAX_CODEX_CORRELATION_LOG_CACHE: usize = 2_048;
const MAX_DISCOVERY_WARNING_LOG_SITES: usize = 256;
const MAX_RECONCILE_FAILURES: usize = 2_048;

type DiscoveryWarningSite = (&'static str, u32, u32);
static DISCOVERY_WARNING_LOG_SITES: OnceLock<Mutex<BoundedLru<DiscoveryWarningSite, ()>>> =
    OnceLock::new();
type ReconcileFailureKey = (RuntimeKind, &'static str, SessionId);
static RECONCILE_FAILURES: OnceLock<Mutex<BoundedLru<ReconcileFailureKey, String>>> =
    OnceLock::new();

struct BoundedLru<K, V> {
    entries: HashMap<K, V>,
    recency: VecDeque<K>,
    capacity: usize,
}

impl<K, V> BoundedLru<K, V>
where
    K: Clone + Eq + Hash,
{
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            recency: VecDeque::new(),
            capacity,
        }
    }

    fn get_cloned<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let (owned, value) = self
            .entries
            .get_key_value(key)
            .map(|(owned, value)| (owned.clone(), value.clone()))?;
        touch_recency(&mut self.recency, owned);
        Some(value)
    }

    fn insert(&mut self, key: K, value: V) -> bool {
        self.insert_evicting_where(key, value, |_| true)
    }

    fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (owned, value) = self.entries.remove_entry(key)?;
        if let Some(index) = self
            .recency
            .iter()
            .position(|candidate| candidate == &owned)
        {
            self.recency.remove(index);
        }
        Some(value)
    }

    fn insert_evicting_where(
        &mut self,
        key: K,
        value: V,
        mut may_evict: impl FnMut(&V) -> bool,
    ) -> bool {
        if self.entries.contains_key(&key) {
            self.entries.insert(key.clone(), value);
            touch_recency(&mut self.recency, key);
            return true;
        }
        if self.capacity == 0 {
            return false;
        }
        if self.entries.len() >= self.capacity {
            let Some(index) = self
                .recency
                .iter()
                .position(|candidate| self.entries.get(candidate).is_some_and(&mut may_evict))
            else {
                return false;
            };
            let evicted = self
                .recency
                .remove(index)
                .unwrap_or_else(|| unreachable!("eviction index came from the same queue"));
            self.entries.remove(&evicted);
        }
        self.entries.insert(key.clone(), value);
        touch_recency(&mut self.recency, key);
        true
    }
}

fn touch_recency<K: Eq>(recency: &mut VecDeque<K>, key: K) {
    if let Some(index) = recency.iter().position(|candidate| *candidate == key) {
        recency.remove(index);
    }
    recency.push_back(key);
}

fn discovery_warning_site_is_new(site: DiscoveryWarningSite) -> bool {
    let mut sites = DISCOVERY_WARNING_LOG_SITES
        .get_or_init(|| Mutex::new(BoundedLru::new(MAX_DISCOVERY_WARNING_LOG_SITES)))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if sites.get_cloned(&site).is_some() {
        return false;
    }
    sites.insert(site, ());
    true
}
const COMPANION_LOG_CURSOR_VERSION: u32 = 1;
const COMPANION_BOOTSTRAP_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_GIT_CONFIG_BYTES: usize = 64 * 1_024;

#[derive(Debug, Error)]
enum CursorPersistenceError {
    #[error("Discovery cursor field is invalid: {0}")]
    Domain(#[from] DomainInputError),
    #[error("Discovery cursor record is invalid: {0}")]
    Record(#[from] RecordInputError),
    #[error("Discovery cursor could not be persisted: {0}")]
    Store(#[from] StoreError),
}

/// Bounded best-effort result of one runtime reconciliation pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeDiscoveryReport {
    main_sessions: u32,
    child_sessions: u32,
    warning_count: u32,
}

#[derive(Debug, Error)]
enum CodexBootstrapTailError {
    #[error("Codex rollout bootstrap cursor could not be read")]
    CursorRead(#[source] StoreError),
    #[error("Codex rollout bootstrap tail could not be read")]
    TailRead,
    #[error("Codex rollout bootstrap observation could not be built")]
    ObservationBuild,
    #[error("Codex rollout bootstrap observation could not be ingested")]
    ObservationIngest(#[source] crate::AgentApiError),
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

    fn absorb_scan_failures(&mut self, scanned: &RootScans) {
        for _ in 0..scanned.unavailable_roots {
            self.warn();
        }
        for _ in 0..scanned.failed_scans {
            self.warn();
        }
        for _ in 0..scanned.incomplete_scans {
            self.warn();
        }
    }

    #[track_caller]
    fn warn(&mut self) {
        let caller = std::panic::Location::caller();
        if discovery_warning_site_is_new((caller.file(), caller.line(), caller.column())) {
            tracing::warn!(
                event = "discovery.reconciliation_warning",
                source_file = caller.file(),
                source_line = caller.line(),
                source_column = caller.column(),
                "Runtime discovery evidence could not be reconciled"
            );
        }
        self.warning_count = self.warning_count.saturating_add(1);
    }
}

#[derive(Clone, Copy)]
struct MainParentDiscovery<'a> {
    runtime: RuntimeKind,
    event_key_prefix: &'a str,
    adapter_version: &'a str,
    evidence_source: &'a str,
}

async fn ensure_native_parent(
    api: &AgentApi,
    parent: &NativeSessionKey,
    discovery: MainParentDiscovery<'_>,
    mains: &mut BTreeSet<SessionId>,
    report: &mut RuntimeDiscoveryReport,
) -> Result<SessionId, crate::AgentApiError> {
    let parent_id = SessionId::from_native(parent);
    if mains.contains(&parent_id) {
        return Ok(parent_id);
    }
    if let Some(identity) = api.discovered_session_identity(parent_id).await? {
        if matches!(identity, SessionIdentity::Main(_)) && mains.insert(parent_id) {
            report.main_sessions = report.main_sessions.saturating_add(1);
        }
        return Ok(parent_id);
    }
    // Alias resolution can redirect this registration onto a canonical main, so
    // the parent is the identity discovery returns, not the asserted native key.
    let canonical = api
        .discover_session(DiscoveredSession {
            runtime: discovery.runtime,
            native_id: parent.native_id().to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: discovery_key(discovery.event_key_prefix, parent_id),
            adapter_version: discovery.adapter_version.to_owned(),
            evidence_source: discovery.evidence_source.to_owned(),
            title: None,
            startup_directory: None,
        })
        .await?
        .session
        .session_id();
    if mains.insert(canonical) {
        report.main_sessions = report.main_sessions.saturating_add(1);
    }
    Ok(canonical)
}

/// Claude team reconciliation report.
pub type ClaudeDiscoveryReport = RuntimeDiscoveryReport;

/// Codex Companion reconciliation report.
pub type CompanionDiscoveryReport = RuntimeDiscoveryReport;

/// Native Codex reconciliation report.
pub type CodexDiscoveryReport = RuntimeDiscoveryReport;

#[derive(Clone)]
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

    fn bind_team_alias(&mut self, alias: &ClaudeTeamTranscriptAlias) {
        self.subject = alias.subject.clone();
        self.parent.clone_from(&alias.parent);
        self.kind = if alias.parent.is_some() {
            SessionKind::Child
        } else {
            SessionKind::Main
        };
    }
}

#[derive(Clone)]
struct ClaudeTeamTranscriptAlias {
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    agent_type: Option<String>,
    cwd: PathBuf,
    basis: ClaudeAliasBasis,
}

#[derive(Clone, Copy)]
enum ClaudeAliasBasis {
    TeamMember,
    TeamLead,
    SharedTeamParent,
}

impl ClaudeTeamTranscriptAlias {
    const fn basis(&self) -> &'static str {
        match self.basis {
            ClaudeAliasBasis::TeamMember => "team_member_agent_type_and_cwd",
            ClaudeAliasBasis::TeamLead => "unique_team_lead_cwd",
            ClaudeAliasBasis::SharedTeamParent => "shared_team_parent",
        }
    }

    const fn confidence(&self) -> &'static str {
        match self.basis {
            ClaudeAliasBasis::TeamMember => "high",
            ClaudeAliasBasis::TeamLead | ClaudeAliasBasis::SharedTeamParent => "medium",
        }
    }
}

#[derive(Default)]
struct ClaudeTeamTranscriptAliases(Vec<ClaudeTeamTranscriptAlias>);

impl ClaudeTeamTranscriptAliases {
    fn extend(&mut self, team: &watchdog_claude::ClaudeTeam) {
        if let Some(cwd) = team.lead_cwd() {
            self.0.push(ClaudeTeamTranscriptAlias {
                subject: team.lead().clone(),
                parent: None,
                agent_type: None,
                cwd: cwd.to_path_buf(),
                basis: ClaudeAliasBasis::TeamLead,
            });
        }
        self.0.extend(team.members().iter().filter_map(|member| {
            Some(ClaudeTeamTranscriptAlias {
                subject: member.subject().clone(),
                parent: Some(team.lead().clone()),
                agent_type: Some(member.agent_type()?.to_owned()),
                cwd: member.cwd()?.to_path_buf(),
                basis: ClaudeAliasBasis::TeamMember,
            })
        }));
    }

    fn resolve(&self, bootstrap: &ClaudeTranscriptBootstrap) -> Option<ClaudeTeamTranscriptAlias> {
        let cwd = bootstrap.cwd.as_deref()?;
        if let Some(title) = bootstrap.title.as_deref() {
            let member_candidates = self
                .0
                .iter()
                .filter(|alias| alias.agent_type.as_deref() == Some(title) && alias.cwd == cwd)
                .collect::<Vec<_>>();
            if let [matched] = member_candidates.as_slice() {
                return Some((*matched).clone());
            }
            if let Some(shared_parent) = member_candidates
                .first()
                .and_then(|candidate| candidate.parent.as_ref())
                .filter(|parent| {
                    member_candidates
                        .iter()
                        .all(|candidate| candidate.parent.as_ref() == Some(*parent))
                })
            {
                return Some(ClaudeTeamTranscriptAlias {
                    subject: shared_parent.clone(),
                    parent: None,
                    agent_type: Some(title.to_owned()),
                    cwd: cwd.to_path_buf(),
                    basis: ClaudeAliasBasis::SharedTeamParent,
                });
            }
        }
        let mut lead_candidates = self
            .0
            .iter()
            .filter(|alias| alias.parent.is_none() && alias.cwd == cwd);
        let matched = lead_candidates.next()?;
        lead_candidates.next().is_none().then(|| matched.clone())
    }
}

/// Bounded in-process aliases from wrapper-native sessions to canonical
/// discovered sessions. Claude reconciliation repopulates it before Companion
/// reconciliation on every server start.
#[derive(Clone)]
pub struct DiscoveryAliasRegistry {
    aliases: Arc<Mutex<BoundedLru<NativeSessionKey, Option<SessionId>>>>,
}

impl Default for DiscoveryAliasRegistry {
    fn default() -> Self {
        Self {
            aliases: Arc::new(Mutex::new(BoundedLru::new(MAX_DISCOVERY_ALIAS_CACHE))),
        }
    }
}

impl DiscoveryAliasRegistry {
    #[cfg(test)]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            aliases: Arc::new(Mutex::new(BoundedLru::new(capacity))),
        }
    }

    fn bind(&self, alias: NativeSessionKey, canonical: SessionId) {
        let mut aliases = self
            .aliases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match aliases.get_cloned(&alias) {
            None => {
                aliases.insert_evicting_where(alias, Some(canonical), Option::is_some);
            }
            Some(Some(existing)) if existing != canonical => {
                aliases.insert(alias, None);
            }
            Some(Some(_) | None) => {}
        }
    }

    fn mark_ambiguous(&self, alias: NativeSessionKey) {
        self.aliases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(alias, None);
    }

    fn resolve(&self, alias: &NativeSessionKey) -> Option<SessionId> {
        self.aliases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_cloned(alias)
            .flatten()
    }
}

impl std::fmt::Debug for DiscoveryAliasRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiscoveryAliasRegistry")
            .finish_non_exhaustive()
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
    detected_version: Option<String>,
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
    alias_cache: Arc<Mutex<BoundedLru<String, ClaudeTeamTranscriptAlias>>>,
    version_cache: Arc<Mutex<BoundedLru<String, Option<String>>>>,
    native_aliases: DiscoveryAliasRegistry,
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
        Self::with_alias_registry(api, store, clock, DiscoveryAliasRegistry::default())
    }

    /// Construct discovery with aliases shared with Companion reconciliation.
    #[must_use]
    pub fn with_alias_registry(
        api: AgentApi,
        store: WatchdogStore,
        clock: Arc<dyn Clock>,
        native_aliases: DiscoveryAliasRegistry,
    ) -> Self {
        Self {
            api,
            store,
            clock,
            alias_cache: Arc::new(Mutex::new(BoundedLru::new(
                MAX_CLAUDE_TRANSCRIPT_ALIAS_CACHE,
            ))),
            version_cache: Arc::new(Mutex::new(BoundedLru::new(
                MAX_CLAUDE_TRANSCRIPT_VERSION_CACHE,
            ))),
            native_aliases,
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
        if self.hydrate_native_aliases().await.is_err() {
            report.warn();
        }
        let mut mains = BTreeSet::new();
        let mut children = BTreeSet::new();
        let Ok(budget) = ScanBudget::new(MAX_SCAN_DEPTH, MAX_SCAN_ENTRIES, MAX_SCAN_PATH_BYTES)
        else {
            report.warn();
            return report;
        };
        let scanner = DirectoryScanner::new(budget);
        let configured = claude_roots.to_vec();
        let Some(opened) = off_thread(move || scan_roots(&configured, &scanner)).await else {
            report.warn();
            return report;
        };
        report.absorb_scan_failures(&opened);
        let scans = Arc::new(opened.scans);
        let live_registry_present = self
            .reconcile_live_registries(&scans, worktree_mappings, &mut report, &mut mains)
            .await;
        let now_ms = self.clock.now().wall_time().value();
        let mut aliases = ClaudeTeamTranscriptAliases::default();
        let mut teams = Vec::new();
        let team_scans = Arc::clone(&scans);
        let (configs, config_warnings) = off_thread_or_warn(&mut report, move || {
            collect_team_configs(&team_scans, now_ms)
        })
        .await;
        for _ in 0..config_warnings {
            report.warn();
        }
        for bytes in configs {
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
        self.reconcile_team_tasks(Arc::clone(&scans), teams, &mut report)
            .await;
        let mappings = claude_path_mappings.to_vec();
        let candidate_scans = Arc::clone(&scans);
        let (candidates, candidate_warnings) = off_thread_or_warn(&mut report, move || {
            recent_transcript_candidates(&candidate_scans, &mappings, now_ms)
        })
        .await;
        for _ in 0..candidate_warnings {
            report.warn();
        }
        for (root, candidate) in candidates {
            self.reconcile_transcript(
                &root,
                candidate,
                &aliases,
                worktree_mappings,
                &mut report,
                &mut mains,
                &mut children,
            )
            .await;
        }
        self.complete_absent_live_mains_if_complete(live_registry_present, &mut report)
            .await;
        report
    }

    async fn hydrate_native_aliases(&self) -> Result<(), StoreError> {
        // The batched query resolves every alias with the same exact-session
        // candidates a single lease would; re-leasing per alias would take the
        // process-wide alias mutex once per row for an identical answer.
        for (alias, canonical) in self
            .store
            .discovery_aliases(
                RuntimeKind::ClaudeCode,
                u32::try_from(MAX_DISCOVERY_ALIAS_CACHE).unwrap_or(u32::MAX),
            )
            .await?
        {
            match canonical {
                Some(canonical) => self.native_aliases.bind(alias, canonical),
                None => self.native_aliases.mark_ambiguous(alias),
            }
        }
        Ok(())
    }

    async fn complete_absent_live_mains_if_complete(
        &self,
        present: Option<BTreeSet<SessionId>>,
        report: &mut RuntimeDiscoveryReport,
    ) {
        if let Some(present) = present
            && self
                .complete_absent_live_mains(&present, report)
                .await
                .is_err()
        {
            report.warn();
        }
    }

    async fn reconcile_live_registries(
        &self,
        scans: &[(CapabilityRoot, watchdog_runtime::ScanResult)],
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
    ) -> Option<BTreeSet<SessionId>> {
        let mut present = BTreeSet::new();
        let mut found = false;
        let mut complete = true;
        for (root, scan) in scans {
            if !is_claude_live_registry_root(root) {
                continue;
            }
            found = true;
            if scan.uncertainty().is_some()
                || self
                    .reconcile_live_registry(
                        root,
                        scan,
                        worktree_mappings,
                        report,
                        mains,
                        &mut present,
                    )
                    .await
                    .is_err()
            {
                complete = false;
            }
        }
        (found && complete).then_some(present)
    }

    async fn reconcile_live_registry(
        &self,
        root: &CapabilityRoot,
        scan: &watchdog_runtime::ScanResult,
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        present: &mut BTreeSet<SessionId>,
    ) -> Result<(), ()> {
        for file in scan.files() {
            if file.parent() != Some(root.path())
                || file.extension().and_then(std::ffi::OsStr::to_str) != Some("json")
            {
                continue;
            }
            let Some(stem) = file.file_stem().and_then(std::ffi::OsStr::to_str) else {
                report.warn();
                return Err(());
            };
            let Ok(filename_pid) = stem.parse::<u32>() else {
                report.warn();
                return Err(());
            };
            let Ok(relative) = file.strip_prefix(root.path()) else {
                report.warn();
                return Err(());
            };
            let Some(bytes) = read_live_registry_record(root, relative).await else {
                report.warn();
                return Err(());
            };
            let Ok(live) = watchdog_claude::parse_live_session_record(&bytes) else {
                report.warn();
                return Err(());
            };
            let session_id = SessionId::from_native(live.subject());
            present.insert(session_id);
            if live.pid().value() != filename_pid {
                report.warn();
                return Err(());
            }
            let Some(process) = verified_live_process(live.pid()).await else {
                report.warn();
                return Err(());
            };
            if process.start_time_ticks() != live.process_start_ticks() {
                report.warn();
                return Err(());
            }
            let startup_directory =
                validated_directory(Some(live.cwd()), worktree_mappings, report);
            let view = self
                .api
                .discover_session(DiscoveredSession {
                    runtime: RuntimeKind::ClaudeCode,
                    native_id: live.subject().native_id().to_owned(),
                    kind: SessionKind::Main,
                    parent: None,
                    event_key: format!(
                        "claude-live:{}:{}",
                        live.pid().value(),
                        live.process_start_ticks()
                    ),
                    adapter_version: live.version().to_owned(),
                    evidence_source: "claude:live-session-registry".to_owned(),
                    title: live.title().map(ToOwned::to_owned),
                    startup_directory,
                })
                .await
                .map_err(|error| {
                    log_reconcile_failure(RuntimeKind::ClaudeCode, "main", &error);
                })?;
            self.ingest_live_session_evidence(&live, &process).await?;
            self.api
                .mark_native_reconciled(
                    view.session.session_id(),
                    live.version(),
                    "claude:live-session-registry:present",
                )
                .await
                .map_err(|_| ())?;
            if mains.insert(session_id) {
                report.main_sessions = report.main_sessions.saturating_add(1);
            }
        }
        Ok(())
    }

    async fn ingest_live_session_evidence(
        &self,
        live: &watchdog_claude::ClaudeLiveSession,
        process: &watchdog_domain::ProcessIdentity,
    ) -> Result<(), ()> {
        let source = || {
            ObservationSource::new(
                AdapterIdentity::new(RuntimeKind::ClaudeCode, live.version()).map_err(|_| ())?,
                "live-session-registry",
                EvidenceTrust::Authoritative,
                None,
            )
            .map_err(|_| ())
        };
        let compatibility =
            if minor_version_mismatch(live.version(), watchdog_claude::TESTED_CLAUDE_VERSION) {
                ObservationPayload::Compatibility(
                    watchdog_claude::ClaudeParseError::UnsupportedRecord
                        .compatibility_warning_for_version(live.version()),
                )
            } else {
                ObservationPayload::CompatibilityResolved
            };
        for (suffix, payload) in [
            (
                format!(
                    "{}:process:{}",
                    live.subject().native_id(),
                    live.process_start_ticks()
                ),
                ObservationPayload::ProcessIdentity(process.clone()),
            ),
            (
                format!(
                    "{}:state:{}:{}:{}",
                    live.subject().native_id(),
                    live.process_start_ticks(),
                    live.updated_at_ms(),
                    state_key(live.state())
                ),
                ObservationPayload::NativeState(live.state()),
            ),
            (
                format!(
                    "{}:compatibility:{}:{}",
                    live.subject().native_id(),
                    live.version(),
                    watchdog_claude::TESTED_CLAUDE_VERSION
                ),
                compatibility,
            ),
        ] {
            let observation = ObservationEnvelope::new(
                ObservationId::from_native(RuntimeKind::ClaudeCode, "live-session", suffix)
                    .map_err(|_| ())?,
                live.subject().clone(),
                self.clock.now(),
                source()?,
                payload,
            )
            .map_err(|_| ())?;
            self.api
                .ingest_native_observation(observation)
                .await
                .map_err(|_| ())?;
        }
        Ok(())
    }

    async fn complete_absent_live_mains(
        &self,
        present: &BTreeSet<SessionId>,
        report: &mut RuntimeDiscoveryReport,
    ) -> Result<(), ()> {
        let sessions = self
            .store
            .sessions_by_kind(SessionKind::Main, MAX_CLAUDE_SESSIONS)
            .await
            .map_err(|_| ())?;
        for session in sessions {
            if session.native.runtime() != RuntimeKind::ClaudeCode
                || present.contains(&session.session.session_id())
            {
                continue;
            }
            let Some(snapshot) = self.store.snapshot(session.session).await.map_err(|_| ())? else {
                report.warn();
                continue;
            };
            if matches!(
                snapshot.state(),
                DetailedState::Completed
                    | DetailedState::Failed
                    | DetailedState::Cancelled
                    | DetailedState::Disappeared
            ) {
                continue;
            }
            let process_key = snapshot.reducer_snapshot().and_then(|reducer| {
                reducer.process_identity().map(|process| {
                    format!("{}:{}", process.pid().value(), process.start_time_ticks())
                })
            });
            let event_key = process_key.unwrap_or_else(|| "legacy-absence".to_owned());
            let observation = ObservationEnvelope::new(
                ObservationId::from_native(
                    RuntimeKind::ClaudeCode,
                    "live-session-absent",
                    format!("{}:{event_key}", session.native.native_id()),
                )
                .map_err(|_| ())?,
                session.native,
                self.clock.now(),
                ObservationSource::new(
                    AdapterIdentity::new(
                        RuntimeKind::ClaudeCode,
                        watchdog_claude::TESTED_CLAUDE_VERSION,
                    )
                    .map_err(|_| ())?,
                    "live-session-registry:absent",
                    EvidenceTrust::Authoritative,
                    None,
                )
                .map_err(|_| ())?,
                ObservationPayload::NativeState(DetailedState::Completed),
            )
            .map_err(|_| ())?;
            if self
                .api
                .ingest_native_observation(observation)
                .await
                .is_err()
            {
                report.warn();
            }
        }
        Ok(())
    }

    async fn reconcile_team(
        &self,
        team: &watchdog_claude::ClaudeTeam,
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) {
        let asserted_main = SessionId::from_native(team.lead());
        let main_directory = validated_directory(team.lead_cwd(), worktree_mappings, report);
        // Alias resolution can redirect the lead onto a canonical main, so every
        // member registers against the identity discovery returns.
        let main_id = match self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: team.lead().native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: discovery_key("claude-team", asserted_main),
                adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION.to_owned(),
                evidence_source: "claude:team-config".to_owned(),
                title: None,
                startup_directory: main_directory,
            })
            .await
        {
            Ok(view) => view.session.session_id(),
            Err(error) => {
                log_reconcile_failure(RuntimeKind::ClaudeCode, "main", &error);
                report.warn();
                return;
            }
        };
        if self
            .api
            .mark_native_reconciled(
                main_id,
                watchdog_claude::TESTED_CLAUDE_VERSION,
                "claude:team-config:present",
            )
            .await
            .is_err()
        {
            report.warn();
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
            } else {
                if self
                    .api
                    .mark_native_reconciled(
                        child_id,
                        watchdog_claude::TESTED_CLAUDE_VERSION,
                        "claude:team-config:present",
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
    }

    async fn reconcile_team_tasks(
        &self,
        scans: Arc<Vec<(CapabilityRoot, watchdog_runtime::ScanResult)>>,
        teams: Vec<watchdog_claude::ClaudeTeam>,
        report: &mut RuntimeDiscoveryReport,
    ) {
        let (aggregates, warnings) =
            off_thread_or_warn(report, move || collect_team_task_aggregates(&scans, &teams)).await;
        for _ in 0..warnings {
            report.warn();
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
        let Ok((candidate, bootstrap)) = self
            .prepare_transcript_candidate(root, candidate, aliases)
            .await
        else {
            log_claude_transcript_prepare_failure();
            report.warn();
            return;
        };
        let Ok(parent) = self
            .ensure_transcript_parent(&candidate, mains, report)
            .await
        else {
            log_claude_parent_failure();
            report.warn();
            return;
        };
        let session_id = SessionId::from_native(&candidate.subject);
        let Ok(existing) = self.existing_metadata(session_id).await else {
            tracing::warn!(
                event = "discovery.claude_metadata_failed",
                session_id = %session_id,
                "Claude transcript metadata could not be loaded"
            );
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
            let title_root = root.clone();
            let transcript = candidate.relative.clone();
            off_thread(move || Self::subagent_title(&title_root, &transcript))
                .await
                .flatten()
                .or_else(|| bootstrap.title.clone())
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
        self.persist_transcript_alias(&candidate, session_id, report)
            .await;
        self.enrich_claude_warning(root, &candidate, view.session, &bootstrap, report)
            .await;
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

    async fn persist_transcript_alias(
        &self,
        candidate: &ClaudeTranscriptCandidate,
        canonical: SessionId,
        report: &mut RuntimeDiscoveryReport,
    ) {
        if candidate.transcript_session == candidate.subject
            || candidate.parent.as_ref() == Some(&candidate.transcript_session)
        {
            return;
        }
        if self
            .store
            .save_discovery_alias(
                &candidate.transcript_session,
                canonical,
                self.clock.now().wall_time(),
            )
            .await
            .is_err()
        {
            report.warn();
            return;
        }
        let Ok(lease) = self
            .store
            .lease_discovery_alias(&candidate.transcript_session)
            .await
        else {
            report.warn();
            return;
        };
        match lease.resolution() {
            DiscoveryAliasResolution::Unique(resolved) if resolved == canonical => {
                self.native_aliases
                    .bind(candidate.transcript_session.clone(), canonical);
            }
            DiscoveryAliasResolution::Ambiguous => {
                self.native_aliases
                    .mark_ambiguous(candidate.transcript_session.clone());
            }
            DiscoveryAliasResolution::Absent | DiscoveryAliasResolution::Unique(_) => {
                report.warn();
            }
        }
    }

    async fn enrich_claude_warning(
        &self,
        root: &CapabilityRoot,
        candidate: &ClaudeTranscriptCandidate,
        session: SessionIdentity,
        bootstrap: &ClaudeTranscriptBootstrap,
        report: &mut RuntimeDiscoveryReport,
    ) {
        let warning_needs_version = self
            .warning_needs_detected_version(session)
            .await
            .unwrap_or(false);
        if !bootstrap.drifted && !warning_needs_version {
            return;
        }
        report.warn();
        let error = watchdog_claude::ClaudeParseError::UnsupportedRecord;
        let detected_version = match bootstrap.detected_version.clone() {
            Some(detected) => Some(detected),
            None => self.detect_transcript_version(root, candidate).await,
        };
        let Some(detected_version) = detected_version else {
            if warning_needs_version {
                let _ = self
                    .emit_claude_compatibility_resolution(candidate, "version-unavailable")
                    .await;
            }
            return;
        };
        if !minor_version_mismatch(&detected_version, watchdog_claude::TESTED_CLAUDE_VERSION) {
            let _ = self
                .emit_claude_compatibility_resolution(
                    candidate,
                    &format!("compatible-version:{detected_version}"),
                )
                .await;
            return;
        }
        let warning = error.compatibility_warning_for_version(&detected_version);
        let event_key = format!("detected-version-v2:{detected_version}");
        let _ = self
            .emit_claude_compatibility_warning(candidate, &event_key, warning)
            .await;
    }

    async fn prepare_transcript_candidate(
        &self,
        root: &CapabilityRoot,
        mut candidate: ClaudeTranscriptCandidate,
        aliases: &ClaudeTeamTranscriptAliases,
    ) -> Result<(ClaudeTranscriptCandidate, ClaudeTranscriptBootstrap), ()> {
        let cached_alias = self.cached_alias(&candidate.path_key);
        let cursor = self
            .store
            .file_cursor(&candidate.path_key)
            .await
            .map_err(|_| ())?;
        let path_session = self
            .store
            .session_by_id(SessionId::from_native(&candidate.transcript_session))
            .await
            .map_err(|_| ())?;
        let path_session_requires_reconciliation = if let Some(record) = path_session.as_ref() {
            self.store
                .snapshot(record.session)
                .await
                .map_err(|_| ())?
                .and_then(|snapshot| {
                    snapshot
                        .reducer_snapshot()
                        .map(watchdog_domain::SessionSnapshot::reconciliation_required)
                })
                .unwrap_or(false)
        } else {
            true
        };
        let alias_recheck = candidate.kind == SessionKind::Main
            && cached_alias.is_none()
            && path_session_requires_reconciliation
            && !aliases.0.is_empty();
        let bootstrap = if cursor.is_none() || alias_recheck {
            let bootstrap_root = root.clone();
            let bootstrap_candidate = candidate.clone();
            off_thread(move || Self::bootstrap_transcript(&bootstrap_root, &bootstrap_candidate))
                .await
                .ok_or(())?
        } else {
            ClaudeTranscriptBootstrap::default()
        };
        if candidate.kind == SessionKind::Main {
            if let Some(alias) = cached_alias {
                candidate.bind_team_alias(&alias);
            } else if let Some(canonical) =
                self.native_aliases.resolve(&candidate.transcript_session)
            {
                let record = self
                    .store
                    .session_by_id(canonical)
                    .await
                    .map_err(|_| ())?
                    .ok_or(())?;
                let identity = record.session;
                candidate.subject = record.native;
                candidate.kind = match identity {
                    SessionIdentity::Main(_) => {
                        candidate.parent = None;
                        SessionKind::Main
                    }
                    SessionIdentity::Child(_) => {
                        candidate.parent = Some(
                            self.store
                                .selected_parent(canonical)
                                .await
                                .map_err(|_| ())?
                                .ok_or(())?
                                .native,
                        );
                        SessionKind::Child
                    }
                };
            } else if let Some(alias) = aliases.resolve(&bootstrap) {
                tracing::info!(
                    event = "discovery.correlation_selected",
                    runtime = RuntimeKind::ClaudeCode.as_str(),
                    correlation_basis = alias.basis(),
                    confidence = alias.confidence(),
                    "Selected unique Claude transcript correlation"
                );
                candidate.bind_team_alias(&alias);
                self.cache_alias(&candidate.path_key, alias);
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
        ensure_native_parent(
            &self.api,
            parent_native,
            MainParentDiscovery {
                runtime: RuntimeKind::ClaudeCode,
                event_key_prefix: "claude-transcript-parent",
                adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION,
                evidence_source: "claude:transcript-path",
            },
            mains,
            report,
        )
        .await
        .map(Some)
        .map_err(|_| ())
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
            .get_cloned(path_key.as_str())
    }

    fn cache_alias(&self, path_key: &BoundedText<4_096>, alias: ClaudeTeamTranscriptAlias) {
        let mut cache = self
            .alias_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.insert(path_key.as_str().to_owned(), alias);
    }

    async fn warning_needs_detected_version(&self, session: SessionIdentity) -> Result<bool, ()> {
        let snapshot = self
            .store
            .snapshot(session)
            .await
            .map_err(|_| ())?
            .ok_or(())?;
        Ok(snapshot
            .reducer_snapshot()
            .and_then(watchdog_domain::SessionSnapshot::compatibility_warning)
            .is_some_and(|warning| !warning.message().contains("detected Claude Code ")))
    }

    async fn detect_transcript_version(
        &self,
        root: &CapabilityRoot,
        candidate: &ClaudeTranscriptCandidate,
    ) -> Option<String> {
        let key = candidate.path_key.as_str();
        if let Some(cached) = self
            .version_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_cloned(key)
        {
            return cached;
        }
        let version_root = root.clone();
        let transcript = candidate.relative.clone();
        let detected =
            off_thread(move || Self::scan_transcript_version(&version_root, &transcript)).await?;
        let mut cache = self
            .version_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.insert(key.to_owned(), detected.clone());
        detected
    }

    fn scan_transcript_version(root: &CapabilityRoot, relative: &Path) -> Option<String> {
        let reader = claude_transcript_reader();
        let mut cursor = reader
            .cursor_at_start(root, relative, CLAUDE_TRANSCRIPT_PARSER_VERSION)
            .ok()?;
        for _ in 0..MAX_CLAUDE_BOOTSTRAP_BATCHES {
            let ReadOutcome::Records(batch) = reader.read(root, relative, &cursor).ok()? else {
                return None;
            };
            if let Some(version) = batch
                .records()
                .iter()
                .find_map(|record| watchdog_claude::parse_transcript_version(record))
            {
                return Some(version.as_str().to_owned());
            }
            cursor = batch.cursor().clone();
            if !batch.continuation_required() {
                return None;
            }
        }
        None
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
                if let Some(version) = watchdog_claude::parse_transcript_version(record) {
                    bootstrap.detected_version = Some(version.as_str().to_owned());
                }
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

    fn subagent_title(root: &CapabilityRoot, relative: &Path) -> Option<String> {
        let sidecar = relative.with_extension("meta.json");
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
            let cursor_root = root.clone();
            let transcript = candidate.relative.clone();
            let created = off_thread(move || {
                reader.cursor_at_end(&cursor_root, &transcript, CLAUDE_TRANSCRIPT_PARSER_VERSION)
            })
            .await;
            match created {
                Some(Ok(cursor)) => {
                    if let Err(error) = self
                        .persist_claude_cursor(&candidate.path_key, &cursor, None)
                        .await
                    {
                        tracing::warn!(
                            event = "discovery.claude_cursor_initialize_failed",
                            error = %error,
                            "Claude transcript cursor could not be initialized"
                        );
                        report.warn();
                    }
                }
                Some(Err(_)) | None => report.warn(),
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
            let read_root = root.clone();
            let transcript = candidate.relative.clone();
            let current = cursor.clone();
            let Some(Ok(outcome)) =
                off_thread(move || reader.read(&read_root, &transcript, &current)).await
            else {
                report.warn();
                return;
            };
            let ReadOutcome::Records(batch) = outcome else {
                report.warn();
                let recovery_root = root.clone();
                let transcript = candidate.relative.clone();
                let recovered = off_thread(move || {
                    reader.cursor_at_end(
                        &recovery_root,
                        &transcript,
                        CLAUDE_TRANSCRIPT_PARSER_VERSION,
                    )
                })
                .await;
                match recovered {
                    Some(Ok(new_cursor)) => {
                        if let Err(error) = self
                            .persist_claude_cursor(
                                &candidate.path_key,
                                &new_cursor,
                                last_observation_id,
                            )
                            .await
                        {
                            tracing::warn!(
                                event = "discovery.claude_cursor_recovery_failed",
                                error = %error,
                                "Claude transcript recovery cursor could not be persisted"
                            );
                        }
                    }
                    Some(Err(_)) | None => report.warn(),
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
            if let Err(error) = self
                .persist_claude_cursor(&candidate.path_key, &cursor, last_observation_id)
                .await
            {
                tracing::warn!(
                    event = "discovery.claude_cursor_persist_failed",
                    error = %error,
                    "Claude transcript cursor could not be persisted"
                );
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
                let Some(version) = watchdog_claude::parse_transcript_version(record) else {
                    return Ok((None, true));
                };
                if !minor_version_mismatch(version.as_str(), watchdog_claude::TESTED_CLAUDE_VERSION)
                {
                    return Ok((
                        self.emit_claude_compatibility_resolution(
                            candidate,
                            &format!("compatible-version:{}", version.as_str()),
                        )
                        .await,
                        true,
                    ));
                }
                let warning = error.compatibility_warning_for_version(version.as_str());
                return Ok((
                    self.emit_claude_compatibility_warning(candidate, event_key, warning)
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
        let view = self
            .api
            .ingest_native_observation(observation)
            .await
            .map_err(|_| ())?;
        if view.snapshot.source_conflict() {
            self.emit_claude_source_conflict_resolution(candidate, event_key)
                .await;
        }
        Ok((Some(observation_id), false))
    }

    /// Clear the source-conflict latch once this transcript agrees with its
    /// native identity again.
    ///
    /// Without a reset path the latch is a one-way ratchet, so every session
    /// that ever conflicted would report an uncertain outcome forever. The
    /// returned snapshot already carries the flag, so this costs no extra read
    /// and fires at most once per conflict.
    async fn emit_claude_source_conflict_resolution(
        &self,
        candidate: &ClaudeTranscriptCandidate,
        event_key: &str,
    ) {
        let resolved = ObservationId::from_native(
            RuntimeKind::ClaudeCode,
            "transcript-conflict-resolved",
            event_key,
        )
        .ok()
        .zip(claude_transcript_source("transcript:identity-agreed").ok())
        .and_then(|(observation_id, source)| {
            ObservationEnvelope::new(
                observation_id,
                candidate.subject.clone(),
                self.clock.now(),
                source,
                ObservationPayload::SourceConflictResolved,
            )
            .ok()
        });
        if let Some(observation) = resolved
            && self
                .api
                .ingest_native_observation(observation)
                .await
                .is_err()
        {
            tracing::debug!(
                event = "discovery.conflict_resolution_dropped",
                "Claude transcript identity agreed again but the resolution did not persist"
            );
        }
    }

    async fn persist_claude_cursor(
        &self,
        path_key: &BoundedText<4_096>,
        cursor: &FileCursor,
        last_observation_id: Option<ObservationId>,
    ) -> Result<(), CursorPersistenceError> {
        let record = FileCursorRecord::new(
            path_key.clone(),
            cursor.identity().device(),
            cursor.identity().inode(),
            cursor.complete_offset(),
            cursor.complete_offset(),
            BoundedText::new(
                "parser_version",
                format!("claude-transcript-v{}", cursor.parser_version()),
            )?,
            last_observation_id,
        )?;
        self.store.save_file_cursor(&record).await?;
        Ok(())
    }

    async fn emit_claude_compatibility_warning(
        &self,
        candidate: &ClaudeTranscriptCandidate,
        event_key: &str,
        warning: watchdog_domain::CompatibilityWarning,
    ) -> Option<ObservationId> {
        if !warning.has_detected_version()
            && stored_warning_has_detected_version(&self.store, &candidate.subject).await
        {
            return None;
        }
        self.emit_claude_compatibility_payload(
            candidate,
            event_key,
            ObservationPayload::Compatibility(warning),
        )
        .await
    }

    async fn emit_claude_compatibility_resolution(
        &self,
        candidate: &ClaudeTranscriptCandidate,
        event_key: &str,
    ) -> Option<ObservationId> {
        self.emit_claude_compatibility_payload(
            candidate,
            event_key,
            ObservationPayload::CompatibilityResolved,
        )
        .await
    }

    async fn emit_claude_compatibility_payload(
        &self,
        candidate: &ClaudeTranscriptCandidate,
        event_key: &str,
        payload: ObservationPayload,
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
            payload,
        )
        .ok()?;
        self.api
            .ingest_native_observation(observation)
            .await
            .ok()
            .map(|_| observation_id)
    }
}

async fn stored_warning_has_detected_version(
    store: &WatchdogStore,
    subject: &NativeSessionKey,
) -> bool {
    let session_id = SessionId::from_native(subject);
    let Ok(Some(record)) = store.session_by_id(session_id).await else {
        return false;
    };
    store
        .snapshot(record.session)
        .await
        .ok()
        .flatten()
        .and_then(|snapshot| {
            snapshot
                .reducer_snapshot()
                .and_then(watchdog_domain::SessionSnapshot::compatibility_warning)
                .map(watchdog_domain::CompatibilityWarning::has_detected_version)
        })
        .unwrap_or(false)
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

/// Read one Claude live-session registry record off the async worker threads.
async fn read_live_registry_record(root: &CapabilityRoot, relative: &Path) -> Option<Vec<u8>> {
    let registry_root = root.clone();
    let registry_file = relative.to_path_buf();
    off_thread(move || {
        read_bounded_file(
            &registry_root,
            &registry_file,
            watchdog_claude::MAX_HOOK_BYTES,
        )
    })
    .await?
    .ok()
    .flatten()
}

/// Freshly verify a live-session PID off the async worker threads.
async fn verified_live_process(
    pid: watchdog_domain::ProcessId,
) -> Option<watchdog_domain::ProcessIdentity> {
    off_thread(move || watchdog_process::LinuxProcessSampler::new(1)?.read_identity(pid))
        .await?
        .inspect_err(|error| {
            tracing::warn!(
                event = "claude.live_process_verification_failed",
                pid = pid.value(),
                error = %error,
                "Claude live-session PID could not be freshly verified"
            );
        })
        .ok()
}

/// Read every Companion workspace summary found under the scanned roots.
fn collect_companion_summaries(
    scans: &[(CapabilityRoot, watchdog_runtime::ScanResult)],
) -> (Vec<(CapabilityRoot, PathBuf, Vec<u8>)>, u32) {
    let mut summaries = Vec::new();
    let mut warnings = 0_u32;
    for (root, scan) in scans {
        let candidates =
            std::iter::once(root.path().to_owned()).chain(scan.directories().iter().cloned());
        for directory in candidates {
            let Ok(relative) = directory.strip_prefix(root.path()) else {
                warnings = warnings.saturating_add(1);
                continue;
            };
            let summary_path = relative.join("state.json");
            match read_bounded_file(root, &summary_path, watchdog_companion::MAX_SUMMARY_BYTES) {
                Ok(Some(bytes)) => summaries.push((root.clone(), relative.to_path_buf(), bytes)),
                Ok(None) => {}
                Err(()) => warnings = warnings.saturating_add(1),
            }
        }
    }
    (summaries, warnings)
}

/// Aggregate every team task file into its owning member session.
fn collect_team_task_aggregates(
    scans: &[(CapabilityRoot, watchdog_runtime::ScanResult)],
    teams: &[watchdog_claude::ClaudeTeam],
) -> (BTreeMap<SessionId, ClaudeTaskAggregate>, u32) {
    let mut aggregates = BTreeMap::<SessionId, ClaudeTaskAggregate>::new();
    let mut warnings = 0_u32;
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
                warnings = warnings.saturating_add(1);
                continue;
            }
            let bytes = match read_bounded_file(root, &relative, watchdog_claude::MAX_HOOK_BYTES) {
                Ok(Some(bytes)) => bytes,
                Ok(None) => continue,
                Err(()) => {
                    warnings = warnings.saturating_add(1);
                    continue;
                }
            };
            let Ok(task) = watchdog_claude::parse_task_record(&bytes) else {
                warnings = warnings.saturating_add(1);
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
                warnings = warnings.saturating_add(1);
                continue;
            }
            if !member.is_active()
                && !matches!(
                    task.state(),
                    DetailedState::Completed | DetailedState::Failed | DetailedState::Cancelled
                )
            {
                continue;
            }
            let Some(modified_ns) = task_modified_ns(root, &relative) else {
                warnings = warnings.saturating_add(1);
                continue;
            };
            let session_id = SessionId::from_native(member.subject());
            aggregates
                .entry(session_id)
                .or_insert_with(|| ClaudeTaskAggregate::new(member.subject().clone()))
                .observe(task.state(), modified_ns);
        }
    }
    (aggregates, warnings)
}

/// Read every recent team config found under the scanned Claude roots.
fn collect_team_configs(
    scans: &[(CapabilityRoot, watchdog_runtime::ScanResult)],
    now_ms: i64,
) -> (Vec<Vec<u8>>, u32) {
    let mut configs = Vec::new();
    let mut warnings = 0_u32;
    for (root, scan) in scans {
        let candidates =
            std::iter::once(root.path().to_owned()).chain(scan.directories().iter().cloned());
        for directory in candidates {
            let Ok(relative) = directory.strip_prefix(root.path()) else {
                warnings = warnings.saturating_add(1);
                continue;
            };
            let config = relative.join("config.json");
            match read_bounded_config(root, &config) {
                Ok(Some(bytes)) => {
                    if recent_capability_file(root, &config, now_ms, CLAUDE_BOOTSTRAP_WINDOW_MS) {
                        configs.push(bytes);
                    }
                }
                Ok(None) => {}
                Err(()) => warnings = warnings.saturating_add(1),
            }
        }
    }
    (configs, warnings)
}

/// Select the scanned transcripts modified inside the Claude bootstrap window.
fn recent_transcript_candidates(
    scans: &[(CapabilityRoot, watchdog_runtime::ScanResult)],
    path_mappings: &[WorktreePathMapping],
    now_ms: i64,
) -> (Vec<(CapabilityRoot, ClaudeTranscriptCandidate)>, u32) {
    let mut candidates = Vec::new();
    let mut warnings = 0_u32;
    for (root, scan) in scans {
        for file in scan.files() {
            let Some(candidate) = ClaudeTranscriptCandidate::from_file(root, file, path_mappings)
            else {
                continue;
            };
            let Some(modified) = modified_ms(root, &candidate.relative) else {
                warnings = warnings.saturating_add(1);
                continue;
            };
            if modified >= now_ms.saturating_sub(CLAUDE_BOOTSTRAP_WINDOW_MS) {
                candidates.push((root.clone(), candidate));
            }
        }
    }
    (candidates, warnings)
}

fn is_claude_live_registry_root(root: &CapabilityRoot) -> bool {
    root.path().file_name().and_then(std::ffi::OsStr::to_str) == Some("sessions")
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
    native_aliases: DiscoveryAliasRegistry,
}

impl CompanionDiscovery {
    /// Construct discovery over the shared durable ingestion service.
    #[must_use]
    pub fn new(api: AgentApi, store: WatchdogStore, clock: Arc<dyn Clock>) -> Self {
        Self::with_alias_registry(api, store, clock, DiscoveryAliasRegistry::default())
    }

    /// Construct discovery with aliases shared from Claude reconciliation.
    #[must_use]
    pub fn with_alias_registry(
        api: AgentApi,
        store: WatchdogStore,
        clock: Arc<dyn Clock>,
        native_aliases: DiscoveryAliasRegistry,
    ) -> Self {
        Self {
            api,
            store,
            clock,
            native_aliases,
        }
    }

    /// Scan bounded workspace summaries, tolerate absent/pruned detail files,
    /// and reconcile valid jobs independently from malformed workspaces.
    pub async fn reconcile(
        &self,
        companion_roots: &[PathBuf],
        _worktree_mappings: &[WorktreePathMapping],
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
        let configured = companion_roots.to_vec();
        let Some(opened) = off_thread(move || scan_roots(&configured, &scanner)).await else {
            report.warn();
            return report;
        };
        report.absorb_scan_failures(&opened);
        let (summaries, summary_warnings) = off_thread_or_warn(&mut report, move || {
            collect_companion_summaries(&opened.scans)
        })
        .await;
        for _ in 0..summary_warnings {
            report.warn();
        }
        for (root, relative, bytes) in summaries {
            let Ok(snapshot) = parser.parse_summary(&bytes) else {
                report.warn();
                continue;
            };
            for job in snapshot.jobs() {
                let detail =
                    read_companion_detail(&root, &relative, &parser, job, &mut report).await;
                let Ok(reconciled) = parser.reconcile(Some(job), detail.as_ref()) else {
                    report.warn();
                    continue;
                };
                match self
                    .should_reconcile_companion_job(&root, &relative, reconciled.job())
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(()) => {
                        report.warn();
                        continue;
                    }
                }
                if reconciled.consistency() == watchdog_companion::CompanionConsistency::Conflicted
                {
                    report.warn();
                }
                let reconciled_session = self
                    .reconcile_companion_job(
                        &parser,
                        &reconciled,
                        &mut report,
                        &mut mains,
                        &mut children,
                    )
                    .await;
                if reconciled_session {
                    self.reconcile_companion_log(
                        &root,
                        &relative,
                        &parser,
                        reconciled.job(),
                        &mut report,
                    )
                    .await;
                }
            }
        }
        report
    }

    async fn should_reconcile_companion_job(
        &self,
        root: &CapabilityRoot,
        workspace_relative: &Path,
        job: &watchdog_companion::CompanionJob,
    ) -> Result<bool, ()> {
        if !matches!(
            job.state(),
            DetailedState::Completed
                | DetailedState::Failed
                | DetailedState::Cancelled
                | DetailedState::Disappeared
        ) {
            return Ok(true);
        }
        if self
            .store
            .session_by_id(SessionId::from_native(job.subject()))
            .await
            .map_err(|_| ())?
            .is_some()
        {
            return Ok(true);
        }
        if !safe_native_filename(job.subject().native_id()) {
            return Ok(false);
        }
        let detail = workspace_relative
            .join("jobs")
            .join(format!("{}.json", job.subject().native_id()));
        let recency_root = root.clone();
        let now_ms = self.clock.now().wall_time().value();
        off_thread(move || {
            recent_capability_file(
                &recency_root,
                &detail,
                now_ms,
                COMPANION_BOOTSTRAP_WINDOW_MS,
            )
        })
        .await
        .ok_or(())
    }

    async fn reconcile_companion_job(
        &self,
        parser: &watchdog_companion::CompanionParser,
        reconciled: &watchdog_companion::ReconciledCompanionJob,
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) -> bool {
        let reconciled_job = reconciled.job();
        let Some(parent) = reconciled.parent() else {
            report.warn();
            return false;
        };
        let aliased_parent = self.native_aliases.resolve(parent);
        let parent_id = aliased_parent.unwrap_or_else(|| SessionId::from_native(parent));
        if aliased_parent.is_some() {
            if !matches!(self.store.session_by_id(parent_id).await, Ok(Some(_))) {
                report.warn();
                return false;
            }
        } else {
            let adapter_version =
                format!("companion-{}", watchdog_companion::TESTED_COMPANION_VERSION);
            if let Err(error) = ensure_native_parent(
                &self.api,
                parent,
                MainParentDiscovery {
                    runtime: RuntimeKind::ClaudeCode,
                    event_key_prefix: "companion-parent",
                    adapter_version: &adapter_version,
                    evidence_source: "companion:parent-summary",
                },
                mains,
                report,
            )
            .await
            {
                log_session_reconcile_failure(
                    RuntimeKind::CodexCompanion,
                    "parent",
                    parent_id,
                    &error,
                );
                report.warn();
                return false;
            }
        }
        clear_session_reconcile_failure(RuntimeKind::CodexCompanion, "parent", parent_id);

        let child_id = SessionId::from_native(reconciled.subject());
        // Companion's workspaceRoot identifies where the dispatch was issued,
        // not necessarily where the child performs its work. Treating it as a
        // worktree would grant false filesystem ownership. Exact child paths
        // come from native runtime evidence or explicit MCP registration.
        let startup_directory = None;
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
            log_session_reconcile_failure(RuntimeKind::CodexCompanion, "child", child_id, &error);
            report.warn();
            return false;
        }
        let event_key = companion_event_key(child_id, reconciled);
        let Ok(observation) = parser.observation(reconciled, &event_key, self.clock.now()) else {
            report.warn();
            return false;
        };
        if let Err(error) = self.api.ingest_native_observation(observation).await {
            log_session_reconcile_failure(RuntimeKind::CodexCompanion, "child", child_id, &error);
            report.warn();
            return false;
        }
        clear_session_reconcile_failure(RuntimeKind::CodexCompanion, "child", child_id);
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
        let log_root = root.clone();
        let Some(Ok(current)) = off_thread(move || {
            reader.cursor_at_end(&log_root, &relative, COMPANION_LOG_CURSOR_VERSION)
        })
        .await
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
        let Some(Ok(identity)) =
            off_thread(move || watchdog_process::LinuxProcessSampler::new(1)?.read_identity(pid))
                .await
        else {
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

async fn read_companion_detail(
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
    let detail_root = root.clone();
    let detail = off_thread(move || {
        read_bounded_file(
            &detail_root,
            &detail_path,
            watchdog_companion::MAX_DETAIL_BYTES,
        )
    })
    .await;
    let bytes = match detail {
        Some(Ok(Some(bytes))) => bytes,
        Some(Ok(None)) => return None,
        Some(Err(())) | None => {
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

#[derive(Default)]
struct ClaudeParentIndex {
    candidates: Vec<ClaudeParentCandidate>,
}

struct ClaudeParentCandidate {
    session: SessionId,
    startup_directory: String,
    repository: Option<String>,
}

impl ClaudeParentIndex {
    fn resolve(
        &self,
        rollout: &watchdog_codex::CodexRolloutEvidence,
    ) -> (Option<SessionId>, &'static str, &'static str) {
        if rollout.originator() != Some("Claude Code") {
            return (None, "not_claude_originated", "none");
        }
        let Some(codex_cwd) = rollout.cwd() else {
            return (None, "claude_origin_without_cwd", "low");
        };
        let exact = self
            .candidates
            .iter()
            .filter(|candidate| Path::new(&candidate.startup_directory) == codex_cwd)
            .map(|candidate| candidate.session)
            .collect::<Vec<_>>();
        if let Some(selected) = one_candidate(&exact) {
            return (Some(selected), "claude_origin_and_unique_cwd", "high");
        }
        if exact.len() > 1 {
            return (None, "claude_origin_and_ambiguous_cwd", "low");
        }
        let Some(repository) = rollout
            .repository_url()
            .and_then(GitHubEnricher::canonical_remote)
        else {
            return (None, "claude_origin_without_unique_parent", "low");
        };
        let candidates = self
            .candidates
            .iter()
            .filter(|candidate| candidate.repository.as_deref() == Some(repository.as_str()))
            .map(|candidate| candidate.session)
            .collect::<Vec<_>>();
        if let Some(selected) = one_candidate(&candidates) {
            (
                Some(selected),
                "claude_origin_and_unique_repository",
                "medium",
            )
        } else {
            (None, "claude_origin_without_unique_parent", "low")
        }
    }
}

#[derive(Clone)]
struct CodexCorrelationLogCache {
    outcomes: Arc<Mutex<BoundedLru<SessionId, CodexCorrelationLogOutcome>>>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct CodexCorrelationLogOutcome {
    selected: Option<SessionId>,
    basis: &'static str,
}

impl CodexCorrelationLogCache {
    fn changed(
        &self,
        subject: SessionId,
        selected: Option<SessionId>,
        basis: &'static str,
    ) -> bool {
        let outcome = CodexCorrelationLogOutcome { selected, basis };
        let mut outcomes = self
            .outcomes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let existing = outcomes.get_cloned(&subject);
        if existing == Some(outcome) {
            return false;
        }
        outcomes.insert(subject, outcome);
        true
    }
}

impl Default for CodexCorrelationLogCache {
    fn default() -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(BoundedLru::new(MAX_CODEX_CORRELATION_LOG_CACHE))),
        }
    }
}

/// Automatic bounded Codex thread/spawn-edge discovery from read-only state.
#[derive(Clone)]
pub struct CodexDiscovery {
    api: AgentApi,
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
    correlation_logs: CodexCorrelationLogCache,
}

impl CodexDiscovery {
    /// Construct discovery over the shared durable ingestion service.
    #[must_use]
    pub fn new(api: AgentApi, store: WatchdogStore, clock: Arc<dyn Clock>) -> Self {
        Self {
            api,
            store,
            clock,
            correlation_logs: CodexCorrelationLogCache::default(),
        }
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
        let rollout_sources = self
            .reconcile_rollout_roots(
                codex_roots,
                codex_path_mappings,
                worktree_mappings,
                &mut report,
                &mut mains,
                &mut children,
            )
            .await;
        for configured_root in codex_roots {
            let configured = configured_root.clone();
            let resolved = off_thread(move || codex_state_database(&configured)).await;
            let database_path = match resolved {
                Some(Ok(Some(path))) => path,
                Some(Ok(None)) => continue,
                Some(Err(())) | None => {
                    report.warn();
                    continue;
                }
            };
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
        for source in rollout_sources {
            self.reconcile_codex_rollout_source(
                CodexRolloutTarget {
                    subject: &source.subject,
                    kind: source.kind,
                },
                CODEX_VERSION_UNKNOWN,
                &source.candidate.root,
                &source.candidate.relative,
                &source.candidate.path_key,
                &mut report,
            )
            .await;
        }
        report
    }

    async fn reconcile_rollout_roots(
        &self,
        codex_roots: &[PathBuf],
        codex_path_mappings: &[WorktreePathMapping],
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) -> Vec<CodexBootstrapRollout> {
        let mut sources = Vec::new();
        let parent_index = if let Ok(index) = self.claude_parent_index(worktree_mappings).await {
            index
        } else {
            report.warn();
            ClaudeParentIndex::default()
        };
        let Ok(budget) = ScanBudget::new(MAX_SCAN_DEPTH, MAX_SCAN_ENTRIES, MAX_SCAN_PATH_BYTES)
        else {
            report.warn();
            return sources;
        };
        let scanner = DirectoryScanner::new(budget).with_order(ScanOrder::Descending);
        let configured = codex_roots.to_vec();
        let Some(opened) = off_thread(move || scan_roots(&configured, &scanner)).await else {
            report.warn();
            return sources;
        };
        report.absorb_scan_failures(&opened);
        let mappings = codex_path_mappings.to_vec();
        let now_ms = self.clock.now().wall_time().value();
        let candidates = off_thread_or_warn(report, move || {
            collect_rollout_candidates(&opened.scans, &mappings, now_ms)
        })
        .await;
        for candidate in candidates {
            if let Some((subject, kind)) = self
                .reconcile_rollout_candidate(
                    &candidate,
                    &parent_index,
                    worktree_mappings,
                    report,
                    mains,
                    children,
                )
                .await
            {
                sources.push(CodexBootstrapRollout {
                    candidate,
                    subject,
                    kind,
                });
            }
        }
        sources
    }

    async fn reconcile_rollout_candidate(
        &self,
        candidate: &CodexRolloutCandidate,
        parent_index: &ClaudeParentIndex,
        worktree_mappings: &[WorktreePathMapping],
        report: &mut RuntimeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) -> Option<(NativeSessionKey, SessionKind)> {
        let source = candidate.clone();
        let observed_at = self.clock.now();
        let parsed = off_thread(move || parse_codex_rollout_metadata(&source, observed_at)).await;
        let metadata = match parsed {
            Some(Ok(Some(metadata))) => metadata,
            Some(Ok(None)) => return None,
            Some(Err(())) | None => {
                report.warn();
                return None;
            }
        };
        let Some(mut kind) = metadata.kind() else {
            report.warn();
            return None;
        };
        let mut parent = match metadata.parent() {
            Some(parent) => {
                let parent_id = match ensure_native_parent(
                    &self.api,
                    parent,
                    MainParentDiscovery {
                        runtime: RuntimeKind::CodexCli,
                        event_key_prefix: "codex-rollout-parent",
                        adapter_version: watchdog_codex::TESTED_CODEX_VERSION,
                        evidence_source: "codex:rollout-metadata",
                    },
                    mains,
                    report,
                )
                .await
                {
                    Ok(parent_id) => parent_id,
                    Err(error) => {
                        log_reconcile_failure(RuntimeKind::CodexCli, "rollout_parent", &error);
                        report.warn();
                        return None;
                    }
                };
                Some(parent_id)
            }
            None => None,
        };
        if parent.is_none()
            && kind == SessionKind::Main
            && let Some(inferred_parent) = self.infer_claude_parent(&metadata, parent_index)
        {
            kind = SessionKind::Child;
            parent = Some(inferred_parent);
        }
        let session_id = SessionId::from_native(metadata.subject());
        let startup_directory = validated_directory(metadata.cwd(), worktree_mappings, report);
        let Ok(view) = self
            .api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::CodexCli,
                native_id: metadata.subject().native_id().to_owned(),
                kind,
                parent,
                event_key: discovery_key("codex-rollout", session_id),
                adapter_version: watchdog_codex::TESTED_CODEX_VERSION.to_owned(),
                evidence_source: "codex:rollout-metadata".to_owned(),
                title: metadata.title().map(ToOwned::to_owned),
                startup_directory,
            })
            .await
        else {
            report.warn();
            return None;
        };
        if self
            .enrich_rollout_repository(view.session, &metadata)
            .await
            .is_err()
        {
            report.warn();
        }
        record_rollout_session(kind, session_id, mains, children, report);
        if let Err(error) = self
            .reconcile_codex_bootstrap_tail(candidate, metadata.subject(), kind)
            .await
        {
            tracing::warn!(
                event = "discovery.codex_bootstrap_tail_failed",
                session_kind = ?kind,
                error = ?error,
                "Codex rollout bootstrap evidence could not be reconciled"
            );
            report.warn();
        }
        Some((metadata.subject().clone(), kind))
    }

    async fn reconcile_codex_bootstrap_tail(
        &self,
        candidate: &CodexRolloutCandidate,
        subject: &NativeSessionKey,
        kind: SessionKind,
    ) -> Result<(), CodexBootstrapTailError> {
        if self
            .store
            .file_cursor(&candidate.path_key)
            .await
            .map_err(CodexBootstrapTailError::CursorRead)?
            .is_some()
        {
            return Ok(());
        }
        let source = candidate.clone();
        let tail_subject = subject.clone();
        let observed_at = self.clock.now();
        let Some(evidence) =
            off_thread(move || parse_codex_rollout_tail(&source, &tail_subject, observed_at))
                .await
                .ok_or(CodexBootstrapTailError::TailRead)?
                .map_err(|()| CodexBootstrapTailError::TailRead)?
        else {
            return Ok(());
        };
        self.api
            .ingest_native_observation(
                codex_rollout_observation_for_kind(evidence.observation(), kind)
                    .map_err(|()| CodexBootstrapTailError::ObservationBuild)?,
            )
            .await
            .map(|_| ())
            .map_err(CodexBootstrapTailError::ObservationIngest)
    }

    async fn enrich_rollout_repository(
        &self,
        session: SessionIdentity,
        metadata: &watchdog_codex::CodexRolloutEvidence,
    ) -> Result<(), ()> {
        self.api
            .enrich_repository_metadata(
                session,
                RepositoryMetadata {
                    remote: metadata
                        .repository_url()
                        .and_then(GitHubEnricher::canonical_remote),
                    ..RepositoryMetadata::default()
                },
            )
            .await
            .map_err(|_| ())
    }

    async fn claude_parent_index(
        &self,
        worktree_mappings: &[WorktreePathMapping],
    ) -> Result<ClaudeParentIndex, ()> {
        let mains = self
            .store
            .sessions_by_kind(SessionKind::Main, MAX_CODEX_THREADS)
            .await
            .map_err(|_| ())?;
        let mut candidates = Vec::new();
        for main in mains {
            if main.native.runtime() != RuntimeKind::ClaudeCode {
                continue;
            }
            let Some(snapshot) = self.store.snapshot(main.session).await.map_err(|_| ())? else {
                continue;
            };
            if snapshot
                .reducer_snapshot()
                .is_some_and(watchdog_domain::SessionSnapshot::reconciliation_required)
            {
                continue;
            }
            if matches!(
                snapshot.state(),
                DetailedState::Completed
                    | DetailedState::Failed
                    | DetailedState::Cancelled
                    | DetailedState::Disappeared
            ) {
                continue;
            }
            let Some(metadata) = self
                .store
                .session_metadata(main.session)
                .await
                .map_err(|_| ())?
            else {
                continue;
            };
            let Some(startup_directory) = metadata.startup_directory() else {
                continue;
            };
            let declared_remote = metadata
                .repository_remote()
                .and_then(GitHubEnricher::canonical_remote);
            let repository = if declared_remote.is_some() {
                declared_remote
            } else {
                let native_directory = PathBuf::from(startup_directory);
                let mappings = worktree_mappings.to_vec();
                off_thread(move || repository_for_native_directory(&native_directory, &mappings))
                    .await
                    .flatten()
            };
            candidates.push(ClaudeParentCandidate {
                session: main.session.session_id(),
                startup_directory: startup_directory.to_owned(),
                repository,
            });
        }
        Ok(ClaudeParentIndex { candidates })
    }

    fn infer_claude_parent(
        &self,
        rollout: &watchdog_codex::CodexRolloutEvidence,
        parent_index: &ClaudeParentIndex,
    ) -> Option<SessionId> {
        let (selected, basis, confidence) = parent_index.resolve(rollout);
        if basis == "not_claude_originated" {
            return None;
        }
        let subject = SessionId::from_native(rollout.subject());
        if !self.correlation_logs.changed(subject, selected, basis) {
            return selected;
        }
        if selected.is_some() {
            tracing::info!(
                event = "discovery.correlation_selected",
                runtime = RuntimeKind::CodexCli.as_str(),
                correlation_basis = basis,
                confidence,
                "Selected unique Claude parent for Claude-originated Codex thread"
            );
        } else {
            tracing::warn!(
                event = "discovery.correlation_ambiguous",
                runtime = RuntimeKind::CodexCli.as_str(),
                correlation_basis = basis,
                confidence,
                "Claude-originated Codex thread has no unique Claude parent"
            );
        }
        selected
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
        let native_path = thread.rollout_path().to_path_buf();
        let mappings = path_mappings.to_vec();
        let Some(Some((root, relative, path_key))) =
            off_thread(move || projected_runtime_file(&native_path, &mappings)).await
        else {
            report.warn();
            return;
        };
        let Ok(path_key) = BoundedText::new("rollout_path_key", path_key) else {
            report.warn();
            return;
        };
        self.reconcile_codex_rollout_source(
            CodexRolloutTarget {
                subject: thread.subject(),
                kind: thread.kind(),
            },
            thread.cli_version(),
            &root,
            &relative,
            &path_key,
            report,
        )
        .await;
    }

    async fn reconcile_codex_rollout_source(
        &self,
        target: CodexRolloutTarget<'_>,
        adapter_version: &str,
        root: &CapabilityRoot,
        relative: &Path,
        path_key: &BoundedText<4_096>,
        report: &mut RuntimeDiscoveryReport,
    ) {
        let reader = codex_rollout_reader();
        let Ok(parser) = watchdog_codex::CodexRolloutParser::new(adapter_version) else {
            report.warn();
            return;
        };
        let Ok(saved) = self.store.file_cursor(path_key).await else {
            report.warn();
            return;
        };
        let Some(saved) = saved else {
            let cursor_root = root.clone();
            let rollout = relative.to_path_buf();
            let created = off_thread(move || {
                reader.cursor_at_end(&cursor_root, &rollout, CODEX_ROLLOUT_PARSER_VERSION)
            })
            .await;
            match created {
                Some(Ok(cursor)) => {
                    if let Err(error) = self.persist_codex_cursor(path_key, &cursor, None).await {
                        tracing::warn!(
                            event = "discovery.codex_cursor_initialize_failed",
                            error = %error,
                            "Codex rollout cursor could not be initialized"
                        );
                        report.warn();
                    }
                }
                Some(Err(_)) | None => report.warn(),
            }
            return;
        };
        self.tail_codex_rollout(
            target.subject,
            adapter_version,
            target.kind,
            report,
            root,
            relative,
            path_key,
            reader,
            &parser,
            &saved,
        )
        .await;
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the arguments are the explicit bounded source capabilities and durable cursor"
    )]
    async fn tail_codex_rollout(
        &self,
        subject: &NativeSessionKey,
        adapter_version: &str,
        kind: SessionKind,
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
            let read_root = root.clone();
            let rollout = relative.to_path_buf();
            let current = cursor.clone();
            let Some(Ok(outcome)) =
                off_thread(move || reader.read(&read_root, &rollout, &current)).await
            else {
                report.warn();
                return;
            };
            let ReadOutcome::Records(batch) = outcome else {
                report.warn();
                let recovery_root = root.clone();
                let rollout = relative.to_path_buf();
                let recovered = off_thread(move || {
                    reader.cursor_at_end(&recovery_root, &rollout, CODEX_ROLLOUT_PARSER_VERSION)
                })
                .await;
                match recovered {
                    Some(Ok(new_cursor)) => {
                        if let Err(error) = self
                            .persist_codex_cursor(path_key, &new_cursor, last_observation_id)
                            .await
                        {
                            tracing::warn!(
                                event = "discovery.codex_cursor_recovery_failed",
                                error = %error,
                                "Codex rollout recovery cursor could not be persisted"
                            );
                        }
                    }
                    Some(Err(_)) | None => report.warn(),
                }
                return;
            };
            for record in batch.records() {
                complete_offset = complete_offset
                    .saturating_add(u64::try_from(record.len()).unwrap_or(u64::MAX))
                    .saturating_add(1);
                let event_key = format!("{}:{complete_offset}", subject.native_id());
                let result = self
                    .ingest_codex_rollout_record(
                        CodexRolloutTarget { subject, kind },
                        adapter_version,
                        parser,
                        record,
                        &event_key,
                        report,
                    )
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
            if let Err(error) = self
                .persist_codex_cursor(path_key, &cursor, last_observation_id)
                .await
            {
                tracing::warn!(
                    event = "discovery.codex_cursor_persist_failed",
                    error = %error,
                    "Codex rollout cursor could not be persisted"
                );
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
        target: CodexRolloutTarget<'_>,
        adapter_version: &str,
        parser: &watchdog_codex::CodexRolloutParser,
        record: &[u8],
        event_key: &str,
        report: &mut RuntimeDiscoveryReport,
    ) -> Result<Option<ObservationId>, ()> {
        match parser.parse_record(record, Some(target.subject), event_key, self.clock.now()) {
            Ok(evidence) => {
                let observation =
                    codex_rollout_observation_for_kind(evidence.observation(), target.kind)?;
                let observation_id = observation.observation_id();
                self.api
                    .ingest_native_observation(observation)
                    .await
                    .map_err(|_| ())?;
                Ok(Some(observation_id))
            }
            Err(error) => {
                report.warn();
                if semver_compatibility_line(adapter_version).is_none() {
                    return Ok(None);
                }
                if !minor_version_mismatch(adapter_version, watchdog_codex::TESTED_CODEX_VERSION) {
                    return Ok(self
                        .emit_codex_compatibility_resolution(
                            target.subject,
                            adapter_version,
                            event_key,
                        )
                        .await);
                }
                Ok(self
                    .emit_codex_compatibility_warning(
                        target.subject,
                        adapter_version,
                        event_key,
                        error.compatibility_warning_for_version(adapter_version),
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
    ) -> Result<(), CursorPersistenceError> {
        let parser_version = BoundedText::new(
            "parser_version",
            format!("codex-rollout-v{}", cursor.parser_version()),
        )?;
        let record = FileCursorRecord::new(
            path_key.clone(),
            cursor.identity().device(),
            cursor.identity().inode(),
            cursor.complete_offset(),
            cursor.complete_offset(),
            parser_version,
            last_observation_id,
        )?;
        self.store.save_file_cursor(&record).await?;
        Ok(())
    }

    async fn emit_codex_compatibility_warning(
        &self,
        subject: &NativeSessionKey,
        adapter_version: &str,
        event_key: &str,
        warning: watchdog_domain::CompatibilityWarning,
    ) -> Option<ObservationId> {
        if !warning.has_detected_version()
            && stored_warning_has_detected_version(&self.store, subject).await
        {
            return None;
        }
        self.emit_codex_compatibility_payload(
            subject,
            adapter_version,
            event_key,
            ObservationPayload::Compatibility(warning),
        )
        .await
    }

    async fn emit_codex_compatibility_resolution(
        &self,
        subject: &NativeSessionKey,
        adapter_version: &str,
        event_key: &str,
    ) -> Option<ObservationId> {
        self.emit_codex_compatibility_payload(
            subject,
            adapter_version,
            event_key,
            ObservationPayload::CompatibilityResolved,
        )
        .await
    }

    async fn emit_codex_compatibility_payload(
        &self,
        subject: &NativeSessionKey,
        adapter_version: &str,
        event_key: &str,
        payload: ObservationPayload,
    ) -> Option<ObservationId> {
        let source = ObservationSource::new(
            AdapterIdentity::new(RuntimeKind::CodexCli, adapter_version).ok()?,
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
            subject.clone(),
            self.clock.now(),
            source,
            payload,
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
        if self
            .store
            .session_by_id(main_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|record| record.session.kind() == SessionKind::Child)
        {
            return;
        }
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
        let parent_id = match ensure_native_parent(
            &self.api,
            parent,
            MainParentDiscovery {
                runtime: RuntimeKind::CodexCli,
                event_key_prefix: "codex-state-parent",
                adapter_version: watchdog_codex::TESTED_CODEX_VERSION,
                evidence_source: "codex:state-db",
            },
            mains,
            report,
        )
        .await
        {
            Ok(parent_id) => parent_id,
            Err(error) => {
                log_reconcile_failure(RuntimeKind::CodexCli, "parent", &error);
                report.warn();
                return;
            }
        };
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

/// Resolve the Codex state database inside a configured root without escaping it.
fn codex_state_database(configured_root: &Path) -> Result<Option<PathBuf>, ()> {
    let root = CapabilityRoot::new(configured_root).map_err(|_| ())?;
    let database_relative = Path::new("state_5.sqlite");
    let database_path = root.path().join(database_relative);
    match database_path.symlink_metadata() {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(()),
    }
    root.open_file(database_relative).map_err(|_| ())?;
    let database_path = database_path.canonicalize().map_err(|_| ())?;
    if !database_path.starts_with(root.path()) {
        return Err(());
    }
    Ok(Some(database_path))
}

/// Select the scanned rollout files modified inside the Codex bootstrap window.
fn collect_rollout_candidates(
    scans: &[(CapabilityRoot, watchdog_runtime::ScanResult)],
    path_mappings: &[WorktreePathMapping],
    now_ms: i64,
) -> Vec<CodexRolloutCandidate> {
    scans
        .iter()
        .flat_map(|(root, scan)| {
            scan.files().iter().filter_map(move |file| {
                CodexRolloutCandidate::from_file(root, file, path_mappings, now_ms)
            })
        })
        .collect()
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

#[derive(Clone)]
struct CodexRolloutCandidate {
    root: CapabilityRoot,
    relative: PathBuf,
    path_key: BoundedText<4_096>,
}

struct CodexBootstrapRollout {
    candidate: CodexRolloutCandidate,
    subject: NativeSessionKey,
    kind: SessionKind,
}

#[derive(Clone, Copy)]
struct CodexRolloutTarget<'a> {
    subject: &'a NativeSessionKey,
    kind: SessionKind,
}

impl CodexRolloutCandidate {
    fn from_file(
        root: &CapabilityRoot,
        file: &Path,
        mappings: &[WorktreePathMapping],
        now_ms: i64,
    ) -> Option<Self> {
        let relative = file.strip_prefix(root.path()).ok()?.to_path_buf();
        if relative.extension().and_then(std::ffi::OsStr::to_str) != Some("jsonl")
            || !relative
                .file_name()
                .and_then(std::ffi::OsStr::to_str)?
                .starts_with("rollout-")
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
            || !recent_capability_file(root, &relative, now_ms, CODEX_BOOTSTRAP_WINDOW_MS)
        {
            return None;
        }
        let mapping = mappings
            .iter()
            .find(|mapping| mapping.mounted_root() == root.path())?;
        let native_path = mapping.native_root().join(&relative);
        let native_path = native_path.to_str()?;
        Some(Self {
            root: root.clone(),
            relative,
            path_key: BoundedText::new("rollout_path_key", format!("codex:{native_path}")).ok()?,
        })
    }
}

fn parse_codex_rollout_metadata(
    candidate: &CodexRolloutCandidate,
    observed_at: TimePoint,
) -> Result<Option<watchdog_codex::CodexRolloutEvidence>, ()> {
    let reader = codex_rollout_bootstrap_reader();
    let cursor = reader
        .cursor_at_start(
            &candidate.root,
            &candidate.relative,
            CODEX_ROLLOUT_PARSER_VERSION,
        )
        .map_err(|_| ())?;
    let parser = watchdog_codex::CodexRolloutParser::new(watchdog_codex::TESTED_CODEX_VERSION)
        .map_err(|_| ())?;
    let ReadOutcome::Records(batch) = reader
        .read(&candidate.root, &candidate.relative, &cursor)
        .map_err(|_| ())?
    else {
        return Err(());
    };
    let Some(record) = batch.records().first() else {
        return Ok(None);
    };
    match parser.parse_record(record, None, candidate.path_key.as_str(), observed_at) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(watchdog_codex::CodexParseError::MissingSubject) => Ok(None),
        Err(_) => Err(()),
    }
}

fn parse_codex_rollout_tail(
    candidate: &CodexRolloutCandidate,
    subject: &NativeSessionKey,
    observed_at: TimePoint,
) -> Result<Option<watchdog_codex::CodexRolloutEvidence>, ()> {
    let mut file = candidate
        .root
        .open_file(&candidate.relative)
        .map_err(|_| ())?;
    let length = file.metadata().map_err(|_| ())?.len();
    let start = length.saturating_sub(MAX_CODEX_BOOTSTRAP_TAIL_BYTES as u64);
    file.seek(SeekFrom::Start(start)).map_err(|_| ())?;
    let mut bytes = Vec::new();
    file.take((MAX_CODEX_BOOTSTRAP_TAIL_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    if bytes.len() > MAX_CODEX_BOOTSTRAP_TAIL_BYTES {
        return Err(());
    }
    let complete = if let Some(complete) = bytes.strip_suffix(b"\n") {
        complete
    } else if let Some(boundary) = bytes.iter().rposition(|byte| *byte == b'\n') {
        &bytes[..boundary]
    } else {
        return Ok(None);
    };
    let complete = if start == 0 {
        complete
    } else if let Some(offset) = complete.iter().position(|byte| *byte == b'\n') {
        &complete[offset + 1..]
    } else {
        return Ok(None);
    };
    let parser = watchdog_codex::CodexRolloutParser::new(watchdog_codex::TESTED_CODEX_VERSION)
        .map_err(|_| ())?;
    for (index, record) in complete
        .split(|byte| *byte == b'\n')
        .rev()
        .filter(|record| !record.is_empty())
        .take(MAX_CODEX_ROLLOUT_RECORDS)
        .enumerate()
    {
        if record.len() > watchdog_codex::MAX_ROLLOUT_RECORD_BYTES {
            continue;
        }
        let event_key = format!(
            "{}:bootstrap-tail:{length}:{index}",
            candidate.path_key.as_str()
        );
        let Ok(evidence) = parser.parse_record(record, Some(subject), &event_key, observed_at)
        else {
            continue;
        };
        if matches!(
            evidence.observation().payload(),
            ObservationPayload::NativeState(DetailedState::Running | DetailedState::Completed)
        ) {
            return Ok(Some(evidence));
        }
    }
    Ok(None)
}

fn codex_rollout_observation_for_kind(
    observation: &ObservationEnvelope,
    kind: SessionKind,
) -> Result<ObservationEnvelope, ()> {
    let payload = if kind == SessionKind::Main
        && matches!(
            observation.payload(),
            ObservationPayload::NativeState(DetailedState::Completed)
        ) {
        ObservationPayload::NativeState(DetailedState::WaitingForUser)
    } else {
        observation.payload().clone()
    };
    ObservationEnvelope::new(
        observation.observation_id(),
        observation.subject().clone(),
        observation.observed_at(),
        observation.source().clone(),
        payload,
    )
    .map_err(|_| ())
}

fn minor_version_mismatch(detected: &str, tested: &str) -> bool {
    match (
        semver_compatibility_line(detected),
        semver_compatibility_line(tested),
    ) {
        (Some(detected), Some(tested)) => detected != tested,
        _ => false,
    }
}

fn semver_compatibility_line(version: &str) -> Option<(u64, u64)> {
    let version = version.strip_prefix('v').unwrap_or(version);
    let version = version.split_once('+').map_or(version, |(core, _)| core);
    let version = version.split_once('-').map_or(version, |(core, _)| core);
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor))
}

/// Wall-clock modification time of a bounded capability file, in milliseconds.
fn modified_ms(root: &CapabilityRoot, relative: &Path) -> Option<i64> {
    let file = root.open_file(relative).ok()?;
    let modified = file.metadata().ok()?.modified().ok()?;
    i64::try_from(modified.duration_since(UNIX_EPOCH).ok()?.as_millis()).ok()
}

fn recent_capability_file(
    root: &CapabilityRoot,
    relative: &Path,
    now_ms: i64,
    window_ms: i64,
) -> bool {
    modified_ms(root, relative).is_some_and(|modified| modified >= now_ms.saturating_sub(window_ms))
}

fn one_candidate(candidates: &[SessionId]) -> Option<SessionId> {
    (candidates.len() == 1).then(|| candidates[0])
}

fn record_rollout_session(
    kind: SessionKind,
    session_id: SessionId,
    mains: &mut BTreeSet<SessionId>,
    children: &mut BTreeSet<SessionId>,
    report: &mut RuntimeDiscoveryReport,
) {
    match kind {
        SessionKind::Main if mains.insert(session_id) => {
            report.main_sessions = report.main_sessions.saturating_add(1);
        }
        SessionKind::Child if children.insert(session_id) => {
            report.child_sessions = report.child_sessions.saturating_add(1);
        }
        SessionKind::Main | SessionKind::Child => {}
    }
}

fn repository_for_native_directory(
    native_directory: &Path,
    mappings: &[WorktreePathMapping],
) -> Option<String> {
    let (_, mounted_directory) = mappings
        .iter()
        .find_map(|mapping| mapping.project_native_directory(native_directory))?;
    let root = CapabilityRoot::new(mounted_directory).ok()?;
    let mut config = root.open_file(Path::new(".git/config")).ok()?;
    if config.metadata().ok()?.len() > MAX_GIT_CONFIG_BYTES as u64 {
        return None;
    }
    let mut bytes = Vec::new();
    config
        .by_ref()
        .take((MAX_GIT_CONFIG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() > MAX_GIT_CONFIG_BYTES {
        return None;
    }
    let text = std::str::from_utf8(&bytes).ok()?;
    let mut origin = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            origin = line.eq_ignore_ascii_case("[remote \"origin\"]");
            continue;
        }
        if !origin {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if key.trim().eq_ignore_ascii_case("url") {
            return GitHubEnricher::canonical_remote(value.trim());
        }
    }
    None
}

fn codex_rollout_bootstrap_reader() -> IncrementalReader {
    IncrementalReader::new(
        ReadBudget::new(
            MAX_CODEX_ROLLOUT_BATCH_BYTES,
            watchdog_codex::MAX_ROLLOUT_RECORD_BYTES,
            1,
        )
        .unwrap_or_else(|_| unreachable!("static Codex bootstrap budget is valid")),
    )
}

fn codex_rollout_reader() -> IncrementalReader {
    IncrementalReader::new(
        ReadBudget::new(
            MAX_CODEX_ROLLOUT_BATCH_BYTES,
            watchdog_codex::MAX_ROLLOUT_RECORD_BYTES,
            MAX_CODEX_ROLLOUT_RECORDS,
        )
        .unwrap_or_else(|_| unreachable!("static Codex rollout budget is valid")),
    )
}

impl std::fmt::Debug for CodexDiscovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodexDiscovery")
            .finish_non_exhaustive()
    }
}

fn companion_event_key(
    session: SessionId,
    reconciled: &watchdog_companion::ReconciledCompanionJob,
) -> String {
    let job = reconciled.job();
    format!(
        "companion-state:{session}:{}:{}:{}:{}",
        job.updated_at().unwrap_or("no-native-time"),
        state_key(job.state()),
        companion_source_key(reconciled.source()),
        companion_consistency_key(reconciled.consistency()),
    )
}

const fn companion_source_key(source: watchdog_companion::CompanionSource) -> &'static str {
    match source {
        watchdog_companion::CompanionSource::Summary => "summary",
        watchdog_companion::CompanionSource::Detail => "detail",
        watchdog_companion::CompanionSource::Both => "both",
    }
}

const fn companion_consistency_key(
    consistency: watchdog_companion::CompanionConsistency,
) -> &'static str {
    match consistency {
        watchdog_companion::CompanionConsistency::SingleSource => "single",
        watchdog_companion::CompanionConsistency::Consistent => "consistent",
        watchdog_companion::CompanionConsistency::Conflicted => "conflicted",
    }
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

fn log_claude_transcript_prepare_failure() {
    tracing::warn!(
        event = "discovery.claude_transcript_prepare_failed",
        "Claude transcript candidate could not be prepared"
    );
}

fn log_claude_parent_failure() {
    tracing::warn!(
        event = "discovery.claude_parent_failed",
        "Claude transcript parent could not be reconciled"
    );
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

fn log_session_reconcile_failure(
    runtime: RuntimeKind,
    session_kind: &'static str,
    session_id: SessionId,
    error: &crate::AgentApiError,
) {
    let key = (runtime, session_kind, session_id);
    let error = error.to_string();
    let mut failures = RECONCILE_FAILURES
        .get_or_init(|| Mutex::new(BoundedLru::new(MAX_RECONCILE_FAILURES)))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if failures.get_cloned(&key).as_deref() == Some(error.as_str()) {
        return;
    }
    failures.insert(key, error.clone());
    drop(failures);
    tracing::warn!(
        event = "adapter.session_reconcile_failed",
        runtime = runtime.as_str(),
        session_kind,
        session_id = %session_id,
        error,
        "Runtime-native session could not be reconciled"
    );
}

fn clear_session_reconcile_failure(
    runtime: RuntimeKind,
    session_kind: &'static str,
    session_id: SessionId,
) {
    let key = (runtime, session_kind, session_id);
    let mut failures = RECONCILE_FAILURES
        .get_or_init(|| Mutex::new(BoundedLru::new(MAX_RECONCILE_FAILURES)))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if failures.remove(&key).is_none() {
        return;
    }
    drop(failures);
    tracing::info!(
        event = "adapter.session_reconcile_recovered",
        runtime = runtime.as_str(),
        session_kind,
        session_id = %session_id,
        "Runtime-native session reconciliation recovered"
    );
}

fn discovery_key(source: &str, session: SessionId) -> String {
    format!("{source}:{session}")
}

/// Run bounded blocking filesystem work off the async worker threads.
///
/// Returns `None` when the blocking task panicked, which callers surface as a
/// discovery warning instead of unwinding the shared discovery worker.
async fn off_thread<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    match tokio::task::spawn_blocking(work).await {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::error!(
                event = "discovery.blocking_work_failed",
                error = %error,
                "Discovery filesystem work did not complete"
            );
            None
        }
    }
}

/// Run blocking discovery work, counting a panicked task as a warning.
async fn off_thread_or_warn<T: Default + Send + 'static>(
    report: &mut RuntimeDiscoveryReport,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    let Some(value) = off_thread(work).await else {
        report.warn();
        return T::default();
    };
    value
}

/// Scanned roots and the failure counts their caller still has to report.
struct RootScans {
    scans: Vec<(CapabilityRoot, watchdog_runtime::ScanResult)>,
    unavailable_roots: u32,
    failed_scans: u32,
    incomplete_scans: u32,
}

/// Open and scan every configured root without following symlinks.
fn scan_roots(configured: &[PathBuf], scanner: &DirectoryScanner) -> RootScans {
    let mut opened = RootScans {
        scans: Vec::new(),
        unavailable_roots: 0,
        failed_scans: 0,
        incomplete_scans: 0,
    };
    for configured_root in configured {
        let Ok(root) = CapabilityRoot::new(configured_root) else {
            opened.unavailable_roots = opened.unavailable_roots.saturating_add(1);
            continue;
        };
        let Ok(scan) = scanner.scan(&root, Path::new(".")) else {
            opened.failed_scans = opened.failed_scans.saturating_add(1);
            continue;
        };
        if scan.uncertainty().is_some() {
            opened.incomplete_scans = opened.incomplete_scans.saturating_add(1);
        }
        opened.scans.push((root, scan));
    }
    opened
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
                .map(|path| (mapping.native_root().components().count(), path))
        })
        .max_by_key(|(specificity, _)| *specificity)
        .map(|(_, path)| path)
        .or_else(|| {
            report.warn();
            None
        })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, os::unix::fs::symlink, path::Path, sync::Arc};

    use watchdog_domain::{
        CompatibilityWarning, NativeSessionKey, RuntimeKind, SessionId, SessionKind, TimePoint,
        WallTimeMs, WarningKind,
    };
    use watchdog_store::WatchdogStore;
    use watchdog_testkit::FakeClock;

    use super::{
        AgentApi, BoundedLru, CODEX_VERSION_UNKNOWN, CodexBootstrapTailError,
        CodexCorrelationLogCache, CodexDiscovery, DiscoveredSession, DiscoveryAliasRegistry,
        MAX_CODEX_CORRELATION_LOG_CACHE, MainParentDiscovery, RuntimeDiscoveryReport,
        WorktreePathMapping, companion_event_key, ensure_native_parent, minor_version_mismatch,
        off_thread,
    };

    /// Discovery filesystem work must not hold an async worker: the gate is
    /// released only by a runtime timer, which cannot fire while one is blocked.
    #[tokio::test]
    async fn blocking_discovery_work_leaves_the_async_executor_free() {
        let (release, gate) = std::sync::mpsc::channel();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let _ = release.send(());
        });

        let released =
            off_thread(move || gate.recv_timeout(std::time::Duration::from_secs(2)).is_ok()).await;

        assert_eq!(released, Some(true));
    }

    #[tokio::test]
    async fn a_panicking_blocking_task_degrades_instead_of_unwinding_discovery() {
        let outcome = off_thread(|| -> bool { panic!("blocking discovery failure") }).await;

        assert_eq!(outcome, None);
    }

    fn session(runtime: RuntimeKind, native_id: &str) -> SessionId {
        SessionId::from_native(
            &NativeSessionKey::new(runtime, native_id).expect("valid native session key"),
        )
    }

    #[test]
    fn codex_correlation_logging_only_repeats_when_the_outcome_changes() {
        let cache = CodexCorrelationLogCache::default();
        let subject = session(RuntimeKind::CodexCli, "codex-child");
        let parent = session(RuntimeKind::ClaudeCode, "claude-parent");

        assert!(cache.changed(subject, None, "claude_origin_without_unique_parent"));
        assert!(!cache.changed(subject, None, "claude_origin_without_unique_parent"));
        assert!(cache.changed(subject, Some(parent), "claude_origin_and_unique_cwd"));
        assert!(!cache.changed(subject, Some(parent), "claude_origin_and_unique_cwd"));
    }

    #[test]
    fn codex_bootstrap_failures_keep_actionable_stage_context() {
        assert_eq!(
            CodexBootstrapTailError::TailRead.to_string(),
            "Codex rollout bootstrap tail could not be read"
        );
        assert_eq!(
            CodexBootstrapTailError::ObservationBuild.to_string(),
            "Codex rollout bootstrap observation could not be built"
        );
    }

    #[test]
    fn codex_correlation_pressure_preserves_existing_suppression_state() {
        let cache = CodexCorrelationLogCache::default();
        let retained = session(RuntimeKind::CodexCli, "retained-codex-child");
        assert!(cache.changed(retained, None, "no_unique_parent"));
        for index in 1..MAX_CODEX_CORRELATION_LOG_CACHE {
            assert!(cache.changed(
                session(RuntimeKind::CodexCli, &format!("codex-child-{index}")),
                None,
                "no_unique_parent",
            ));
        }
        assert!(!cache.changed(retained, None, "no_unique_parent"));
        assert!(cache.changed(
            session(RuntimeKind::CodexCli, "overflow-codex-child"),
            None,
            "no_unique_parent",
        ));

        assert!(
            !cache.changed(retained, None, "no_unique_parent"),
            "one new subject must not erase every prior one-shot outcome"
        );
    }

    #[test]
    fn bounded_cache_evicts_the_least_recently_used_entry() {
        let mut cache = BoundedLru::new(2);
        cache.insert("hot", 1);
        cache.insert("cold", 2);
        assert_eq!(cache.get_cloned(&"hot"), Some(1));

        cache.insert("new", 3);

        assert_eq!(cache.get_cloned(&"hot"), Some(1));
        assert_eq!(cache.get_cloned(&"cold"), None);
        assert_eq!(cache.get_cloned(&"new"), Some(3));
    }

    #[test]
    fn repeated_discovery_warning_site_logs_once_per_process() {
        let site = ("discovery-test.rs", u32::MAX, u32::MAX);
        assert!(super::discovery_warning_site_is_new(site));
        assert!(!super::discovery_warning_site_is_new(site));
    }

    #[tokio::test]
    async fn native_parent_discovery_reuses_an_existing_child_role() {
        let fixture = tempfile::tempdir().expect("fixture root should exist");
        let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(
            WallTimeMs::new(1_000),
            1_000,
        )));
        let api = AgentApi::new(store, clock)
            .await
            .expect("API should initialize");
        let root = api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: "nested-root".to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "nested-root".to_owned(),
                adapter_version: "test".to_owned(),
                evidence_source: "test:root".to_owned(),
                title: None,
                startup_directory: None,
            })
            .await
            .expect("root should be discovered");
        let parent_native =
            NativeSessionKey::new(RuntimeKind::CodexCli, "nested-parent").expect("valid parent");
        let parent = api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::CodexCli,
                native_id: parent_native.native_id().to_owned(),
                kind: SessionKind::Child,
                parent: Some(root.session.session_id()),
                event_key: "nested-parent".to_owned(),
                adapter_version: "test".to_owned(),
                evidence_source: "test:parent".to_owned(),
                title: None,
                startup_directory: None,
            })
            .await
            .expect("nested parent should be discovered");
        let mut mains = BTreeSet::new();
        let mut report = RuntimeDiscoveryReport::default();

        let resolved = ensure_native_parent(
            &api,
            &parent_native,
            MainParentDiscovery {
                runtime: RuntimeKind::CodexCli,
                event_key_prefix: "nested-parent-fallback",
                adapter_version: "test",
                evidence_source: "test:fallback",
            },
            &mut mains,
            &mut report,
        )
        .await
        .expect("an existing child may own native descendants");

        assert_eq!(resolved, parent.session.session_id());
        assert_eq!(report.main_sessions(), 0);
    }

    #[test]
    fn companion_observation_identity_changes_when_evidence_source_changes() {
        let parser = watchdog_companion::CompanionParser::new("1.0.6")
            .expect("parser version should be supported");
        let summary = parser
            .parse_summary(
                br#"{"version":1,"jobs":[{"id":"job","workspaceRoot":"/work","sessionId":"root","status":"running","phase":"running","pid":42,"updatedAt":"same"}]}"#,
            )
            .expect("summary should parse");
        let detail = parser
            .parse_detail(
                br#"{"id":"job","workspaceRoot":"/work","sessionId":"root","status":"running","phase":"running","pid":42,"updatedAt":"same"}"#,
            )
            .expect("detail should parse");
        let summary_only = parser
            .reconcile(Some(&summary.jobs()[0]), None)
            .expect("summary should reconcile");
        let both = parser
            .reconcile(Some(&summary.jobs()[0]), Some(&detail))
            .expect("matching sources should reconcile");
        let subject = SessionId::from_native(summary_only.subject());

        assert_ne!(
            companion_event_key(subject, &summary_only),
            companion_event_key(subject, &both),
            "idempotency identity must include the evidence source and consistency"
        );
    }

    #[test]
    fn conflicting_native_alias_remains_ambiguous_across_repeated_scans() {
        let aliases = DiscoveryAliasRegistry::default();
        let wrapper =
            NativeSessionKey::new(RuntimeKind::ClaudeCode, "wrapper").expect("valid wrapper");
        let first = session(RuntimeKind::ClaudeCode, "first");
        let second = session(RuntimeKind::ClaudeCode, "second");

        aliases.bind(wrapper.clone(), first);
        aliases.bind(wrapper.clone(), second);
        assert_eq!(aliases.resolve(&wrapper), None);

        aliases.bind(wrapper.clone(), first);
        assert_eq!(aliases.resolve(&wrapper), None);
    }

    #[test]
    fn conflicting_native_alias_remains_ambiguous_after_cache_pressure() {
        let aliases = DiscoveryAliasRegistry::with_capacity(2);
        let wrapper =
            NativeSessionKey::new(RuntimeKind::ClaudeCode, "ambiguous-wrapper").expect("valid key");
        aliases.bind(wrapper.clone(), session(RuntimeKind::ClaudeCode, "first"));
        aliases.bind(wrapper.clone(), session(RuntimeKind::ClaudeCode, "second"));
        aliases.bind(
            NativeSessionKey::new(RuntimeKind::ClaudeCode, "wrapper-1").expect("valid key"),
            session(RuntimeKind::ClaudeCode, "canonical-1"),
        );

        aliases.bind(
            NativeSessionKey::new(RuntimeKind::ClaudeCode, "overflow-wrapper").expect("valid key"),
            session(RuntimeKind::ClaudeCode, "overflow-canonical"),
        );

        assert_eq!(aliases.resolve(&wrapper), None);
        aliases.bind(wrapper.clone(), session(RuntimeKind::ClaudeCode, "first"));
        assert_eq!(aliases.resolve(&wrapper), None);
    }

    #[test]
    fn upgrade_badge_requires_a_major_or_minor_semver_mismatch() {
        assert!(!minor_version_mismatch("2.1.212", "2.1.214"));
        assert!(!minor_version_mismatch("v2.1.0-beta.1", "2.1.214"));
        assert!(!minor_version_mismatch("2.1.214+build.7", "2.1.214"));
        assert!(minor_version_mismatch("2.2.0", "2.1.214"));
        assert!(minor_version_mismatch("3.1.0", "2.1.214"));
        assert!(!minor_version_mismatch("unknown", "2.1.214"));
    }

    #[tokio::test]
    async fn codex_emitter_rejects_a_versionless_warning_downgrade() {
        let fixture = tempfile::tempdir().expect("fixture root");
        let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(
            WallTimeMs::new(1_000),
            1_000,
        )));
        let api = AgentApi::new(store.clone(), clock.clone())
            .await
            .expect("API should initialize");
        let view = api
            .discover_session(DiscoveredSession {
                runtime: RuntimeKind::CodexCli,
                native_id: "codex-rich-warning".to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "codex-rich-warning-discovery".to_owned(),
                adapter_version: "0.999.0".to_owned(),
                evidence_source: "test:codex-rich-warning".to_owned(),
                title: None,
                startup_directory: None,
            })
            .await
            .expect("session should be discovered");
        let subject = NativeSessionKey::new(RuntimeKind::CodexCli, "codex-rich-warning")
            .expect("subject should be valid");
        let discovery = CodexDiscovery::new(api, store.clone(), clock);
        let rich = CompatibilityWarning::new_with_detected_version(
            WarningKind::Upgrade,
            "detected Codex CLI 0.999.0, tested with Codex CLI 0.144.5",
            "0.999.0",
        )
        .expect("rich warning should be valid");
        assert!(
            discovery
                .emit_codex_compatibility_warning(&subject, "0.999.0", "rich", rich)
                .await
                .is_some()
        );
        let versionless = CompatibilityWarning::new(
            WarningKind::Upgrade,
            "Update Agent Watchdog's Codex adapter",
        )
        .expect("versionless warning should be valid");

        assert!(
            discovery
                .emit_codex_compatibility_warning(
                    &subject,
                    CODEX_VERSION_UNKNOWN,
                    "versionless",
                    versionless,
                )
                .await
                .is_none()
        );
        let snapshot = store
            .snapshot(view.session)
            .await
            .expect("snapshot should query")
            .expect("snapshot should exist");
        assert_eq!(
            snapshot
                .reducer_snapshot()
                .expect("reducer snapshot should exist")
                .compatibility_warning()
                .and_then(CompatibilityWarning::detected_version),
            Some("0.999.0")
        );
    }

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
