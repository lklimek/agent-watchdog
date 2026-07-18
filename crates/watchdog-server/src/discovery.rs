use std::{
    collections::BTreeSet,
    io::Read as _,
    path::{Path, PathBuf},
};

use watchdog_domain::{RuntimeKind, SessionId, SessionKind};
use watchdog_runtime::{CapabilityRoot, DirectoryScanner, ScanBudget};

use crate::{AgentApi, DiscoveredSession};

const MAX_SCAN_DEPTH: usize = 4;
const MAX_SCAN_ENTRIES: usize = 2_048;
const MAX_SCAN_PATH_BYTES: usize = 2 * 1_024 * 1_024;

/// Bounded best-effort result of one Claude team reconciliation pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClaudeDiscoveryReport {
    main_sessions: u32,
    child_sessions: u32,
    warning_count: u32,
}

impl ClaudeDiscoveryReport {
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
        allowed_worktree_roots: &[PathBuf],
    ) -> ClaudeDiscoveryReport {
        let mut report = ClaudeDiscoveryReport::default();
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
                    allowed_worktree_roots,
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
        allowed_worktree_roots: &[PathBuf],
        report: &mut ClaudeDiscoveryReport,
        mains: &mut BTreeSet<SessionId>,
        children: &mut BTreeSet<SessionId>,
    ) {
        let main_id = SessionId::from_native(team.lead());
        let main_directory = validated_directory(team.lead_cwd(), allowed_worktree_roots, report);
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
            log_reconcile_failure("main", &error);
            report.warn();
            return;
        }
        if mains.insert(main_id) {
            report.main_sessions = report.main_sessions.saturating_add(1);
        }
        for member in team.members() {
            let child_id = SessionId::from_native(member.subject());
            let child_directory = validated_directory(member.cwd(), allowed_worktree_roots, report);
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
                log_reconcile_failure("child", &error);
                report.warn();
            } else if children.insert(child_id) {
                report.child_sessions = report.child_sessions.saturating_add(1);
            }
        }
    }
}

fn log_reconcile_failure(session_kind: &'static str, error: &crate::AgentApiError) {
    tracing::warn!(
        event = "adapter.session_reconcile_failed",
        runtime = RuntimeKind::ClaudeCode.as_str(),
        session_kind,
        error = %error,
        "Runtime-native session could not be reconciled"
    );
}

fn discovery_key(source: &str, session: SessionId) -> String {
    format!("{source}:{session}")
}

fn read_bounded_config(root: &CapabilityRoot, relative: &Path) -> Result<Option<Vec<u8>>, ()> {
    let Ok(mut file) = root.open_file(relative) else {
        return Ok(None);
    };
    let limit = u64::try_from(watchdog_claude::MAX_TEAM_CONFIG_BYTES).map_err(|_| ())?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    if bytes.len() > watchdog_claude::MAX_TEAM_CONFIG_BYTES {
        return Err(());
    }
    Ok(Some(bytes))
}

fn validated_directory(
    candidate: Option<&Path>,
    allowed_roots: &[PathBuf],
    report: &mut ClaudeDiscoveryReport,
) -> Option<String> {
    let candidate = candidate?;
    let Ok(canonical) = candidate.canonicalize() else {
        report.warn();
        return None;
    };
    if !allowed_roots.iter().any(|root| canonical.starts_with(root)) {
        report.warn();
        return None;
    }
    if let Some(path) = canonical.to_str() {
        Some(path.to_owned())
    } else {
        report.warn();
        None
    }
}
