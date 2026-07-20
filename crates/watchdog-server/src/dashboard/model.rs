use std::{collections::BTreeMap, fmt, path::Path, sync::Arc};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast;
use watchdog_domain::{
    Clock, CompactState, DetailedState, MainSessionId, RuntimeKind, SessionKind,
};
use watchdog_store::{AdapterHealthStatus, StoreError, WatchdogStore};

const MAX_MAIN_SESSIONS: u32 = 1_000;
const MAX_TREE_SESSIONS: u32 = 1_000;
const LIVE_CAPACITY: usize = 128;

/// Dashboard session scope selected by the operator.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardScope {
    /// Exclude terminal main sessions.
    #[default]
    Active,
    /// Include retained terminal main sessions.
    All,
}

/// Dashboard ordering selected by the operator.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardSort {
    /// Waiting/stalled, then idle, then other sessions.
    #[default]
    Attention,
    /// Case-insensitive startup-directory order.
    Directory,
}

/// Read-only dashboard query.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct DashboardQuery {
    /// Active-only or all retained main sessions.
    #[serde(default)]
    pub scope: DashboardScope,
    /// Attention-first or directory order.
    #[serde(default)]
    pub sort: DashboardSort,
}

/// Actionable compatibility/component warning distinct from normalized state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DashboardWarning {
    /// Single-word actionable badge.
    pub badge: String,
    /// Human-readable bounded action or degradation detail.
    pub message: String,
    /// Affected runtime, when known.
    pub runtime: Option<RuntimeKind>,
}

/// One non-expandable operator-facing main-session card.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DashboardCard {
    /// Stable main-session ID.
    pub session_id: MainSessionId,
    /// Native runtime namespace.
    pub runtime: RuntimeKind,
    /// Native title or startup-directory basename fallback.
    pub title: String,
    /// Full startup directory, never deliberately truncated.
    pub startup_directory: String,
    /// Current branch, when detected.
    pub branch: Option<String>,
    /// GitHub pull request number, when enriched.
    pub pull_request_number: Option<u64>,
    /// Validated GitHub pull request URL, when enriched.
    pub pull_request_url: Option<String>,
    /// Main session's own normalized state.
    pub state: DetailedState,
    /// Main session's own compact state.
    pub compact_state: CompactState,
    /// Whether this card belongs in the default active-session projection.
    pub in_active_scope: bool,
    /// Latest trustworthy activity wall time.
    pub last_activity_ms: i64,
    /// Child counts by compact status; children never become top-level cards.
    pub child_counts: BTreeMap<CompactState, u32>,
    /// Compatibility warning rendered as a separate badge.
    pub warning: Option<DashboardWarning>,
}

/// Revisioned read-only browser snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DashboardSnapshot {
    /// Highest committed domain event ID.
    pub revision: u64,
    /// Server wall time used for relative ages.
    pub generated_at_ms: i64,
    /// Applied response scope.
    pub scope: DashboardScope,
    /// Applied deterministic order.
    pub sort: DashboardSort,
    /// Main-session cards only.
    pub sessions: Vec<DashboardCard>,
    /// Page-level adapter health warnings.
    pub warnings: Vec<DashboardWarning>,
}

/// Shared read model and best-effort live snapshot broadcaster.
#[derive(Clone)]
pub struct DashboardService {
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
    live: broadcast::Sender<Arc<DashboardSnapshot>>,
}

impl fmt::Debug for DashboardService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DashboardService")
            .field("live_receivers", &self.live.receiver_count())
            .finish_non_exhaustive()
    }
}

impl DashboardService {
    /// Construct the dashboard read model over the durable store.
    #[must_use]
    pub fn new(store: WatchdogStore, clock: Arc<dyn Clock>) -> Self {
        let (live, _) = broadcast::channel(LIVE_CAPACITY);
        Self { store, clock, live }
    }

    /// Build one bounded snapshot from current durable projections.
    ///
    /// # Errors
    ///
    /// Returns [`DashboardError`] for corrupt/missing store projections.
    pub async fn snapshot(
        &self,
        query: DashboardQuery,
    ) -> Result<DashboardSnapshot, DashboardError> {
        let mains = self
            .store
            .sessions_by_kind(SessionKind::Main, MAX_MAIN_SESSIONS)
            .await?;
        let mut sessions = Vec::with_capacity(mains.len());
        for main in mains {
            let snapshot = self
                .store
                .snapshot(main.session)
                .await?
                .ok_or(DashboardError::MissingSnapshot)?;
            let reducer = snapshot.reducer_snapshot();
            let in_active_scope = !terminal(snapshot.state())
                && !reducer.is_some_and(watchdog_domain::SessionSnapshot::reconciliation_required);
            if query.scope == DashboardScope::Active && !in_active_scope {
                continue;
            }
            let metadata = self.store.session_metadata(main.session).await?;
            let startup_directory = metadata
                .as_ref()
                .and_then(watchdog_store::SessionMetadataRecord::startup_directory)
                .unwrap_or("(unknown startup directory)")
                .to_owned();
            let title = metadata
                .as_ref()
                .and_then(watchdog_store::SessionMetadataRecord::title)
                .map_or_else(
                    || fallback_title(&startup_directory, main.native.native_id()),
                    ToOwned::to_owned,
                );
            let warning = reducer
                .and_then(|snapshot| snapshot.compatibility_warning())
                .map(|warning| DashboardWarning {
                    badge: warning.badge().to_owned(),
                    message: warning.message().to_owned(),
                    runtime: Some(main.native.runtime()),
                });
            let last_activity_ms = reducer.map_or_else(
                || snapshot.updated_at().value(),
                |snapshot| snapshot.last_activity().wall_time().value(),
            );
            let children = self
                .store
                .sessions_for_root(main.root, MAX_TREE_SESSIONS)
                .await?;
            let mut child_counts = BTreeMap::new();
            for child in children
                .into_iter()
                .filter(|record| record.session.kind() == SessionKind::Child)
            {
                let child_snapshot = self
                    .store
                    .snapshot(child.session)
                    .await?
                    .ok_or(DashboardError::MissingSnapshot)?;
                *child_counts
                    .entry(child_snapshot.state().compact())
                    .or_default() += 1;
            }
            sessions.push(DashboardCard {
                session_id: main.root,
                runtime: main.native.runtime(),
                title,
                startup_directory,
                branch: metadata
                    .as_ref()
                    .and_then(watchdog_store::SessionMetadataRecord::branch)
                    .map(ToOwned::to_owned),
                pull_request_number: metadata
                    .as_ref()
                    .and_then(watchdog_store::SessionMetadataRecord::pull_request_number),
                pull_request_url: metadata
                    .as_ref()
                    .and_then(watchdog_store::SessionMetadataRecord::pull_request_url)
                    .filter(|url| valid_pull_request_url(url))
                    .map(ToOwned::to_owned),
                state: snapshot.state(),
                compact_state: snapshot.state().compact(),
                in_active_scope,
                last_activity_ms,
                child_counts,
                warning,
            });
        }
        sort_cards(&mut sessions, query.sort);
        Ok(DashboardSnapshot {
            revision: self.store.latest_event_id().await?.value(),
            generated_at_ms: self.clock.now().wall_time().value(),
            scope: query.scope,
            sort: query.sort,
            sessions,
            warnings: self.adapter_warnings().await?,
        })
    }

    /// Broadcast a fresh all-session snapshot to connected SSE consumers.
    ///
    /// A missing receiver is not an error; durable state remains authoritative.
    ///
    /// # Errors
    ///
    /// Returns [`DashboardError`] if the durable snapshot cannot be built.
    pub async fn publish(&self) -> Result<Arc<DashboardSnapshot>, DashboardError> {
        let snapshot = Arc::new(
            self.snapshot(DashboardQuery {
                scope: DashboardScope::All,
                sort: DashboardSort::Attention,
            })
            .await?,
        );
        let _ = self.live.send(snapshot.clone());
        Ok(snapshot)
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<Arc<DashboardSnapshot>> {
        self.live.subscribe()
    }

    async fn adapter_warnings(&self) -> Result<Vec<DashboardWarning>, StoreError> {
        let mut warnings = Vec::new();
        for runtime in [
            RuntimeKind::ClaudeCode,
            RuntimeKind::CodexCli,
            RuntimeKind::CodexCompanion,
        ] {
            let Some(health) = self.store.adapter_health(runtime).await? else {
                continue;
            };
            if health.status == AdapterHealthStatus::Healthy {
                continue;
            }
            warnings.push(DashboardWarning {
                badge: if health.status == AdapterHealthStatus::Degraded {
                    "DEGRADED"
                } else {
                    "FAILED"
                }
                .to_owned(),
                message: health.message.map_or_else(
                    || {
                        "Runtime monitoring is incomplete; check Watchdog health and logs"
                            .to_owned()
                    },
                    |message| message.as_str().to_owned(),
                ),
                runtime: Some(runtime),
            });
        }
        Ok(warnings)
    }
}

/// Dashboard projection failure.
#[derive(Debug, Error)]
pub enum DashboardError {
    /// Durable store read failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A retained session lacks its required current snapshot.
    #[error("Dashboard session snapshot is missing")]
    MissingSnapshot,
    /// Snapshot serialization failed.
    #[error("Dashboard snapshot serialization failed")]
    Serialization(#[from] serde_json::Error),
}

fn terminal(state: DetailedState) -> bool {
    matches!(
        state,
        DetailedState::Completed
            | DetailedState::Failed
            | DetailedState::Cancelled
            | DetailedState::Disappeared
    )
}

fn fallback_title(directory: &str, native_id: &str) -> String {
    Path::new(directory)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(native_id)
        .to_owned()
}

fn valid_pull_request_url(url: &str) -> bool {
    url.strip_prefix("https://github.com/")
        .is_some_and(|path| !path.is_empty() && !path.contains(['?', '#']))
}

fn sort_cards(cards: &mut [DashboardCard], sort: DashboardSort) {
    cards.sort_by(|left, right| {
        let directory = || {
            left.startup_directory
                .to_lowercase()
                .cmp(&right.startup_directory.to_lowercase())
                .then_with(|| left.session_id.cmp(&right.session_id))
        };
        match sort {
            DashboardSort::Attention => attention_rank(left.compact_state)
                .cmp(&attention_rank(right.compact_state))
                .then_with(directory),
            DashboardSort::Directory => directory(),
        }
    });
}

const fn attention_rank(state: CompactState) -> u8 {
    match state {
        CompactState::Waiting | CompactState::Stalled => 0,
        CompactState::Idle => 1,
        CompactState::Active
        | CompactState::Finished
        | CompactState::Failed
        | CompactState::Unknown => 2,
    }
}
