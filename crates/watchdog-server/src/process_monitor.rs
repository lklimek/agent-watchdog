use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use thiserror::Error;
use watchdog_domain::{
    AdapterIdentity, BoundedText, Clock, EvidenceTrust, ObservationEnvelope, ObservationId,
    ObservationPayload, ObservationSource, ProcessId, ProcessIdentity, SessionId, SessionKind,
};
use watchdog_process::{
    ActivityStrength, LinuxProcessSampler, ProcessActivity, ProcessTreeSnapshot,
};
use watchdog_store::{StoreError, WatchdogStore};

use crate::{AgentApi, AgentApiError};

const MAX_SESSIONS_PER_KIND: u32 = 1_000;
const PROCESS_ADAPTER_VERSION: &str = "linux-procfs-v1";

trait ProcessTreeSampler: std::fmt::Debug + Send + Sync {
    fn sample_trees(
        &self,
        roots: &[ProcessIdentity],
    ) -> Result<BTreeMap<ProcessId, Result<ProcessTreeSnapshot, ()>>, ()>;
}

impl ProcessTreeSampler for LinuxProcessSampler {
    fn sample_trees(
        &self,
        roots: &[ProcessIdentity],
    ) -> Result<BTreeMap<ProcessId, Result<ProcessTreeSnapshot, ()>>, ()> {
        LinuxProcessSampler::sample_trees(self, roots)
            .map(|trees| {
                trees
                    .into_iter()
                    .map(|(pid, tree)| (pid, tree.map_err(|_| ())))
                    .collect()
            })
            .map_err(|_| ())
    }
}

/// One bounded process-monitor pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProcessMonitorReport {
    monitored_sessions: u32,
    progressed_sessions: u32,
    warning_count: u32,
}

impl ProcessMonitorReport {
    pub(crate) const fn monitored_sessions(self) -> u32 {
        self.monitored_sessions
    }

    pub(crate) const fn progressed_sessions(self) -> u32 {
        self.progressed_sessions
    }

    pub(crate) const fn warning_count(self) -> u32 {
        self.warning_count
    }
}

/// Periodic Linux process-tree delta monitor for verified session identities.
#[derive(Clone)]
pub(crate) struct ProcessMonitor {
    api: AgentApi,
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
    sampler: Arc<dyn ProcessTreeSampler>,
    previous: Arc<Mutex<HashMap<SessionId, ProcessTreeSnapshot>>>,
}

impl std::fmt::Debug for ProcessMonitor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessMonitor")
            .finish_non_exhaustive()
    }
}

impl ProcessMonitor {
    pub(crate) fn new(
        api: AgentApi,
        store: WatchdogStore,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProcessMonitorError> {
        let sampler = LinuxProcessSampler::new(32_768).map_err(|_| ProcessMonitorError::Sampler)?;
        Ok(Self::with_sampler(api, store, clock, Arc::new(sampler)))
    }

    fn with_sampler(
        api: AgentApi,
        store: WatchdogStore,
        clock: Arc<dyn Clock>,
        sampler: Arc<dyn ProcessTreeSampler>,
    ) -> Self {
        Self {
            api,
            store,
            clock,
            sampler,
            previous: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn reconcile(&self) -> Result<ProcessMonitorReport, ProcessMonitorError> {
        let mut sessions = self
            .store
            .sessions_by_kind(SessionKind::Main, MAX_SESSIONS_PER_KIND)
            .await?;
        sessions.extend(
            self.store
                .sessions_by_kind(SessionKind::Child, MAX_SESSIONS_PER_KIND)
                .await?,
        );
        let mut report = ProcessMonitorReport::default();
        let mut monitored = Vec::new();
        for session in sessions {
            let Some(snapshot) = self.store.snapshot(session.session).await? else {
                report.warning_count = report.warning_count.saturating_add(1);
                continue;
            };
            let Some(reducer) = snapshot.reducer_snapshot() else {
                report.warning_count = report.warning_count.saturating_add(1);
                continue;
            };
            let Some(identity) = reducer.process_identity() else {
                continue;
            };
            report.monitored_sessions = report.monitored_sessions.saturating_add(1);
            monitored.push((session, identity.clone()));
        }
        let roots = monitored
            .iter()
            .map(|(_, identity)| identity.clone())
            .collect::<Vec<_>>();
        if roots.is_empty() {
            return Ok(report);
        }
        let trees = self
            .sampler
            .sample_trees(&roots)
            .map_err(|()| ProcessMonitorError::Sampler)?;
        for (session, identity) in monitored {
            let Some(Ok(current)) = trees.get(&identity.pid()) else {
                self.remove_previous(session.session.session_id());
                report.warning_count = report.warning_count.saturating_add(1);
                continue;
            };
            let activity = self.replace_previous(session.session.session_id(), current.clone());
            let Some(activity) = activity else {
                continue;
            };
            if !activity.uncertainties().is_empty() {
                report.warning_count = report.warning_count.saturating_add(1);
                continue;
            }
            let Some(summary) = activity_summary(&activity) else {
                continue;
            };
            let observation =
                process_observation(&session.native, self.clock.now(), &activity, summary)?;
            if self
                .api
                .ingest_native_observation(observation)
                .await
                .is_err()
            {
                report.warning_count = report.warning_count.saturating_add(1);
            } else {
                report.progressed_sessions = report.progressed_sessions.saturating_add(1);
            }
        }
        Ok(report)
    }

    fn replace_previous(
        &self,
        session: SessionId,
        current: ProcessTreeSnapshot,
    ) -> Option<ProcessActivity> {
        let mut previous = self
            .previous
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let activity = previous
            .get(&session)
            .filter(|before| before.root() == current.root())
            .map(|before| current.activity_since(before));
        previous.insert(session, current);
        activity
    }

    fn remove_previous(&self, session: SessionId) {
        self.previous
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&session);
    }
}

fn activity_summary(activity: &ProcessActivity) -> Option<&'static str> {
    match activity.strength() {
        ActivityStrength::StrongCpu => {
            Some("All four CPU times grew versus the previous process snapshot")
        }
        ActivityStrength::Activity => {
            Some("Correlated process CPU, I/O, or child-process activity advanced")
        }
        ActivityStrength::Neutral => None,
    }
}

fn process_observation(
    native: &watchdog_domain::NativeSessionKey,
    observed_at: watchdog_domain::TimePoint,
    activity: &ProcessActivity,
    summary: &'static str,
) -> Result<ObservationEnvelope, ProcessMonitorError> {
    let cpu = activity.cpu();
    let event_key = format!(
        "{}:{}:{}:{}:{}:{}:{}:{}",
        SessionId::from_native(native),
        observed_at.wall_time().value(),
        observed_at.monotonic_ms(),
        cpu.user_ticks(),
        cpu.system_ticks(),
        cpu.children_user_ticks(),
        cpu.children_system_ticks(),
        activity.new_processes(),
    );
    let source = ObservationSource::new(
        AdapterIdentity::new(native.runtime(), PROCESS_ADAPTER_VERSION)?,
        "process:tree-delta",
        EvidenceTrust::Corroborating,
        None,
    )?;
    Ok(ObservationEnvelope::new(
        ObservationId::from_native(native.runtime(), "process-tree", event_key)?,
        native.clone(),
        observed_at,
        source,
        ObservationPayload::Progress(BoundedText::new("progress", summary)?),
    )?)
}

/// Process-monitor initialization or reconciliation failure.
#[derive(Debug, Error)]
pub(crate) enum ProcessMonitorError {
    #[error("Linux process sampler initialization failed")]
    Sampler,
    #[error(transparent)]
    Domain(#[from] watchdog_domain::DomainInputError),
    #[error(transparent)]
    Observation(#[from] watchdog_domain::ObservationError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    AgentApi(#[from] AgentApiError),
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use watchdog_domain::{
        NativeSessionKey, ProcessId, RuntimeKind, SessionKind, TimePoint, WallTimeMs,
    };
    use watchdog_process::{
        CommandFingerprint, CpuCounters, IoCounters, ProcessSample, ProcessState,
    };
    use watchdog_testkit::FakeClock;

    use super::*;
    use crate::DiscoveredSession;

    #[derive(Debug)]
    struct FakeSampler {
        snapshots: Mutex<VecDeque<ProcessTreeSnapshot>>,
    }

    impl ProcessTreeSampler for FakeSampler {
        fn sample_trees(
            &self,
            roots: &[ProcessIdentity],
        ) -> Result<BTreeMap<ProcessId, Result<ProcessTreeSnapshot, ()>>, ()> {
            let snapshot = self
                .snapshots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .ok_or(())?;
            assert_eq!(roots, [snapshot.root().clone()]);
            Ok(BTreeMap::from([(snapshot.root().pid(), Ok(snapshot))]))
        }
    }

    #[tokio::test]
    async fn all_four_cpu_deltas_record_actionable_session_progress() {
        let fixture = tempfile::tempdir().expect("fixture root");
        let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
            .await
            .expect("store should open");
        let clock = Arc::new(FakeClock::new(TimePoint::new(
            WallTimeMs::new(10_000),
            5_000,
        )));
        let api = AgentApi::new(store.clone(), clock.clone())
            .await
            .expect("API should initialize");
        let native = NativeSessionKey::new(RuntimeKind::CodexCompanion, "process-job")
            .expect("native identity should be valid");
        let session = api
            .discover_session(DiscoveredSession {
                runtime: native.runtime(),
                native_id: native.native_id().to_owned(),
                kind: SessionKind::Main,
                parent: None,
                event_key: "discover-process-job".to_owned(),
                adapter_version: "test".to_owned(),
                evidence_source: "test:process-discovery".to_owned(),
                title: None,
                startup_directory: None,
            })
            .await
            .expect("session should be discovered");
        let process = ProcessIdentity::new(
            ProcessId::new(4_242).expect("PID should be valid"),
            99,
            BoundedText::new("executable", "/usr/bin/process-monitor-test")
                .expect("executable should be valid"),
        );
        api.ingest_native_observation(process_identity_observation(
            &native,
            process.clone(),
            clock.now(),
        ))
        .await
        .expect("process identity should apply");

        let sampler = Arc::new(FakeSampler {
            snapshots: Mutex::new(VecDeque::from([
                tree(process.clone(), CpuCounters::new(1, 1, 1, 1)),
                tree(process, CpuCounters::new(2, 2, 2, 2)),
            ])),
        });
        let monitor = ProcessMonitor::with_sampler(api, store.clone(), clock.clone(), sampler);
        let baseline = monitor.reconcile().await.expect("baseline should sample");
        assert_eq!(baseline.monitored_sessions(), 1);
        assert_eq!(baseline.progressed_sessions(), 0);

        clock.advance(watchdog_domain::DurationMs::new(1_000));
        let progress = monitor.reconcile().await.expect("delta should sample");
        assert_eq!(progress.progressed_sessions(), 1);
        assert_eq!(progress.warning_count(), 0);
        let stored = store
            .snapshot(session.session)
            .await
            .expect("snapshot should query")
            .expect("snapshot should exist");
        assert_eq!(
            stored
                .reducer_snapshot()
                .expect("reducer state should exist")
                .last_progress_summary(),
            Some("All four CPU times grew versus the previous process snapshot")
        );
    }

    fn tree(root: ProcessIdentity, cpu: CpuCounters) -> ProcessTreeSnapshot {
        ProcessTreeSnapshot::new(
            root.clone(),
            [ProcessSample::new(
                root,
                None,
                ProcessState::Running,
                cpu,
                Some(IoCounters::new(1, 1)),
                CommandFingerprint::from_redacted_cmdline(b"test\0", false),
            )],
            [],
        )
        .expect("tree should be coherent")
    }

    fn process_identity_observation(
        native: &NativeSessionKey,
        process: ProcessIdentity,
        now: TimePoint,
    ) -> ObservationEnvelope {
        ObservationEnvelope::new(
            ObservationId::from_native(native.runtime(), "process", "identity")
                .expect("observation ID should be valid"),
            native.clone(),
            now,
            ObservationSource::new(
                AdapterIdentity::new(native.runtime(), PROCESS_ADAPTER_VERSION)
                    .expect("adapter should be valid"),
                "process:identity",
                EvidenceTrust::Corroborating,
                None,
            )
            .expect("source should be valid"),
            ObservationPayload::ProcessIdentity(process),
        )
        .expect("observation should be valid")
    }
}
