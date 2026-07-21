#![cfg(target_os = "linux")]
//! Durable child-only termination saga acceptance tests.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use watchdog_domain::{
    AdapterIdentity, BoundedText, ChildSessionId, DeadlineCommand, DomainEvent, DomainEventKind,
    DurationMs, EventId, EvidenceTrust, MainSessionId, NativeSessionKey, ObservationEnvelope,
    ObservationId, ObservationPayload, ObservationSource, ProcessId, ProcessIdentity, ReducerInput,
    ReducerPolicy, RuntimeKind, SessionIdentity, SessionSnapshot, TerminationActionOutcome,
    TerminationBlocker, TerminationCandidate, TerminationComponent, TerminationFacts,
    TerminationHealth, TerminationStage, TimePoint, WallTimeMs, reduce,
};
use watchdog_process::{ProcessControl, ProcessControlError, ProcessSignal, VerifiedProcessHandle};
use watchdog_runtime::EventSequence;
use watchdog_server::{
    AgentApi, GracefulCancelError, GracefulCancelSupport, GracefulCanceller, RegisterSession,
    TerminationConfig, TerminationContext, TerminationEngine, TerminationStatus, TransportKey,
    VerifiedChild,
};
use watchdog_store::{OutboxDestination, TerminationAdvance, WatchdogStore};
use watchdog_testkit::FakeClock;

const MINUTE: u64 = 60_000;

fn time(minutes: u64) -> TimePoint {
    TimePoint::new(
        WallTimeMs::new(i64::try_from(minutes * MINUTE).expect("fixture time should fit")),
        minutes * MINUTE,
    )
}

fn process(start_time: u64) -> ProcessIdentity {
    ProcessIdentity::new(
        ProcessId::new(42).expect("fixture PID should be valid"),
        start_time,
        BoundedText::new("executable", "/usr/bin/claude").expect("executable should be bounded"),
    )
}

#[derive(Debug)]
struct FakeHandle {
    identity: ProcessIdentity,
    signals: Arc<Mutex<Vec<ProcessSignal>>>,
}

impl VerifiedProcessHandle for FakeHandle {
    fn identity(&self) -> &ProcessIdentity {
        &self.identity
    }

    fn signal(&self, signal: ProcessSignal) -> Result<(), ProcessControlError> {
        self.signals
            .lock()
            .expect("signal log should not be poisoned")
            .push(signal);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct FakeProcessControl {
    signals: Arc<Mutex<Vec<ProcessSignal>>>,
    fail_open: AtomicBool,
}

impl FakeProcessControl {
    fn signals(&self) -> Vec<ProcessSignal> {
        self.signals
            .lock()
            .expect("signal log should not be poisoned")
            .clone()
    }
}

impl ProcessControl for FakeProcessControl {
    fn open_verified(
        &self,
        expected: &ProcessIdentity,
    ) -> Result<Box<dyn VerifiedProcessHandle>, ProcessControlError> {
        if self.fail_open.load(Ordering::SeqCst) {
            return Err(ProcessControlError::IdentityMismatch {
                pid: expected.pid(),
            });
        }
        Ok(Box::new(FakeHandle {
            identity: expected.clone(),
            signals: self.signals.clone(),
        }))
    }
}

#[derive(Debug)]
struct FakeGraceful {
    results: Mutex<VecDeque<Result<GracefulCancelSupport, GracefulCancelError>>>,
    calls: Mutex<Vec<(ChildSessionId, RuntimeKind, String)>>,
}

impl FakeGraceful {
    fn new(
        results: impl IntoIterator<Item = Result<GracefulCancelSupport, GracefulCancelError>>,
    ) -> Self {
        Self {
            results: Mutex::new(results.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn call_count(&self) -> usize {
        self.calls
            .lock()
            .expect("call log should not be poisoned")
            .len()
    }
}

impl GracefulCanceller for FakeGraceful {
    fn request_cancel(
        &self,
        child: VerifiedChild<'_>,
    ) -> Result<GracefulCancelSupport, GracefulCancelError> {
        self.calls
            .lock()
            .expect("call log should not be poisoned")
            .push((child.child, child.runtime, child.native_id.to_owned()));
        self.results
            .lock()
            .expect("result queue should not be poisoned")
            .pop_front()
            .unwrap_or(Ok(GracefulCancelSupport::Unsupported))
    }
}

struct Fixture {
    store: WatchdogStore,
    engine: TerminationEngine,
    process_control: Arc<FakeProcessControl>,
    graceful: Arc<FakeGraceful>,
    root: MainSessionId,
    child: ChildSessionId,
    snapshot: SessionSnapshot,
    process: ProcessIdentity,
}

async fn fixture(
    config: TerminationConfig,
    graceful_result: Result<GracefulCancelSupport, GracefulCancelError>,
) -> Fixture {
    let directory = tempfile::tempdir().expect("temporary directory should exist");
    let path = directory.path().join("watchdog.db");
    let _retained = directory.keep();
    let store = WatchdogStore::open(&path)
        .await
        .expect("database should open");
    let api = AgentApi::new(store.clone(), Arc::new(FakeClock::new(time(0))))
        .await
        .expect("agent API should initialize");
    let transport = TransportKey::new("termination-test").expect("transport should be valid");
    let root = match api
        .register_session(
            &transport,
            RegisterSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: "main-1".to_owned(),
                kind: watchdog_domain::SessionKind::Main,
                parent: None,
                event_key: "register-main".to_owned(),
            },
        )
        .await
        .expect("main should register")
        .session
    {
        SessionIdentity::Main(root) => root,
        SessionIdentity::Child(_) => panic!("main registration returned child"),
    };
    let child = match api
        .register_session(
            &transport,
            RegisterSession {
                runtime: RuntimeKind::ClaudeCode,
                native_id: "child-1".to_owned(),
                kind: watchdog_domain::SessionKind::Child,
                parent: Some(root.session_id()),
                event_key: "register-child".to_owned(),
            },
        )
        .await
        .expect("child should register")
        .session
    {
        SessionIdentity::Child(child) => child,
        SessionIdentity::Main(_) => panic!("child registration returned main"),
    };
    let snapshot = reduce(
        SessionSnapshot::new(SessionIdentity::Child(child), root, time(0)),
        ReducerInput::Tick(time(15)),
        ReducerPolicy::default(),
    )
    .into_snapshot();
    let process_control = Arc::new(FakeProcessControl::default());
    let graceful = Arc::new(FakeGraceful::new([graceful_result]));
    let sequence = Arc::new(
        EventSequence::from_store(&store)
            .await
            .expect("event sequence should resume"),
    );
    let engine = TerminationEngine::new(
        store.clone(),
        sequence,
        process_control.clone(),
        graceful.clone(),
        config,
    );
    Fixture {
        store,
        engine,
        process_control,
        graceful,
        root,
        child,
        snapshot,
        process: process(100),
    }
}

fn context<'a>(
    fixture: &'a Fixture,
    snapshot: &'a SessionSnapshot,
    now: TimePoint,
) -> TerminationContext<'a> {
    TerminationContext {
        candidate: TerminationCandidate::new(fixture.child),
        root: fixture.root,
        facts: TerminationFacts {
            snapshot,
            runtime: RuntimeKind::ClaudeCode,
            trustworthy_relation: true,
            active_operation: false,
            fresh_process: Some(&fixture.process),
            health: TerminationHealth::healthy(),
            now,
            terminate_after_stalled: DurationMs::new(60 * MINUTE),
        },
        native_id: "child-1",
        child_exited: false,
        process_identity_changed: false,
    }
}

fn observation(sequence: u64, payload: ObservationPayload) -> ObservationEnvelope {
    ObservationEnvelope::new(
        ObservationId::from_native(RuntimeKind::ClaudeCode, "termination", sequence.to_string())
            .expect("observation ID should be valid"),
        NativeSessionKey::new(RuntimeKind::ClaudeCode, "child-1")
            .expect("native identity should be valid"),
        time(76 + sequence),
        ObservationSource::new(
            AdapterIdentity::new(RuntimeKind::ClaudeCode, "test").expect("adapter should be valid"),
            "termination-test",
            EvidenceTrust::Authoritative,
            None,
        )
        .expect("source should be valid"),
        payload,
    )
    .expect("observation should be valid")
}

#[tokio::test]
async fn warning_graceful_term_and_kill_are_durable_and_sequenced() {
    let fixture = fixture(
        TerminationConfig::default(),
        Err(GracefulCancelError::Unavailable),
    )
    .await;
    assert_eq!(
        fixture
            .engine
            .reconcile(context(&fixture, &fixture.snapshot, time(75)))
            .await
            .expect("warning should persist"),
        TerminationStatus::Started
    );
    assert!(fixture.process_control.signals().is_empty());

    assert_eq!(
        fixture
            .engine
            .reconcile(context(&fixture, &fixture.snapshot, time(85)))
            .await
            .expect("graceful stage should persist"),
        TerminationStatus::Advanced
    );
    let graceful_stage = fixture
        .store
        .termination_saga(fixture.child)
        .await
        .expect("saga should load")
        .expect("saga should exist");
    assert_eq!(graceful_stage.stage, TerminationStage::GracefulCancel);
    assert_eq!(
        graceful_stage.last_outcome,
        Some(TerminationActionOutcome::GracefulFailed)
    );
    assert_eq!(fixture.graceful.call_count(), 1);

    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(95)))
        .await
        .expect("SIGTERM should persist");
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(105)))
        .await
        .expect("SIGKILL should persist");
    assert_eq!(
        fixture.process_control.signals(),
        vec![ProcessSignal::Terminate, ProcessSignal::Kill]
    );
    let saga = fixture
        .store
        .termination_saga(fixture.child)
        .await
        .expect("saga should load")
        .expect("saga should exist");
    assert_eq!(saga.stage, TerminationStage::Sigkill);
    assert_eq!(saga.revision, 4);
    let events = fixture
        .store
        .events_after(fixture.root, EventId::new(0), 100)
        .await
        .expect("events should load");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event.kind(),
                watchdog_domain::DomainEventKind::TerminationChanged { .. }
            ))
            .count(),
        4
    );
}

#[tokio::test]
async fn changed_native_cancellation_identity_aborts_before_any_signal() {
    let fixture = fixture(
        TerminationConfig::default(),
        Err(GracefulCancelError::EvidenceChanged),
    )
    .await;
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(75)))
        .await
        .expect("warning should persist");
    assert_eq!(
        fixture
            .engine
            .reconcile(context(&fixture, &fixture.snapshot, time(85)))
            .await
            .expect("changed native identity should reconcile safely"),
        TerminationStatus::Aborted
    );
    assert!(fixture.process_control.signals().is_empty());
    assert_eq!(
        fixture
            .store
            .termination_saga(fixture.child)
            .await
            .expect("saga should load")
            .expect("saga should exist")
            .stage,
        TerminationStage::Aborted
    );
}

#[tokio::test]
async fn parent_extension_aborts_grace_and_old_deadline_cannot_resume_it() {
    let fixture = fixture(
        TerminationConfig::default(),
        Ok(GracefulCancelSupport::Requested),
    )
    .await;
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(75)))
        .await
        .expect("warning should persist");
    let extended = reduce(
        fixture.snapshot.clone(),
        ReducerInput::Observation(observation(
            1,
            ObservationPayload::Deadline(DeadlineCommand::Set(time(200).wall_time())),
        )),
        ReducerPolicy::default(),
    )
    .into_snapshot();
    assert_eq!(
        fixture
            .engine
            .reconcile(context(&fixture, &extended, time(80)))
            .await
            .expect("extension should abort"),
        TerminationStatus::Aborted
    );
    assert_eq!(
        fixture
            .engine
            .reconcile(context(&fixture, &fixture.snapshot, time(500)))
            .await
            .expect("aborted saga should remain terminal"),
        TerminationStatus::NoAction
    );
    assert!(fixture.process_control.signals().is_empty());
    assert_eq!(fixture.graceful.call_count(), 0);
    for destination in [
        OutboxDestination::Browser,
        OutboxDestination::HomeAssistant,
        OutboxDestination::Webhook,
    ] {
        let pending = fixture
            .store
            .pending_outbox_for(destination, 10)
            .await
            .expect("human outbox should load");
        assert_eq!(pending.len(), 1, "only the warning should notify humans");
        let warning: watchdog_domain::DomainEvent =
            serde_json::from_slice(pending[0].payload()).expect("warning payload should decode");
        assert!(matches!(
            warning.kind(),
            watchdog_domain::DomainEventKind::TerminationChanged {
                stage: TerminationStage::WarningGrace,
                ..
            }
        ));
    }
}

#[tokio::test]
async fn degraded_health_suspends_without_advancing_or_signalling() {
    let fixture = fixture(
        TerminationConfig::default(),
        Ok(GracefulCancelSupport::Requested),
    )
    .await;
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(75)))
        .await
        .expect("warning should persist");
    let mut degraded = context(&fixture, &fixture.snapshot, time(100));
    degraded.facts.health = degraded
        .facts
        .health
        .with_unhealthy(TerminationComponent::Adapter);
    assert_eq!(
        fixture
            .engine
            .reconcile(degraded)
            .await
            .expect("degradation should suspend"),
        TerminationStatus::Suspended
    );
    let saga = fixture
        .store
        .termination_saga(fixture.child)
        .await
        .expect("saga should load")
        .expect("saga should exist");
    assert_eq!(saga.stage, TerminationStage::WarningGrace);
    assert_eq!(saga.revision, 1);
    assert!(fixture.process_control.signals().is_empty());
}

#[tokio::test]
async fn pid_reuse_between_graceful_and_signal_aborts_without_touching_replacement() {
    let fixture = fixture(
        TerminationConfig::default(),
        Ok(GracefulCancelSupport::Requested),
    )
    .await;
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(75)))
        .await
        .expect("warning should persist");
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(85)))
        .await
        .expect("graceful stage should persist");
    let replacement = process(101);
    let mut reused = context(&fixture, &fixture.snapshot, time(95));
    reused.facts.fresh_process = Some(&replacement);
    assert_eq!(
        fixture
            .engine
            .reconcile(reused)
            .await
            .expect("PID reuse should abort"),
        TerminationStatus::Aborted
    );
    assert!(fixture.process_control.signals().is_empty());
    let saga = fixture
        .store
        .termination_saga(fixture.child)
        .await
        .expect("saga should load")
        .expect("saga should exist");
    assert!(
        saga.safety
            .blockers
            .contains(&TerminationBlocker::ProcessIdentityChanged)
    );
}

#[tokio::test]
async fn sigkill_opt_out_can_resume_after_policy_is_reenabled() {
    let config = TerminationConfig::new(
        true,
        false,
        DurationMs::new(10 * MINUTE),
        DurationMs::new(10 * MINUTE),
    )
    .expect("config should be valid");
    let fixture = fixture(config, Ok(GracefulCancelSupport::Unsupported)).await;
    for minute in [75, 85, 95, 105] {
        fixture
            .engine
            .reconcile(context(&fixture, &fixture.snapshot, time(minute)))
            .await
            .expect("saga stage should reconcile");
    }
    assert_eq!(
        fixture.process_control.signals(),
        vec![ProcessSignal::Terminate]
    );
    let saga = fixture
        .store
        .termination_saga(fixture.child)
        .await
        .expect("saga should load")
        .expect("saga should exist");
    assert_eq!(saga.stage, TerminationStage::Sigterm);
    assert!(saga.next_action_at.is_some());
    assert_eq!(
        saga.last_outcome,
        Some(TerminationActionOutcome::SigkillDisabled)
    );

    let revision = saga.revision;
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(115)))
        .await
        .expect("disabled SIGKILL should remain stable");
    assert_eq!(
        fixture
            .store
            .termination_saga(fixture.child)
            .await
            .expect("saga should load")
            .expect("saga should exist")
            .revision,
        revision,
        "disabled reconciliation must not emit repeated audit events"
    );

    let enabled = TerminationEngine::new(
        fixture.store.clone(),
        Arc::new(
            EventSequence::from_store(&fixture.store)
                .await
                .expect("event sequence should resume"),
        ),
        fixture.process_control.clone(),
        fixture.graceful.clone(),
        TerminationConfig::default(),
    );
    assert_eq!(
        enabled
            .reconcile(context(&fixture, &fixture.snapshot, time(115)))
            .await
            .expect("re-enabled SIGKILL should resume the saga"),
        TerminationStatus::Advanced
    );
    assert_eq!(
        fixture.process_control.signals(),
        vec![ProcessSignal::Terminate, ProcessSignal::Kill]
    );
}

#[tokio::test]
async fn legacy_sigkill_opt_out_without_a_deadline_resumes_after_upgrade() {
    let disabled = TerminationConfig::new(
        true,
        false,
        DurationMs::new(10 * MINUTE),
        DurationMs::new(10 * MINUTE),
    )
    .expect("config should be valid");
    let fixture = fixture(disabled, Ok(GracefulCancelSupport::Unsupported)).await;
    for minute in [75, 85, 95, 105] {
        fixture
            .engine
            .reconcile(context(&fixture, &fixture.snapshot, time(minute)))
            .await
            .expect("disabled saga should reconcile");
    }
    let mut legacy = fixture
        .store
        .termination_saga(fixture.child)
        .await
        .expect("saga should load")
        .expect("saga should exist");
    legacy.revision += 1;
    legacy.next_action_at = None;
    legacy.last_outcome = Some(TerminationActionOutcome::AutomationDisabled);
    let event_id = EventId::new(
        fixture
            .store
            .latest_event_id()
            .await
            .expect("event sequence should load")
            .value()
            + 1,
    );
    fixture
        .store
        .apply_termination_advance(&TerminationAdvance::new(
            legacy,
            DomainEvent::new(
                event_id,
                fixture.root,
                SessionIdentity::Child(fixture.child),
                time(105).wall_time(),
                DomainEventKind::TerminationChanged {
                    stage: TerminationStage::Sigterm,
                    outcome: TerminationActionOutcome::AutomationDisabled,
                },
            ),
            [],
        ))
        .await
        .expect("legacy saga should persist");

    let enabled = TerminationEngine::new(
        fixture.store.clone(),
        Arc::new(
            EventSequence::from_store(&fixture.store)
                .await
                .expect("event sequence should resume"),
        ),
        fixture.process_control.clone(),
        fixture.graceful.clone(),
        TerminationConfig::default(),
    );
    assert_eq!(
        enabled
            .reconcile(context(&fixture, &fixture.snapshot, time(115)))
            .await
            .expect("legacy SIGKILL opt-out should resume"),
        TerminationStatus::Advanced
    );
    assert_eq!(
        fixture.process_control.signals(),
        vec![ProcessSignal::Terminate, ProcessSignal::Kill]
    );
}

#[tokio::test]
async fn fresh_child_exit_completes_saga_before_any_os_signal() {
    let fixture = fixture(
        TerminationConfig::default(),
        Ok(GracefulCancelSupport::Requested),
    )
    .await;
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(75)))
        .await
        .expect("warning should persist");
    let mut exited = context(&fixture, &fixture.snapshot, time(85));
    exited.child_exited = true;
    exited.facts.fresh_process = None;
    assert_eq!(
        fixture
            .engine
            .reconcile(exited)
            .await
            .expect("exit should complete"),
        TerminationStatus::Completed
    );
    assert!(fixture.process_control.signals().is_empty());
    assert_eq!(fixture.graceful.call_count(), 0);
}

#[tokio::test]
async fn restart_resumes_from_durable_stage_without_skipping_grace() {
    let fixture = fixture(
        TerminationConfig::default(),
        Ok(GracefulCancelSupport::Requested),
    )
    .await;
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(75)))
        .await
        .expect("warning should persist");
    let restarted = TerminationEngine::new(
        fixture.store.clone(),
        Arc::new(
            EventSequence::from_store(&fixture.store)
                .await
                .expect("event sequence should restart"),
        ),
        fixture.process_control.clone(),
        fixture.graceful.clone(),
        TerminationConfig::default(),
    );

    assert_eq!(
        restarted
            .reconcile(context(&fixture, &fixture.snapshot, time(84)))
            .await
            .expect("pre-deadline restart should reconcile"),
        TerminationStatus::NoAction
    );
    assert_eq!(fixture.graceful.call_count(), 0);
    assert_eq!(
        restarted
            .reconcile(context(&fixture, &fixture.snapshot, time(85)))
            .await
            .expect("deadline should advance one stage"),
        TerminationStatus::Advanced
    );
    assert_eq!(fixture.graceful.call_count(), 1);
    assert!(fixture.process_control.signals().is_empty());
}

#[tokio::test]
async fn failed_fresh_pidfd_verification_aborts_and_records_diagnostic() {
    let fixture = fixture(
        TerminationConfig::default(),
        Ok(GracefulCancelSupport::Unsupported),
    )
    .await;
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(75)))
        .await
        .expect("warning should persist");
    fixture
        .engine
        .reconcile(context(&fixture, &fixture.snapshot, time(85)))
        .await
        .expect("graceful stage should persist");
    fixture
        .process_control
        .fail_open
        .store(true, Ordering::SeqCst);

    assert_eq!(
        fixture
            .engine
            .reconcile(context(&fixture, &fixture.snapshot, time(95)))
            .await
            .expect("failed verification should abort"),
        TerminationStatus::Aborted
    );
    assert!(fixture.process_control.signals().is_empty());
    let saga = fixture
        .store
        .termination_saga(fixture.child)
        .await
        .expect("saga should load")
        .expect("saga should exist");
    assert_eq!(saga.stage, TerminationStage::Aborted);
    assert!(
        saga.safety
            .blockers
            .contains(&TerminationBlocker::ProcessControlFailure)
    );
}
