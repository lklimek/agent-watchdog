//! Transactional persistence acceptance tests.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use watchdog_domain::{
    AdapterIdentity, BoundedText, ChildSessionId, DetailedState, DomainEvent, DomainEventKind,
    EventId, EvidenceTrust, MainSessionId, NativeSessionKey, ObservationEnvelope, ObservationId,
    ObservationPayload, ObservationSource, RuntimeKind, SessionId, SessionIdentity, TimePoint,
    WallTimeMs,
};
use watchdog_store::{
    ActivityEvidence, ActivitySampleRecord, AdapterHealthRecord, AdapterHealthStatus,
    ApplyObservation, ApplyResult, DeadlineRecord, FileCursorRecord, InboxOffsetRecord,
    NotificationAttemptRecord, NotificationChannel, NotificationOutcome, OutboxDestination,
    RegisteredWatchPathRecord, RelationRecord, SessionMetadataRecord, SnapshotUpdate, StoreCounts,
    StoreError, TerminationGate, TerminationSafetyRecord, TerminationSagaRecord, TerminationStage,
    WatchdogStore,
};

fn fixture(observation_key: &str, event_id: u64, revision: u64) -> ApplyObservation {
    fixture_for("session-1", observation_key, event_id, revision)
}

fn fixture_for(
    native_id: &str,
    observation_key: &str,
    event_id: u64,
    revision: u64,
) -> ApplyObservation {
    fixture_for_fingerprint(native_id, observation_key, event_id, revision, "hook:stop")
}

fn fixture_for_fingerprint(
    native_id: &str,
    observation_key: &str,
    event_id: u64,
    revision: u64,
    fingerprint: &str,
) -> ApplyObservation {
    let subject =
        NativeSessionKey::new(RuntimeKind::ClaudeCode, native_id).expect("fixture should be valid");
    let root = MainSessionId::from(SessionId::from_native(&subject));
    let session = SessionIdentity::Main(root);
    let observation = ObservationEnvelope::new(
        ObservationId::from_native(RuntimeKind::ClaudeCode, "hook", observation_key)
            .expect("fixture should be valid"),
        subject,
        TimePoint::new(WallTimeMs::new(1_000), 500),
        ObservationSource::new(
            AdapterIdentity::new(RuntimeKind::ClaudeCode, "2.1.212")
                .expect("fixture should be valid"),
            fingerprint,
            EvidenceTrust::Authoritative,
            None,
        )
        .expect("fixture should be valid"),
        ObservationPayload::NativeState(DetailedState::Running),
    )
    .expect("fixture should be valid");
    let snapshot = SnapshotUpdate::new(
        session,
        root,
        revision,
        DetailedState::Running,
        WallTimeMs::new(1_000),
    )
    .expect("fixture should be valid");
    let event = DomainEvent::new(
        EventId::new(event_id),
        root,
        session,
        WallTimeMs::new(1_000),
        DomainEventKind::StateChanged {
            from: DetailedState::Starting,
            to: DetailedState::Running,
        },
    );

    ApplyObservation::new(
        observation,
        snapshot,
        vec![event],
        [OutboxDestination::ParentInbox, OutboxDestination::Sse],
    )
}

fn child_fixture(root: MainSessionId, event_id: u64) -> (ApplyObservation, ChildSessionId) {
    let subject =
        NativeSessionKey::new(RuntimeKind::ClaudeCode, "child-1").expect("fixture should be valid");
    let child = ChildSessionId::from(SessionId::from_native(&subject));
    let session = SessionIdentity::Child(child);
    let observation = ObservationEnvelope::new(
        ObservationId::from_native(RuntimeKind::ClaudeCode, "hook", "child-observation-1")
            .expect("fixture should be valid"),
        subject,
        TimePoint::new(WallTimeMs::new(1_100), 600),
        observation_source(),
        ObservationPayload::NativeState(DetailedState::Running),
    )
    .expect("fixture should be valid");
    let snapshot = SnapshotUpdate::new(
        session,
        root,
        1,
        DetailedState::Running,
        WallTimeMs::new(1_100),
    )
    .expect("fixture should be valid");
    let event = DomainEvent::new(
        EventId::new(event_id),
        root,
        session,
        WallTimeMs::new(1_100),
        DomainEventKind::StateChanged {
            from: DetailedState::Starting,
            to: DetailedState::Running,
        },
    );
    (
        ApplyObservation::new(
            observation,
            snapshot,
            vec![event],
            [OutboxDestination::ParentInbox],
        ),
        child,
    )
}

fn observation_source() -> ObservationSource {
    ObservationSource::new(
        AdapterIdentity::new(RuntimeKind::ClaudeCode, "2.1.212").expect("fixture should be valid"),
        "hook:stop",
        EvidenceTrust::Authoritative,
        None,
    )
    .expect("fixture should be valid")
}

struct TestDatabase {
    path: PathBuf,
}

impl TestDatabase {
    fn new(name: &str) -> Self {
        static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        Self {
            path: std::env::temp_dir().join(format!(
                "agent-watchdog-{}-{sequence}-{name}.db",
                std::process::id()
            )),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-shm", "-wal"] {
            let path = PathBuf::from(format!("{}{suffix}", self.path.display()));
            if let Err(error) = fs::remove_file(path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                eprintln!("failed to remove test database sidecar: {error}");
            }
        }
    }
}

#[tokio::test]
async fn observation_snapshot_event_and_outbox_commit_atomically() {
    let database = TestDatabase::new("atomic-commit");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");

    assert_eq!(
        store
            .apply_observation(&fixture("observation-1", 1, 1))
            .await
            .expect("transaction should commit"),
        ApplyResult::Applied
    );
    let counts = store.counts().await.expect("counts should load");

    assert_eq!(counts.observations, 1);
    assert_eq!(counts.snapshots, 1);
    assert_eq!(counts.events, 1);
    assert_eq!(counts.outbox, 2);
}

#[tokio::test]
async fn duplicate_observation_is_idempotent() {
    let database = TestDatabase::new("duplicate-observation");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");
    let apply = fixture("observation-1", 2, 1);

    assert_eq!(
        store
            .apply_observation(&apply)
            .await
            .expect("first transaction should commit"),
        ApplyResult::Applied
    );
    assert_eq!(
        store
            .apply_observation(&apply)
            .await
            .expect("duplicate should be harmless"),
        ApplyResult::Duplicate
    );
    let counts = store.counts().await.expect("counts should load");

    assert_eq!(counts.observations, 1);
    assert_eq!(counts.events, 1);
    assert_eq!(counts.outbox, 2);
}

#[tokio::test]
async fn duplicate_identity_with_different_content_is_rejected() {
    let database = TestDatabase::new("conflicting-observation");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");
    store
        .apply_observation(&fixture_for_fingerprint(
            "session-1",
            "observation-1",
            30,
            1,
            "hook:first",
        ))
        .await
        .expect("seed transaction should commit");

    assert!(
        store
            .apply_observation(&fixture_for_fingerprint(
                "session-1",
                "observation-1",
                31,
                2,
                "hook:different",
            ))
            .await
            .is_err()
    );
    let counts = store.counts().await.expect("counts should load");
    assert_eq!(counts.observations, 1);
    assert_eq!(counts.events, 1);
}

#[tokio::test]
async fn event_conflict_rolls_back_observation_and_snapshot() {
    let database = TestDatabase::new("rollback-conflict");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");
    store
        .apply_observation(&fixture("observation-1", 3, 1))
        .await
        .expect("seed transaction should commit");

    let conflicting = fixture("observation-2", 3, 2);
    assert!(store.apply_observation(&conflicting).await.is_err());

    let counts = store.counts().await.expect("counts should load");
    assert_eq!(counts.observations, 1);
    assert_eq!(counts.events, 1);
    assert_eq!(counts.outbox, 2);
}

#[tokio::test]
async fn stale_snapshot_rolls_back_new_observation() {
    let database = TestDatabase::new("stale-snapshot");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");
    store
        .apply_observation(&fixture("observation-1", 5, 2))
        .await
        .expect("seed transaction should commit");

    assert!(
        store
            .apply_observation(&fixture("observation-2", 6, 1))
            .await
            .is_err()
    );
    let counts = store.counts().await.expect("counts should load");
    assert_eq!(counts.observations, 1);
    assert_eq!(counts.events, 1);
}

#[tokio::test]
async fn undelivered_outbox_survives_reopen() {
    let database = TestDatabase::new("restart-outbox");
    {
        let store = WatchdogStore::open(database.path())
            .await
            .expect("database should open");
        store
            .apply_observation(&fixture("observation-1", 4, 1))
            .await
            .expect("transaction should commit");
    }

    let reopened = WatchdogStore::open(database.path())
        .await
        .expect("database should reopen");
    let pending = reopened
        .pending_outbox(10)
        .await
        .expect("outbox should load");

    assert_eq!(pending.len(), 2);
    assert!(
        pending
            .iter()
            .all(|entry| entry.event_id() == EventId::new(4))
    );
}

#[tokio::test]
async fn outbox_acknowledgement_is_durable_and_idempotent() {
    let database = TestDatabase::new("outbox-ack");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");
    store
        .apply_observation(&fixture("observation-1", 7, 1))
        .await
        .expect("transaction should commit");
    let pending = store.pending_outbox(10).await.expect("outbox should load");

    assert!(
        store
            .acknowledge_outbox(pending[0].outbox_id(), WallTimeMs::new(2_000))
            .await
            .expect("acknowledgement should commit")
    );
    assert!(
        !store
            .acknowledge_outbox(pending[0].outbox_id(), WallTimeMs::new(3_000))
            .await
            .expect("duplicate acknowledgement should be harmless")
    );
    assert_eq!(
        store
            .pending_outbox(10)
            .await
            .expect("outbox should load")
            .len(),
        1
    );
}

#[tokio::test]
async fn outbox_delivery_can_read_one_destination_without_head_of_line_blocking() {
    let database = TestDatabase::new("outbox-destination");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");
    store
        .apply_observation(&fixture("observation-1", 8, 1))
        .await
        .expect("transaction should commit");

    let sse = store
        .pending_outbox_for(OutboxDestination::Sse, 10)
        .await
        .expect("destination should load");

    assert_eq!(sse.len(), 1);
    assert_eq!(sse[0].destination(), "sse");
    assert_eq!(sse[0].event_id(), EventId::new(8));
}

#[tokio::test]
async fn initialization_enables_required_sqlite_pragmas_and_schema() {
    let database = TestDatabase::new("health-pragmas");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");

    let health = store.health().await.expect("health should load");

    assert_eq!(health.journal_mode, "wal");
    assert!(health.foreign_keys);
    assert!(health.schema_version >= 1);
    assert_eq!(health.application_table_count, 16);
}

#[tokio::test]
async fn manual_wipe_removes_watchdog_data_without_touching_adjacent_files() {
    let database = TestDatabase::new("manual-wipe");
    let native_file = database.path().with_extension("native-state");
    fs::write(&native_file, b"runtime-owned")
        .expect("isolated adjacent fixture should be writable");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");
    store
        .apply_observation(&fixture("observation-1", 8, 1))
        .await
        .expect("transaction should commit");

    store
        .wipe_watchdog_data()
        .await
        .expect("wipe should commit");

    assert_eq!(
        store.counts().await.expect("counts should load"),
        StoreCounts {
            observations: 0,
            snapshots: 0,
            events: 0,
            outbox: 0,
        }
    );
    assert_eq!(
        fs::read(&native_file).expect("native fixture should remain"),
        b"runtime-owned"
    );
    fs::remove_file(native_file).expect("native fixture should be removable");
}

#[tokio::test]
async fn registered_watch_paths_are_durable_idempotent_and_event_keys_cannot_be_reused() {
    let database = TestDatabase::new("registered-watch-paths");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("store should open");
    let main = fixture("watch-main", 1, 1);
    let root = MainSessionId::from(SessionId::from_native(
        &NativeSessionKey::new(RuntimeKind::ClaudeCode, "session-1")
            .expect("fixture main should be valid"),
    ));
    store
        .apply_observation(&main)
        .await
        .expect("main should persist");
    let (child_apply, child) = child_fixture(root, 2);
    store
        .apply_observation(&child_apply)
        .await
        .expect("child should persist");
    let record = RegisteredWatchPathRecord::new(
        SessionIdentity::Child(child),
        root,
        "watch-path-1",
        "/host/worktrees/project-a",
        WallTimeMs::new(2_000),
    )
    .expect("record should be valid");

    assert!(store.save_registered_watch_path(&record).await.unwrap());
    assert!(!store.save_registered_watch_path(&record).await.unwrap());
    assert_eq!(
        store.registered_watch_paths(10).await.unwrap(),
        vec![record.clone()]
    );
    assert_eq!(
        store
            .registered_watch_paths_for(SessionIdentity::Child(child), 10)
            .await
            .unwrap(),
        vec![record]
    );

    let conflicting = RegisteredWatchPathRecord::new(
        SessionIdentity::Child(child),
        root,
        "watch-path-1",
        "/host/worktrees/project-b",
        WallTimeMs::new(2_001),
    )
    .expect("conflicting record should be structurally valid");
    assert!(matches!(
        store.save_registered_watch_path(&conflicting).await,
        Err(StoreError::WatchPathIdentityConflict)
    ));
    let duplicate_path = RegisteredWatchPathRecord::new(
        SessionIdentity::Child(child),
        root,
        "watch-path-2",
        "/host/worktrees/project-a",
        WallTimeMs::new(2_002),
    )
    .expect("duplicate path should be structurally valid");
    assert!(matches!(
        store.save_registered_watch_path(&duplicate_path).await,
        Err(StoreError::WatchPathIdentityConflict)
    ));

    drop(store);
    let reopened = WatchdogStore::open(database.path())
        .await
        .expect("store should reopen");
    assert_eq!(reopened.registered_watch_paths(10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn concurrent_session_transactions_and_reads_converge() {
    let database = TestDatabase::new("concurrent");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");
    let mut tasks = Vec::new();
    for index in 1..=16_u64 {
        let task_store = store.clone();
        tasks.push(tokio::spawn(async move {
            task_store
                .apply_observation(&fixture_for(
                    &format!("session-{index}"),
                    &format!("observation-{index}"),
                    index,
                    1,
                ))
                .await
        }));
    }
    for task in tasks {
        assert_eq!(
            task.await
                .expect("task should join")
                .expect("write should commit"),
            ApplyResult::Applied
        );
    }

    let counts = store.counts().await.expect("counts should load");
    assert_eq!(counts.observations, 16);
    assert_eq!(counts.snapshots, 16);
    assert_eq!(counts.events, 16);
    assert_eq!(counts.outbox, 32);
}

#[tokio::test]
async fn incompatible_migration_checksum_fails_reopen() {
    let database = TestDatabase::new("incompatible-migration");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");
    drop(store);
    let options = sqlx::sqlite::SqliteConnectOptions::new().filename(database.path());
    let pool = sqlx::SqlitePool::connect_with(options)
        .await
        .expect("fixture database should connect");
    sqlx::query("UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = 1")
        .execute(&pool)
        .await
        .expect("fixture checksum should mutate");
    pool.close().await;

    assert!(WatchdogStore::open(database.path()).await.is_err());
}

#[tokio::test]
async fn corrupt_database_fails_initialization() {
    let database = TestDatabase::new("corrupt-database");
    fs::write(database.path(), b"not a sqlite database")
        .expect("isolated fixture should be writable");

    assert!(WatchdogStore::open(database.path()).await.is_err());
}

#[tokio::test]
async fn restart_repositories_round_trip_typed_bounded_records() {
    let database = TestDatabase::new("repository-round-trip");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");
    store
        .apply_observation(&fixture("observation-main", 20, 1))
        .await
        .expect("main should commit");
    let main_native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "session-1")
        .expect("fixture should be valid");
    let root = MainSessionId::from(SessionId::from_native(&main_native));
    let (child_apply, child) = child_fixture(root, 21);
    store
        .apply_observation(&child_apply)
        .await
        .expect("child should commit");

    let main_record = store
        .session(SessionIdentity::Main(root))
        .await
        .expect("session lookup should succeed")
        .expect("main session should exist");
    assert_eq!(main_record.root, root);
    assert_eq!(main_record.native.native_id(), "session-1");
    let sessions = store
        .sessions_for_root(root, 10)
        .await
        .expect("tree sessions should load");
    assert_eq!(sessions.len(), 2);
    assert!(
        sessions
            .iter()
            .any(|record| record.session == SessionIdentity::Child(child))
    );

    let metadata = SessionMetadataRecord::new(
        SessionIdentity::Main(root),
        Some("<script>alert('escaped')</script>".to_owned()),
        Some("/data/git-worktrees/agent-watchdog/feature".to_owned()),
        Some("git@github.com:lklimek/agent-watchdog.git".to_owned()),
        Some("feature/dashboard".to_owned()),
        Some(42),
        Some("https://github.com/lklimek/agent-watchdog/pull/42".to_owned()),
        WallTimeMs::new(2_000),
    )
    .expect("metadata should be bounded");
    store
        .save_session_metadata(&metadata)
        .await
        .expect("metadata should persist");
    let loaded_metadata = store
        .session_metadata(SessionIdentity::Main(root))
        .await
        .expect("metadata should load")
        .expect("session should exist");
    assert_eq!(loaded_metadata, metadata);

    assert_primary_ledger_reads(&store, root).await;
    assert_relation_and_activity(&store, root, child).await;
    assert_cursor_and_deadline(&store, child).await;
    assert_saga_and_inbox(&store, root, child).await;
    assert_health_and_notification(&store).await;
}

async fn assert_primary_ledger_reads(store: &WatchdogStore, root: MainSessionId) {
    let observation_id =
        ObservationId::from_native(RuntimeKind::ClaudeCode, "hook", "observation-main")
            .expect("fixture should be valid");
    let observation = store
        .observation(observation_id)
        .await
        .expect("observation should load")
        .expect("observation should exist");
    assert_eq!(observation.observation_id(), observation_id);

    let snapshot = store
        .snapshot(SessionIdentity::Main(root))
        .await
        .expect("snapshot should load")
        .expect("snapshot should exist");
    assert_eq!(snapshot.revision(), 1);
    assert_eq!(snapshot.state(), DetailedState::Running);

    let events = store
        .events_after(root, EventId::new(0), 10)
        .await
        .expect("events should load");
    assert_eq!(
        events.iter().map(DomainEvent::id).collect::<Vec<_>>(),
        vec![EventId::new(20), EventId::new(21)]
    );
}

async fn assert_relation_and_activity(
    store: &WatchdogStore,
    root: MainSessionId,
    child: ChildSessionId,
) {
    let relation = RelationRecord {
        child,
        parent: SessionIdentity::Main(root),
        root,
        selected: true,
        basis: watchdog_domain::CorrelationBasis::ExactNative,
        provenance: observation_source(),
        valid_from: WallTimeMs::new(1_100),
        valid_until: None,
    };
    store
        .save_relation(&relation)
        .await
        .expect("relation should persist");
    assert_eq!(
        store
            .relations_for_root(root, 10)
            .await
            .expect("relations should load"),
        vec![relation]
    );

    let activity = ActivitySampleRecord {
        session: SessionIdentity::Child(child),
        observed_at: WallTimeMs::new(1_200),
        evidence: ActivityEvidence::ProcessCpu {
            user_ticks: 1,
            system_ticks: 2,
            child_user_ticks: 3,
            child_system_ticks: 4,
        },
    };
    store
        .append_activity(&activity)
        .await
        .expect("activity should persist");
    assert_eq!(
        store
            .recent_activity(SessionIdentity::Child(child), 10)
            .await
            .expect("activity should load"),
        vec![activity]
    );
    let latest = ActivitySampleRecord {
        session: SessionIdentity::Child(child),
        observed_at: WallTimeMs::new(1_300),
        evidence: ActivityEvidence::ProcessCpu {
            user_ticks: 5,
            system_ticks: 6,
            child_user_ticks: 7,
            child_system_ticks: 8,
        },
    };
    store
        .save_latest_activity(&latest)
        .await
        .expect("latest diagnostic should replace its prior signal kind");
    assert_eq!(
        store
            .recent_activity(SessionIdentity::Child(child), 10)
            .await
            .expect("latest diagnostic should load"),
        vec![latest]
    );
}

async fn assert_cursor_and_deadline(store: &WatchdogStore, child: ChildSessionId) {
    let path_key = BoundedText::new("path_key", "claude/transcript/session-1")
        .expect("fixture should be valid");
    let cursor = FileCursorRecord::new(
        path_key.clone(),
        7,
        11,
        4_096,
        4_000,
        BoundedText::new("parser_version", "claude-jsonl-v1").expect("fixture should be valid"),
        None,
    )
    .expect("cursor should be valid");
    store
        .save_file_cursor(&cursor)
        .await
        .expect("cursor should persist");
    assert_eq!(
        store
            .file_cursor(&path_key)
            .await
            .expect("cursor should load"),
        Some(cursor)
    );

    let deadline = DeadlineRecord {
        session: SessionIdentity::Child(child),
        deadline: Some(WallTimeMs::new(9_000)),
        paused_at: None,
        provenance: observation_source(),
    };
    store
        .save_deadline(&deadline)
        .await
        .expect("deadline should persist");
    assert_eq!(
        store
            .deadline(SessionIdentity::Child(child))
            .await
            .expect("deadline should load"),
        Some(deadline)
    );
}

async fn assert_saga_and_inbox(store: &WatchdogStore, root: MainSessionId, child: ChildSessionId) {
    let saga = TerminationSagaRecord {
        child,
        stage: TerminationStage::WarningGrace,
        revision: 1,
        next_action_at: Some(WallTimeMs::new(10_000)),
        safety: TerminationSafetyRecord {
            passed_gates: [
                TerminationGate::TrustworthyChild,
                TerminationGate::NoSourceConflict,
                TerminationGate::NoActiveOperation,
                TerminationGate::NotWaitingForUser,
                TerminationGate::DeadlineAllows,
            ]
            .into_iter()
            .collect(),
            blockers: BTreeSet::default(),
            process: None,
        },
        last_outcome: Some(watchdog_domain::TerminationActionOutcome::WarningScheduled),
    };
    store
        .save_termination_saga(&saga)
        .await
        .expect("saga should persist");
    assert_eq!(
        store
            .termination_saga(child)
            .await
            .expect("saga should load"),
        Some(saga)
    );
    let stale_saga = TerminationSagaRecord {
        child,
        stage: TerminationStage::Sigterm,
        revision: 1,
        next_action_at: None,
        safety: TerminationSafetyRecord {
            passed_gates: BTreeSet::default(),
            blockers: BTreeSet::default(),
            process: None,
        },
        last_outcome: Some(watchdog_domain::TerminationActionOutcome::SignalSent),
    };
    assert!(store.save_termination_saga(&stale_saga).await.is_err());

    let offset = InboxOffsetRecord {
        parent: root,
        last_event_id: EventId::new(20),
        updated_at: WallTimeMs::new(2_000),
    };
    assert!(
        store
            .save_inbox_offset(offset)
            .await
            .expect("offset should persist")
    );
    assert!(
        !store
            .save_inbox_offset(InboxOffsetRecord {
                parent: root,
                last_event_id: EventId::new(19),
                updated_at: WallTimeMs::new(3_000),
            })
            .await
            .expect("backward offset should be harmless")
    );
    assert_eq!(
        store.inbox_offset(root).await.expect("offset should load"),
        Some(offset)
    );
}

async fn assert_health_and_notification(store: &WatchdogStore) {
    let adapter_health = AdapterHealthRecord {
        adapter: AdapterIdentity::new(RuntimeKind::ClaudeCode, "2.1.212")
            .expect("fixture should be valid"),
        status: AdapterHealthStatus::Healthy,
        last_success: Some(WallTimeMs::new(2_000)),
        last_error: None,
        affected_scope: None,
        message: None,
    };
    store
        .save_adapter_health(&adapter_health)
        .await
        .expect("health should persist");
    assert_eq!(
        store
            .adapter_health(RuntimeKind::ClaudeCode)
            .await
            .expect("health should load"),
        Some(adapter_health)
    );

    let attempt = NotificationAttemptRecord {
        event_id: EventId::new(20),
        channel: NotificationChannel::HomeAssistant,
        attempted_at: WallTimeMs::new(2_100),
        outcome: NotificationOutcome::Delivered,
        message: None,
    };
    store
        .record_notification_attempt(&attempt)
        .await
        .expect("notification attempt should persist");
    assert_eq!(
        store
            .notification_attempts(EventId::new(20), 10)
            .await
            .expect("notification attempts should load"),
        vec![attempt]
    );
}

#[test]
fn restart_record_invariants_reject_invalid_offsets_and_revisions() {
    let path_key = BoundedText::new("path_key", "runtime/file").expect("fixture should be valid");
    let parser_version = BoundedText::new("parser_version", "v1").expect("fixture should be valid");
    assert!(
        FileCursorRecord::new(path_key.clone(), 1, 2, 10, 11, parser_version.clone(), None,)
            .is_err()
    );
    let valid_cursor = FileCursorRecord::new(path_key, 1, 2, 10, 9, parser_version, None)
        .expect("fixture should be valid");
    let mut cursor_json = serde_json::to_value(valid_cursor).expect("cursor should serialize");
    cursor_json["complete_record_offset"] = serde_json::json!(11);
    assert!(serde_json::from_value::<FileCursorRecord>(cursor_json).is_err());

    let native = NativeSessionKey::new(RuntimeKind::ClaudeCode, "session-invariant")
        .expect("fixture should be valid");
    let root = MainSessionId::from(SessionId::from_native(&native));
    let valid = SnapshotUpdate::new(
        SessionIdentity::Main(root),
        root,
        1,
        DetailedState::Running,
        WallTimeMs::new(1),
    )
    .expect("fixture should be valid");
    let mut json = serde_json::to_value(valid).expect("snapshot should serialize");
    json["revision"] = serde_json::json!(0);

    assert!(serde_json::from_value::<SnapshotUpdate>(json).is_err());
}
