//! Transactional persistence acceptance tests.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use watchdog_domain::{
    AdapterIdentity, DetailedState, DomainEvent, DomainEventKind, EventId, EvidenceTrust,
    MainSessionId, NativeSessionKey, ObservationEnvelope, ObservationId, ObservationPayload,
    ObservationSource, RuntimeKind, SessionId, SessionIdentity, TimePoint, WallTimeMs,
};
use watchdog_store::{
    ApplyObservation, ApplyResult, OutboxDestination, SnapshotUpdate, StoreCounts, WatchdogStore,
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
            "hook:stop",
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
async fn initialization_enables_required_sqlite_pragmas_and_schema() {
    let database = TestDatabase::new("health-pragmas");
    let store = WatchdogStore::open(database.path())
        .await
        .expect("database should open");

    let health = store.health().await.expect("health should load");

    assert_eq!(health.journal_mode, "wal");
    assert!(health.foreign_keys);
    assert!(health.schema_version >= 1);
    assert_eq!(health.application_table_count, 13);
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
