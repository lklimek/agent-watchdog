//! Explicitly invoked production-service scale acceptance tests.

use std::{sync::Arc, time::Instant};

use watchdog_domain::{CompactState, DurationMs, RuntimeKind, SessionKind, TimePoint, WallTimeMs};
use watchdog_server::{AgentApi, DashboardQuery, DashboardService, DiscoveredSession};
use watchdog_store::WatchdogStore;
use watchdog_testkit::FakeClock;

const MAIN_SESSION_COUNT: usize = 50;
const CHILDREN_PER_MAIN: usize = 9;
const TOTAL_SESSION_COUNT: usize = MAIN_SESSION_COUNT * (CHILDREN_PER_MAIN + 1);
const MAX_INGESTION_TIME: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_SNAPSHOT_TIME: std::time::Duration = std::time::Duration::from_secs(2);
const MAX_HIGH_WATER_RSS_BYTES: u64 = 256 * 1024 * 1024;
const RESTART_SOAK_CYCLES: usize = 10;

#[tokio::test]
#[ignore = "release load gate; run explicitly with --ignored --test load"]
async fn target_population_converges_and_dashboard_remains_responsive() {
    let fixture = tempfile::tempdir().expect("load fixture should exist");
    let database = fixture.path().join("watchdog.db");
    let store = WatchdogStore::open(&database)
        .await
        .expect("database should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(10_000),
        5_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("agent API should initialize");

    let ingestion_started = Instant::now();
    for main_index in 0..MAIN_SESSION_COUNT {
        let main = api
            .discover_session(discovery(
                format!("main-{main_index}"),
                SessionKind::Main,
                None,
                format!("/repositories/project-{main_index:02}"),
            ))
            .await
            .expect("main should converge");
        for child_index in 0..CHILDREN_PER_MAIN {
            api.discover_session(discovery(
                format!("main-{main_index}-child-{child_index}"),
                SessionKind::Child,
                Some(main.session.session_id()),
                format!("/repositories/project-{main_index:02}/child-{child_index}"),
            ))
            .await
            .expect("child should converge");
        }
    }
    let ingestion_elapsed = ingestion_started.elapsed();
    assert!(
        ingestion_elapsed <= MAX_INGESTION_TIME,
        "target population ingestion took {ingestion_elapsed:?}"
    );

    let counts = store.counts().await.expect("counts should load");
    let expected_sessions = i64::try_from(TOTAL_SESSION_COUNT).expect("population should fit i64");
    assert_eq!(counts.snapshots, expected_sessions);
    assert_eq!(counts.observations, expected_sessions);

    let dashboard = DashboardService::new(store.clone(), clock.clone());
    let snapshot_started = Instant::now();
    let snapshot = dashboard
        .snapshot(DashboardQuery::default())
        .await
        .expect("dashboard should remain available");
    let snapshot_elapsed = snapshot_started.elapsed();
    assert!(
        snapshot_elapsed <= MAX_SNAPSHOT_TIME,
        "target dashboard snapshot took {snapshot_elapsed:?}"
    );
    assert_eq!(snapshot.sessions.len(), MAIN_SESSION_COUNT);
    let expected_children =
        u32::try_from(CHILDREN_PER_MAIN).expect("child count should fit dashboard counter");
    assert!(
        snapshot.sessions.iter().all(|main| {
            main.child_counts.get(&CompactState::Active) == Some(&expected_children)
        })
    );

    let restarted = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("agent API should rebuild lanes after restart");
    assert_eq!(
        restarted
            .reconcile_timers()
            .await
            .expect("restarted timers should reconcile")
            .evaluated_sessions(),
        TOTAL_SESSION_COUNT
    );
    let restarted_snapshot = DashboardService::new(store, clock)
        .snapshot(DashboardQuery::default())
        .await
        .expect("dashboard should converge after restart");
    assert_eq!(restarted_snapshot.sessions, snapshot.sessions);

    let high_water_rss_bytes = linux_high_water_rss_bytes();
    assert!(
        high_water_rss_bytes <= MAX_HIGH_WATER_RSS_BYTES,
        "load test high-water RSS was {high_water_rss_bytes} bytes"
    );
    eprintln!(
        "load metrics: sessions={TOTAL_SESSION_COUNT} ingestion_ms={} snapshot_ms={} high_water_rss_bytes={high_water_rss_bytes}",
        ingestion_elapsed.as_millis(),
        snapshot_elapsed.as_millis()
    );
}

#[tokio::test]
#[ignore = "release restart-soak gate; run explicitly with --ignored --test load"]
async fn target_population_survives_repeated_restart_reconciliation() {
    let fixture = tempfile::tempdir().expect("load fixture should exist");
    let database = fixture.path().join("watchdog.db");
    let clock = Arc::new(FakeClock::new(TimePoint::new(WallTimeMs::new(50_000), 100)));

    for cycle in 0..RESTART_SOAK_CYCLES {
        let store = WatchdogStore::open(&database)
            .await
            .expect("database should reopen during soak");
        let health = store.health().await.expect("store health should load");
        assert_eq!(health.journal_mode, "wal");
        assert!(health.foreign_keys);

        let api = AgentApi::new(store.clone(), clock.clone())
            .await
            .expect("agent API should restart");
        api.mark_restarted()
            .await
            .expect("restart boundary should be durable");
        populate_target_sessions(&api, cycle).await;

        let counts = store.counts().await.expect("counts should load");
        let expected_sessions =
            i64::try_from(TOTAL_SESSION_COUNT).expect("population should fit i64");
        assert_eq!(counts.snapshots, expected_sessions);

        let snapshot = DashboardService::new(store, clock.clone())
            .snapshot(DashboardQuery::default())
            .await
            .expect("dashboard should converge after every restart");
        assert_eq!(snapshot.sessions.len(), MAIN_SESSION_COUNT);
        let expected_children =
            u32::try_from(CHILDREN_PER_MAIN).expect("child count should fit dashboard counter");
        assert!(snapshot.sessions.iter().all(|main| {
            main.child_counts.get(&CompactState::Active) == Some(&expected_children)
        }));

        clock.advance(DurationMs::new(1_000));
    }

    let store = WatchdogStore::open(&database)
        .await
        .expect("database should open after soak");
    assert_eq!(
        store
            .counts()
            .await
            .expect("final counts should load")
            .snapshots,
        i64::try_from(TOTAL_SESSION_COUNT).expect("population should fit i64")
    );
}

async fn populate_target_sessions(api: &AgentApi, cycle: usize) {
    for main_index in 0..MAIN_SESSION_COUNT {
        let mut main_discovery = discovery(
            format!("main-{main_index}"),
            SessionKind::Main,
            None,
            format!("/repositories/project-{main_index:02}"),
        );
        main_discovery.event_key = format!("restart:{cycle}:main-{main_index}");
        let main = api
            .discover_session(main_discovery)
            .await
            .expect("main should reconcile after restart");
        for child_index in 0..CHILDREN_PER_MAIN {
            let mut child_discovery = discovery(
                format!("main-{main_index}-child-{child_index}"),
                SessionKind::Child,
                Some(main.session.session_id()),
                format!("/repositories/project-{main_index:02}/child-{child_index}"),
            );
            child_discovery.event_key =
                format!("restart:{cycle}:main-{main_index}:child-{child_index}");
            api.discover_session(child_discovery)
                .await
                .expect("child should reconcile after restart");
        }
    }
}

fn discovery(
    native_id: String,
    kind: SessionKind,
    parent: Option<watchdog_domain::SessionId>,
    startup_directory: String,
) -> DiscoveredSession {
    DiscoveredSession {
        runtime: RuntimeKind::CodexCli,
        event_key: format!("load:{native_id}"),
        adapter_version: "load-fixture".to_owned(),
        evidence_source: "load:synthetic-native".to_owned(),
        title: None,
        native_id,
        kind,
        parent,
        startup_directory: Some(startup_directory),
    }
}

fn linux_high_water_rss_bytes() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status")
        .expect("Linux load gate should expose process status");
    let kibibytes = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .expect("Linux process status should expose VmHWM in KiB");
    kibibytes * 1024
}
