//! Inotify worktree-ownership attribution acceptance tests.

use std::{path::PathBuf, sync::Arc};

use watchdog_domain::{RuntimeKind, SessionId, SessionKind, TimePoint, WallTimeMs};
use watchdog_server::{AgentApi, DiscoveredSession, FilesystemActivityReconciler};
use watchdog_store::{RegisteredWatchPathRecord, WatchdogStore};
use watchdog_testkit::FakeClock;

#[tokio::test]
async fn single_owner_advances_while_shared_worktree_remains_neutral() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
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
    let parent = discover_main(&api).await;
    discover_child(&api, parent, "single", "/host/repositories/single").await;
    discover_child(&api, parent, "shared-a", "/host/repositories/shared").await;
    discover_child(&api, parent, "shared-b", "/host/repositories/shared").await;
    let reconciler = FilesystemActivityReconciler::new(api, store.clone(), clock);

    let report = reconciler
        .reconcile(&[
            PathBuf::from("/host/repositories/single/src/lib.rs"),
            PathBuf::from("/host/repositories/shared/target/output"),
        ])
        .await
        .expect("ownership should reconcile");

    assert_eq!(report.attributed(), 1);
    assert_eq!(report.ambiguous(), 1);
    assert_eq!(report.unowned(), 0);
    assert_eq!(report.warnings(), 0);
    assert_eq!(
        progress_for(&store, "single").await,
        Some("Filesystem activity in owned worktree".to_owned())
    );
    assert_eq!(
        progress_for(&store, "shared-a").await,
        Some("Session registered with Agent Watchdog".to_owned())
    );
    assert_eq!(
        progress_for(&store, "shared-b").await,
        Some("Session registered with Agent Watchdog".to_owned())
    );
}

#[tokio::test]
async fn registered_additional_path_becomes_exact_child_activity_ownership() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
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
    let parent = discover_main(&api).await;
    discover_child(&api, parent, "registered", "/host/repositories/original").await;
    let child = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query")
        .into_iter()
        .find(|record| record.native.native_id() == "registered")
        .expect("registered child should exist");
    store
        .save_registered_watch_path(
            &RegisteredWatchPathRecord::new(
                child.session,
                child.root,
                "registered-path",
                "/host/repositories/additional",
                WallTimeMs::new(10_000),
            )
            .expect("registered path should be valid"),
        )
        .await
        .expect("registered path should persist");
    let reconciler = FilesystemActivityReconciler::new(api, store.clone(), clock);

    let report = reconciler
        .reconcile(&[PathBuf::from("/host/repositories/additional/src/lib.rs")])
        .await
        .expect("ownership should reconcile");

    assert_eq!(report.attributed(), 1);
    assert_eq!(report.ambiguous(), 0);
    assert_eq!(
        progress_for(&store, "registered").await,
        Some("Filesystem activity in owned worktree".to_owned())
    );
}

async fn discover_main(api: &AgentApi) -> SessionId {
    api.discover_session(DiscoveredSession {
        runtime: RuntimeKind::ClaudeCode,
        native_id: "main".to_owned(),
        kind: SessionKind::Main,
        parent: None,
        event_key: "main".to_owned(),
        adapter_version: "test".to_owned(),
        evidence_source: "test:discovery".to_owned(),
        title: None,
        startup_directory: Some("/host/repositories/main".to_owned()),
    })
    .await
    .expect("main should be discovered")
    .session
    .session_id()
}

async fn discover_child(api: &AgentApi, parent: SessionId, native_id: &str, directory: &str) {
    api.discover_session(DiscoveredSession {
        runtime: RuntimeKind::ClaudeCode,
        native_id: native_id.to_owned(),
        kind: SessionKind::Child,
        parent: Some(parent),
        event_key: format!("child:{native_id}"),
        adapter_version: "test".to_owned(),
        evidence_source: "test:discovery".to_owned(),
        title: None,
        startup_directory: Some(directory.to_owned()),
    })
    .await
    .expect("child should be discovered");
}

async fn progress_for(store: &WatchdogStore, native_id: &str) -> Option<String> {
    let sessions = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    let session = sessions
        .iter()
        .find(|record| record.native.native_id() == native_id)
        .expect("child should exist");
    store
        .snapshot(session.session)
        .await
        .expect("snapshot should query")
        .expect("snapshot should exist")
        .reducer_snapshot()
        .expect("reducer snapshot should exist")
        .last_progress_summary()
        .map(ToOwned::to_owned)
}
