//! Automatic runtime discovery and best-effort reconciliation acceptance tests.

use std::{fs, sync::Arc};

use serde_json::json;
use watchdog_domain::{SessionKind, TimePoint, WallTimeMs};
use watchdog_server::{AgentApi, ClaudeTeamDiscovery, DashboardQuery, DashboardService};
use watchdog_store::WatchdogStore;
use watchdog_testkit::FakeClock;

#[tokio::test]
async fn claude_team_discovery_keeps_good_sessions_when_another_team_is_malformed() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let team_root = fixture.path().join("teams");
    let worktree_root = fixture.path().join("worktrees");
    let good_team = team_root.join("watchdog-team");
    let bad_team = team_root.join("partial-team");
    let main_worktree = worktree_root.join("main");
    let child_worktree = worktree_root.join("child");
    for directory in [
        &team_root,
        &worktree_root,
        &good_team,
        &bad_team,
        &main_worktree,
        &child_worktree,
    ] {
        fs::create_dir(directory).expect("fixture directory should be created");
    }
    fs::write(
        good_team.join("config.json"),
        serde_json::to_vec(&json!({
            "name": "watchdog-team",
            "leadSessionId": "lead-session",
            "members": [
                {"agentType": "team-lead", "name": "lead", "cwd": main_worktree, "isActive": true},
                {"agentType": "developer", "name": "rust-worker", "agentId": "child-session", "cwd": child_worktree, "isActive": true}
            ]
        }))
        .expect("fixture JSON should serialize"),
    )
    .expect("team config should be written");
    fs::write(bad_team.join("config.json"), b"{partial")
        .expect("malformed config should be written");

    let database = fixture.path().join("watchdog.db");
    let store = WatchdogStore::open(&database)
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(10_000),
        5_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let discovery = ClaudeTeamDiscovery::new(api);

    let report = discovery.reconcile(&[team_root], &[worktree_root]).await;
    assert_eq!(report.main_sessions(), 1);
    assert_eq!(report.child_sessions(), 1);
    assert_eq!(report.warning_count(), 1);

    let sessions = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("main sessions should query");
    assert_eq!(sessions.len(), 1);
    let dashboard = DashboardService::new(store, clock)
        .snapshot(DashboardQuery::default())
        .await
        .expect("dashboard should project discovered sessions");
    assert_eq!(dashboard.sessions.len(), 1);
    assert_eq!(dashboard.sessions[0].title, "main");
    assert_eq!(
        dashboard.sessions[0].startup_directory,
        main_worktree.to_string_lossy()
    );
    assert_eq!(dashboard.sessions[0].child_counts.values().sum::<u32>(), 1);
}
