//! Automatic runtime discovery and best-effort reconciliation acceptance tests.

use std::{fs, sync::Arc};

use serde_json::json;
use watchdog_domain::{DetailedState, DurationMs, SessionKind, TimePoint, WallTimeMs};
use watchdog_server::{
    AgentApi, ClaudeTeamDiscovery, CompanionDiscovery, DashboardQuery, DashboardService,
    WorktreePathMapping,
};
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
    let native_worktree_root = std::path::PathBuf::from("/host/repositories");
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
                {"agentType": "team-lead", "name": "lead", "cwd": native_worktree_root.join("main"), "isActive": true},
                {"agentType": "developer", "name": "rust-worker", "agentId": "child-session", "cwd": native_worktree_root.join("child"), "isActive": true}
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

    let mapping = WorktreePathMapping::new(native_worktree_root.clone(), worktree_root.clone())
        .expect("path mapping should be valid");
    let report = discovery.reconcile(&[team_root], &[mapping]).await;
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
        native_worktree_root.join("main").to_string_lossy()
    );
    assert_eq!(dashboard.sessions[0].child_counts.values().sum::<u32>(), 1);
}

#[tokio::test]
async fn companion_discovery_reconciles_summary_without_optional_registration_or_detail() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let companion_root = fixture.path().join("companion");
    let workspace_state = companion_root.join("workspace-state");
    let malformed_state = companion_root.join("malformed-state");
    let mounted_worktrees = fixture.path().join("worktrees");
    let mounted_job = mounted_worktrees.join("job");
    for directory in [
        &companion_root,
        &workspace_state,
        &malformed_state,
        &mounted_worktrees,
        &mounted_job,
    ] {
        fs::create_dir(directory).expect("fixture directory should be created");
    }
    fs::write(
        workspace_state.join("state.json"),
        serde_json::to_vec(&json!({
            "version": 1,
            "jobs": [{
                "id": "companion-job",
                "sessionId": "claude-parent",
                "workspaceRoot": "/host/repositories/job",
                "title": "Review persistence",
                "status": "running",
                "phase": "verifying",
                "pid": 4242,
                "updatedAt": "2026-07-18T10:00:00Z"
            }]
        }))
        .expect("fixture JSON should serialize"),
    )
    .expect("summary should be written");
    fs::write(malformed_state.join("state.json"), b"{partial")
        .expect("malformed summary should be written");

    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(20_000),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let discovery = CompanionDiscovery::new(api, clock.clone());
    let mapping = WorktreePathMapping::new("/host/repositories", mounted_worktrees)
        .expect("path mapping should be valid");

    let report = discovery
        .reconcile(
            std::slice::from_ref(&companion_root),
            std::slice::from_ref(&mapping),
        )
        .await;
    assert_eq!(report.main_sessions(), 1);
    assert_eq!(report.child_sessions(), 1);
    assert_eq!(report.warning_count(), 1);

    clock.advance(DurationMs::new(1_000));
    let repeated = discovery
        .reconcile(
            std::slice::from_ref(&companion_root),
            std::slice::from_ref(&mapping),
        )
        .await;
    assert_eq!(repeated.main_sessions(), 1);
    assert_eq!(repeated.child_sessions(), 1);
    assert_eq!(repeated.warning_count(), 1);

    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(children.len(), 1);
    let snapshot = store
        .snapshot(children[0].session)
        .await
        .expect("snapshot should query")
        .expect("snapshot should exist");
    assert_eq!(
        snapshot
            .reducer_snapshot()
            .expect("reducer snapshot should be retained")
            .state(),
        DetailedState::Running
    );
    let metadata = store
        .session_metadata(children[0].session)
        .await
        .expect("metadata should query")
        .expect("metadata should exist");
    assert_eq!(metadata.title(), Some("Review persistence"));
    assert_eq!(metadata.startup_directory(), Some("/host/repositories/job"));
}
