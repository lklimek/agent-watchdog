//! Automatic runtime discovery and best-effort reconciliation acceptance tests.

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use sqlx::sqlite::SqliteConnectOptions;
use watchdog_domain::{
    DetailedState, DurationMs, RuntimeKind, SessionId, SessionKind, TimePoint, WallTimeMs,
};
use watchdog_server::{
    AgentApi, ClaudeDiscovery, CodexDiscovery, CompanionDiscovery, DashboardQuery,
    DashboardService, DiscoveredSession, DiscoveryAliasRegistry, WorktreePathMapping,
};
use watchdog_store::WatchdogStore;
use watchdog_testkit::FakeClock;

#[tokio::test]
async fn claude_team_discovery_keeps_good_sessions_when_another_team_is_malformed() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let team_root = fixture.path().join("teams");
    let task_root = fixture.path().join("tasks");
    let team_tasks = task_root.join("watchdog-team");
    let worktree_root = fixture.path().join("worktrees");
    let good_team = team_root.join("watchdog-team");
    let bad_team = team_root.join("partial-team");
    let main_worktree = worktree_root.join("main");
    let child_worktree = worktree_root.join("child");
    let native_worktree_root = std::path::PathBuf::from("/host/repositories");
    for directory in [
        &team_root,
        &team_tasks,
        &worktree_root,
        &good_team,
        &bad_team,
        &main_worktree,
        &child_worktree,
    ] {
        fs::create_dir_all(directory).expect("fixture directory should be created");
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
    fs::write(
        team_tasks.join("1.json"),
        b"{\"id\":\"1\",\"subject\":\"Implement parser\",\"status\":\"in_progress\",\"owner\":\"rust-worker\"}",
    )
    .expect("team task should be written");

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
    let discovery = ClaudeDiscovery::new(api, store.clone(), clock.clone());

    let mapping = WorktreePathMapping::new(native_worktree_root.clone(), worktree_root.clone())
        .expect("path mapping should be valid");
    let report = discovery
        .reconcile(
            &[team_root.clone(), task_root.clone()],
            &[],
            std::slice::from_ref(&mapping),
        )
        .await;
    assert_eq!(report.main_sessions(), 1);
    assert_eq!(report.child_sessions(), 1);
    assert_eq!(report.warning_count(), 1);

    let sessions = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("main sessions should query");
    assert_eq!(sessions.len(), 1);
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(children.len(), 1);
    assert_session_state(&store, children[0].session, DetailedState::Running).await;
    fs::write(
        team_tasks.join("1.json"),
        b"{\"id\":\"1\",\"subject\":\"Implement parser\",\"status\":\"completed\",\"owner\":\"rust-worker\"}",
    )
    .expect("team task should update");
    clock.advance(DurationMs::new(1));
    let completed = discovery
        .reconcile(&[team_root, task_root], &[], std::slice::from_ref(&mapping))
        .await;
    assert_eq!(completed.warning_count(), 1);
    assert_session_state(&store, children[0].session, DetailedState::Completed).await;
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
async fn claude_project_discovery_tails_main_and_subagent_transcripts_incrementally() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let (projects_root, mounted_worktrees, child_transcript) =
        create_claude_transcript_fixtures(fixture.path());

    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let now_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be valid")
            .as_millis(),
    )
    .expect("fixture time should fit");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(now_ms),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let discovery = ClaudeDiscovery::new(api, store.clone(), clock.clone());
    let runtime_mapping =
        WorktreePathMapping::new("/home/test/.claude/projects", projects_root.clone())
            .expect("runtime path mapping should be valid");
    let worktree_mapping = WorktreePathMapping::new("/host/repositories", mounted_worktrees)
        .expect("worktree mapping should be valid");

    let report = reconcile_claude_fixture(
        &discovery,
        &projects_root,
        &runtime_mapping,
        &worktree_mapping,
    )
    .await;
    assert_eq!(report.main_sessions(), 1);
    assert_eq!(report.child_sessions(), 1);
    assert_eq!(report.warning_count(), 0);

    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(children.len(), 1);
    let metadata = store
        .session_metadata(children[0].session)
        .await
        .expect("metadata should query")
        .expect("metadata should exist");
    assert_eq!(metadata.title(), Some("security-reviewer"));
    assert_eq!(
        metadata.startup_directory(),
        Some("/host/repositories/child")
    );
    assert_eq!(metadata.branch(), Some("feat/claude"));

    let mut transcript = fs::OpenOptions::new()
        .append(true)
        .open(child_transcript)
        .expect("child transcript should reopen");
    transcript
        .write_all(
            b"{\"type\":\"assistant\",\"sessionId\":\"main-session\",\"agentId\":\"child-1\",\"cwd\":\"/host/repositories/child\",\"message\":{\"content\":\"FUTURE_SECRET_CONTENT\"}}\n",
        )
        .expect("future activity should append");
    clock.advance(DurationMs::new(1_000));

    let appended = reconcile_claude_fixture(
        &discovery,
        &projects_root,
        &runtime_mapping,
        &worktree_mapping,
    )
    .await;
    assert_eq!(appended.warning_count(), 0);
    let snapshot = load_snapshot(&store, children[0].session).await;
    assert_eq!(
        snapshot
            .reducer_snapshot()
            .expect("reducer snapshot should exist")
            .last_progress_summary(),
        Some("Claude transcript activity")
    );

    transcript
        .write_all(b"{\"type\":\"future-record\",\"sessionId\":\"main-session\",\"agentId\":\"child-1\"}\n")
        .expect("future schema record should append");
    clock.advance(DurationMs::new(1_000));
    let drifted = reconcile_claude_fixture(
        &discovery,
        &projects_root,
        &runtime_mapping,
        &worktree_mapping,
    )
    .await;
    assert_eq!(drifted.warning_count(), 1);
    let drifted_snapshot = load_snapshot(&store, children[0].session).await;
    assert_eq!(
        drifted_snapshot
            .reducer_snapshot()
            .expect("reducer snapshot should exist")
            .compatibility_warning()
            .expect("schema drift should be actionable")
            .badge(),
        "UPGRADE"
    );
}

#[tokio::test]
async fn claude_team_member_transcript_does_not_create_a_duplicate_main() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let (projects, teams, worktrees, teammate_transcript) =
        create_claude_team_alias_fixtures(fixture.path());
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms()),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let discovery = ClaudeDiscovery::new(api.clone(), store.clone(), clock.clone());
    let runtime_mappings = [
        WorktreePathMapping::new("/home/test/.claude/projects", projects.clone())
            .expect("projects mapping should be valid"),
        WorktreePathMapping::new("/home/test/.claude/teams", teams.clone())
            .expect("teams mapping should be valid"),
    ];
    let worktree_mapping = WorktreePathMapping::new("/host/repositories", worktrees)
        .expect("worktree mapping should be valid");

    let report = discovery
        .reconcile(
            &[projects.clone(), teams.clone()],
            &runtime_mappings,
            std::slice::from_ref(&worktree_mapping),
        )
        .await;
    assert_eq!(report.main_sessions(), 1);
    assert_eq!(report.child_sessions(), 1);
    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(mains.len(), 1);
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].native.native_id(), "team-worker");

    append_claude_activity(&teammate_transcript, "teammate-session", None);
    clock.advance(DurationMs::new(1_000));
    let restarted = ClaudeDiscovery::new(api, store.clone(), clock.clone());
    restarted
        .reconcile(
            &[projects, teams],
            &runtime_mappings,
            std::slice::from_ref(&worktree_mapping),
        )
        .await;
    let snapshot = load_snapshot(&store, children[0].session).await;
    assert_eq!(
        snapshot
            .reducer_snapshot()
            .expect("reducer snapshot should exist")
            .last_progress_summary(),
        Some("Claude transcript activity")
    );
}

#[tokio::test]
async fn claude_team_lead_aliases_a_post_reset_transcript_by_unique_cwd() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let (projects, teams, worktrees, _) = create_claude_team_alias_fixtures(fixture.path());
    let config = teams.join("session-main/config.json");
    let mut team: serde_json::Value =
        serde_json::from_slice(&fs::read(&config).expect("team config should read"))
            .expect("team config should parse");
    team["leadSessionId"] = json!("retained-team-lead");
    fs::write(
        &config,
        serde_json::to_vec(&team).expect("team config should serialize"),
    )
    .expect("team config should update");

    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms()),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let discovery = ClaudeDiscovery::new(api, store.clone(), clock);
    let runtime_mappings = [
        WorktreePathMapping::new("/home/test/.claude/projects", projects.clone())
            .expect("projects mapping should be valid"),
        WorktreePathMapping::new("/home/test/.claude/teams", teams.clone())
            .expect("teams mapping should be valid"),
    ];
    let worktree_mapping = WorktreePathMapping::new("/host/repositories", worktrees)
        .expect("worktree mapping should be valid");

    discovery
        .reconcile(
            &[projects, teams],
            &runtime_mappings,
            std::slice::from_ref(&worktree_mapping),
        )
        .await;

    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    assert_eq!(mains.len(), 1);
    assert_eq!(mains[0].native.native_id(), "retained-team-lead");
}

#[tokio::test]
async fn ambiguous_claude_team_lead_cwd_does_not_guess_an_alias() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let projects = fixture.path().join("projects");
    let project = projects.join("-host-main");
    let teams = fixture.path().join("teams");
    let worktrees = fixture.path().join("worktrees");
    fs::create_dir_all(&project).expect("project directory should exist");
    fs::create_dir_all(worktrees.join("main")).expect("worktree should exist");
    fs::write(
        project.join("current-transcript.jsonl"),
        b"{\"type\":\"assistant\",\"sessionId\":\"current-transcript\",\"cwd\":\"/host/main\"}\n",
    )
    .expect("transcript should write");
    for lead in ["retained-one", "retained-two"] {
        let team = teams.join(lead);
        fs::create_dir_all(&team).expect("team directory should exist");
        fs::write(
            team.join("config.json"),
            serde_json::to_vec(&json!({
                "leadSessionId": lead,
                "members": [{"agentType":"team-lead","name":"lead","cwd":"/host/main"}]
            }))
            .expect("team config should serialize"),
        )
        .expect("team config should write");
    }
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms()),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let runtime_mappings = [
        WorktreePathMapping::new("/runtime/projects", projects.clone())
            .expect("project mapping should be valid"),
        WorktreePathMapping::new("/runtime/teams", teams.clone())
            .expect("team mapping should be valid"),
    ];
    let worktree_mapping =
        WorktreePathMapping::new("/host", worktrees).expect("worktree mapping should be valid");

    ClaudeDiscovery::new(api, store.clone(), clock)
        .reconcile(
            &[projects, teams],
            &runtime_mappings,
            std::slice::from_ref(&worktree_mapping),
        )
        .await;
    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    assert_eq!(mains.len(), 3);
    assert!(
        mains
            .iter()
            .any(|main| main.native.native_id() == "current-transcript")
    );
}

#[tokio::test]
async fn inactive_claude_team_member_is_terminal_instead_of_stalling() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let teams = fixture.path().join("teams");
    let team = teams.join("watchdog-team");
    let worktrees = fixture.path().join("worktrees");
    for directory in [&team, &worktrees.join("main"), &worktrees.join("child")] {
        fs::create_dir_all(directory).expect("fixture directory should exist");
    }
    fs::write(
        team.join("config.json"),
        serde_json::to_vec(&json!({
            "name": "watchdog-team",
            "leadSessionId": "lead",
            "members": [
                {"agentType":"team-lead","name":"lead","cwd":"/host/main"},
                {"agentType":"developer","name":"done","agentId":"child","cwd":"/host/child","isActive":false}
            ]
        }))
        .expect("team config should serialize"),
    )
    .expect("team config should write");
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms()),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let discovery = ClaudeDiscovery::new(api, store.clone(), clock);
    let mapping =
        WorktreePathMapping::new("/host", worktrees).expect("worktree mapping should be valid");

    discovery
        .reconcile(&[teams], &[], std::slice::from_ref(&mapping))
        .await;
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(children.len(), 1);
    assert_session_state(&store, children[0].session, DetailedState::Completed).await;
}

#[tokio::test]
async fn stale_claude_team_config_does_not_bootstrap_historical_sessions() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let teams = fixture.path().join("teams");
    let team = teams.join("historical-team");
    fs::create_dir_all(&team).expect("team directory should exist");
    fs::write(
        team.join("config.json"),
        br#"{"leadSessionId":"old-lead","members":[]}"#,
    )
    .expect("team config should write");
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let two_days_ms = 2 * 24 * 60 * 60 * 1_000;
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms() + two_days_ms),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");

    ClaudeDiscovery::new(api, store.clone(), clock)
        .reconcile(&[teams], &[], &[])
        .await;
    assert!(
        store
            .sessions_by_kind(SessionKind::Main, 10)
            .await
            .expect("mains should query")
            .is_empty()
    );
}

#[tokio::test]
async fn claude_originated_codex_rollout_joins_unique_repository_main() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let mounted = fixture.path().join("mounted");
    let claude_repo = mounted.join("repositories/main");
    let codex_worktree = mounted.join("worktrees/child");
    let rollout_root = fixture.path().join("codex-rollouts");
    for directory in [&claude_repo, &codex_worktree, &rollout_root] {
        fs::create_dir_all(directory).expect("fixture directory should exist");
    }
    fs::create_dir_all(claude_repo.join(".git")).expect("git metadata should exist");
    fs::write(
        claude_repo.join(".git/config"),
        b"[remote \"origin\"]\n\turl = git@github.com:example/project.git\n",
    )
    .expect("git config should write");
    fs::write(
        rollout_root.join("rollout-claude-child.jsonl"),
        b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-child\",\"cwd\":\"/host/worktrees/child\",\"originator\":\"Claude Code\",\"source\":\"vscode\",\"git\":{\"repository_url\":\"https://github.com/example/project.git\"}}}\n",
    )
    .expect("rollout should write");

    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms()),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let main = api
        .discover_session(DiscoveredSession {
            runtime: RuntimeKind::ClaudeCode,
            native_id: "claude-main".to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: "claude-main-discovery".to_owned(),
            adapter_version: "test".to_owned(),
            evidence_source: "test".to_owned(),
            title: Some("Claude coordinator".to_owned()),
            startup_directory: Some("/host/repositories/main".to_owned()),
        })
        .await
        .expect("Claude main should register");
    let rollout_mapping = WorktreePathMapping::new("/state", rollout_root.clone())
        .expect("rollout mapping should be valid");
    let worktree_mapping =
        WorktreePathMapping::new("/host", mounted).expect("worktree mapping should be valid");

    CodexDiscovery::new(api, store.clone(), clock)
        .reconcile(
            &[rollout_root],
            std::slice::from_ref(&rollout_mapping),
            std::slice::from_ref(&worktree_mapping),
        )
        .await;
    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(mains.len(), 1);
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].native.runtime(), RuntimeKind::CodexCli);
    assert_eq!(children[0].root.session_id(), main.root.session_id());
    assert_eq!(
        SessionId::from_native(&children[0].native),
        children[0].session.session_id()
    );
}

#[tokio::test]
async fn claude_originated_codex_rollout_does_not_guess_between_two_parents() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let mounted = fixture.path().join("mounted");
    let worktree = mounted.join("main");
    let rollouts = fixture.path().join("rollouts");
    fs::create_dir_all(&worktree).expect("worktree should exist");
    fs::create_dir_all(&rollouts).expect("rollout root should exist");
    fs::write(
        rollouts.join("rollout-ambiguous.jsonl"),
        b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-ambiguous\",\"cwd\":\"/host/main\",\"originator\":\"Claude Code\",\"source\":\"vscode\"}}\n",
    )
    .expect("rollout should write");
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms()),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    for native_id in ["claude-one", "claude-two"] {
        api.discover_session(DiscoveredSession {
            runtime: RuntimeKind::ClaudeCode,
            native_id: native_id.to_owned(),
            kind: SessionKind::Main,
            parent: None,
            event_key: format!("discover-{native_id}"),
            adapter_version: "test".to_owned(),
            evidence_source: "test".to_owned(),
            title: None,
            startup_directory: Some("/host/main".to_owned()),
        })
        .await
        .expect("Claude main should register");
    }
    let rollout_mapping = WorktreePathMapping::new("/state", rollouts.clone())
        .expect("rollout mapping should be valid");
    let worktree_mapping =
        WorktreePathMapping::new("/host", mounted).expect("worktree mapping should be valid");

    CodexDiscovery::new(api, store.clone(), clock)
        .reconcile(
            &[rollouts],
            std::slice::from_ref(&rollout_mapping),
            std::slice::from_ref(&worktree_mapping),
        )
        .await;
    assert_eq!(
        store
            .sessions_by_kind(SessionKind::Main, 10)
            .await
            .expect("mains should query")
            .len(),
        3
    );
    assert!(
        store
            .sessions_by_kind(SessionKind::Child, 10)
            .await
            .expect("children should query")
            .is_empty()
    );
}

#[tokio::test]
async fn companion_wrapper_parent_reuses_the_claude_team_member_alias() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let projects = fixture.path().join("projects");
    let project = projects.join("-host-repositories-main");
    let teams = fixture.path().join("teams");
    let team = teams.join("session-main");
    let companion = fixture.path().join("companion/workspace");
    let worktrees = fixture.path().join("worktrees");
    for directory in [&project, &team, &companion, &worktrees.join("main")] {
        fs::create_dir_all(directory).expect("fixture directory should exist");
    }
    fs::write(
        project.join("wrapper-session.jsonl"),
        b"{\"type\":\"agent-setting\",\"agentSetting\":\"codex:codex-rescue\",\"sessionId\":\"wrapper-session\"}\n{\"type\":\"assistant\",\"sessionId\":\"wrapper-session\",\"cwd\":\"/host/repositories/main\"}\n",
    )
    .expect("wrapper transcript should write");
    fs::write(
        team.join("config.json"),
        serde_json::to_vec(&json!({
            "name": "session-main",
            "leadSessionId": "lead",
            "members": [
                {"agentType":"team-lead","name":"lead","cwd":"/host/repositories/main"},
                {"agentType":"codex:codex-rescue","name":"codex-worker","agentId":"team-worker","cwd":"/host/repositories/main","isActive":true}
            ]
        }))
        .expect("team config should serialize"),
    )
    .expect("team config should write");
    fs::write(
        companion.join("state.json"),
        serde_json::to_vec(&json!({
            "version": 1,
            "jobs": [{
                "id": "companion-job",
                "sessionId": "wrapper-session",
                "workspaceRoot": "/host/repositories/main",
                "status": "running"
            }]
        }))
        .expect("Companion state should serialize"),
    )
    .expect("Companion state should write");

    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms()),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let aliases = DiscoveryAliasRegistry::default();
    let runtime_mappings = [
        WorktreePathMapping::new("/home/test/.claude/projects", projects.clone())
            .expect("project mapping should be valid"),
        WorktreePathMapping::new("/home/test/.claude/teams", teams.clone())
            .expect("team mapping should be valid"),
    ];
    let worktree_mapping = WorktreePathMapping::new("/host/repositories", worktrees)
        .expect("worktree mapping should be valid");
    ClaudeDiscovery::with_alias_registry(
        api.clone(),
        store.clone(),
        clock.clone(),
        aliases.clone(),
    )
    .reconcile(
        &[projects, teams],
        &runtime_mappings,
        std::slice::from_ref(&worktree_mapping),
    )
    .await;
    CompanionDiscovery::with_alias_registry(api, store.clone(), clock, aliases)
        .reconcile(&[fixture.path().join("companion")], &[])
        .await;

    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(mains.len(), 1);
    assert_eq!(mains[0].native.native_id(), "lead");
    assert_eq!(children.len(), 2);
    assert!(
        children
            .iter()
            .all(|child| child.root.session_id() == mains[0].root.session_id())
    );
}

#[tokio::test]
async fn stale_terminal_companion_job_does_not_bootstrap_history() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let companion = fixture.path().join("companion/workspace");
    let jobs = companion.join("jobs");
    fs::create_dir_all(&jobs).expect("job directory should exist");
    fs::write(
        companion.join("state.json"),
        serde_json::to_vec(&json!({
            "version": 1,
            "jobs": [
                {"id":"old-job","sessionId":"old-wrapper","workspaceRoot":"/work","status":"completed"},
                {"id":"active-job","sessionId":"active-wrapper","workspaceRoot":"/work","status":"running"}
            ]
        }))
        .expect("summary should serialize"),
    )
    .expect("summary should write");
    fs::write(
        jobs.join("old-job.json"),
        br#"{"id":"old-job","sessionId":"old-wrapper","workspaceRoot":"/work","status":"completed"}"#,
    )
    .expect("detail should write");
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms() + 2 * 24 * 60 * 60 * 1_000),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");

    CompanionDiscovery::new(api, store.clone(), clock)
        .reconcile(&[fixture.path().join("companion")], &[])
        .await;

    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(mains.len(), 1);
    assert_eq!(mains[0].native.native_id(), "active-wrapper");
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].native.native_id(), "active-job");
}

async fn load_snapshot(
    store: &WatchdogStore,
    session: watchdog_domain::SessionIdentity,
) -> watchdog_store::SnapshotUpdate {
    store
        .snapshot(session)
        .await
        .expect("snapshot should query")
        .expect("snapshot should exist")
}

async fn assert_session_state(
    store: &WatchdogStore,
    session: watchdog_domain::SessionIdentity,
    expected: DetailedState,
) {
    assert_eq!(
        load_snapshot(store, session)
            .await
            .reducer_snapshot()
            .expect("reducer snapshot should exist")
            .state(),
        expected
    );
}

async fn reconcile_claude_fixture(
    discovery: &ClaudeDiscovery,
    projects_root: &Path,
    runtime_mapping: &WorktreePathMapping,
    worktree_mapping: &WorktreePathMapping,
) -> watchdog_server::RuntimeDiscoveryReport {
    discovery
        .reconcile(
            &[projects_root.to_path_buf()],
            std::slice::from_ref(runtime_mapping),
            std::slice::from_ref(worktree_mapping),
        )
        .await
}

fn create_claude_transcript_fixtures(root: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let projects_root = root.join("projects");
    let project = projects_root.join("-host-repositories-main");
    let subagents = project.join("main-session/subagents");
    let mounted_worktrees = root.join("worktrees");
    for directory in [
        &subagents,
        &mounted_worktrees.join("main"),
        &mounted_worktrees.join("child"),
    ] {
        fs::create_dir_all(directory).expect("fixture directory should be created");
    }
    fs::write(
        project.join("main-session.jsonl"),
        b"{\"type\":\"agent-setting\",\"agentSetting\":\"claudius:claudius\",\"sessionId\":\"main-session\"}\n{\"type\":\"assistant\",\"sessionId\":\"main-session\",\"cwd\":\"/host/repositories/main\",\"gitBranch\":\"feat/claude\",\"message\":{\"content\":\"SECRET_TRANSCRIPT_CONTENT\"}}\n",
    )
    .expect("main transcript should be written");
    let child_transcript = subagents.join("agent-child-1.jsonl");
    fs::write(
        &child_transcript,
        b"{\"type\":\"assistant\",\"sessionId\":\"main-session\",\"agentId\":\"child-1\",\"cwd\":\"/host/repositories/child\",\"gitBranch\":\"feat/claude\",\"message\":{\"content\":\"SECRET_TRANSCRIPT_CONTENT\"}}\n",
    )
    .expect("child transcript should be written");
    fs::write(
        subagents.join("agent-child-1.meta.json"),
        b"{\"agentType\":\"security-reviewer\",\"description\":\"SECRET_TRANSCRIPT_CONTENT\"}",
    )
    .expect("child metadata should be written");
    (projects_root, mounted_worktrees, child_transcript)
}

fn create_claude_team_alias_fixtures(root: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let projects = root.join("projects");
    let project = projects.join("-host-repositories-main");
    let teams = root.join("teams");
    let team = teams.join("session-main");
    let worktrees = root.join("worktrees");
    for directory in [
        &project,
        &team,
        &worktrees.join("main"),
        &worktrees.join("worker"),
    ] {
        fs::create_dir_all(directory).expect("fixture directory should be created");
    }
    fs::write(
        project.join("main-session.jsonl"),
        b"{\"type\":\"assistant\",\"sessionId\":\"main-session\",\"cwd\":\"/host/repositories/main\"}\n",
    )
    .expect("lead transcript should be written");
    let teammate = project.join("teammate-session.jsonl");
    fs::write(
        &teammate,
        b"{\"type\":\"agent-setting\",\"agentSetting\":\"claudius:developer\",\"sessionId\":\"teammate-session\"}\n{\"type\":\"assistant\",\"sessionId\":\"teammate-session\",\"cwd\":\"/host/repositories/worker\"}\n",
    )
    .expect("teammate transcript should be written");
    fs::write(
        team.join("config.json"),
        serde_json::to_vec(&json!({
            "leadSessionId": "main-session",
            "members": [
                {"agentType": "team-lead", "name": "lead", "cwd": "/host/repositories/main", "isActive": true},
                {"agentType": "claudius:developer", "name": "worker", "agentId": "team-worker", "cwd": "/host/repositories/worker", "isActive": true}
            ]
        }))
        .expect("team config should serialize"),
    )
    .expect("team config should be written");
    (projects, teams, worktrees, teammate)
}

fn append_claude_activity(path: &Path, session: &str, agent: Option<&str>) {
    let mut transcript = fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("transcript should reopen");
    let record = json!({
        "type": "assistant",
        "sessionId": session,
        "agentId": agent,
        "message": {"content": "FUTURE_SECRET_CONTENT"}
    });
    serde_json::to_writer(&mut transcript, &record).expect("activity should serialize");
    transcript.write_all(b"\n").expect("newline should append");
}

fn current_time_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be valid")
            .as_millis(),
    )
    .expect("fixture time should fit")
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
    let state_path = workspace_state.join("state.json");
    write_companion_summary(&state_path);
    fs::write(malformed_state.join("state.json"), b"{partial")
        .expect("malformed summary should be written");
    let jobs = workspace_state.join("jobs");
    fs::create_dir(&jobs).expect("job directory should be created");
    let log_path = jobs.join("companion-job.log");
    fs::write(&log_path, b"existing private output\n").expect("job log should be written");

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
    let discovery = CompanionDiscovery::new(api, store.clone(), clock.clone());
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

    update_companion_summary(&state_path);
    write_companion_detail(&workspace_state);
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
    let reducer = snapshot
        .reducer_snapshot()
        .expect("reducer snapshot should be retained");
    assert_eq!(reducer.state(), DetailedState::Running);
    #[cfg(target_os = "linux")]
    assert_eq!(
        reducer
            .process_identity()
            .expect("fresh native PID should be verified")
            .pid()
            .value(),
        std::process::id()
    );
    let metadata = store
        .session_metadata(children[0].session)
        .await
        .expect("metadata should query")
        .expect("metadata should exist");
    assert_eq!(metadata.title(), Some("Detailed persistence review"));
    assert_eq!(metadata.startup_directory(), None);

    assert_companion_log_activity(
        &discovery,
        &store,
        &clock,
        &companion_root,
        &mapping,
        &log_path,
        children[0].session,
    )
    .await;
}

#[tokio::test]
async fn companion_discovery_keeps_jobs_from_distinct_wrapper_sessions() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let companion_root = fixture.path().join("companion");
    let workspace_state = companion_root.join("workspace-state");
    fs::create_dir_all(&workspace_state).expect("workspace state should exist");
    fs::write(
        workspace_state.join("state.json"),
        serde_json::to_vec(&json!({
            "version": 1,
            "jobs": [
                {
                    "id": "companion-job-a",
                    "sessionId": "wrapper-session-a",
                    "workspaceRoot": "/host/repositories/coordinator",
                    "status": "running"
                },
                {
                    "id": "companion-job-b",
                    "sessionId": "wrapper-session-b",
                    "workspaceRoot": "/host/repositories/coordinator",
                    "status": "running"
                }
            ]
        }))
        .expect("fixture JSON should serialize"),
    )
    .expect("summary should be written");

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
    let discovery = CompanionDiscovery::new(api, store.clone(), clock);

    let report = discovery.reconcile(&[companion_root], &[]).await;
    assert_eq!(report.main_sessions(), 2);
    assert_eq!(report.child_sessions(), 2);
    assert_eq!(report.warning_count(), 0);

    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(mains.len(), 2);
    assert_eq!(children.len(), 2);
    assert!(
        children
            .iter()
            .all(|child| mains.iter().any(|main| main.root == child.root)),
        "each Companion job must remain attached to its own exact wrapper session"
    );
}

fn write_companion_summary(path: &Path) {
    fs::write(
        path,
        serde_json::to_vec(&json!({
            "version": 1,
            "jobs": [{
                "id": "companion-job",
                "sessionId": "claude-parent",
                "workspaceRoot": "/host/repositories/job",
                "title": "Review persistence",
                "status": "running",
                "phase": "verifying",
                "pid": std::process::id(),
                "updatedAt": "2026-07-18T10:00:00Z"
            }]
        }))
        .expect("fixture JSON should serialize"),
    )
    .expect("summary should be written");
}

async fn assert_companion_log_activity(
    discovery: &CompanionDiscovery,
    store: &WatchdogStore,
    clock: &FakeClock,
    companion_root: &PathBuf,
    mapping: &WorktreePathMapping,
    log_path: &Path,
    session: watchdog_domain::SessionIdentity,
) {
    let before_log = load_snapshot(store, session)
        .await
        .reducer_snapshot()
        .expect("reducer snapshot should be retained")
        .last_activity();
    fs::OpenOptions::new()
        .append(true)
        .open(log_path)
        .expect("job log should reopen")
        .write_all(b"SECRET_COMPANION_OUTPUT\n")
        .expect("job log should append");
    clock.advance(DurationMs::new(1_000));
    let report = discovery
        .reconcile(
            std::slice::from_ref(companion_root),
            std::slice::from_ref(mapping),
        )
        .await;
    assert_eq!(report.warning_count(), 1);
    let after_log = load_snapshot(store, session)
        .await
        .reducer_snapshot()
        .expect("reducer snapshot should be retained")
        .clone();
    assert!(after_log.last_activity().monotonic_ms() > before_log.monotonic_ms());
    assert_eq!(
        after_log.last_progress_summary(),
        Some("Codex Companion log activity")
    );
    assert!(!format!("{after_log:?}").contains("SECRET_COMPANION_OUTPUT"));
}

fn update_companion_summary(path: &std::path::Path) {
    let updated = fs::read_to_string(path)
        .expect("summary should remain readable")
        .replace("2026-07-18T10:00:00Z", "2026-07-18T10:01:00Z");
    fs::write(path, updated).expect("summary update should be written");
}

fn write_companion_detail(workspace_state: &std::path::Path) {
    let jobs = workspace_state.join("jobs");
    fs::create_dir_all(&jobs).expect("detail directory should exist");
    fs::write(
        jobs.join("companion-job.json"),
        serde_json::to_vec(&json!({
            "id": "companion-job",
            "sessionId": "claude-parent",
            "workspaceRoot": "/host/repositories/job",
            "title": "Detailed persistence review",
            "status": "running",
            "phase": "verifying",
            "pid": std::process::id(),
            "updatedAt": "2026-07-18T10:01:00Z"
        }))
        .expect("detail JSON should serialize"),
    )
    .expect("detail should be written");
}

#[tokio::test]
async fn codex_rollout_metadata_discovers_live_hierarchy_without_sqlite_visibility() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let rollout_root = fixture.path().join("codex-rollouts");
    let mounted_worktrees = fixture.path().join("worktrees");
    for directory in [
        &rollout_root,
        &mounted_worktrees,
        &mounted_worktrees.join("main"),
        &mounted_worktrees.join("child"),
    ] {
        fs::create_dir(directory).expect("fixture directory should be created");
    }
    create_rollout_metadata_fixtures(&rollout_root);
    let now_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should follow epoch")
            .as_millis(),
    )
    .expect("current time should fit");
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(now_ms),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let discovery = CodexDiscovery::new(api, store.clone(), clock.clone());
    let mapping = WorktreePathMapping::new("/host/repositories", mounted_worktrees)
        .expect("path mapping should be valid");
    let rollout_mapping = WorktreePathMapping::new("/state", &rollout_root)
        .expect("rollout path mapping should be valid");

    let report = discovery
        .reconcile(
            std::slice::from_ref(&rollout_root),
            std::slice::from_ref(&rollout_mapping),
            std::slice::from_ref(&mapping),
        )
        .await;
    assert_eq!(report.main_sessions(), 1);
    assert_eq!(report.child_sessions(), 1);
    assert_eq!(report.warning_count(), 0);

    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(mains.len(), 1);
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].root, mains[0].root);
    let child_metadata = store
        .session_metadata(children[0].session)
        .await
        .expect("metadata should query")
        .expect("metadata should exist");
    assert_eq!(child_metadata.title(), Some("reviewer"));
    assert_eq!(
        child_metadata.startup_directory(),
        Some("/host/repositories/child")
    );

    append_rollout_activity(&rollout_root.join("rollout-child.jsonl"));
    clock.advance(DurationMs::new(1_000));
    let appended = discovery
        .reconcile(
            std::slice::from_ref(&rollout_root),
            std::slice::from_ref(&rollout_mapping),
            std::slice::from_ref(&mapping),
        )
        .await;
    assert_eq!(appended.warning_count(), 0);
    let child_snapshot = store
        .snapshot(children[0].session)
        .await
        .expect("child snapshot should query")
        .expect("child snapshot should exist");
    assert_eq!(
        child_snapshot
            .reducer_snapshot()
            .expect("reducer snapshot should exist")
            .last_progress_summary(),
        Some("Codex rollout activity")
    );

    append_rollout_completion(&rollout_root.join("rollout-child.jsonl"));
    clock.advance(DurationMs::new(1_000));
    discovery
        .reconcile(
            std::slice::from_ref(&rollout_root),
            std::slice::from_ref(&rollout_mapping),
            std::slice::from_ref(&mapping),
        )
        .await;
    assert_session_state(&store, children[0].session, DetailedState::Completed).await;
}

#[tokio::test]
async fn codex_rollout_bootstrap_recovers_a_terminal_tail_record() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let rollouts = fixture.path().join("rollouts");
    let worktrees = fixture.path().join("worktrees");
    fs::create_dir_all(&rollouts).expect("rollout root should exist");
    fs::create_dir_all(worktrees.join("main")).expect("worktree should exist");
    fs::write(
        rollouts.join("rollout-completed.jsonl"),
        b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"completed-thread\",\"cwd\":\"/host/main\",\"source\":\"vscode\"}}\n{\"type\":\"response_item\",\"payload\":{\"content\":\"SECRET_TRANSCRIPT_BODY\"}}\n{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"last_agent_message\":\"SECRET_RESULT\"}}\n",
    )
    .expect("rollout should write");
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms()),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let rollout_mapping = WorktreePathMapping::new("/state", rollouts.clone())
        .expect("rollout mapping should be valid");
    let worktree_mapping =
        WorktreePathMapping::new("/host", worktrees).expect("worktree mapping should be valid");

    CodexDiscovery::new(api, store.clone(), clock)
        .reconcile(
            &[rollouts],
            std::slice::from_ref(&rollout_mapping),
            std::slice::from_ref(&worktree_mapping),
        )
        .await;
    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    assert_eq!(mains.len(), 1);
    assert_session_state(&store, mains[0].session, DetailedState::Completed).await;
    assert!(!format!("{:?}", load_snapshot(&store, mains[0].session).await).contains("SECRET"));
}

#[tokio::test]
async fn codex_rollout_bootstrap_ignores_a_terminal_record_without_newline_boundary() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let rollouts = fixture.path().join("rollouts");
    let worktrees = fixture.path().join("worktrees");
    fs::create_dir_all(&rollouts).expect("rollout root should exist");
    fs::create_dir_all(worktrees.join("main")).expect("worktree should exist");
    fs::write(
        rollouts.join("rollout-partial.jsonl"),
        b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"partial-thread\",\"cwd\":\"/host/main\",\"source\":\"vscode\"}}\n{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\"}}",
    )
    .expect("rollout should write");
    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(current_time_ms()),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let rollout_mapping = WorktreePathMapping::new("/state", rollouts.clone())
        .expect("rollout mapping should be valid");
    let worktree_mapping =
        WorktreePathMapping::new("/host", worktrees).expect("worktree mapping should be valid");

    CodexDiscovery::new(api, store.clone(), clock)
        .reconcile(
            &[rollouts],
            std::slice::from_ref(&rollout_mapping),
            std::slice::from_ref(&worktree_mapping),
        )
        .await;
    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    assert_eq!(mains.len(), 1);
    assert_session_state(&store, mains[0].session, DetailedState::Starting).await;
}

#[tokio::test]
async fn codex_discovery_selects_recent_unarchived_threads_and_exact_spawn_edges() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let state_root = fixture.path().join("codex-state");
    let rollout_root = fixture.path().join("codex-rollouts");
    let mounted_worktrees = fixture.path().join("worktrees");
    for directory in [
        &state_root,
        &rollout_root,
        &mounted_worktrees,
        &mounted_worktrees.join("main"),
        &mounted_worktrees.join("child"),
        &mounted_worktrees.join("old"),
    ] {
        fs::create_dir(directory).expect("fixture directory should be created");
    }
    create_rollout_fixtures(&rollout_root);
    let now_ms = 2_000_000_000_000_i64;
    create_codex_state(&state_root.join("state_5.sqlite"), now_ms).await;
    set_codex_version(&state_root.join("state_5.sqlite"), "codex-main", "0.999.0").await;

    let store = WatchdogStore::open(&fixture.path().join("watchdog.db"))
        .await
        .expect("store should open");
    let clock = Arc::new(FakeClock::new(TimePoint::new(
        WallTimeMs::new(now_ms),
        10_000,
    )));
    let api = AgentApi::new(store.clone(), clock.clone())
        .await
        .expect("API should initialize");
    let discovery = CodexDiscovery::new(api, store.clone(), clock.clone());
    let mapping = WorktreePathMapping::new("/host/repositories", mounted_worktrees)
        .expect("path mapping should be valid");
    let rollout_mapping = WorktreePathMapping::new("/state", &rollout_root)
        .expect("rollout path mapping should be valid");

    let report = discovery
        .reconcile(
            std::slice::from_ref(&state_root),
            std::slice::from_ref(&rollout_mapping),
            std::slice::from_ref(&mapping),
        )
        .await;
    assert_eq!(report.main_sessions(), 1);
    assert_eq!(report.child_sessions(), 1);
    assert_eq!(report.warning_count(), 0);

    let mains = store
        .sessions_by_kind(SessionKind::Main, 10)
        .await
        .expect("mains should query");
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(mains.len(), 1);
    assert_eq!(children.len(), 1);
    assert!(
        store
            .snapshot(mains[0].session)
            .await
            .expect("main snapshot should query")
            .expect("main snapshot should exist")
            .reducer_snapshot()
            .expect("reducer snapshot should exist")
            .compatibility_warning()
            .is_none(),
        "a different version remains optimistic while its known schema parses"
    );
    let child_metadata = store
        .session_metadata(children[0].session)
        .await
        .expect("metadata should query")
        .expect("metadata should exist");
    assert_eq!(child_metadata.title(), Some("reviewer"));
    assert_eq!(
        child_metadata.startup_directory(),
        Some("/host/repositories/child")
    );
    assert_repository_metadata(&child_metadata);

    append_rollout_activity(&rollout_root.join("codex-child.jsonl"));
    clock.advance(DurationMs::new(1_000));

    let appended = discovery
        .reconcile(
            std::slice::from_ref(&state_root),
            std::slice::from_ref(&rollout_mapping),
            std::slice::from_ref(&mapping),
        )
        .await;
    assert_eq!(appended.warning_count(), 0);
    let child_snapshot = store
        .snapshot(children[0].session)
        .await
        .expect("child snapshot should query")
        .expect("child snapshot should exist");
    assert_eq!(
        child_snapshot
            .reducer_snapshot()
            .expect("reducer snapshot should exist")
            .last_progress_summary(),
        Some("Codex rollout activity")
    );
}

fn create_rollout_fixtures(rollout_root: &std::path::Path) {
    for id in ["codex-main", "codex-child"] {
        fs::write(
            rollout_root.join(format!("{id}.jsonl")),
            b"{\"type\":\"event_msg\",\"payload\":{}}\n",
        )
        .expect("rollout fixture should be written");
    }
}

fn create_rollout_metadata_fixtures(rollout_root: &std::path::Path) {
    fs::write(
        rollout_root.join("rollout-main.jsonl"),
        b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-main\",\"cwd\":\"/host/repositories/main\",\"agent_nickname\":\"Main\",\"source\":{}}}\n",
    )
    .expect("main rollout fixture should be written");
    fs::write(
        rollout_root.join("rollout-child.jsonl"),
        b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-child\",\"parent_thread_id\":\"codex-main\",\"cwd\":\"/host/repositories/child\",\"agent_nickname\":\"reviewer\",\"source\":{\"subagent\":{}}}}\n",
    )
    .expect("child rollout fixture should be written");
}

fn assert_repository_metadata(metadata: &watchdog_store::SessionMetadataRecord) {
    assert_eq!(metadata.branch(), Some("feat/watchdog"));
    assert_eq!(
        metadata.repository_remote(),
        Some("https://github.com/lklimek/agent-watchdog.git")
    );
}

fn append_rollout_activity(path: &std::path::Path) {
    let mut rollout = fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("rollout should reopen for append");
    rollout
        .write_all(b"{\"type\":\"event_msg\",\"payload\":{}}\n")
        .expect("new rollout activity should append");
}

fn append_rollout_completion(path: &std::path::Path) {
    let mut rollout = fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("rollout should reopen for append");
    rollout
        .write_all(
            b"{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"last_agent_message\":\"SECRET_TRANSCRIPT_BODY\"}}\n",
        )
        .expect("completion should append");
}

async fn set_codex_version(path: &std::path::Path, id: &str, version: &str) {
    let pool = sqlx::SqlitePool::connect_with(SqliteConnectOptions::new().filename(path))
        .await
        .expect("fixture database should reopen");
    sqlx::query("UPDATE threads SET cli_version = ? WHERE id = ?")
        .bind(version)
        .bind(id)
        .execute(&pool)
        .await
        .expect("fixture version should update");
    pool.close().await;
}

async fn create_codex_state(path: &std::path::Path, now_ms: i64) {
    let pool = sqlx::SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true),
    )
    .await
    .expect("fixture database should open");
    sqlx::query(
        "CREATE TABLE threads (id TEXT PRIMARY KEY, rollout_path TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, source TEXT NOT NULL, model_provider TEXT NOT NULL, cwd TEXT NOT NULL, title TEXT NOT NULL, sandbox_policy TEXT NOT NULL, approval_mode TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0, git_branch TEXT, git_origin_url TEXT, cli_version TEXT NOT NULL DEFAULT '', agent_nickname TEXT, agent_role TEXT, recency_at_ms INTEGER NOT NULL DEFAULT 0)",
    )
    .execute(&pool)
    .await
    .expect("threads schema should exist");
    sqlx::query(
        "CREATE TABLE thread_spawn_edges (parent_thread_id TEXT NOT NULL, child_thread_id TEXT NOT NULL PRIMARY KEY, status TEXT NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("edge schema should exist");
    for (id, cwd, title, nickname, archived, recency) in [
        (
            "codex-main",
            "/host/repositories/main",
            "Native main",
            None,
            0_i64,
            now_ms - 1_000,
        ),
        (
            "codex-child",
            "/host/repositories/child",
            "Native child",
            Some("reviewer"),
            0,
            now_ms - 500,
        ),
        (
            "codex-old",
            "/host/repositories/old",
            "Old session",
            None,
            0,
            now_ms - 86_400_001,
        ),
        (
            "codex-archived",
            "/host/repositories/old",
            "Archived session",
            None,
            1,
            now_ms,
        ),
    ] {
        sqlx::query("INSERT INTO threads (id, rollout_path, created_at, updated_at, source, model_provider, cwd, title, sandbox_policy, approval_mode, archived, git_branch, git_origin_url, cli_version, agent_nickname, recency_at_ms) VALUES (?, ?, 1, 1, 'cli', 'openai', ?, ?, '{}', 'default', ?, 'feat/watchdog', 'https://github.com/lklimek/agent-watchdog.git', '0.144.5', ?, ?)")
            .bind(id)
            .bind(format!("/state/{id}.jsonl"))
            .bind(cwd)
            .bind(title)
            .bind(archived)
            .bind(nickname)
            .bind(recency)
            .execute(&pool)
            .await
            .expect("thread fixture should insert");
    }
    sqlx::query("INSERT INTO thread_spawn_edges VALUES ('codex-main', 'codex-child', 'active')")
        .execute(&pool)
        .await
        .expect("edge fixture should insert");
    pool.close().await;
}
