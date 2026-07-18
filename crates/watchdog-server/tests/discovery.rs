//! Automatic runtime discovery and best-effort reconciliation acceptance tests.

use std::{fs, io::Write as _, sync::Arc};

use serde_json::json;
use sqlx::sqlite::SqliteConnectOptions;
use watchdog_domain::{DetailedState, DurationMs, SessionKind, TimePoint, WallTimeMs};
use watchdog_server::{
    AgentApi, ClaudeTeamDiscovery, CodexDiscovery, CompanionDiscovery, DashboardQuery,
    DashboardService, WorktreePathMapping,
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
    let state_path = workspace_state.join("state.json");
    fs::write(
        &state_path,
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
    assert_eq!(metadata.startup_directory(), Some("/host/repositories/job"));
}

fn update_companion_summary(path: &std::path::Path) {
    let updated = fs::read_to_string(path)
        .expect("summary should remain readable")
        .replace("2026-07-18T10:00:00Z", "2026-07-18T10:01:00Z");
    fs::write(path, updated).expect("summary update should be written");
}

fn write_companion_detail(workspace_state: &std::path::Path) {
    let jobs = workspace_state.join("jobs");
    fs::create_dir(&jobs).expect("detail directory should be created");
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
