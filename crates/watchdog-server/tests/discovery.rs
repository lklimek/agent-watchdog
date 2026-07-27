//! Automatic runtime discovery and best-effort reconciliation acceptance tests.
#![cfg(target_os = "linux")]

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
    DetailedState, DurationMs, MainSessionId, NativeSessionKey, ProcessId, RuntimeKind, SessionId,
    SessionIdentity, SessionKind, TimePoint, WallTimeMs,
};
use watchdog_process::LinuxProcessSampler;
use watchdog_server::{
    AgentApi, ClaudeDiscovery, CodexDiscovery, CompanionDiscovery, DashboardQuery,
    DashboardService, DiscoveredSession, DiscoveryAliasRegistry, TransportKey, WorktreePathMapping,
};
use watchdog_store::WatchdogStore;
use watchdog_testkit::FakeClock;

struct ChildProcessGuard(std::process::Child);

impl ChildProcessGuard {
    fn spawn_sleep() -> Self {
        Self(
            std::process::Command::new("sleep")
                .arg("30")
                .spawn()
                .expect("second live process should start"),
        )
    }

    fn id(&self) -> u32 {
        self.0.id()
    }
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

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
    assert_claude_child_metadata(&store, children[0].session).await;

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

    assert_patch_and_minor_compatibility_policy(
        &mut transcript,
        &clock,
        &discovery,
        &projects_root,
        (&runtime_mapping, &worktree_mapping),
        &store,
        children[0].session,
    )
    .await;

    assert_versionless_drift_preserves_detected_warning(
        &mut transcript,
        &clock,
        &discovery,
        &projects_root,
        (&runtime_mapping, &worktree_mapping),
        &store,
        children[0].session,
    )
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn claude_live_registry_excludes_absent_retained_main_without_directory_deduplication() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let registry = fixture.path().join("sessions");
    let worktrees = fixture.path().join("worktrees");
    fs::create_dir_all(&registry).expect("registry should exist");
    fs::create_dir_all(worktrees.join("repo")).expect("worktree should exist");
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
    api.discover_session(DiscoveredSession {
        runtime: RuntimeKind::ClaudeCode,
        native_id: "retained-main".to_owned(),
        kind: SessionKind::Main,
        parent: None,
        event_key: "retained-main".to_owned(),
        adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION.to_owned(),
        evidence_source: "test:retained".to_owned(),
        title: None,
        startup_directory: Some("/host/repositories/repo".to_owned()),
    })
    .await
    .expect("retained main should be discovered");

    let pid = ProcessId::new(std::process::id()).expect("test PID should be valid");
    let process = LinuxProcessSampler::new(1)
        .expect("sampler should initialize")
        .read_identity(pid)
        .expect("test process should be readable");
    write_live_claude_session(&registry, "live-main", "native title", "idle", &process);

    let discovery = ClaudeDiscovery::new(api, store.clone(), clock);
    let worktree_mapping = WorktreePathMapping::new("/host/repositories", worktrees)
        .expect("worktree mapping should be valid");
    let report = discovery
        .reconcile(&[registry], &[], &[worktree_mapping])
        .await;
    assert_eq!(report.main_sessions(), 1);
    let live = NativeSessionKey::new(RuntimeKind::ClaudeCode, "live-main")
        .expect("live native ID should be valid");
    assert_session_state(
        &store,
        SessionIdentity::Main(MainSessionId::from(SessionId::from_native(&live))),
        DetailedState::WaitingForUser,
    )
    .await;
    let live_snapshot = load_snapshot(
        &store,
        SessionIdentity::Main(MainSessionId::from(SessionId::from_native(&live))),
    )
    .await;
    assert_eq!(
        live_snapshot
            .reducer_snapshot()
            .expect("live reducer snapshot should exist")
            .process_identity(),
        Some(&process)
    );
    let retained = NativeSessionKey::new(RuntimeKind::ClaudeCode, "retained-main")
        .expect("retained native ID should be valid");
    assert_session_state(
        &store,
        SessionIdentity::Main(MainSessionId::from(SessionId::from_native(&retained))),
        DetailedState::Completed,
    )
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn claude_live_registry_keeps_concurrent_mains_in_one_directory_distinct() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let registry = fixture.path().join("sessions");
    let worktrees = fixture.path().join("worktrees");
    fs::create_dir_all(&registry).expect("registry should exist");
    fs::create_dir_all(worktrees.join("repo")).expect("worktree should exist");
    let second_process = ChildProcessGuard::spawn_sleep();
    let sampler = LinuxProcessSampler::new(1).expect("sampler should initialize");
    let first = sampler
        .read_identity(ProcessId::new(std::process::id()).expect("test PID should be valid"))
        .expect("test process should be readable");
    let second = sampler
        .read_identity(
            ProcessId::new(second_process.id()).expect("second process PID should be valid"),
        )
        .expect("second process should be readable");
    write_live_claude_session(&registry, "live-main-one", "first", "busy", &first);
    write_live_claude_session(&registry, "live-main-two", "second", "idle", &second);

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
    let mapping = WorktreePathMapping::new("/host/repositories", worktrees)
        .expect("worktree mapping should be valid");
    let report = ClaudeDiscovery::new(api, store.clone(), clock)
        .reconcile(&[registry], &[], &[mapping])
        .await;
    assert_eq!(report.main_sessions(), 2);
    assert_eq!(
        store
            .sessions_by_kind(SessionKind::Main, 10)
            .await
            .expect("mains should query")
            .len(),
        2
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn malformed_claude_live_registry_never_retires_absent_mains() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let registry = fixture.path().join("sessions");
    fs::create_dir_all(&registry).expect("registry should exist");
    fs::write(registry.join("1.json"), b"{incomplete")
        .expect("malformed registry record should be written");
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
    api.discover_session(DiscoveredSession {
        runtime: RuntimeKind::ClaudeCode,
        native_id: "retained-main".to_owned(),
        kind: SessionKind::Main,
        parent: None,
        event_key: "retained-main".to_owned(),
        adapter_version: watchdog_claude::TESTED_CLAUDE_VERSION.to_owned(),
        evidence_source: "test:retained".to_owned(),
        title: None,
        startup_directory: Some("/host/repositories/repo".to_owned()),
    })
    .await
    .expect("retained main should be discovered");

    let report = ClaudeDiscovery::new(api, store.clone(), clock)
        .reconcile(&[registry], &[], &[])
        .await;
    assert_eq!(report.warning_count(), 1);
    let retained = NativeSessionKey::new(RuntimeKind::ClaudeCode, "retained-main")
        .expect("retained native ID should be valid");
    assert_session_state(
        &store,
        SessionIdentity::Main(MainSessionId::from(SessionId::from_native(&retained))),
        DetailedState::Starting,
    )
    .await;
}

fn write_live_claude_session(
    registry: &Path,
    session_id: &str,
    title: &str,
    status: &str,
    process: &watchdog_domain::ProcessIdentity,
) {
    fs::write(
        registry.join(format!("{}.json", process.pid().value())),
        serde_json::to_vec(&json!({
            "pid": process.pid().value(),
            "sessionId": session_id,
            "cwd": "/host/repositories/repo",
            "kind": "interactive",
            "name": title,
            "procStart": process.start_time_ticks().to_string(),
            "status": status,
            "updatedAt": current_time_ms(),
            "version": watchdog_claude::TESTED_CLAUDE_VERSION,
        }))
        .expect("registry JSON should serialize"),
    )
    .expect("registry record should be written");
}

async fn assert_patch_and_minor_compatibility_policy(
    transcript: &mut fs::File,
    clock: &FakeClock,
    discovery: &ClaudeDiscovery,
    projects_root: &Path,
    mappings: (&WorktreePathMapping, &WorktreePathMapping),
    store: &WatchdogStore,
    session: SessionIdentity,
) {
    transcript
        .write_all(b"{\"type\":\"future-record\",\"sessionId\":\"main-session\",\"agentId\":\"child-1\",\"version\":\"2.1.212\"}\n")
        .expect("future schema record should append");
    clock.advance(DurationMs::new(1_000));
    let drifted = reconcile_claude_fixture(discovery, projects_root, mappings.0, mappings.1).await;
    assert_eq!(drifted.warning_count(), 1);
    let drifted_snapshot = load_snapshot(store, session).await;
    assert!(
        drifted_snapshot
            .reducer_snapshot()
            .expect("reducer snapshot should exist")
            .compatibility_warning()
            .is_none(),
        "patch-only drift must not add an UPGRADE badge"
    );

    transcript
        .write_all(b"{\"type\":\"future-minor-record\",\"sessionId\":\"main-session\",\"agentId\":\"child-1\",\"version\":\"2.2.0\"}\n")
        .expect("minor schema record should append");
    clock.advance(DurationMs::new(1_000));
    let minor_drift =
        reconcile_claude_fixture(discovery, projects_root, mappings.0, mappings.1).await;
    assert_eq!(minor_drift.warning_count(), 1);
    let minor_snapshot = load_snapshot(store, session).await;
    let warning = minor_snapshot
        .reducer_snapshot()
        .expect("reducer snapshot should exist")
        .compatibility_warning()
        .expect("schema drift should be actionable");
    assert_upgrade_versions(warning, "Claude Code 2.2.0", "Claude Code 2.1.214");
}

async fn assert_claude_child_metadata(store: &WatchdogStore, session: SessionIdentity) {
    let metadata = store
        .session_metadata(session)
        .await
        .expect("metadata should query")
        .expect("metadata should exist");
    assert_eq!(metadata.title(), Some("security-reviewer"));
    assert_eq!(
        metadata.startup_directory(),
        Some("/host/repositories/child")
    );
    assert_eq!(metadata.branch(), Some("feat/claude"));
}

async fn assert_versionless_drift_preserves_detected_warning(
    transcript: &mut fs::File,
    clock: &FakeClock,
    discovery: &ClaudeDiscovery,
    projects_root: &Path,
    mappings: (&WorktreePathMapping, &WorktreePathMapping),
    store: &WatchdogStore,
    session: SessionIdentity,
) {
    transcript
        .write_all(
            b"{\"type\":\"another-future-record\",\"sessionId\":\"main-session\",\"agentId\":\"child-1\"}\n",
        )
        .expect("versionless future schema record should append");
    clock.advance(DurationMs::new(1_000));
    let versionless_drift =
        reconcile_claude_fixture(discovery, projects_root, mappings.0, mappings.1).await;
    assert_eq!(versionless_drift.warning_count(), 1);
    let preserved_snapshot = load_snapshot(store, session).await;
    let warning = preserved_snapshot
        .reducer_snapshot()
        .expect("reducer snapshot should exist")
        .compatibility_warning()
        .expect("schema drift should remain actionable");
    assert_upgrade_versions(warning, "Claude Code 2.2.0", "Claude Code 2.1.214");
}

fn assert_upgrade_versions(
    warning: &watchdog_domain::CompatibilityWarning,
    detected: &str,
    tested: &str,
) {
    assert_eq!(warning.badge(), "UPGRADE");
    assert!(warning.message().contains(&format!("detected {detected}")));
    assert!(warning.message().contains(&format!("tested with {tested}")));
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

    api.mark_restarted()
        .await
        .expect("restart boundary should persist");
    let restarted = ClaudeDiscovery::new(api.clone(), store.clone(), clock.clone());
    restarted
        .reconcile(
            &[projects.clone(), teams.clone()],
            &runtime_mappings,
            std::slice::from_ref(&worktree_mapping),
        )
        .await;
    for session in [mains[0].session, children[0].session] {
        assert!(
            !load_snapshot(&store, session)
                .await
                .reducer_snapshot()
                .expect("reducer snapshot should exist")
                .reconciliation_required(),
            "current team config should clear the restart gate"
        );
    }

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
async fn ambiguous_teammates_with_one_team_parent_do_not_create_a_main() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let (projects, teams, worktrees, _) = create_claude_team_alias_fixtures(fixture.path());
    let config = teams.join("session-main/config.json");
    let mut team: serde_json::Value =
        serde_json::from_slice(&fs::read(&config).expect("team config should read"))
            .expect("team config should parse");
    team["members"]
        .as_array_mut()
        .expect("members should be an array")
        .push(json!({
            "agentType": "claudius:developer",
            "name": "worker-2",
            "agentId": "team-worker-2",
            "cwd": "/host/repositories/worker",
            "isActive": true
        }));
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
    let runtime_mappings = [
        WorktreePathMapping::new("/home/test/.claude/projects", projects.clone())
            .expect("projects mapping should be valid"),
        WorktreePathMapping::new("/home/test/.claude/teams", teams.clone())
            .expect("teams mapping should be valid"),
    ];
    let worktree_mapping = WorktreePathMapping::new("/host/repositories", worktrees)
        .expect("worktree mapping should be valid");

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
    let children = store
        .sessions_by_kind(SessionKind::Child, 10)
        .await
        .expect("children should query");
    assert_eq!(mains.len(), 1);
    assert_eq!(mains[0].native.native_id(), "main-session");
    assert_eq!(children.len(), 2);
}

#[tokio::test]
async fn claude_team_lead_aliases_a_post_reset_transcript_by_unique_cwd() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let (projects, teams, worktrees, teammate_transcript) =
        create_claude_team_alias_fixtures(fixture.path());
    fs::remove_file(teammate_transcript).expect("unrelated teammate transcript should be removed");
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
    let discovery = ClaudeDiscovery::new(api.clone(), store.clone(), clock.clone());
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
            std::slice::from_ref(&projects),
            &runtime_mappings,
            std::slice::from_ref(&worktree_mapping),
        )
        .await;
    api.mark_restarted()
        .await
        .expect("restart boundary should persist");

    let restarted = ClaudeDiscovery::new(api, store.clone(), clock.clone());
    restarted
        .reconcile(
            &[projects.clone(), teams.clone()],
            &runtime_mappings,
            std::slice::from_ref(&worktree_mapping),
        )
        .await;
    append_claude_activity(
        &projects.join("-host-repositories-main/main-session.jsonl"),
        "main-session",
        None,
    );
    clock.advance(DurationMs::new(1_000));
    restarted
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
    assert_eq!(mains.len(), 2, "retained history must not be deleted");
    let dashboard = DashboardService::new(store, clock)
        .snapshot(DashboardQuery::default())
        .await
        .expect("dashboard should render");
    assert_eq!(dashboard.sessions.len(), 1);
    let canonical = mains
        .iter()
        .find(|main| main.native.native_id() == "retained-team-lead")
        .expect("canonical lead should exist");
    assert_eq!(
        dashboard.sessions[0].session_id.session_id(),
        canonical.root.session_id()
    );
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
async fn inactive_claude_team_member_does_not_claim_a_terminal_state() {
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
    assert_session_state(&store, children[0].session, DetailedState::Starting).await;
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
async fn claude_originated_codex_ignores_unreconciled_retained_parent() {
    let fixture = tempfile::tempdir().expect("fixture root should exist");
    let mounted = fixture.path().join("mounted");
    let worktree = mounted.join("main");
    let rollouts = fixture.path().join("rollouts");
    fs::create_dir_all(&worktree).expect("worktree should exist");
    fs::create_dir_all(&rollouts).expect("rollout root should exist");
    fs::write(
        rollouts.join("rollout-current.jsonl"),
        b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-current\",\"cwd\":\"/host/main\",\"originator\":\"Claude Code\",\"source\":\"vscode\"}}\n",
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
    let mut current = None;
    for native_id in ["claude-current", "claude-retained"] {
        let view = api
            .discover_session(DiscoveredSession {
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
        if native_id == "claude-current" {
            current = Some(view.session.session_id());
        }
    }
    api.mark_restarted()
        .await
        .expect("restart boundary should persist");
    let transport = TransportKey::new("current-parent").expect("transport should be valid");
    let current = current.expect("current main should exist");
    api.bind_discovered_main(&transport, current)
        .await
        .expect("current main should bind");
    api.report_progress(
        &transport,
        current,
        "current-parent:progress",
        "Current native evidence".to_owned(),
        None,
    )
    .await
    .expect("current main should reconcile");
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
        2
    );
    assert_eq!(
        store
            .sessions_by_kind(SessionKind::Child, 10)
            .await
            .expect("children should query")
            .len(),
        1
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the restart regression keeps its complete native fixture visible"
)]
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
        &[projects.clone(), teams.clone()],
        &runtime_mappings,
        std::slice::from_ref(&worktree_mapping),
    )
    .await;
    CompanionDiscovery::with_alias_registry(api.clone(), store.clone(), clock.clone(), aliases)
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

    fs::remove_file(team.join("config.json")).expect("team config should be removable");
    let restarted_aliases = DiscoveryAliasRegistry::default();
    ClaudeDiscovery::with_alias_registry(
        api.clone(),
        store.clone(),
        clock.clone(),
        restarted_aliases.clone(),
    )
    .reconcile(
        &[projects, teams],
        &runtime_mappings,
        std::slice::from_ref(&worktree_mapping),
    )
    .await;
    let restarted =
        CompanionDiscovery::with_alias_registry(api, store.clone(), clock, restarted_aliases)
            .reconcile(&[fixture.path().join("companion")], &[])
            .await;

    assert_eq!(restarted.warning_count(), 0);
    assert_eq!(
        store
            .sessions_by_kind(SessionKind::Main, 10)
            .await
            .expect("mains should query after restart")
            .len(),
        1
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
    append_rollout_completion(&rollout_root.join("rollout-main.jsonl"));
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
    assert_session_state(&store, mains[0].session, DetailedState::WaitingForUser).await;
    assert_eq!(children[0].root, mains[0].root);
    assert_codex_child_metadata(&store, children[0].session).await;

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

    append_rollout_completion(&rollout_root.join("rollout-main.jsonl"));
    clock.advance(DurationMs::new(1_000));
    discovery
        .reconcile(
            std::slice::from_ref(&rollout_root),
            std::slice::from_ref(&rollout_mapping),
            std::slice::from_ref(&mapping),
        )
        .await;
    assert_session_state(&store, mains[0].session, DetailedState::WaitingForUser).await;
}

async fn assert_codex_child_metadata(store: &WatchdogStore, session: SessionIdentity) {
    let child_metadata = store
        .session_metadata(session)
        .await
        .expect("metadata should query")
        .expect("metadata should exist");
    assert_eq!(child_metadata.title(), Some("reviewer"));
    assert_eq!(
        child_metadata.startup_directory(),
        Some("/host/repositories/child")
    );
}

#[tokio::test]
async fn codex_rollout_bootstrap_recovers_main_turn_completion_as_waiting_for_user() {
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
    assert_session_state(&store, mains[0].session, DetailedState::WaitingForUser).await;
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
    create_rollout_metadata_fixtures(&rollout_root);
    let now_ms = current_time_ms();
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
    let codex_roots = [state_root.clone(), rollout_root.clone()];

    let report = discovery
        .reconcile(
            &codex_roots,
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
    assert_no_compatibility_warning(&store, mains[0].session).await;
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

    append_rollout_activity(&rollout_root.join("rollout-child.jsonl"));
    clock.advance(DurationMs::new(1_000));

    let appended = discovery
        .reconcile(
            &codex_roots,
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

    assert_codex_version_drift(
        &discovery,
        &store,
        &clock,
        &codex_roots,
        &rollout_root,
        (&rollout_mapping, &mapping),
        mains[0].session,
    )
    .await;
}

async fn assert_no_compatibility_warning(store: &WatchdogStore, session: SessionIdentity) {
    let snapshot = load_snapshot(store, session).await;
    assert!(
        snapshot
            .reducer_snapshot()
            .expect("reducer snapshot should exist")
            .compatibility_warning()
            .is_none(),
        "a different version remains optimistic while its known schema parses"
    );
}

async fn assert_codex_version_drift(
    discovery: &CodexDiscovery,
    store: &WatchdogStore,
    clock: &FakeClock,
    codex_roots: &[PathBuf],
    rollout_root: &Path,
    mappings: (&WorktreePathMapping, &WorktreePathMapping),
    main: SessionIdentity,
) {
    let (rollout_mapping, worktree_mapping) = mappings;
    let mut rollout = fs::OpenOptions::new()
        .append(true)
        .open(rollout_root.join("rollout-main.jsonl"))
        .expect("main rollout should reopen");
    rollout
        .write_all(b"{\"type\":\"future_record\",\"payload\":{}}\n")
        .expect("schema drift should append");
    clock.advance(DurationMs::new(1_000));

    let drifted = discovery
        .reconcile(
            codex_roots,
            std::slice::from_ref(rollout_mapping),
            std::slice::from_ref(worktree_mapping),
        )
        .await;
    assert_eq!(drifted.warning_count(), 1);
    let snapshot = load_snapshot(store, main).await;
    let warning = snapshot
        .reducer_snapshot()
        .expect("reducer snapshot should exist")
        .compatibility_warning()
        .expect("major/minor schema drift should be actionable");
    assert_upgrade_versions(warning, "Codex CLI 0.999.0", "Codex CLI 0.144.5");

    rollout
        .write_all(b"{\"type\":\"another_future_record\",\"payload\":{}}\n")
        .expect("versionless schema drift should append");
    clock.advance(DurationMs::new(1_000));
    let rollout_roots = [rollout_root.to_path_buf()];
    let versionless = discovery
        .reconcile(
            &rollout_roots,
            std::slice::from_ref(rollout_mapping),
            std::slice::from_ref(worktree_mapping),
        )
        .await;
    assert_eq!(versionless.warning_count(), 1);
    let snapshot = load_snapshot(store, main).await;
    let warning = snapshot
        .reducer_snapshot()
        .expect("reducer snapshot should exist")
        .compatibility_warning()
        .expect("versionless drift must not clear detected-version evidence");
    assert_upgrade_versions(warning, "Codex CLI 0.999.0", "Codex CLI 0.144.5");
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
            .bind(format!("/state/rollout-{}.jsonl", id.trim_start_matches("codex-")))
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
