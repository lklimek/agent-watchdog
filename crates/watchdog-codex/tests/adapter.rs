//! Codex app-server and read-only state fallback contracts.

use std::error::Error as _;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use watchdog_codex::{
    CodexAppServerParser, CodexHookParser, CodexParseError, CodexRolloutParser, CodexStateError,
    CodexStateReader, MAX_APP_SERVER_BYTES,
};
use watchdog_domain::{
    DetailedState, DomainInputError, NativeSessionKey, ObservationPayload, RuntimeKind,
    SessionKind, TimePoint, WallTimeMs,
};

fn now() -> TimePoint {
    TimePoint::new(WallTimeMs::new(2_000), 900)
}

#[test]
fn thread_started_exposes_exact_subagent_parent_and_metadata() {
    let parser = CodexAppServerParser::new("0.144.5").expect("version should be valid");
    let evidence = parser
        .parse_notification(
            br#"{
                "method":"thread/started",
                "params":{"thread":{
                    "id":"child-1",
                    "parentThreadId":"main-1",
                    "cwd":"/work/wt",
                    "name":"reviewer",
                    "status":{"type":"active","activeFlags":[]}
                }}
            }"#,
            "app-event-1",
            now(),
        )
        .expect("current notification should parse");

    assert_eq!(evidence.kind(), Some(SessionKind::Child));
    assert_eq!(evidence.subject().native_id(), "child-1");
    assert_eq!(
        evidence.parent().expect("parent should exist").native_id(),
        "main-1"
    );
    assert_eq!(evidence.title(), Some("reviewer"));
    assert!(matches!(
        evidence.observation().payload(),
        ObservationPayload::NativeState(DetailedState::Running)
    ));
}

#[test]
fn thread_status_waiting_on_approval_requires_user_attention() {
    let parser = CodexAppServerParser::new("0.144.5").expect("version should be valid");
    let evidence = parser
        .parse_notification(
            br#"{
                "method":"thread/status/changed",
                "params":{"threadId":"main-1","status":{"type":"active","activeFlags":["waitingOnApproval"]}}
            }"#,
            "app-event-2",
            now(),
        )
        .expect("current status should parse");

    assert!(matches!(
        evidence.observation().payload(),
        ObservationPayload::NativeState(DetailedState::WaitingForUser)
    ));
}

#[test]
fn failed_turn_maps_without_retaining_native_error_text() {
    let parser = CodexAppServerParser::new("0.144.5").expect("version should be valid");
    let evidence = parser
        .parse_notification(
            br#"{
                "method":"turn/completed",
                "params":{"threadId":"main-1","turn":{"id":"turn-1","status":"failed","error":{"message":"SECRET_NATIVE_ERROR"}}}
            }"#,
            "app-event-3",
            now(),
        )
        .expect("failed turn should parse");

    assert!(matches!(
        evidence.observation().payload(),
        ObservationPayload::NativeState(DetailedState::Failed)
    ));
    assert!(!format!("{evidence:?}").contains("SECRET_NATIVE_ERROR"));
}

#[test]
fn schema_drift_and_oversize_become_upgrade_errors() {
    let parser = CodexAppServerParser::new("future-version").expect("version should be valid");
    let drift = parser
        .parse_notification(
            br#"{"method":"future/event","params":{"secret":"do-not-log"}}"#,
            "app-event-4",
            now(),
        )
        .expect_err("unknown method should not invent state");
    assert!(matches!(drift, CodexParseError::UnsupportedEvent));
    assert_eq!(drift.compatibility_warning().badge(), "UPGRADE");
    let warning = drift.compatibility_warning_for_version("0.150.0");
    assert!(warning.message().contains("detected Codex CLI 0.150.0"));
    assert!(warning.message().contains("tested with Codex CLI 0.144.5"));
    assert_eq!(warning.detected_version(), Some("0.150.0"));
    assert!(!drift.to_string().contains("future/event"));

    let oversized = vec![b'x'; MAX_APP_SERVER_BYTES + 1];
    assert!(matches!(
        parser.parse_notification(&oversized, "large", now()),
        Err(CodexParseError::InputTooLarge { .. })
    ));
}

#[test]
fn oversized_detected_version_falls_back_to_bounded_warning() {
    let parser = CodexAppServerParser::new("future-version").expect("version should be valid");
    let error = parser
        .parse_notification(
            br#"{"method":"future/event","params":{}}"#,
            "app-event-long-version",
            now(),
        )
        .expect_err("unknown method should not invent state");

    let warning = error.compatibility_warning_for_version(&"v".repeat(1_024));

    assert_eq!(warning.badge(), "UPGRADE");
    assert!(warning.message().contains("tested with Codex CLI"));
    assert!(warning.detected_version().is_none());
}

#[test]
fn codex_state_error_preserves_domain_source() {
    let error = CodexStateError::from(DomainInputError::TooLong {
        field: "cli_version",
        max_bytes: 128,
        actual_bytes: 129,
    });

    let source = error
        .source()
        .expect("domain error source should be retained");
    assert!(source.to_string().contains("cli_version"));
}

#[test]
fn rollout_session_metadata_exposes_exact_parent_without_retaining_content() {
    let parser = CodexRolloutParser::new("0.144.5").expect("version should be valid");
    let evidence = parser
        .parse_record(
            br#"{
                "timestamp":"2026-07-17T12:00:00Z",
                "type":"session_meta",
                "payload":{
                    "session_id":"child-2",
                    "id":"child-2",
                    "parent_thread_id":"main-2",
                    "cwd":"/work/child",
                    "cli_version":"0.144.5",
                    "agent_nickname":"safety-reviewer",
                    "source":{"subagent":{"thread_spawn":{"parent_thread_id":"main-2","depth":1}}},
                    "base_instructions":"SECRET_INSTRUCTIONS"
                }
            }"#,
            None,
            "rollout-1",
            now(),
        )
        .expect("current rollout metadata should parse");

    assert_eq!(evidence.kind(), Some(SessionKind::Child));
    assert_eq!(evidence.subject().native_id(), "child-2");
    assert_eq!(
        evidence
            .parent()
            .expect("exact parent should exist")
            .native_id(),
        "main-2"
    );
    assert_eq!(evidence.title(), Some("safety-reviewer"));
    assert_eq!(
        evidence.cwd().and_then(|path| path.to_str()),
        Some("/work/child")
    );
    assert!(!format!("{evidence:?}").contains("SECRET_INSTRUCTIONS"));
}

#[test]
fn rollout_activity_uses_file_subject_and_does_not_retain_message_body() {
    let parser = CodexRolloutParser::new("0.144.5").expect("version should be valid");
    let subject =
        NativeSessionKey::new(RuntimeKind::CodexCli, "child-2").expect("subject should be valid");
    let evidence = parser
        .parse_record(
            br#"{
                "timestamp":"2026-07-17T12:01:00Z",
                "type":"response_item",
                "payload":{"type":"message","content":"SECRET_TRANSCRIPT_BODY"}
            }"#,
            Some(&subject),
            "rollout-2",
            now(),
        )
        .expect("known rollout activity should parse");

    assert!(matches!(
        evidence.observation().payload(),
        ObservationPayload::Progress(_)
    ));
    assert!(!format!("{evidence:?}").contains("SECRET_TRANSCRIPT_BODY"));
}

#[test]
fn rollout_event_messages_preserve_supported_task_lifecycle() {
    let parser = CodexRolloutParser::new("0.144.6").expect("version should be valid");
    let subject =
        NativeSessionKey::new(RuntimeKind::CodexCli, "claude-child").expect("valid subject");

    for (event_type, expected) in [
        ("task_started", DetailedState::Running),
        ("task_complete", DetailedState::Completed),
    ] {
        let record = serde_json::json!({
            "timestamp": "2026-07-20T07:44:35Z",
            "type": "event_msg",
            "payload": {
                "type": event_type,
                "last_agent_message": "SECRET_TRANSCRIPT_BODY"
            }
        });
        let bytes = serde_json::to_vec(&record).expect("fixture should serialize");
        let evidence = parser
            .parse_record(&bytes, Some(&subject), event_type, now())
            .expect("supported lifecycle event should parse");

        assert!(matches!(
            evidence.observation().payload(),
            ObservationPayload::NativeState(state) if *state == expected
        ));
        assert!(!format!("{evidence:?}").contains("SECRET_TRANSCRIPT_BODY"));
    }
}

#[test]
fn rollout_metadata_retains_bounded_launch_origin_and_repository() {
    let parser = CodexRolloutParser::new("0.144.6").expect("version should be valid");
    let evidence = parser
        .parse_record(
            br#"{
                "type":"session_meta",
                "payload":{
                    "id":"claude-launched-thread",
                    "cwd":"/work/child",
                    "originator":"Claude Code",
                    "source":"vscode",
                    "git":{"repository_url":"https://github.com/example/project.git"},
                    "base_instructions":"SECRET_INSTRUCTIONS"
                }
            }"#,
            None,
            "rollout-origin",
            now(),
        )
        .expect("current rollout metadata should parse");

    assert_eq!(evidence.originator(), Some("Claude Code"));
    assert_eq!(
        evidence.repository_url(),
        Some("https://github.com/example/project.git")
    );
    assert!(!format!("{evidence:?}").contains("SECRET_INSTRUCTIONS"));
}

#[test]
fn rollout_activity_without_file_identity_fails_closed() {
    let parser = CodexRolloutParser::new("0.144.5").expect("version should be valid");
    let error = parser
        .parse_record(
            br#"{"timestamp":"now","type":"event_msg","payload":{"type":"task_started"}}"#,
            None,
            "rollout-3",
            now(),
        )
        .expect_err("activity cannot invent a session association");

    assert!(matches!(error, CodexParseError::MissingSubject));
}

#[test]
fn official_codex_subagent_hooks_establish_exact_lifecycle() {
    let parser = CodexHookParser::new("0.144.5").expect("version should be valid");
    let started = parser
        .parse_hook(
            br#"{
                "session_id":"main-3",
                "transcript_path":"/state/main.jsonl",
                "cwd":"/work/child",
                "hook_event_name":"SubagentStart",
                "model":"gpt-5.4",
                "permission_mode":"default",
                "turn_id":"turn-1",
                "agent_id":"child-3",
                "agent_type":"reviewer"
            }"#,
            "hook-1",
            now(),
        )
        .expect("current hook should parse");
    assert_eq!(started.kind(), SessionKind::Child);
    assert_eq!(started.subject().native_id(), "child-3");
    assert_eq!(
        started.parent().expect("parent should exist").native_id(),
        "main-3"
    );
    assert_eq!(started.title(), Some("reviewer"));
    assert!(matches!(
        started.observation().payload(),
        ObservationPayload::NativeState(DetailedState::Starting)
    ));

    let stopped = parser
        .parse_hook(
            br#"{
                "session_id":"main-3",
                "transcript_path":"/state/main.jsonl",
                "agent_transcript_path":"/state/child.jsonl",
                "cwd":"/work/child",
                "hook_event_name":"SubagentStop",
                "model":"gpt-5.4",
                "permission_mode":"default",
                "turn_id":"turn-1",
                "agent_id":"child-3",
                "agent_type":"reviewer",
                "stop_hook_active":false,
                "last_assistant_message":"SECRET_RESULT"
            }"#,
            "hook-2",
            now(),
        )
        .expect("current stop hook should parse");
    assert!(matches!(
        stopped.observation().payload(),
        ObservationPayload::NativeState(DetailedState::Completed)
    ));
    assert_eq!(
        stopped.transcript_path().and_then(|path| path.to_str()),
        Some("/state/child.jsonl")
    );
    assert!(!format!("{stopped:?}").contains("SECRET_RESULT"));
}

#[test]
fn app_server_thread_closed_is_idle_not_task_completion() {
    let parser = CodexAppServerParser::new("0.144.5").expect("version should be valid");
    let evidence = parser
        .parse_notification(
            br#"{"method":"thread/closed","params":{"threadId":"child-3"}}"#,
            "close-1",
            now(),
        )
        .expect("current close event should parse");

    assert!(matches!(
        evidence.observation().payload(),
        ObservationPayload::NativeState(DetailedState::Idle)
    ));
}

#[tokio::test]
async fn current_sqlite_state_discovers_all_bounded_threads_and_spawn_edges_read_only() {
    let directory = tempfile::tempdir().expect("temporary directory should exist");
    let path = directory.path().join("state_5.sqlite");
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let pool = sqlx::SqlitePool::connect_with(options)
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
    for (id, cwd, title, recency) in [
        ("main-1", "/work/main", "Main title", 10_i64),
        ("child-1", "/work/wt", "Child title", 20_i64),
    ] {
        sqlx::query("INSERT INTO threads (id, rollout_path, created_at, updated_at, source, model_provider, cwd, title, sandbox_policy, approval_mode, git_branch, git_origin_url, cli_version, recency_at_ms) VALUES (?, ?, 1, 1, 'cli', 'openai', ?, ?, '{}', 'default', 'feat/watchdog', 'https://github.com/lklimek/agent-watchdog.git', '0.144.5', ?)")
            .bind(id)
            .bind(format!("/state/{id}.jsonl"))
            .bind(cwd)
            .bind(title)
            .bind(recency)
            .execute(&pool)
            .await
            .expect("thread fixture should insert");
    }
    sqlx::query("INSERT INTO thread_spawn_edges VALUES ('main-1', 'child-1', 'active')")
        .execute(&pool)
        .await
        .expect("edge fixture should insert");
    sqlx::query("INSERT INTO threads (id, rollout_path, created_at, updated_at, source, model_provider, cwd, title, sandbox_policy, approval_mode, archived, cli_version, recency_at_ms) VALUES ('archived-1', '/state/archived.jsonl', 1, 1, 'cli', 'openai', '/work/old', 'Archived', '{}', 'default', 1, '0.144.5', 30)")
        .execute(&pool)
        .await
        .expect("archived fixture should insert");
    pool.close().await;

    let reader = CodexStateReader::open(&path)
        .await
        .expect("read-only state should open");
    let batch = reader
        .discover_threads(10)
        .await
        .expect("current schema should parse");

    assert_eq!(batch.len(), 3);
    let child = batch
        .iter()
        .find(|thread| thread.subject().native_id() == "child-1")
        .expect("child should exist");
    assert_eq!(child.kind(), SessionKind::Child);
    assert_eq!(
        child.parent().expect("parent should exist").native_id(),
        "main-1"
    );
    assert_eq!(child.cwd().to_str(), Some("/work/wt"));
    assert_eq!(child.git_branch(), Some("feat/watchdog"));
    assert_eq!(
        child.git_origin_url(),
        Some("https://github.com/lklimek/agent-watchdog.git")
    );

    let recent = reader
        .discover_recent_threads(WallTimeMs::new(10), 10)
        .await
        .expect("recent unarchived state should parse");
    assert_eq!(recent.len(), 2);
    assert!(recent.iter().all(|thread| !thread.archived()));
    assert!(
        recent
            .iter()
            .all(|thread| thread.recency_at().value() >= 10)
    );
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn held_sqlite_database_identity_survives_path_replacement_with_live_wal() {
    let directory = tempfile::tempdir().expect("temporary directory should exist");
    let path = directory.path().join("state_5.sqlite");
    let held_path = directory.path().join("validated-state.sqlite");
    let pool = sqlx::SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal),
    )
    .await
    .expect("live fixture database should open");
    sqlx::query("PRAGMA wal_autocheckpoint = 0")
        .execute(&pool)
        .await
        .expect("automatic checkpoints should be disabled");
    create_minimal_state(&pool, "validated-thread").await;

    let held = std::fs::File::open(&path).expect("validated database should be held");
    fs::rename(&path, &held_path).expect("validated database should move");
    for suffix in ["-wal", "-shm"] {
        fs::rename(
            format!("{}{}", path.display(), suffix),
            format!("{}{}", held_path.display(), suffix),
        )
        .expect("live SQLite sidecar should move with its database");
    }

    let attacker = sqlx::SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true),
    )
    .await
    .expect("replacement database should open");
    create_minimal_state(&attacker, "replacement-thread").await;
    attacker.close().await;

    let reader = CodexStateReader::open_file(held)
        .await
        .expect("held database should open with its live WAL");
    let threads = reader
        .discover_threads(10)
        .await
        .expect("held database should remain readable");

    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].subject().native_id(), "validated-thread");
    pool.close().await;
}

async fn create_minimal_state(pool: &sqlx::SqlitePool, id: &str) {
    sqlx::query(
        "CREATE TABLE threads (id TEXT PRIMARY KEY, rollout_path TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, source TEXT NOT NULL, model_provider TEXT NOT NULL, cwd TEXT NOT NULL, title TEXT NOT NULL, sandbox_policy TEXT NOT NULL, approval_mode TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0, git_branch TEXT, git_origin_url TEXT, cli_version TEXT NOT NULL DEFAULT '', agent_nickname TEXT, agent_role TEXT, recency_at_ms INTEGER NOT NULL DEFAULT 0)",
    )
    .execute(pool)
    .await
    .expect("threads schema should exist");
    sqlx::query(
        "CREATE TABLE thread_spawn_edges (parent_thread_id TEXT NOT NULL, child_thread_id TEXT NOT NULL PRIMARY KEY, status TEXT NOT NULL)",
    )
    .execute(pool)
    .await
    .expect("edge schema should exist");
    sqlx::query("INSERT INTO threads (id, rollout_path, created_at, updated_at, source, model_provider, cwd, title, sandbox_policy, approval_mode, cli_version, recency_at_ms) VALUES (?, '/state/rollout.jsonl', 1, 1, 'cli', 'openai', '/work', 'Thread', '{}', 'default', '0.144.5', 1)")
        .bind(id)
        .execute(pool)
        .await
        .expect("thread fixture should insert");
}
