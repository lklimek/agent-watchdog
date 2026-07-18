//! Claude hook and automatic team-discovery contracts.

use watchdog_claude::{
    ClaudeHookParser, ClaudeParseError, TESTED_CLAUDE_VERSION, parse_subagent_metadata,
    parse_task_record, parse_team_config, parse_transcript_record,
};
use watchdog_domain::{
    DetailedState, ObservationPayload, RuntimeKind, SessionKind, TimePoint, WallTimeMs,
};

fn now() -> TimePoint {
    TimePoint::new(WallTimeMs::new(1_000), 500)
}

#[test]
fn session_start_hook_discovers_a_main_without_registration() {
    let parser = ClaudeHookParser::new(TESTED_CLAUDE_VERSION).expect("version should be valid");
    let evidence = parser
        .parse_hook(
            br#"{
                "session_id":"main-1",
                "transcript_path":"/state/project/main-1.jsonl",
                "cwd":"/work/repo",
                "hook_event_name":"SessionStart",
                "source":"startup",
                "model":"claude-sonnet-4-6"
            }"#,
            "hook-file-1",
            now(),
        )
        .expect("current hook should parse");

    assert_eq!(evidence.kind(), SessionKind::Main);
    assert_eq!(evidence.subject().runtime(), RuntimeKind::ClaudeCode);
    assert_eq!(evidence.subject().native_id(), "main-1");
    assert_eq!(evidence.parent(), None);
    assert!(matches!(
        evidence.observation().payload(),
        ObservationPayload::NativeState(DetailedState::Running)
    ));
    assert_eq!(
        evidence.cwd().expect("cwd should exist").to_str(),
        Some("/work/repo")
    );
}

#[test]
fn subagent_hooks_create_an_exact_parent_relation_and_terminal_state() {
    let parser = ClaudeHookParser::new(TESTED_CLAUDE_VERSION).expect("version should be valid");
    let started = parser
        .parse_hook(
            br#"{
                "session_id":"main-1",
                "transcript_path":"/state/main.jsonl",
                "cwd":"/work/child",
                "hook_event_name":"SubagentStart",
                "agent_id":"agent-7",
                "agent_type":"Explore"
            }"#,
            "hook-file-2",
            now(),
        )
        .expect("subagent start should parse");
    assert_eq!(started.kind(), SessionKind::Child);
    assert_eq!(started.subject().native_id(), "agent-7");
    assert_eq!(
        started.parent().expect("parent should exist").native_id(),
        "main-1"
    );
    assert!(matches!(
        started.observation().payload(),
        ObservationPayload::NativeState(DetailedState::Starting)
    ));

    let stopped = parser
        .parse_hook(
            br#"{
                "session_id":"main-1",
                "cwd":"/work/child",
                "hook_event_name":"SubagentStop",
                "agent_id":"agent-7",
                "agent_type":"Explore",
                "agent_transcript_path":"/state/agent-7.jsonl",
                "last_assistant_message":"SECRET_TRANSCRIPT_CONTENT"
            }"#,
            "hook-file-3",
            now(),
        )
        .expect("subagent stop should parse");
    assert!(matches!(
        stopped.observation().payload(),
        ObservationPayload::NativeState(DetailedState::Completed)
    ));
    assert!(!format!("{stopped:?}").contains("SECRET_TRANSCRIPT_CONTENT"));
}

#[test]
fn team_config_discovers_active_members_without_newest_session_guessing() {
    let team = parse_team_config(
        br#"{
            "name":"watchdog-team",
            "leadSessionId":"lead-1",
            "createdAt":1234,
            "members":[
                {"agentType":"team-lead","name":"lead","cwd":"/work/main","isActive":true},
                {"agentType":"developer","name":"bilby","agentId":"child-1","cwd":"/work/wt","isActive":true},
                {"agentType":"reviewer","name":"done","agentId":"child-2","cwd":"/work/wt2","isActive":false}
            ]
        }"#,
    )
    .expect("current team config should parse");

    assert_eq!(team.name(), Some("watchdog-team"));
    assert_eq!(team.lead().native_id(), "lead-1");
    assert_eq!(team.members().len(), 1);
    assert_eq!(team.members()[0].subject().native_id(), "child-1");
    assert_eq!(team.members()[0].name(), "bilby");
    assert_eq!(
        team.lead_cwd().expect("lead cwd should exist").to_str(),
        Some("/work/main")
    );
}

#[test]
fn malformed_or_drifted_hook_returns_an_actionable_bounded_error() {
    let parser = ClaudeHookParser::new("future-version").expect("version should be bounded");
    let error = parser
        .parse_hook(
            br#"{"session_id":"main-1","cwd":"/work","hook_event_name":"NewEvent"}"#,
            "hook-file-4",
            now(),
        )
        .expect_err("unknown event must not invent state");

    assert!(matches!(error, ClaudeParseError::UnsupportedEvent));
    assert_eq!(error.compatibility_warning().badge(), "UPGRADE");
    assert!(!error.to_string().contains("NewEvent"));
}

#[test]
fn oversized_hook_is_rejected_before_json_allocation() {
    let parser = ClaudeHookParser::new(TESTED_CLAUDE_VERSION).expect("version should be valid");
    let input = vec![b'x'; watchdog_claude::MAX_HOOK_BYTES + 1];
    let error = parser
        .parse_hook(&input, "oversized", now())
        .expect_err("oversized input must be rejected");

    assert!(matches!(error, ClaudeParseError::InputTooLarge { .. }));
}

#[test]
fn incremental_transcript_record_emits_metadata_only_activity() {
    let signal = parse_transcript_record(
        br#"{
            "type":"assistant",
            "sessionId":"main-1",
            "cwd":"/work/repo",
            "gitBranch":"feat/claude-ingestion",
            "message":{"content":"SECRET_TRANSCRIPT_CONTENT"}
        }"#,
    )
    .expect("recognized current record should parse");

    assert!(signal.is_activity());
    assert_eq!(signal.session_id(), Some("main-1"));
    assert_eq!(signal.git_branch(), Some("feat/claude-ingestion"));
    assert!(!format!("{signal:?}").contains("SECRET_TRANSCRIPT_CONTENT"));
}

#[test]
fn current_metadata_records_are_recognized_but_do_not_invent_activity() {
    for record in [
        br#"{"type":"agent-setting","agentSetting":"claudius:claudius","sessionId":"main-1"}"#
            .as_slice(),
        br#"{"type":"file-history-snapshot","snapshot":{"secret":"SECRET_TRANSCRIPT_CONTENT"}}"#
            .as_slice(),
        br#"{"type":"last-prompt","sessionId":"main-1","lastPrompt":"SECRET_TRANSCRIPT_CONTENT"}"#
            .as_slice(),
        br#"{"type":"mode","sessionId":"main-1","mode":"default"}"#.as_slice(),
        br#"{"type":"permission-mode","sessionId":"main-1","permissionMode":"default"}"#.as_slice(),
    ] {
        let signal = parse_transcript_record(record)
            .expect("current metadata-only record should be accepted");
        assert!(!signal.is_activity());
        assert!(!format!("{signal:?}").contains("SECRET_TRANSCRIPT_CONTENT"));
    }
}

#[test]
fn native_agent_titles_are_bounded_metadata_only() {
    let setting = parse_transcript_record(
        br#"{"type":"agent-setting","agentSetting":"claudius:claudius","sessionId":"main-1"}"#,
    )
    .expect("agent setting should parse");
    assert_eq!(setting.agent_setting(), Some("claudius:claudius"));

    let subagent = parse_subagent_metadata(
        br#"{"agentType":"security-reviewer","description":"SECRET_TRANSCRIPT_CONTENT"}"#,
    )
    .expect("subagent metadata should parse");
    assert_eq!(subagent.agent_type(), Some("security-reviewer"));
    assert!(!format!("{subagent:?}").contains("SECRET_TRANSCRIPT_CONTENT"));
}

#[test]
fn team_task_status_is_bounded_and_runtime_neutral() {
    let task = parse_task_record(
        br#"{"id":"7","subject":"Review implementation","status":"in_progress","owner":"bilby"}"#,
    )
    .expect("current task record should parse");

    assert_eq!(task.owner(), Some("bilby"));
    assert_eq!(task.state(), DetailedState::Running);
    assert_eq!(task.title(), Some("Review implementation"));
}

#[test]
fn unassigned_native_task_is_valid_but_has_no_session_owner() {
    let task = parse_task_record(br#"{"id":"8","subject":"Unclaimed work","status":"pending"}"#)
        .expect("unassigned task should remain valid");

    assert_eq!(task.owner(), None);
    assert_eq!(task.state(), DetailedState::Starting);
}
