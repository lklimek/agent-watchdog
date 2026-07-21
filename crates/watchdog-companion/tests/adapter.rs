//! Codex Companion persisted-state compatibility tests.

use watchdog_companion::{
    CompanionConsistency, CompanionParseError, CompanionParser, CompanionSource,
};
use watchdog_domain::{DetailedState, ObservationPayload, TimePoint, WallTimeMs};

fn now() -> TimePoint {
    TimePoint::new(WallTimeMs::new(4_000), 1_200)
}

#[test]
fn summary_exposes_workspace_session_pid_phase_and_status() {
    let parser = CompanionParser::new("1.0.6").expect("version should be valid");
    let snapshot = parser
        .parse_summary(
            br#"{
            "version":1,
            "config":{"stopReviewGate":false},
            "jobs":[{
                "id":"task-a1",
                "kind":"task",
                "title":"Review the reducer",
                "workspaceRoot":"/work/tree",
                "sessionId":"claude-main-1",
                "status":"running",
                "phase":"verifying",
                "pid":4242,
                "threadId":"codex-thread-1",
                "turnId":"turn-1",
                "updatedAt":"2026-07-17T16:00:00Z"
            }]
        }"#,
        )
        .expect("current summary should parse");

    let job = &snapshot.jobs()[0];
    assert_eq!(job.subject().native_id(), "task-a1");
    assert_eq!(
        job.parent().expect("session should exist").native_id(),
        "claude-main-1"
    );
    assert_eq!(job.pid(), Some(4242));
    assert_eq!(job.phase(), Some("verifying"));
    assert_eq!(job.state(), DetailedState::Running);
    assert_eq!(job.workspace_root().to_str(), Some("/work/tree"));
    assert!(job.graceful_cancellation().is_some());
}

#[test]
fn cancelled_summary_remains_distinct_from_completed() {
    let parser = CompanionParser::new("1.0.6").expect("version should be valid");
    let snapshot = parser
        .parse_summary(
            br#"{"version":1,"jobs":[{
                "id":"task-cancelled","workspaceRoot":"/work/tree","status":"cancelled"
            }]}"#,
        )
        .expect("cancelled summary should parse");

    assert_eq!(snapshot.jobs()[0].state(), DetailedState::Cancelled);
}

#[test]
fn non_atomic_active_terminal_pair_becomes_unknown_not_false_completion() {
    let parser = CompanionParser::new("1.0.6").expect("version should be valid");
    let summary = parser
        .parse_summary(
            br#"{"version":1,"jobs":[{
            "id":"task-a1","workspaceRoot":"/work/tree","status":"running",
            "phase":"running","pid":4242,"updatedAt":"later"
        }]}"#,
        )
        .expect("summary should parse");
    let detail = parser
        .parse_detail(
            br#"{
            "id":"task-a1","workspaceRoot":"/work/tree","status":"completed",
            "phase":"done","pid":null,"completedAt":"now","result":{"secret":"DO_NOT_LOG"}
        }"#,
        )
        .expect("detail should parse without retaining result");
    let reconciled = parser
        .reconcile(Some(&summary.jobs()[0]), Some(&detail))
        .expect("same job should reconcile conservatively");

    assert_eq!(reconciled.state(), DetailedState::Unknown);
    assert_eq!(reconciled.consistency(), CompanionConsistency::Conflicted);
    assert!(!format!("{reconciled:?}").contains("DO_NOT_LOG"));

    let reverse_summary = parser
        .parse_summary(
            br#"{"version":1,"jobs":[{
            "id":"task-a1","workspaceRoot":"/work/tree","status":"completed",
            "phase":"done","pid":null,"updatedAt":"later"
        }]}"#,
        )
        .expect("terminal summary should parse");
    let reverse_detail = parser
        .parse_detail(
            br#"{
            "id":"task-a1","workspaceRoot":"/work/tree","status":"running",
            "phase":"finalizing","pid":4242
        }"#,
        )
        .expect("active detail should parse");
    let reverse = parser
        .reconcile(Some(&reverse_summary.jobs()[0]), Some(&reverse_detail))
        .expect("reverse write ordering should also be conservative");
    assert_eq!(reverse.state(), DetailedState::Unknown);
    assert_eq!(reverse.consistency(), CompanionConsistency::Conflicted);
}

#[test]
fn consistent_terminal_pair_emits_terminal_observation() {
    let parser = CompanionParser::new("1.0.6").expect("version should be valid");
    let summary = parser
        .parse_summary(
            br#"{"version":1,"jobs":[{
            "id":"review-a1","workspaceRoot":"/work/tree","status":"failed",
            "phase":"failed","pid":null,"errorMessage":"bounded failure"
        }]}"#,
        )
        .expect("summary should parse");
    let detail = parser
        .parse_detail(
            br#"{
            "id":"review-a1","workspaceRoot":"/work/tree","status":"failed",
            "phase":"failed","pid":null,"errorMessage":"bounded failure"
        }"#,
        )
        .expect("detail should parse");
    let reconciled = parser
        .reconcile(Some(&summary.jobs()[0]), Some(&detail))
        .expect("matching terminal state should reconcile");
    let observation = parser
        .observation(&reconciled, "job-event-1", now())
        .expect("observation should be bounded");

    assert_eq!(reconciled.state(), DetailedState::Failed);
    assert_eq!(reconciled.consistency(), CompanionConsistency::Consistent);
    assert!(matches!(
        observation.payload(),
        ObservationPayload::NativeState(DetailedState::Failed)
    ));
}

#[test]
fn summary_only_job_survives_missing_pruned_detail() {
    let parser = CompanionParser::new("1.0.6").expect("version should be valid");
    let summary = parser
        .parse_summary(
            br#"{"version":1,"jobs":[{
            "id":"task-old","workspaceRoot":"/work/tree","status":"completed","phase":"done"
        }]}"#,
        )
        .expect("summary should parse");
    let reconciled = parser
        .reconcile(Some(&summary.jobs()[0]), None)
        .expect("missing detail is normal under pruning");

    assert_eq!(reconciled.state(), DetailedState::Completed);
    assert_eq!(reconciled.source(), CompanionSource::Summary);
}

#[test]
fn schema_drift_is_actionable_and_does_not_invent_state() {
    let parser = CompanionParser::new("future").expect("version should be valid");
    let error = parser
        .parse_summary(br#"{"version":2,"jobs":[]}"#)
        .expect_err("unknown summary version should fail closed");

    assert!(matches!(error, CompanionParseError::UnsupportedVersion));
    assert_eq!(error.compatibility_warning().badge(), "UPGRADE");
}

#[test]
fn log_append_is_activity_without_reading_native_log_content() {
    let parser = CompanionParser::new("1.0.6").expect("version should be valid");
    let summary = parser
        .parse_summary(
            br#"{"version":1,"jobs":[{
            "id":"task-log","workspaceRoot":"/work/tree","status":"running","phase":"running"
        }]}"#,
        )
        .expect("summary should parse");
    let observation = parser
        .log_activity(summary.jobs()[0].subject(), "log-append-1", now())
        .expect("registered log append should produce activity");

    assert!(matches!(
        observation.payload(),
        ObservationPayload::Progress(_)
    ));
}
