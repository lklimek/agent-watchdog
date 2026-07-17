use std::{
    os::unix::process::ExitStatusExt,
    path::PathBuf,
    process::{Command, Stdio},
};

use agent_watchdog_phase0_probes::{
    CpuCounters, ExpectedProcessIdentity, ProcessControlError, ProcessIdentityMismatch,
    VerifiedPidFd, parse_proc_stat, read_process_identity, verify_process_identity,
};

#[test]
fn parse_proc_stat_handles_spaces_and_parentheses_in_command() {
    let input =
        "321 (worker (phase 0)) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24";

    let parsed = parse_proc_stat(input).expect("fixture should parse");

    assert_eq!(parsed.pid, 321);
    assert_eq!(parsed.parent_pid, 1);
    assert_eq!(parsed.start_time_ticks, 19);
    assert_eq!(
        parsed.cpu,
        CpuCounters {
            user_ticks: 11,
            system_ticks: 12,
            children_user_ticks: 13,
            children_system_ticks: 14,
        }
    );
}

#[test]
fn pidfd_open_rejects_reused_identity_without_signalling() {
    let mut child = Command::new("sleep")
        .arg("300")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("isolated sleep helper should start");
    let mut identity =
        read_process_identity(child.id()).expect("helper identity should be readable");
    identity.start_time_ticks += 1;

    let error = VerifiedPidFd::open(&identity).expect_err("changed identity must be rejected");

    assert!(matches!(
        error,
        ProcessControlError::Identity(ProcessIdentityMismatch::StartTime { .. })
    ));
    assert!(
        child
            .try_wait()
            .expect("helper status should be readable")
            .is_none()
    );
    child.kill().expect("test helper should be terminated");
    child.wait().expect("test helper should be reaped");
}

#[test]
fn verified_pidfd_terminates_matching_helper() {
    let mut child = Command::new("sleep")
        .arg("300")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("isolated sleep helper should start");
    let identity = read_process_identity(child.id()).expect("helper identity should be readable");
    let pidfd = VerifiedPidFd::open(&identity).expect("matching helper should open");

    pidfd.terminate().expect("pidfd SIGTERM should succeed");
    let status = child.wait().expect("test helper should be reaped");

    assert_eq!(status.signal(), Some(15));
}

#[test]
fn all_four_cpu_counters_growing_is_strong_activity() {
    let previous = CpuCounters {
        user_ticks: 10,
        system_ticks: 20,
        children_user_ticks: 30,
        children_system_ticks: 40,
    };
    let current = CpuCounters {
        user_ticks: 11,
        system_ticks: 22,
        children_user_ticks: 33,
        children_system_ticks: 44,
    };

    let activity = current
        .activity_since(previous)
        .expect("monotonic counters should compare");

    assert!(activity.any_grew());
    assert!(activity.all_four_grew());
}

#[test]
fn unchanged_cpu_counters_are_neutral() {
    let counters = CpuCounters {
        user_ticks: 10,
        system_ticks: 20,
        children_user_ticks: 30,
        children_system_ticks: 40,
    };

    let activity = counters
        .activity_since(counters)
        .expect("unchanged counters remain comparable");

    assert!(!activity.any_grew());
    assert!(!activity.all_four_grew());
}

#[test]
fn decreasing_cpu_counter_invalidates_comparison() {
    let previous = CpuCounters {
        user_ticks: 10,
        system_ticks: 20,
        children_user_ticks: 30,
        children_system_ticks: 40,
    };
    let current = CpuCounters {
        user_ticks: 9,
        ..previous
    };

    assert!(current.activity_since(previous).is_none());
}

#[test]
fn verify_process_identity_rejects_start_time_mismatch() {
    let expected = ExpectedProcessIdentity {
        pid: 42,
        start_time_ticks: 99,
        executable: PathBuf::from("/usr/bin/example"),
    };

    let error = verify_process_identity(&expected, 100, &PathBuf::from("/usr/bin/example"))
        .expect_err("a reused PID must be rejected");

    assert_eq!(
        error,
        ProcessIdentityMismatch::StartTime {
            expected: 99,
            actual: 100,
        }
    );
}

#[test]
fn verify_process_identity_rejects_executable_mismatch() {
    let expected = ExpectedProcessIdentity {
        pid: 42,
        start_time_ticks: 99,
        executable: PathBuf::from("/usr/bin/example"),
    };

    let error = verify_process_identity(&expected, 99, &PathBuf::from("/usr/bin/other"))
        .expect_err("a different executable must be rejected");

    assert_eq!(
        error,
        ProcessIdentityMismatch::Executable {
            expected: PathBuf::from("/usr/bin/example"),
            actual: PathBuf::from("/usr/bin/other"),
        }
    );
}
