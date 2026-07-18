#![cfg(target_os = "linux")]
//! Isolated Linux procfs and pidfd integration tests.

use std::{
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

use watchdog_domain::{BoundedText, ProcessId, ProcessIdentity};
use watchdog_process::{
    LinuxProcessControl, LinuxProcessSampler, ProcessControl, ProcessControlError, ProcessSignal,
    VerifiedProcessHandle,
};

#[derive(Debug)]
struct FakeHandle {
    identity: ProcessIdentity,
}

impl VerifiedProcessHandle for FakeHandle {
    fn identity(&self) -> &ProcessIdentity {
        &self.identity
    }

    fn signal(&self, _signal: ProcessSignal) -> Result<(), ProcessControlError> {
        Ok(())
    }
}

#[derive(Debug)]
struct FakeControl;

fn spawn_helper() -> Child {
    Command::new(std::env::current_exe().expect("test binary path should exist"))
        .args([
            "--exact",
            "purpose_built_process_helper",
            "--ignored",
            "--nocapture",
        ])
        .env("WATCHDOG_PROCESS_HELPER", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("purpose-built helper should start")
}

#[test]
#[ignore = "spawned explicitly by pidfd integration tests"]
fn purpose_built_process_helper() {
    assert_eq!(std::env::var("WATCHDOG_PROCESS_HELPER").as_deref(), Ok("1"));
    loop {
        thread::park_timeout(Duration::from_mins(1));
    }
}

impl ProcessControl for FakeControl {
    fn open_verified(
        &self,
        expected: &ProcessIdentity,
    ) -> Result<Box<dyn VerifiedProcessHandle>, ProcessControlError> {
        Ok(Box::new(FakeHandle {
            identity: expected.clone(),
        }))
    }
}

#[test]
fn pidfd_contract_has_a_side_effect_free_fake() {
    let identity = ProcessIdentity::new(
        ProcessId::new(42).expect("fixture PID should be valid"),
        100,
        BoundedText::new("executable", "/test/helper")
            .expect("fixture executable should be bounded"),
    );

    let handle = FakeControl
        .open_verified(&identity)
        .expect("fake should accept the identity");

    assert_eq!(handle.identity(), &identity);
    handle
        .signal(ProcessSignal::Terminate)
        .expect("fake signal should be recorded without a host side effect");
}

#[test]
fn linux_sampler_reads_only_the_selected_helper_tree() {
    let mut helper = spawn_helper();
    thread::sleep(Duration::from_millis(20));
    let sampler = LinuxProcessSampler::new(4_096).expect("sampling budget should be valid");
    let identity = sampler
        .read_identity(ProcessId::new(helper.id()).expect("helper PID should be valid"))
        .expect("helper identity should be readable");

    let snapshot = sampler
        .sample_tree(&identity)
        .expect("helper tree should be sampled");

    assert_eq!(snapshot.root(), &identity);
    assert!(snapshot.processes().contains_key(&identity.pid()));
    helper.kill().expect("helper should stop");
    helper.wait().expect("helper should be reaped");
}

#[test]
fn linux_sampler_builds_multiple_trees_from_one_bounded_capture() {
    let mut first = spawn_helper();
    let mut second = spawn_helper();
    thread::sleep(Duration::from_millis(20));
    let sampler = LinuxProcessSampler::new(4_096).expect("sampling budget should be valid");
    let identities = [&first, &second].map(|helper| {
        sampler
            .read_identity(ProcessId::new(helper.id()).expect("helper PID should be valid"))
            .expect("helper identity should be readable")
    });

    let snapshots = sampler
        .sample_trees(&identities)
        .expect("shared process-table capture should succeed");

    for identity in &identities {
        let snapshot = snapshots
            .get(&identity.pid())
            .expect("each root should have a result")
            .as_ref()
            .expect("each helper tree should sample");
        assert_eq!(snapshot.root(), identity);
        assert!(snapshot.processes().contains_key(&identity.pid()));
    }
    for helper in [&mut first, &mut second] {
        helper.kill().expect("helper should stop");
        helper.wait().expect("helper should be reaped");
    }
}

#[test]
fn verified_pidfd_signals_only_the_matching_helper() {
    let mut helper = spawn_helper();
    thread::sleep(Duration::from_millis(20));
    let sampler = LinuxProcessSampler::new(4_096).expect("sampling budget should be valid");
    let identity = sampler
        .read_identity(ProcessId::new(helper.id()).expect("helper PID should be valid"))
        .expect("helper identity should be readable");
    let control = LinuxProcessControl::new();

    let handle = control
        .open_verified(&identity)
        .expect("matching helper should produce a pidfd");
    handle
        .signal(ProcessSignal::Terminate)
        .expect("literal SIGTERM should be delivered through pidfd");
    let status = helper.wait().expect("helper should be reaped");

    assert!(!status.success());
}

#[test]
fn pidfd_open_rejects_stale_start_time() {
    let mut helper = spawn_helper();
    thread::sleep(Duration::from_millis(20));
    let sampler = LinuxProcessSampler::new(4_096).expect("sampling budget should be valid");
    let current = sampler
        .read_identity(ProcessId::new(helper.id()).expect("helper PID should be valid"))
        .expect("helper identity should be readable");
    let stale = ProcessIdentity::new(
        current.pid(),
        current.start_time_ticks().saturating_add(1),
        BoundedText::new("executable", current.executable())
            .expect("live executable should remain bounded"),
    );

    let error = LinuxProcessControl::new()
        .open_verified(&stale)
        .expect_err("stale identity must be rejected");

    assert!(matches!(
        error,
        ProcessControlError::IdentityMismatch { .. }
    ));
    assert!(
        helper
            .try_wait()
            .expect("helper status should be readable")
            .is_none()
    );
    helper.kill().expect("helper should stop");
    helper.wait().expect("helper should be reaped");
}
