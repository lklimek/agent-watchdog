#![cfg(target_os = "linux")]
//! Non-recursive bounded inotify service acceptance tests.

use std::{
    fs,
    path::Path,
    thread,
    time::{Duration, Instant},
};

use watchdog_runtime::{
    CapabilityRoot, WatchService, WatchSignal, WatchTargetId, WatchUncertainty,
};

fn wait_for_signal(service: &WatchService) -> WatchSignal {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(signal) = service.next_signal() {
            return signal;
        }
        assert!(Instant::now() < deadline, "watch signal should arrive");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn file_change_invalidates_only_its_registered_target() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");
    let target = WatchTargetId::new(7).expect("target ID should be valid");
    let mut service = WatchService::new(16).expect("watch service should start");
    service
        .add_target(target, &capability, Path::new(""))
        .expect("target should be watched");

    fs::write(root.path().join("session.jsonl"), b"record\n").expect("fixture should be written");

    let signal = wait_for_signal(&service);
    assert_eq!(signal, WatchSignal::Targets(vec![target]));
}

#[test]
fn local_queue_saturation_becomes_reconciliation_not_blocking() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");
    let target = WatchTargetId::new(9).expect("target ID should be valid");
    let mut service = WatchService::new(1).expect("watch service should start");
    service
        .add_target(target, &capability, Path::new(""))
        .expect("target should be watched");

    for index in 0..100 {
        fs::write(root.path().join(format!("event-{index}")), b"x")
            .expect("event fixture should be written");
    }
    thread::sleep(Duration::from_millis(100));

    assert_eq!(
        service.next_signal(),
        Some(WatchSignal::ReconcileAll(
            WatchUncertainty::LocalQueueSaturated
        ))
    );
}
