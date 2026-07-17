use std::{fs, io::Write, time::Duration};

use agent_watchdog_phase0_probes::InotifyProbe;

#[test]
fn append_partial_rotation_and_truncation_emit_targeted_events() {
    let directory = tempfile::tempdir().expect("temporary watch root should exist");
    let probe = InotifyProbe::new(directory.path()).expect("watch should be created");
    let active = directory.path().join("session.jsonl");
    let rotated = directory.path().join("session.jsonl.1");

    let mut file = fs::File::create(&active).expect("transcript should be created");
    file.write_all(br#"{"state":"run"#)
        .expect("partial record should be written");
    file.write_all(b"ning\"}\n")
        .expect("record should be completed");
    drop(file);
    fs::rename(&active, &rotated).expect("transcript should rotate");
    fs::write(&active, b"{\"state\":\"idle\"}\n").expect("replacement should be written");
    fs::File::create(&active).expect("replacement should truncate");

    let counts = probe
        .collect_for(Duration::from_millis(250))
        .expect("queued events should be readable");

    assert!(counts.created >= 2);
    assert!(counts.modified >= 2);
    assert!(counts.closed_after_write >= 2);
    assert!(counts.moved_to >= 1);
    assert_eq!(counts.queue_overflows, 0);
}

#[test]
#[ignore = "explicit Phase 0 kernel queue-limit exercise"]
fn queue_overflow_is_reported_for_bounded_reconciliation() {
    let directory = tempfile::tempdir().expect("temporary watch root should exist");
    let probe = InotifyProbe::new(directory.path()).expect("watch should be created");

    for index in 0..25_000 {
        fs::File::create(directory.path().join(format!("event-{index}")))
            .expect("event file should be created");
    }

    let counts = probe
        .collect_for(Duration::from_millis(250))
        .expect("queued events should be readable");

    assert_eq!(counts.queue_overflows, 1);
}
