#![cfg(target_os = "linux")]
//! Bounded transcript cursor and partial-record acceptance tests.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use watchdog_runtime::{
    CapabilityRoot, FileCursor, IncrementalReader, ReadBudget, ReadOutcome, ReconcileReason,
};

const EIGHT_GIB: u64 = 8 * 1024 * 1024 * 1024;

fn reader() -> IncrementalReader {
    IncrementalReader::new(
        ReadBudget::new(64 * 1024, 4 * 1024, 100).expect("budget should be valid"),
    )
}

#[test]
fn huge_transcript_reads_only_the_appended_suffix() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    let path = root.path().join("session.jsonl");
    fs::File::create(&path)
        .expect("fixture should exist")
        .set_len(EIGHT_GIB)
        .expect("sparse fixture should resize");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");
    let cursor = reader()
        .cursor_at_end(&capability, Path::new("session.jsonl"), 1)
        .expect("initial cursor should be created");
    let mut writer = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("fixture should reopen");
    writer
        .write_all(b"{\"state\":\"running\"}\n")
        .expect("record should append");

    let outcome = reader()
        .read(&capability, Path::new("session.jsonl"), &cursor)
        .expect("append should be readable");
    let ReadOutcome::Records(batch) = outcome else {
        panic!("append should produce complete records");
    };

    assert_eq!(batch.records(), &[b"{\"state\":\"running\"}".to_vec()]);
    assert_eq!(batch.bytes_read(), 20);
    assert_eq!(batch.cursor().read_offset(), EIGHT_GIB + 20);
}

#[test]
fn partial_record_waits_for_completion() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    fs::write(root.path().join("session.jsonl"), b"{\"state\":")
        .expect("partial fixture should be written");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");
    let initial = reader()
        .cursor_at_start(&capability, Path::new("session.jsonl"), 1)
        .expect("initial cursor should be created");

    let first = reader()
        .read(&capability, Path::new("session.jsonl"), &initial)
        .expect("partial append should be readable");
    let ReadOutcome::Records(first) = first else {
        panic!("bounded partial record should remain buffered");
    };
    assert!(first.records().is_empty());
    assert_eq!(first.cursor().complete_offset(), 0);

    OpenOptions::new()
        .append(true)
        .open(root.path().join("session.jsonl"))
        .expect("fixture should reopen")
        .write_all(b"\"running\"}\n")
        .expect("record should complete");
    let second = reader()
        .read(&capability, Path::new("session.jsonl"), first.cursor())
        .expect("completed record should be readable");
    let ReadOutcome::Records(second) = second else {
        panic!("completed record should be emitted");
    };

    assert_eq!(second.records(), &[b"{\"state\":\"running\"}".to_vec()]);
    assert_eq!(second.cursor().complete_offset(), 20);
}

#[test]
fn truncation_and_replacement_require_reconciliation() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    let path = root.path().join("session.jsonl");
    fs::write(&path, b"one\ntwo\n").expect("fixture should be written");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");
    let cursor = reader()
        .cursor_at_end(&capability, Path::new("session.jsonl"), 1)
        .expect("cursor should be created");

    fs::File::create(&path).expect("fixture should truncate");
    let truncated = reader()
        .read(&capability, Path::new("session.jsonl"), &cursor)
        .expect("truncation should be classified");
    assert_eq!(
        truncated,
        ReadOutcome::ReconcileRequired(ReconcileReason::Truncated)
    );

    fs::write(root.path().join("replacement"), b"new\n").expect("replacement should be written");
    fs::rename(root.path().join("replacement"), &path).expect("fixture should be replaced");
    let replaced = reader()
        .read(&capability, Path::new("session.jsonl"), &cursor)
        .expect("replacement should be classified");
    assert_eq!(
        replaced,
        ReadOutcome::ReconcileRequired(ReconcileReason::Replaced)
    );
}

#[test]
fn oversized_partial_record_preserves_the_trusted_cursor() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    fs::write(root.path().join("session.jsonl"), vec![b'x'; 32])
        .expect("oversized fixture should be written");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");
    let bounded =
        IncrementalReader::new(ReadBudget::new(64, 8, 10).expect("small budget should be valid"));
    let cursor = bounded
        .cursor_at_start(&capability, Path::new("session.jsonl"), 1)
        .expect("cursor should be created");

    let outcome = bounded
        .read(&capability, Path::new("session.jsonl"), &cursor)
        .expect("oversized partial should be classified");

    assert_eq!(
        outcome,
        ReadOutcome::ReconcileRequired(ReconcileReason::PartialRecordTooLarge)
    );
    assert_eq!(cursor.read_offset(), 0);
}

#[test]
fn record_budget_requests_bounded_continuation() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    fs::write(root.path().join("session.jsonl"), b"one\ntwo\nthree\n")
        .expect("fixture should be written");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");
    let bounded =
        IncrementalReader::new(ReadBudget::new(64, 16, 1).expect("record budget should be valid"));
    let cursor = bounded
        .cursor_at_start(&capability, Path::new("session.jsonl"), 1)
        .expect("cursor should be created");

    let first = bounded
        .read(&capability, Path::new("session.jsonl"), &cursor)
        .expect("first record should be readable");
    let ReadOutcome::Records(first) = first else {
        panic!("record budget should return a bounded batch");
    };

    assert_eq!(first.records(), &[b"one".to_vec()]);
    assert!(first.continuation_required());
    assert_eq!(first.cursor().read_offset(), 4);
}

#[test]
fn restart_resumes_from_the_last_complete_boundary() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    fs::write(root.path().join("session.jsonl"), b"{\"state\":")
        .expect("partial fixture should be written");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");
    let initial = reader()
        .cursor_at_start(&capability, Path::new("session.jsonl"), 1)
        .expect("cursor should be created");
    let first = reader()
        .read(&capability, Path::new("session.jsonl"), &initial)
        .expect("partial record should be buffered");
    let ReadOutcome::Records(first) = first else {
        panic!("partial record should return a cursor");
    };
    let restarted = FileCursor::resume_from_complete(
        first.cursor().identity(),
        first.cursor().complete_offset(),
        first.cursor().parser_version(),
    );
    OpenOptions::new()
        .append(true)
        .open(root.path().join("session.jsonl"))
        .expect("fixture should reopen")
        .write_all(b"\"running\"}\n")
        .expect("record should complete");

    let second = reader()
        .read(&capability, Path::new("session.jsonl"), &restarted)
        .expect("complete record should be reread safely");
    let ReadOutcome::Records(second) = second else {
        panic!("completed record should be emitted after restart");
    };

    assert_eq!(second.records(), &[b"{\"state\":\"running\"}".to_vec()]);
}

#[test]
fn debug_output_never_contains_record_contents() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    fs::write(
        root.path().join("session.jsonl"),
        b"{\"token\":\"super-secret\"}\npartial-secret",
    )
    .expect("secret-shaped fixture should be written");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");
    let initial = reader()
        .cursor_at_start(&capability, Path::new("session.jsonl"), 1)
        .expect("cursor should be created");

    let outcome = reader()
        .read(&capability, Path::new("session.jsonl"), &initial)
        .expect("fixture should be readable");
    let debug = format!("{outcome:?}");

    assert!(!debug.contains("super-secret"));
    assert!(!debug.contains("partial-secret"));
    assert!(!debug.contains("token"));
}
