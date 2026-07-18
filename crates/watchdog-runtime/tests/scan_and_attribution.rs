#![cfg(target_os = "linux")]
//! Directory budget and shared-worktree attribution acceptance tests.

use std::{fs, path::Path};

use watchdog_domain::{ChildSessionId, NativeSessionKey, RuntimeKind, SessionId};
use watchdog_runtime::{
    Attribution, CapabilityRoot, DirectoryScanner, ScanBudget, ScanUncertainty, WorktreeOwners,
};

fn child(native_id: &str) -> ChildSessionId {
    let native = NativeSessionKey::new(RuntimeKind::CodexCli, native_id)
        .expect("fixture native ID should be valid");
    ChildSessionId::from(SessionId::from_native(&native))
}

#[test]
fn directory_scan_stops_at_the_entry_budget() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    for name in ["a", "b", "c"] {
        fs::create_dir(root.path().join(name)).expect("fixture directory should exist");
    }
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");
    let scanner =
        DirectoryScanner::new(ScanBudget::new(4, 2, 1_024).expect("scan budget should be valid"));

    let result = scanner
        .scan(&capability, Path::new(""))
        .expect("bounded scan should complete");

    assert_eq!(result.directories().len(), 2);
    assert_eq!(result.uncertainty(), Some(ScanUncertainty::EntryBudget));
}

#[test]
fn one_owner_receives_any_worktree_change() {
    let root = tempfile::tempdir().expect("temporary worktree should exist");
    let owner = child("one");
    let mut owners = WorktreeOwners::new();
    owners.register(root.path(), owner);

    assert_eq!(
        owners.attribute(root.path(), None),
        Attribution::Child(owner)
    );
}

#[test]
fn shared_worktree_is_neutral_without_process_evidence() {
    let root = tempfile::tempdir().expect("temporary worktree should exist");
    let first = child("one");
    let second = child("two");
    let mut owners = WorktreeOwners::new();
    owners.register(root.path(), first);
    owners.register(root.path(), second);

    assert_eq!(owners.attribute(root.path(), None), Attribution::Ambiguous);
    assert_eq!(
        owners.attribute(root.path(), Some(second)),
        Attribution::Child(second)
    );
}

#[test]
fn nested_change_uses_the_most_specific_registered_worktree() {
    let root = tempfile::tempdir().expect("temporary worktree should exist");
    let outer = child("outer");
    let nested = child("nested");
    let nested_root = root.path().join("packages/nested");
    let changed = nested_root.join("src/lib.rs");
    let mut owners = WorktreeOwners::new();
    owners.register(root.path(), outer);
    owners.register(&nested_root, nested);

    assert_eq!(owners.attribute(&changed, None), Attribution::Child(nested));
}
