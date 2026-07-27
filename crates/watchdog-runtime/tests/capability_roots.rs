//! Capability-root path containment acceptance tests.
#![cfg(target_os = "linux")]

use std::{
    ffi::OsString,
    fs,
    io::Read,
    os::unix::{ffi::OsStringExt, fs::symlink},
    path::Path,
};

use watchdog_runtime::{CapabilityRoot, PathAccessError};

#[test]
fn capability_opens_an_in_root_file() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    fs::write(root.path().join("session.jsonl"), b"record\n").expect("fixture should be written");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");

    let mut file = capability
        .open_file(Path::new("session.jsonl"))
        .expect("in-root file should open");
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)
        .expect("in-root file should be readable");

    assert_eq!(contents, b"record\n");
}

#[test]
fn parent_traversal_is_rejected_before_access() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");

    let error = capability
        .open_file(Path::new("../outside"))
        .expect_err("parent traversal must be rejected");

    assert_eq!(error, PathAccessError::InvalidRelativePath);
}

#[test]
fn symlink_escape_is_rejected_by_kernel_resolution() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    let outside = tempfile::tempdir().expect("outside directory should exist");
    fs::write(outside.path().join("secret"), b"not readable")
        .expect("outside fixture should be written");
    symlink(outside.path(), root.path().join("escape")).expect("escape symlink should exist");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");

    let error = capability
        .open_file(Path::new("escape/secret"))
        .expect_err("symlink escape must be rejected");

    assert!(matches!(error, PathAccessError::KernelRejected));
}

#[test]
fn non_utf8_names_remain_bounded_and_supported() {
    let root = tempfile::tempdir().expect("temporary root should exist");
    let name = OsString::from_vec(vec![b's', b'e', 0xff, b's']);
    fs::write(root.path().join(&name), b"record").expect("fixture should be written");
    let capability = CapabilityRoot::new(root.path()).expect("root should be accepted");

    let mut file = capability
        .open_file(Path::new(&name))
        .expect("non-UTF-8 in-root file should open");
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)
        .expect("fixture should be readable");

    assert_eq!(contents, b"record");
}
