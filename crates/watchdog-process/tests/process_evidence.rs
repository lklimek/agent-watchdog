//! Synthetic process evidence and CPU delta acceptance tests.

use watchdog_domain::{BoundedText, ProcessId, ProcessIdentity};
use watchdog_process::{
    ActivityStrength, CommandFingerprint, CpuCounters, IoCounters, ProcStat, ProcessSample,
    ProcessState, ProcessTreeSnapshot, SampleUncertainty,
};

fn identity(pid: u32, start_time_ticks: u64, executable: &str) -> ProcessIdentity {
    ProcessIdentity::new(
        ProcessId::new(pid).expect("fixture PID should be positive"),
        start_time_ticks,
        BoundedText::new("executable", executable).expect("fixture executable should be bounded"),
    )
}

fn sample(
    pid: u32,
    parent_pid: u32,
    start_time_ticks: u64,
    cpu: CpuCounters,
    io: IoCounters,
) -> ProcessSample {
    ProcessSample::new(
        identity(pid, start_time_ticks, "/usr/bin/cargo"),
        ProcessId::new(parent_pid).ok(),
        ProcessState::Sleeping,
        cpu,
        Some(io),
        CommandFingerprint::from_redacted_cmdline(b"cargo\0test\0--workspace\0", false),
    )
}

#[test]
fn proc_stat_handles_spaces_and_parentheses_in_command() {
    let input = "321 (worker (phase 3)) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19";

    let parsed = ProcStat::parse(input).expect("valid proc stat should parse");

    assert_eq!(parsed.pid().value(), 321);
    assert_eq!(parsed.parent_pid().map(ProcessId::value), Some(1));
    assert_eq!(parsed.start_time_ticks(), 19);
    assert_eq!(parsed.state(), ProcessState::Sleeping);
    assert_eq!(parsed.cpu(), CpuCounters::new(11, 12, 13, 14));
}

#[test]
fn proc_io_uses_storage_bytes_not_untrusted_labels() {
    let input = "rchar: 10\nwchar: 20\nsyscr: 3\nsyscw: 4\nread_bytes: 50\nwrite_bytes: 60\ncancelled_write_bytes: 0\n";

    let counters = IoCounters::parse(input).expect("valid proc I/O should parse");

    assert_eq!(counters, IoCounters::new(50, 60));
}

#[test]
fn each_cpu_counter_class_records_activity() {
    let cases = [
        CpuCounters::new(1, 0, 0, 0),
        CpuCounters::new(0, 1, 0, 0),
        CpuCounters::new(0, 0, 1, 0),
        CpuCounters::new(0, 0, 0, 1),
    ];

    for delta in cases {
        let previous = ProcessTreeSnapshot::new(
            identity(10, 100, "/usr/bin/cargo"),
            [sample(
                10,
                1,
                100,
                CpuCounters::new(10, 20, 30, 40),
                IoCounters::new(100, 200),
            )],
            [],
        )
        .expect("previous tree should be valid");
        let current = ProcessTreeSnapshot::new(
            identity(10, 100, "/usr/bin/cargo"),
            [sample(
                10,
                1,
                100,
                CpuCounters::new(
                    10 + delta.user_ticks(),
                    20 + delta.system_ticks(),
                    30 + delta.children_user_ticks(),
                    40 + delta.children_system_ticks(),
                ),
                IoCounters::new(100, 200),
            )],
            [],
        )
        .expect("current tree should be valid");

        let activity = current.activity_since(&previous);

        assert_eq!(activity.strength(), ActivityStrength::Activity);
        assert_eq!(activity.cpu(), delta);
    }
}

#[test]
fn all_four_cpu_times_growing_is_strong_activity() {
    let previous = ProcessTreeSnapshot::new(
        identity(10, 100, "/usr/bin/cargo"),
        [sample(
            10,
            1,
            100,
            CpuCounters::new(10, 20, 30, 40),
            IoCounters::new(100, 200),
        )],
        [],
    )
    .expect("previous tree should be valid");
    let current = ProcessTreeSnapshot::new(
        identity(10, 100, "/usr/bin/cargo"),
        [sample(
            10,
            1,
            100,
            CpuCounters::new(11, 22, 33, 44),
            IoCounters::new(100, 200),
        )],
        [],
    )
    .expect("current tree should be valid");

    let activity = current.activity_since(&previous);

    assert_eq!(activity.strength(), ActivityStrength::StrongCpu);
    assert_eq!(activity.cpu(), CpuCounters::new(1, 2, 3, 4));
}

#[test]
fn unchanged_counters_are_neutral() {
    let tree = ProcessTreeSnapshot::new(
        identity(10, 100, "/usr/bin/cargo"),
        [sample(
            10,
            1,
            100,
            CpuCounters::new(10, 20, 30, 40),
            IoCounters::new(100, 200),
        )],
        [],
    )
    .expect("tree should be valid");

    let activity = tree.activity_since(&tree);

    assert_eq!(activity.strength(), ActivityStrength::Neutral);
    assert!(activity.uncertainties().is_empty());
}

#[test]
fn counter_decrease_is_uncertain_not_inactivity() {
    let previous = ProcessTreeSnapshot::new(
        identity(10, 100, "/usr/bin/cargo"),
        [sample(
            10,
            1,
            100,
            CpuCounters::new(10, 20, 30, 40),
            IoCounters::new(100, 200),
        )],
        [],
    )
    .expect("previous tree should be valid");
    let current = ProcessTreeSnapshot::new(
        identity(10, 100, "/usr/bin/cargo"),
        [sample(
            10,
            1,
            100,
            CpuCounters::new(9, 20, 30, 40),
            IoCounters::new(100, 200),
        )],
        [],
    )
    .expect("current tree should be valid");

    let activity = current.activity_since(&previous);

    assert_eq!(activity.strength(), ActivityStrength::Neutral);
    assert!(
        activity
            .uncertainties()
            .contains(&SampleUncertainty::CounterReset(
                ProcessId::new(10).expect("fixture PID should be positive")
            ))
    );
}

#[test]
fn live_descendant_activity_counts_for_the_tree() {
    let previous = ProcessTreeSnapshot::new(
        identity(10, 100, "/usr/bin/cargo"),
        [
            sample(
                10,
                1,
                100,
                CpuCounters::new(10, 20, 30, 40),
                IoCounters::new(100, 200),
            ),
            sample(
                11,
                10,
                110,
                CpuCounters::new(5, 6, 0, 0),
                IoCounters::new(50, 60),
            ),
        ],
        [],
    )
    .expect("previous tree should be valid");
    let current = ProcessTreeSnapshot::new(
        identity(10, 100, "/usr/bin/cargo"),
        [
            sample(
                10,
                1,
                100,
                CpuCounters::new(10, 20, 30, 40),
                IoCounters::new(100, 200),
            ),
            sample(
                11,
                10,
                110,
                CpuCounters::new(9, 6, 0, 0),
                IoCounters::new(50, 60),
            ),
        ],
        [],
    )
    .expect("current tree should be valid");

    let activity = current.activity_since(&previous);

    assert_eq!(activity.strength(), ActivityStrength::Activity);
    assert_eq!(activity.cpu().user_ticks(), 4);
}

#[test]
fn pid_start_time_change_is_uncertain() {
    let previous = ProcessTreeSnapshot::new(
        identity(10, 100, "/usr/bin/cargo"),
        [sample(
            10,
            1,
            100,
            CpuCounters::new(10, 20, 30, 40),
            IoCounters::new(100, 200),
        )],
        [],
    )
    .expect("previous tree should be valid");
    let current = ProcessTreeSnapshot::new(
        identity(10, 101, "/usr/bin/cargo"),
        [sample(
            10,
            1,
            101,
            CpuCounters::new(1, 2, 3, 4),
            IoCounters::new(1, 2),
        )],
        [],
    )
    .expect("current tree should be valid");

    let activity = current.activity_since(&previous);

    assert!(
        activity
            .uncertainties()
            .contains(&SampleUncertainty::IdentityChanged(
                ProcessId::new(10).expect("fixture PID should be positive")
            ))
    );
}

#[test]
fn new_descendant_is_activity_with_explicit_provenance() {
    let previous = ProcessTreeSnapshot::new(
        identity(10, 100, "/usr/bin/cargo"),
        [sample(
            10,
            1,
            100,
            CpuCounters::new(10, 20, 30, 40),
            IoCounters::new(100, 200),
        )],
        [],
    )
    .expect("previous tree should be valid");
    let current = ProcessTreeSnapshot::new(
        identity(10, 100, "/usr/bin/cargo"),
        [
            sample(
                10,
                1,
                100,
                CpuCounters::new(10, 20, 30, 40),
                IoCounters::new(100, 200),
            ),
            sample(
                11,
                10,
                110,
                CpuCounters::new(0, 0, 0, 0),
                IoCounters::new(0, 0),
            ),
        ],
        [],
    )
    .expect("current tree should be valid");

    let activity = current.activity_since(&previous);

    assert_eq!(activity.strength(), ActivityStrength::Activity);
    assert_eq!(activity.new_processes(), 1);
}

#[test]
fn command_fingerprint_never_retains_argument_contents() {
    let fingerprint = CommandFingerprint::from_redacted_cmdline(
        b"cargo\0test\0--token=super-secret-value\0",
        false,
    );

    let debug = format!("{fingerprint:?}");
    assert_eq!(fingerprint.argument_count(), 3);
    assert_eq!(fingerprint.observed_bytes(), 38);
    assert!(!debug.contains("super-secret-value"));
    assert!(!debug.contains("token"));
}
