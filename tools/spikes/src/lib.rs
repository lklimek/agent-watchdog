//! Reproducible Rust probes for Agent Watchdog Phase 0.

use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    mem::MaybeUninit,
    num::ParseIntError,
    os::fd::OwnedFd,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use rustix::{
    fs::inotify,
    io::Errno,
    process::{Pid, PidfdFlags, Signal, pidfd_open, pidfd_send_signal},
};
use thiserror::Error;

const STATE_INDEX: usize = 0;
const PARENT_PID_INDEX: usize = 1;
const USER_TICKS_INDEX: usize = 11;
const SYSTEM_TICKS_INDEX: usize = 12;
const CHILDREN_USER_TICKS_INDEX: usize = 13;
const CHILDREN_SYSTEM_TICKS_INDEX: usize = 14;
const START_TIME_INDEX: usize = 19;

/// Four cumulative CPU counters reported by Linux `/proc/<pid>/stat`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CpuCounters {
    pub user_ticks: u64,
    pub system_ticks: u64,
    pub children_user_ticks: u64,
    pub children_system_ticks: u64,
}

/// Monotonic deltas between two trustworthy CPU counter snapshots.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CpuActivity {
    pub user_ticks: u64,
    pub system_ticks: u64,
    pub children_user_ticks: u64,
    pub children_system_ticks: u64,
}

impl CpuCounters {
    /// Compare two cumulative snapshots, rejecting every counter reset.
    #[must_use]
    pub fn activity_since(self, previous: Self) -> Option<CpuActivity> {
        Some(CpuActivity {
            user_ticks: self.user_ticks.checked_sub(previous.user_ticks)?,
            system_ticks: self.system_ticks.checked_sub(previous.system_ticks)?,
            children_user_ticks: self
                .children_user_ticks
                .checked_sub(previous.children_user_ticks)?,
            children_system_ticks: self
                .children_system_ticks
                .checked_sub(previous.children_system_ticks)?,
        })
    }
}

impl CpuActivity {
    /// Return whether any trustworthy counter grew.
    #[must_use]
    pub fn any_grew(self) -> bool {
        self.user_ticks > 0
            || self.system_ticks > 0
            || self.children_user_ticks > 0
            || self.children_system_ticks > 0
    }

    /// Return whether all four counter classes grew.
    #[must_use]
    pub fn all_four_grew(self) -> bool {
        self.user_ticks > 0
            && self.system_ticks > 0
            && self.children_user_ticks > 0
            && self.children_system_ticks > 0
    }
}

/// Process fields needed to correlate activity and prevent PID-reuse signals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcStat {
    pub pid: u32,
    pub parent_pid: u32,
    pub start_time_ticks: u64,
    pub cpu: CpuCounters,
}

/// Errors produced while parsing a Linux process stat record.
#[derive(Debug, Error)]
pub enum ProcStatError {
    #[error("Process stat record has no command boundary")]
    MissingCommandBoundary,
    #[error("Process stat record is missing field {field}")]
    MissingField { field: &'static str },
    #[error("Process stat field {field} is invalid")]
    InvalidField {
        field: &'static str,
        #[source]
        source: ParseIntError,
    },
    #[error("Process stat state field is empty")]
    EmptyState,
}

/// Expected identity used immediately before opening a pidfd.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpectedProcessIdentity {
    pub pid: u32,
    pub start_time_ticks: u64,
    pub executable: PathBuf,
}

/// A process no longer matches the identity selected by the coordinator.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ProcessIdentityMismatch {
    #[error("Process start time changed from {expected} to {actual}")]
    StartTime { expected: u64, actual: u64 },
    #[error("Process executable changed from {expected:?} to {actual:?}")]
    Executable { expected: PathBuf, actual: PathBuf },
}

/// Errors returned while reading, verifying, or signalling a process.
#[derive(Debug, Error)]
pub enum ProcessControlError {
    #[error("Failed to read process stat for PID {pid}")]
    ReadStat {
        pid: u32,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to read process executable for PID {pid}")]
    ReadExecutable {
        pid: u32,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse process stat for PID {pid}")]
    ParseStat {
        pid: u32,
        #[source]
        source: ProcStatError,
    },
    #[error(transparent)]
    Identity(#[from] ProcessIdentityMismatch),
    #[error("PID {pid} is outside the supported Linux process range")]
    InvalidPid { pid: u32 },
    #[error("Failed to open pidfd for PID {pid}")]
    OpenPidFd {
        pid: u32,
        #[source]
        source: Errno,
    },
    #[error("Failed to send SIGTERM through pidfd")]
    SendSignal(#[source] Errno),
}

/// A pidfd opened only after fresh start-time and executable verification.
#[derive(Debug)]
pub struct VerifiedPidFd {
    descriptor: OwnedFd,
}

/// Aggregated filesystem events used to size reconciliation behavior.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InotifyCounts {
    pub total: u64,
    pub created: u64,
    pub modified: u64,
    pub closed_after_write: u64,
    pub moved_to: u64,
    pub deleted: u64,
    pub queue_overflows: u64,
}

/// A nonblocking inotify watch used by the Phase 0 filesystem probe.
#[derive(Debug)]
pub struct InotifyProbe {
    descriptor: OwnedFd,
}

/// Result of one cursor-based bounded file read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuffixRead {
    Appended {
        bytes: Vec<u8>,
        next_offset: u64,
        at_eof: bool,
    },
    Truncated {
        file_len: u64,
    },
}

/// Inputs for the synthetic observation-loop capacity probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityConfig {
    /// Number of main sessions in the population.
    pub main_sessions: usize,
    /// Number of main and child sessions in the population.
    pub total_agents: usize,
    /// Number of observations sent through the bounded queue.
    pub observations: usize,
    /// Maximum observations buffered between producer and reducer.
    pub queue_capacity: usize,
}

/// Measurements produced by one synthetic capacity run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityResult {
    /// Observations applied by the reducer.
    pub processed_observations: usize,
    /// Agents that received at least one observation.
    pub converged_agents: usize,
    /// Largest observations in flight: queue, consumer handoff, and blocked producer.
    pub peak_queue_occupancy: usize,
    /// End-to-end wall-clock duration.
    pub elapsed: Duration,
    /// Median observation queue latency.
    pub latency_p50: Duration,
    /// 95th percentile observation queue latency.
    pub latency_p95: Duration,
    /// 99th percentile observation queue latency.
    pub latency_p99: Duration,
    /// Process CPU-counter deltas when procfs remained comparable.
    pub cpu: Option<CpuActivity>,
    /// Resident memory at the start of the run, in KiB.
    pub rss_before_kib: u64,
    /// Resident memory after the run, in KiB.
    pub rss_after_kib: u64,
}

/// Failures produced by the synthetic capacity probe.
#[derive(Debug, Error)]
pub enum CapacityProbeError {
    /// The requested population or queue cannot model the target workload.
    #[error("Invalid capacity configuration")]
    InvalidConfiguration,
    /// Procfs data needed for a measurement could not be read.
    #[error("Failed to read capacity measurement from procfs")]
    Procfs(#[source] std::io::Error),
    /// The observation consumer stopped before the producer finished.
    #[error("Capacity probe consumer stopped unexpectedly")]
    ConsumerStopped,
    /// The observation consumer panicked.
    #[error("Capacity probe consumer panicked")]
    ConsumerPanicked,
}

#[derive(Clone, Copy, Debug)]
struct SyntheticObservation {
    agent_index: usize,
    sequence: u64,
    emitted_at: Instant,
}

/// Parse the Linux stat fields needed by the process sampler.
///
/// # Errors
///
/// Returns [`ProcStatError`] when the record is malformed or a required field
/// cannot be parsed.
pub fn parse_proc_stat(input: &str) -> Result<ProcStat, ProcStatError> {
    let command_end = input
        .rfind(") ")
        .ok_or(ProcStatError::MissingCommandBoundary)?;
    let pid_text = input
        .get(
            ..input
                .find(' ')
                .ok_or(ProcStatError::MissingField { field: "pid" })?,
        )
        .ok_or(ProcStatError::MissingField { field: "pid" })?;
    let fields = input
        .get(command_end + 2..)
        .ok_or(ProcStatError::MissingCommandBoundary)?
        .split_ascii_whitespace()
        .collect::<Vec<_>>();

    let state = field(&fields, STATE_INDEX, "state")?;
    if state.is_empty() {
        return Err(ProcStatError::EmptyState);
    }

    Ok(ProcStat {
        pid: parse_field(pid_text, "pid")?,
        parent_pid: parse_at(&fields, PARENT_PID_INDEX, "ppid")?,
        start_time_ticks: parse_at(&fields, START_TIME_INDEX, "starttime")?,
        cpu: CpuCounters {
            user_ticks: parse_at(&fields, USER_TICKS_INDEX, "utime")?,
            system_ticks: parse_at(&fields, SYSTEM_TICKS_INDEX, "stime")?,
            children_user_ticks: parse_at(&fields, CHILDREN_USER_TICKS_INDEX, "cutime")?,
            children_system_ticks: parse_at(&fields, CHILDREN_SYSTEM_TICKS_INDEX, "cstime")?,
        },
    })
}

/// Verify fresh process facts against the coordinator's selected identity.
///
/// # Errors
///
/// Returns [`ProcessIdentityMismatch`] when the start time or executable no
/// longer matches the selected identity.
pub fn verify_process_identity(
    expected: &ExpectedProcessIdentity,
    actual_start_time_ticks: u64,
    actual_executable: &Path,
) -> Result<(), ProcessIdentityMismatch> {
    if expected.start_time_ticks != actual_start_time_ticks {
        return Err(ProcessIdentityMismatch::StartTime {
            expected: expected.start_time_ticks,
            actual: actual_start_time_ticks,
        });
    }
    if expected.executable != actual_executable {
        return Err(ProcessIdentityMismatch::Executable {
            expected: expected.executable.clone(),
            actual: actual_executable.to_path_buf(),
        });
    }
    Ok(())
}

/// Read a process identity from Linux procfs without inspecting its arguments.
///
/// # Errors
///
/// Returns [`ProcessControlError`] when procfs cannot be read or parsed.
pub fn read_process_identity(pid: u32) -> Result<ExpectedProcessIdentity, ProcessControlError> {
    let stat_text = fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|source| ProcessControlError::ReadStat { pid, source })?;
    let stat = parse_proc_stat(&stat_text)
        .map_err(|source| ProcessControlError::ParseStat { pid, source })?;
    let executable = fs::read_link(format!("/proc/{pid}/exe"))
        .map_err(|source| ProcessControlError::ReadExecutable { pid, source })?;

    Ok(ExpectedProcessIdentity {
        pid,
        start_time_ticks: stat.start_time_ticks,
        executable,
    })
}

impl VerifiedPidFd {
    /// Re-read identity and open a pidfd only when both facts still match.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessControlError`] when identity verification or pidfd
    /// creation fails.
    pub fn open(expected: &ExpectedProcessIdentity) -> Result<Self, ProcessControlError> {
        let actual = read_process_identity(expected.pid)?;
        verify_process_identity(
            expected,
            actual.start_time_ticks,
            actual.executable.as_path(),
        )?;
        let raw_pid = i32::try_from(expected.pid)
            .ok()
            .and_then(Pid::from_raw)
            .ok_or(ProcessControlError::InvalidPid { pid: expected.pid })?;
        let descriptor = pidfd_open(raw_pid, PidfdFlags::empty()).map_err(|source| {
            ProcessControlError::OpenPidFd {
                pid: expected.pid,
                source,
            }
        })?;
        Ok(Self { descriptor })
    }

    /// Send SIGTERM to the verified process through its stable pidfd.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessControlError`] when the kernel rejects the signal.
    pub fn terminate(&self) -> Result<(), ProcessControlError> {
        pidfd_send_signal(&self.descriptor, Signal::TERM).map_err(ProcessControlError::SendSignal)
    }
}

impl InotifyProbe {
    /// Open one nonblocking watch for every event beneath a directory entry.
    ///
    /// # Errors
    ///
    /// Returns [`Errno`] when the inotify instance or watch cannot be created.
    pub fn new(path: &Path) -> Result<Self, Errno> {
        let descriptor =
            inotify::init(inotify::CreateFlags::CLOEXEC | inotify::CreateFlags::NONBLOCK)?;
        inotify::add_watch(&descriptor, path, inotify::WatchFlags::ALL_EVENTS)?;
        Ok(Self { descriptor })
    }

    /// Drain and aggregate queued events for a bounded duration.
    ///
    /// # Errors
    ///
    /// Returns [`Errno`] when the event queue cannot be read.
    pub fn collect_for(self, duration: Duration) -> Result<InotifyCounts, Errno> {
        let mut buffer = vec![MaybeUninit::<u8>::uninit(); 64 * 1024].into_boxed_slice();
        let mut reader = inotify::Reader::new(&self.descriptor, &mut buffer);
        let mut counts = InotifyCounts::default();
        let deadline = Instant::now() + duration;

        while Instant::now() < deadline {
            match reader.next() {
                Ok(event) => counts.record(event.events()),
                Err(Errno::AGAIN) => thread::sleep(Duration::from_millis(5)),
                Err(error) => return Err(error),
            }
        }
        Ok(counts)
    }
}

impl InotifyCounts {
    fn record(&mut self, flags: inotify::ReadFlags) {
        self.total += 1;
        self.created += u64::from(flags.contains(inotify::ReadFlags::CREATE));
        self.modified += u64::from(flags.contains(inotify::ReadFlags::MODIFY));
        self.closed_after_write += u64::from(flags.contains(inotify::ReadFlags::CLOSE_WRITE));
        self.moved_to += u64::from(flags.contains(inotify::ReadFlags::MOVED_TO));
        self.deleted += u64::from(flags.contains(inotify::ReadFlags::DELETE));
        self.queue_overflows += u64::from(flags.contains(inotify::ReadFlags::QUEUE_OVERFLOW));
    }
}

/// Read at most `byte_budget` bytes beginning at a previously saved cursor.
///
/// # Errors
///
/// Returns [`std::io::Error`] when metadata, seeking, or reading fails.
pub fn read_bounded_suffix(
    path: &Path,
    offset: u64,
    byte_budget: usize,
) -> std::io::Result<SuffixRead> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if offset > file_len {
        return Ok(SuffixRead::Truncated { file_len });
    }

    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = Vec::with_capacity(byte_budget.min(64 * 1024));
    file.by_ref()
        .take(u64::try_from(byte_budget).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)?;
    let next_offset = offset + u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    let current_len = file.metadata()?.len();
    Ok(SuffixRead::Appended {
        bytes,
        next_offset,
        at_eof: next_offset >= current_len,
    })
}

/// Run a bounded producer/reducer loop for a synthetic multi-agent population.
///
/// The occupancy measurement includes the bounded channel plus at most one
/// consumer handoff and one blocked producer. Production uses this same shape:
/// bounded ingestion plus a single reducer owner, without one task per session.
///
/// # Errors
///
/// Returns [`CapacityProbeError`] when the configuration is invalid, procfs
/// cannot be sampled, or the reducer thread fails.
pub fn run_capacity_probe(config: CapacityConfig) -> Result<CapacityResult, CapacityProbeError> {
    if config.main_sessions == 0
        || config.total_agents < config.main_sessions
        || config.observations < config.total_agents
        || config.queue_capacity == 0
    {
        return Err(CapacityProbeError::InvalidConfiguration);
    }

    let rss_before_kib = read_self_rss_kib().map_err(CapacityProbeError::Procfs)?;
    let cpu_before = read_self_cpu().map_err(CapacityProbeError::Procfs)?;
    let started = Instant::now();
    let queued = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = mpsc::sync_channel::<SyntheticObservation>(config.queue_capacity);

    let consumer_queued = Arc::clone(&queued);
    let consumer = thread::spawn(move || {
        let mut states = vec![0_u64; config.total_agents];
        let mut latencies = Vec::with_capacity(config.observations);
        while let Ok(observation) = receiver.recv() {
            consumer_queued.fetch_sub(1, Ordering::Relaxed);
            states[observation.agent_index] = observation.sequence;
            latencies.push(observation.emitted_at.elapsed());
        }
        (states, latencies)
    });

    for index in 0..config.observations {
        let occupancy = queued.fetch_add(1, Ordering::Relaxed) + 1;
        peak.fetch_max(occupancy, Ordering::Relaxed);
        let sequence = u64::try_from(index / config.total_agents + 1).unwrap_or(u64::MAX);
        let observation = SyntheticObservation {
            agent_index: index % config.total_agents,
            sequence,
            emitted_at: Instant::now(),
        };
        if sender.send(observation).is_err() {
            return Err(CapacityProbeError::ConsumerStopped);
        }
    }
    drop(sender);

    let (states, mut latencies) = consumer
        .join()
        .map_err(|_| CapacityProbeError::ConsumerPanicked)?;
    latencies.sort_unstable();
    let cpu_after = read_self_cpu().map_err(CapacityProbeError::Procfs)?;
    let rss_after_kib = read_self_rss_kib().map_err(CapacityProbeError::Procfs)?;

    Ok(CapacityResult {
        processed_observations: latencies.len(),
        converged_agents: states.iter().filter(|sequence| **sequence > 0).count(),
        peak_queue_occupancy: peak.load(Ordering::Relaxed),
        elapsed: started.elapsed(),
        latency_p50: percentile(&latencies, 50),
        latency_p95: percentile(&latencies, 95),
        latency_p99: percentile(&latencies, 99),
        cpu: cpu_after.activity_since(cpu_before),
        rss_before_kib,
        rss_after_kib,
    })
}

fn read_self_cpu() -> std::io::Result<CpuCounters> {
    let input = fs::read_to_string("/proc/self/stat")?;
    parse_proc_stat(&input)
        .map(|stat| stat.cpu)
        .map_err(std::io::Error::other)
}

fn read_self_rss_kib() -> std::io::Result<u64> {
    let status = fs::read_to_string("/proc/self/status")?;
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|line| line.split_ascii_whitespace().next())
        .ok_or_else(|| std::io::Error::other("VmRSS is missing"))?;
    value.parse().map_err(std::io::Error::other)
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let last = sorted.len().saturating_sub(1);
    let index = last.saturating_mul(percentile).div_ceil(100);
    sorted.get(index).copied().unwrap_or_default()
}

fn field<'a>(
    fields: &'a [&str],
    index: usize,
    name: &'static str,
) -> Result<&'a str, ProcStatError> {
    fields
        .get(index)
        .copied()
        .ok_or(ProcStatError::MissingField { field: name })
}

fn parse_at<T>(fields: &[&str], index: usize, name: &'static str) -> Result<T, ProcStatError>
where
    T: std::str::FromStr<Err = ParseIntError>,
{
    parse_field(field(fields, index, name)?, name)
}

fn parse_field<T>(value: &str, name: &'static str) -> Result<T, ProcStatError>
where
    T: std::str::FromStr<Err = ParseIntError>,
{
    value.parse().map_err(|source| ProcStatError::InvalidField {
        field: name,
        source,
    })
}
