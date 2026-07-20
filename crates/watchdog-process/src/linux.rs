use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File},
    io::{self, Read},
    os::fd::OwnedFd,
    path::PathBuf,
};

use rustix::{
    io::Errno,
    process::{Pid, PidfdFlags, Signal, pidfd_open, pidfd_send_signal},
};
use thiserror::Error;
use watchdog_domain::{BoundedText, DomainInputError, ProcessId, ProcessIdentity};

use crate::{
    CommandFingerprint, IoCounters, ProcStat, ProcStatParseError, ProcessSample, ProcessTreeError,
    ProcessTreeSnapshot, SampleUncertainty,
};

const PROC_ROOT: &str = "/proc";
const MAX_STAT_BYTES: u64 = 64 * 1024;
const MAX_IO_BYTES: u64 = 64 * 1024;
const MAX_COMMAND_BYTES: u64 = 16 * 1024;
const MAX_COMM_BYTES: u64 = 512;

/// Linux procfs sampler with a hard process-enumeration budget.
#[derive(Clone, Copy, Debug)]
pub struct LinuxProcessSampler {
    max_processes: usize,
}

impl LinuxProcessSampler {
    /// Configure the maximum process-table entries inspected per sample.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessReadError::InvalidBudget`] when the limit is zero.
    pub const fn new(max_processes: usize) -> Result<Self, ProcessReadError> {
        if max_processes == 0 {
            return Err(ProcessReadError::InvalidBudget);
        }
        Ok(Self { max_processes })
    }

    /// Read a fresh PID/start-time/executable identity.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessReadError`] when the process vanishes, procfs is
    /// unreadable, or the executable cannot be represented safely.
    pub fn read_identity(&self, pid: ProcessId) -> Result<ProcessIdentity, ProcessReadError> {
        let stat = read_stat(pid)?;
        let executable = read_executable(pid)?;
        Ok(ProcessIdentity::new(
            stat.pid(),
            stat.start_time_ticks(),
            executable,
        ))
    }

    /// Sample the selected root and descendants with bounded procfs work.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessReadError`] when the selected root cannot be freshly
    /// verified or the resulting tree is internally inconsistent.
    pub fn sample_tree(
        &self,
        expected_root: &ProcessIdentity,
    ) -> Result<ProcessTreeSnapshot, ProcessReadError> {
        let (stats, uncertainties) = self.enumerate_stats()?;
        self.sample_tree_from_stats(expected_root, &stats, &uncertainties)
    }

    /// Sample multiple independently verified roots from one bounded process-table capture.
    ///
    /// Root executable/PID identities and per-process I/O remain freshly read,
    /// but `/proc` directory enumeration and parent relationships are shared.
    /// One root failure is isolated in its map entry.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessReadError`] when the shared process table cannot be enumerated.
    pub fn sample_trees(
        &self,
        expected_roots: &[ProcessIdentity],
    ) -> Result<BTreeMap<ProcessId, Result<ProcessTreeSnapshot, ProcessReadError>>, ProcessReadError>
    {
        let (stats, uncertainties) = self.enumerate_stats()?;
        Ok(expected_roots
            .iter()
            .map(|root| {
                (
                    root.pid(),
                    self.sample_tree_from_stats(root, &stats, &uncertainties),
                )
            })
            .collect())
    }

    fn enumerate_stats(
        self,
    ) -> Result<(BTreeMap<ProcessId, ProcStat>, BTreeSet<SampleUncertainty>), ProcessReadError>
    {
        let mut uncertainties = BTreeSet::new();
        let mut stats = BTreeMap::new();
        let entries = fs::read_dir(PROC_ROOT).map_err(ProcessReadError::Enumerate)?;
        let mut inspected = 0_usize;
        for entry in entries {
            if inspected >= self.max_processes {
                uncertainties.insert(SampleUncertainty::EnumerationIncomplete);
                break;
            }
            let Ok(entry) = entry else {
                uncertainties.insert(SampleUncertainty::EnumerationIncomplete);
                continue;
            };
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
                .and_then(|value| ProcessId::new(value).ok())
            else {
                continue;
            };
            inspected = inspected.saturating_add(1);
            if stats.contains_key(&pid) {
                continue;
            }
            if let Ok(stat) = read_stat(pid) {
                stats.insert(pid, stat);
            }
        }
        Ok((stats, uncertainties))
    }

    fn sample_tree_from_stats(
        self,
        expected_root: &ProcessIdentity,
        stats: &BTreeMap<ProcessId, ProcStat>,
        shared_uncertainties: &BTreeSet<SampleUncertainty>,
    ) -> Result<ProcessTreeSnapshot, ProcessReadError> {
        let fresh_root = self.read_identity(expected_root.pid())?;
        if &fresh_root != expected_root {
            return Err(ProcessReadError::IdentityMismatch {
                pid: expected_root.pid(),
            });
        }
        let mut uncertainties = shared_uncertainties.clone();

        let selected = descendant_ids(expected_root.pid(), stats);
        let mut samples = Vec::with_capacity(selected.len());
        for pid in selected {
            let Some(stat) = stats.get(&pid).copied() else {
                uncertainties.insert(SampleUncertainty::ProcessVanished(pid));
                continue;
            };
            match sample_process(stat) {
                Ok(sample) => samples.push(sample),
                Err(error) if pid == expected_root.pid() => return Err(error),
                Err(_) => {
                    uncertainties.insert(SampleUncertainty::ProcessVanished(pid));
                }
            }
        }
        ProcessTreeSnapshot::new(fresh_root, samples, uncertainties).map_err(ProcessReadError::Tree)
    }
}

/// Literal signals allowed after a verified pidfd is opened.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessSignal {
    /// Request graceful OS termination.
    Terminate,
    /// Force termination when policy explicitly permits it.
    Kill,
}

impl ProcessSignal {
    const fn rustix(self) -> Signal {
        match self {
            Self::Terminate => Signal::TERM,
            Self::Kill => Signal::KILL,
        }
    }
}

/// A pidfd handle that can signal only the process verified at open time.
pub trait VerifiedProcessHandle: std::fmt::Debug + Send + Sync {
    /// Identity verified immediately around pidfd creation.
    fn identity(&self) -> &ProcessIdentity;

    /// Send one literal signal through the verified pidfd.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessControlError`] when the kernel rejects the operation.
    fn signal(&self, signal: ProcessSignal) -> Result<(), ProcessControlError>;
}

/// Injectable pidfd opener used by the termination saga and deterministic fakes.
pub trait ProcessControl: std::fmt::Debug + Send + Sync {
    /// Verify the identity, open its pidfd, and verify again.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessControlError`] without opening a usable handle when any
    /// PID, start-time, executable, or kernel check fails.
    fn open_verified(
        &self,
        expected: &ProcessIdentity,
    ) -> Result<Box<dyn VerifiedProcessHandle>, ProcessControlError>;
}

/// Linux pidfd implementation with fresh identity checks.
#[derive(Clone, Copy, Debug, Default)]
pub struct LinuxProcessControl;

impl LinuxProcessControl {
    /// Construct Linux verified process control.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ProcessControl for LinuxProcessControl {
    fn open_verified(
        &self,
        expected: &ProcessIdentity,
    ) -> Result<Box<dyn VerifiedProcessHandle>, ProcessControlError> {
        let sampler = LinuxProcessSampler::new(1).map_err(ProcessControlError::Read)?;
        verify_identity(expected, &sampler.read_identity(expected.pid())?)?;
        let raw_pid = i32::try_from(expected.pid().value())
            .ok()
            .and_then(Pid::from_raw)
            .ok_or(ProcessControlError::InvalidPid {
                pid: expected.pid(),
            })?;
        let descriptor = pidfd_open(raw_pid, PidfdFlags::empty()).map_err(|source| {
            ProcessControlError::OpenPidFd {
                pid: expected.pid(),
                source,
            }
        })?;
        verify_identity(expected, &sampler.read_identity(expected.pid())?)?;
        Ok(Box::new(LinuxPidFd {
            descriptor,
            identity: expected.clone(),
        }))
    }
}

#[derive(Debug)]
struct LinuxPidFd {
    descriptor: OwnedFd,
    identity: ProcessIdentity,
}

impl VerifiedProcessHandle for LinuxPidFd {
    fn identity(&self) -> &ProcessIdentity {
        &self.identity
    }

    fn signal(&self, signal: ProcessSignal) -> Result<(), ProcessControlError> {
        pidfd_send_signal(&self.descriptor, signal.rustix())
            .map_err(ProcessControlError::SendSignal)
    }
}

/// Bounded procfs sampling failures with no command or native record content.
#[derive(Debug, Error)]
pub enum ProcessReadError {
    /// A zero process-table budget cannot make progress.
    #[error("Process sampling budget must be positive")]
    InvalidBudget,
    /// The process table cannot be enumerated.
    #[error("Failed to enumerate Linux process evidence")]
    Enumerate(#[source] io::Error),
    /// A required procfs record cannot be read.
    #[error("Failed to read Linux process evidence for PID {pid:?}")]
    Read {
        /// Process being sampled.
        pid: ProcessId,
        /// I/O diagnostic without record content.
        #[source]
        source: io::Error,
    },
    /// A process stat record is invalid.
    #[error("Failed to parse Linux process evidence for PID {pid:?}")]
    ParseStat {
        /// Process being sampled.
        pid: ProcessId,
        /// Typed parser diagnostic without record content.
        #[source]
        source: ProcStatParseError,
    },
    /// The executable path is not valid UTF-8.
    #[error("Linux process executable is not valid UTF-8 for PID {pid:?}")]
    NonUtf8Executable {
        /// Process being sampled.
        pid: ProcessId,
    },
    /// The executable path exceeds the domain evidence bound.
    #[error("Linux process executable exceeds its evidence bound for PID {pid:?}")]
    ExecutableBound {
        /// Process being sampled.
        pid: ProcessId,
        /// Length-only domain diagnostic.
        #[source]
        source: DomainInputError,
    },
    /// A fresh root identity does not match the selected process.
    #[error("Linux process identity changed for PID {pid:?}")]
    IdentityMismatch {
        /// Process whose identity changed.
        pid: ProcessId,
    },
    /// Caller-provided samples cannot form a coherent tree.
    #[error("Linux process tree is internally inconsistent")]
    Tree(#[source] ProcessTreeError),
}

/// Verified pidfd creation and literal-signal failures.
#[derive(Debug, Error)]
pub enum ProcessControlError {
    /// Fresh procfs evidence could not be read.
    #[error("Failed to verify process identity")]
    Read(#[from] ProcessReadError),
    /// The PID, start time, or executable changed.
    #[error("Process identity changed for PID {pid:?}")]
    IdentityMismatch {
        /// Process whose identity changed.
        pid: ProcessId,
    },
    /// Numeric PID cannot be represented by the kernel API.
    #[error("Process PID is outside the supported Linux range")]
    InvalidPid {
        /// Unsupported process identifier.
        pid: ProcessId,
    },
    /// Kernel rejected pidfd creation.
    #[error("Failed to open verified pidfd for PID {pid:?}")]
    OpenPidFd {
        /// Process selected for verified control.
        pid: ProcessId,
        /// Kernel diagnostic.
        #[source]
        source: Errno,
    },
    /// Kernel rejected a literal pidfd signal.
    #[error("Failed to send signal through verified pidfd")]
    SendSignal(#[source] Errno),
}

fn verify_identity(
    expected: &ProcessIdentity,
    actual: &ProcessIdentity,
) -> Result<(), ProcessControlError> {
    if expected != actual {
        return Err(ProcessControlError::IdentityMismatch {
            pid: expected.pid(),
        });
    }
    Ok(())
}

fn descendant_ids(root: ProcessId, stats: &BTreeMap<ProcessId, ProcStat>) -> Vec<ProcessId> {
    let mut selected = BTreeSet::from([root]);
    let mut queue = VecDeque::from([root]);
    while let Some(parent) = queue.pop_front() {
        for (pid, stat) in stats {
            if stat.parent_pid() == Some(parent) && selected.insert(*pid) {
                queue.push_back(*pid);
            }
        }
    }
    selected.into_iter().collect()
}

fn sample_process(stat: ProcStat) -> Result<ProcessSample, ProcessReadError> {
    let pid = stat.pid();
    let executable = read_executable(pid)?;
    let identity = ProcessIdentity::new(pid, stat.start_time_ticks(), executable);
    let io = read_bounded(path(pid, "io"), MAX_IO_BYTES)
        .ok()
        .and_then(|record| IoCounters::parse(&record).ok());
    let (command, truncated) = read_bounded_bytes(path(pid, "cmdline"), MAX_COMMAND_BYTES)
        .unwrap_or_else(|_| (Vec::new(), true));
    Ok(ProcessSample::new(
        identity,
        stat.parent_pid(),
        stat.state(),
        stat.cpu(),
        io,
        CommandFingerprint::from_redacted_cmdline(&command, truncated),
    ))
}

fn read_stat(pid: ProcessId) -> Result<ProcStat, ProcessReadError> {
    let record = read_bounded(path(pid, "stat"), MAX_STAT_BYTES)
        .map_err(|source| ProcessReadError::Read { pid, source })?;
    ProcStat::parse(&record).map_err(|source| ProcessReadError::ParseStat { pid, source })
}

fn read_executable(pid: ProcessId) -> Result<BoundedText<512>, ProcessReadError> {
    let executable = if let Ok(executable) = fs::read_link(path(pid, "exe")) {
        executable
            .to_str()
            .ok_or(ProcessReadError::NonUtf8Executable { pid })?
            .to_owned()
    } else {
        let comm = read_bounded(path(pid, "comm"), MAX_COMM_BYTES)
            .map_err(|source| ProcessReadError::Read { pid, source })?;
        format!("comm:{}", comm.trim_end())
    };
    BoundedText::new("process_executable", executable)
        .map_err(|source| ProcessReadError::ExecutableBound { pid, source })
}

fn path(pid: ProcessId, record: &str) -> PathBuf {
    PathBuf::from(PROC_ROOT)
        .join(pid.value().to_string())
        .join(record)
}

fn read_bounded(path: PathBuf, max_bytes: u64) -> Result<String, io::Error> {
    let (bytes, truncated) = read_bounded_bytes(path, max_bytes)?;
    if truncated {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "procfs record exceeds its byte budget",
        ));
    }
    String::from_utf8(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "procfs record is not valid UTF-8",
        )
    })
}

fn read_bounded_bytes(path: PathBuf, max_bytes: u64) -> Result<(Vec<u8>, bool), io::Error> {
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    let truncated = u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes;
    if truncated {
        bytes.truncate(usize::try_from(max_bytes).unwrap_or(usize::MAX));
    }
    Ok((bytes, truncated))
}
