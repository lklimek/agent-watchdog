use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;
use watchdog_domain::{ProcessId, ProcessIdentity};

/// Four cumulative CPU counters from Linux `/proc/<pid>/stat`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[expect(
    clippy::struct_field_names,
    reason = "the Linux ABI and evidence schema use the ticks unit explicitly"
)]
pub struct CpuCounters {
    user_ticks: u64,
    system_ticks: u64,
    children_user_ticks: u64,
    children_system_ticks: u64,
}

impl CpuCounters {
    /// Construct cumulative counters or a delta with the same four classes.
    #[must_use]
    pub const fn new(
        user_ticks: u64,
        system_ticks: u64,
        children_user_ticks: u64,
        children_system_ticks: u64,
    ) -> Self {
        Self {
            user_ticks,
            system_ticks,
            children_user_ticks,
            children_system_ticks,
        }
    }

    /// User-mode CPU ticks consumed by the process.
    #[must_use]
    pub const fn user_ticks(self) -> u64 {
        self.user_ticks
    }

    /// Kernel-mode CPU ticks consumed by the process.
    #[must_use]
    pub const fn system_ticks(self) -> u64 {
        self.system_ticks
    }

    /// User-mode ticks consumed by waited-for children.
    #[must_use]
    pub const fn children_user_ticks(self) -> u64 {
        self.children_user_ticks
    }

    /// Kernel-mode ticks consumed by waited-for children.
    #[must_use]
    pub const fn children_system_ticks(self) -> u64 {
        self.children_system_ticks
    }

    pub(crate) fn checked_delta(self, previous: Self) -> Option<Self> {
        Some(Self::new(
            self.user_ticks.checked_sub(previous.user_ticks)?,
            self.system_ticks.checked_sub(previous.system_ticks)?,
            self.children_user_ticks
                .checked_sub(previous.children_user_ticks)?,
            self.children_system_ticks
                .checked_sub(previous.children_system_ticks)?,
        ))
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.user_ticks.checked_add(other.user_ticks)?,
            self.system_ticks.checked_add(other.system_ticks)?,
            self.children_user_ticks
                .checked_add(other.children_user_ticks)?,
            self.children_system_ticks
                .checked_add(other.children_system_ticks)?,
        ))
    }

    fn any_grew(self) -> bool {
        self.user_ticks > 0
            || self.system_ticks > 0
            || self.children_user_ticks > 0
            || self.children_system_ticks > 0
    }

    fn all_four_grew(self) -> bool {
        self.user_ticks > 0
            && self.system_ticks > 0
            && self.children_user_ticks > 0
            && self.children_system_ticks > 0
    }
}

/// Cumulative storage I/O counters from Linux `/proc/<pid>/io`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IoCounters {
    read_bytes: u64,
    write_bytes: u64,
}

impl IoCounters {
    /// Construct cumulative storage I/O counters or their delta.
    #[must_use]
    pub const fn new(read_bytes: u64, write_bytes: u64) -> Self {
        Self {
            read_bytes,
            write_bytes,
        }
    }

    /// Bytes fetched from storage.
    #[must_use]
    pub const fn read_bytes(self) -> u64 {
        self.read_bytes
    }

    /// Bytes sent to storage.
    #[must_use]
    pub const fn write_bytes(self) -> u64 {
        self.write_bytes
    }

    fn checked_delta(self, previous: Self) -> Option<Self> {
        Some(Self::new(
            self.read_bytes.checked_sub(previous.read_bytes)?,
            self.write_bytes.checked_sub(previous.write_bytes)?,
        ))
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.read_bytes.checked_add(other.read_bytes)?,
            self.write_bytes.checked_add(other.write_bytes)?,
        ))
    }

    fn any_grew(self) -> bool {
        self.read_bytes > 0 || self.write_bytes > 0
    }
}

/// Kernel process state observed in `/proc/<pid>/stat`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessState {
    /// Running or runnable.
    Running,
    /// Interruptible sleep.
    Sleeping,
    /// Uninterruptible disk sleep.
    DiskSleep,
    /// Stopped by a signal or debugger.
    Stopped,
    /// Zombie awaiting collection.
    Zombie,
    /// Idle kernel thread.
    Idle,
    /// A kernel state not known to this build.
    Other(char),
}

impl ProcessState {
    pub(crate) const fn from_code(code: char) -> Self {
        match code {
            'R' => Self::Running,
            'S' => Self::Sleeping,
            'D' => Self::DiskSleep,
            'T' | 't' => Self::Stopped,
            'Z' => Self::Zombie,
            'I' => Self::Idle,
            other => Self::Other(other),
        }
    }
}

/// Secret-free shape of a process command line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandFingerprint {
    argument_count: u32,
    observed_bytes: u64,
    truncated: bool,
}

impl CommandFingerprint {
    /// Summarize an already bounded command line without retaining its bytes.
    #[must_use]
    pub fn from_redacted_cmdline(command: &[u8], truncated: bool) -> Self {
        let argument_count = command
            .split(|byte| *byte == 0)
            .filter(|argument| !argument.is_empty())
            .count();
        Self {
            argument_count: u32::try_from(argument_count).unwrap_or(u32::MAX),
            observed_bytes: u64::try_from(command.len()).unwrap_or(u64::MAX),
            truncated,
        }
    }

    /// Number of non-empty arguments observed.
    #[must_use]
    pub const fn argument_count(self) -> u32 {
        self.argument_count
    }

    /// Number of command-line bytes inspected before redaction.
    #[must_use]
    pub const fn observed_bytes(self) -> u64 {
        self.observed_bytes
    }

    /// Whether the input exceeded the reader's byte budget.
    #[must_use]
    pub const fn truncated(self) -> bool {
        self.truncated
    }
}

/// One comparable process sample within a correlated tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessSample {
    identity: ProcessIdentity,
    parent_pid: Option<ProcessId>,
    state: ProcessState,
    cpu: CpuCounters,
    io: Option<IoCounters>,
    command: CommandFingerprint,
}

impl ProcessSample {
    /// Construct a process sample from independently read procfs facts.
    #[must_use]
    pub const fn new(
        identity: ProcessIdentity,
        parent_pid: Option<ProcessId>,
        state: ProcessState,
        cpu: CpuCounters,
        io: Option<IoCounters>,
        command: CommandFingerprint,
    ) -> Self {
        Self {
            identity,
            parent_pid,
            state,
            cpu,
            io,
            command,
        }
    }

    /// Fresh PID, start time, and executable identity.
    #[must_use]
    pub const fn identity(&self) -> &ProcessIdentity {
        &self.identity
    }

    /// Parent PID when the kernel reports a positive value.
    #[must_use]
    pub const fn parent_pid(&self) -> Option<ProcessId> {
        self.parent_pid
    }

    /// Kernel process state.
    #[must_use]
    pub const fn state(&self) -> ProcessState {
        self.state
    }

    /// Cumulative CPU counters.
    #[must_use]
    pub const fn cpu(&self) -> CpuCounters {
        self.cpu
    }

    /// Cumulative storage I/O counters when readable.
    #[must_use]
    pub const fn io(&self) -> Option<IoCounters> {
        self.io
    }

    /// Secret-free command-line shape.
    #[must_use]
    pub const fn command(&self) -> CommandFingerprint {
        self.command
    }
}

/// A sampling condition that prevents treating missing evidence as inactivity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SampleUncertainty {
    /// A process vanished between enumeration and sampling.
    ProcessVanished(ProcessId),
    /// The PID's start time or executable identity changed.
    IdentityChanged(ProcessId),
    /// A cumulative CPU or I/O counter decreased.
    CounterReset(ProcessId),
    /// A cumulative tree delta overflowed.
    CounterOverflow(ProcessId),
    /// Process I/O data was unavailable.
    IoUnavailable(ProcessId),
    /// Procfs enumeration or parsing was incomplete.
    EnumerationIncomplete,
}

/// Snapshot construction failures that indicate an invalid caller tree.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProcessTreeError {
    /// The selected root process is absent from the supplied samples.
    #[error("Process tree does not contain its selected root")]
    MissingRoot,
    /// The selected root identity disagrees with its sample.
    #[error("Process tree root identity disagrees with its sample")]
    RootIdentityMismatch,
    /// A PID appears more than once in one snapshot.
    #[error("Process tree contains a duplicate PID")]
    DuplicatePid {
        /// Duplicated process identifier.
        pid: ProcessId,
    },
}

/// One bounded, correlated process-tree sample.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessTreeSnapshot {
    root: ProcessIdentity,
    processes: BTreeMap<ProcessId, ProcessSample>,
    uncertainties: BTreeSet<SampleUncertainty>,
}

impl ProcessTreeSnapshot {
    /// Construct a tree and validate its root and unique process identities.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessTreeError`] when the supplied samples do not form a
    /// coherent snapshot around the selected root.
    pub fn new(
        root: ProcessIdentity,
        samples: impl IntoIterator<Item = ProcessSample>,
        uncertainties: impl IntoIterator<Item = SampleUncertainty>,
    ) -> Result<Self, ProcessTreeError> {
        let mut processes = BTreeMap::new();
        for sample in samples {
            let pid = sample.identity().pid();
            if processes.insert(pid, sample).is_some() {
                return Err(ProcessTreeError::DuplicatePid { pid });
            }
        }
        let root_sample = processes
            .get(&root.pid())
            .ok_or(ProcessTreeError::MissingRoot)?;
        if root_sample.identity() != &root {
            return Err(ProcessTreeError::RootIdentityMismatch);
        }
        Ok(Self {
            root,
            processes,
            uncertainties: uncertainties.into_iter().collect(),
        })
    }

    /// Selected root identity.
    #[must_use]
    pub const fn root(&self) -> &ProcessIdentity {
        &self.root
    }

    /// Processes attributed to this tree.
    #[must_use]
    pub const fn processes(&self) -> &BTreeMap<ProcessId, ProcessSample> {
        &self.processes
    }

    /// Sampling uncertainties present before comparison.
    #[must_use]
    pub const fn uncertainties(&self) -> &BTreeSet<SampleUncertainty> {
        &self.uncertainties
    }

    /// Compare two snapshots without turning absence or resets into inactivity.
    #[must_use]
    pub fn activity_since(&self, previous: &Self) -> ProcessActivity {
        let mut cpu = CpuCounters::default();
        let mut io = IoCounters::default();
        let mut uncertainties = previous.uncertainties.clone();
        uncertainties.extend(self.uncertainties.iter().copied());
        let mut new_processes = 0_u32;

        for (pid, current) in &self.processes {
            let Some(before) = previous.processes.get(pid) else {
                new_processes = new_processes.saturating_add(1);
                continue;
            };
            if current.identity().start_time_ticks() != before.identity().start_time_ticks()
                || current.identity().executable() != before.identity().executable()
            {
                uncertainties.insert(SampleUncertainty::IdentityChanged(*pid));
                continue;
            }
            match current.cpu.checked_delta(before.cpu) {
                Some(delta) => match cpu.checked_add(delta) {
                    Some(total) => cpu = total,
                    None => {
                        uncertainties.insert(SampleUncertainty::CounterOverflow(*pid));
                    }
                },
                None => {
                    uncertainties.insert(SampleUncertainty::CounterReset(*pid));
                }
            }
            match (current.io, before.io) {
                (Some(current_io), Some(before_io)) => match current_io.checked_delta(before_io) {
                    Some(delta) => match io.checked_add(delta) {
                        Some(total) => io = total,
                        None => {
                            uncertainties.insert(SampleUncertainty::CounterOverflow(*pid));
                        }
                    },
                    None => {
                        uncertainties.insert(SampleUncertainty::CounterReset(*pid));
                    }
                },
                _ => {
                    uncertainties.insert(SampleUncertainty::IoUnavailable(*pid));
                }
            }
        }

        for pid in previous.processes.keys() {
            if !self.processes.contains_key(pid) {
                uncertainties.insert(SampleUncertainty::ProcessVanished(*pid));
            }
        }

        let strength = if cpu.all_four_grew() {
            ActivityStrength::StrongCpu
        } else if cpu.any_grew() || io.any_grew() || new_processes > 0 {
            ActivityStrength::Activity
        } else {
            ActivityStrength::Neutral
        };
        ProcessActivity {
            strength,
            cpu,
            io,
            new_processes,
            uncertainties,
        }
    }
}

/// Corroboration level derived from trustworthy positive deltas.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivityStrength {
    /// Comparable counters did not grow; this does not prove a stall.
    Neutral,
    /// CPU, I/O, or process-tree evidence progressed.
    Activity,
    /// All four CPU counter classes grew across the tree.
    StrongCpu,
}

/// Bounded delta and uncertainty evidence between process-tree samples.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessActivity {
    strength: ActivityStrength,
    cpu: CpuCounters,
    io: IoCounters,
    new_processes: u32,
    uncertainties: BTreeSet<SampleUncertainty>,
}

impl ProcessActivity {
    /// Derived progress strength; neutral is never negative evidence.
    #[must_use]
    pub const fn strength(&self) -> ActivityStrength {
        self.strength
    }

    /// Aggregate deltas for all four CPU counter classes.
    #[must_use]
    pub const fn cpu(&self) -> CpuCounters {
        self.cpu
    }

    /// Aggregate storage I/O deltas.
    #[must_use]
    pub const fn io(&self) -> IoCounters {
        self.io
    }

    /// Descendants newly observed since the previous sample.
    #[must_use]
    pub const fn new_processes(&self) -> u32 {
        self.new_processes
    }

    /// Conditions requiring reconciliation or termination suspension.
    #[must_use]
    pub const fn uncertainties(&self) -> &BTreeSet<SampleUncertainty> {
        &self.uncertainties
    }
}
