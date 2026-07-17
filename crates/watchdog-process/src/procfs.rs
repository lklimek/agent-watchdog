use std::num::ParseIntError;

use thiserror::Error;
use watchdog_domain::ProcessId;

use crate::{CpuCounters, IoCounters, ProcessState};

const STATE_INDEX: usize = 0;
const PARENT_PID_INDEX: usize = 1;
const USER_TICKS_INDEX: usize = 11;
const SYSTEM_TICKS_INDEX: usize = 12;
const CHILDREN_USER_TICKS_INDEX: usize = 13;
const CHILDREN_SYSTEM_TICKS_INDEX: usize = 14;
const START_TIME_INDEX: usize = 19;

/// Parsed process fields required for identity and activity evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcStat {
    pid: ProcessId,
    parent_pid: Option<ProcessId>,
    state: ProcessState,
    start_time_ticks: u64,
    cpu: CpuCounters,
}

impl ProcStat {
    /// Parse a Linux `/proc/<pid>/stat` record without trusting command text.
    ///
    /// # Errors
    ///
    /// Returns [`ProcStatParseError`] for missing or invalid required fields.
    pub fn parse(record: &str) -> Result<Self, ProcStatParseError> {
        let command_end = record
            .rfind(')')
            .ok_or(ProcStatParseError::MissingCommandBoundary)?;
        let pid_end = record
            .find(' ')
            .ok_or(ProcStatParseError::MissingField { field: "pid" })?;
        let pid = parse_pid(&record[..pid_end], "pid")?;
        let fields: Vec<&str> = record[command_end + 1..].split_whitespace().collect();
        let state = required(&fields, STATE_INDEX, "state")?
            .chars()
            .next()
            .ok_or(ProcStatParseError::MissingField { field: "state" })?;
        let parent_raw = parse_u32(
            required(&fields, PARENT_PID_INDEX, "parent_pid")?,
            "parent_pid",
        )?;
        let parent_pid = ProcessId::new(parent_raw).ok();
        Ok(Self {
            pid,
            parent_pid,
            state: ProcessState::from_code(state),
            start_time_ticks: parse_u64(
                required(&fields, START_TIME_INDEX, "start_time_ticks")?,
                "start_time_ticks",
            )?,
            cpu: CpuCounters::new(
                parse_u64(
                    required(&fields, USER_TICKS_INDEX, "user_ticks")?,
                    "user_ticks",
                )?,
                parse_u64(
                    required(&fields, SYSTEM_TICKS_INDEX, "system_ticks")?,
                    "system_ticks",
                )?,
                parse_u64(
                    required(&fields, CHILDREN_USER_TICKS_INDEX, "children_user_ticks")?,
                    "children_user_ticks",
                )?,
                parse_u64(
                    required(
                        &fields,
                        CHILDREN_SYSTEM_TICKS_INDEX,
                        "children_system_ticks",
                    )?,
                    "children_system_ticks",
                )?,
            ),
        })
    }

    /// Process identifier encoded in the record.
    #[must_use]
    pub const fn pid(self) -> ProcessId {
        self.pid
    }

    /// Parent identifier, excluding kernel PID zero.
    #[must_use]
    pub const fn parent_pid(self) -> Option<ProcessId> {
        self.parent_pid
    }

    /// Kernel state at sample time.
    #[must_use]
    pub const fn state(self) -> ProcessState {
        self.state
    }

    /// Kernel start time used to detect PID reuse.
    #[must_use]
    pub const fn start_time_ticks(self) -> u64 {
        self.start_time_ticks
    }

    /// Four cumulative CPU counter classes.
    #[must_use]
    pub const fn cpu(self) -> CpuCounters {
        self.cpu
    }
}

impl IoCounters {
    /// Parse storage-byte counters from Linux `/proc/<pid>/io`.
    ///
    /// # Errors
    ///
    /// Returns [`IoParseError`] when either required counter is absent or invalid.
    pub fn parse(record: &str) -> Result<Self, IoParseError> {
        let mut read_bytes = None;
        let mut write_bytes = None;
        for line in record.lines() {
            let Some((key, raw_value)) = line.split_once(':') else {
                continue;
            };
            match key {
                "read_bytes" => read_bytes = Some(parse_io(raw_value, "read_bytes")?),
                "write_bytes" => write_bytes = Some(parse_io(raw_value, "write_bytes")?),
                _ => {}
            }
        }
        Ok(Self::new(
            read_bytes.ok_or(IoParseError::MissingField {
                field: "read_bytes",
            })?,
            write_bytes.ok_or(IoParseError::MissingField {
                field: "write_bytes",
            })?,
        ))
    }
}

/// Failures parsing bounded Linux process-stat records.
#[derive(Debug, Error)]
pub enum ProcStatParseError {
    /// The command field has no closing delimiter.
    #[error("Process stat record has no command boundary")]
    MissingCommandBoundary,
    /// A required field is absent.
    #[error("Process stat record is missing field {field}")]
    MissingField {
        /// Stable field name safe for logs.
        field: &'static str,
    },
    /// A required numeric field is malformed.
    #[error("Process stat field {field} is invalid")]
    InvalidField {
        /// Stable field name safe for logs.
        field: &'static str,
        /// Parser diagnostic without record content.
        #[source]
        source: ParseIntError,
    },
    /// PID zero cannot identify a process.
    #[error("Process stat PID must be positive")]
    ZeroPid,
}

/// Failures parsing bounded Linux process-I/O records.
#[derive(Debug, Error)]
pub enum IoParseError {
    /// A required counter is absent.
    #[error("Process I/O record is missing field {field}")]
    MissingField {
        /// Stable field name safe for logs.
        field: &'static str,
    },
    /// A required counter is malformed.
    #[error("Process I/O field {field} is invalid")]
    InvalidField {
        /// Stable field name safe for logs.
        field: &'static str,
        /// Parser diagnostic without record content.
        #[source]
        source: ParseIntError,
    },
}

fn required<'a>(
    fields: &'a [&str],
    index: usize,
    field: &'static str,
) -> Result<&'a str, ProcStatParseError> {
    fields
        .get(index)
        .copied()
        .ok_or(ProcStatParseError::MissingField { field })
}

fn parse_pid(value: &str, field: &'static str) -> Result<ProcessId, ProcStatParseError> {
    ProcessId::new(parse_u32(value, field)?).map_err(|_| ProcStatParseError::ZeroPid)
}

fn parse_u32(value: &str, field: &'static str) -> Result<u32, ProcStatParseError> {
    value
        .parse()
        .map_err(|source| ProcStatParseError::InvalidField { field, source })
}

fn parse_u64(value: &str, field: &'static str) -> Result<u64, ProcStatParseError> {
    value
        .parse()
        .map_err(|source| ProcStatParseError::InvalidField { field, source })
}

fn parse_io(value: &str, field: &'static str) -> Result<u64, IoParseError> {
    value
        .trim()
        .parse()
        .map_err(|source| IoParseError::InvalidField { field, source })
}
