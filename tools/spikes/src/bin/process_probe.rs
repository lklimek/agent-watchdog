use std::{env, num::ParseIntError, path::PathBuf};

use agent_watchdog_phase0_probes::{ExpectedProcessIdentity, ProcessControlError, VerifiedPidFd};
use thiserror::Error;

#[derive(Debug, Error)]
enum ProbeError {
    #[error("Usage: process_probe PID START_TIME EXECUTABLE")]
    MissingArgument,
    #[error("Invalid numeric argument")]
    InvalidNumber(#[from] ParseIntError),
    #[error(transparent)]
    Process(#[from] ProcessControlError),
}

fn main() -> Result<(), ProbeError> {
    let mut arguments = env::args_os().skip(1);
    let pid = arguments
        .next()
        .ok_or(ProbeError::MissingArgument)?
        .into_string()
        .map_err(|_| ProbeError::MissingArgument)?
        .parse()?;
    let start_time_ticks = arguments
        .next()
        .ok_or(ProbeError::MissingArgument)?
        .into_string()
        .map_err(|_| ProbeError::MissingArgument)?
        .parse()?;
    let executable = PathBuf::from(arguments.next().ok_or(ProbeError::MissingArgument)?);
    if arguments.next().is_some() {
        return Err(ProbeError::MissingArgument);
    }

    let identity = ExpectedProcessIdentity {
        pid,
        start_time_ticks,
        executable,
    };
    VerifiedPidFd::open(&identity)?.terminate()?;
    println!("Verified and sent SIGTERM to PID {pid}");
    Ok(())
}
