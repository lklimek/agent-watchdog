//! Platform-gated process evidence and verified process control.

#[cfg(target_os = "linux")]
mod linux;
mod model;
mod procfs;

#[cfg(target_os = "linux")]
pub use linux::{
    LinuxProcessControl, LinuxProcessSampler, ProcessControl, ProcessControlError,
    ProcessReadError, ProcessSignal, VerifiedProcessHandle,
};
pub use model::{
    ActivityStrength, CommandFingerprint, CpuCounters, IoCounters, ProcessActivity, ProcessSample,
    ProcessState, ProcessTreeError, ProcessTreeSnapshot, SampleUncertainty,
};
pub use procfs::{IoParseError, ProcStat, ProcStatParseError};

/// Compile-time process-operation support for the current target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlatformSupport {
    /// Linux host operation is supported and tested.
    Linux,
    /// macOS is build-only and process operation is unsupported.
    MacOsBuildOnly,
    /// Other targets compile only where dependencies permit.
    Unsupported,
}

impl PlatformSupport {
    /// Return support for the current compilation target.
    #[must_use]
    pub const fn current() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::Linux
        }
        #[cfg(target_os = "macos")]
        {
            Self::MacOsBuildOnly
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Self::Unsupported
        }
    }

    /// Return whether host process sampling and signalling may be configured.
    #[must_use]
    pub const fn operation_available(self) -> bool {
        matches!(self, Self::Linux)
    }
}
