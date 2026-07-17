//! Supported and compile-only platform boundary contracts.

use watchdog_process::PlatformSupport;

#[test]
fn platform_support_reports_linux_only_operation() {
    let support = PlatformSupport::current();

    #[cfg(target_os = "linux")]
    assert_eq!(support, PlatformSupport::Linux);

    #[cfg(target_os = "macos")]
    assert_eq!(support, PlatformSupport::MacOsBuildOnly);
}
