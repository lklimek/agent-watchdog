//! Agent Watchdog Linux server binary.

#[tokio::main]
async fn main() {
    if let Some(argument) = std::env::args_os().nth(1) {
        if argument == std::ffi::OsStr::new("healthcheck") {
            if watchdog_server::healthcheck_from_environment().is_ok() {
                return;
            }
            std::process::exit(1);
        }
        std::process::exit(64);
    }
    if let Err(error) = watchdog_server::init_tracing() {
        eprintln!("{error}");
        std::process::exit(1);
    }
    if let Err(error) = watchdog_server::run_from_environment().await {
        tracing::error!(
            event = "server.failed",
            error_code = error.code(),
            "Agent Watchdog stopped"
        );
        std::process::exit(1);
    }
}
