use std::{error::Error, time::Duration};

use agent_watchdog_phase0_probes::{CapacityConfig, run_capacity_probe};

fn micros(duration: Duration) -> u128 {
    duration.as_micros()
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = CapacityConfig {
        main_sessions: 50,
        total_agents: 500,
        observations: 250_000,
        queue_capacity: 4_096,
    };
    let result = run_capacity_probe(config)?;
    let cpu = result.cpu.unwrap_or_default();

    println!("main_sessions={}", config.main_sessions);
    println!("total_agents={}", config.total_agents);
    println!("observations={}", result.processed_observations);
    println!("converged_agents={}", result.converged_agents);
    println!("queue_capacity={}", config.queue_capacity);
    println!("peak_queue_occupancy={}", result.peak_queue_occupancy);
    println!("elapsed_us={}", micros(result.elapsed));
    println!("latency_p50_us={}", micros(result.latency_p50));
    println!("latency_p95_us={}", micros(result.latency_p95));
    println!("latency_p99_us={}", micros(result.latency_p99));
    println!("rss_before_kib={}", result.rss_before_kib);
    println!("rss_after_kib={}", result.rss_after_kib);
    println!("cpu_user_ticks={}", cpu.user_ticks);
    println!("cpu_system_ticks={}", cpu.system_ticks);
    println!("cpu_children_user_ticks={}", cpu.children_user_ticks);
    println!("cpu_children_system_ticks={}", cpu.children_system_ticks);

    Ok(())
}
