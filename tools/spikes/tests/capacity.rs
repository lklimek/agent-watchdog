use agent_watchdog_phase0_probes::{CapacityConfig, CapacityProbeError, run_capacity_probe};

#[test]
fn target_population_converges_through_a_bounded_queue() {
    let config = CapacityConfig {
        main_sessions: 50,
        total_agents: 500,
        observations: 10_000,
        queue_capacity: 64,
    };

    let result = run_capacity_probe(config).expect("target population should converge");

    assert_eq!(result.processed_observations, config.observations);
    assert_eq!(result.converged_agents, config.total_agents);
    assert!(result.peak_queue_occupancy <= config.queue_capacity + 2);
}

#[test]
fn invalid_capacity_population_is_rejected() {
    let error = run_capacity_probe(CapacityConfig {
        main_sessions: 50,
        total_agents: 49,
        observations: 500,
        queue_capacity: 64,
    })
    .expect_err("children cannot make the total smaller than main sessions");

    assert!(matches!(error, CapacityProbeError::InvalidConfiguration));
}
