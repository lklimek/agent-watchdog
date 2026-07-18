# 50-main/500-agent capacity spike

Status: verified

Date: 2026-07-17

## Question

Can a Rust process move observations for 50 main sessions and 450 children
through one bounded ingestion queue and one reducer owner without per-session
tasks or unbounded memory growth?

## Environment and command

- Linux 7.0.0-27-generic x86_64
- Rust 1.96.0, optimized release build
- 50 main sessions, 500 total agents
- 250,000 round-robin observations
- bounded `std::sync::mpsc::sync_channel` capacity: 4,096

```text
cargo build --manifest-path tools/spikes/Cargo.toml --release --bin capacity_probe
/usr/bin/time -v /data/target/ff771cf653b30474/release/capacity_probe
```

The repository's required cached Cargo wrapper was used for the build. The
target path is checkout-specific and is shown only to make this run reproducible
in the recorded environment.

## Result

| Measurement | Result |
|---|---:|
| Agents converged | 500 / 500 |
| Observations reduced | 250,000 / 250,000 |
| Wall time | 15.581 ms |
| User/system CPU | 0.02 s / 0.00 s |
| Queue latency p50 / p95 / p99 | 182 / 191 / 195 microseconds |
| Peak in-flight observations | 4,098 |
| Configured queue slots | 4,096 |
| RSS before / after | 2,336 / 6,520 KiB |
| Maximum RSS (`time -v`) | 6,560 KiB |

The two observations beyond the channel capacity are bounded handoff slots: one
can be held by the consumer after receive and one by the blocked producer. They
are not additional queue storage.

## Initial budgets

These are gates for later production load tests, not claims that the bare spike
models adapter parsing, SQLite, HTTP, or MCP costs:

- ingestion channel capacity: 4,096 observations;
- total observations in flight: at most 4,098 for the single-producer test
  shape, with every production producer using bounded admission;
- synthetic 250,000-observation convergence: under 2 seconds wall time and
  under 2 CPU-seconds on the reference class of host;
- synthetic p99 queue-to-reducer latency: under 10 ms;
- production 50/500 maximum RSS: 256 MiB;
- production no-change steady-state CPU: below 10% of one core averaged over 10
  minutes, excluding explicitly configured reconciliation scans;
- production representative burst convergence: p99 under 250 ms with health and
  UI endpoints remaining responsive.

The production gates deliberately leave headroom for the database, adapters,
and HTTP stack while remaining tight enough to catch rescans, per-agent task
explosions, and unbounded queues. Rebaseline only with a recorded rationale.

## Production-service population gate

The ignored `watchdog-server` load test exercises the production SQLite store,
session coordinator, hierarchy, restart lane reconstruction, and dashboard read
model with 50 mains and 450 children. It is explicit rather than part of every
commit's default test run:

```text
/home/ubuntu/.codex/claudius/scripts/cargo-cached.sh \
  test -p watchdog-server --test load -- --ignored --nocapture
```

On the reference Linux host, repeated debug-profile runs ingested and durably
converged all 500 sessions in 379–416 ms, built the 50-card dashboard (including
all child counts) in 32–35 ms, and reported a test-process high-water RSS of
13.2–17.0 MiB.
It then reconstructed all 500 reducer lanes from the same database and produced
an identical dashboard. The executable gate permits 30 seconds for ingestion,
2 seconds for a dashboard snapshot, and 256 MiB high-water RSS to tolerate CI
variance while still catching catastrophic regressions. These figures cover
the in-process production path, not container overhead, live adapter parsing,
the 10-minute steady-state CPU gate, or burst p99; those remain separate release
measurements.

The same explicit test target also performs a restart soak. It closes and
reopens the production SQLite-backed service ten times at the full 50/500
population, writes a durable restart boundary, reconciles every session with a
fresh native observation, and verifies WAL/foreign-key health plus the complete
dashboard hierarchy after every cycle. The population and restart tests
completed together in 5.67 seconds on the reference host. This gate exercises
restart idempotency and durable lane reconstruction; interruption at every
termination-saga stage remains a separate safety test.

## Slow-consumer gate

The dashboard suite fills the bounded SSE broadcast channel and verifies that a
lagging client receives `resync_required` while ingestion and later snapshots
continue. An explicit notification test connects the production notifier to a
webhook that accepts a request but never responds. While the five-second
production timeout is active, a separate agent progress observation must commit
within one second. The timed-out webhook is then audited and acknowledged once;
restarting the dispatcher does not send it again.

```text
/home/ubuntu/.codex/claudius/scripts/cargo-cached.sh \
  test -p watchdog-server --test notifications \
  hanging_webhook_times_out_once_without_blocking_agent_ingestion \
  -- --ignored
```

The reference run completed in 5.04 seconds, matching the intentional timeout
without delaying agent ingestion.

## Architectural impact

Keep a bounded observation channel and a single transactional reducer owner.
Adapters may share a small fixed worker set; they must not create an unbounded
task or queue per discovered session. Expose queue saturation as health evidence
and reconcile affected sources after drops rather than silently allocating.
