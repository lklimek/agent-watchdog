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

## Architectural impact

Keep a bounded observation channel and a single transactional reducer owner.
Adapters may share a small fixed worker set; they must not create an unbounded
task or queue per discovered session. Expose queue saturation as health evidence
and reconcile affected sources after drops rather than silently allocating.
