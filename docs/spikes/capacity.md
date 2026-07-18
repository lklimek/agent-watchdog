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

## Production container steady-state and burst gates

The current production image was exercised on 2026-07-18 with the complete
Axum/SQLite/discovery/process-monitor service, not the bare capacity probe:

- source implementation commit: `ec3258a` (with documentation checkpoint
  `96c4e89`);
- image: `agent-watchdog:perf`, image ID
  `sha256:079446c29393d8c1ae43ff0b7a10591ff7bafd6c0b0b8217d7d23344741a7dcf`;
- host: Linux 7.0.0-27-generic x86_64;
- isolated synthetic roots only; no user runtime state was mounted;
- read-only root filesystem, non-root UID/GID, all capabilities dropped,
  `no-new-privileges`, host PID namespace, and a 512-PID limit;
- 50 main sessions and 450 correlated children, populated through the
  authenticated production Claude hook endpoint.

The representative terminal-lifecycle burst sent 450 `SubagentStop` and 50
`SessionEnd` events with 32 concurrent HTTP workers. Each duration covers the
complete authenticated HTTP request and durable ingestion response:

| Burst measurement | Result |
|---|---:|
| Events accepted | 500 / 500 (HTTP 204) |
| Total wall time | 425.025 ms |
| Request latency p50 / p95 / p99 | 19.996 / 50.362 / 65.732 ms |
| Maximum request latency | 168.636 ms |
| Post-burst authenticated health | HTTP 200, ready, 25.519 ms |
| Post-burst 50-card/450-child dashboard API | HTTP 200, 22.698 ms |

An additional semantically identical 500-event pass used distinct fixture IDs
while continuously requesting the authenticated rendered `/ui` endpoint. All
500 events returned HTTP 204 with 45.732 ms p99 latency; all 21 concurrent UI
requests returned HTTP 200 with 49.480 ms p99/maximum latency. This directly
checks the rendered UI responsiveness portion of the gate rather than relying
only on its JSON read model.

After the burst left all 500 sessions terminal, `docker stats` sampled the
whole container for exactly 600 wall-clock seconds. The first reading was
discarded; 2,400 subsequent readings contributed to the average. CPU values
use Docker's two-decimal percentage precision, so `0.000%` below means every
reported sample was below the display threshold rather than claiming literally
zero instructions executed.

| Steady-state measurement | Result | Gate |
|---|---:|---:|
| Elapsed time | 600 s | 600 s |
| Samples | 2,400 | — |
| Average CPU | 0.000% of one core (reported) | < 10% |
| Maximum sampled CPU | 0.000% (reported) | — |
| Maximum sampled container memory | 30.050 MiB | < 256 MiB RSS |
| Final authenticated health | HTTP 200, ready, 27.951 ms | responsive |
| Final 50-card dashboard API | HTTP 200, 28.410 ms | responsive |

Five-minute runtime reconciliations ran during the window with no warnings or
errors. Container memory is a conservative cgroup-wide measurement that includes
the service RSS and container overhead. Exact commands, security settings, raw
figures, and interpretation are preserved in
`/data/artifacts/agent-watchdog/2026-07-18/qa/container-capacity.md`.

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

## Filesystem-storm safety gate

The Linux watcher test writes 100 files into a target backed by a one-slot
callback queue. The producer remains nonblocking, collapses the loss into one
`LocalQueueSaturated` reconciliation signal, and does not allocate an unbounded
event backlog. The server records watcher uncertainty as degraded health and
schedules bounded adapter reconciliation. A termination-monitor regression test
also proves that degraded watcher health prevents a stalled child from entering
the termination saga; restoring both watcher and adapter health is required
before the saga may start. Terminal and safety observations use the independent
durable coordinator path and remain intact.

The supported Compose stack was additionally exercised by creating 12,000 files
in a newly discovered QA worktree directory in 232 ms. Inotify coalescing kept
the production 4,096-slot callback boundary below saturation, watcher health
remained healthy, the service stayed ready, and an authenticated dashboard
request completed with HTTP 200 in 3.1 ms. Forced overflow remains deterministic
in the one-slot watcher test above; the Compose burst demonstrates that ordinary
high-rate topology activity remains responsive without requiring overflow.

## Architectural impact

Keep a bounded observation channel and a single transactional reducer owner.
Adapters may share a small fixed worker set; they must not create an unbounded
task or queue per discovered session. Expose queue saturation as health evidence
and reconcile affected sources after drops rather than silently allocating.
