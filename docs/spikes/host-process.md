# Host PID/UID and signalling spike

Status: verified

Date: 2026-07-17

## Question

Can an unprivileged, same-UID container in the host PID namespace read the four
Linux CPU counters and signal only a freshly verified helper through pidfd?

## Method

Rust tests in `tools/spikes/tests/process.rs` cover `/proc/<pid>/stat` parsing,
counter comparison, start-time/executable mismatch, pidfd opening, and SIGTERM.
The Rust `cpu_counter_helper` and `process_probe` binaries were then mounted
read-only into an Ubuntu 26.04 container with:

```text
--read-only --pid=host --user 1000:1001 --cap-drop ALL
--security-opt no-new-privileges
```

The helper is purpose-built and isolated. No agent or unrelated host process is
ever selected or signalled.

## Result

- The container observed 493 processes through the host PID namespace.
- A deliberately wrong start time returned an identity error and left the
  helper running.
- The baseline counters were `0,0,0,0`; a later snapshot was `13,3,14,2`, so
  `utime`, `stime`, `cutime`, and `cstime` all grew.
- The matching pidfd sent SIGTERM to the exact helper and the shell observed
  signal exit status 143.
- The container remained non-root, had every capability dropped, used a
  read-only root, and did not mount the Docker socket.

The targeted Rust test ledger record is
`be06c6ad18fd013e415be9f0b2b8dd48`; its log names all eight process tests.

## Decision

The host PID/same numeric UID model is viable on this host without privileged
mode or added capabilities. Production signalling keeps three distinct layers:

1. pure child-only termination policy;
2. fresh PID start-time, executable, and runtime verification;
3. pidfd open followed by a literal signal API.

If `/proc` visibility, pidfd, identity, or critical health is unavailable, the
service reports uncertainty and suspends destructive automation. PID alone is
never an identity.
