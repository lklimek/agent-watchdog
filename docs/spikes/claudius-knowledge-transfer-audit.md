# Claudius watchdog knowledge-transfer audit

Status: verified; one applicable mismatch fixed

Date: 2026-07-18

## Scope

This final-QA audit compares the Rust implementation with
`/data/artifacts/claudius/2026-07-17/watchdog-knowledge-transfer.html`, interpreted
as lessons from the `github.com/lklimek/claudius` plugin rather than as a design
to port. The transfer document was intentionally not read until final QA, per
the product owner's instruction.

## Results

| Transfer pitfall or recommendation | Agent Watchdog result | Evidence/decision |
|---|---|---|
| One coordinator session ID silently gates out jobs launched by distinct wrapper sessions. | Not present. | Companion discovery iterates every bounded summary job independently and creates the exact persisted wrapper session as that job's parent. `companion_discovery_keeps_jobs_from_distinct_wrapper_sessions` covers two simultaneous, distinct wrapper sessions. Explicit MCP delegation can attach a known orchestration relation without weakening automatic discovery. |
| `workspaceRoot` is the dispatch cwd, not necessarily the child's worktree. | Applicable mismatch fixed. | Automatic Companion discovery no longer persists `workspaceRoot` as child `startup_directory`, so it cannot grant false filesystem ownership. Exact paths come from trustworthy runtime evidence or MCP registration. The existing discovery acceptance test now asserts this fail-safe behavior. |
| Wrapper `idle_notification` is early, absent, or otherwise unreliable. | Not used. | No adapter, reducer, timer, or notification path consumes this field. Native state, process evidence, content-free log growth, official hooks, and MCP reports drive state instead. |
| Selecting the newest file in a shared `jobs/` directory can bind the wrong job. | Not present. | Every summary entry is keyed by validated native job ID; the optional detail and log paths are constructed as `jobs/<safe-id>.json` and `jobs/<safe-id>.log`. No newest-file selection exists. |
| Real task-worker PIDs should be sampled rather than assumed absent. | Implemented. | A positive native PID is freshly resolved through Linux process identity, including start-time ticks, before it becomes corroborating evidence. Later CPU sampling and termination revalidate identity to resist PID reuse. |
| Job logs provide useful activity cadence. | Implemented with a privacy-preserving boundary. | The watcher records append cadence through device/inode/offset changes and emits a content-free progress observation. It intentionally does not parse command text or timestamps: inotify gives current observation time, reconciliation handles missed events, and avoiding log contents preserves the no-transcript/no-prompt data boundary. |
| Dispatches should register exact job/worktree ownership at spawn time. | Implemented as optional enrichment. | MCP exposes durable `register_session`, `register_delegation`, and tree-scoped `register_watch_path`. Automatic monitoring remains functional without registration, while an agent with exact knowledge can eliminate ambiguity. |
| Tmux pane titles/socket state are spoofable by a same-UID process. | Avoided. | The Rust service does not consume tmux panes, titles, sockets, or wrapper notifications. Process identity comes from `/proc`; filesystem activity remains corroborating and ambiguity-safe. |
| Persistent failures must not emit unbounded unsanitized diagnostics. | Implemented. | Native inputs are bounded/typed, errors exposed by parsers and reconciliation are content-free, structured `tracing` uses stable event names, durable health records are bounded, and retained diagnostics omit transcript/job-log bodies. |
| Healthy monitoring should be edge-triggered and quiet. | Implemented. | Reducer events and outbox messages are transition-driven and durable; unchanged observations do not repeatedly notify. Low-frequency reconciliation remains an internal health/discovery action rather than a user alert stream. |
| A participant must not block on a notification delivered only to its coordinator. | Addressed by interface shape. | Parent events are a durable pull/acknowledgement inbox, and human channels are separate. Agent push remains a future enhancement because current runtimes do not offer a reliable generic push channel; callers can poll with bounded waits and extend deadlines over MCP. |

## Fix made during this audit

Before this review, `reconcile_companion_job` capability-validated native
`workspaceRoot` and saved it as the child's startup directory. That was safe
against path escape but semantically unsafe: a valid coordinator cwd could be
mistaken for the job's owned worktree and refresh the child on unrelated writes.
The adapter now records no automatic Companion child directory. This sacrifices
an untrustworthy display hint and preserves correct stall evidence. Explicit
MCP registration restores the real path when the dispatching agent knows it.

## Accepted design differences

- Claudius task-gates its alert definition. Agent Watchdog instead follows the
  product decision to prefer early investigation and tolerate false alarms,
  while long grace periods, waiting states, CPU/process evidence, and main-session
  termination exclusion prevent dangerous action.
- The transfer document recommends parsing structured log timestamps. V1 keeps
  log contents entirely opaque. Inotify and exact offset growth provide timely
  cadence without expanding the sensitive-data boundary; delayed reconciliation
  is visible as lower-confidence evidence rather than backdating from log text.
- Automatic Companion hierarchy follows the exact wrapper session persisted by
  Companion. It does not guess the human coordinator from cwd/session heuristics.
  Agents can supply the true orchestration relationship through MCP.
