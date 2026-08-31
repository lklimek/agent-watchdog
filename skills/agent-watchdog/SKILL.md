---
name: agent-watchdog
description: Monitor coordinated Claude Code, Codex CLI, and Codex Companion sessions with Agent Watchdog. Use when coordinating delegated agents, tracking their progress and deadlines, processing lifecycle events, diagnosing monitoring health, or recovering Watchdog state after an MCP transport reconnect.
---

# Agent Watchdog coordinator workflow

Use Agent Watchdog as a durable monitoring aid. Keep the runtime, process, git,
and task ledger as independent sources of truth.

## Invariants

The server states the first three in its own MCP `instructions`, delivered on
connect; they are repeated here only as the entry points this skill builds on.
Where the two ever differ, the server's instructions are current.

1. Call `register_session` for the coordinating main session before every other
   Watchdog call on a new MCP transport.
2. Generate a fresh, globally unique `event_key` for every logical mutating
   call. Reuse a key only to retry that exact mutation with byte-for-byte
   equivalent meaning. Never reuse it for a later progress update or changed
   payload.
3. Process an entire `list_events` page before acknowledging its `next_cursor`.
4. Use only IDs returned or verified by the relevant runtime and Watchdog. Do
   not infer, truncate, or fabricate IDs.
5. Corroborate lifecycle conclusions against direct evidence before acting on
   them.

## Register the tree

### Bind the coordinator first

On the first Watchdog interaction:

1. Determine the coordinator's actual runtime and runtime-native session ID.
2. Call `register_session` with `kind=main`, that `runtime`, the exact
   `native_id`, and a fresh `event_key`.
3. Retain the returned Watchdog session UUID as the main `session_id`.

Supported runtime values are `claude_code`, `codex_cli`, and
`codex_companion`. Registration binds the opaque MCP transport identity to one
immutable main-session tree. A transport cannot switch trees.

### Register every child and delegation

For each real delegation:

1. Obtain the child's runtime-native ID from the runtime or dispatch result.
2. Call `register_session` with `kind=child`, the child's runtime and
   `native_id`, the main or child parent's Watchdog `session_id`, and a fresh
   `event_key`.
3. Retain the returned child Watchdog UUID.
4. Call `register_delegation` with the parent and child Watchdog UUIDs, a fresh
   `event_key`, and an optional absolute `deadline_ms`.
5. If an exact worktree or owned directory is known, call
   `register_watch_path` with the child Watchdog UUID, the existing path, and
   another fresh `event_key`.

Register nested children against their actual in-tree parent. A child that
runs as its own process with its own MCP connection can register itself the
same way: `kind=child` plus the parent `session_id` it was handed binds that
connection to the parent's tree, so it needs no main registration of its own.
Registering a child does not replace `register_delegation`: the latter records
the exact relationship and expected check-in.

## Report lifecycle changes

- Call `report_progress` with the Watchdog `session_id`, a fresh `event_key`, a
  bounded useful summary, and an optional operation label whenever material
  progress occurs.
- Call `report_waiting` with `waiting_for=agent`, `tool`, `user`, or
  `intentional`. Use `intentional` only for a deliberate pause; it also pauses
  timer accounting.
- Report completion with `complete_session` and an outcome of `completed`,
  `failed`, or `cancelled`. Do this once direct evidence supports the terminal
  outcome.

Every later report is a new mutation and therefore needs a new `event_key`.
Only an uncertain transport retry of the identical report reuses the original
key.

## Manage deadlines

Use `update_deadline` with a fresh `event_key` for every change:

- `set` requires `deadline_ms` as an absolute Unix epoch time in milliseconds.
- `pause` suspends timer accounting.
- `resume` restarts timer accounting.
- `clear` removes the explicit deadline.

Set a realistic check-in deadline at delegation time, extend or shorten it
when the work changes, pause it during a deliberate wait, and resume or clear
it promptly. A waiting state is not automatically terminal.

## Process the durable inbox safely

Treat `list_events` as a delivery-and-acknowledgement loop:

1. On the first read, omit `after` to resume from the stored durable cursor.
2. Read the ordered events and save the returned `next_cursor`.
3. Process every event: inspect its current session, diagnostics, provenance,
   relation evidence, and suggested checks; then corroborate important
   conclusions.
4. Only after processing succeeds, make the next `list_events` call with the
   previous page's `next_cursor` as `after`. That call acknowledges the page
   before reading later events.
5. If processing fails or the coordinator may have crashed, do not advance
   `after`; replay is safer than losing an alert.

Use bounded polling or long waits that still permit coordinator progress and
deadline maintenance. An empty page is normal.

## Interpret identity, provenance, and health

- A runtime-native ID belongs to `claude_code`, `codex_cli`, or
  `codex_companion`. A Watchdog `session_id` is the runtime-neutral UUID
  returned by registration. Use Watchdog UUIDs for all later tree operations.
- `get_session`, `list_sessions`, and `get_session_tree` return normalized
  state plus native identity. The tree also retains current and superseded
  relationship evidence.
- Session `provenance` identifies the observation source that last advanced
  the reduced snapshot. Inspect adapter identity, evidence fingerprint, trust,
  confidence when present, timestamps, conflicts, and process evidence instead
  of treating the normalized state as an unexplained fact.
- `get_watchdog_health` reports database WAL/foreign-key/schema status and
  persisted adapter health. Read warnings and freshness per runtime. Adapter
  health describes monitoring coverage; it is not proof that a particular
  child is alive, finished, or failed.

## Recover after a transport reconnect

After an MCP reconnect, plugin reload, lost-binding error, or
`MCP transport is not bound to a main session` response:

1. Re-run `register_session` for the same main runtime and runtime-native ID
   before any other Watchdog call. Re-registration is idempotent and safe and
   should return the same stable Watchdog session.
2. Use a fresh `event_key` for this new rebind operation. Reuse the prior key
   only when retrying the exact request whose result is unknown.
3. Re-read `get_session_tree` and `get_watchdog_health`.
4. Reconcile children, relations, deadlines, and the durable event cursor
   against dispatch records and direct evidence before continuing.

If child registration fails after reconnect, first confirm the main binding
and the child's runtime/native identity. Do not churn IDs or blindly alternate
keys around an identity conflict.

### Why a binding disappears

A transport binding ends in one of three ways, all recoverable by the same
re-registration above:

- **Idle expiry.** A transport with no traffic for the configured idle window
  (`[mcp] idle_ttl_seconds`, 48 hours by default) is closed and its scope
  released.
- **Capacity eviction.** The server admits a bounded number of concurrent
  transports (`[mcp] max_sessions`, 64 by default). A new connection arriving
  at that cap evicts the single longest-idle transport rather than being
  refused, so reconnect churn never locks anyone out. The `mcp_sessions` field
  of `get_watchdog_health` reports current occupancy, the cap, and how many
  transports have been evicted.
- **Explicit close** when a client disconnects cleanly.

Only durable events are affected by none of these: the inbox cursor is stored
server-side, so a lost transport never discards unread events.

An HTTP `408` is **not** one of these. It means the request body never reached
the server in full within `[mcp] request_body_timeout_seconds` (30 by default),
so the call never ran. The binding survives: retry the same call, reusing its
`event_key` because the mutation did not happen. Do not re-register the tree.

Registering a main session may resolve a runtime-native ID through the
discovery **alias** table onto an already-discovered canonical session. Always
use the `session_id` from the registration response as the parent for later
child registrations; never re-derive it from the native ID you supplied.

## Corroborate, don't trust alone

Independently verify consequential lifecycle state while the known reliability
gaps below remain open:

- For code work, check the expected worktree, `git status`, relevant diff,
  branch/commit ancestry, and the claimed commit.
- For a running job, check the runtime's task record, process existence and
  executable identity, bounded CPU/I/O evidence, and fresh output.
- For completion or failure, check the coordinator's task ledger or dispatch
  record and the artifact/result itself.
- For alerts, compare Watchdog events, session provenance, signal timestamps,
  source conflicts, and adapter health with those direct sources.

Event and session diagnostics carry `outcome_uncertain`. It is `true` when the
runtime vanished, when the state is unknown, or when sources still disagree —
that is, whenever Watchdog holds no evidence that establishes what actually
happened. It is not a synonym for "not finished": an ordinary running or
waiting child reports `false`. When it is `true`, run the checks listed in the
accompanying `suggested_checks` entry before retrying, replacing, or
discarding that job's work.

This guidance is required by the following Agent Watchdog Prior Knowledge:

- Decision, 2026-07-17: the rmcp Streamable HTTP service injects an opaque
  transport identity, and each transport binds exactly once to a main-session
  tree.
- Open reliability observations, current through 2026-07-24: main-session
  transport binding has dropped repeatedly mid-session and required
  re-registration; `get_watchdog_health` has reported all adapters as
  `degraded` immediately after an MCP reconnect; and
  `register_session(kind=child, runtime=codex_companion)` has failed first as
  transport-unbound and then as an identity conflict on retry.

Therefore, use Watchdog to focus investigation and preserve durable events,
but never use its lifecycle state alone to terminate work, discard a result,
or declare success.
