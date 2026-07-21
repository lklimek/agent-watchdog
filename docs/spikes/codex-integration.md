# Codex official-integration scope

Status: resolved and implemented

Date: 2026-07-18

## Question

Can Agent Watchdog safely use current Codex app-server or hooks for automatic
production monitoring, and can the supported narrow Compose mount treat the
native SQLite database as live state?

## Environment and commands

- Codex CLI 0.144.5 on Linux x86_64.
- Current Codex manual fetched through the official OpenAI documentation helper.
- `codex app-server --help`.
- `codex app-server generate-json-schema --out
  /data/tmp/agent-watchdog-codex-schema`.

No existing session, transcript, or user database content was inspected. The
generated protocol schemas contain `thread/list`, `thread/read`, exact parent
thread fields, lifecycle notifications, and `turn/interrupt` for the installed
version.

## Findings

The official app-server is a deep integration for clients that launch or connect
through that server. The current CLI supports stdio, Unix-socket, and WebSocket
transports, but official documentation still labels WebSocket transport
experimental and unsupported. Starting another app-server does not make it a
passive observer of unrelated CLI processes, so requiring it would violate
automatic discovery and change the user's launch workflow.

Official Codex hooks are enabled by default and expose `SessionStart`,
`SubagentStart`, `SubagentStop`, and `Stop`. Common input supplies the parent
session ID and cwd; subagent events add exact agent ID/type. Hooks require user
configuration and trust, so they are authoritative optional enrichment rather
than a prerequisite.

A static bind mount of only `state_5.sqlite` cannot expose sibling WAL/SHM files
inside the container. Binding those siblings individually is also not a durable
solution because SQLite may delete and recreate them, leaving the container
pinned to stale inodes. Mounting the entire Codex home would expose credentials
and configuration contrary to the concrete least-privilege inventory.

## Decision

- Ship the bounded Bearer-authenticated `/hooks/codex` endpoint and document
  optional user-level lifecycle hooks.
- Keep app-server parsing available for a future explicitly shared transport;
  do not make it a hidden v1 dependency or expose an unauthenticated listener.
- Treat Codex Companion graceful cancellation as unsupported until Companion
  publishes an authenticated endpoint with verifiable job ownership. Thread
  and turn IDs alone do not authorize sending `turn/interrupt` to a broker.
- Treat the single-file SQLite source as bounded bootstrap metadata, not as the
  sole live discovery source. Automatic live discovery must independently scan
  capability-mounted rollout roots and use process evidence, so missing WAL rows
  cannot hide a current session.
- Never broaden the Compose mount to all of `~/.codex` merely to read WAL/SHM.

## Implementation evidence

- `/hooks/codex` accepts bounded, Bearer-authenticated official lifecycle input
  without retaining prompt, result, or transcript bodies.
- `CodexDiscovery` scans capability-mounted rollout roots in descending
  lexicographic order, so timestamp-prefixed current paths win when a result
  budget is reached. The scanner retains only the bounded selected set and also
  enforces its wall-time budget.
- Rollout-only integration coverage proves current main/child metadata and
  subsequent activity are discovered without any visible SQLite database.
- Existing SQLite integration coverage still proves recent/unarchived filtering
  and exact spawn-edge preservation.

## Primary sources

- <https://developers.openai.com/codex/app-server>
- <https://developers.openai.com/codex/hooks>
