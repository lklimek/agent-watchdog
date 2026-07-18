# Current runtime and native-state spike

Status: metadata verified; isolated live exercises require dedicated credentials

Date: 2026-07-17

## Question

Do the current supported runtimes still expose the discovery and hierarchy
evidence assumed by the architecture, and which concrete paths require
read-only mounts?

## Environment

- Linux 7.0.0-27-generic x86_64
- Claude Code 2.1.214
- Codex CLI 0.144.5
- Codex Companion 1.0.6
- OpenCode 1.17.15, inspected only as future-adapter context

Versions came from `claude --version`, `codex --version`, `opencode --version`,
and the Companion plugin manifest. No transcript body or session message was
read during this spike.

## Evidence

Claude's current official hooks reference still defines `SessionStart`,
`SubagentStart`, and `SubagentStop`. Common input includes `session_id`, `cwd`,
and `transcript_path`; subagent events add `agent_id`, `agent_type`, and, on
stop, `agent_transcript_path` and `last_assistant_message`. Default local roots
present on the host are `~/.claude/projects`, `~/.claude/teams`,
`~/.claude/tasks`, and the optional hook inbox configured by the operator.

Codex's official app-server remains the preferred deep integration and exposes
conversation history, approvals, and streamed agent events. The current local
SQLite database is `~/.codex/state_5.sqlite`, uses WAL, and still contains
`threads`, `thread_spawn_edges`, `agent_jobs`, and `agent_job_items`. Schema-only
inspection confirmed exact parent/child columns and current thread metadata,
including cwd, title, Git fields, CLI version, nickname, role, model, and rollout
path. Rollout JSONL under `~/.codex/sessions` remains the read-only fallback.

Companion 1.0.6 resolves per-workspace data beneath
`$CLAUDE_PLUGIN_DATA/state/<workspace>-<hash>` and falls back to the system
temporary directory when the plugin variable is absent. It stores `state.json`
plus `jobs/`, caps summary jobs at 50, deletes pruned detail/log files, and writes
the summary directly. The adapter must therefore parse summary/detail
independently, tolerate pruning, and never infer failure from absence alone.

## Minimum read-only mount inventory

- Concrete Claude projects, teams, tasks, and configured hook-inbox roots.
- `~/.codex/state_5.sqlite` together with its WAL/SHM siblings when present, plus
  concrete session/rollout roots.
- The concrete Companion plugin-data state root used by the installation.
- Configured repository/worktree prefixes used for activity attribution.
- Host `/proc` visibility through the host PID namespace, not a broad filesystem
  mount.

Every path is canonicalized against a configured allowlist. The Compose file
must not mount an entire home directory merely to cover these defaults.

## Decision

The adapter strategy remains valid. Official hooks/app-server evidence has
priority, while native files stay version-guarded, read-only, and optional.
Live creation of throwaway Claude, Codex, and Companion children remains an
explicit adapter compatibility test; automated tests never inspect existing
user sessions.

## 2026-07-18 live-exercise isolation result

The installed versions were rechecked as Claude Code 2.1.214 and Codex CLI
0.144.5; the inspected Companion manifest remains 1.0.6. No existing runtime
session or transcript was opened.

The current Codex CLI offers `--ephemeral`, but that mode deliberately writes no
session files and therefore cannot verify automatic disk discovery. Its
`--ignore-user-config` option still uses the runtime state root for
authentication. No dedicated throwaway credentials/runtime account were
provided for Codex, Claude, or Companion, and borrowing the operator's existing
state would violate the live-test isolation rule. The live matrix is therefore
recorded as **not run**, not passed. It may run only in a disposable runtime
account/container with dedicated credentials and dedicated native roots; absence
of that environment must never be worked around by reading real user state.

## Primary sources

- <https://code.claude.com/docs/en/hooks>
- <https://code.claude.com/docs/en/agent-teams>
- <https://learn.chatgpt.com/docs/app-server>
- <https://learn.chatgpt.com/docs/agent-configuration/subagents>
