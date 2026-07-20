# Lessons learned

This document records durable implementation and QA lessons from the initial
Agent Watchdog release. It complements the normative requirements and
architecture documents; when a lesson changes product behavior, those
documents remain the source of truth.

## Runtime evidence is not automatically ownership evidence

- A path can be safe for the service to read without identifying the worktree
  owned by a particular session. Capability validation and semantic ownership
  validation are separate decisions.
- Codex Companion's `workspaceRoot` identifies the wrapper dispatch directory.
  It must not become a child session's filesystem ownership path. A real child
  worktree comes from trustworthy native evidence or explicit MCP
  registration.
- Multiple jobs reported by different wrapper sessions must remain distinct;
  do not collapse them behind a single coordinator-session identifier.
- Claude Code team-lead IDs, transcript IDs after `/clear`, wrapper teammate
  IDs, Companion job IDs, and Codex thread IDs are different identity layers.
  Correlate them through exact references or one unique bounded heuristic; a
  shared repository alone is insufficient when multiple mains match.
- Native runtime files are retained history, not an active-session registry.
  Fresh-store bootstrap applies recency to terminal/configuration artifacts,
  preserves genuinely active records, and performs a bounded lifecycle tail
  read before placing an incremental cursor at EOF. Tail recovery accepts only
  newline-delimited complete records; syntactically valid trailing JSON without
  its record boundary remains a partial write.

## Prefer privacy-preserving activity signals

- Companion job-log growth is useful without parsing command text, transcript
  bodies, or embedded timestamps. Offset growth is activity evidence; delayed
  observation lowers confidence instead of reconstructing history from
  sensitive content.
- CPU counters are positive activity evidence when they advance. Unchanged
  counters are neutral, not proof that an agent is stalled.
- Main sessions are never automatic termination targets. Safety decisions need
  fresh process identity and watcher-health evidence, not merely an elapsed
  timer.
- Reconciliation diagnostics are state changes, not heartbeat logs. A bounded
  per-session cache emits a correlation selection or ambiguity only when first
  observed or changed, keeping persistent uncertainty visible without flooding
  structured logs.

## Compatibility claims need the right environment

- A live runtime matrix requires disposable credentials and isolated runtime
  roots. Existing operator sessions are never acceptable fixtures, and Codex
  `--ephemeral` cannot prove on-disk auto-discovery.
- Linux cross-compilation does not replace a real macOS build when a bundled C
  dependency needs Apple's compiler and SDK. The macOS claim is therefore
  build-only CI on a real `macos-15` runner.
- Runtime-version drift follows an optimistic compatibility policy: continue
  on best effort, expose actionable `UPGRADE` warnings, and preserve evidence
  rather than silently dropping sessions.

## Release evidence must cover deployment behavior

- Workspace tests alone do not exercise the supported product boundary. Final
  QA also builds the release image, expands the supported Compose deployment,
  checks authentication and security headers through Traefik, runs the browser
  at desktop and mobile widths, and exercises explicit load/restart and
  slow-notification gates.
- Dependency policy and redistribution obligations are different checks.
  Allowing `CDLA-Permissive-2.0` in `cargo-deny` still requires distributing the
  exact third-party license text with source and container artifacts.
- Capacity claims should be recorded with reproducible fixtures, commands,
  environment, and thresholds. A single successful request is not a useful
  production-capacity gate.

## Deferred Claudius knowledge transfer

The prior Python watchdog was intentionally treated only as requirements
evidence. Its deferred knowledge-transfer audit is recorded in
`spikes/claudius-knowledge-transfer-audit.md`; the Rust implementation was
checked against those pitfalls only during final QA, as requested.

## Separate capabilities, watch targets, and event destinations

- Configured worktree prefixes are read capabilities, not requests to recursively
  watch every repository below them. Enumerating a broad prefix before runtime
  roots can consume every inotify target and delay lifecycle evidence until the
  periodic fallback.
- Linux inotify has no recursive directory mark for this unprivileged container,
  so runtime roots and exact worktrees require bounded existing-directory
  enumeration once at watcher construction and again only after topology or
  target-registry changes. Ordinary file appends do not rebuild the registry.
- Enumeration entries and inotify directory targets need separate limits.
  Thousands of transcript files must be inspected while finding directories but
  do not themselves consume watch targets.
- Watch targets carry typed destinations. Runtime changes request only the owning
  adapter; worktree changes request only ownership attribution; overflow,
  startup, configuration changes, and the five-minute fallback may request all
  adapters. Atomic runtime bits plus a one-second window coalesce append bursts
  without losing a different runtime's request.
- An automatic active-worktree watch requires exactly one active child owner.
  Shared worktrees cannot identify one child and are omitted unless a narrower
  durable MCP registration supplies exact ownership. Oversized exact unique
  worktrees remain actionable bounded-coverage degradation rather than silently
  claiming complete filesystem evidence.
