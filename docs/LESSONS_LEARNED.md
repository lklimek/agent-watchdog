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
