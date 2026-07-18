# Implementation checkpoint

This file is the clean-context handoff for continuing implementation. Read it
with `REQUIREMENTS.md`, `ARCHITECTURE.md`, `TEST_SPECIFICATION.md`,
`DEVELOPMENT_PLAN.md`, `TRACEABILITY.md`, and `OPERATIONS.md`; those documents
remain normative.

## Repository state

- Working branch: `feat/implementation`.
- Latest implementation commit before this handoff refresh:
  `1b4c4f5 fix(codex): discover sessions without sqlite wal`.
- No implementation commits in this phase have been pushed. Pushing remains an
  explicit user decision; never push implicitly or directly to `main`.
- The implementation worktree was clean immediately after `1b4c4f5`; the
  handoff refresh itself is documentation-only.
- Implementation is in Phase 10 integration hardening. Final release QA and the
  lessons-learned phase have not started.
- Do not work on macOS compatibility until the end of the project.

## Implemented and locally committed

- Rust workspace, typed domain/reducer/correlation/safety model, SQLite WAL store,
  transactional outbox, restartable reducer lanes, and bounded health records.
- Automatic Claude Code, native Codex CLI, and Codex Companion discovery with
  exact hierarchy where native state provides it, incremental cursors, bounded
  scans, compatibility warnings, process/CPU evidence, worktree attribution,
  GitHub metadata, and failure containment.
- Claude projects/transcripts, teams/members/tasks, and official lifecycle hook
  ingestion. Hook paths remain untrusted and are not accepted as capabilities.
- Codex state database/spawn-edge ingestion, independent recent rollout
  discovery/activity tailing, and official lifecycle-hook ingestion. The narrow
  SQLite bind is bootstrap evidence and may lag WAL rows; rollout, process, and
  optional hook sources continue independently. App-server parsing remains
  adapter-ready because app-server cannot passively observe unrelated CLI
  processes.
- Companion summary/detail reconciliation and content-free log-growth activity.
- MCP tools and durable parent inbox, authenticated HTTP transport, child-only
  staged termination, process identity/PID-reuse gates, deadlines, reminders,
  SSE dashboard, human notifications, GitHub enrichment, configuration reload,
  structured tracing, hardened Docker image, and Traefik Compose deployment.
- Dashboard matches the approved HTML wireframe: main-session cards by default,
  attention/idle ordering, child status counts, directory sort, live SSE,
  responsive mobile layout, accessibility basics, and no transcript bodies.
- Token counts/cost are not in v1. OpenCode and reliable agent push remain future
  enhancements.

## Latest integration-hardening slices

- `abe0242`: explicit 50-main/500-session restart soak (ten reopen/reconcile
  cycles) added to the production-service load target.
- `21bf7a0`: hanging webhook gate proves the five-second production timeout does
  not block agent ingestion and is never retried after acknowledgement.
- `270e928`: termination now fails closed when watcher coverage is degraded.
- `d720c76`: filesystem reconciliation has independent generation-guarded health;
  a newer overflow cannot be cleared by an older scan. Dashboard delivery has a
  separate component and can no longer overwrite safety health.
- `cabf59f`: official Codex lifecycle hooks now enter through a bounded,
  Bearer-authenticated endpoint without retaining prompt/result bodies.
- `1b4c4f5`: current Codex main/child rollout metadata is discovered even when
  SQLite is absent or behind WAL. Descending bounded scans prefer current
  timestamped paths, preserve exact hierarchy, and tail subsequent activity.

## Verification already completed

- Exact Playwright binary:
  `/home/ubuntu/.nvm/versions/node/v22.23.1/bin/playwright`.
- Authenticated Compose browser suite: 3/3 passed for dashboard content/security,
  a 360-CSS-pixel viewport without horizontal overflow, and keyboard controls.
- `cargo test -p watchdog-server --all-targets`: passed after the health changes
  (ignored release load/slow-peer tests compile but do not run by default).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed
  after the health changes.
- `cargo test --workspace --all-targets`: passed at the start of the 2026-07-18
  continuation after the hook slice.
- Rollout-only and SQLite-backed Codex discovery tests pass together; all six
  cross-adapter discovery tests, all seven scan/attribution tests, and strict
  Clippy for `watchdog-runtime` plus `watchdog-server` pass after `1b4c4f5`.
- Explicit load target: 2/2 passed; 500-session ingestion about 379–416 ms,
  dashboard 32–35 ms, high-water RSS 13.2–17.0 MiB, and ten restart cycles. See
  `docs/spikes/capacity.md` for exact commands and budgets.
- Explicit hanging-webhook test: passed in 5.04 seconds.
- Watcher saturation, complete termination suite, huge sparse transcript cursor,
  and server Compose-contract tests passed.
- Supported Compose image rebuilt from the checkpoint source. Both containers
  were healthy after recreation. A live burst created 12,000 QA worktree files
  in 232 ms; the service stayed responsive. The burst fixture was then removed
  and the application container recreated successfully.

Run a full workspace test again before the next commit because the last broad
test was server-only even though strict workspace Clippy covered every crate.
Use the cached Cargo wrapper without `CLAUDIUS_FORCE=1`:

```text
CLAUDIUS_CACHE_DIR=/data/tmp/agent-watchdog-claudius-cache \
  /home/ubuntu/.codex/claudius/scripts/cargo-cached.sh <cargo arguments>
```

## Reusable QA environment

- Compose environment: `/data/tmp/agent-watchdog-compose-qa/.env`.
- Scratch Playwright config/test:
  `/data/tmp/agent-watchdog-compose-qa/playwright.config.mjs` and
  `/data/tmp/agent-watchdog-compose-qa/dashboard.spec.mjs`.
- Durable screenshots/results:
  `/data/artifacts/agent-watchdog/2026-07-17/qa/`.
- Local image: `ghcr.io/lklimek/agent-watchdog:qa`.
- Published QA route: `http://127.0.0.1:18080/ui` through Traefik.
- The intentionally invalid QA Codex SQLite fixture degrades only the Codex
  adapter. Claude, Companion, health, UI, and MCP continue. This is useful
  failure-containment evidence, not a product defect.
- Termination automation is disabled in the QA TOML.

## Remaining Phase 10 work

1. Close the remaining functional gaps recorded in `TRACEABILITY.md`: complete
   parent diagnostics and the supported standard-path configuration wording.
2. Complete explicitly isolated live-runtime tests for current Claude, Codex CLI,
   and Companion versions. Never inspect existing user sessions.
3. Measure the remaining release budgets: ten-minute steady-state CPU and a
   representative production burst p99. Record exact environment/results.
4. Run full format, workspace test, strict Clippy, `cargo deny`, `cargo audit`,
   Compose config/security checks, browser QA, explicit load/slow-peer tests, and
   any supported live matrix tests.
5. Perform fresh security/self-review and independent review; fix all critical or
   high findings.
6. Only during final QA, read
   `/data/artifacts/claudius/2026-07-17/watchdog-knowledge-transfer.html`,
   interpret it in the context of `https://github.com/lklimek/claudius`, and
   audit the implementation against every applicable pitfall. Do not read it
   earlier.
7. Do the deferred macOS build-only check at the end without adding unsupported
   deployment features.
8. Update final version matrix, known limitations, release evidence, TODOs, and
   lessons learned; then stop for the user's explicit push decision.
