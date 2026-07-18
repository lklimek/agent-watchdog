# Implementation checkpoint

This file is the clean-context handoff for continuing implementation. Read it
with `REQUIREMENTS.md`, `ARCHITECTURE.md`, `TEST_SPECIFICATION.md`,
`DEVELOPMENT_PLAN.md`, `TRACEABILITY.md`, and `OPERATIONS.md`; those documents
remain normative.

## Repository state

- Working branch: `feat/implementation`.
- Latest committed checkpoint before the final-QA slice:
  `6504412 docs: record production capacity gates`.
- No implementation commits in this phase have been pushed. Pushing remains an
  explicit user decision; never push implicitly or directly to `main`.
- The final-QA slice includes a Companion ownership correction, release-license
  packaging, a build-only macOS CI job, final evidence, and handoff documents.
- Implementation, Linux final QA, self-review, and lessons learned are complete.
  Only the explicitly external closure items listed below remain.
- macOS runtime compatibility remains unsupported; only the end-of-project
  build-only CI claim was added.

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
- `7b0cd27`: durable tree-scoped MCP watch-path registration validates concrete
  native paths through configured read-only capability mappings, restores them
  after restart, prioritizes them in the watcher budget, and attributes exact
  child filesystem activity without expanding Compose mounts.
- `ec3258a`: Linux process sampling now precedes every timer evaluation and
  retains only the latest CPU delta per session/kind. Durable parent events
  expose a bounded diagnostic bundle with PID identity, CPU provenance/times,
  operation, conflicts, typed correlation basis/evidence, and suggested checks.
  Standard/additional runtime roots now have an explicit tested
  native-to-mounted configuration contract. `TRACEABILITY.md` has no remaining
  functional gaps.

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
- `cargo test --workspace --all-targets` passed after the diagnostic bundle and
  typed correlation-basis changes. The new parent diagnostic and process CPU
  persistence tests appear in the run; the release load/slow-peer gates remain
  explicitly ignored by the default suite.
- The focused standard/additional runtime-mapping test passed after its final
  addition. Strict Clippy passed for domain, store, and server with all targets
  and warnings denied after all functional-closure changes.
- Watch-path store, capability/restart, MCP schema, filesystem ownership, full
  server, and strict Clippy suites passed before `7b0cd27`.
- Explicit load target: 2/2 passed; 500-session ingestion about 379–416 ms,
  dashboard 32–35 ms, high-water RSS 13.2–17.0 MiB, and ten restart cycles. See
  `docs/spikes/capacity.md` for exact commands and budgets.
- Explicit hanging-webhook test: passed in 5.04 seconds.
- Isolated production-container capacity gates passed with 50 mains/450 children:
  500 authenticated lifecycle events at concurrency 32 had 65.732 ms p99 and
  168.636 ms maximum latency; the server remained ready. A subsequent exact
  600-second no-change run averaged 0.000% CPU at Docker's two-decimal precision
  and reached 30.050 MiB maximum sampled container memory. A second distinct
  500-event pass kept all 21 concurrent rendered-UI requests at HTTP 200 with
  49.480 ms p99/maximum latency. See
  `docs/spikes/capacity.md` and the durable 2026-07-18 QA artifact.
- Watcher saturation, complete termination suite, huge sparse transcript cursor,
  and server Compose-contract tests passed.
- Supported Compose image rebuilt from the checkpoint source. Both containers
  were healthy after recreation. A live burst created 12,000 QA worktree files
  in 232 ms; the service stayed responsive. The burst fixture was then removed
  and the application container recreated successfully.

Use the cached Cargo wrapper without `CLAUDIUS_FORCE=1` for all remaining Rust
verification:

```text
CLAUDIUS_CACHE_DIR=/data/tmp/agent-watchdog-claudius-cache \
  /home/ubuntu/.codex/claudius/scripts/cargo-cached.sh <cargo arguments>
```

## QA environments and evidence

- The earlier Compose/Playwright scratch environment beneath
  `/data/tmp/agent-watchdog-compose-qa` did not survive the host reboot. Its
  durable screenshots/results remain under the artifact path below; recreate
  fresh scratch configuration for the final browser rerun.
- Durable screenshots/results:
  `/data/artifacts/agent-watchdog/2026-07-17/qa/`.
- Local image: `ghcr.io/lklimek/agent-watchdog:qa`.
- Published QA route: `http://127.0.0.1:18080/ui` through Traefik.
- The intentionally invalid QA Codex SQLite fixture degrades only the Codex
  adapter. Claude, Companion, health, UI, and MCP continue. This is useful
  failure-containment evidence, not a product defect.
- Termination automation is disabled in the QA TOML.
- The 2026-07-18 capacity environment uses only synthetic roots beneath
  `/data/tmp/agent-watchdog-perf`, image `agent-watchdog:perf`, and port 18081.
  Its durable report is
  `/data/artifacts/agent-watchdog/2026-07-18/qa/container-capacity.md`.
  Both exact QA containers were stopped and removed after evidence capture; the
  image, database, and synthetic fixtures remain for a reproducible rerun.
- Installed runtime versions were rechecked without opening user state. Codex
  `--ephemeral` cannot exercise disk discovery, while a persisted exercise
  requires a dedicated authenticated runtime root. With no isolated credentials
  provided, the Claude/Codex/Companion live matrix is explicitly **not run** and
  remains a release limitation rather than being silently exercised against
  operator sessions.
- The deferred Claudius knowledge-transfer document was read during final QA and
  audited item by item in `docs/spikes/claudius-knowledge-transfer-audit.md`.
  One applicable mismatch was fixed: Companion `workspaceRoot` no longer grants
  automatic child filesystem ownership because it identifies the dispatch cwd,
  not necessarily the job worktree.
- Final Linux QA passed: workspace format/tests/strict Clippy, production and
  Phase-0 `cargo-audit`/`cargo-deny`, Compose expansion/security contract,
  release image build/license extraction, explicit load/restart and slow-peer
  gates, and a fresh 3/3 Playwright run through Traefik.
- The Linux host cannot fully cross-check the SQLite-backed workspace for macOS
  because it has no Apple compiler/SDK; the failure occurs in bundled SQLite C
  compilation before project Rust code. Domain/process/testkit crates check for
  `x86_64-apple-darwin`, and CI now has a build-only `macos-15` workspace job.
  That CI job is unexecuted until the branch is pushed.
- Final diff/security self-review found no remaining critical or high issue.
  The Companion ownership correction, exact license distribution, and macOS CI
  job are covered by tests or release evidence above.

## Remaining external closure work

1. Run the explicitly isolated live-runtime matrix only if a disposable
   credentialed runtime environment is supplied. Never inspect existing user
   sessions; otherwise retain the recorded not-run release limitation.
2. Obtain an actual macOS CI result after the branch is pushed; the Linux
   cross-check cannot compile bundled SQLite without an Apple SDK.
3. Stop for the user's explicit push decision. Never push implicitly or
   directly to `main`.
