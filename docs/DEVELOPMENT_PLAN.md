# Agent Watchdog Development Plan

Status: ready after planning approval and context compaction

Date: 2026-07-17

Inputs: [REQUIREMENTS.md](REQUIREMENTS.md), [UX_SPECIFICATION.md](UX_SPECIFICATION.md), [ARCHITECTURE.md](ARCHITECTURE.md), [TEST_SPECIFICATION.md](TEST_SPECIFICATION.md)

## 1. Delivery strategy

Build Agent Watchdog as a sequence of vertical, testable capabilities. Safety and
evidence correctness precede runtime breadth and UI polish. Each milestone leaves
the workspace green and produces a usable diagnostic slice; no milestone depends
on an untested monolithic state machine.

Implementation must not begin until the current long planning context is
compacted and the new session has re-read these five planning documents.

## 2. Working rules

- Work only on feature branches/worktrees; never push directly to `main` or any
  protected/base branch.
- The product owner authorizes ordinary non-destructive pushes and GitHub actions
  through the repository collaborator credentials. Do not use `ghsudo`. Force
  pushes, branch deletion, history replacement, releases, merges, and other
  destructive or externally consequential actions still require explicit
  handling under the active environment instructions.
- After context compaction and planning approval, execute the plan unattended and
  continue through safe in-scope work. Pause only for a material product choice,
  required new authority, destructive action, or failed architecture assumption
  that changes documented behavior.
- Use TDD for domain, persistence, authorization, correlation, timer, and safety
  behavior: failing test, minimal implementation, refactor, self-review.
- Load and continuously apply the environment's `coding-best-practices`,
  `rust-best-practices`, `frontend-best-practices`, and
  `security-best-practices` guidance whenever the corresponding work is in
  scope; treat these as working constraints rather than a one-time review step.
- Stage specific files and keep commits conventional and reviewable.
- Run formatter, relevant tests, Clippy with warnings denied, and applicable
  dependency/security checks before implementation commits.
- Treat current runtime docs and live behavior as versioned inputs; record exact
  versions in compatibility tests.
- Never use real user sessions, transcripts, or processes in automated tests.
- Preserve a working server at every milestone; unfinished adapters remain
  disabled or explicitly degraded.
- Record decisions that materially depart from the architecture before coding
  the departure.
- Record non-trivial implementation decisions and spike results in the repository
  so a later unattended session does not depend on conversational memory.

## 3. Dependency order

```text
Phase 0: risk spikes
       │
       ▼
Phase 1: workspace + domain types
       │
       ├───────────────┐
       ▼               ▼
Phase 2: store      Phase 3: evidence infrastructure
       └───────┬───────┘
               ▼
Phase 4: coordinator/reducer service
               │
       ┌───────┼──────────┐
       ▼       ▼          ▼
Phase 5a    Phase 5b   Phase 5c
Claude      Codex      Companion adapters
       └───────┬──────────┘
               ▼
Phase 6: MCP + durable inbox
               ▼
Phase 7: termination saga
               ▼
Phase 8: web UI + notifications
               ▼
Phase 9: Compose/security/operations
               ▼
Phase 10: compatibility, load, release QA
```

Runtime adapters may be developed independently once the coordinator contract is
stable, but their integration should land one at a time to keep failures easy to
attribute.

## 4. Phase 0 — Resolve high-risk assumptions

### Objectives

Prove the host integration and performance assumptions that could invalidate the
architecture before building application code.

### Spikes

1. **Host PID/UID and signaling**
   - Run an unprivileged Compose container with host PID namespace and the
     operator's numeric UID/GID.
   - Read a purpose-built helper's `/proc` identity and four CPU counters.
   - Open a pidfd and signal only that helper.
   - Prove executable/start-time mismatch aborts the operation.
2. **Read-only runtime state**
   - Against isolated throwaway current Claude, Codex, and Companion sessions,
     list the minimum mount paths and confirm incremental/read-only access.
   - Confirm Codex SQLite WAL behavior under a read-only mount and document the
     fallback when it cannot be opened safely.
3. **Inotify and huge files**
   - Append to sparse large JSONL files through rotation, truncation, and partial
     writes; measure bytes read and overflow recovery.
4. **MCP transport scoping**
   - Verify the current `rmcp` Streamable HTTP server can associate initialized
     transports with one main-session scope and durable cursor semantics.
5. **Baseline capacity**
   - Run a synthetic 50-main/500-agent observation loop to establish initial
     memory, CPU, queue, and latency budgets for `TEST_SPECIFICATION.md`.

### Exit criteria

- Each spike has a short decision record under `docs/spikes/` with command,
  environment, result, and architectural impact.
- Any failed assumption is resolved in the architecture and approved before
  dependent work.
- No spike code is promoted without normal tests and review.

## 5. Phase 1 — Workspace, quality gates, and domain skeleton

### Deliverables

- Cargo workspace and the crate boundaries from the architecture.
- Pinned toolchain policy using current stable Rust; no initial MSRV promise.
- Formatting, Clippy, test, dependency-license/advisory, and Compose validation
  commands in local/CI workflows.
- Domain newtypes for main/child/native IDs, runtime kind, detailed/compact state,
  observations, evidence provenance, warnings, capabilities, deadlines, process
  identity, and events.
- `Clock`, ID factory, and policy interfaces with deterministic test versions.
- Redaction-safe error and tracing conventions.

### TDD slice

- Identity namespace and stable UUID behavior.
- Main/child type separation.
- State projection table.
- Observation bounds and idempotency identity.
- Secret/debug redaction.

### Exit criteria

- `watchdog-domain` has no I/O dependencies.
- macOS-target compilation is deferred to Phase 10; Linux-only operation remains
  explicit at platform boundaries meanwhile.
- T-DIS-010, T-STA-001, T-KILL-001, and core input-bound tests pass.

## 6. Phase 2 — SQLite store and transactional outbox

### Deliverables

- Embedded migrations for every table in Architecture §13.
- WAL/foreign-key/busy-timeout initialization and schema-version health.
- Repositories for observations, sessions/relations, snapshots, deadlines,
  termination sagas, file cursors, outbox/inbox offsets, adapter health, and
  notification attempts.
- Atomic `apply_observation` transaction boundary.
- Manual Watchdog-data wipe command guarded against external paths.

### TDD slice

- Duplicate observation insertion.
- Transaction failure at every boundary.
- Restart/outbox recovery.
- Concurrent read/write behavior.
- Migration forward application and incompatible-schema failure.
- Manual wipe isolation.

### Exit criteria

- T-DATA-001 through T-DATA-005 pass.
- Database corruption/migration errors fail readiness and disable termination.
- No full transcript body can enter the schema through typed APIs.

## 7. Phase 3 — Evidence infrastructure

### 7.1 Allowed roots and filesystem watcher

- Capability-root path resolver with canonical mount inventory.
- Shared `notify`/inotify watcher with deduplicated targets.
- Incremental cursor reader, partial record limits, rotation/truncation identity,
  overflow handling, scan budgets, and bounded reconciliation scheduler.
- Worktree ownership/attribution interface.

### 7.2 Linux process sampler

- `/proc` parser for PID/PPID/start time/state, executable, I/O, and `utime`,
  `stime`, `cutime`, `cstime`.
- Process-tree snapshot/delta calculation with counter-reset uncertainty.
- pidfd abstraction and fake implementation.
- Safe command fingerprint redaction.
- Non-Linux unsupported implementation behind the same traits.

### TDD slice

- T-EVD-001 through T-EVD-010.
- T-CPU-001 through T-CPU-010.
- T-SEC-001 and path race cases.

### Exit criteria

- A huge transcript append reads only the bounded suffix.
- “All four CPU times grew” produces strong activity corroboration.
- Unchanged CPU counters are neutral.
- Watcher/process uncertainty is surfaced and can suspend destructive action.

## 8. Phase 4 — Correlation, reducer, scheduler, and coordinator

### Deliverables

- Candidate correlation engine with lexicographic evidence and ambiguity gap.
- Deterministic session reducer and compact UI projection.
- Per-session serialized coordinator with bounded/coalescing observation queues.
- Monotonic stall/deadline/reminder scheduler and conservative restart behavior.
- Adapter supervisor and component health registry.
- Atomic reducer-to-store/outbox integration.

### TDD slice

- T-DIS-003 through T-DIS-005.
- T-STA-002 through T-STA-013.
- Property tests for duplicates, permutations allowed by causal order, conflict
  introduction/resolution, and restart.
- Queue overflow/coalescing tests proving terminal evidence preservation.

### Exit criteria

- Synthetic sessions progress through every normalized state correctly.
- Long-operation evidence blocks false stalls.
- Source conflicts become unknown and suspend automation.
- Repeat alerts follow the five-minute cadence without duplicate transitions.

## 9. Phase 5 — Runtime adapters

Each adapter lands as a separate reviewable slice with discovery, reconciliation,
compatibility status, health, and isolated live tests. Hooks/registration enrich
automatic discovery but are never prerequisites.

### Phase 5a — Claude Code

- Current hook event parser and optional hook installation documentation.
- Project/session JSONL incremental parser.
- Teams, tasks, members, and subagent hierarchy discovery.
- Native title/startup-directory derivation.
- Version guard and `UPGRADE` degradation.
- Graceful cancellation capability only if the current supported API is safe and
  documented; otherwise capability reports unsupported.

Exit: T-DIS-001/002 for Claude, applicable ingestion tests, and
T-LIVE-CLAUDE-001/002 pass in the live environment.

### Phase 5b — Native Codex CLI

- Current official hook/app-server event integration where available.
- Read-only thread/spawn-edge discovery with JSONL fallback.
- Rollout activity/operation parser and process correlation.
- Version guard and per-session `UPGRADE` warning.
- Official interruption path where supported.

Exit: native main/subagent exact hierarchy, fallback degradation, and
T-LIVE-CODEX-001/002 pass.

### Phase 5c — Codex Companion

- Per-workspace summary/job/log discovery.
- Non-atomic detail/summary reconciliation.
- PID/session/phase/terminal evidence and pruning tolerance.
- Supported graceful cancellation capability.

Exit: T-LIVE-COMP-001/002 and synthetic write-order/pruning tests pass.

### Cross-adapter exit criteria

- One adapter may be failed/degraded while the other two stay correct.
- Two runtimes sharing a repository cannot collide by native ID/path.
- The tested runtime version matrix is recorded without claiming older-version
  guarantees.

## 10. Phase 6 — MCP and durable parent experience

### Deliverables

- Streamable HTTP MCP transport behind constant-time Bearer authentication.
- Transport-to-main-session scope binding.
- All tools listed in Architecture §14 with bounded schemas and idempotency keys.
- Durable event cursor/inbox with evidence-rich parent payloads.
- Optional best-effort resource/event notification when supported by the client.
- Compatibility warning field suitable for the parent to relay to the user.

### TDD slice

- T-MCP-001 through T-MCP-008.
- Cross-tree access attempts, guessed IDs, reconnect/cursor behavior, duplicate
  progress IDs, oversized strings, and warning serialization.

### Exit criteria

- An autodiscovered parent can query its tree without prior child registration.
- A disconnected parent receives every meaningful undelivered child event.
- Failure of optional push has no effect on inbox correctness.

## 11. Phase 7 — Automated child termination

This phase starts only after adapter, process, reducer, and persistence health are
stable. It is feature-flagged off during development despite the final v1 default.

### Deliverables

- Pure termination gate accepting only `ChildSessionId`.
- Durable warning/grace/cancel/TERM/KILL saga.
- Parent deadline/intentional-waiting cancellation path.
- Runtime graceful cancellation adapters.
- pidfd signal executor with fresh identity/executable/runtime verification.
- TOML `SIGKILL` opt-out and global emergency-disable configuration.
- Structured audit events and health suspension.

### TDD slice

- T-KILL-001 through T-KILL-014.
- Model/property test that removes every gate condition.
- Linux helper integration tests for exit, TERM handling, TERM ignore, PID reuse
  simulation, and disabled KILL.

### Exit criteria

- Every release-blocking termination invariant is proven.
- No automated test targets a real runtime process.
- Human/code review explicitly signs off the safety gate before enabling the v1
  default in Compose.

## 12. Phase 8 — Approved web UI and notifications

### Deliverables

- Maud-rendered authenticated dashboard matching the approved HTML wireframe.
- Read-only JSON snapshot API and revisioned SSE with resync behavior.
- Active/all scope, attention/directory sorting, main-session cards, child counts,
  `UPGRADE`, responsive mobile layout, light/dark, reduced motion, and accessible
  semantics.
- Browser notification permission/status.
- Notification center plus Home Assistant and generic one-attempt webhooks.
- GitHub PR enrichment with cache and offline branch fallback.

### TDD/QA slice

- T-HTTP-001 through T-HTTP-005.
- T-UI-001 through T-UI-010.
- T-NOT-001 through T-NOT-005.
- Automated accessibility scan plus keyboard and narrow-viewport browser checks.
- Visual comparison against the approved artifact at desktop and mobile sizes.

### Exit criteria

- The operator can identify waiting/stalled sessions in a few seconds.
- Human notifications never contain PID or diagnostic internals.
- The dashboard stays useful and visibly stale while SSE reconnects.

## 13. Phase 9 — Compose, security, and operations

### Deliverables

- Multi-stage minimal image and reproducible build metadata.
- Supported `compose.yaml` with Traefik routes, IP allowlist, auth, health,
  unprivileged host UID/GID, host PID namespace, exact read-only mounts,
  persistent database, read-only root filesystem, and no direct app port.
- Example `.env` and mounted TOML with safe placeholders and validation docs.
- Detailed authenticated health and minimal liveness endpoints.
- Atomic SIGHUP/admin config reload.
- Structured tracing configuration and redaction tests.
- Installation, upgrade, compatibility-warning, manual wipe, and troubleshooting
  documentation.

### Security review

- Threat-model auth, session scoping, path traversal/symlink races, hostile
  transcripts, webhook behavior, native state races, PID reuse, container mounts,
  and signal permissions.
- Review every dependency and enabled feature; run license/advisory/source policy.
- Confirm no secrets, real transcript excerpts, broad mounts, or Docker socket.

### Exit criteria

- T-OPS-001 through T-OPS-007 and T-SEC-001/002 pass.
- A clean Linux host can follow documented `.env` + TOML + `docker compose up`.
- macOS workspace build is informative but not advertised as supported operation.

## 14. Phase 10 — Compatibility, scale, and v1 release QA

### Deliverables

- Exact current supported runtime/version matrix.
- macOS build-only compilation evidence without operation support claims.
- Full live-runtime suite in isolated roots.
- Target 50-main/500-agent load and restart soak.
- Large-transcript and filesystem-storm evidence.
- CPU/memory/queue/latency budgets filled into the test specification.
- Requirement-to-test-to-code traceability matrix.
- Fresh security/self-review and independent code review.
- MIT license, README product statement, architecture/operations links, and known
  limitations.

### Final verification

1. Run all default workspace formatting, test, Clippy, and dependency checks.
2. Run Linux Compose and security-context tests.
3. Run browser/accessibility checks at approved viewports.
4. Run load/soak suite.
5. Run explicitly enabled live-runtime tests and record versions/results.
6. Exercise a degraded adapter and prove unaffected runtime continuity.
7. Exercise every termination gate with the helper process.
8. Review logs/database/UI/webhooks for leaked transcript or credential content.
9. Only now read
   `/data/artifacts/claudius/2026-07-17/watchdog-knowledge-transfer.html`,
   interpret it in the context of `https://github.com/lklimek/claudius`, and
   record evidence that every applicable pitfall is avoided or resolved.

### Exit criteria

- All release-blocking invariants in the test specification pass.
- Every v1 requirement is implemented and traceable or explicitly re-approved as
  deferred.
- No unresolved critical/high security or correctness finding remains.
- Any test that could not run is reported as a release blocker or explicitly
  accepted limitation, never silently omitted.

## 15. Commit and review slices

Prefer commits that complete one coherent tested capability, for example:

1. `chore: scaffold Rust workspace and quality gates`
2. `feat(domain): define session observations and state projection`
3. `feat(store): add transactional observation reducer storage`
4. `feat(process): sample verified Linux process-tree activity`
5. `feat(runtime): add bounded inotify transcript ingestion`
6. `feat(claude): discover Claude sessions and subagents`
7. `feat(codex): discover Codex threads and spawn relations`
8. `feat(companion): reconcile workspace jobs`
9. `feat(mcp): expose scoped durable session inbox`
10. `feat(safety): add gated child termination saga`
11. `feat(web): add responsive session dashboard and SSE`
12. `feat(notify): deliver concise human notifications`
13. `chore(deploy): add hardened Compose deployment`

Actual commits may split further. Do not combine broad adapter work, destructive
automation, and deployment in one review.

## 16. Risks and planned responses

| Risk | Earliest resolution | Response |
|---|---|---|
| Read-only native Codex SQLite cannot reliably see WAL | Adapter integration | Prefer official events/JSONL; mark DB evidence optional/degraded. |
| Same-UID container cannot signal a freshly verified host helper | Resolved in Phase 0 | Container proof passed without added capabilities; production keeps fail-closed health and identity gates. |
| rmcp cannot bind scope as assumed | Resolved in Phase 0 | Custom manager injected its generated transport ID; production uses exact-once application binding and a separate durable inbox cursor. |
| Runtime formats drift during development | Phases 0/5 | Version guard, per-adapter live tests, `UPGRADE`, no destructive action. |
| Inotify watch count exceeds host limits | Phases 0/3 | Deduplicate roots, expose health, document host tuning, bounded reconciliation. |
| Shared worktree activity cannot be attributed | Phase 3 | Treat it as neutral for every child, per requirement. |
| False positives are noisy | Phases 4/10 | Keep sensitive defaults, attach evidence, tune runtime policy from measured live tests. |
| False negative during long command | Phases 3/4 | Combine four CPU counters, descendants, I/O, output, and native operation evidence. |
| SQLite/outbox contention at target scale | Phases 2/10 | Short transactions, coalescing, WAL, bounded queues; measure before adding infrastructure. |
| Termination regression | Phase 7 onward | Child-only types, pure gate, pidfd, fresh verification, feature flag, explicit safety review. |

## 17. Planned future compatibility, not v1 scope

The adapter contract and synthetic adapter tests remain the only OpenCode work in
v1. Token counts, cost, session details, automatic pruning, metrics, supported
macOS operation, multi-host service, and guaranteed agent push must not enter an
implementation milestone without a new requirements decision.

## 18. Definition of done for each phase

A phase is complete only when:

- its specified acceptance tests pass;
- formatting and relevant Clippy checks pass with warnings denied;
- public APIs and operational behavior are documented;
- new dependencies have rationale and review;
- logs/errors are reviewed for secrets and unbounded data;
- code is self-reviewed against the architecture and coding practices;
- changes are committed on the feature branch;
- anything not run or any deviation is reported explicitly.

## 19. Implementation-session bootstrap after compaction

The fresh implementation context should begin by reading, in this order:

1. `AGENTS.md` instructions provided for the environment;
2. `docs/REQUIREMENTS.md`;
3. `docs/UX_SPECIFICATION.md`;
4. `docs/ARCHITECTURE.md`;
5. `docs/TEST_SPECIFICATION.md`;
6. this development plan;
7. current Git status/log and Phase 0 only.

It must confirm no material decision is missing, update the working plan to Phase
0, and avoid importing assumptions from the pre-compaction conversation that are
not present in these documents.

## 20. Clean-context execution contract

The implementation agent must assume the planning conversation is unavailable.
Before editing code it must:

1. verify the five documents exist and their status/precedence matches
   `REQUIREMENTS.md` §14;
2. inspect live Git status, remote, default branch, open PRs, and repository
   instructions instead of trusting any recorded ephemeral branch state;
3. search project memory and live neighboring repositories as leads, then verify
   every relevant finding against files and current primary documentation;
4. confirm the approved wireframe remains the v1 UX target;
5. begin with Phase 0 spikes and write their results into `docs/spikes/`;
6. update architecture/test/development documents before code if a spike changes
   an assumption;
7. keep implementation decisions within documented outcomes; request product
   input when alternatives would change scope, safety, user experience, or
   runtime support;
8. run unattended through reversible, non-destructive, in-scope work, reporting
   decisions and verification at phase boundaries;
9. never infer OpenCode implementation, token accounting, cost, metrics,
   automatic pruning, supported macOS operation, or writable human controls into
   v1;
10. never weaken main-session exclusion, waiting-user protection, identity/PID
    verification, source-conflict suspension, or the one-hour-plus-grace
    termination policy to make an integration easier.

The clean-context agent should be able to derive every product and architectural
choice from the five documents. Conversation history is neither required nor
authoritative once these documents are committed.

## 21. Live-runtime corrective increment

The 2026-07-20 owner-authorized local-system probe found related integration
gaps: Codex completion was flattened into progress; inactive Claude members
aged into stalled; a retained Claude team lead and its post-reset transcript
became two main cards; and Claude-originated Codex threads appeared as top-level
mains. Companion wrapper IDs did not reuse Claude transcript aliases, retained
terminal jobs polluted a fresh database, the default Compose template omitted
the separate `/data/git-worktrees` prefix, and a first-seen completed Codex
rollout initialized its cursor at EOF without recovering terminal state.
Simultaneous adapter warnings were visually indistinguishable, old team configs
polluted a fresh active view, and unchanged correlation outcomes were logged on
every filesystem invalidation.

Execute this increment in the normal TDD order:

1. Add RED parser/discovery/render regressions T-DIS-013–020 and T-UI-011.
2. Decode Codex lifecycle payload types without retaining bodies.
3. Retain Claude member activity state, terminalize inactive members, apply the
   24-hour team-config bootstrap gate, and add the unique lead alias.
4. Retain Codex originator metadata and attach explicit Claude-launched threads
   only through a unique validated directory/repository match.
5. Share Claude aliases with Companion discovery, admit historical terminal jobs
   only when recent, and recover the newest lifecycle record with one bounded
   tail read before initializing a Codex rollout cursor at EOF.
6. Render human-readable runtime labels on page-level warnings; correct the
   standard Companion state path; and add a second concrete read-only worktree
   mount/mapping for the agent worktree prefix.
7. Cache bounded Codex-correlation log outcomes so only a first or changed
   selection/ambiguity is emitted.
8. Run targeted adapter/server tests, workspace formatting, workspace tests,
   strict Clippy, and a fresh-volume Compose probe against the owner-authorized
   real local runtime files.
9. Record exact live outcomes and unresolved evidence limitations before commit.

The existing production-like database is not rewritten or auto-pruned by this
increment. Fresh-volume validation proves bootstrap behavior; operators may use
the documented manual wipe for already-retained duplicate/history records.
