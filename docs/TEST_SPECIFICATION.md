# Agent Watchdog Test Specification

Status: proposed for implementation

Date: 2026-07-17

Inputs: [REQUIREMENTS.md](REQUIREMENTS.md), [ARCHITECTURE.md](ARCHITECTURE.md), [UX_SPECIFICATION.md](UX_SPECIFICATION.md)

## 1. Purpose

This specification defines the evidence required to claim Agent Watchdog v1 is
correct. Its highest priority is preventing false destructive action. Its second
priority is detecting missing or stalled delegated work quickly enough to save
coordinator time. Performance, compatibility containment, security, and the
approved human experience follow.

Runtime compatibility tests use live, explicitly enabled installations. Pure
logic uses synthetic typed observations and fake time; tests do not depend on
captured private transcripts or unstable native blobs.

## 2. Test principles

- Write a failing test before production logic for reducers, correlation,
  timers, authentication, path policy, persistence, and termination gates.
- Test observable behavior and safety invariants, not private function layout.
- Use deterministic clocks, IDs, filesystem roots, and process samples.
- Model native input through typed adapter fixtures generated in test code.
- Never run destructive tests against a real agent process or user runtime data.
- Live-runtime tests create isolated temporary sessions under dedicated roots and
  require an explicit opt-in environment flag.
- A flaky test is a defect; it is not retried to manufacture a pass.
- Every regression gets a minimal test at the lowest layer that can reproduce it.

## 3. Test layers

| Layer | Scope | Default execution |
|---|---|---|
| Unit | Domain reducer, correlation, timer policy, path/credential helpers | Every commit |
| Component | SQLite store, parsers, watcher cursors, process sampler, routes | Every commit or targeted crate |
| Integration | Server with synthetic adapters, MCP, SSE, outbox, restart | CI |
| Compose | Mounts, UID/PID behavior, Traefik routes/auth, health | Linux CI/manual gate |
| Load/soak | 50 mains/500 agents, large transcripts, event bursts | Release gate |
| Live compatibility | Current Claude, Codex CLI, Codex Companion | Explicit manual/nightly environment |
| Visual/accessibility | Approved dashboard at desktop/mobile, keyboard/a11y checks | CI plus approval gate |

## 4. Test infrastructure

`watchdog-testkit` provides:

- a paused Tokio clock and wall/monotonic `TestClock`;
- stable ID and observation builders;
- synthetic runtime adapters with programmable capabilities/failures;
- an isolated SQLite database with migrations;
- temporary capability roots and controlled filesystem mutations;
- fake process snapshots, PID reuse, process trees, and signal recorder;
- an HTTP/MCP client harness with redacted request capture;
- a webhook receiver with attempt counter and response controls;
- SSE clients that can disconnect, lag, and reconnect;
- property-test generators for observation order, duplicate delivery, and state
  conflicts.

Linux process integration tests spawn a purpose-built helper binary owned by the
test user. The helper can consume CPU, sleep, fork children, emit output, ignore
`SIGTERM`, and report its PID/start time. It never resembles a supported runtime
executable unless the test explicitly supplies a fake runtime classifier.

## 5. Required test cases

### 5.1 Identity, discovery, and correlation

| ID | Scenario | Expected result | Requirements |
|---|---|---|---|
| T-DIS-001 | Discover supported main and child without MCP registration. | Stable sessions and hierarchy appear. | FR-DIS-001, FR-DIS-002 |
| T-DIS-002 | Two worktrees in one repository run concurrently. | Separate main roots, activity clocks, and cards. | FR-DIS-009 |
| T-DIS-003 | Exact native parent conflicts with a heuristic candidate. | Exact relation wins; rejected evidence is retained. | FR-DIS-005, FR-DIS-006 |
| T-DIS-004 | Two heuristic parents score too closely. | Child remains unassigned/unknown; no guessed relation. | FR-DIS-006 |
| T-DIS-005 | MCP relation enriches a heuristic-only child. | Relation changes transactionally without duplicate alerts. | FR-DIS-005, FR-STA-009 |
| T-DIS-006 | Path registration is inside an allowlisted root. | Target is accepted and watched. | FR-DIS-007 |
| T-DIS-007 | Path registration is outside or escapes an allowlisted root. | Request is rejected without access. | FR-DIS-007, FR-SEC-002 |
| T-DIS-008 | Default and overridden runtime locations coexist. | Both are discovered within configured mounts. | FR-DIS-008 |
| T-DIS-009 | Runtime-neutral adapter emits API parent/child events. | Core represents them without filesystem fields. | FR-DIS-003 |
| T-DIS-010 | Native session ID repeats in two runtimes. | Watchdog identities remain distinct. | FR-DIS-004 |
| T-DIS-011 | Codex SQLite contains recent, old, and archived threads plus an exact spawn edge. | Only recent unarchived threads bootstrap; the exact edge is retained. | FR-DIS-002, FR-DIS-006 |
| T-DIS-012 | Current Codex rollout metadata exists while SQLite is absent or behind its WAL. | Recent main/child hierarchy and subsequent activity are discovered from bounded rollout reads. | FR-DIS-001, FR-DIS-002, FR-DIS-006 |
| T-DIS-013 | Codex rollout emits `event_msg.payload.type=task_started`, then `task_complete`. | The child transitions running then completed; transcript content is neither retained nor logged. | FR-DIS-010, FR-SEC-001 |
| T-DIS-014 | A recent Claude transcript and exactly one recent team lead have the same validated cwd but different session IDs. | Only the configured lead is a main session and transcript activity advances it; two matching leads remain unaliased. | FR-DIS-005, FR-DIS-011 |
| T-DIS-015 | A Claude team member changes from active to inactive without a hook record. | The retained child becomes completed in the next reconciliation and cannot age into stalled. | FR-DIS-012, FR-STA-006 |
| T-DIS-016 | Codex metadata says `originator=Claude Code` and exactly one active Claude main matches its directory or repository. | Codex is registered beneath that Claude root with logged correlation basis/confidence; ambiguous candidates are not joined. | FR-DIS-005, FR-DIS-006, FR-DIS-013 |
| T-DIS-017 | A team config is older than the 24-hour bootstrap window in a fresh store. | It does not create a main or child; a recent config still does. | FR-DIS-014, FR-EVD-008 |
| T-DIS-018 | A Companion job names a wrapper session whose recent Claude transcript uniquely aliases to a team member. | The job reuses the Claude team root; no wrapper main is created. | FR-DIS-005, FR-DIS-015 |
| T-DIS-019 | One old terminal Companion job and one active job exist in a fresh store. | Only the active job bootstraps; a tracked job can still accept a later summary-only terminal transition. | FR-DIS-016, FR-EVD-008 |
| T-DIS-020 | Watchdog first sees a Codex rollout after its final `task_complete` was already written, or while that final JSON object lacks its newline boundary. | One bounded tail read recovers only a complete terminal record before the durable EOF cursor is initialized; a partial final record is ignored and subsequent scans remain incremental. | FR-DIS-010, FR-EVD-002, FR-EVD-004, FR-EVD-008 |

### 5.2 Filesystem ingestion and native parsing

| ID | Scenario | Expected result | Requirements |
|---|---|---|---|
| T-EVD-001 | Append one JSONL record to a multi-gigabyte sparse transcript. | Only bounded appended bytes are read. | FR-EVD-001, FR-EVD-002, FR-EVD-008 |
| T-EVD-002 | Append half a record, then complete it. | No state change until one complete record exists. | FR-EVD-004 |
| T-EVD-003 | Replace, truncate, or rotate a watched file. | Cursor safely reconciles; no invented terminal event. | FR-EVD-003 |
| T-EVD-004 | Simulate inotify queue overflow. | Affected scope becomes uncertain and bounded reconciliation runs. | FR-EVD-003, FR-EVD-009 |
| T-EVD-005 | Parser receives malformed or unknown fields. | Last trusted state remains; warning/backoff is recorded. | FR-EVD-004, FR-COMP-002 |
| T-EVD-006 | Directory exceeds depth/entry/byte budget. | Scan stops, health degrades, server remains responsive. | FR-EVD-008, FR-OPS-004 |
| T-EVD-007 | One child owns a worktree and a file changes. | Its activity clock advances. | FR-EVD-005 |
| T-EVD-008 | Multiple children share a worktree; attribution is absent. | No child activity clock advances. | FR-EVD-006 |
| T-EVD-009 | Multiple children share a worktree; one process has attributable file evidence. | Only that child advances. | FR-EVD-006 |
| T-EVD-010 | Filesystem and authoritative native state materially disagree. | Session becomes unknown and termination is suspended. | FR-EVD-009 |

### 5.3 Process and CPU evidence

| ID | Scenario | Expected result | Requirements |
|---|---|---|---|
| T-CPU-001 | `utime` grows between samples. | Activity is recorded. | FR-EVD-007, FR-EVD-010 |
| T-CPU-002 | `stime` grows between samples. | Activity is recorded. | FR-EVD-007, FR-EVD-010 |
| T-CPU-003 | `cutime` or `cstime` grows. | Activity is recorded with child-counter provenance. | FR-EVD-010 |
| T-CPU-004 | All four counter classes grow. | Strong CPU corroboration is recorded once. | FR-EVD-010 |
| T-CPU-005 | Counters are unchanged. | Result is neutral; no stall transition occurs from CPU alone. | FR-EVD-010 |
| T-CPU-006 | Live descendant consumes CPU while parent sleeps. | Process-tree activity is recorded for the session. | FR-EVD-007 |
| T-CPU-007 | A counter decreases or PID start time changes. | Delta is invalid/uncertain, never negative inactivity. | FR-EVD-010, FR-KILL-005 |
| T-CPU-008 | Long-running test has CPU, I/O, child, or output progress but silent transcript. | It stays active beyond stall thresholds. | FR-EVD-007 |
| T-CPU-009 | Process exits between enumeration and sampling. | Race is tolerated; no synthetic failure without reconciliation. | FR-EVD-009 |
| T-CPU-010 | Command line contains a secret-shaped value. | Logs/evidence omit or redact it. | FR-OPS-006, FR-SEC-001 |

### 5.4 State reduction, deadlines, and alerts

| ID | Scenario | Expected result | Requirements |
|---|---|---|---|
| T-STA-001 | Detailed runtime states normalize and compact correctly. | API and UI projections match the status model. | §7, FR-API-003 |
| T-STA-002 | No progress reaches 5m and 15m with no active evidence. | Suspect then stalled transitions occur exactly once. | FR-STA-004, FR-STA-005 |
| T-STA-003 | Waiting for user crosses every time threshold. | Never stalled or eligible for termination. | §7.3, FR-KILL-002 |
| T-STA-004 | Parent extends/shortens a deadline. | Timer changes immediately and auditable event is stored. | FR-STA-001, FR-STA-002 |
| T-STA-005 | Parent pauses/resumes or marks intentional waiting. | Appropriate timers stop/restart without elapsed-time leakage. | FR-STA-002 |
| T-STA-006 | No MCP heartbeat arrives while native progress continues. | Session remains active. | FR-STA-003 |
| T-STA-007 | Verified PID dies or native state fails. | Failure alert occurs within one reconciliation cycle. | FR-STA-006 |
| T-STA-008 | Alert threshold is reached. | Fresh cheap checks run and evidence fields are complete. | FR-STA-007 |
| T-STA-009 | Stall remains unresolved for 16 minutes. | Reminders occur at five-minute cadence, without duplicate transitions. | FR-STA-008, FR-NOT-004 |
| T-STA-010 | Duplicate/out-of-order observations arrive across restart. | Idempotency holds and terminal events are not redelivered. | FR-STA-009, FR-DATA-001 |
| T-STA-011 | Main completes while a child remains active. | Main becomes unknown/conflicted, not finished. | §7.3 |
| T-STA-012 | Child stalls while parent is active. | Parent state stays active; child count/badge changes. | §7.3, FR-UI-001 |
| T-STA-013 | Server restarts with an expired wall deadline. | Fresh reconciliation is required before alert/termination. | FR-DATA-001, FR-KILL-002 |
| T-STA-014 | A restarted process receives fresh evidence whose monotonic value is below the prior process's persisted value. | The durable restart boundary resets ordering; fresh evidence applies and clears reconciliation. | FR-DATA-001, FR-STA-009 |

### 5.5 Termination safety

| ID | Scenario | Expected result | Requirements |
|---|---|---|---|
| T-KILL-001 | Any main session is passed to termination entry points. | Type/API rejects it; no signal record exists. | FR-KILL-001 |
| T-KILL-002 | Each termination precondition is removed one at a time. | Saga cannot start in every case. | FR-KILL-002 |
| T-KILL-003 | Child is continuously stalled for 1h and all gates pass. | Parent warning and 10m grace are created; no immediate signal. | FR-KILL-002, FR-KILL-003 |
| T-KILL-004 | Parent extends deadline during grace. | Saga is cancelled and remains cancelled after old deadline. | FR-KILL-003 |
| T-KILL-005 | Child resumes CPU or native progress during grace. | Saga is cancelled. | FR-KILL-003 |
| T-KILL-006 | Runtime supports graceful cancellation and child exits. | No OS signal follows. | FR-KILL-004 |
| T-KILL-007 | Graceful cancel fails; matching child survives. | `SIGTERM` is recorded after re-verification. | FR-KILL-004, FR-KILL-005 |
| T-KILL-008 | Child ignores `SIGTERM`; `SIGKILL` enabled. | `SIGKILL` follows only after configured grace and re-verification. | FR-KILL-004, FR-KILL-006 |
| T-KILL-009 | `SIGKILL` disabled. | Saga stops after `SIGTERM` and reports surviving process. | FR-KILL-006 |
| T-KILL-010 | PID exists but start time/executable/runtime mismatches. | Signal is aborted and diagnostic event is emitted. | FR-KILL-005 |
| T-KILL-011 | PID is reused between graceful cancel and OS signal. | pidfd/start-time validation prevents signaling the replacement. | FR-KILL-005 |
| T-KILL-012 | Adapter, queue, database, or process evidence is degraded. | All affected destructive automation is suspended. | FR-KILL-002, FR-COMP-002 |
| T-KILL-013 | Cancellation completes. | Native runtime files are byte-for-byte unchanged by Watchdog. | FR-KILL-007 |
| T-KILL-014 | Server restarts in every saga stage. | It resumes conservatively, reconciles, and never skips a gate. | FR-DATA-001 |

### 5.6 Persistence and configuration

| ID | Scenario | Expected result | Requirements |
|---|---|---|---|
| T-DATA-001 | Observation, snapshot, event, and outbox write succeeds. | All commit atomically. | FR-DATA-001 |
| T-DATA-002 | Transaction fails at each write boundary. | None of the partial state becomes visible. | FR-DATA-001 |
| T-DATA-003 | Service restarts with undelivered inbox/outbox rows. | Delivery resumes without duplicate event identity. | FR-DATA-001, FR-MCP-004 |
| T-DATA-004 | History exceeds an arbitrary age/size. | No automatic record deletion runs. | FR-DATA-002 |
| T-DATA-005 | Administrative wipe runs in isolated environment. | Watchdog data resets; mounted native files/processes remain unchanged. | FR-DATA-003, FR-DATA-004 |
| T-CFG-001 | Valid TOML reload changes roots/policies. | New immutable config applies atomically. | FR-CFG-001, FR-CFG-002 |
| T-CFG-002 | Invalid TOML/root/threshold reload occurs. | Last valid config stays active; health is actionable. | FR-CFG-002 |
| T-CFG-003 | Environment changes without restart. | Running configuration does not silently change. | FR-CFG-001 |
| T-CFG-004 | Secret appears in config error context. | Error is redacted. | FR-SEC-001 |

### 5.7 MCP, HTTP, authentication, and scoping

| ID | Scenario | Expected result | Requirements |
|---|---|---|---|
| T-MCP-001 | Missing, malformed, oversized, or wrong Bearer credential. | MCP fails closed without credential reflection. | FR-MCP-001, FR-SEC-001 |
| T-MCP-002 | Transport binds to one main session and queries a descendant. | Request succeeds. | FR-MCP-002 |
| T-MCP-003 | Bound transport queries unrelated session tree. | Request is rejected. | FR-MCP-002 |
| T-MCP-004 | Every proposed MCP tool receives valid/invalid bounds. | Schemas, idempotency, state/provenance responses behave as specified. | FR-MCP-003 |
| T-MCP-005 | Parent disconnects before a child event and reconnects with cursor. | Undelivered event is returned. | FR-MCP-004 |
| T-MCP-006 | Alert event is read by parent. | PID identity, CPU deltas, timestamps, conflicts, correlation, operation, and suggestions exist. | FR-MCP-005 |
| T-MCP-007 | Push is unsupported or fails. | Durable inbox remains correct. | FR-MCP-006 |
| T-MCP-008 | Adapter version drifts. | Tool response includes actionable `UPGRADE` warning. | FR-MCP-007 |
| T-HTTP-001 | Missing/wrong/oversized Basic credential requests UI/API. | Browser challenge/failure contains no session metadata. | FR-UI-008, FR-SEC-001 |
| T-HTTP-002 | API attempts a state mutation. | No route exists or method is rejected. | FR-API-001 |
| T-HTTP-003 | Native title contains HTML/script. | Rendered page escapes it; CSP blocks execution. | FR-SEC-003 |
| T-HTTP-004 | SSE client lags broadcast capacity. | It receives `resync_required`, reconnects, and converges. | FR-UI-007 |
| T-HTTP-005 | Runtime warning and state coexist. | JSON distinguishes state from warning badge. | FR-API-003 |

### 5.8 Dashboard and accessibility

| ID | Scenario | Expected result | Requirements |
|---|---|---|---|
| T-UI-001 | Dashboard first loads with active and completed sessions. | Only active main-session cards show by default. | FR-UI-001 |
| T-UI-002 | Waiting/stalled, idle, and other sessions coexist. | Attention order is waiting/stalled, idle, then others. | FR-UI-002 |
| T-UI-003 | Directory sort is selected. | Cards sort case-insensitively by startup directory. | FR-UI-003 |
| T-UI-004 | GitHub enrichment succeeds/fails. | Linked PR appears on success; branch remains on failure. | FR-UI-004, FR-API-002 |
| T-UI-005 | Operator inspects every card/control. | No expansion, mutation, acknowledgement, or kill action exists. | FR-UI-005 |
| T-UI-006 | Page renders at 360px width and 200% zoom. | No primary-content horizontal scroll or lost state/count text. | FR-UI-006 |
| T-UI-007 | SSE disconnects. | Last snapshot stays visible with stale/reconnecting status; no polling occurs. | FR-UI-007 |
| T-UI-008 | Keyboard-only and screen-reader smoke. | Logical focus, visible focus, labels, headings, live status, and non-color state cues pass. | UX §13 |
| T-UI-009 | Light/dark and reduced-motion preferences vary. | Content remains legible and motion preference is honored. | UX §13 |
| T-UI-010 | Main has children in several states. | Text counts match snapshot; child cards do not appear. | FR-UI-001, FR-UI-004 |
| T-UI-011 | Claude and Codex adapters are degraded simultaneously. | Each page warning includes its human-readable runtime label and remains accessible. | FR-EVD-011, FR-UI-006 |

### 5.9 Notifications

| ID | Scenario | Expected result | Requirements |
|---|---|---|---|
| T-NOT-001 | Main stalls, completes, or waits for user. | Enabled human channels receive concise event. | FR-NOT-001, FR-NOT-002 |
| T-NOT-002 | Child-only transition does not affect main state. | No human webhook; parent MCP/count still updates. | FR-NOT-001 |
| T-NOT-003 | Webhook returns error, hangs, or oversized response. | One bounded attempt is recorded; no retry queue entry. | FR-NOT-003 |
| T-NOT-004 | Human event is serialized. | Only issue, main title, and startup directory are present. | FR-UI-009 |
| T-NOT-005 | Alert recovers after reminders. | Future reminders stop and recovery event persists. | FR-NOT-004 |

### 5.10 Operations, security, and compatibility

| ID | Scenario | Expected result | Requirements |
|---|---|---|---|
| T-OPS-001 | Inspect rendered Compose config. | App port unpublished; exact read-only mounts; no `/`, whole home, or Docker socket. | FR-OPS-001, FR-OPS-002 |
| T-OPS-002 | Run container with host PID namespace and configured host UID. | It reads helper process evidence and can signal only the verified same-user helper. | FR-OPS-001, FR-KILL-005 |
| T-OPS-003 | Inspect container security context. | Non-root, capabilities dropped, no-new-privileges, read-only root filesystem. | FR-OPS-001 |
| T-OPS-004 | One adapter crashes or rejects schema. | Its health/sessions degrade; other runtime and server remain available. | FR-OPS-005, FR-COMP-002 |
| T-OPS-005 | Critical database/reducer/process/auth component fails. | Readiness fails and termination is globally suspended. | FR-OPS-005 |
| T-OPS-006 | Generate operational errors, transitions, and repeated identical correlation outcomes. | `tracing` output is structured, bounded, and secret/transcript free; an unchanged correlation outcome logs once and logs again only after it changes. | FR-OPS-006 |
| T-OPS-007 | Query deployment for metrics endpoint/exporter. | None is required or exposed in v1. | FR-OPS-007 |
| T-OPS-008 | Run the target synthetic population. | 50 mains/500 total agents converge with bounded resources and no transcript rescans. | FR-OPS-003 |
| T-SEC-001 | Paths contain `..`, symlink escape, race replacement, or non-UTF-8 components. | Capability-root access prevents escape; errors remain bounded. | FR-SEC-002 |
| T-SEC-002 | Transcript/MCP strings contain markup, shell syntax, control chars, or huge fields. | Input is bounded/escaped and never executed. | FR-SEC-003 |
| T-COMP-001 | Supported runtime emits recognized current schema. | Adapter healthy with documented tested version. | FR-COMP-001 |
| T-COMP-002 | One required native field changes/removes. | Affected sessions get `UPGRADE`; best effort and other adapters continue. | FR-COMP-002 |
| T-COMP-003 | Build workspace on macOS target in CI where available. | It compiles with unsupported process/watchdog operation clearly gated. | Product summary |
| T-COMP-004 | Run the explicitly enabled isolated live-runtime matrix. | Current supported child types are observed without reading real user data. | FR-COMP-003 |
| T-COMP-005 | Review implementation PR evidence for pure logic. | Each reducer/timer/correlation/safety change shows a failing synthetic typed-event test before implementation. | FR-COMP-004 |
| T-COMP-006 | Run the required pre-commit/release quality commands. | Formatter, tests, Clippy with warnings denied, and applicable security checks pass or are explicitly reported. | FR-COMP-005 |

## 6. Load and resource tests

### T-LOAD-001 — Target population

Run 50 main sessions and 450 children with representative adapters, timers,
process samples, SSE clients, and events. Assert bounded queues, no task explosion,
responsive health/UI, and correct state convergence.

### T-LOAD-002 — Huge transcript append

Use sparse multi-gigabyte files with saved cursors. Append bounded records at a
representative burst rate. Assert bytes read scale with appends rather than file
size and resident memory remains within the release budget established by the
implementation spike.

### T-LOAD-003 — Filesystem storm

Generate a worktree event burst and forced watcher overflow. Assert coalescing,
bounded reconciliation, visible degradation, and preservation of terminal/safety
observations.

### T-LOAD-004 — Slow clients and webhooks

Attach lagging SSE/MCP clients and hanging webhook endpoints. Assert they do not
block ingestion/reduction, SSE resynchronizes, durable inbox remains complete,
and webhooks time out once.

### T-LOAD-005 — Restart soak

Repeatedly stop/restart during observations, outbox delivery, deadline expiry,
and every termination-saga stage. Assert database integrity, idempotency, and no
signal without fresh reconciliation.

The Phase 0 baseline establishes these initial release gates:

- 4,096 queued observations and bounded producer admission;
- synthetic 250,000-observation convergence under 2 seconds wall time and 2
  CPU-seconds, with p99 queue-to-reducer latency under 10 ms;
- maximum RSS at 50 mains/500 total agents of 256 MiB;
- no-change steady-state CPU below 10% of one core over 10 minutes, excluding
  explicitly configured reconciliation scans;
- representative burst convergence p99 under 250 ms while health and UI remain
  responsive.

The measured bare-loop baseline and rebaseline policy are recorded in
`docs/spikes/capacity.md`. Release evidence must use the production service, not
substitute the spike binary.

## 7. Live-runtime compatibility matrix

These tests are ignored/skipped unless their explicit opt-in variable and a
dedicated temporary root are present. They must never inspect existing user
sessions.

| ID | Runtime exercise | Pass condition |
|---|---|---|
| T-LIVE-CLAUDE-001 | Start isolated Claude main, spawn subagent, wait, complete/fail. | Exact IDs/hierarchy and lifecycle normalize correctly. |
| T-LIVE-CLAUDE-002 | Exercise team member/task where current runtime supports it. | Team child and update delivery are observed without modifying team files. |
| T-LIVE-CODEX-001 | Start isolated Codex thread and native subagent. | Thread/spawn relation, process, and terminal state are observed. |
| T-LIVE-CODEX-002 | Run a long `cargo test`-like helper under Codex. | CPU/process evidence prevents false stall during transcript silence. |
| T-LIVE-COMP-001 | Launch isolated Companion job through supported interface. | Workspace/job/session/PID/phase and terminal result reconcile. |
| T-LIVE-COMP-002 | Exercise non-atomic detail/summary update ordering. | No false terminal or recovery transition occurs. |

The release report records exact runtime versions, host kernel, Docker/Compose,
and whether each live test ran. Unsupported versions do not block startup; their
behavior is covered by compatibility degradation tests.

## 8. Verification commands

The implementation pipeline must include, adjusted only when the workspace makes
a command inapplicable:

```text
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo deny check advisories bans licenses sources
docker compose config --quiet
```

Targeted tests run during TDD before broad workspace checks. Compose, load, live,
and browser accessibility commands are documented when their harnesses are added.

## 9. Release-blocking invariants

V1 cannot ship if any of these is unproven:

1. a main session is impossible to select for automated termination;
2. PID reuse/executable mismatch prevents every OS signal;
3. waiting-for-user and source-conflicted sessions never enter termination;
4. unchanged CPU counters alone never cause a stall;
5. active long operations remain active through corroborating process evidence;
6. transcript ingestion remains incremental and bounded;
7. an adapter failure cannot corrupt another adapter's sessions;
8. durable parent events survive restart and disconnection;
9. mounted paths and untrusted native data cannot escape configured roots;
10. the approved dashboard remains usable at the mobile acceptance viewport.

## 10. Requirement traceability rule

Every implementation PR must cite the requirement and test IDs it completes.
Before v1 release, an automated or reviewed matrix must show every `FR-*` entry
in `REQUIREMENTS.md` covered by at least one test above. Section-level state and
UX rules without `FR-*` IDs must also appear in the matrix. Missing traceability
is a release blocker, not documentation cleanup.
