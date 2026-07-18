# Agent Watchdog Architecture

Status: proposed for implementation

Date: 2026-07-17

Inputs: [REQUIREMENTS.md](REQUIREMENTS.md), [UX_SPECIFICATION.md](UX_SPECIFICATION.md)

## 1. Architectural objective

Agent Watchdog must answer a deceptively difficult question: is a delegated
agent still making progress, or has its parent stopped receiving updates because
something failed?

No single signal is reliable enough. Transcripts can be silent during builds,
state files can be partially written, PIDs can be reused, files can be shared,
and runtime formats can drift. The architecture therefore treats every input as
an attributed observation, preserves provenance, and derives state through a
deterministic reducer. Destructive action is a separate, stricter decision path.

The design optimizes for:

- early, evidence-rich investigation;
- exact session hierarchy whenever the runtime exposes it;
- bounded work at 50 main sessions and 500 total agents;
- continued service when one runtime adapter degrades;
- safety invariants that cannot be bypassed by adapter output;
- replaceable adapters for evolving agent runtimes.

## 2. Context and trust boundaries

```text
                         trusted LAN / VPN
                                  │
                                  ▼
                        ┌──────────────────┐
                        │     Traefik      │
                        │ IP allowlist/TLS │
                        └───────┬──────────┘
                                │ route-specific auth
                  ┌─────────────┴─────────────┐
                  ▼                           ▼
          Basic Auth UI/API             Bearer MCP
                  │                           │
                  └─────────────┬─────────────┘
                                ▼
                    ┌───────────────────────┐
                    │ agent-watchdog-server │
                    └──┬──────┬──────┬─────┘
                       │      │      │
              read-only│      │      │verified cancellation/signals
                       ▼      ▼      ▼
             runtime state  /proc  runtime APIs / host processes
             transcripts    events
                       │
                       ▼
                configured roots only
```

The supported environment is a single trusted operator on one Linux host. The
shared Bearer token and caller-supplied session identity prevent accidental
cross-session access; they are not multi-tenant authentication. Session IDs are
high-entropy and each MCP transport is bound to one main-session scope after
registration. A holder of the shared server token remains trusted.

Transcript content, filesystem paths, native JSON, Git metadata, webhook
responses, and all MCP arguments are untrusted input.

## 3. Chosen implementation stack

The project is a current-stable Rust workspace. It reuses MemCan's proven
general-purpose choices where they fit, but has no source or workspace dependency
on MemCan.

| Concern | Choice | Rationale |
|---|---|---|
| Async runtime | Tokio | Matches Axum/rmcp; timers, signals, bounded channels, and async I/O. |
| HTTP | Axum + Tower | Same family as MemCan; typed routing, middleware, SSE, graceful shutdown. |
| MCP | `rmcp`, Streamable HTTP server | Current Rust MCP stack already exercised by MemCan. |
| HTML | Maud + small static JS/CSS | The approved dashboard has one read-only screen; a SPA toolchain would add little value. |
| Persistence | SQLite WAL through SQLx | Transactional reducer/outbox, migrations, async pooling, disposable local database. |
| Filesystem events | `notify` with Linux inotify backend | Event-driven Linux behavior while keeping unsupported macOS builds possible. |
| Serialization/config | Serde + TOML | Typed runtime records and operator configuration. |
| Telemetry | `tracing` + `tracing-subscriber` | Structured, redacted logs consistent with MemCan. |
| HTTP client | Reqwest | Webhooks and optional GitHub API enrichment with explicit timeouts. |
| Security primitives | `subtle`, `secrecy`, `zeroize` where applicable | Constant-time credential comparison and reduced accidental secret exposure. |
| Linux process control | `rustix` plus a small `/proc` reader | Raw CPU counters, PID start time, pidfd, safe signals, and no shell parsing. |

Exact dependency versions are selected and locked at implementation start after
checking current releases and security advisories. Default features are disabled
where practical; each dependency must justify its enabled features.

## 4. Workspace and component boundaries

```text
agent-watchdog/
├── Cargo.toml
├── crates/
│   ├── watchdog-domain/       pure types, reducer, correlation, timers, gates
│   ├── watchdog-store/        SQLite migrations and transactional repositories
│   ├── watchdog-runtime/      adapter traits and shared ingestion utilities
│   ├── watchdog-claude/       Claude Code discovery and evidence
│   ├── watchdog-codex/        Codex CLI discovery and evidence
│   ├── watchdog-companion/    Codex Companion discovery and evidence
│   ├── watchdog-process/      Linux /proc sampling and verified process control
│   ├── watchdog-server/       orchestration, HTTP, MCP, SSE, notifications, UI
│   └── watchdog-testkit/      builders, fake time, synthetic adapters, fixtures
├── migrations/
├── web/                       static CSS/JS copied into the binary/image
├── config/
├── docker/
├── compose.yaml
└── docs/
```

The domain crate contains no filesystem, network, database, wall-clock, or OS
calls. Runtime crates emit observations and capabilities; they never mutate
current state directly and cannot send signals. The process crate exposes facts
and verified operations but does not decide when to terminate. The server is the
composition root.

This separation lets the critical state and termination logic run under fake
time with synthetic typed events, while live-runtime tests remain explicitly
enabled integration tests.

## 5. Core domain model

### 5.1 Identities

```rust
SessionId        // watchdog UUID, stable within retained history
NativeSessionKey // runtime + native identifier
MainSessionId    // newtype; cannot be passed as a ChildSessionId
ChildSessionId   // newtype required by termination APIs
RepositoryId
WorktreeId
ObservationId    // deterministic idempotency key
EventId          // durable ordered event identifier
```

When a runtime supplies a stable native ID, `SessionId` is UUIDv5 over the
runtime and native ID. Heuristic-only discoveries receive UUIDv4 and may later be
merged transactionally into an exact identity. Native IDs are never globally
unique without the runtime namespace.

### 5.2 Sessions and hierarchy

`Session` stores kind (`main` or `child`), runtime, native ID, parent/root IDs,
title, startup directory, repository/worktree, PID identity, native version,
capabilities, first/last seen times, and compatibility warning.

`SessionRelation` represents exact or inferred parent/child and main/worktree
links. It stores provenance, confidence, evidence, and validity interval. The
dashboard consumes only the selected active hierarchy; diagnostics retain
rejected candidates.

### 5.3 Observations

All adapters convert native data into a common envelope:

```text
ObservationEnvelope
├── observation_id (stable idempotency key)
├── adapter and adapter version
├── native source/version/location fingerprint
├── observed_at_wall and observed_at_monotonic
├── subject candidate identifiers
├── trust: authoritative | corroborating | heuristic | uncertain
├── payload: typed observation enum
└── bounded evidence metadata (never a full transcript)
```

Typed payloads include discovery, hierarchy, native state, transcript activity,
filesystem activity, process sample, tool/operation activity, MCP progress,
deadline change, completion/failure, disappearance, compatibility warning, and
source conflict.

### 5.4 Derived state

`SessionSnapshot` is the reducer output: normalized state, compact state, last
trusted transition, last activity by signal class, active operation, deadline,
warning badges, conflict set, child counts, and revision.

The reducer accepts one observation at a time and returns a new snapshot plus
zero or more domain events. It is deterministic for a given prior snapshot,
observation order, policy, and clock input.

## 6. Data flow and concurrency

```text
runtime adapters ─┐
inotify watchers ─┤
process sampler  ─┤  bounded Observation channel
MCP reports      ─┘              │
                                  ▼
                         identity/correlation
                                  │
                                  ▼
                        deterministic reducer
                                  │ one SQLite transaction
                    ┌─────────────┼──────────────┐
                    ▼             ▼              ▼
              observation     snapshot       outbox events
                  ledger       revision            │
                                                   ▼
                                      MCP inbox / SSE / webhooks
```

Adapters run independently under supervised Tokio tasks. Each has a bounded
input/output queue and health record. A panic or repeated error degrades only
that adapter and restarts it with backoff.

A coordinator serializes state transitions per session. Different sessions may
reduce concurrently, but SQLite commits use short transactions. An observation,
snapshot update, transition event, and notification outbox entry commit
atomically. Delivery workers acknowledge outbox records separately.

Backpressure policy is evidence-aware:

- terminal, waiting-user, PID disappearance, source conflict, and termination
  observations are never intentionally dropped;
- repeated activity samples may coalesce to the newest sample per session;
- watcher overflow marks the affected root uncertain and schedules bounded
  reconciliation;
- queue saturation degrades health and suspends destructive automation for the
  affected sessions.

## 7. Runtime adapter contract

Each adapter implements a runtime-neutral interface conceptually equivalent to:

```rust
trait RuntimeAdapter {
    fn kind(&self) -> RuntimeKind;
    fn capabilities(&self) -> AdapterCapabilities;
    async fn discover(&self, roots: &AllowedRoots) -> Result<DiscoveryBatch>;
    async fn reconcile(&self, scope: ReconcileScope) -> Result<Vec<Observation>>;
    async fn graceful_cancel(&self, child: &VerifiedChild) -> CancelResult;
    fn compatibility(&self) -> CompatibilityStatus;
}
```

Adapters do not own watchers. They declare watch targets and parsers to a shared
watch service, which deduplicates directories and routes file events back by
target ID. The interface deliberately matches the future OpenCode model: API/SSE
adapters can emit the same observations without filesystem semantics.

### 7.1 Claude Code

Evidence precedence:

1. official lifecycle hooks when configured;
2. exact team/member/task configuration and session IDs;
3. session/subagent transcripts read incrementally;
4. process ancestry and worktree evidence;
5. timestamp heuristics.

Automatic discovery watches the mounted Claude projects, teams, and tasks.
Optional official lifecycle hooks post to the exact `/hooks/claude` endpoint
through the trusted-source Traefik route and application Bearer authentication.
Hooks are enrichment, not a monitoring prerequisite. Team files are read-only
and never used as a cancellation mechanism.

Hook requests are capped at 64 KiB before parsing. A UUIDv5 of the request bytes
is the stable retry identity; raw bytes and secret-shaped message/prompt fields
are never persisted or logged. Exact session and agent IDs create the hierarchy
and authoritative state. Hook-supplied host paths remain untrusted and are not
stored as metadata; independently capability-validated discovery or MCP may add
paths later.

For Claude Code 2.1.214, automatic project discovery accepts only the observed
`<project>/<session-id>.jsonl` main layout and
`<project>/<session-id>/subagents/agent-<agent-id>.jsonl` child layout. The path
supplies the exact hierarchy and each parsed child record must agree with both
the parent `sessionId` and `agentId`; disagreement becomes a source conflict.
Only transcripts modified within the 24-hour bootstrap window are admitted.
The first encounter reads at most one bounded 2 MiB/128-record prefix for cwd,
branch, and native agent metadata, then durably initializes the activity cursor
at EOF. Later passes never repeat that bootstrap and read at most four bounded
append batches. Existing message/tool bodies are neither replayed nor retained.
Known metadata-only records advance the cursor without inventing activity;
unknown complete records add an actionable `UPGRADE` warning to the affected
session. Optional subagent sidecars contribute only their bounded `agentType`.
Team configs are reconciled before project transcripts. A root-level teammate
transcript is rebound to the one active team member whose native `agentType` and
cwd both match, preventing its independent transcript UUID from creating a
duplicate main-session card. Ambiguous matches remain independent rather than
guessing. Positive bindings are cached under the same 2,048-entry bound; after
a restart, an unresolved teammate cursor performs one bounded prefix recheck.

Task snapshots are joined through the team config's exact native name and a
unique active member owner. Unassigned tasks remain neutral. For each member,
any in-progress task normalizes to running, then pending to starting; when no
active task remains, the newest terminal task supplies completed, failed, or
cancelled. The task file's nanosecond modification time and aggregate count form
the idempotent native revision, and already stored revisions are not replayed.

### 7.2 Native Codex CLI

Evidence precedence:

1. app-server or official hook events when available;
2. exact thread metadata, parent IDs, and spawn edges from current local state;
3. incremental rollout JSONL records;
4. process ancestry and worktree evidence;
5. timestamp heuristics.

Internal SQLite/JSONL schemas are guarded by runtime version and field-level
parsing. The adapter opens native SQLite read-only and tolerates absent WAL/SHM;
failure falls back to JSONL/process evidence and adds `UPGRADE` when the format is
unrecognized. It never migrates, locks for writing, or repairs Codex state.

SQLite bootstrap selects only unarchived threads with native recency in the
preceding 24 hours, under a 1,000-row cap, then preserves exact spawn edges.
This is deliberately a bounded false-positive-tolerant heuristic, not a claim
that retained Codex threads are running. Official hook/app-server evidence,
MCP registration, and trustworthy process correlation may add or retain an
older live session; the bootstrap window prevents all retained history from
flooding the default dashboard.

### 7.3 Codex Companion

The adapter observes per-workspace state, job detail, logs, launcher/session IDs,
PIDs, phase, and terminal errors. Summary and job files are independently parsed
because they are not an atomic pair. Missing records after native pruning are not
treated as failure without corroboration.

For `jobs/<safe-job-id>.log`, Watchdog persists only device, inode, and byte
offset. The first sighting starts at EOF; a later increase on the same file
identity emits one content-free progress observation keyed by the new offset.
Log contents are never read or retained. Replacement, truncation, and native
pruning establish a neutral new baseline rather than inventing progress or
failure.

### 7.4 OpenCode boundary

No OpenCode crate ships in v1. The shared model already represents API-discovered
parent/child sessions, runtime state, SSE events, abort capability, and version
warnings. Adding it should require one adapter crate and live compatibility tests,
not a schema or reducer redesign.

## 8. Filesystem ingestion

The watch service uses the `notify` Linux inotify backend. It watches only
capability roots constructed from concrete read-only mounts and configured
allowlisted prefixes. Existing directories receive non-recursive watches under
the global cap. A directory create/remove/rename event triggers one bounded
registry rebuild so newly created session directories become observable;
ordinary file appends only schedule targeted reconciliation.

Each file cursor records device/inode identity, byte offset, last complete record
boundary, parser version, and last observation ID. Appends read from the saved
offset with a byte limit. An incomplete trailing record remains buffered only up
to a strict cap. Truncation, replacement, rotation, and inotify overflow schedule
targeted reconciliation.

Directory enumeration has depth, entry-count, byte, and time budgets. A budget
breach creates a compatibility/health warning rather than expanding the scan.
The enumerator bounds regular files and directories together before sorting and
never follows symlink entries, so a large history cannot cause an unbounded
directory allocation.
Periodic low-frequency reconciliation catches missed events; it is scoped to
known runtime roots and active sessions, never a full home-directory crawl.

Worktree changes refresh a child only when ownership is unambiguous. With one
child on a worktree, any in-root change is attributable, including changes in
nested directories. The most-specific registered worktree prefix wins. With
multiple children, the system requires process/file-descriptor or
native-operation evidence. If it cannot attribute the change, no child's clock
is refreshed. One coalesced inotify batch emits at most one progress observation
per child and never exposes the changed path in logs or stored evidence.

## 9. Process evidence and CPU activity

The Linux process sampler uses host PID namespace visibility and records a
`ProcessIdentity` of PID, `/proc/<pid>/stat` start time, executable identity, and
runtime kind. PID alone is never an identity.

For each correlated process and current descendant, a sample includes:

- own user CPU time (`utime`);
- own system CPU time (`stime`);
- waited-for-children user CPU time (`cutime`);
- waited-for-children system CPU time (`cstime`);
- process state and parent PID;
- available I/O counters;
- executable and command fingerprint, redacted to avoid secrets.

A positive delta in any trustworthy cumulative CPU counter is activity. Growth
across all four counter classes is strong corroboration. Unchanged counters are
neutral: they never independently prove a stall. Descendants are sampled
directly, so active child work is visible before it is included in a parent's
waited-for-child counters.

Counter decrease, PID start-time change, missing process data, or process-tree
ambiguity invalidates the comparison and creates uncertainty; it does not create
synthetic inactivity.

The server samples verified identities every five seconds. Each cycle captures
the bounded `/proc` process table and parent relationships once, then derives
every independently verified session tree from that shared capture; it does not
walk `/proc` once per agent. Root executable identity and per-process I/O remain
freshly checked, and one root failure is isolated from the others. The first
sample is a baseline. Later trustworthy CPU, I/O, or new-descendant growth emits a
corroborating progress observation; all-four-counter growth records the
actionable summary “All four CPU times grew versus the previous process
snapshot.” Neutral samples emit nothing. Any uncertainty degrades process
health and cannot authorize destructive action.

## 10. Correlation

Correlation produces candidates, not silent guesses. The score is lexicographic:

1. exact native parent/session relation;
2. MCP-registered relation;
3. verified process ancestry;
4. unique startup directory/worktree plus compatible time window;
5. transcript reference or runtime-specific link;
6. weaker timestamp/path heuristics.

An exact relation wins unless another authoritative source conflicts. Heuristic
correlation requires one unique best candidate above the runtime threshold and a
minimum gap to the runner-up. Otherwise the child remains unassigned or unknown.
Confidence and all evidence remain in logs/database, not the default UI.

Re-correlation is transactional. It updates hierarchy and child counts without
replaying notifications already delivered under the same native observation.

## 11. State, deadlines, and alerts

Runtime-native states first normalize to the detailed state model, then project
to compact UI states. Activity is a separate signal from state.

The stall engine uses monotonic elapsed time during a boot. Persisted wall-clock
deadlines support restart recovery, but a restart never immediately triggers a
kill: adapters must reconcile and produce fresh trustworthy evidence first.
The server evaluates retained main and child sessions every five seconds. A
pre-threshold evaluation that changes nothing is not persisted, so the scheduler
does not grow the observation ledger in proportion to polling frequency.

Fallback policy:

```text
last trustworthy progress
        │
        ├─ < 5m ───────── running/idle according to native state
        ├─ 5m ─────────── suspect internally; corroborate
        ├─ 15m ────────── stalled + parent alert
        ├─ every 5m ───── unresolved reminder
        ├─ 60m stalled ── termination warning if every gate passes
        └─ +10m grace ─── graceful cancellation sequence
```

Waiting for user pauses stall and termination clocks. Known active operations
remain active while CPU, I/O, output, child-process, or native operation evidence
progresses. A parent-provided deadline overrides the fallback until changed or
expired.

Immediately before an alert, the coordinator requests cheap fresh process and
adapter checks. The resulting event includes the evidence snapshot used for the
decision.

## 12. Termination safety architecture

Termination is implemented as an auditable saga, not a timer callback. Only a
`ChildSessionId` can construct a `TerminationCandidate`; main IDs fail at the type
boundary.

The pure `TerminationGate` requires all conditions simultaneously:

- continuously stalled for at least one hour;
- not waiting for user;
- no active operation or recent trustworthy activity;
- no deadline extension or intentional-waiting override;
- no source conflict, queue overflow, adapter degradation, or uncertain identity;
- child classification and parent relation are trustworthy;
- a fresh process identity matches runtime, PID start time, and executable.

Passing the gate creates a durable warning event and a ten-minute grace deadline.
Any contrary observation cancels the saga. After grace:

1. call the runtime's supported graceful cancellation API;
2. reconcile and wait the configured grace interval;
3. open or reuse a Linux pidfd after re-verification and send `SIGTERM`;
4. reconcile and wait;
5. if enabled, re-verify and send `SIGKILL` through pidfd;
6. record the outcome at every stage.

A five-second Linux worker loads only stored child sessions, batches selected
relation evidence by root, samples a fresh PID/start-time/executable identity,
and derives health gates from store, queue, owning adapter, and process-sampler
status. Missing evidence suspends the saga without mutation. The worker and
normal reducer lanes share one event allocator, preventing event-ID collisions,
and each pass reads the current reloadable automation, grace, `SIGKILL`, and
post-stall timing policy. Main sessions are excluded by the store query and by
the `TerminationCandidate` type boundary.

If pidfd is unavailable, the fallback verifies PID start time and executable
immediately before each signal. Signals are never constructed through a shell.
The server must run as the same numeric host UID as monitored agents; it does not
run privileged and receives no Docker socket.

Runtime state records host-native working directories, while the supported
container mounts each allowlisted host prefix at a narrow container path. An
explicit ordered `native_worktree_roots` to `allowed_worktree_roots` projection
validates each native path against the canonical mounted target without
requiring a broad host mount. Traversal, missing targets, and symlink escapes
discard path evidence and degrade the affected adapter. Human-facing metadata
retains the native host path; filesystem access always uses the projected
container path.

The same positional projection applies independently to Claude, Codex, and
Companion native-state roots. Codex state-database rollout paths are projected
to a held directory capability and tailed with a persisted device/inode and
complete-record offset. First discovery starts at EOF; each pass is capped by
bytes, record size, record count, and batch count. Partial trailing records are
re-read from the last complete boundary after restart, while replacement or
truncation moves to a newly established EOF boundary and degrades adapter health.
Incompatible complete records advance the cursor, degrade adapter health, and
place an `UPGRADE` warning on the exactly associated session.
Only typed lifecycle/activity evidence is retained; transcript bodies are not.

## 13. Persistence

SQLite uses WAL, foreign keys, a busy timeout, and migrations embedded in the
binary. Core tables:

| Table | Purpose |
|---|---|
| `sessions` | Stable identity, hierarchy root, runtime, paths, title, current metadata. |
| `session_relations` | Candidate/selected hierarchy with provenance and validity. |
| `observations` | Bounded normalized evidence and idempotency key. |
| `session_snapshots` | Current reducer output and monotonically increasing revision. |
| `state_transitions` | Durable state/event history. |
| `activity_samples` | Recent signal timestamps and bounded process deltas. |
| `file_cursors` | Incremental parser identity and offsets. |
| `deadlines` | Parent overrides, pause state, expiry, provenance. |
| `termination_sagas` | Warning/grace/signal stage and safety snapshot. |
| `outbox` | Undelivered MCP/SSE/webhook/browser event fan-out. |
| `inbox_offsets` | Parent MCP durable event cursor. |
| `adapter_health` | Version, compatibility, last success/error, degraded scope. |
| `notification_attempts` | One-shot human notification audit. |

Full transcript bodies are never stored. Observation evidence and native-derived
summaries have per-field and per-record byte caps.

Manual wipe is an explicit administrative CLI operation that stops ingestion,
deletes only the Watchdog database and cached summaries/notifications, recreates
the schema, and restarts discovery. It never follows paths into mounted runtime
state.

Persisted monotonic values are never compared across server processes. Before
startup reconciliation, the server durably marks every retained session as
restart-required, clears its process-local monotonic observation cursor, and
emits the reconciliation-required event. Fresh trusted native evidence clears
that gate; deadlines and termination remain conservative until then.

## 14. MCP surface

The MCP endpoint uses Streamable HTTP and shared Bearer authentication. At
initialization/registration, a transport is bound to one main-session scope.
Every query/mutation resolves the supplied session ID within that root tree.

The rmcp transport ID is surfaced by a Watchdog `SessionManager` wrapper as a
typed request extension. The application binds that opaque ID exactly once;
rebinding and cross-tree targets fail server-side. rmcp's SSE replay cursor is
transport-only. Parent event delivery and acknowledgement use a separate
durable SQLite inbox cursor, so transport loss or resume failure cannot discard
agent-visible events. This boundary is executable in the Phase 0 scoping test.

Proposed tools:

| Tool | Purpose |
|---|---|
| `register_session` | Enrich an autodiscovered main/child or declare a new in-scope session. |
| `register_delegation` | Record parent/child relation and optional expected check-in. |
| `report_progress` | Add event-driven progress with bounded summary and optional operation. |
| `report_waiting` | Mark waiting for user/tool/agent or intentionally waiting. |
| `complete_session` | Report terminal outcome; native evidence still reconciles it. |
| `update_deadline` | Extend, shorten, pause, resume, or clear a check-in deadline. |
| `get_session` | Read normalized state and evidence for one in-scope session. |
| `list_sessions` | List sessions in the caller's tree with filters. |
| `get_session_tree` | Read the hierarchy and aggregate child states. |
| `list_events` | Durable inbox cursor/long-poll style retrieval. |
| `get_watchdog_health` | Read relevant adapter and compatibility warnings. |

All mutating tools are idempotent through a caller event key. Responses include
server time, snapshot revision, normalized state, warning field, and evidence
provenance. Tool text is bounded and treated as untrusted.

MCP resource subscription or server notification may hint that events are
available, but it is never the delivery authority. Durable `list_events` remains
the baseline because clients are not guaranteed to turn a protocol notification
into model-visible input.

## 15. HTTP and web UI

Routes are split by exposure and authentication:

```text
GET  /health/live             minimal unauthenticated container liveness
GET  /health                  authenticated detailed component health
GET  /ui                      Basic Auth dashboard
GET  /api/v1/sessions         Basic Auth read-only JSON snapshot
GET  /api/v1/events           Basic Auth SSE
POST /mcp                     Bearer-authenticated MCP transport
```

The server renders the approved card markup with Maud. A small dependency-free
script opens SSE, applies revisioned events, filters completed sessions, and
sorts cards. Initial HTML contains the snapshot, so the page remains useful if
JavaScript or SSE fails. If an SSE client falls behind, the server sends a
`resync_required` event; the client reconnects for a fresh snapshot. There is no
polling fallback.

Basic/Bearer values are length bounded and compared in constant time. Responses
set CSP, `X-Content-Type-Options`, frame denial, referrer policy, and no-store for
session data. Native titles and paths are escaped by Maud and never inserted with
raw HTML.

## 16. Notifications and delivery

Domain events fan out through event-specific routes in the same transaction as
the observation and snapshot update:

- parent MCP inbox: durable until read/acknowledged;
- SSE/web notification center: live best effort plus snapshot recovery;
- browser notification: client best effort after user permission;
- Home Assistant webhook: one attempt;
- generic webhook: one attempt.

All meaningful events go to the parent inbox and SSE projection. Human routes
are added only for main-session stall/failure alerts, unresolved reminders,
completion, and waiting-for-user transitions, plus termination-saga warnings.
Child-only state changes therefore update the parent's diagnostics and browser
child counts without sending a human webhook. Disabled webhook destinations are
terminally acknowledged, and an existing per-channel attempt suppresses replay
after restart.

Human payloads contain only issue, main-session title, and startup directory.
Agent payloads contain PID identity, trusted state, signal timestamps, CPU deltas,
active operation, conflicts, correlation basis, and suggested diagnostics.

Webhook URLs are configured secrets, restricted to `http`/`https`, length
bounded, and called with connection/total timeouts and response-size limits.
V1 assumes operator-controlled destinations; private-network webhooks are valid
for Home Assistant, so generic SSRF blocking would conflict with the product.

## 17. Configuration and reload

Environment variables contain bootstrap settings and secrets: database path,
listen address, Basic Auth credentials, Bearer token, optional GitHub token, and
webhook secrets. Mounted TOML contains allowlisted roots, runtime locations,
exclusions, thresholds, signal grace periods, adapter enablement, GitHub policy,
and `SIGKILL` opt-out.

Configuration parsing returns an immutable validated snapshot. `SIGHUP` and an
administrative CLI reload build a complete candidate, validate/canonicalize all
roots and policies, then atomically swap it. Invalid reloads retain the old
snapshot and produce health/log warnings. Environment changes require restart.

## 18. Compose deployment

The supported Compose project contains:

- `agent-watchdog`: read-only root filesystem, unprivileged configured UID/GID,
  host PID namespace, dropped capabilities, `no-new-privileges`, tmpfs scratch,
  persistent SQLite volume, concrete read-only runtime/worktree mounts, and no
  published port;
- `traefik`: the only published listener, trusted-network IP allowlist, route
  separation, Basic Auth middleware for UI/API, and Bearer forwarding to MCP;
- health checks and explicit dependency/restart policy.

The application container must share the monitored user's numeric UID/GID so
same-user signals are permitted. Host PID namespace is required for process
evidence; the image remains non-privileged. `/`, the whole home directory, and
the Docker socket are never mounted.

Linux Docker Compose is the only supported runtime. Platform-specific modules
are `cfg(target_os = "linux")`; non-Linux builds expose unsupported stubs so the
workspace can compile on macOS without claiming operational support.

## 19. Health, compatibility, and failure containment

Each subsystem reports `healthy`, `degraded`, or `failed`, last success, bounded
error, version, and affected scope. Overall readiness is degraded—not failed—when
one runtime adapter breaks while others can monitor safely.

Unknown native fields are ignored. Missing required fields, schema failures, or
unsupported runtime behavior produce an `UPGRADE` warning on affected sessions
and suspend destructive automation for them. Best-effort evidence continues.

The process sampler, database, reducer, and authorization layer are critical.
Failure in any one makes readiness fail and all termination automation stop.

Logs use structured fields and stable event names. They exclude auth headers,
tokens, raw transcript content, command-line secrets, and unbounded paths/text.

## 20. GitHub enrichment

`GitHubEnricher` is an optional, cached component. It parses a supported GitHub
remote and branch, then resolves an open PR through the GitHub API when a token is
configured. A separately configured `gh` executable adapter may be used with
literal argument arrays, no shell, strict timeout, and bounded output.

Current Codex thread state supplies bounded `git_origin_url` and `git_branch`
fields directly, so branch fallback appears without running `git` in the
distroless container. Only supported GitHub remotes are retained, canonicalized
to a credential-free HTTPS URL; unsupported or credential-bearing remotes are
discarded while their branch remains available. A background worker checks
main-session repository/branch pairs every 30 seconds; the five-minute cache
prevents repeated API traffic.
Successful lookup stores a locally constructed GitHub PR URL. No match, offline
service, unsupported remote, authorization failure, or schema failure clears a
stale PR while retaining the branch.

Cache keys include repository and branch. Failures preserve the branch and do
not degrade monitoring health beyond the enrichment component.

## 21. Key architectural decisions

| Decision | Chosen direction | Consequence |
|---|---|---|
| State computation | Typed observations + deterministic reducer | More explicit modeling; strong replay/TDD and provenance. |
| Adapter isolation | Per-runtime crates and supervised tasks | Runtime drift does not take down the server. |
| Persistence | SQLite transactional state + outbox | Durable inbox/dedup without external infrastructure. |
| UI | Maud SSR + minimal JS/SSE | Small image and accessible first render; no SPA ecosystem. |
| Process activity | Raw cumulative CPU/process-tree deltas | Correct long-operation evidence; Linux-specific collector behind a trait. |
| Termination | Separate gated saga using child newtypes and pidfd | Extra implementation work justified by preventing main/PID-reuse kills. |
| Filesystem | inotify-driven cursors + bounded reconciliation | Scales to huge transcripts and survives missed events. |
| Runtime files | Read-only, version-guarded evidence | No repair risk; adapters can degrade independently. |
| Push | Durable MCP inbox first | Works across clients; push remains optional optimization. |
| Frontend details | Main-session cards only | Meets the approved attention workflow; child detail deferred. |

## 22. Deferred decisions

- Token accounting schema fields are not reserved until a future requirements
  pass defines native data availability and semantics.
- OpenCode's concrete transport and cancellation implementation waits for its
  adapter milestone.
- Prometheus, automatic retention, backups, multi-host federation, supported
  macOS operation, and writable human controls remain outside v1.

## 23. Architecture acceptance gates

Implementation may begin when:

1. all safety invariants in sections 9, 11, and 12 have corresponding test cases;
2. every requirement maps to an architectural component and planned test;
3. the Compose mount/UID/PID model is proven with a minimal spike;
4. current runtime versions and exact available evidence are recorded;
5. dependency versions pass license/advisory review;
6. the product owner accepts any material deviation discovered by the spikes.

## 24. Research baseline to re-verify after context reset

These are implementation leads observed on 2026-07-17, not stable API
guarantees. Phase 0 must verify them against live files and current primary
documentation before code depends on them.

### 24.1 Versions observed during planning

| Runtime/tool | Observed version |
|---|---:|
| Claude Code | 2.1.214 |
| Native Codex CLI | 0.144.5 |
| Codex Companion | 1.0.6 |
| OpenCode | 1.17.15 (future adapter research only) |

The supported matrix is whichever current versions are verified at
implementation/release time, not automatically the versions in this table.

### 24.2 Predecessor evidence

The neighboring `claudius` repository was `github.com/lklimek/claudius` during
planning. Relevant requirement sources were:

- `../claudius/scripts/agent-watchdog.py` (approximately 2,300 lines);
- `../claudius/tests/test_agent_watchdog.py` (approximately 2,087 lines);
- `../claudius/tmp/codex-monitoring-design.md`.

Treat these only as failure scenarios and evidence-source inventory. Do not port
their scanner, state machine, newest-session selection, tmux spoofing, or
cross-session heuristics as the new design. Known failures are listed in
`REQUIREMENTS.md` §10.

### 24.3 MemCan reference

The neighboring `../memcan` repository was `github.com/lklimek/memcan`. Its live
workspace demonstrated Tokio, Axum 0.8, rmcp 1.1, Maud 0.27, Reqwest 0.13,
`tracing`, and `subtle` in a Rust server behind Traefik, including constant-time
Basic Auth, CSP/no-store headers, and component health patterns.

Re-check current versions and reuse the crates/patterns only where they match this
architecture. Do not add a MemCan workspace dependency or copy its application
structure.

### 24.4 Native discovery leads

- Claude official `SessionStart`, `SubagentStart`, and `SubagentStop` hooks
  exposed session ID, transcript path, cwd/model, and subagent
  ID/type/transcript/final message. Team config/tasks were observed under
  `~/.claude/teams/<team>/config.json` and `~/.claude/tasks/<team>/`; project
  transcripts under `~/.claude/projects/`. Transcripts can be disabled or pruned.
- Current Codex local state used `~/.codex/state_5.sqlite`. Relevant observed
  tables included `threads`, `thread_spawn_edges`, `agent_jobs`, and
  `agent_job_items`; spawn edges used parent thread ID, child thread ID, and
  status. Session JSONL metadata exposed `parent_thread_id` plus subagent source,
  path, and nickname fields. These are internal and require version guards.
- Codex app-server was documented as bidirectional JSON-RPC with thread/turn
  lifecycle, streamed events, and interrupt support. Prefer it and official hooks
  over internal files when they provide equivalent automatic coverage.
- Codex Companion 1.0.6 was inspected at
  `/home/ubuntu/.claude/plugins/cache/openai-codex/codex/1.0.6/`. Its v1 store
  used per-workspace state, `jobs/<id>.json`, logs, launcher PIDs, session IDs,
  phases, terminal errors, and a bounded recent-summary set. Summary/detail
  writes were not atomic as a pair, repeated progress could update only a log,
  terminal records cleared their PID, and old jobs could be pruned.
- OpenCode documentation exposed `/session/status`,
  `/session/<id>/children`, `/event` SSE, abort, and asynchronous prompts.
  Preserve this shape at the adapter boundary, but ship no OpenCode behavior in
  v1.

### 24.5 Durable lessons carried into implementation

- Transcript modification time alone is unreliable during builds; process and
  worktree evidence must corroborate it.
- A worker previously judged stale may resume after a long delay. This reinforces
  the one-hour continuous-stall gate, warning/grace period, fresh evidence, and
  child-only termination invariant.
- Shared-host “newest session” selection and shared-workspace mtimes caused
  cross-session attribution. Exact identity and ambiguity-safe correlation are
  mandatory.
- Native state may be partial, non-atomic, pruned, or version-drifted. Preserve
  last trusted state, expose uncertainty, and keep destructive automation off.
