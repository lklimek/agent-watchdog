# V1 requirement traceability

Status: implementation audit in progress

Date: 2026-07-18

This matrix connects every v1 `FR-*` requirement to its acceptance cases and
the production/test locations that provide evidence. `Automated` means the
listed repository tests pass in the normal Rust suite. `Manual` means the
implementation exists and has recorded Compose/browser inspection evidence.
`Pending` is a planned release gate. `Partial` and `Gap` are implementation work,
not accepted deferrals.

## Discovery and evidence

| Requirement | Acceptance tests | Implementation and test evidence | Status |
|---|---|---|---|
| FR-DIS-001 | T-DIS-001, T-DIS-012 | `discovery.rs`; Claude/Codex/Companion adapter and server discovery tests | Automated |
| FR-DIS-002 | T-DIS-001, T-DIS-011, T-DIS-012 | Runtime adapter crates; `server/tests/discovery.rs`; lifecycle-hook tests | Automated; isolated live matrix pending |
| FR-DIS-003 | T-DIS-009 | Runtime-neutral domain identity, observation, capability, and state types; `domain/tests/model.rs` | Automated |
| FR-DIS-004 | T-DIS-010 | Runtime-namespaced native IDs and role-preserving identities; `domain/tests/identity.rs` | Automated |
| FR-DIS-005 | T-DIS-003, T-DIS-005 | Deterministic correlation priority and retained relation evidence; `domain/tests/correlation.rs` | Automated |
| FR-DIS-006 | T-DIS-003, T-DIS-004, T-DIS-011, T-DIS-012 | Correlation engine plus exact native Claude/Codex/Companion hierarchy parsers | Automated |
| FR-DIS-007 | T-DIS-006, T-DIS-007 | Durable scoped `register_watch_path`, capability projection, prioritized watcher rebuild, and exact child ownership | Automated |
| FR-DIS-008 | T-DIS-008 | Tracked standard path templates, explicit Compose binds, and tested coexistence of standard/additional native-to-mounted TOML mappings | Automated |
| FR-DIS-009 | T-DIS-002 | Startup-directory/worktree metadata, root-scoped stores, dashboard cards, and ambiguity-safe worktree activity | Automated |
| FR-DIS-010 | T-DIS-013, T-DIS-020 | Typed Codex rollout lifecycle transitions and bounded terminal bootstrap | Automated and live-verified |
| FR-DIS-011 | T-DIS-014 | Unique retained-team lead alias; ambiguous matches fail open | Automated and live-verified |
| FR-DIS-012 | T-DIS-015 | Inactive Claude member terminal reconciliation | Automated and live-verified |
| FR-DIS-013 | T-DIS-016 | Explicit Claude-origin Codex child correlation | Automated and live-verified |
| FR-DIS-014 | T-DIS-017 | Recent-only team-config bootstrap | Automated and fresh-volume verified |
| FR-DIS-015 | T-DIS-018 | Shared Claude/Companion wrapper alias registry | Automated and live-verified |
| FR-DIS-016 | T-DIS-019 | Recent terminal Companion bootstrap with tracked-job exception | Automated and fresh-volume verified |
| FR-EVD-001 | T-EVD-001 | Linux `WatchService`, targeted invalidation, server watcher supervisor | Automated |
| FR-EVD-002 | T-EVD-001 | Durable incremental cursor and complete-record reader; huge sparse-file test | Automated |
| FR-EVD-003 | T-EVD-003, T-EVD-004 | File identity/truncation reconciliation, watcher uncertainty, generation-guarded health | Automated |
| FR-EVD-004 | T-DIS-020, T-EVD-002, T-EVD-005 | Partial-record cursor/tail behavior and adapter parse containment | Automated |
| FR-EVD-005 | T-EVD-007 | `FilesystemActivityMonitor`; single-owner activity test | Automated |
| FR-EVD-006 | T-EVD-008, T-EVD-009 | `WorktreeOwners` ambiguity policy and filesystem-activity integration | Automated |
| FR-EVD-007 | T-CPU-001, T-CPU-002, T-CPU-006, T-CPU-008 | Process-tree sampler and server process monitor; synthetic/live-helper CPU and descendant tests | Automated |
| FR-EVD-008 | T-EVD-001, T-EVD-006 | Record, directory depth/entry/path-byte, queue, and wall-time budgets; scan/incremental tests | Automated |
| FR-EVD-009 | T-CPU-009, T-EVD-004, T-EVD-010 | Provenance-aware reducer conflict state, watcher degradation, process-race containment | Automated |
| FR-EVD-010 | T-CPU-001–005, T-CPU-007 | Four `/proc` CPU counters, prior-snapshot delta model, strong/neutral/uncertain tests | Automated |
| FR-EVD-011 | T-UI-011 | Runtime-labelled page degradation warnings | Automated and browser-verified |
| FR-EVD-012 | T-EVD-011, T-EVD-013, T-EVD-014 | Runtime-first watch allocation plus ephemeral uniquely-owned active-child and durable registered exact paths | Automated; live Compose validation pending |
| FR-EVD-013 | T-EVD-012 | Typed watch destinations and lossless coalesced adapter-specific discovery requests | Automated; live Compose validation pending |

## State, termination, and agent API

| Requirement | Acceptance tests | Implementation and test evidence | Status |
|---|---|---|---|
| FR-STA-001 | T-STA-004 | Optional registration/delegation plus durable deadlines in Agent API/MCP | Automated |
| FR-STA-002 | T-STA-004, T-STA-005 | Set/shorten/extend/pause/resume/clear deadline commands and reducer tests | Automated |
| FR-STA-003 | T-STA-006 | Native progress remains authoritative without heartbeat | Automated |
| FR-STA-004 | T-STA-002 | Adapter provenance plus global reducer fallback policy | Automated |
| FR-STA-005 | T-STA-002 | Five-minute suspect and fifteen-minute stall defaults and fake-time tests | Automated |
| FR-STA-006 | T-STA-007 | Authoritative failure and verified process disappearance reduce immediately | Automated |
| FR-STA-007 | T-STA-008 | Process sampling precedes timer evaluation; `AgentDiagnosticView` includes PID, latest CPU delta/provenance, trusted times, operation, conflicts, correlation, and suggested checks | Automated |
| FR-STA-008 | T-STA-009 | Monotonic five-minute reminder reducer/outbox behavior | Automated |
| FR-STA-009 | T-DIS-005, T-STA-010, T-STA-014 | Stable observation IDs, transactional store, durable event cursor, restart boundary | Automated |
| FR-KILL-001 | T-KILL-001 | `ChildSessionId`-only safety/termination entry points and main rejection tests | Automated |
| FR-KILL-002 | T-KILL-002, T-KILL-003, T-KILL-012, T-STA-003, T-STA-013 | Typed safety gates, fresh-health/reconciliation checks, one-hour policy | Automated |
| FR-KILL-003 | T-KILL-003–005 | Durable warning/grace stages and cancellation on extension/progress | Automated |
| FR-KILL-004 | T-KILL-006–008 | Durable graceful→TERM→KILL saga; graceful capability is optional | Automated |
| FR-KILL-005 | T-CPU-007, T-KILL-007, T-KILL-010, T-KILL-011, T-OPS-002 | Fresh PID/start-time/executable checks and pidfd helper integration | Automated |
| FR-KILL-006 | T-KILL-008, T-KILL-009 | Default-enabled configurable KILL stage and opt-out test | Automated |
| FR-KILL-007 | T-KILL-013 | Adapter boundary is read-only; cancellation interface and OS signals are the only mutation capabilities | Automated contract and final security inspection passed |
| FR-MCP-001 | T-MCP-001 | Bearer middleware before rmcp parsing/allocation; auth and transport tests | Automated |
| FR-MCP-002 | T-MCP-002, T-MCP-003 | One immutable main-tree scope per transport and cross-tree rejection | Automated |
| FR-MCP-003 | T-MCP-004 | Twelve bounded runtime-neutral MCP tools and real rmcp schema/behavior tests | Automated |
| FR-MCP-004 | T-DATA-003, T-MCP-005 | Transactional parent inbox and durable cursor across reconnect/restart | Automated |
| FR-MCP-005 | T-MCP-006 | Durable `AgentEventView` includes the explicit bounded FR-STA-007 diagnostic bundle without transcript retrieval | Automated |
| FR-MCP-006 | T-MCP-007 | Durable inbox is authoritative and capability model permits optional push | Automated fallback; no supported push transport in v1 |
| FR-MCP-007 | T-MCP-008 | Actionable `UPGRADE` compatibility warning in snapshots, MCP, API, and UI | Automated |

## UI, notifications, and API

| Requirement | Acceptance tests | Implementation and test evidence | Status |
|---|---|---|---|
| FR-UI-001 | T-STA-012, T-UI-001, T-UI-010 | Active-main default projection and child status aggregation | Automated and browser-verified |
| FR-UI-002 | T-UI-002 | Attention→idle→other deterministic card ordering | Automated |
| FR-UI-003 | T-UI-003 | Case-insensitive startup-directory sort control | Automated |
| FR-UI-004 | T-UI-004, T-UI-010 | Title/directory/branch-or-PR/activity/state/count card model; GitHub fallback/cache tests | Automated |
| FR-UI-005 | T-UI-005 | Read-only routes and non-expandable cards | Automated and browser-verified |
| FR-UI-006 | T-UI-006 | Responsive CSS and 360-pixel Playwright evidence | Manual browser gate passed |
| FR-UI-007 | T-HTTP-004, T-UI-007 | SSE-only live delivery, stale/reconnecting state, resync-on-lag | Automated and browser-verified |
| FR-UI-008 | T-HTTP-001 | Basic Auth plus Traefik source allowlist; no published application port | Automated Compose/auth contract |
| FR-UI-009 | T-NOT-004 | Bounded human payload contains only issue/title/startup directory | Automated |
| FR-UI-010 | T-HTTP-006 | Authenticated root redirects to `/ui` without bypassing the Basic Auth challenge | Automated |
| FR-NOT-001 | T-NOT-001, T-NOT-002 | Main-impacting human destinations and child-only suppression in outbox policy | Automated |
| FR-NOT-002 | T-NOT-001 | SSE/web center, browser notifications, Home Assistant, and generic webhook dispatch | Automated; browser permission behavior manually verified |
| FR-NOT-003 | T-NOT-003 | Bounded one-attempt webhook delivery, timeout/redirect/error audit | Automated |
| FR-NOT-004 | T-NOT-005, T-STA-009 | Reminder cadence and recovery cancellation | Automated |
| FR-API-001 | T-HTTP-002 | Read-only dashboard JSON/SSE router; mutation routes absent | Automated |
| FR-API-002 | T-UI-004 | GitHub remote parsing, PR lookup cache, offline/branch fallback | Automated |
| FR-API-003 | T-HTTP-005, T-STA-001 | Separate normalized state and compatibility-warning fields | Automated |

## Persistence, configuration, operations, and security

| Requirement | Acceptance tests | Implementation and test evidence | Status |
|---|---|---|---|
| FR-DATA-001 | T-DATA-001–003, T-KILL-014, T-STA-010, T-STA-013, T-STA-014 | SQLite WAL store, atomic observation/snapshot/event/outbox writes, restart repositories | Automated |
| FR-DATA-002 | T-DATA-004 | No pruning worker or retention delete path; history persists until wipe | Automated by store/restart behavior; final absence audit passed |
| FR-DATA-003 | T-DATA-005 | Store-only manual wipe test and Compose volume wipe procedure | Automated |
| FR-DATA-004 | T-DATA-005 | Disposable named volume and documented no-backup guarantee | Automated/manual contract |
| FR-CFG-001 | T-CFG-001, T-CFG-003 | Environment bootstrap snapshot, mounted TOML, secret types, concrete roots | Automated |
| FR-CFG-002 | T-CFG-001, T-CFG-002 | Atomic SIGHUP reload and last-valid snapshot/error retention | Automated |
| FR-OPS-001 | T-OPS-001–003 | Hardened multi-stage image, Traefik-only Compose, non-root/read-only context | Automated contract and live Compose evidence |
| FR-OPS-002 | T-OPS-001 | Exact read-only mounts, host PID namespace, no Docker socket/home/root mount | Automated contract |
| FR-OPS-003 | T-OPS-008 | 50-main/500-session production-service load and restart test | Explicit load target passed |
| FR-OPS-004 | T-EVD-006 | Event-driven workers, bounded queues/scans/records, degradation health; isolated 500-session container gate | Automated; ten-minute CPU averaged 0.000% at Docker precision and burst p99 was 65.732 ms |
| FR-OPS-005 | T-OPS-004, T-OPS-005 | Component health registry, isolated adapter degradation, critical readiness failure | Automated |
| FR-OPS-006 | T-CPU-010, T-OPS-006 | Structured `tracing`, stable event codes, redacted Debug/error tests, bounded change-only correlation log cache | Automated and live reconciliation noise-checked |
| FR-OPS-007 | T-OPS-007 | Health/log interface only; no metrics route or exporter | Automated route/Compose inspection |
| FR-SEC-001 | T-CFG-004, T-CPU-010, T-HTTP-001, T-MCP-001 | Constant-time bounded credentials, secret wrappers, redacted errors/debug | Automated |
| FR-SEC-002 | T-DIS-007, T-SEC-001 | Linux capability roots, canonical mappings, openat2/no-symlink tests, and rejected MCP traversal/out-of-prefix/symlink registrations | Automated |
| FR-SEC-003 | T-HTTP-003, T-SEC-002 | Bounded typed parsers, no shell construction from native content, Maud escaping/CSP | Automated |

## Compatibility and engineering process

| Requirement | Acceptance tests | Implementation and test evidence | Status |
|---|---|---|---|
| FR-COMP-001 | T-COMP-001 | Version-guarded adapters and documented observed versions | Automated; owner-authorized live probe passed best-effort discovery, formal isolated matrix pending |
| FR-COMP-002 | T-COMP-002, T-EVD-005, T-KILL-012, T-OPS-004 | Per-adapter/session `UPGRADE`, best-effort state, termination suspension | Automated |
| FR-COMP-003 | T-COMP-004 | Synthetic adapters exist; Claudius transfer pitfalls audited; owner-authorized corrective live probe recorded separately from the formal matrix | **Corrective live evidence passed; formal isolated live QA pending** |
| FR-COMP-004 | T-COMP-005 | Reducer/correlation/timer/safety slices have synthetic typed-event TDD evidence and final diff self-review | Complete |
| FR-COMP-005 | T-COMP-006 | Per-commit formatter, targeted tests, strict Clippy, audit/deny, Compose/image/browser release evidence | Final Linux engineering gates passed |

## Non-FR acceptance rules

| Source rule | Acceptance tests | Evidence | Status |
|---|---|---|---|
| Requirements §7 detailed/compact state model | T-STA-001, T-STA-003, T-STA-011, T-STA-012 | Domain state projection and parent/child reducer aggregation tests | Automated |
| UX Specification §13 accessibility | T-UI-008, T-UI-009 | Semantic Maud markup, focus/live regions, color-scheme/reduced-motion CSS, Playwright keyboard/mobile checks | Final fresh 3/3 browser rerun passed through Traefik |
| Product summary macOS build-only claim | T-COMP-003 | Linux boundaries are compile-gated; `macos-15` CI build-only job added; domain/process/testkit cross-check passes | **Actual macOS CI result pending push; Linux full cross-check blocked by missing Apple C SDK** |

## Load and live-release cases

| Cases | Current evidence | Status |
|---|---|---|
| T-LOAD-001, T-LOAD-005 | 50 mains/500 total sessions and ten reopen/reconcile cycles in `server/tests/load.rs` | Passed explicit release target; termination-stage restart coverage remains in `server/tests/termination.rs` |
| T-LOAD-002 | Huge sparse transcript incremental-read test | Automated |
| T-LOAD-003 | Watcher saturation tests, a 12,000-file live Compose burst, and 500-event production-hook burst | Passed; hook burst p99 65.732 ms and service remained ready |
| T-LOAD-004 | Lagging SSE, durable inbox, and hanging one-attempt webhook tests | Automated |
| T-LIVE-CLAUDE-001/002 | Formal cases require disposable roots; a separate owner-authorized read-only corrective probe verified team/member hierarchy | **Corrective evidence passed; isolated cases pending** |
| T-LIVE-CODEX-001/002 | Formal cases require disposable roots; a separate owner-authorized read-only probe verified three Claude-originated threads | **Corrective evidence passed; isolated cases pending** |
| T-LIVE-COMP-001/002 | Formal cases require disposable roots; a separate owner-authorized read-only probe verified wrapper aliasing and retained state | **Corrective evidence passed; isolated cases pending** |

## Open closure work

1. Complete the isolated live-runtime matrix if a dedicated credentialed runtime
   environment is available and obtain the first `macos-15` CI build-only result
   after push. The deferred Claudius knowledge-transfer audit and all Linux
   release gates are complete.
