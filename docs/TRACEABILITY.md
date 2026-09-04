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
| FR-DIS-005 | T-DIS-003, T-DIS-005, T-DIS-023 | Deterministic correlation priority, retained relation evidence, and nested native-parent reuse | Automated |
| FR-DIS-006 | T-DIS-003, T-DIS-004, T-DIS-011, T-DIS-012, T-DIS-023 | Correlation engine plus exact native Claude/Codex/Companion hierarchy parsers | Automated |
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
| FR-DIS-017 | T-DIS-021 | Shared-team-parent transcript alias without false main promotion | Automated and live-derived regression |
| FR-DIS-018 | T-DIS-022 | PID/start-verified Claude live-registry reconciliation and absence handling | Automated and live-verified |
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
| FR-STA-007 | T-STA-008, T-STA-018 | Process sampling precedes timer evaluation; `AgentDiagnosticView` includes PID, latest CPU delta/provenance, trusted times, operation, conflicts, correlation, outcome uncertainty, and a target-branch/worktree cross-check for runtime disappearance | Automated |
| FR-STA-008 | T-STA-009 | Monotonic five-minute reminder reducer/outbox behavior | Automated |
| FR-STA-009 | T-DIS-005, T-STA-010, T-STA-014, T-STA-017 | Source-sensitive stable observation IDs, transactional store, durable event cursor, restart boundary | Automated |
| FR-KILL-001 | T-KILL-001 | `ChildSessionId`-only safety/termination entry points and main rejection tests | Automated |
| FR-KILL-002 | T-KILL-002, T-KILL-003, T-KILL-012, T-STA-003, T-STA-013, T-OPS-010 | Typed safety gates, recoverable queue pressure, fresh-health/reconciliation checks, one-hour policy | Automated |
| FR-KILL-003 | T-KILL-003–005 | Durable warning/grace stages and cancellation on extension/progress | Automated |
| FR-KILL-004 | T-KILL-006–008, T-KILL-015 | Durable graceful→TERM→KILL saga; unsupported Companion transport falls through to freshly verified TERM | Automated |
| FR-KILL-005 | T-CPU-007, T-KILL-007, T-KILL-010, T-KILL-011, T-OPS-002 | Fresh PID/start-time/executable checks and pidfd helper integration | Automated |
| FR-KILL-006 | T-KILL-008, T-KILL-009 | Default-enabled configurable KILL stage and opt-out test | Automated |
| FR-KILL-007 | T-KILL-013 | Adapter boundary is read-only; cancellation interface and OS signals are the only mutation capabilities | Automated contract and final security inspection passed |
| FR-MCP-001 | T-MCP-001 | Bearer middleware before rmcp parsing/allocation; auth and transport tests | Automated |
| FR-MCP-002 | T-MCP-002, T-MCP-003 | One immutable main-tree scope per transport and cross-tree rejection | Automated |
| FR-MCP-003 | T-MCP-004, T-MCP-010 | Twelve bounded runtime-neutral MCP tools plus live rmcp input/output-schema, structured-content, and legacy-text compatibility tests | Automated |
| FR-MCP-004 | T-DATA-003, T-MCP-005, T-MCP-009 | Transactional parent inbox with separate delivered/acknowledged ceilings across roots and restart | Automated |
| FR-MCP-005 | T-MCP-006 | Durable `AgentEventView` includes the explicit bounded FR-STA-007 diagnostic bundle without transcript retrieval | Automated |
| FR-MCP-006 | T-MCP-007 | Durable inbox is authoritative and capability model permits optional push | Automated fallback; no supported push transport in v1 |
| FR-MCP-007 | T-MCP-008 | Major/minor-gated actionable `UPGRADE` warning in snapshots, MCP, API, and UI | Automated |
| FR-MCP-008 | T-MCP-011, T-MCP-013 | `[mcp]` TOML bounds validated at load; `mcp_idle_expiry_reclaims_capacity_and_transport_scope` and `mcp_session_admission_is_atomic_under_concurrent_create_and_restore` | Automated |
| FR-MCP-009 | T-MCP-012 | Longest-idle eviction through the shared release path plus the `get_watchdog_health` occupancy gauge; `mcp_session_admission_evicts_the_longest_idle_session_at_capacity`, `mcp_admission_at_capacity_evicts_the_longest_idle_transport`, `mcp_router_publishes_session_occupancy_in_agent_health` | Automated |
| FR-MCP-010 | T-MCP-014, T-MCP-016 | Leased alias resolution on main registration; hook and discovery callers bind the returned canonical identity (`hook_children_register_against_the_alias_resolved_canonical_main`, `mcp_main_registration_resolves_discovery_alias_before_child_retry`); exact identity outranks inferred aliases (`exact_identity_outranks_inferred_discovery_alias`) | Automated |
| FR-MCP-012 | T-MCP-016, T-MCP-017, T-MCP-018 | One current target per alias key with monotonic supersession (`discovery_aliases_survive_restart_and_supersede_older_evidence`, `newer_discovery_alias_evidence_is_not_demoted_by_a_later_older_observation`, `a_self_registered_transcript_stops_accumulating_team_lead_guesses`); registration self-heals from an unusable target (`main_registration_self_heals_when_its_alias_target_finished`, `main_registration_self_heals_when_its_alias_target_is_not_a_main_session`, `a_leased_discovery_alias_can_be_forgotten_without_touching_other_keys`); dirty-database upgrade (`upgrading_a_dirty_alias_table_keeps_only_current_resolvable_evidence`) | Automated |
| FR-MCP-013 | T-MCP-019 | Parent resolution and main/child/nested binding: `a_spawned_agent_registers_itself_as_a_child_on_its_own_transport`, `a_nested_child_registers_itself_against_its_actual_child_parent`, `a_coordinator_registered_child_binds_its_own_transport_by_re_registering`, `a_bound_transport_cannot_register_a_child_into_another_tree`; pre-bind rejection: `a_rejected_child_registration_leaves_its_transport_unbound`; post-bind rollback and event-aware retry: `a_child_registration_that_fails_after_bind_leaves_its_transport_unbound`, `an_exact_registration_retry_repairs_a_relation_after_partial_persistence`, `a_delayed_exact_registration_retry_does_not_restore_an_older_parent`, `a_registration_event_cannot_be_reused_for_a_different_parent`; pending authorization and concurrent commit-versus-rollback: `a_pending_scope_rejects_scoped_reads_and_mutations_until_commit`, `a_pending_registration_does_not_authorize_scoped_calls_until_commit`, `concurrent_binds_of_the_same_unbound_transport_and_root_yield_exactly_one_fresh_bind`, `a_guard_for_an_already_bound_matching_root_does_not_release_on_drop`, `a_fresh_guard_rollback_after_a_matching_guard_commits_keeps_the_binding`, `a_rollback_keeps_the_binding_until_the_last_pending_registration_resolves`; close/reuse generations: `a_stale_commit_cannot_authorize_a_reused_transport_binding`, `a_stale_rollback_cannot_consume_a_reused_transport_pending_slot`; upgrade backfill: `relation_event_migration_backfills_existing_relation_fingerprints` | Automated |
| FR-MCP-011 | T-MCP-015 | Request-body bound layered inside `/mcp` authentication; `mcp_post_with_a_stalled_request_body_is_bounded_and_logged`, `mcp_server_push_stream_outlives_the_request_body_bound`, `mcp_limits_default_to_the_shipped_bounds_and_reject_a_zero_bound` | Automated |

## UI, notifications, and API

| Requirement | Acceptance tests | Implementation and test evidence | Status |
|---|---|---|---|
| FR-UI-001 | T-STA-012, T-STA-015, T-STA-016, T-UI-001, T-UI-010, T-UI-012 | Reconciled active-main projection and child status aggregation | Automated and browser-verified |
| FR-UI-002 | T-UI-002 | Attention→idle→other deterministic card ordering | Automated |
| FR-UI-003 | T-UI-003 | Case-insensitive startup-directory sort control | Automated |
| FR-UI-004 | T-UI-004, T-UI-010 | Title/directory/branch-or-PR/activity/state/count card model; GitHub fallback/cache tests | Automated |
| FR-UI-005 | T-UI-005 | Read-only routes and non-expandable cards | Automated and browser-verified |
| FR-UI-006 | T-UI-006 | Responsive CSS and 360-pixel Playwright evidence | Manual browser gate passed |
| FR-UI-007 | T-HTTP-004, T-UI-007 | SSE-only live delivery, stale/reconnecting state, resync-on-lag | Automated and browser-verified |
| FR-UI-008 | T-HTTP-001 | Traefik Basic Auth plus source allowlist as the single layer; no published application port | Automated Compose/auth contract |
| FR-UI-009 | T-NOT-004 | Bounded human payload contains only issue/title/startup directory | Automated |
| FR-UI-010 | T-HTTP-006 | Root redirects to `/ui` behind the proxy's Basic Auth challenge | Automated |
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
| FR-DATA-001 | T-DATA-001–003, T-KILL-014, T-STA-010, T-STA-013–016 | SQLite WAL store, atomic observation/snapshot/event/outbox writes, restart repositories | Automated |
| FR-DATA-002 | T-DATA-004 | No pruning worker or retention delete path; history persists until wipe | Automated by store/restart behavior; final absence audit passed |
| FR-DATA-003 | T-DATA-005 | Store-only manual wipe test and Compose volume wipe procedure | Automated |
| FR-DATA-004 | T-DATA-005 | Disposable named volume and documented no-backup guarantee | Automated/manual contract |
| FR-CFG-001 | T-CFG-001, T-CFG-003 | Environment bootstrap snapshot, mounted TOML, secret types, concrete roots | Automated |
| FR-CFG-002 | T-CFG-001, T-CFG-002 | Atomic SIGHUP reload and last-valid snapshot/error retention | Automated |
| FR-OPS-001 | T-OPS-001–003 | Hardened multi-stage image, Traefik-only Compose, non-root/read-only context | Automated contract and live Compose evidence |
| FR-OPS-002 | T-OPS-001 | Exact read-only mounts, host PID namespace, no Docker socket/home/root mount | Automated contract |
| FR-OPS-003 | T-OPS-008, T-OPS-010 | 50-main/500-session load plus bounded in-memory admission and exact durable rejection recovery | Explicit load target passed |
| FR-OPS-004 | T-EVD-006 | Event-driven workers, bounded queues/scans/records, degradation health; isolated 500-session container gate | Automated; ten-minute CPU averaged 0.000% at Docker precision and burst p99 was 65.732 ms |
| FR-OPS-005 | T-OPS-004, T-OPS-005 | Component health registry, isolated adapter degradation, critical readiness failure | Automated |
| FR-OPS-006 | T-CPU-010, T-OPS-006, T-OPS-009 | Structured stage-aware `tracing`, stable event codes, redacted diagnostics, and bounded change-only correlation log cache | Automated and live reconciliation noise-checked |
| FR-OPS-007 | T-OPS-007 | Health/log interface only; no metrics route or exporter | Automated route/Compose inspection |
| FR-SEC-001 | T-CFG-004, T-CPU-010, T-HTTP-001, T-MCP-001 | Constant-time bounded credentials, secret wrappers, redacted errors/debug | Automated |
| FR-SEC-002 | T-DIS-007, T-KILL-015, T-SEC-001 | Linux capability roots, canonical mappings, rejected path escapes, and no unauthenticated Companion cancellation transport | Automated |
| FR-SEC-003 | T-HTTP-003, T-SEC-002 | Bounded typed parsers, no shell construction from native content, Maud escaping/CSP | Automated |

## Compatibility and engineering process

| Requirement | Acceptance tests | Implementation and test evidence | Status |
|---|---|---|---|
| FR-COMP-001 | T-COMP-001 | Version-guarded adapters and documented observed versions | Automated; owner-authorized live probe passed best-effort discovery, formal isolated matrix pending |
| FR-COMP-002 | T-COMP-002, T-EVD-005, T-KILL-012, T-OPS-004 | Major/minor-gated, version-explicit per-session `UPGRADE`, best-effort state, termination suspension | Automated |
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
