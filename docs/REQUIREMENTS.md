# Agent Watchdog Requirements

Status: planning baseline, pending final approval

Date: 2026-07-17

Repository: `github.com/lklimek/agent-watchdog`

## 1. Problem statement

In multi-agent orchestration sessions, a coordinating Claude or Codex agent can wait for a delegated worker status update that never arrives. The coordinator may lose many minutes before it notices a failed spawn, dead process, stalled worker, missing message, or broken runtime integration.

Agent Watchdog monitors main sessions and their delegated work, detects missing progress quickly, and reports actionable evidence to the parent agent. It favors early investigation over silence: false alarms are acceptable when they help the coordinator recover lost work time.

## 2. Product summary

Agent Watchdog is a Linux-hosted Rust service deployed with Docker Compose. It automatically discovers Claude Code, native Codex CLI, and Codex Companion sessions; correlates their sub-agents, worktrees, processes, and activity; exposes status to agents over MCP; and presents a compact, read-only web dashboard for the human operator.

The supported v1 deployment is Docker Compose on Linux. The Rust workspace should compile on macOS where its dependencies permit, but macOS operation is untested and unsupported.

## 3. Personas

### P1 — Coordinating agent

A top-level Claude Code or Codex agent delegates work and needs to learn quickly when a child stops progressing, disappears, fails, finishes without reporting, or needs more time.

### P2 — Human operator

A single operator runs several orchestration sessions on one Linux host and needs a mobile-friendly overview of which main sessions require attention.

### P3 — Maintainer

A project maintainer adds runtime adapters, validates the latest runtime releases, diagnoses compatibility drift, and operates the Compose service without granting broad host access.

## 4. Goals

- Detect missing delegated-agent updates before the coordinator wastes substantial time.
- Monitor without mandatory agent registration or manual session setup.
- Prefer sensitive detection with cheap corroboration over delayed, overly conservative alerts.
- Keep sessions correctly scoped across concurrent repositories, working directories, Git worktrees, and runtimes.
- Give parent agents detailed diagnostic evidence and human operators concise, glanceable status.
- Continue monitoring healthy runtimes when one adapter or data source degrades.
- Bound hot-path work: use filesystem events, incremental parsing, caching, and targeted reconciliation.

## 5. Non-goals for v1

- OpenCode monitoring implementation. The domain model and adapter boundary must accommodate OpenCode later.
- Token counting, token-cost calculation, budgets, or token analytics.
- Native macOS deployment support.
- Alternate local-process deployment modes, setup scripts, or special behavior
  for operators who run the binary outside the supported Linux Compose stack.
- Multi-host monitoring, multiple operators, or multi-tenant isolation.
- A writable web UI, session detail pages, transcript viewer, or general process manager.
- Full transcript replication, automatic history backup, or automatic history pruning.
- Automatic repair or modification of native runtime state files.
- Guaranteed server-to-agent push. Push is a nice-to-have optimization where a runtime supports it safely.
- Monetary cost calculation.

## 6. Terms

- **Main session**: a user-started, top-level agent conversation. It is never subject to automatic termination.
- **Sub-agent**: any spawned or delegated child agent/thread, including Claude subagents, Claude team members, native Codex subagents, and Codex Companion jobs.
- **Startup directory**: the directory from which the user started the main agent command. This is the primary directory shown in the UI.
- **Repository**: the Git repository containing a session path, when present.
- **Worktree**: a Git working tree associated with a main session or child.
- **Observation**: a runtime API event, MCP report, filesystem event, process fact, or parsed native record.
- **Trusted state**: the most recent state for which all required authoritative sources agree.
- **Degraded session**: a session monitored on a best-effort basis after runtime compatibility drift. It carries an `UPGRADE` badge and warning message.
- **Unknown session**: a session whose material sources conflict. Destructive automation is suspended until the conflict clears.

## 7. Status model

### 7.1 Detailed normalized states

The internal model and MCP/JSON APIs support:

- `starting`
- `running`
- `waiting_for_agent`
- `waiting_for_tool`
- `waiting_for_user`
- `idle`
- `stalled`
- `completed`
- `failed`
- `cancelled`
- `disappeared`
- `unknown`

### 7.2 Compact web projection

The web UI defaults to the compact states:

- `active`
- `waiting`
- `stalled`
- `finished`
- `failed`
- `unknown`

The UI may show detailed state as secondary text, but must remain understandable from compact state and child-count badges alone.

### 7.3 State rules

- `waiting_for_user` never becomes stalled and never starts termination timers.
- A main session is `idle` when it is alive, is not waiting, has no recent agent/worktree activity, and has not crossed its stall threshold.
- A main session that reports completion while any child remains active becomes `unknown`; it is not shown as completed.
- A source conflict makes the affected state `unknown`. The server alerts/logs the conflict and suppresses termination and archival automation until trust is restored.
- Main state and child state remain separate. A child problem adds prominent counts/badges to the main-session card but does not overwrite the parent’s own state.

## 8. Functional requirements

### 8.1 Discovery and identity

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-DIS-001 | The server must automatically discover main sessions and children without MCP registration. | A supported runtime session started under an allowlisted path appears without calling any Agent Watchdog tool. |
| FR-DIS-002 | V1 must monitor Claude Code, native Codex CLI, and Codex Companion. | Each adapter can discover a live main session and at least one supported child type. |
| FR-DIS-003 | The model must be ready for an OpenCode adapter without implementing it. | Runtime-neutral session, event, hierarchy, and capability types represent OpenCode parent/child sessions and SSE/API evidence without schema changes. |
| FR-DIS-004 | The hierarchy is main session → repository/worktree contexts → sub-agents/jobs. | Every child has one main-session root or is explicitly unassigned when identity cannot be established. |
| FR-DIS-005 | Correlation priority is exact native identifiers, then MCP registration, then heuristics. | Diagnostic logs record which basis won; heuristic confidence/evidence is logged but not shown by default. |
| FR-DIS-006 | Heuristics may use process ancestry, cwd/worktree, timestamps, transcript references, and runtime metadata. | A session lacking exact parent metadata is correlated when evidence produces one best match; ambiguous matches remain unassigned or unknown. |
| FR-DIS-007 | Configured root prefixes and paths discovered from active sessions are both watched. Agents may register additional allowlisted paths through MCP. | A registered path outside the configured prefix is rejected; an in-prefix path becomes observable. |
| FR-DIS-008 | Built-in runtime state locations must support TOML overrides and additional allowlisted paths. | Default Claude/Codex installs work without path configuration; non-default paths can be added without rebuilding. |
| FR-DIS-009 | Sessions with different startup directories or Git worktrees must remain distinct even when they share a repository. | Two concurrent worktrees appear as separate main sessions and cannot refresh each other’s agent-specific clocks. |

### 8.2 Evidence collection and performance

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-EVD-001 | Filesystem observation must be event driven using Linux filesystem notifications. | An appended runtime record schedules targeted ingestion without a periodic full-tree scan. |
| FR-EVD-002 | Transcript processing must resume from a saved byte offset and parse only appended complete records. | Appending one record to a large transcript reads the appended region, not the full file. |
| FR-EVD-003 | Rotation, replacement, truncation, and watcher overflow must trigger bounded reconciliation. | The adapter marks data uncertain during reconciliation and resumes from a safe boundary without inventing transitions. |
| FR-EVD-004 | Partial, malformed, or temporarily missing files preserve the last trusted state and retry with backoff. | One partial write cannot synthesize failure, disappearance, completion, or recovery. |
| FR-EVD-005 | Any attributable change beneath a child’s worktree counts as activity. | A child-owned source change refreshes that child’s activity clock. |
| FR-EVD-006 | When multiple agents share one worktree, changes should be attributed through process/file evidence; if attribution is unavailable, worktree activity must not refresh any child clock. | An unrelated change cannot make every child on a shared worktree appear active. |
| FR-EVD-007 | Known long-running operations remain active while their correlated process tree shows CPU-time growth, I/O, child-process, or output activity. | A progressing `cargo test` does not trigger a stall even without transcript writes. |
| FR-EVD-008 | Runtime readers must avoid unbounded file reads, directory enumeration, and hot-loop logging. | Configured limits produce a degraded warning and targeted reconciliation rather than resource exhaustion. |
| FR-EVD-009 | Filesystem/process evidence and native APIs are independent sources. | A disagreement becomes `unknown`; neither source silently overrides the other. |
| FR-EVD-010 | Process sampling must compare cumulative CPU counters with the prior snapshot for the correlated process tree. On Linux, this includes each process's user, system, waited-for-children user, and waited-for-children system counters, plus currently correlated descendants. | Growth in any trustworthy counter records activity; growth across all four per-process counter classes is strong corroboration. Unchanged counters remain neutral evidence and do not alone prove a stall. |

### 8.3 Progress, delegation, and stall detection

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-STA-001 | Parents may register a delegation and expected check-in deadline through MCP, but registration is optional. | Unregistered children are still monitored; registered deadlines improve missing-update detection. |
| FR-STA-002 | Parents/children may extend, shorten, pause, resume, or mark a deadline intentionally waiting through MCP. | A valid update immediately changes the relevant timer and is recorded as an event. |
| FR-STA-003 | MCP progress reports are event driven; periodic heartbeats are not required. | A registered session remains monitorable through native evidence when it sends no heartbeat. |
| FR-STA-004 | Each runtime adapter may provide runtime-specific stall rules. | Adapter state declares its rule/source; unsupported rules fall back to the global thresholds. |
| FR-STA-005 | Global fallback thresholds are suspect after 5 minutes without progress and stalled after 15 minutes. | A child with no active-operation evidence emits the corresponding transitions at those monotonic elapsed times. |
| FR-STA-006 | Observable failures must be recognized within one reconciliation cycle. | A dead verified PID or authoritative failed status schedules an alert without waiting for the stall duration. |
| FR-STA-007 | Cheap corroborating checks run immediately before an alert. | Alert evidence includes last trusted transition, signal times, PID, process-tree CPU deltas, active-operation summary, conflicts, correlation basis, and suggested checks. |
| FR-STA-008 | Alerts repeat every 5 minutes while unresolved. | Identical observations do not create duplicate transitions, but an unresolved alert produces a scheduled reminder every five minutes. |
| FR-STA-009 | Event identities and state transitions must be idempotent across restarts. | Restart recovery does not repeat an already delivered terminal or transition event unless its reminder is due. |

### 8.4 Automated child termination

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-KILL-001 | Main sessions must never be automatically terminated. | No timer or adapter path can select a normalized main session as a signal target. |
| FR-KILL-002 | A child termination sequence may begin only after one continuous hour stalled, with no active long-running operation, no waiting-for-user state, no parent extension, no source conflict, and trustworthy child classification. | Removing any one precondition prevents the sequence. |
| FR-KILL-003 | The parent receives a warning and a 10-minute grace period before signals escalate. | A parent deadline extension or intentionally-waiting report cancels the pending escalation. |
| FR-KILL-004 | Termination escalates through a native graceful cancellation request where supported, then `SIGTERM`, then `SIGKILL`. | Each stage is recorded and a recovered/exited child prevents later stages. |
| FR-KILL-005 | Before an OS signal, the server verifies that the MCP/native PID still exists and its executable matches the declared runtime. | PID/executable mismatch aborts the signal and creates an agent diagnostic. |
| FR-KILL-006 | Automatic `SIGKILL` is enabled by default and can be disabled in TOML. | With `SIGKILL` disabled, the sequence stops after `SIGTERM` and reports the remaining process. |
| FR-KILL-007 | The service never edits native runtime state to simulate cancellation or completion. | Only supported runtime cancellation APIs and verified OS signals alter external state. |

### 8.5 Parent-agent MCP experience

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-MCP-001 | The service exposes Streamable HTTP MCP behind Bearer authentication. | Missing/wrong Bearer credentials fail closed. |
| FR-MCP-002 | An MCP client supplies its session ID and may query only that session and its descendants. | A request for an unrelated tree is rejected even with the shared server token. |
| FR-MCP-003 | Agents can register sessions/delegations, report progress/waiting/completion, manage deadlines, list their tree, read events, and read health. | Tool schemas use runtime-neutral IDs and return normalized states plus provenance. |
| FR-MCP-004 | Every meaningful child event is available to the parent through a durable inbox. | Disconnecting and reconnecting does not lose undelivered events. |
| FR-MCP-005 | Parent-facing alerts include PID and detailed diagnostic evidence. | The event includes the fields in FR-STA-007 without requiring transcript retrieval. |
| FR-MCP-006 | Best-effort push is a nice-to-have when a runtime/client supports safe delivery. | Failure or lack of push support never replaces or blocks durable inbox delivery. |
| FR-MCP-007 | Runtime compatibility drift appears as an `UPGRADE` warning field with an actionable message. | The agent can tell the user which watchdog/runtime compatibility needs attention. |

The v1 MCP tool set is:

- `register_session`
- `register_delegation`
- `report_progress`
- `report_waiting`
- `complete_session`
- `update_deadline`
- `get_session`
- `list_sessions`
- `get_session_tree`
- `list_events`
- `get_watchdog_health`

### 8.6 Human web experience

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-UI-001 | The default dashboard shows active main sessions only. | Children are summarized as counts by status and do not create top-level cards. |
| FR-UI-002 | Waiting-for-user and stalled main sessions sort first, idle sessions next, then all others. | The default ordering is deterministic within each group. |
| FR-UI-003 | The operator may switch sorting to startup-directory alphabetical order. | Changing sort order does not change filters or session state. |
| FR-UI-004 | Each card shows session title, startup directory, branch or linked GitHub PR, last activity, main state, and child counts by status. | Missing GitHub connectivity leaves an unlinked branch instead of failing the card. |
| FR-UI-005 | V1 cards are not expandable and the UI is read-only. | No acknowledge, deadline, cancellation, termination, or transcript action is present. |
| FR-UI-006 | The dashboard is usable on a narrow mobile viewport. | No horizontal scrolling is required for the primary session facts and status counts. |
| FR-UI-007 | SSE provides live updates. If disconnected, the UI shows the condition and continuously retries; it does not poll. | Stale data remains visible with a disconnected indicator until SSE recovers. |
| FR-UI-008 | The UI uses shared Basic Auth and is exposed only through Traefik on a trusted LAN/VPN allowlist. | Direct server ports are not published in the supported Compose configuration. |
| FR-UI-009 | Human alerts are concise and identify the main-session title, startup directory, and issue. | Detailed PID/evidence is absent from browser/webhook messages. |

### 8.7 Human notifications

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-NOT-001 | Human notifications fire when a main session stalls, completes, or waits for user. | Other child-only transitions remain visible as counts unless they affect the main state. |
| FR-NOT-002 | Channels include the web notification center, browser notifications, Home Assistant webhook, and a generic webhook. | Each enabled channel receives the same concise human event. |
| FR-NOT-003 | Webhook delivery is one best-effort attempt without retry. | Failure is logged and visible in health/diagnostics but creates no retry queue. |
| FR-NOT-004 | Unresolved alerts repeat every 5 minutes. | Recovery stops reminders and records a recovery event. |

### 8.8 HTTP API and GitHub enrichment

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-API-001 | The server exposes a read-only JSON API and SSE event stream for the web UI and integrations. | Mutating session endpoints are absent outside MCP. |
| FR-API-002 | GitHub PR enrichment resolves the repository remote and branch using GitHub/`gh`, caches results, and degrades offline. | A private/unreachable repository still shows its branch and session status. |
| FR-API-003 | Responses distinguish normalized state from warning badges such as `UPGRADE`. | Compatibility drift does not overwrite a usable best-effort state. |

### 8.9 Persistence and configuration

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-DATA-001 | SQLite in WAL mode stores normalized sessions, current state, events, correlations, summaries, delivery state, and timers. | A restart reconstructs active state from native sources and reconciles without duplicate alerts. |
| FR-DATA-002 | Watchdog history remains until the operator manually wipes it; v1 does not auto-prune. | No age/size task deletes stored history automatically. |
| FR-DATA-003 | Manual cleanup deletes only Watchdog history, summaries, and notification records. | Native runtime files and active processes are untouched. |
| FR-DATA-004 | The database is disposable; export, backup, and rebuild guarantees are not required. | Documentation identifies the persistent volume and manual wipe procedure. |
| FR-CFG-001 | Environment variables configure essentials/secrets; mounted TOML configures allowlisted roots, exclusions, thresholds, and runtime policy. | Secrets need not appear in TOML or command lines. |
| FR-CFG-002 | `SIGHUP` or an explicit administrative command reloads TOML; environment changes require restart. | Invalid reload preserves the last valid configuration and reports an actionable error. |

### 8.10 Deployment, security, and operations

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-OPS-001 | Supported deployment is Linux Docker Compose with Traefik, trusted-network allowlist, health checks, and an unprivileged Rust container. | No direct application port is published in the supported profile. |
| FR-OPS-002 | Host access is read-only and explicitly mounted: runtime state, configured repository/worktree prefixes, host process evidence, and required runtime/tmux sockets only. | The supported Compose file does not mount `/`, a home directory wholesale, or the Docker socket. |
| FR-OPS-003 | The service targets 50 simultaneous main sessions and 500 total agents. | Load testing demonstrates bounded queues/memory and no full-transcript rescans at target scale. |
| FR-OPS-004 | Resource efficiency has priority over strict sub-second latency. | Event-driven ingestion and bounded reconciliation meet FR-STA-006 without aggressive full-tree polling. |
| FR-OPS-005 | `/health` reports overall status and individual adapters/subsystems. | A broken Codex adapter can be distinguished from a healthy server and Claude adapter. |
| FR-OPS-006 | Operational telemetry uses `tracing`, matching MemCan conventions. | Compose logs are structured and secrets/transcript contents are redacted. |
| FR-OPS-007 | Prometheus metrics are deferred; logs and health are the v1 operational interface. | No metrics system is required for v1 acceptance. |
| FR-SEC-001 | UI uses Basic Auth and MCP/API uses a shared Bearer token; comparison and logging must not leak credentials. | Raw passwords/tokens never appear in debug output or error responses. |
| FR-SEC-002 | All paths received from runtime data or MCP are canonicalized and checked against allowlisted prefixes before access. | Symlink/path traversal cannot expand host access beyond mounted/configured roots. |
| FR-SEC-003 | Transcript-derived content is untrusted, bounded, escaped in HTML, and excluded from shell construction. | Adversarial transcript text cannot execute commands or inject markup. |

### 8.11 Compatibility and testing

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-COMP-001 | V1 targets the latest Claude Code, Codex CLI, and Codex Companion releases at implementation time. | The tested version matrix is documented. |
| FR-COMP-002 | Other versions run optimistically. Unexpected format/API changes keep best-effort monitoring active and add `UPGRADE`. | Only the affected adapter/sessions degrade; the server and other adapters continue. |
| FR-COMP-003 | Runtime compatibility is verified with manually or explicitly enabled live-runtime smoke tests. | Live tests spawn/observe/finish supported child types without touching real user data. |
| FR-COMP-004 | Pure normalization, timer, correlation, and safety logic follows TDD with synthetic typed events. | Tests are written from the test specification and fail before their implementation. |
| FR-COMP-005 | Formatter, targeted tests, Clippy with warnings denied, and relevant security checks run before each implementation commit. | Handoff reports commands that ran and anything that could not run. |

## 9. Evidence-source strategy

The architecture must preserve source provenance and use runtime-specific precedence. The following research informs the requirements but does not prescribe implementation details.

### Claude Code

- Official lifecycle hooks expose `session_id`, `cwd`, transcript path, `agent_id`, agent type, and separate sub-agent transcript paths for `SubagentStart`/`SubagentStop`.
- Claude agent-team configuration and tasks are generated under `~/.claude/teams/{team-name}/config.json` and `~/.claude/tasks/{team-name}/`; official documentation warns not to edit them.
- Main transcripts live under `~/.claude/projects/<project>/<session-id>.jsonl` and may be disabled or removed by runtime configuration.
- Team messages and idle notifications normally arrive automatically, which makes a missing update itself an important failure symptom.

### Native Codex CLI

- Codex app-server offers bidirectional JSON-RPC, thread/turn lifecycle, streamed events, and interruption for clients that launch or connect through it.
- Current local Codex state contains thread metadata and exact `parent_thread_id`/spawn edges, plus rollout events for task start/completion and sub-agent activity. These on-disk formats require compatibility guards because they are not the stable app-server API.
- Codex hooks expose session and sub-agent lifecycle data in current releases and can enrich monitoring when installed.

### Codex Companion

- Version 1.0.6 persists per-workspace `state.json`, detailed `jobs/<id>.json`, logs, launcher PIDs, session IDs, phases, and terminal errors.
- Summary and detailed files are not atomic as a pair; repeated progress may update only a log; terminal records clear their PID; and the store prunes retained jobs.
- These observations are requirements evidence only. The existing Claudius scanner/state machine is not an implementation template.

### OpenCode future adapter

- OpenCode exposes an HTTP server with session lists/status, parent-child session APIs, abort, asynchronous prompts, and SSE events.
- V1 keeps the core model capable of representing this adapter but ships no OpenCode monitoring behavior.

## 10. Known failure modes from the predecessor

The Claudius watchdog demonstrates requirements and failure cases, not a correct design. The replacement must explicitly prevent:

- Selecting the newest team/session on a shared host and monitoring the wrong coordinator.
- Missing sub-agents or Codex jobs because discovery begins from an incomplete workspace set.
- Treating transcript silence during a long build/test as a stall.
- Missing a genuinely stalled or disappeared child.
- Repeating identical transition noise without durable deduplication.
- Attributing shared-workspace changes or shared state-file mtimes to the wrong child.
- Treating partial JSON, PID reuse, record pruning, or version drift as authoritative state transitions.

## 11. Future enhancements

- OpenCode runtime adapter using its documented session/status/SSE API.
- Token counts per main-session tree and per model/repository; monetary cost remains a later, separate enhancement.
- Supported native macOS deployment.
- Reliable runtime-specific push delivery where clients surface it as model-visible input.
- Session detail pages, transcript excerpts, richer filters, writable operator controls, metrics, exports, and automated pruning/backups.

## 12. Primary documentation consulted

- [Claude Code hooks reference](https://code.claude.com/docs/en/hooks)
- [Claude Code agent teams](https://code.claude.com/docs/en/agent-teams)
- [Claude Code session storage](https://code.claude.com/docs/en/sessions)
- [Claude Code tools reference](https://code.claude.com/docs/en/tools-reference)
- [Codex app-server](https://developers.openai.com/codex/app-server)
- [Codex subagents](https://developers.openai.com/codex/subagents)
- [Model Context Protocol resource subscriptions](https://modelcontextprotocol.io/specification/draft/server/resources)
- [OpenCode server API](https://opencode.ai/docs/server/)
- [OpenCode agents](https://opencode.ai/docs/agents/)

## 13. Approved planning decisions and defaults

This ledger is normative when the conversation that produced the requirements is
no longer available. Detailed requirements above take precedence if wording ever
appears to conflict.

| Area | V1 decision |
|---|---|
| Product identity | Public MIT project `agent-watchdog` at `github.com/lklimek/agent-watchdog`. |
| Problem | Monitor subagents in multi-agent orchestration sessions so coordinators investigate missing updates quickly, accepting useful false alarms. |
| Operator/host | One trusted operator and one Linux host; target 50 main sessions and 500 total agents. |
| Deployment | Docker Compose with Traefik is the only supported deployment. The Rust workspace should compile on macOS, but operation there is unsupported. No v1 setup script or alternate local-mode features. |
| Runtimes | Claude Code main/subagents/teams, native Codex CLI main/subagents, and Codex Companion jobs. OpenCode is adapter-ready but not implemented or tested. |
| Discovery | Automatic discovery is mandatory. MCP registration and official hooks are optional enrichment. Agents may register only paths within configured allowlisted prefixes. |
| Hierarchy | Main session → repository/worktree contexts → children. Exact native relation, then MCP relation, then logged heuristic evidence. |
| Human default | Active main-session cards only; no child cards or detail page. Waiting-for-user and stalled first, idle next, others last; optional directory A–Z sort. |
| Card identity | Native title, falling back to startup-directory basename; show full startup directory, branch/linked PR, last activity, state, and child counts. |
| Agent diagnostics | Parent receives every meaningful child event and detailed evidence including PID and CPU deltas. Human channels receive only issue, main title, and startup directory. |
| Push | Nice to have where a client makes it useful; durable MCP inbox/pull is authoritative. |
| Activity | inotify-driven incremental files, attributable worktree changes, native events, process tree, I/O/output, and four Linux CPU counter classes. Shared-worktree activity is neutral when attribution fails. |
| Fallback timing | Suspect after 5 minutes, stalled after 15 minutes, unresolved alert every 5 minutes. Runtime-specific policy may override. |
| Long operations | A silent transcript during `cargo test` or another long command is not a problem while correlated process evidence progresses. |
| Waiting user | Never stalled or auto-killed; promoted in UI/human notifications. |
| Termination | Children only after one continuous hour stalled and every safety gate; warn parent, allow 10-minute grace, graceful cancel → `SIGTERM` → `SIGKILL`. `SIGKILL` defaults on with TOML opt-out. Main sessions are impossible targets. |
| Human events | Main waiting-for-user, stalled, and completed through web/browser, Home Assistant webhook, and generic webhook. Webhooks get one attempt and no retry. |
| Compatibility | Test latest versions at implementation time. Other versions run optimistically; unexpected formats add actionable `UPGRADE`, continue best effort, and suspend affected destructive automation. |
| Persistence | SQLite WAL; retain Watchdog history until manual wipe; no automatic pruning, backup, or export requirement. Never alter native runtime state. |
| Configuration | Secrets/essentials in environment; roots, exclusions, thresholds, and policy in mounted TOML. Atomic TOML reload; environment changes require restart. |
| Observability | `tracing` operational logs and detailed health only; no Prometheus in v1. Correlation confidence/evidence is logged, not shown in the default UI. |
| Tokens/cost | No token counts or cost calculation in v1. Token counts per session/model/repository are a future enhancement; cost remains later still. |
| Toolchain | Current stable Rust at implementation start, with no initial MSRV promise. |
