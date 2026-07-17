# Agent Watchdog UX Specification

Status: wireframe approval required  
Date: 2026-07-17  
Derived from: [REQUIREMENTS.md](REQUIREMENTS.md)

## 1. Experience objective

The operator should answer one question in a few seconds:

> Which orchestration session needs me right now?

The dashboard is not an agent debugger or transcript browser in v1. It is a compact queue of main sessions, with enough child counts and context to decide where to return.

## 2. Design principles

1. **Main sessions are the unit of attention.** Sub-agents are summarized as counts rather than separate dashboard rows.
2. **Waiting and stalled come first.** The default order reflects human urgency, not start time.
3. **Directories anchor identity.** The startup directory is more recognizable than a generated session ID.
4. **State is redundant, not color-only.** Text, glyph, border treatment, and count labels communicate status.
5. **Details stay with agents.** PID, source conflicts, process evidence, and suggested diagnostics go through MCP; human alerts stay concise.
6. **Disconnected is visible.** The dashboard keeps its last snapshot while SSE reconnects, clearly marking that it may be stale.
7. **Mobile is a primary viewport.** Cards reflow into one column without hiding the session title, directory, state, or child counts.

## 3. Information architecture

V1 has one authenticated dashboard route:

```text
/ui
└── Active main sessions
    ├── attention sessions: waiting, stalled
    ├── idle sessions
    └── other active sessions
```

The supporting read-only JSON API and SSE stream are not presented as separate human screens.

Cards do not expand and session detail pages are deferred.

## 4. Dashboard controls

- **Scope**: active main sessions by default; a control may include recently completed sessions.
- **Sort**:
  - `Attention` (default): waiting/stalled → idle → others.
  - `Directory A–Z`: case-insensitive startup-directory ordering.
- **Connection**: compact `Live`, `Reconnecting`, or `Stale` indicator.
- **Notifications**: browser notification permission/status control where supported.

No acknowledgement, deadline, cancel, terminate, configuration, or transcript controls appear in v1.

## 5. Main-session card anatomy

Each card contains:

1. Compact state label and optional `UPGRADE` compatibility badge.
2. Native session title; startup-directory basename is the fallback.
3. Full startup directory, allowed to wrap safely.
4. Branch name or linked GitHub PR when resolved.
5. Relative last-activity time.
6. Child counts grouped by compact status.

Example child summary:

```text
6 sub-agents   3 active   1 waiting   1 stalled   1 finished
```

Zero-count groups are omitted. A child problem does not replace the main session’s own state.

## 6. State presentation

| Compact state | Suggested glyph | Visual intent | Attention order |
|---|---:|---|---:|
| Waiting | `!` | Amber accent, explicit “Waiting for you” | 1 |
| Stalled | `×` | Red accent, explicit elapsed duration | 1 |
| Idle | `○` | Neutral muted treatment | 2 |
| Active | `▶` | Blue/green active treatment | 3 |
| Failed | `×` | Red label; remains visible when completed sessions are included | 3 |
| Finished | `✓` | Green/muted terminal treatment | 3 |
| Unknown | `?` | Hatched/neutral warning treatment | 3 |

`UPGRADE` is a separate actionable badge, with accessible text such as “Watchdog update required for Codex CLI 0.x.” It does not silently change the normalized state.

## 7. Desktop wireframe

Target: wide desktop browser, default attention sort.

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ Agent Watchdog                                         ● Live   Notifications │
│ Monitoring sub-agents in multi-agent orchestration sessions                 │
├──────────────────────────────────────────────────────────────────────────────┤
│ Sessions: [ Active ▾ ]                     Sort: [ Attention first ▾ ]       │
├──────────────────────────────────────────────────────────────────────────────┤
│ ! WAITING FOR YOU                                               12m ago     │
│ Agent Watchdog architecture                                                 │
│ /home/ubuntu/git/agent-watchdog                                             │
│ docs/planning  ·  PR #—                                                     │
│ 4 sub-agents     2 active     1 waiting     1 finished                      │
├──────────────────────────────────────────────────────────────────────────────┤
│ × STALLED                                                     stalled 18m   │
│ Fix platform state transition race                                           │
│ /data/git-worktrees/platform/fix-state-race                                  │
│ fix/state-race  ·  PR #842 ↗                                                 │
│ 6 sub-agents     3 active     1 stalled     2 finished                       │
├──────────────────────────────────────────────────────────────────────────────┤
│ ○ IDLE                                                         active 4m    │
│ Review dependency updates                                                    │
│ /home/ubuntu/git/memcan                                                      │
│ dependabot/batch  ·  PR #91 ↗                                                │
│ 2 sub-agents     2 finished                                                  │
├──────────────────────────────────────────────────────────────────────────────┤
│ ▶ ACTIVE                                                       active now   │
│ Prepare Dash SDK release                                                     │
│ /data/git-worktrees/platform/release                                         │
│ release/v3.2  ·  no PR                                                       │
│ 5 sub-agents     4 active     1 finished                                     │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Desktop layout notes

- The whole card is informational, not clickable in v1.
- State and age occupy opposite ends of the first row.
- Title is the strongest text after the state.
- Directory uses a monospace face and wraps; it is never ellipsized without a way to recover the value.
- Branch/PR is secondary and remains readable offline.
- Child counts use text plus small state glyphs; colors are supplementary.

## 8. Mobile wireframe

Target: approximately 360 CSS pixels wide.

```text
┌──────────────────────────────────┐
│ Agent Watchdog            ● Live │
│ 4 active sessions                │
├──────────────────────────────────┤
│ Show: [ Active ▾ ]               │
│ Sort: [ Attention first ▾ ]      │
├──────────────────────────────────┤
│ ! WAITING FOR YOU                │
│ Agent Watchdog architecture      │
│                                  │
│ /home/ubuntu/git/                │
│ agent-watchdog                   │
│                                  │
│ docs/planning                    │
│ Last activity 12m ago            │
│                                  │
│ 4 sub-agents                     │
│ 2 active · 1 waiting · 1 done    │
├──────────────────────────────────┤
│ × STALLED · 18m                  │
│ Fix platform state transition    │
│ race                             │
│                                  │
│ /data/git-worktrees/platform/    │
│ fix-state-race                   │
│                                  │
│ PR #842 ↗                        │
│ Last activity 18m ago            │
│                                  │
│ 6 sub-agents                     │
│ 3 active · 1 stalled · 2 done    │
└──────────────────────────────────┘
```

### Mobile layout notes

- Controls stack and retain native labels.
- The state line stays above the title.
- The path wraps at directory separators where possible.
- Child counts wrap as a compact sentence, not a wide table.
- Touch targets for controls and links are at least 44×44 CSS pixels.

## 9. Disconnected and degraded states

### SSE reconnecting

```text
┌──────────────────────────────────────────────────────────────────────┐
│ ◌ Reconnecting — status may be stale since 14:32:08                  │
└──────────────────────────────────────────────────────────────────────┘
```

- The last successful snapshot remains visible.
- The client retries SSE with bounded exponential backoff and jitter.
- There is no HTTP polling fallback.
- On reconnection, the client receives a snapshot/version before applying subsequent events.

### Runtime compatibility drift

```text
▶ ACTIVE   [UPGRADE]
Prepare release
...
```

- The accessible badge description states which watchdog adapter needs an update.
- Best-effort state continues to render.
- MCP includes the full warning field so the parent can tell the operator what failed.

## 10. Empty, loading, and error states

### Loading

Show a short “Loading active sessions…” message with no fake skeleton data.

### No active sessions

```text
No active orchestration sessions found.
Agent Watchdog is monitoring the configured Claude and Codex locations.
```

### Adapter degraded

Keep unaffected session cards visible. Show one concise page-level warning naming the affected runtime and refer operators to `/health` or logs.

### Authentication failure

Use the browser’s Basic Auth challenge. Do not render session metadata in an unauthenticated error page.

## 11. Human notifications

Browser, web notification-center, Home Assistant, and generic webhook messages share a concise shape:

```text
Agent Watchdog: waiting for user
Agent Watchdog architecture
/home/ubuntu/git/agent-watchdog
```

Human messages include only:

- issue/state;
- main-session title;
- startup directory.

PID, process details, correlation confidence, transcript data, and suggested diagnostics are reserved for agent-facing MCP events and technical logs.

Human notifications fire for:

- main session waiting for user;
- main session stalled;
- main session completed.

Unresolved alerts repeat every five minutes. Webhook delivery is attempted once per notification occurrence.

## 12. Agent-facing interaction

The parent agent does not depend on the web UI. Its durable MCP inbox provides:

- normalized state and native state;
- child and parent IDs;
- PID;
- correlated process-tree CPU-time deltas since the previous snapshot, including
  Linux user, system, waited-for-children user, and waited-for-children system
  counters where available;
- last trusted change and evidence timestamps;
- active operation/process summary;
- source conflicts and correlation basis;
- suggested diagnostic checks;
- deadline and termination-stage information;
- `UPGRADE` warning text when compatibility degrades.

The parent may change deadlines or intentional-waiting state through MCP. Push notification is optional; durable inbox delivery is authoritative.

## 13. Accessibility and responsive acceptance

- Semantic headings, lists, forms, and status text are used before ARIA supplementation.
- Every state has a text label and glyph; color is never the only signal.
- Keyboard focus is clearly visible.
- Controls and PR links are keyboard reachable in logical order.
- Live changes do not steal focus. Urgent updates use a restrained live region.
- Motion respects `prefers-reduced-motion`.
- System light/dark color preference is supported.
- Primary content meets WCAG 2.2 AA contrast targets.
- At 360 CSS pixels, the dashboard has no horizontal scrolling for primary content.

## 14. Deferred UX

- Expandable child trees and session detail pages.
- Transcript excerpts or raw transcript viewer.
- Human acknowledgement, deadline, cancellation, or termination controls.
- Advanced filters by repository, runtime, state, and age.
- Token counts and per-model breakdowns.
- Config editing, history cleanup, health dashboards, and metrics pages.

## 15. Wireframe decisions requiring approval

1. One non-clickable card per active main session, with no detail page in v1.
2. State-first cards with startup directory as the identity anchor.
3. Child status represented as counts only.
4. Desktop cards use full-width rows; mobile uses the same card content in one column.
5. Waiting/stalled are the only states promoted to the top attention group.
