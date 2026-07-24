# Linux Docker Compose operations

Agent Watchdog v1 supports one deployment: Docker Compose on Linux. The stack
runs the Rust service under the monitored user's numeric UID/GID, shares the
host PID namespace for process evidence, and exposes only Traefik. Native
runtime state and worktrees are mounted read-only through concrete paths; the
Docker socket, `/`, and a whole home directory are never mounted.

The implementation is not yet a production release. These instructions define
the supported deployment contract and are also the Phase 9 acceptance path.

## Prerequisites

- Linux with Docker Engine and the Compose plugin;
- the numeric UID/GID that owns the monitored agent processes and state;
- concrete existing paths for Claude Code, Codex CLI, Codex Companion, and the
  permitted repository/worktree prefix;
- a trusted LAN or encrypted VPN CIDR from which the published listener may be
  reached.

Plain HTTP is currently used between the operator and the bundled Traefik
listener. Do not expose it to an untrusted network: Basic credentials are not
transport encryption. Prefer an encrypted VPN, or terminate TLS at a trusted
outer proxy until certificate management is part of this Compose profile.

## Initial configuration

Create local files from the tracked examples. They are ignored by Git and the
Docker build context.

```text
cp .env.example .env
cp config/watchdog.example.toml config/watchdog.toml
```

Set `WATCHDOG_UID` and `WATCHDOG_GID` to the monitored user's values from
`id -u` and `id -g`. Set every host path to a concrete existing file or
directory. `WATCHDOG_WORKTREE_ROOT_PATH` is the narrow prefix under which
repositories and Git worktrees may be monitored; it must never be `/` or an
entire home directory.

`WATCHDOG_CLAUDE_SESSIONS_PATH` must point to the monitored user's
`~/.claude/sessions` directory. Watchdog mounts it read-only at
`/monitored/claude/sessions`; list the matching native/mounted paths last in
`native_claude_roots` and `claude_roots`. This process-scoped registry separates
live interactive mains from retained transcripts. A malformed or incomplete
registry degrades Claude discovery and suppresses absence inference; it never
causes a mass terminal transition.

Use distinct, long random values for `WATCHDOG_BASIC_PASSWORD` and
`WATCHDOG_BEARER_TOKEN`. Create the Traefik password file with the same Basic
username and password configured in `.env`, then restrict it to the monitored
user:

Use `htpasswd` from Apache's utilities to generate a BCrypt entry. It prompts
for the password and prints the complete username/hash line:

```text
htpasswd -nB <WATCHDOG_BASIC_USERNAME>
```

Write that one printed line to `config/traefik-users` and run:

```text
chmod 0600 config/traefik-users
```

Set `WATCHDOG_TRUSTED_CIDRS` to a comma-separated allowlist of the actual
operator LAN/VPN networks. Avoid broad private-network defaults when a narrower
subnet is known. The allowlist uses the connection source address observed by
Traefik; it does not trust client-supplied forwarded headers.

The mounted TOML controls adapter roots, exclusions, thresholds, GitHub
enrichment, and termination policy. Adapter roots and
`allowed_worktree_roots` are container paths from the tracked example.
In v1, a “standard location” is this tracked Compose/environment/TOML template
after its `/home/example` values are replaced with concrete paths for the
monitored user; it is not an implicit mount or a broad home-directory scan.
`native_worktree_roots` contains the corresponding host prefixes that runtimes
persist in their state, in the same order. For example, map host
`/home/example/git` to mounted `/monitored/worktrees`; the service validates
native paths through this projection while retaining the host path for the UI
and notifications. The standard template also maps the dedicated agent prefix
`/data/git-worktrees` to `/monitored/agent-worktrees`; both mappings are concrete
read-only allowlist entries, not a broad `/data` mount. The
`native_claude_roots`, `native_codex_roots`, and
`native_companion_roots` arrays likewise correspond positionally to their
mounted adapter roots. These exact mappings let the server follow native file
paths recorded in runtime state without mounting a home directory. A missing or
escaping target is ignored and degrades only that adapter.

Codex automatic discovery scans rollout roots newest-first under depth, entry,
path-byte, record-size, and wall-time budgets. It reads only the first bounded
metadata record needed to establish identity and hierarchy, places a durable
cursor at EOF, and then parses only bounded, complete records appended
afterward. This avoids loading retained transcripts. Cursor replacement,
truncation, oversized records, and schema drift never cause an implicit scan
from byte zero. File discontinuities and incompatible records degrade adapter
health. An incompatible record additionally places an actionable `UPGRADE`
warning on the affected session only when detected and tested SemVer versions
differ in major or minor; patch-only drift does not add the badge.

The exact `state_5.sqlite` bind supplies bootstrap thread metadata but may lag
rows that Codex has not checkpointed out of its WAL. Watchdog therefore does not
treat that file as its sole live source: rollout scanning, lifecycle hooks when
configured, and process evidence continue independently. Do not mount all of
`~/.codex` or bind `-wal`/`-shm` siblings individually; the former exposes
credentials and configuration, while the latter can pin stale inodes after
SQLite recreates them.

Agents may use MCP `register_watch_path` to add an existing directory beneath a
configured native worktree prefix. The server projects it through the matching
read-only container mount, rejects traversal, missing paths, exclusions, and
symlink escapes, and restores accepted registrations after restart. Explicit
registrations receive watcher-budget priority and become exact child ownership
for filesystem activity. Registration cannot expand the Compose mount or access
a path outside the preconfigured prefix.

Keep `automation_enabled = false` while validating a new
installation. Main sessions are excluded from automated termination regardless
of this switch.

## Validate and start

Validate interpolation before Docker creates anything:

```text
docker compose config --quiet
docker compose config
```

Inspect the rendered mounts before first boot. The application must have no
published `ports`, all runtime/worktree/config binds must be read-only and
concrete, and neither service may mount the Docker socket, `/`, or a whole home
directory.

Build and start the stack:

```text
docker compose up --build -d
docker compose ps
```

Both containers must become `healthy`. Open the configured HTTP bind address and
port; the authenticated root redirects to `/ui`. `/health` is the authenticated
detailed health report; `/health/live` is intentionally minimal but remains
protected by Traefik on the published listener. MCP clients connect to `/mcp`
with the shared Bearer token.

The application database lives in the Compose-managed `watchdog-data` volume.
On its first mount, Docker copies a sticky world-writable directory scaffold so
the configured non-root UID can create the SQLite database. Database files are
then owned by that UID.

## Optional Claude lifecycle hooks

Automatic filesystem discovery works without hooks. For lower-latency native
state enrichment, configure Claude Code lifecycle commands to POST their stdin
payload to `/hooks/claude`. This route bypasses browser Basic auth, but remains
behind the Traefik source allowlist and requires the same application Bearer
credential as MCP. Requests larger than 64 KiB are rejected, and Watchdog never
retains hook message/prompt bodies.

Keep the credential out of the hook command line and process list. For example,
create a user-readable-only curl configuration outside the monitored roots:

```text
url = "https://watchdog.example/hooks/claude"
header = "Authorization: Bearer replace-with-WATCHDOG_BEARER_TOKEN"
header = "Content-Type: application/json"
data-binary = "@-"
connect-timeout = 2
max-time = 5
fail-with-body
silent
show-error
```

Use `curl --config /absolute/path/to/hook-curl.conf || true` as the command hook
for `SessionStart`, `Stop`, `StopFailure`, `SessionEnd`, `Notification`,
`SubagentStart`, and `SubagentStop`. The fail-open suffix keeps optional
monitoring unavailability from blocking the agent. Protect the configuration
with mode `0600`, and follow the current Claude Code hooks documentation when
adding the command to user settings.

## Optional Codex lifecycle hooks

Codex automatic filesystem/process discovery also works without hooks. Current
Codex releases can send `SessionStart`, `SubagentStart`, `SubagentStop`, and
`Stop` lifecycle input to `/hooks/codex`. This route has the same trusted-source
and Bearer requirements as `/hooks/claude`, and Watchdog discards prompt,
assistant-message, and transcript content.

Create a second mode-`0600` curl configuration using the Claude example above,
changing only the URL to:

```text
url = "https://watchdog.example/hooks/codex"
```

Add one user-level Codex command hook for each of the four lifecycle events,
using `curl --config /absolute/path/to/codex-hook-curl.conf || true`. Hooks are
optional enrichment and must fail open. Review and trust the exact user hook in
Codex, and follow the current official hooks documentation because hook setup is
versioned runtime behavior. An empty successful response is intentional.

## Logs and health

```text
docker compose logs --tail 200 agent-watchdog
docker compose logs --tail 200 traefik
docker compose ps
```

Application logs are structured JSON from `tracing`. They contain stable event
and error codes, not credentials or transcript bodies. Adapter failures should
degrade only the affected adapter while the server stays ready. Critical store,
reducer, process-sampler, or authorization failures make readiness fail and
suspend destructive automation.

An `UPGRADE` warning on a session means the detected runtime and tested adapter
differ at the SemVer major/minor compatibility line. A patch-only difference,
including prerelease or build metadata on the same line, does not add the badge.
Monitoring continues on a best-effort basis, but destructive automation is
suspended for badged sessions. Update Agent Watchdog and inspect detailed health
before treating the warning as resolved. The warning reports both the detected
version and the Watchdog-tested version. Missing or unparseable version evidence
may degrade adapter health but does not prove a per-session mismatch.
`termination_automation` degradation means the child-only reconciliation pass
failed safely; no new signal stage is attempted until the worker and all safety
components are healthy again.
`filesystem_reconciliation` degradation means watcher events were lost or the
bounded activity queue was saturated. Monitoring continues and a full bounded
runtime reconciliation is requested. Automated termination remains suspended
until that reconciliation finishes without a newer filesystem uncertainty.
`watcher` degradation with a message about directories not fitting within bounds
means an enabled runtime root or an exact uniquely-owned active/MCP-registered worktree exceeded
the 4,096-target/depth/byte safety budget, could not be projected safely, or was
rejected by the host inotify backend. Broad configured worktree prefixes are
capability allowlists and are not recursively watched, so their repository count
alone does not degrade health. The server remains available and continues
bounded best-effort reconciliation, but automatic termination stays suspended
when required exact coverage is incomplete. Runtime roots receive target
priority, followed by explicit registrations and uniquely-owned active child
worktrees. Shared active worktrees are omitted because their events cannot be
attributed safely. Raising
host inotify limits does not bypass the application's own bounded target policy.
`dashboard_delivery` is independent: a lagging or failed browser/SSE delivery
cannot authorize or suspend process signals.

## Reload configuration

After editing `config/watchdog.toml`, request an atomic reload:

```text
docker compose kill --signal SIGHUP agent-watchdog
```

A valid candidate replaces the previous immutable configuration. An invalid
candidate is rejected, the last valid configuration stays active, and `/health`
and structured logs report an actionable warning. Environment and secret
changes require container recreation with `docker compose up -d`.

## Upgrade

For a source checkout, fetch the intended reviewed revision, inspect changes to
`.env.example`, `config/watchdog.example.toml`, mounts, and image digests, then:

```text
docker compose build --pull
docker compose up -d
docker compose ps
```

Traefik is pinned by version and multi-platform digest. Do not replace the pin
with `latest`. Agent Watchdog intentionally starts optimistically with runtime
versions other than those tested. Compatibility problems degrade health instead
of causing silent startup refusal; per-session `UPGRADE` warnings require a
confirmed detected/tested SemVer major/minor mismatch.

On Linux, verified process trees are sampled immediately before each timer
evaluation. The latest trustworthy CPU delta per session replaces the previous
sample instead of creating a five-second history. Parent MCP events include
that delta and its `linux-procfs-v1` provenance together with PID identity,
trusted timestamps, active operation, conflicts, correlation evidence, and
suggested checks. A neutral CPU delta is diagnostic evidence only and never
proves a stall by itself.

## Manual history wipe

V1 never auto-prunes history. A wipe is deliberate and irrecoverable for
Watchdog state, but it must not change mounted runtime state or running agent
processes:

```text
docker compose down --volumes
docker compose up -d
```

This removes the Compose-managed database volume and recreates it empty. Verify
the Compose project name before running the command. Export/backup guarantees
are outside v1 scope.

## Troubleshooting

- `store` startup failure: confirm the named volume is mounted at
  `/var/lib/agent-watchdog`, the container is using the intended numeric UID,
  and a newly created volume root has mode `1777`.
- Traefik returns `403`: the request source is outside
  `WATCHDOG_TRUSTED_CIDRS`; narrow and correct the allowlist rather than
  disabling it.
- Traefik returns `401`: the password file and application Basic credentials
  must describe the same username/password. MCP uses the separate Bearer token.
- One adapter is `degraded`: inspect only that adapter's bounded health message
  and its exact mounted roots. Other runtimes should remain available.
- `notifications` is `degraded`: a durable human-channel outbox record could not
  be decoded, delivered, audited, or acknowledged. Agent inbox and monitoring
  continue; inspect structured `notifications.delivery_failed` logs and the
  configured webhook receiver.
- Config reload warning: fix TOML syntax, threshold ordering, absent paths, or
  an exclusion outside every capability root, then send `SIGHUP` again.
- UI says reconnecting: the last snapshot remains visible while SSE retries;
  there is intentionally no polling fallback.
- An external `CODEX_GONE reason=runtime-gone` signal confirms process absence,
  not job failure. Before retrying or replacing completed work, inspect the exact
  target branch and worktree for commits or changes newer than the last trusted
  activity. Agent Watchdog exposes the same distinction as
  `outcome_uncertain=true` on disappearance diagnostics.

  The Codex Companion runtime owns its job JSON/log finalization. If it commits
  work and exits before durably publishing terminal status, Agent Watchdog
  cannot repair that stale upstream record or safely infer `completed` without
  the expected branch, baseline commit, and exclusive job ownership.
