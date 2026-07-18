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
`native_worktree_roots` contains the corresponding host prefixes that runtimes
persist in their state, in the same order. For example, map host
`/home/example/git` to mounted `/monitored/worktrees`; the service validates
native paths through this projection while retaining the host path for the UI
and notifications. Keep `automation_enabled = false` while validating a new
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

Both containers must become `healthy`. Open `/ui` on the configured HTTP bind
address and port. `/health` is the authenticated detailed health report;
`/health/live` is intentionally minimal but remains protected by Traefik on the
published listener. MCP clients connect to `/mcp` with the shared Bearer token.

The application database lives in the Compose-managed `watchdog-data` volume.
On its first mount, Docker copies a sticky world-writable directory scaffold so
the configured non-root UID can create the SQLite database. Database files are
then owned by that UID.

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

An `UPGRADE` warning on a session means a runtime format or behavior is newer
than the tested adapter. Monitoring continues on a best-effort basis, but
destructive automation is suspended for affected sessions. Update Agent
Watchdog and inspect detailed health before treating the warning as resolved.

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
versions other than those tested; compatibility problems become `UPGRADE`
warnings instead of silent startup refusal.

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
