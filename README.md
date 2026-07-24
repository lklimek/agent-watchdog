# Agent Watchdog

Agent Watchdog monitors subagents in multi-agent orchestration sessions so a
coordinating Claude or Codex agent can detect missing progress updates quickly.
It is designed to recover minutes otherwise lost when a worker stalls,
disappears, fails, or never reports back.

V1 is being implemented as a Rust service deployed with Docker Compose on Linux. It
automatically discovers Claude Code, native Codex CLI, and Codex Companion
sessions, combines runtime, filesystem, and process-tree evidence, provides an
MCP interface for parent agents, and presents a responsive read-only dashboard.

Implementation follows the approved planning package:

- [Requirements](docs/REQUIREMENTS.md)
- [Approved UX and wireframes](docs/UX_SPECIFICATION.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Test specification](docs/TEST_SPECIFICATION.md)
- [Development plan](docs/DEVELOPMENT_PLAN.md)
- [V1 requirement traceability](docs/TRACEABILITY.md)
- [Implementation checkpoint](docs/IMPLEMENTATION_STATUS.md)
- [Contribution and quality gates](CONTRIBUTING.md)
- [Linux Docker Compose operations](docs/OPERATIONS.md)

Phase 0 architecture probes and their measured decisions are recorded under
[`docs/spikes`](docs/spikes/README.md). Production crates live under `crates/`.

License: [MIT](LICENSE)

## Claude Code plugin

Agent Watchdog ships as a Claude Code plugin with an HTTP MCP connection and a
coordinator skill. Start the server first by following the
[Linux Docker Compose operations guide](docs/OPERATIONS.md).

Keep the endpoint and Bearer credential in the environment that launches
Claude Code. `AGENT_WATCHDOG_URL` is the base URL without `/mcp`; it defaults to
`http://localhost:8080`. `WATCHDOG_BEARER_TOKEN` is required and must match the
server setting:

```bash
export AGENT_WATCHDOG_URL=http://localhost:8080
export WATCHDOG_BEARER_TOKEN='<load from your secret manager>'
claude
```

Do not commit the real token or place it in tracked shell, Claude, or project
configuration. Use HTTPS for any endpoint that is not confined to the trusted
local host or network.

After the Agent Watchdog entry is published in the `lklimek/agents`
marketplace, install it from inside Claude Code:

```text
/plugin marketplace add lklimek/agents
/plugin install agent-watchdog@lklimek
/reload-plugins
```

Confirm the server is connected with `/mcp`. Coordinators can then invoke
`/agent-watchdog:agent-watchdog`, and Claude can load the skill automatically
when coordinating delegated agents. For local development before marketplace
publication, run `claude --plugin-dir .` from this repository and use
`/reload-plugins` after edits.
