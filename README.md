# Agent Watchdog

Agent Watchdog monitors subagents in multi-agent orchestration sessions so a
coordinating Claude or Codex agent can detect missing progress updates quickly.
It is designed to recover minutes otherwise lost when a worker stalls,
disappears, fails, or never reports back.

The planned v1 is a Rust service deployed with Docker Compose on Linux. It
automatically discovers Claude Code, native Codex CLI, and Codex Companion
sessions, combines runtime, filesystem, and process-tree evidence, provides an
MCP interface for parent agents, and presents a responsive read-only dashboard.

The project is currently in planning. Start with:

- [Requirements](docs/REQUIREMENTS.md)
- [Approved UX and wireframes](docs/UX_SPECIFICATION.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Test specification](docs/TEST_SPECIFICATION.md)
- [Development plan](docs/DEVELOPMENT_PLAN.md)

Implementation begins only after the planning package is approved and the
working agent context is compacted.

License: [MIT](LICENSE)
