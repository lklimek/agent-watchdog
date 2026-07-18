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
- [Contribution and quality gates](CONTRIBUTING.md)
- [Linux Docker Compose operations](docs/OPERATIONS.md)

Phase 0 architecture probes and their measured decisions are recorded under
[`docs/spikes`](docs/spikes/README.md). Production crates live under `crates/`.

License: [MIT](LICENSE)
