# Phase 0 spike index

Status: complete

Date: 2026-07-17

Phase 0 verifies assumptions that could invalidate the architecture before
production code depends on them. Each record names the environment, commands,
result, and architectural impact.

| Spike | Record | Status |
|---|---|---|
| Current runtimes and native state | [runtime-state.md](runtime-state.md) | Verified metadata; isolated live exercises remain for adapter milestones |
| Dependency baseline | [dependencies.md](dependencies.md) | Verified registry baseline; advisory lockfile check follows workspace creation |
| MCP transport scoping | [mcp-scoping.md](mcp-scoping.md) | Verified |
| Host PID/UID and signaling | [host-process.md](host-process.md) | Verified |
| inotify and huge files | [filesystem-ingestion.md](filesystem-ingestion.md) | Verified |
| 50/500 baseline | [capacity.md](capacity.md) | Verified |

Reproducible Phase 0 helpers are Rust binaries and tests in `tools/spikes/`.
They use the same stable toolchain and `rustix` APIs selected for the service.
