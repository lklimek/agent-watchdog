# MCP transport-scoping spike

Status: verified

Date: 2026-07-17

## Question

Can rmcp's current Streamable HTTP server associate one logical transport with a
watchdog main-session scope while keeping durable event cursors in Watchdog's
database?

## Evidence

rmcp 2.2.0 stateful Streamable HTTP assigns an opaque `Mcp-Session-Id`, requires
it on later requests, rejects unknown sessions, and exposes a `SessionManager`
trait with create, initialize, route, close, resume, and optional external-store
restore operations. A custom manager can persist transport state.

Source inspection found an important boundary: `StreamableHttpService` invokes
the service factory before `SessionManager::create_session`, so the generated
transport ID is not a service-factory argument. Subsequent HTTP requests carry
the ID in headers, but the initialize request does not.

## Decision

Use a small Watchdog `SessionManager` wrapper around rmcp's local transport:

1. create the opaque rmcp transport ID;
2. inject that ID as a typed request extension while forwarding initialize and
   subsequent messages;
3. bind it exactly once to a validated watchdog main-session ID supplied during
   initialization or `register_session`;
4. reject rebinding and every cross-tree request server-side;
5. store the parent inbox cursor and event delivery in Watchdog's SQLite tables,
   independent of rmcp's SSE replay cursor.

This preserves the trusted-single-operator model while preventing accidental
cross-session access. The generated transport ID is not treated as authorization;
Bearer authentication still fails closed before MCP dispatch.

## Executable contract result

`tools/spikes/tests/mcp_scoping.rs` implements a custom manager around rmcp's
`LocalSessionManager` and runs its real worker transport. Two generated transport
IDs reached their respective initialize-handler `RequestContext` as typed
extensions. The application-level contract then proved:

- two transports bind exactly once to different main roots;
- same-tree descendants are readable;
- cross-tree targets and rebinding are rejected;
- an inbox read after cursor 1 returns durable event 2 independently of rmcp's
  transport replay.

The test passed against rmcp 2.2.0 on Rust 1.96.0. Production server integration
tests T-MCP-002, T-MCP-003, and T-MCP-005 still exercise the same contract over
HTTP and SQLite; the spike resolves whether rmcp exposes the transport identity
at the required boundary.

```text
cargo test --manifest-path tools/spikes/Cargo.toml --test mcp_scoping -- --nocapture
```

## Primary sources

- <https://docs.rs/rmcp/2.2.0/rmcp/transport/streamable_http_server/session/>
- <https://docs.rs/rmcp/2.2.0/rmcp/transport/streamable_http_server/session/trait.SessionManager.html>
- <https://modelcontextprotocol.io/specification/2025-06-18/basic/transports>
