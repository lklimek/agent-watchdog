# Dependency and toolchain baseline spike

Status: verified for the Phase 0 lockfile

Date: 2026-07-17

## Question

Can the architecture use current stable, permissively licensed Rust crates with
the required features and without inheriting the host's stale default toolchain?

## Environment and method

The host default is Rust 1.92.0, while its installed `stable` channel is Rust
1.96.0. Registry metadata was queried directly from crates.io on 2026-07-17.
MemCan's live manifest was checked only as prior art.

## Selected baseline

| Crate | Version | License | Intended feature policy |
|---|---:|---|---|
| Tokio | 1.53.0 | MIT | runtime, macros, sync, time, signal, process/I/O as used |
| Axum | 0.8.9 | MIT | HTTP/1, JSON, query, Tokio; no default feature bundle |
| Tower | 0.5.3 | MIT | only middleware traits/utilities used |
| rmcp | 2.2.0 | Apache-2.0 | server, macros, stateful Streamable HTTP server |
| Maud | 0.27.0 | MIT OR Apache-2.0 | Axum integration |
| SQLx | 0.9.0 | MIT OR Apache-2.0 | SQLite, runtime, migrations; no multi-database defaults |
| notify | 8.2.0 | CC0-1.0 | stable release; Linux backend plus macOS-build support |
| Reqwest | 0.13.4 | MIT OR Apache-2.0 | JSON and Rustls only; no cookies/system proxy |
| rustix | 1.1.4 | MIT/Apache-2.0 family | process, filesystem, thread, runtime features as used |
| Serde | 1.0.228 | MIT OR Apache-2.0 | derive |
| TOML | 0.9.8 | MIT OR Apache-2.0 | typed configuration |
| tracing | 0.1.44 | MIT | structured events |
| tracing-subscriber | 0.3.23 | MIT | env filter and JSON |
| subtle | 2.6.1 | BSD-3-Clause | constant-time comparison without default extras |
| secrecy | 0.10.3 | MIT OR Apache-2.0 | secret wrappers; Serde only if required |
| zeroize | 1.9.0 | MIT OR Apache-2.0 | secret cleanup |
| thiserror | 2.0.18 | MIT OR Apache-2.0 | typed errors |
| uuid | 1.24.0 | MIT OR Apache-2.0 | v4/v5 and Serde |
| proptest | 1.11.0 | MIT OR Apache-2.0 | development only |

`notify` 9.0.0-rc.4 is a prerelease and is not selected. SQLx 0.9.0 requires
Rust 1.94, so the repository must use the current stable channel rather than
the host's 1.92 default.

## Decision

Pin `rust-toolchain.toml` to the current stable release observed at
implementation start (`1.96.0`) and use exact workspace dependency versions.
Disable defaults where practical and justify features in the workspace
manifest. A floating `stable` channel was rejected before the deployment slice
because it made the container silently install Rust 1.97.1 during a later build.

The Phase 0 Rust crate now has a lockfile. `cargo audit` loaded 1,166 RustSec
advisories and found no vulnerability in its 88-package graph. `cargo-deny`
0.20.2 reported advisories, bans, licenses, and sources all `ok` under the
repository's fail-closed `deny.toml`. The production workspace must repeat both
checks as its graph grows; this result does not approve dependencies that have
not been added yet.

```text
cargo audit --file tools/spikes/Cargo.lock
cargo deny --manifest-path tools/spikes/Cargo.toml --config deny.toml check
```

## Phase 3 production review

The production process and watcher crates re-verified `rustix 1.1.4` and
`notify 8.2.0` before direct use. Current RustSec, OSV, and GitHub Advisory
queries found no advisory affecting either exact version. The historical
`rustix` PID ownership advisory affects older 0.x releases, not 1.1.4.

`notify` is used only as an invalidation source. Its default configuration
follows symlinks and recursive mode performs an internal unbudgeted walk, so the
production service explicitly disables symlink following, registers only
capability-checked non-recursive directories from its own bounded scanner, and
uses a bounded nonblocking callback. Kernel rescan flags, callback saturation,
backend failure, scan limits, and path races produce uncertainty and bounded
reconciliation. File access is independently contained by Linux `openat2` with
`BENEATH | NO_MAGICLINKS`.

The resolved Linux graph currently selects `inotify 0.11.4`, `inotify-sys
0.1.8`, and `notify-types 2.1.0`. `CC0-1.0` and `ISC` are deliberately allowed
for this graph alongside the existing permissive policy. Watcher supervision
and periodic reconciliation remain required because stable `notify` 8.x lacks
several unreleased 9.x Linux churn fixes; the project does not select a release
candidate for production.

## Phase 9 server review

The runnable server selected exact `toml 1.1.3`, `tracing 0.1.44`, and
`tracing-subscriber 0.3.23` versions with parsing/Serde and structured JSON
logging features only. Their registry metadata, enabled feature trees,
licenses, and current RustSec state were reviewed before inclusion. The
production lockfile audit covered 316 dependencies with no vulnerability.

## Primary sources

- <https://crates.io/>
- <https://docs.rs/rmcp/2.2.0/rmcp/>
- <https://docs.rs/sqlx/0.9.0/sqlx/>
- <https://docs.rs/notify/8.2.0/notify/>
