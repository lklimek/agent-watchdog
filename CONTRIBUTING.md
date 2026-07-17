# Contributing

Agent Watchdog uses the current stable Rust toolchain pinned by
`rust-toolchain.toml`. The supported runtime deployment is Linux Docker Compose;
macOS is a build-only compatibility target.

## Local quality gates

Run the narrowest relevant package test while developing. Before committing a
coherent slice, run:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo audit --file Cargo.lock
cargo deny --config deny.toml check
```

Phase 0 probes are an excluded standalone crate and retain their own lockfile:

```text
cargo clippy --manifest-path tools/spikes/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path tools/spikes/Cargo.toml --all-targets
cargo audit --file tools/spikes/Cargo.lock
cargo deny --manifest-path tools/spikes/Cargo.toml --config deny.toml check
```

Never run tests against real agent state. Live compatibility tests require an
explicit opt-in and isolated temporary runtime roots.

## Design constraints

- Runtime adapters emit typed observations and never mutate reduced state.
- The domain crate performs no filesystem, network, database, clock, or process
  operations.
- Main-session identifiers cannot enter child termination APIs.
- All untrusted text and filesystem work is bounded before allocation or scans.
- Logs and errors contain identifiers, sizes, and stable field names, never
  credentials or transcript bodies.
- Pure policy, reducer, correlation, and safety changes start with a failing
  synthetic typed-event test.
