# Final QA report

Date: 2026-07-18 UTC; corrective addendum 2026-07-20 UTC

Branch: `feat/implementation`

Status: Linux release gates passed; live corrective regressions passed; two
explicit external limitations remain

## Passed gates

| Gate | Result |
|---|---|
| Formatting | `cargo fmt --all -- --check` passed. |
| Workspace tests | `cargo test --workspace --all-targets` passed; release-only tests remained intentionally ignored in this command. |
| Strict lint | `cargo clippy --workspace --all-targets --all-features -- -D warnings` passed. |
| Production supply chain | `cargo-audit` scanned 316 locked dependencies against 1,166 RustSec advisories; no vulnerability. `cargo-deny 0.20.2` reported advisories, bans, licenses, and sources all OK. |
| Phase-0 supply chain | `cargo-audit` scanned 88 locked dependencies; no vulnerability. `cargo-deny 0.20.2` reported all checks OK. |
| License distribution | `webpki-root-certs 1.0.8` CDLA notice reviewed and allowed; MIT and CDLA texts extracted from the final distroless image and byte-compared with tracked originals. |
| Compose/security | `docker compose config --quiet` and the hardened Compose contract passed. No home/root/Docker-socket mount was introduced. |
| Release image | `docker buildx build --load -t agent-watchdog:final-qa -f docker/Dockerfile .` passed. |
| Browser | Fresh supported Compose stack through Traefik: 3/3 Playwright 1.61.1 tests passed for authenticated/security headers and SSE, populated cards/child counts, 360px no-overflow layout, and keyboard/view controls. No console errors. |
| 50/500 load and restart | Explicit ignored gate passed: 404 ms ingestion, 32 ms dashboard, 17,371,136-byte high-water RSS, and ten restart/reconcile cycles. |
| Slow webhook | Explicit ignored production-timeout gate passed in 5.05 seconds without blocking ingestion. |
| Container performance | 500-event p99 65.732 ms; concurrent rendered UI p99 49.480 ms; 600-second steady-state reported 0.000% average CPU at Docker precision and 30.050 MiB maximum container memory. |
| Claudius knowledge transfer | Audited item by item. One mismatch fixed: Companion `workspaceRoot` no longer establishes child filesystem ownership. Distinct wrapper-session jobs have a regression test. |
| Final self-review | No remaining critical or high issue found in the final diff/security review. |

## Explicit limitations

### Formal isolated live runtime matrix not run

Installed Claude Code 2.1.214, Codex CLI 0.144.5, and Companion 1.0.6 metadata
were verified without opening user sessions. No disposable credentialed runtime
account was available. Codex `--ephemeral` writes no discoverable session state,
so it cannot validate disk discovery. The live matrix is recorded as not run,
not silently passed; it must never borrow operator transcripts or credentials.

The product owner later authorized a read-only corrective probe of the existing
local runtime state. That evidence is recorded below, but it does not replace a
repeatable isolated credentialed matrix.

## 2026-07-20 live corrective addendum

The owner-authorized probe exposed mismatches that synthetic tests had not:
post-reset Claude transcript/team IDs, inactive members, retained Companion
history, wrapper-to-team aliasing, Claude-origin Codex launches, and terminal
Codex records written before the first cursor. T-DIS-013 through T-DIS-020,
T-UI-011, and the strengthened T-OPS-006 cover the fixes.

A fresh-volume Compose stack (`agent-watchdog-qa3`, port 8083) read the real
runtime roots through the supported read-only mounts. Its durable database
contained three Codex CLI children beneath their correct Claude mains:

- thread `019f7e7b…` beneath Claude lead `8a852e11…`;
- thread `019f7ef7…` beneath Claude lead `11ed5789…`;
- thread `019f7ef8…` beneath Claude lead `b1a25d6a…`, with its startup directory
  projected from `/data/git-worktrees`.

The `8a852e11…` main appeared once with four finished children. Companion jobs
whose wrapper transcript uniquely identified a Claude team member stayed in the
same Claude tree. A retained worktree Codex rollout remained a separate main
where repository evidence matched multiple active Claude mains; this is the
required ambiguity-safe result. Identical Codex correlation outcomes logged
once after startup rather than once per inotify-triggered reconciliation.

The watcher reported the documented bounded-capacity degradation because both
configured worktree prefixes exceed 4,096 directories within the scan depth.
The service stayed ready and automatic termination remained suspended. Claude
and Codex adapters also reported best-effort `UPGRADE` degradation for records
outside the tested parser set; Companion remained healthy.

### macOS build result pending CI

The Rust 1.96 `x86_64-apple-darwin` standard library was installed at the final
project gate. Domain, process, and testkit crates cross-check successfully. The
full workspace stops in `libsqlite3-sys` before project Rust compilation because
this Linux host has no Apple C compiler/SDK and its `cc` rejects `-arch` and
`-mmacosx-version-min`. A build-only `macos-15` GitHub Actions job now runs
`cargo check --workspace --all-targets`; its first result requires pushing the
branch. No unsupported macOS deployment/runtime behavior was added.

## Durable evidence

- Capacity: `/data/artifacts/agent-watchdog/2026-07-18/qa/container-capacity.md`
- Browser failure artifacts (empty on success):
  `/data/artifacts/agent-watchdog/2026-07-18/qa/playwright-results/`
- Cached Cargo verification logs:
  `/data/tmp/agent-watchdog-claudius-cache/ledger/logs/`
- Claudius transfer audit:
  `docs/spikes/claudius-knowledge-transfer-audit.md`

No branch was pushed during this phase. Temporary QA containers and networks
were removed; named test volumes, local images, and synthetic fixtures remain.
