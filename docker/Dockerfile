# syntax=docker/dockerfile:1.18
FROM rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc AS builder

WORKDIR /source
COPY . .
RUN cargo build --locked --release -p watchdog-server
RUN mkdir -p \
    /rootfs/etc/agent-watchdog \
    /rootfs/monitored/claude/projects \
    /rootfs/monitored/claude/teams \
    /rootfs/monitored/claude/tasks \
    /rootfs/monitored/codex/state \
    /rootfs/monitored/codex/sessions \
    /rootfs/monitored/companion \
    /rootfs/monitored/worktrees \
    /rootfs/var/lib/agent-watchdog
RUN chmod 1777 /rootfs/var/lib/agent-watchdog \
    && touch /rootfs/var/lib/agent-watchdog/.volume-initialized

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:66aa873a4a14fb164aa01296058efd8253744606d72715e45acface073359faa

ARG BUILD_DATE=unknown
ARG VERSION=0.1.0
ARG VCS_REF=unknown
LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.description="Monitoring subagents in multi-agent orchestration sessions" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.source="https://github.com/lklimek/agent-watchdog" \
      org.opencontainers.image.title="Agent Watchdog" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=builder /rootfs/ /
COPY --from=builder /source/target/release/watchdog-server /usr/local/bin/agent-watchdog

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/agent-watchdog"]
