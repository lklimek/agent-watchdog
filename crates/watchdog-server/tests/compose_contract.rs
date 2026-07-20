//! Static security contract for the supported Docker Compose deployment.

use std::fs;

#[test]
fn supported_compose_profile_keeps_host_access_narrow_and_containers_hardened() {
    let compose = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../compose.yaml"))
        .expect("read supported Compose file");

    let app = service_block(&compose, "agent-watchdog", "traefik");
    assert!(
        !app.contains("ports:"),
        "application port must stay unpublished"
    );
    assert!(app.contains("pid: host"));
    assert!(app.contains("read_only: true"));
    assert!(app.contains("cap_drop:\n      - ALL"));
    assert!(app.contains("no-new-privileges:true"));
    assert!(!app.contains("/var/run/docker.sock"));
    assert!(!app.contains("target: /home"));
    assert!(!app.contains("target: /\n"));
    assert!(app.contains("source: ${WATCHDOG_AGENT_WORKTREE_ROOT_PATH"));
    assert!(app.contains("target: /monitored/agent-worktrees"));
    assert!(
        !app.contains("nocopy: true"),
        "the image must initialize named-volume permissions for the configured UID"
    );

    let dockerfile = fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docker/Dockerfile"
    ))
    .expect("read supported Dockerfile");
    assert!(dockerfile.contains("FROM rust:1.96.0-bookworm@sha256:"));
    assert!(dockerfile.contains("chmod 1777 /rootfs/var/lib/agent-watchdog"));
    assert!(dockerfile.contains("/rootfs/var/lib/agent-watchdog/.volume-initialized"));

    let toolchain = fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../rust-toolchain.toml"
    ))
    .expect("read pinned Rust toolchain");
    assert!(toolchain.contains("channel = \"1.96.0\""));

    let proxy = service_block(&compose, "traefik", "volumes:");
    assert!(proxy.contains("image: traefik:v3.7.8@sha256:"));
    assert_eq!(proxy.matches("ports:").count(), 1);
    assert!(proxy.contains("read_only: true"));
    assert!(proxy.contains("cap_drop:\n      - ALL"));
    assert!(proxy.contains("no-new-privileges:true"));
    assert!(!proxy.contains("/var/run/docker.sock"));
    assert!(proxy.contains("--global.checkNewVersion=false"));
    for setting in [
        "allowEncodedSlash=false",
        "allowEncodedBackSlash=false",
        "allowEncodedNullCharacter=false",
        "allowEncodedSemicolon=false",
        "allowEncodedPercent=false",
        "allowEncodedQuestionMark=false",
        "allowEncodedHash=false",
    ] {
        assert!(proxy.contains(setting), "missing path hardening: {setting}");
    }

    for mount in app.split("- type: bind").skip(1) {
        assert!(
            mount.contains("read_only: true"),
            "every host bind must be read-only"
        );
        assert!(
            mount.contains("create_host_path: false"),
            "Compose must not create misspelled host paths"
        );
    }

    let dynamic = fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../config/traefik-dynamic.yaml"
    ))
    .expect("read Traefik routing policy");
    assert!(dynamic.contains("watchdog-claude-hooks:"));
    assert!(dynamic.contains("rule: Path(`/hooks/claude`)"));
    assert!(dynamic.contains("watchdog-codex-hooks:"));
    assert!(dynamic.contains("rule: Path(`/hooks/codex`)"));
    let hook_route = dynamic
        .split("watchdog-claude-hooks:")
        .nth(1)
        .expect("hook route should exist")
        .split("watchdog-web:")
        .next()
        .expect("web route should follow hook route");
    assert!(hook_route.contains("watchdog-trusted"));
    assert!(!hook_route.contains("watchdog-basic-auth"));
    let codex_hook_route = dynamic
        .split("watchdog-codex-hooks:")
        .nth(1)
        .expect("Codex hook route should exist")
        .split("watchdog-web:")
        .next()
        .expect("web route should follow Codex hook route");
    assert!(codex_hook_route.contains("watchdog-trusted"));
    assert!(!codex_hook_route.contains("watchdog-basic-auth"));
}

fn service_block<'a>(compose: &'a str, service: &str, next: &str) -> &'a str {
    let start = compose
        .find(&format!("  {service}:\n"))
        .expect("service exists");
    let remainder = &compose[start..];
    let end = remainder
        .find(&format!("\n  {next}:"))
        .unwrap_or(remainder.len());
    &remainder[..end]
}
