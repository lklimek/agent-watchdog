//! Claude Code plugin packaging contracts.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("server crate should be nested below the repository root")
        .to_owned()
}

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).expect("plugin contract fixture should be readable")
}

fn read_json(path: impl AsRef<Path>) -> Value {
    serde_json::from_str(&read(path)).expect("plugin contract fixture should be valid JSON")
}

#[test]
fn plugin_manifest_matches_workspace_metadata_and_declares_mcp_config() {
    let root = repository_root();
    let manifest = read_json(root.join(".claude-plugin/plugin.json"));
    let manifest_object = manifest
        .as_object()
        .expect("plugin manifest should be a JSON object");
    let actual_fields = manifest_object
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let expected_fields = [
        "author",
        "description",
        "keywords",
        "license",
        "mcpServers",
        "name",
        "repository",
        "version",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_fields, expected_fields,
        "plugin manifest should contain only supported schema fields"
    );

    let workspace: toml::Value = toml::from_str(&read(root.join("Cargo.toml")))
        .expect("workspace manifest should be valid TOML");
    let package = &workspace["workspace"]["package"];
    assert_eq!(manifest["name"], "agent-watchdog");
    assert_eq!(manifest["version"].as_str(), package["version"].as_str());
    assert_eq!(manifest["license"].as_str(), package["license"].as_str());
    assert_eq!(
        manifest["repository"].as_str(),
        package["repository"].as_str()
    );
    assert_eq!(manifest["author"]["name"], "lklimek");
    assert_eq!(
        manifest["mcpServers"], "./.claude-plugin/.mcp.json",
        "the nested config avoids duplicate project-level MCP discovery"
    );
}

#[test]
fn mcp_config_uses_streamable_http_and_environment_only_bearer_auth() {
    let root = repository_root();
    let config = read_json(root.join(".claude-plugin/.mcp.json"));
    let top_level = config
        .as_object()
        .expect("MCP config should be a JSON object");
    assert_eq!(
        top_level.keys().map(String::as_str).collect::<Vec<_>>(),
        ["mcpServers"],
        "Claude Code requires MCP definitions below mcpServers"
    );

    let server = &config["mcpServers"]["agent-watchdog"];
    assert_eq!(server["type"], "http");
    assert_eq!(
        server["url"],
        "${AGENT_WATCHDOG_URL:-http://localhost:8080}/mcp"
    );
    assert_eq!(
        server["headers"]["Authorization"],
        "Bearer ${WATCHDOG_BEARER_TOKEN}"
    );
    assert_eq!(
        server
            .as_object()
            .expect("server definition should be an object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        ["headers", "type", "url"].into_iter().collect()
    );
}

#[test]
fn coordinator_skill_covers_the_safe_lifecycle_contract() {
    let root = repository_root();
    let skill = read(root.join("skills/agent-watchdog/SKILL.md"));
    let required_terms = [
        "register_session",
        "register_delegation",
        "register_watch_path",
        "report_progress",
        "report_waiting",
        "complete_session",
        "update_deadline",
        "list_events",
        "next_cursor",
        "event_key",
        "get_session_tree",
        "get_watchdog_health",
        "runtime-native",
        "provenance",
        "transport reconnect",
        "direct evidence",
        "Corroborate, don't trust alone",
        "all adapters",
        "identity conflict",
    ];
    for term in required_terms {
        assert!(
            skill.contains(term),
            "coordinator skill should document `{term}`"
        );
    }

    let frontmatter = skill
        .strip_prefix("---\n")
        .and_then(|rest| rest.split_once("\n---\n"))
        .map(|(frontmatter, _)| frontmatter)
        .expect("skill should have YAML frontmatter");
    assert!(frontmatter.contains("name: agent-watchdog"));
    assert!(frontmatter.contains("description:"));
    assert_eq!(
        frontmatter.lines().count(),
        2,
        "skill frontmatter should contain only name and description"
    );
}

#[test]
fn readme_explains_marketplace_install_and_secret_safe_setup() {
    let readme = read(repository_root().join("README.md"));
    for term in [
        "/plugin marketplace add lklimek/agents",
        "/plugin install agent-watchdog@lklimek",
        "AGENT_WATCHDOG_URL",
        "WATCHDOG_BEARER_TOKEN",
        "/reload-plugins",
        "Do not commit",
    ] {
        assert!(readme.contains(term), "README should document `{term}`");
    }
}
