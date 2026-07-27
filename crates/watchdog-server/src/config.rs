use std::{
    fs::File,
    io::Read as _,
    path::{Path, PathBuf},
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use serde::Deserialize;
use thiserror::Error;
use watchdog_domain::{
    BoundedText, DeadlinePolicy, DurationMs, PolicyError, ReducerPolicy, ReducerPolicyError,
    RuntimeKind,
};

use crate::{McpSessionLimits, McpSessionLimitsError, PathMappingError, WorktreePathMapping};

const MAX_CONFIG_BYTES: u64 = 1_024 * 1_024;
const MAX_RELOAD_ERROR_BYTES: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdapterConfig {
    claude: bool,
    codex: bool,
    companion: bool,
}

impl AdapterConfig {
    pub(crate) const fn claude(self) -> bool {
        self.claude
    }

    pub(crate) const fn codex(self) -> bool {
        self.codex
    }

    pub(crate) const fn companion(self) -> bool {
        self.companion
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeConfig {
    worktree_mappings: Vec<WorktreePathMapping>,
    claude_roots: Vec<PathBuf>,
    claude_path_mappings: Vec<WorktreePathMapping>,
    codex_roots: Vec<PathBuf>,
    codex_path_mappings: Vec<WorktreePathMapping>,
    companion_roots: Vec<PathBuf>,
    companion_path_mappings: Vec<WorktreePathMapping>,
    exclusions: Vec<PathBuf>,
    reducer_policy: ReducerPolicy,
    deadline_policy: DeadlinePolicy,
    adapters: AdapterConfig,
    github_enabled: bool,
    automation_enabled: bool,
    sigkill_enabled: bool,
    warning_grace: DurationMs,
    action_grace: DurationMs,
    mcp_sessions: McpSessionLimits,
}

impl RuntimeConfig {
    pub(crate) fn worktree_mappings(&self) -> &[WorktreePathMapping] {
        &self.worktree_mappings
    }

    pub(crate) fn claude_roots(&self) -> &[PathBuf] {
        &self.claude_roots
    }

    pub(crate) fn codex_roots(&self) -> &[PathBuf] {
        &self.codex_roots
    }

    pub(crate) fn companion_roots(&self) -> &[PathBuf] {
        &self.companion_roots
    }

    pub(crate) fn runtime_path_mappings(&self, runtime: RuntimeKind) -> &[WorktreePathMapping] {
        match runtime {
            RuntimeKind::ClaudeCode => &self.claude_path_mappings,
            RuntimeKind::CodexCli => &self.codex_path_mappings,
            RuntimeKind::CodexCompanion => &self.companion_path_mappings,
            RuntimeKind::OpenCode => &[],
        }
    }

    pub(crate) fn exclusions(&self) -> &[PathBuf] {
        &self.exclusions
    }

    pub(crate) const fn reducer_policy(&self) -> ReducerPolicy {
        self.reducer_policy
    }

    pub(crate) const fn deadline_policy(&self) -> DeadlinePolicy {
        self.deadline_policy
    }

    pub(crate) const fn adapters(&self) -> AdapterConfig {
        self.adapters
    }

    pub(crate) const fn github_enabled(&self) -> bool {
        self.github_enabled
    }

    pub(crate) const fn automation_enabled(&self) -> bool {
        self.automation_enabled
    }

    pub(crate) const fn sigkill_enabled(&self) -> bool {
        self.sigkill_enabled
    }

    pub(crate) const fn warning_grace(&self) -> DurationMs {
        self.warning_grace
    }

    pub(crate) const fn action_grace(&self) -> DurationMs {
        self.action_grace
    }

    /// MCP transport-admission policy, applied at startup only.
    pub(crate) const fn mcp_sessions(&self) -> McpSessionLimits {
        self.mcp_sessions
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ConfigManager {
    path: PathBuf,
    current: Arc<RwLock<Arc<RuntimeConfig>>>,
    last_reload_error: Arc<RwLock<Option<BoundedText<MAX_RELOAD_ERROR_BYTES>>>>,
}

impl ConfigManager {
    pub(crate) fn load(path: &Path) -> Result<Self, ConfigError> {
        let config = Arc::new(load_config(path)?);
        Ok(Self {
            path: path.to_owned(),
            current: Arc::new(RwLock::new(config)),
            last_reload_error: Arc::new(RwLock::new(None)),
        })
    }

    pub(crate) fn current(&self) -> Arc<RuntimeConfig> {
        Arc::clone(&self.read_current())
    }

    pub(crate) fn reload(&self) -> Result<Arc<RuntimeConfig>, ConfigError> {
        match load_config(&self.path) {
            Ok(candidate) => {
                let candidate = Arc::new(candidate);
                *self.write_current() = Arc::clone(&candidate);
                *self.write_reload_error() = None;
                Ok(candidate)
            }
            Err(error) => {
                let message = format!("configuration reload rejected: {error}");
                *self.write_reload_error() = BoundedText::new("reload_error", message).ok();
                Err(error)
            }
        }
    }

    pub(crate) fn last_reload_error(&self) -> Option<String> {
        self.read_reload_error()
            .as_ref()
            .map(|error| error.as_str().to_owned())
    }

    fn read_current(&self) -> RwLockReadGuard<'_, Arc<RuntimeConfig>> {
        self.current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_current(&self) -> RwLockWriteGuard<'_, Arc<RuntimeConfig>> {
        self.current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read_reload_error(
        &self,
    ) -> RwLockReadGuard<'_, Option<BoundedText<MAX_RELOAD_ERROR_BYTES>>> {
        self.last_reload_error
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_reload_error(
        &self,
    ) -> RwLockWriteGuard<'_, Option<BoundedText<MAX_RELOAD_ERROR_BYTES>>> {
        self.last_reload_error
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn load_config(path: &Path) -> Result<RuntimeConfig, ConfigError> {
    let mut file = File::open(path).map_err(|_| ConfigError::Read)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ConfigError::Read)?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(ConfigError::TooLarge);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| ConfigError::InvalidUtf8)?;
    let raw: RawConfig = toml::from_str(text).map_err(|_| ConfigError::InvalidToml)?;
    raw.validate()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    paths: RawPaths,
    #[serde(default)]
    thresholds: RawThresholds,
    #[serde(default)]
    adapters: RawAdapters,
    #[serde(default)]
    github: RawGitHub,
    #[serde(default)]
    termination: RawTermination,
    #[serde(default)]
    mcp: RawMcp,
}

impl RawConfig {
    fn validate(self) -> Result<RuntimeConfig, ConfigError> {
        let worktree_mappings = worktree_mappings(
            self.paths.native_worktree_roots,
            self.paths.allowed_worktree_roots,
        )?;
        let mut allowed_worktree_roots = worktree_mappings
            .iter()
            .map(|mapping| mapping.mounted_root().to_owned())
            .collect::<Vec<_>>();
        allowed_worktree_roots.sort_unstable();
        allowed_worktree_roots.dedup();
        if allowed_worktree_roots.is_empty() {
            return Err(ConfigError::MissingWorktreeRoot);
        }
        let claude_roots = canonical_directories(self.paths.claude_roots)?;
        let codex_roots = canonical_directories(self.paths.codex_roots)?;
        let companion_roots = canonical_directories(self.paths.companion_roots)?;
        let claude_path_mappings =
            runtime_path_mappings(self.paths.native_claude_roots, claude_roots.clone())?;
        let codex_path_mappings =
            runtime_path_mappings(self.paths.native_codex_roots, codex_roots.clone())?;
        let companion_path_mappings =
            runtime_path_mappings(self.paths.native_companion_roots, companion_roots.clone())?;
        if (self.adapters.claude && claude_roots.is_empty())
            || (self.adapters.codex && codex_roots.is_empty())
            || (self.adapters.companion && companion_roots.is_empty())
        {
            return Err(ConfigError::MissingRuntimeRoot);
        }
        let mut capability_roots = allowed_worktree_roots.clone();
        capability_roots.extend(claude_roots.iter().cloned());
        capability_roots.extend(codex_roots.iter().cloned());
        capability_roots.extend(companion_roots.iter().cloned());
        let exclusions = canonical_directories(self.paths.exclusions)?;
        if exclusions.iter().any(|exclusion| {
            !capability_roots
                .iter()
                .any(|root| exclusion.starts_with(root))
        }) {
            return Err(ConfigError::ExclusionOutsideRoots);
        }

        let suspect_after = seconds(self.thresholds.suspect_after_seconds)?;
        let stalled_after = seconds(self.thresholds.stalled_after_seconds)?;
        let reminder_every = seconds(self.thresholds.reminder_every_seconds)?;
        let terminate_after_stalled = seconds(self.thresholds.terminate_after_stalled_seconds)?;
        let warning_grace = seconds(self.thresholds.warning_grace_seconds)?;
        let action_grace = seconds(self.thresholds.action_grace_seconds)?;
        let reducer_policy = ReducerPolicy::new(suspect_after, stalled_after, reminder_every)?;
        let deadline_policy =
            DeadlinePolicy::new(stalled_after, terminate_after_stalled, action_grace)?;
        let mcp_sessions = McpSessionLimits::new(
            self.mcp.max_sessions,
            std::time::Duration::from_secs(self.mcp.idle_ttl_seconds),
        )?;

        Ok(RuntimeConfig {
            worktree_mappings,
            claude_roots,
            claude_path_mappings,
            codex_roots,
            codex_path_mappings,
            companion_roots,
            companion_path_mappings,
            exclusions,
            reducer_policy,
            deadline_policy,
            adapters: AdapterConfig {
                claude: self.adapters.claude,
                codex: self.adapters.codex,
                companion: self.adapters.companion,
            },
            github_enabled: self.github.enabled,
            automation_enabled: self.termination.automation_enabled,
            sigkill_enabled: self.termination.sigkill_enabled,
            warning_grace,
            action_grace,
            mcp_sessions,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPaths {
    allowed_worktree_roots: Vec<PathBuf>,
    #[serde(default)]
    native_worktree_roots: Vec<PathBuf>,
    #[serde(default)]
    native_claude_roots: Vec<PathBuf>,
    #[serde(default)]
    claude_roots: Vec<PathBuf>,
    #[serde(default)]
    native_codex_roots: Vec<PathBuf>,
    #[serde(default)]
    codex_roots: Vec<PathBuf>,
    #[serde(default)]
    native_companion_roots: Vec<PathBuf>,
    #[serde(default)]
    companion_roots: Vec<PathBuf>,
    #[serde(default)]
    exclusions: Vec<PathBuf>,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
#[allow(clippy::struct_field_names)]
struct RawThresholds {
    suspect_after_seconds: u64,
    stalled_after_seconds: u64,
    reminder_every_seconds: u64,
    terminate_after_stalled_seconds: u64,
    warning_grace_seconds: u64,
    action_grace_seconds: u64,
}

impl Default for RawThresholds {
    fn default() -> Self {
        Self {
            suspect_after_seconds: 5 * 60,
            stalled_after_seconds: 15 * 60,
            reminder_every_seconds: 5 * 60,
            terminate_after_stalled_seconds: 60 * 60,
            warning_grace_seconds: 10 * 60,
            action_grace_seconds: 10 * 60,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawAdapters {
    claude: bool,
    codex: bool,
    companion: bool,
}

impl Default for RawAdapters {
    fn default() -> Self {
        Self {
            claude: true,
            codex: true,
            companion: true,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawGitHub {
    enabled: bool,
}

impl Default for RawGitHub {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawTermination {
    automation_enabled: bool,
    sigkill_enabled: bool,
}

impl Default for RawTermination {
    fn default() -> Self {
        Self {
            automation_enabled: true,
            sigkill_enabled: true,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawMcp {
    max_sessions: usize,
    idle_ttl_seconds: u64,
}

impl Default for RawMcp {
    fn default() -> Self {
        Self {
            max_sessions: McpSessionLimits::DEFAULT_MAX_SESSIONS,
            idle_ttl_seconds: McpSessionLimits::DEFAULT_IDLE_TTL.as_secs(),
        }
    }
}

fn canonical_directories(paths: Vec<PathBuf>) -> Result<Vec<PathBuf>, ConfigError> {
    let mut canonical = paths
        .into_iter()
        .map(|path| {
            let path = path
                .canonicalize()
                .map_err(|_| ConfigError::InvalidDirectory)?;
            if !path.is_dir() {
                return Err(ConfigError::InvalidDirectory);
            }
            if path == Path::new("/") {
                return Err(ConfigError::OverbroadDirectory);
            }
            Ok(path)
        })
        .collect::<Result<Vec<_>, ConfigError>>()?;
    canonical.sort_unstable();
    canonical.dedup();
    Ok(canonical)
}

fn worktree_mappings(
    native_roots: Vec<PathBuf>,
    mounted_roots: Vec<PathBuf>,
) -> Result<Vec<WorktreePathMapping>, ConfigError> {
    if mounted_roots.is_empty() {
        return Err(ConfigError::MissingWorktreeRoot);
    }
    if mounted_roots.iter().any(|root| root == Path::new("/")) {
        return Err(ConfigError::OverbroadDirectory);
    }
    let native_roots = if native_roots.is_empty() {
        mounted_roots.clone()
    } else {
        native_roots
    };
    if native_roots.len() != mounted_roots.len() {
        return Err(ConfigError::WorktreeMappingCount);
    }
    let mut mappings = native_roots
        .into_iter()
        .zip(mounted_roots)
        .map(|(native, mounted)| WorktreePathMapping::new(native, mounted).map_err(Into::into))
        .collect::<Result<Vec<_>, ConfigError>>()?;
    mappings.sort_unstable_by(|left, right| {
        left.native_root()
            .cmp(right.native_root())
            .then_with(|| left.mounted_root().cmp(right.mounted_root()))
    });
    mappings.dedup();
    Ok(mappings)
}

fn runtime_path_mappings(
    native_roots: Vec<PathBuf>,
    mounted_roots: Vec<PathBuf>,
) -> Result<Vec<WorktreePathMapping>, ConfigError> {
    let native_roots = if native_roots.is_empty() {
        mounted_roots.clone()
    } else {
        native_roots
    };
    if native_roots.len() != mounted_roots.len() {
        return Err(ConfigError::RuntimeMappingCount);
    }
    native_roots
        .into_iter()
        .zip(mounted_roots)
        .map(|(native, mounted)| WorktreePathMapping::new(native, mounted).map_err(Into::into))
        .collect()
}

fn seconds(value: u64) -> Result<DurationMs, ConfigError> {
    value
        .checked_mul(1_000)
        .map(DurationMs::new)
        .ok_or(ConfigError::DurationOverflow)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ConfigError {
    #[error("Unable to read configuration")]
    Read,
    #[error("Configuration exceeds 1048576 bytes")]
    TooLarge,
    #[error("Configuration is not valid UTF-8")]
    InvalidUtf8,
    #[error("Configuration is not valid TOML")]
    InvalidToml,
    #[error("At least one allowed worktree root is required")]
    MissingWorktreeRoot,
    #[error("Native and mounted worktree root counts must match")]
    WorktreeMappingCount,
    #[error("Native and mounted runtime state root counts must match")]
    RuntimeMappingCount,
    #[error("Every enabled runtime adapter requires at least one state root")]
    MissingRuntimeRoot,
    #[error("Configured path is absent or not a directory")]
    InvalidDirectory,
    #[error("Configured path is too broad")]
    OverbroadDirectory,
    #[error("Configured exclusion is outside every capability root")]
    ExclusionOutsideRoots,
    #[error("Configured duration is too large")]
    DurationOverflow,
    #[error(transparent)]
    PathMapping(#[from] PathMappingError),
    #[error(transparent)]
    ReducerPolicy(#[from] ReducerPolicyError),
    #[error(transparent)]
    DeadlinePolicy(#[from] PolicyError),
    #[error(transparent)]
    McpSessionLimits(#[from] McpSessionLimitsError),
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use tempfile::TempDir;

    use super::ConfigManager;

    #[test]
    fn valid_config_is_canonicalized_and_builds_domain_policies() {
        let fixture = ConfigFixture::new();
        fixture.write(&fixture.valid_toml());

        let manager = ConfigManager::load(&fixture.config_path).expect("valid configuration");
        let config = manager.current();

        assert_eq!(
            config.worktree_mappings()[0].native_root(),
            std::path::Path::new("/host/repositories")
        );
        assert_eq!(
            config.worktree_mappings()[0].mounted_root(),
            fixture.worktrees
        );
        assert_eq!(config.claude_roots(), [fixture.claude.as_path()]);
        assert_eq!(
            config.runtime_path_mappings(watchdog_domain::RuntimeKind::CodexCli)[0].native_root(),
            fixture.codex
        );
        assert!(config.adapters().claude());
        assert!(config.adapters().codex());
        assert!(config.adapters().companion());
        assert!(config.github_enabled());
        assert!(config.sigkill_enabled());
        assert_eq!(
            config.reducer_policy(),
            watchdog_domain::ReducerPolicy::new(
                watchdog_domain::DurationMs::new(300_000),
                watchdog_domain::DurationMs::new(900_000),
                watchdog_domain::DurationMs::new(300_000),
            )
            .expect("valid reducer policy")
        );
    }

    #[test]
    fn standard_and_additional_runtime_locations_coexist_as_explicit_mappings() {
        let fixture = ConfigFixture::new();
        let additional = fixture.root.path().join("codex-additional");
        fs::create_dir(&additional).expect("additional Codex root should exist");
        let standard_line = format!("codex_roots = [{}]", toml_string(&fixture.codex));
        let mapped_lines = format!(
            "native_codex_roots = [\"/host/.codex\", \"/srv/codex-state\"]\n\
             codex_roots = [{}, {}]",
            toml_string(&fixture.codex),
            toml_string(&additional)
        );
        fixture.write(&fixture.valid_toml().replace(&standard_line, &mapped_lines));

        let manager = ConfigManager::load(&fixture.config_path)
            .expect("standard and additional roots should load");
        let config = manager.current();
        assert_eq!(config.codex_roots().len(), 2);
        let mappings = config.runtime_path_mappings(watchdog_domain::RuntimeKind::CodexCli);
        assert_eq!(mappings.len(), 2);
        assert_eq!(
            mappings[0].native_root(),
            std::path::Path::new("/host/.codex")
        );
        assert_eq!(
            mappings[1].native_root(),
            std::path::Path::new("/srv/codex-state")
        );
    }

    #[test]
    fn mcp_session_limits_default_to_the_shipped_bounds_and_reject_an_empty_cap() {
        let fixture = ConfigFixture::new();
        fixture.write(&fixture.valid_toml());
        let limits = ConfigManager::load(&fixture.config_path)
            .expect("omitted [mcp] should fall back to defaults")
            .current()
            .mcp_sessions();
        assert_eq!(limits.max_sessions(), 64);
        assert_eq!(limits.idle_ttl(), std::time::Duration::from_hours(48));

        fixture.write(&format!(
            "{}\n[mcp]\nmax_sessions = 8\nidle_ttl_seconds = 3600\n",
            fixture.valid_toml()
        ));
        let configured = ConfigManager::load(&fixture.config_path)
            .expect("explicit [mcp] should load")
            .current()
            .mcp_sessions();
        assert_eq!(configured.max_sessions(), 8);
        assert_eq!(configured.idle_ttl(), std::time::Duration::from_hours(1));

        fixture.write(&format!(
            "{}\n[mcp]\nmax_sessions = 0\n",
            fixture.valid_toml()
        ));
        assert!(
            ConfigManager::load(&fixture.config_path).is_err(),
            "a zero cap would leave eviction with no slot to free"
        );
    }

    #[test]
    fn invalid_reload_preserves_last_snapshot_and_records_bounded_error() {
        let fixture = ConfigFixture::new();
        fixture.write(&fixture.valid_toml());
        let manager = ConfigManager::load(&fixture.config_path).expect("valid configuration");
        let before = manager.current();

        fixture.write("[thresholds]\nsuspect_after_seconds = 900\nstalled_after_seconds = 300\n");
        assert!(manager.reload().is_err());

        assert!(Arc::ptr_eq(&before, &manager.current()));
        let warning = manager
            .last_reload_error()
            .expect("actionable reload warning");
        assert!(warning.contains("configuration reload rejected"));
        assert!(warning.len() <= 1_024);
    }

    #[test]
    fn exclusion_outside_concrete_capability_roots_is_rejected() {
        let fixture = ConfigFixture::new();
        let outside = fixture.root.path().join("outside");
        fs::create_dir(&outside).expect("outside fixture");
        fixture.write(&fixture.valid_toml().replace(
            "exclusions = []",
            &format!("exclusions = [{}]", toml_string(&outside)),
        ));

        let error = ConfigManager::load(&fixture.config_path).expect_err("outside exclusion");
        assert_eq!(
            error.to_string(),
            "Configured exclusion is outside every capability root"
        );
    }

    #[test]
    fn filesystem_root_is_rejected_as_an_overbroad_capability() {
        let fixture = ConfigFixture::new();
        fixture.write(
            &fixture
                .valid_toml()
                .replace(&toml_string(&fixture.worktrees), "\"/\""),
        );

        let error = ConfigManager::load(&fixture.config_path).expect_err("overbroad root");
        assert_eq!(error.to_string(), "Configured path is too broad");
    }

    #[test]
    fn native_and_mounted_runtime_root_counts_must_match() {
        let fixture = ConfigFixture::new();
        fixture.write(&fixture.valid_toml().replace(
            "native_worktree_roots = [\"/host/repositories\"]",
            "native_worktree_roots = [\"/host/repositories\"]\nnative_codex_roots = [\"/host/.codex\", \"/host/.codex/sessions\"]",
        ));

        let error = ConfigManager::load(&fixture.config_path).expect_err("mismatched roots");
        assert_eq!(
            error.to_string(),
            "Native and mounted runtime state root counts must match"
        );
    }

    struct ConfigFixture {
        root: TempDir,
        config_path: std::path::PathBuf,
        worktrees: std::path::PathBuf,
        claude: std::path::PathBuf,
        codex: std::path::PathBuf,
        companion: std::path::PathBuf,
    }

    impl ConfigFixture {
        fn new() -> Self {
            let root = TempDir::new().expect("fixture root");
            let config_path = root.path().join("watchdog.toml");
            let worktrees = root.path().join("worktrees");
            let claude = root.path().join("claude");
            let codex = root.path().join("codex");
            let companion = root.path().join("companion");
            for path in [&worktrees, &claude, &codex, &companion] {
                fs::create_dir(path).expect("fixture directory");
            }
            Self {
                root,
                config_path,
                worktrees,
                claude,
                codex,
                companion,
            }
        }

        fn valid_toml(&self) -> String {
            format!(
                r#"
[paths]
allowed_worktree_roots = [{worktrees}]
native_worktree_roots = ["/host/repositories"]
claude_roots = [{claude}]
codex_roots = [{codex}]
companion_roots = [{companion}]
exclusions = []

[thresholds]
suspect_after_seconds = 300
stalled_after_seconds = 900
reminder_every_seconds = 300
terminate_after_stalled_seconds = 3600
warning_grace_seconds = 600
action_grace_seconds = 600

[adapters]
claude = true
codex = true
companion = true

[github]
enabled = true

[termination]
automation_enabled = true
sigkill_enabled = true
"#,
                worktrees = toml_string(&self.worktrees),
                claude = toml_string(&self.claude),
                codex = toml_string(&self.codex),
                companion = toml_string(&self.companion),
            )
        }

        fn write(&self, value: &str) {
            fs::write(&self.config_path, value).expect("write config");
        }
    }

    fn toml_string(path: &std::path::Path) -> String {
        format!("{:?}", path.to_string_lossy())
    }
}
