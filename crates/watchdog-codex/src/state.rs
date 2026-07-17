use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use thiserror::Error;
use watchdog_domain::{BoundedText, DomainInputError, NativeSessionKey, RuntimeKind, SessionKind};

const MAX_THREADS: u32 = 1_000;

/// Read-only current Codex local-state fallback.
#[derive(Clone, Debug)]
pub struct CodexStateReader {
    pool: SqlitePool,
}

impl CodexStateReader {
    /// Open a Codex database without create, migration, or repair rights.
    ///
    /// # Errors
    ///
    /// Returns [`CodexStateError`] when the read-only database cannot open.
    pub async fn open(path: &Path) -> Result<Self, CodexStateError> {
        let options = SqliteConnectOptions::from_str("sqlite:")
            .map_err(|_| CodexStateError::Open)?
            .filename(path)
            .read_only(true)
            .create_if_missing(false);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|_| CodexStateError::Open)?;
        Ok(Self { pool })
    }

    /// Load a bounded recent thread set and exact spawn edges.
    ///
    /// # Errors
    ///
    /// Returns [`CodexStateError`] for invalid limits, schema drift, corrupt
    /// bounded fields, or read failure.
    pub async fn discover_threads(&self, limit: u32) -> Result<Vec<CodexThread>, CodexStateError> {
        if limit == 0 || limit > MAX_THREADS {
            return Err(CodexStateError::InvalidLimit);
        }
        let rows = sqlx::query(
            "SELECT t.id, t.rollout_path, t.cwd, t.title, t.archived, t.cli_version, \
                    t.agent_nickname, t.agent_role, e.parent_thread_id \
             FROM threads AS t \
             LEFT JOIN thread_spawn_edges AS e ON e.child_thread_id = t.id \
             ORDER BY t.recency_at_ms DESC, t.id DESC LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|_| CodexStateError::Schema)?;
        rows.into_iter()
            .map(|row| {
                let id: String = row.try_get("id").map_err(|_| CodexStateError::Schema)?;
                let parent = row
                    .try_get::<Option<String>, _>("parent_thread_id")
                    .map_err(|_| CodexStateError::Schema)?
                    .as_deref()
                    .map(native_key)
                    .transpose()?;
                Ok(CodexThread {
                    subject: native_key(&id)?,
                    parent,
                    cwd: PathBuf::from(
                        row.try_get::<String, _>("cwd")
                            .map_err(|_| CodexStateError::Schema)?,
                    ),
                    rollout_path: PathBuf::from(
                        row.try_get::<String, _>("rollout_path")
                            .map_err(|_| CodexStateError::Schema)?,
                    ),
                    title: BoundedText::new(
                        "thread_title",
                        row.try_get::<String, _>("title")
                            .map_err(|_| CodexStateError::Schema)?,
                    )?,
                    archived: row
                        .try_get::<i64, _>("archived")
                        .map_err(|_| CodexStateError::Schema)?
                        != 0,
                    cli_version: BoundedText::new(
                        "cli_version",
                        row.try_get::<String, _>("cli_version")
                            .map_err(|_| CodexStateError::Schema)?,
                    )?,
                    agent_nickname: optional_text(
                        row.try_get::<Option<String>, _>("agent_nickname")
                            .map_err(|_| CodexStateError::Schema)?,
                        "agent_nickname",
                    )?,
                    agent_role: optional_text(
                        row.try_get::<Option<String>, _>("agent_role")
                            .map_err(|_| CodexStateError::Schema)?,
                        "agent_role",
                    )?,
                })
            })
            .collect()
    }
}

/// One current thread row with an exact optional spawn parent.
pub struct CodexThread {
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    cwd: PathBuf,
    rollout_path: PathBuf,
    title: BoundedText<512>,
    archived: bool,
    cli_version: BoundedText<128>,
    agent_nickname: Option<BoundedText<256>>,
    agent_role: Option<BoundedText<256>>,
}

impl CodexThread {
    /// Native thread identity.
    #[must_use]
    pub const fn subject(&self) -> &NativeSessionKey {
        &self.subject
    }

    /// Exact native spawn parent.
    #[must_use]
    pub const fn parent(&self) -> Option<&NativeSessionKey> {
        self.parent.as_ref()
    }

    /// Role established by spawn-edge presence.
    #[must_use]
    pub const fn kind(&self) -> SessionKind {
        if self.parent.is_some() {
            SessionKind::Child
        } else {
            SessionKind::Main
        }
    }

    /// Untrusted startup directory for capability validation.
    #[must_use]
    pub fn cwd(&self) -> &Path {
        &self.cwd
    }

    /// Untrusted rollout path for capability validation.
    #[must_use]
    pub fn rollout_path(&self) -> &Path {
        &self.rollout_path
    }

    /// Native thread title.
    #[must_use]
    pub fn title(&self) -> &str {
        self.title.as_str()
    }

    /// Whether Codex archived the thread.
    #[must_use]
    pub const fn archived(&self) -> bool {
        self.archived
    }

    /// CLI version that created the thread.
    #[must_use]
    pub fn cli_version(&self) -> &str {
        self.cli_version.as_str()
    }

    /// Optional native subagent nickname.
    #[must_use]
    pub fn agent_nickname(&self) -> Option<&str> {
        self.agent_nickname.as_ref().map(BoundedText::as_str)
    }

    /// Optional native subagent role.
    #[must_use]
    pub fn agent_role(&self) -> Option<&str> {
        self.agent_role.as_ref().map(BoundedText::as_str)
    }
}

impl fmt::Debug for CodexThread {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexThread")
            .field("subject", &self.subject)
            .field("parent", &self.parent)
            .field("archived", &self.archived)
            .field("cli_version", &self.cli_version)
            .field("agent_nickname", &self.agent_nickname)
            .field("agent_role", &self.agent_role)
            .finish_non_exhaustive()
    }
}

/// Read-only local-state failure without database path or native SQL content.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CodexStateError {
    /// Database could not be opened read-only.
    #[error("Codex state database could not be opened read-only")]
    Open,
    /// Requested discovery bound is zero or above the service maximum.
    #[error("Codex thread discovery limit is invalid")]
    InvalidLimit,
    /// Expected current tables or fields are unavailable.
    #[error("Codex state schema is unsupported")]
    Schema,
    /// A selected native field violated a bounded domain contract.
    #[error("Codex state contains an invalid bounded field")]
    Domain,
}

impl From<DomainInputError> for CodexStateError {
    fn from(_value: DomainInputError) -> Self {
        Self::Domain
    }
}

fn native_key(value: &str) -> Result<NativeSessionKey, CodexStateError> {
    Ok(NativeSessionKey::new(RuntimeKind::CodexCli, value)?)
}

fn optional_text<const N: usize>(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<BoundedText<N>>, CodexStateError> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| BoundedText::new(field, value).map_err(CodexStateError::from))
        .transpose()
}
