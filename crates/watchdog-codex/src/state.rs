use std::{
    fmt,
    fs::File,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::{
    collections::HashMap,
    fs::Metadata,
    io,
    os::{
        fd::AsRawFd as _,
        unix::fs::{FileExt as _, MetadataExt as _},
    },
};

#[cfg(target_os = "macos")]
use std::{ffi::OsString, os::unix::ffi::OsStringExt as _};

use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use thiserror::Error;
use watchdog_domain::{
    BoundedText, DomainInputError, NativeSessionKey, RuntimeKind, SessionKind, WallTimeMs,
};

const MAX_THREADS: u32 = 1_000;

/// Read-only current Codex local-state fallback.
#[derive(Clone)]
pub struct CodexStateReader {
    source: StateSource,
    held_database: Option<Arc<File>>,
}

#[derive(Clone)]
enum StateSource {
    Pool(SqlitePool),
    Snapshot {
        all: Arc<[CodexThread]>,
        unarchived: Arc<[CodexThread]>,
    },
}

impl fmt::Debug for CodexStateReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexStateReader")
            .field(
                "source",
                &match self.source {
                    StateSource::Pool(_) => "pool",
                    StateSource::Snapshot { .. } => "snapshot",
                },
            )
            .field("holds_database_identity", &self.held_database.is_some())
            .finish_non_exhaustive()
    }
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
        Ok(Self {
            source: StateSource::Pool(pool),
            held_database: None,
        })
    }

    /// Open the exact held Codex database file without trusting its pathname identity.
    ///
    /// The held descriptor supplies the database's live name so `SQLite` retains normal
    /// WAL and journal semantics. The bounded result is consumed before the connection
    /// closes, and the descriptor that followed that connection's lifecycle must be the
    /// held file. Concurrent database opens that make the proof ambiguous are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`CodexStateError`] when the database cannot open read-only or `SQLite`
    /// consumes a different filesystem object.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub async fn open_file(database: File) -> Result<Self, CodexStateError> {
        let (database, held_descriptor, expected) = blocking_state_work(move || {
            let held_descriptor = database.as_raw_fd();
            let expected = DescriptorState {
                identity: file_identity(&database)?,
                path: descriptor_target(&descriptor_path(held_descriptor))
                    .map_err(|_| CodexStateError::Open)?,
                sqlite_database: true,
            };
            Ok((database, held_descriptor, expected))
        })
        .await?;
        let before = blocking_state_work(descriptor_states).await?;
        let reader = Self::open(&expected.path).await?;
        let after_open = blocking_state_work(descriptor_states).await?;
        let threads = reader.discover_threads(MAX_THREADS).await?;
        let unarchived = reader.discover_unarchived_threads(MAX_THREADS).await?;
        let after_query = blocking_state_work(descriptor_states).await?;
        let StateSource::Pool(pool) = reader.source else {
            return Err(CodexStateError::Open);
        };
        pool.close().await;
        let after_close = blocking_state_work(descriptor_states).await?;
        if !connection_database_matches(
            &before,
            &after_open,
            &after_query,
            &after_close,
            held_descriptor,
            &expected,
        ) {
            return Err(CodexStateError::IdentityMismatch);
        }
        Ok(Self {
            source: StateSource::Snapshot {
                all: threads.into(),
                unarchived: unarchived.into(),
            },
            held_database: Some(Arc::new(database)),
        })
    }

    /// Refuse capability-backed discovery where descriptor identity cannot be verified.
    ///
    /// # Errors
    ///
    /// Always returns [`CodexStateError::Open`] on unsupported platforms.
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub async fn open_file(_database: File) -> Result<Self, CodexStateError> {
        Err(CodexStateError::Open)
    }

    /// Load a bounded recent thread set and exact spawn edges.
    ///
    /// # Errors
    ///
    /// Returns [`CodexStateError`] for invalid limits, schema drift, corrupt
    /// bounded fields, or read failure.
    pub async fn discover_threads(&self, limit: u32) -> Result<Vec<CodexThread>, CodexStateError> {
        validate_limit(limit)?;
        if let StateSource::Snapshot { all, .. } = &self.source {
            return Ok(all.iter().take(limit as usize).cloned().collect());
        }
        let StateSource::Pool(pool) = &self.source else {
            return Err(CodexStateError::Open);
        };
        let rows = sqlx::query(
            "SELECT t.id, t.rollout_path, t.cwd, t.title, t.archived, t.cli_version, \
                    t.git_branch, t.git_origin_url, \
                    t.agent_nickname, t.agent_role, t.recency_at_ms, e.parent_thread_id \
             FROM threads AS t \
             LEFT JOIN thread_spawn_edges AS e ON e.child_thread_id = t.id \
             ORDER BY t.recency_at_ms DESC, t.id DESC LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(pool)
        .await
        .map_err(|_| CodexStateError::Schema)?;
        decode_rows(&rows)
    }

    /// Load unarchived threads whose native recency is at or after a cutoff.
    ///
    /// This bounded bootstrap heuristic avoids treating all retained Codex
    /// history as active. Exact hook/app-server/process evidence can retain or
    /// add older sessions independently.
    ///
    /// # Errors
    ///
    /// Returns [`CodexStateError`] for invalid limits, schema drift, corrupt
    /// bounded fields, or read failure.
    pub async fn discover_recent_threads(
        &self,
        cutoff: WallTimeMs,
        limit: u32,
    ) -> Result<Vec<CodexThread>, CodexStateError> {
        validate_limit(limit)?;
        if let StateSource::Snapshot { unarchived, .. } = &self.source {
            return Ok(unarchived
                .iter()
                .filter(|thread| thread.recency_at >= cutoff)
                .take(limit as usize)
                .cloned()
                .collect());
        }
        let StateSource::Pool(pool) = &self.source else {
            return Err(CodexStateError::Open);
        };
        let rows = sqlx::query(
            "SELECT t.id, t.rollout_path, t.cwd, t.title, t.archived, t.cli_version, \
                    t.git_branch, t.git_origin_url, \
                    t.agent_nickname, t.agent_role, t.recency_at_ms, e.parent_thread_id \
             FROM threads AS t \
             LEFT JOIN thread_spawn_edges AS e ON e.child_thread_id = t.id \
             WHERE t.archived = 0 AND t.recency_at_ms >= ? \
             ORDER BY t.recency_at_ms DESC, t.id DESC LIMIT ?",
        )
        .bind(cutoff.value())
        .bind(i64::from(limit))
        .fetch_all(pool)
        .await
        .map_err(|_| CodexStateError::Schema)?;
        decode_rows(&rows)
    }

    async fn discover_unarchived_threads(
        &self,
        limit: u32,
    ) -> Result<Vec<CodexThread>, CodexStateError> {
        validate_limit(limit)?;
        let StateSource::Pool(pool) = &self.source else {
            return Err(CodexStateError::Open);
        };
        let rows = sqlx::query(
            "SELECT t.id, t.rollout_path, t.cwd, t.title, t.archived, t.cli_version, \
                    t.git_branch, t.git_origin_url, \
                    t.agent_nickname, t.agent_role, t.recency_at_ms, e.parent_thread_id \
             FROM threads AS t \
             LEFT JOIN thread_spawn_edges AS e ON e.child_thread_id = t.id \
             WHERE t.archived = 0 \
             ORDER BY t.recency_at_ms DESC, t.id DESC LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(pool)
        .await
        .map_err(|_| CodexStateError::Schema)?;
        decode_rows(&rows)
    }
}

/// One current thread row with an exact optional spawn parent.
#[derive(Clone)]
pub struct CodexThread {
    subject: NativeSessionKey,
    parent: Option<NativeSessionKey>,
    cwd: PathBuf,
    rollout_path: PathBuf,
    title: BoundedText<512>,
    archived: bool,
    cli_version: BoundedText<128>,
    git_branch: Option<BoundedText<512>>,
    git_origin_url: Option<BoundedText<2_048>>,
    agent_nickname: Option<BoundedText<256>>,
    agent_role: Option<BoundedText<256>>,
    recency_at: WallTimeMs,
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

    /// Git branch recorded when Codex created or refreshed the thread.
    #[must_use]
    pub fn git_branch(&self) -> Option<&str> {
        self.git_branch.as_ref().map(BoundedText::as_str)
    }

    /// Git origin URL recorded by Codex.
    #[must_use]
    pub fn git_origin_url(&self) -> Option<&str> {
        self.git_origin_url.as_ref().map(BoundedText::as_str)
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

    /// Native recency marker used only for bounded bootstrap selection.
    #[must_use]
    pub const fn recency_at(&self) -> WallTimeMs {
        self.recency_at
    }
}

fn validate_limit(limit: u32) -> Result<(), CodexStateError> {
    if limit == 0 || limit > MAX_THREADS {
        Err(CodexStateError::InvalidLimit)
    } else {
        Ok(())
    }
}

fn decode_rows(rows: &[SqliteRow]) -> Result<Vec<CodexThread>, CodexStateError> {
    rows.iter().map(decode_row).collect()
}

fn decode_row(row: &SqliteRow) -> Result<CodexThread, CodexStateError> {
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
        git_branch: optional_text(
            row.try_get::<Option<String>, _>("git_branch")
                .map_err(|_| CodexStateError::Schema)?,
            "git_branch",
        )?,
        git_origin_url: optional_text(
            row.try_get::<Option<String>, _>("git_origin_url")
                .map_err(|_| CodexStateError::Schema)?,
            "git_origin_url",
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
        recency_at: WallTimeMs::new(
            row.try_get::<i64, _>("recency_at_ms")
                .map_err(|_| CodexStateError::Schema)?,
        ),
    })
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
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CodexStateError {
    /// Database could not be opened read-only.
    #[error("Codex state database could not be opened read-only")]
    Open,
    /// `SQLite` reopened a different filesystem object than the held database.
    #[error("Codex state database identity changed while opening")]
    IdentityMismatch,
    /// Requested discovery bound is zero or above the service maximum.
    #[error("Codex thread discovery limit is invalid")]
    InvalidLimit,
    /// Expected current tables or fields are unavailable.
    #[error("Codex state schema is unsupported")]
    Schema,
    /// A selected native field violated a bounded domain contract.
    #[error("Codex state contains an invalid bounded field")]
    Domain(#[from] DomainInputError),
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct DescriptorState {
    identity: FileIdentity,
    path: PathBuf,
    sqlite_database: bool,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn file_identity(file: &File) -> Result<FileIdentity, CodexStateError> {
    let metadata = file.metadata().map_err(|_| CodexStateError::Open)?;
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn blocking_state_work<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, CodexStateError> + Send + 'static,
) -> Result<T, CodexStateError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| CodexStateError::Open)?
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn descriptor_states() -> Result<HashMap<i32, DescriptorState>, CodexStateError> {
    let mut states = HashMap::new();
    for entry in std::fs::read_dir(descriptor_directory()).map_err(|_| CodexStateError::Open)? {
        let entry = entry.map_err(|_| CodexStateError::Open)?;
        let Ok(descriptor) = entry.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        let metadata = match std::fs::metadata(entry.path()) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(CodexStateError::Open),
        };
        if !metadata.is_file() {
            continue;
        }
        if let Some(state) = descriptor_state(&entry.path(), &metadata)? {
            states.insert(descriptor, state);
        }
    }
    Ok(states)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn descriptor_state(
    descriptor_path: &Path,
    metadata: &Metadata,
) -> Result<Option<DescriptorState>, CodexStateError> {
    let path = match descriptor_target(descriptor_path) {
        Ok(path) => path,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(CodexStateError::Open),
    };
    Ok(Some(DescriptorState {
        identity: FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        },
        path,
        sqlite_database: descriptor_has_sqlite_header(descriptor_path),
    }))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn connection_database_matches(
    before: &HashMap<i32, DescriptorState>,
    after_open: &HashMap<i32, DescriptorState>,
    after_query: &HashMap<i32, DescriptorState>,
    after_close: &HashMap<i32, DescriptorState>,
    held_descriptor: i32,
    expected: &DescriptorState,
) -> bool {
    let candidates = after_open.iter().filter(|(descriptor, state)| {
        **descriptor != held_descriptor
            && before.get(descriptor) != Some(state)
            && state.sqlite_database
            && after_query
                .get(descriptor)
                .is_some_and(|queried| queried.identity == state.identity)
    });
    let closing_candidates = candidates
        .clone()
        .filter(|(descriptor, state)| {
            after_close
                .get(descriptor)
                .is_none_or(|closed| closed.identity != state.identity)
        })
        .collect::<Vec<_>>();
    if let [(_, opened)] = closing_candidates.as_slice() {
        return *opened == expected;
    }
    if !closing_candidates.is_empty() {
        return false;
    }

    // SQLite may defer closing a Unix descriptor while another connection in this
    // process holds locks on the same inode. In that case no descriptor follows the
    // pool close, so require one unambiguous newly opened exact object instead.
    let mut exact_candidates = candidates.filter(|(_, state)| *state == expected);
    exact_candidates.next().is_some() && exact_candidates.next().is_none()
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn descriptor_has_sqlite_header(path: &Path) -> bool {
    const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";
    let Ok(file) = File::open(path) else {
        return false;
    };
    let mut header = [0_u8; SQLITE_HEADER.len()];
    file.read_at(&mut header, 0)
        .is_ok_and(|read| read == header.len())
        && header == *SQLITE_HEADER
}

#[cfg(target_os = "linux")]
const fn descriptor_directory() -> &'static str {
    "/proc/self/fd"
}

#[cfg(target_os = "macos")]
const fn descriptor_directory() -> &'static str {
    "/dev/fd"
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn descriptor_path(descriptor: i32) -> PathBuf {
    Path::new(descriptor_directory()).join(descriptor.to_string())
}

#[cfg(target_os = "linux")]
fn descriptor_target(path: &Path) -> io::Result<PathBuf> {
    std::fs::read_link(path)
}

#[cfg(target_os = "macos")]
fn descriptor_target(path: &Path) -> io::Result<PathBuf> {
    let descriptor = File::open(path)?;
    let path = rustix::fs::getpath(descriptor).map_err(io::Error::from)?;
    Ok(PathBuf::from(OsString::from_vec(path.into_bytes())))
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

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use std::{collections::HashMap, fs, os::unix::fs::symlink, path::PathBuf, thread};

    use super::{
        DescriptorState, FileIdentity, blocking_state_work, connection_database_matches,
        descriptor_state,
    };

    #[tokio::test]
    async fn descriptor_snapshot_work_runs_off_the_async_worker() {
        let caller = thread::current().id();

        let worker = blocking_state_work(|| Ok(thread::current().id()))
            .await
            .expect("blocking work should complete");

        assert_ne!(worker, caller);
    }

    #[test]
    fn descriptor_disappearing_after_metadata_is_skipped() {
        let fixture = tempfile::tempdir().expect("fixture should exist");
        let target = fixture.path().join("target.sqlite");
        let descriptor = fixture.path().join("descriptor");
        fs::write(&target, b"SQLite format 3\0").expect("target should exist");
        symlink(&target, &descriptor).expect("descriptor link should exist");
        let metadata = fs::metadata(&descriptor).expect("descriptor metadata should exist");
        fs::remove_file(&descriptor).expect("descriptor should disappear");

        let state = descriptor_state(&descriptor, &metadata)
            .expect("a concurrently closed descriptor should not fail the snapshot");

        assert_eq!(state, None);
    }

    #[test]
    fn descriptor_proof_rejects_a_concurrent_open_of_the_held_inode() {
        let database_path = PathBuf::from("/runtime/state_5.sqlite");
        let expected = DescriptorState {
            identity: FileIdentity {
                device: 1,
                inode: 10,
            },
            path: database_path.clone(),
            sqlite_database: true,
        };
        let before = HashMap::new();
        let after_open = HashMap::from([
            (11, expected.clone()),
            (
                12,
                DescriptorState {
                    identity: FileIdentity {
                        device: 1,
                        inode: 20,
                    },
                    path: database_path,
                    sqlite_database: true,
                },
            ),
        ]);
        let after_query = after_open.clone();
        let after_close = HashMap::from([(11, expected.clone())]);

        assert!(!connection_database_matches(
            &before,
            &after_open,
            &after_query,
            &after_close,
            10,
            &expected,
        ));
    }

    #[test]
    fn descriptor_proof_accepts_one_held_database_with_unrelated_opens() {
        let database_path = PathBuf::from("/runtime/state_5.sqlite");
        let expected = DescriptorState {
            identity: FileIdentity {
                device: 1,
                inode: 10,
            },
            path: database_path.clone(),
            sqlite_database: true,
        };
        let before = HashMap::new();
        let after_open = HashMap::from([
            (11, expected.clone()),
            (
                12,
                DescriptorState {
                    identity: FileIdentity {
                        device: 1,
                        inode: 30,
                    },
                    path: PathBuf::from("/runtime/state_5.sqlite-wal"),
                    sqlite_database: false,
                },
            ),
            (
                13,
                DescriptorState {
                    identity: FileIdentity {
                        device: 1,
                        inode: 40,
                    },
                    path: PathBuf::from("/runtime/state_5.sqlite-shm"),
                    sqlite_database: false,
                },
            ),
            (
                14,
                DescriptorState {
                    identity: FileIdentity {
                        device: 2,
                        inode: 50,
                    },
                    path: PathBuf::from("/runtime/watchdog.sqlite"),
                    sqlite_database: true,
                },
            ),
        ]);
        let after_query = after_open.clone();
        let after_close = HashMap::from([(11, expected.clone()), (14, after_open[&14].clone())]);

        assert!(connection_database_matches(
            &before,
            &after_open,
            &after_query,
            &after_close,
            10,
            &expected,
        ));
    }
}
