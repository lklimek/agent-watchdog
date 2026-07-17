use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::Path,
};

use thiserror::Error;

use crate::{CapabilityRoot, PathAccessError};

/// Device and inode identity used to detect replacement and rotation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    /// Construct an identity from trusted filesystem metadata.
    #[must_use]
    pub const fn new(device: u64, inode: u64) -> Self {
        Self { device, inode }
    }

    /// Filesystem device identifier.
    #[must_use]
    pub const fn device(self) -> u64 {
        self.device
    }

    /// Filesystem inode identifier.
    #[must_use]
    pub const fn inode(self) -> u64 {
        self.inode
    }

    fn from_file(file: &File) -> Result<(Self, u64), io::Error> {
        let metadata = file.metadata()?;
        Ok((
            Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            },
            metadata.len(),
        ))
    }
}

/// Persistable incremental cursor with a bounded incomplete record.
#[derive(Clone, Eq, PartialEq)]
pub struct FileCursor {
    identity: FileIdentity,
    read_offset: u64,
    complete_offset: u64,
    partial: Vec<u8>,
    parser_version: u32,
}

impl std::fmt::Debug for FileCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileCursor")
            .field("identity", &self.identity)
            .field("read_offset", &self.read_offset)
            .field("complete_offset", &self.complete_offset)
            .field("partial_bytes", &self.partial.len())
            .field("parser_version", &self.parser_version)
            .finish()
    }
}

impl FileCursor {
    /// Resume safely from a persisted complete-record boundary after restart.
    #[must_use]
    pub const fn resume_from_complete(
        identity: FileIdentity,
        complete_offset: u64,
        parser_version: u32,
    ) -> Self {
        Self {
            identity,
            read_offset: complete_offset,
            complete_offset,
            partial: Vec::new(),
            parser_version,
        }
    }

    /// Device/inode identity at cursor creation.
    #[must_use]
    pub const fn identity(&self) -> FileIdentity {
        self.identity
    }

    /// Next unread file byte.
    #[must_use]
    pub const fn read_offset(&self) -> u64 {
        self.read_offset
    }

    /// End of the last complete emitted record.
    #[must_use]
    pub const fn complete_offset(&self) -> u64 {
        self.complete_offset
    }

    /// Incomplete trailing record retained under the configured cap.
    #[must_use]
    pub fn partial(&self) -> &[u8] {
        &self.partial
    }

    /// Adapter parser version that owns this cursor.
    #[must_use]
    pub const fn parser_version(&self) -> u32 {
        self.parser_version
    }
}

/// Per-read limits for bytes, trailing partial data, and complete records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(
    clippy::struct_field_names,
    reason = "the public budget contract names each independent maximum explicitly"
)]
pub struct ReadBudget {
    max_bytes: usize,
    max_partial_bytes: usize,
    max_records: usize,
}

impl ReadBudget {
    /// Validate non-zero ingestion limits.
    ///
    /// # Errors
    ///
    /// Returns [`ReadBudgetError`] if any limit is zero.
    pub const fn new(
        max_bytes: usize,
        max_partial_bytes: usize,
        max_records: usize,
    ) -> Result<Self, ReadBudgetError> {
        if max_bytes == 0 || max_partial_bytes == 0 || max_records == 0 {
            return Err(ReadBudgetError);
        }
        Ok(Self {
            max_bytes,
            max_partial_bytes,
            max_records,
        })
    }
}

/// Zero-sized ingestion limits cannot make bounded progress.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Incremental read budgets must be positive")]
pub struct ReadBudgetError;

/// Cursor-based reader that never restarts an invalidated file at byte zero.
#[derive(Clone, Copy, Debug)]
pub struct IncrementalReader {
    budget: ReadBudget,
}

impl IncrementalReader {
    /// Construct a reader with validated limits.
    #[must_use]
    pub const fn new(budget: ReadBudget) -> Self {
        Self { budget }
    }

    /// Create a cursor at byte zero for a newly discovered bounded source.
    ///
    /// # Errors
    ///
    /// Returns [`IncrementalReadError`] if the file cannot be opened safely.
    pub fn cursor_at_start(
        &self,
        root: &CapabilityRoot,
        relative: &Path,
        parser_version: u32,
    ) -> Result<FileCursor, IncrementalReadError> {
        Self::cursor(root, relative, parser_version, false)
    }

    /// Create a cursor at EOF so only future appends are read.
    ///
    /// # Errors
    ///
    /// Returns [`IncrementalReadError`] if the file cannot be opened safely.
    pub fn cursor_at_end(
        &self,
        root: &CapabilityRoot,
        relative: &Path,
        parser_version: u32,
    ) -> Result<FileCursor, IncrementalReadError> {
        Self::cursor(root, relative, parser_version, true)
    }

    /// Read one bounded batch of appended complete newline-delimited records.
    ///
    /// # Errors
    ///
    /// Returns [`IncrementalReadError`] for safe-open, seek, or read failures.
    /// Identity/truncation/partial-limit conditions are successful results that
    /// require adapter-specific reconciliation.
    pub fn read(
        &self,
        root: &CapabilityRoot,
        relative: &Path,
        cursor: &FileCursor,
    ) -> Result<ReadOutcome, IncrementalReadError> {
        let mut file = root.open_file(relative)?;
        let (identity, file_len) = FileIdentity::from_file(&file)?;
        if identity != cursor.identity {
            return Ok(ReadOutcome::ReconcileRequired(ReconcileReason::Replaced));
        }
        if file_len < cursor.read_offset {
            return Ok(ReadOutcome::ReconcileRequired(ReconcileReason::Truncated));
        }
        file.seek(SeekFrom::Start(cursor.read_offset))?;
        let mut appended = Vec::new();
        let read_limit = u64::try_from(self.budget.max_bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        file.take(read_limit).read_to_end(&mut appended)?;
        let exceeded_byte_budget = appended.len() > self.budget.max_bytes;
        if exceeded_byte_budget {
            appended.truncate(self.budget.max_bytes);
        }
        let bytes_read = appended.len();
        let previous_partial_len = cursor.partial.len();
        let mut buffer = Vec::with_capacity(previous_partial_len.saturating_add(bytes_read));
        buffer.extend_from_slice(&cursor.partial);
        buffer.extend_from_slice(&appended);

        let mut records = Vec::new();
        let mut record_start = 0_usize;
        let mut consumed = 0_usize;
        for (index, byte) in buffer.iter().enumerate() {
            if *byte != b'\n' {
                continue;
            }
            records.push(buffer[record_start..index].to_vec());
            consumed = index.saturating_add(1);
            record_start = consumed;
            if records.len() == self.budget.max_records {
                break;
            }
        }

        let record_budget_exhausted =
            records.len() == self.budget.max_records && buffer[consumed..].contains(&b'\n');
        let (file_bytes_consumed, partial) = if record_budget_exhausted {
            (consumed.saturating_sub(previous_partial_len), Vec::new())
        } else {
            let partial = buffer[consumed..].to_vec();
            if partial.len() > self.budget.max_partial_bytes {
                return Ok(ReadOutcome::ReconcileRequired(
                    ReconcileReason::PartialRecordTooLarge,
                ));
            }
            (bytes_read, partial)
        };
        let read_offset = cursor
            .read_offset
            .saturating_add(u64::try_from(file_bytes_consumed).unwrap_or(u64::MAX));
        let complete_offset =
            read_offset.saturating_sub(u64::try_from(partial.len()).unwrap_or(u64::MAX));
        let continuation_required =
            exceeded_byte_budget || record_budget_exhausted || read_offset < file_len;
        Ok(ReadOutcome::Records(AppendedRecords {
            records,
            cursor: FileCursor {
                identity,
                read_offset,
                complete_offset,
                partial,
                parser_version: cursor.parser_version,
            },
            bytes_read: u64::try_from(bytes_read).unwrap_or(u64::MAX),
            continuation_required,
        }))
    }

    fn cursor(
        root: &CapabilityRoot,
        relative: &Path,
        parser_version: u32,
        at_end: bool,
    ) -> Result<FileCursor, IncrementalReadError> {
        let file = root.open_file(relative)?;
        let (identity, file_len) = FileIdentity::from_file(&file)?;
        let offset = if at_end { file_len } else { 0 };
        Ok(FileCursor {
            identity,
            read_offset: offset,
            complete_offset: offset,
            partial: Vec::new(),
            parser_version,
        })
    }
}

/// One bounded batch of complete records and its next durable cursor.
#[derive(Clone, Eq, PartialEq)]
pub struct AppendedRecords {
    records: Vec<Vec<u8>>,
    cursor: FileCursor,
    bytes_read: u64,
    continuation_required: bool,
}

impl std::fmt::Debug for AppendedRecords {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppendedRecords")
            .field("record_count", &self.records.len())
            .field("cursor", &self.cursor)
            .field("bytes_read", &self.bytes_read)
            .field("continuation_required", &self.continuation_required)
            .finish()
    }
}

impl AppendedRecords {
    /// Complete records without newline delimiters.
    #[must_use]
    pub fn records(&self) -> &[Vec<u8>] {
        &self.records
    }

    /// Cursor to persist only after the batch is accepted.
    #[must_use]
    pub const fn cursor(&self) -> &FileCursor {
        &self.cursor
    }

    /// File bytes inspected during this bounded read.
    #[must_use]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Whether more file data remains for the bounded scheduler.
    #[must_use]
    pub const fn continuation_required(&self) -> bool {
        self.continuation_required
    }
}

/// Result of one incremental read attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadOutcome {
    /// Complete records and a candidate next cursor.
    Records(AppendedRecords),
    /// The adapter must establish a safe boundary before another read.
    ReconcileRequired(ReconcileReason),
}

/// File conditions that must never trigger an implicit read from byte zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileReason {
    /// Device/inode identity changed.
    Replaced,
    /// Saved byte offset is beyond current EOF.
    Truncated,
    /// Incomplete record exceeded its memory budget.
    PartialRecordTooLarge,
}

/// Safe-open and bounded I/O failures without native record contents.
#[derive(Debug, Error)]
pub enum IncrementalReadError {
    /// Capability policy or kernel containment rejected the path.
    #[error("Incremental source is outside its capability root")]
    Path(#[from] PathAccessError),
    /// Bounded metadata, seek, or read operation failed.
    #[error("Incremental source could not be read")]
    Io(#[from] io::Error),
}
