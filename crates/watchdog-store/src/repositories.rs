use sqlx::Row;
use watchdog_domain::{
    BoundedText, ChildSessionId, DomainEvent, EventId, MainSessionId, NativeSessionKey,
    ObservationEnvelope, ObservationId, RuntimeKind, SessionId, SessionIdentity,
};

use crate::{
    ActivitySampleRecord, AdapterHealthRecord, DeadlineRecord, FileCursorRecord, InboxOffsetRecord,
    NotificationAttemptRecord, RegisteredWatchPathRecord, RelationRecord, SessionMetadataRecord,
    SnapshotUpdate, StoreError, StoredSessionRecord, TerminationSagaRecord, WatchdogStore,
    bounded_json, sqlite_integer,
};

const MAX_PERSISTED_QUEUE_REJECTIONS_PER_SESSION: i64 = 64;

impl WatchdogStore {
    /// Persist uncertainty for durable evidence rejected by bounded admission.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the rejection marker cannot be persisted.
    pub async fn save_observation_queue_rejection(
        &self,
        observation: ObservationId,
        session: SessionIdentity,
        rejected_at: watchdog_domain::WallTimeMs,
    ) -> Result<(), StoreError> {
        let session_id = session.session_id().to_string();
        let observation_id = observation.to_string();
        let mut transaction = self.pool.begin().await?;
        let existing_session: Option<String> = sqlx::query_scalar(
            "SELECT session_id FROM observation_queue_rejections WHERE observation_id = ?",
        )
        .bind(&observation_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if existing_session
            .as_deref()
            .is_some_and(|stored| stored != session_id.as_str())
        {
            return Err(StoreError::IdentityMismatch);
        }
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM observation_queue_rejections WHERE session_id = ?",
        )
        .bind(&session_id)
        .fetch_one(&mut *transaction)
        .await?;
        if existing_session.is_none() && count >= MAX_PERSISTED_QUEUE_REJECTIONS_PER_SESSION {
            sqlx::query(
                "INSERT INTO observation_queue_rejection_overflow \
                 (session_id, rejected_at_ms) VALUES (?, ?) \
                 ON CONFLICT(session_id) DO UPDATE SET rejected_at_ms = excluded.rejected_at_ms",
            )
            .bind(&session_id)
            .bind(rejected_at.value())
            .execute(&mut *transaction)
            .await?;
        } else {
            sqlx::query(
                "INSERT INTO observation_queue_rejections \
             (observation_id, session_id, rejected_at_ms) VALUES (?, ?, ?) \
             ON CONFLICT(observation_id) DO UPDATE SET rejected_at_ms = excluded.rejected_at_ms",
            )
            .bind(observation_id)
            .bind(&session_id)
            .bind(rejected_at.value())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Clear one exactly retried rejection and report whether any uncertainty remains.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the marker cannot be cleared or counted atomically.
    pub async fn clear_observation_queue_rejection(
        &self,
        observation: ObservationId,
        session: SessionIdentity,
    ) -> Result<bool, StoreError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "DELETE FROM observation_queue_rejections \
             WHERE observation_id = ? AND session_id = ?",
        )
        .bind(observation.to_string())
        .bind(session.session_id().to_string())
        .execute(&mut *transaction)
        .await?;
        let remaining: i64 = sqlx::query_scalar(
            "SELECT \
             (SELECT COUNT(*) FROM observation_queue_rejections WHERE session_id = ?) + \
             (SELECT COUNT(*) FROM observation_queue_rejection_overflow WHERE session_id = ?)",
        )
        .bind(session.session_id().to_string())
        .bind(session.session_id().to_string())
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(remaining != 0)
    }

    /// Sessions with durable observations rejected and unreconciled.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the rejection markers cannot be queried.
    pub async fn observation_queue_uncertain_sessions(
        &self,
    ) -> Result<Vec<SessionIdentity>, StoreError> {
        let rows = sqlx::query(
            "SELECT sessions.* FROM sessions JOIN ( \
             SELECT session_id FROM observation_queue_rejections \
             UNION SELECT session_id FROM observation_queue_rejection_overflow \
             ) AS uncertain ON uncertain.session_id = sessions.session_id \
             ORDER BY sessions.session_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| decode_session_row(row, None).map(|record| record.session))
            .collect()
    }

    /// Persist one scoped watch path with caller-key idempotency.
    ///
    /// Returns `true` only for a newly inserted session/path pair.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for a missing/mismatched session tree, a reused
    /// event key with different content, or persistence failure.
    pub async fn save_registered_watch_path(
        &self,
        record: &RegisteredWatchPathRecord,
    ) -> Result<bool, StoreError> {
        let payload = bounded_json(record, "registered watch path")?;
        let mut transaction = self.pool.begin().await?;
        let stored_root: Option<String> =
            sqlx::query_scalar("SELECT root_session_id FROM sessions WHERE session_id = ?")
                .bind(record.session().session_id().to_string())
                .fetch_optional(&mut *transaction)
                .await?;
        let expected_root = record.root().session_id().to_string();
        if stored_root.as_deref() != Some(expected_root.as_str()) {
            return Err(StoreError::IdentityMismatch);
        }
        let existing: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT record_json FROM registered_watch_paths WHERE event_key = ?",
        )
        .bind(record.event_key())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(existing) = existing {
            let existing: RegisteredWatchPathRecord = decode_json(&existing)?;
            if existing.session() == record.session()
                && existing.root() == record.root()
                && existing.event_key() == record.event_key()
                && existing.native_path() == record.native_path()
            {
                transaction.commit().await?;
                return Ok(false);
            }
            return Err(StoreError::WatchPathIdentityConflict);
        }
        let result = sqlx::query(
            "INSERT OR IGNORE INTO registered_watch_paths \
             (event_key, session_id, root_session_id, native_path, registered_at_ms, record_json) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(record.event_key())
        .bind(record.session().session_id().to_string())
        .bind(expected_root)
        .bind(record.native_path())
        .bind(record.registered_at().value())
        .bind(payload)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::WatchPathIdentityConflict);
        }
        transaction.commit().await?;
        Ok(true)
    }

    /// Load all bounded registered paths in deterministic order.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for an invalid limit, corrupt record, or database failure.
    pub async fn registered_watch_paths(
        &self,
        limit: u32,
    ) -> Result<Vec<RegisteredWatchPathRecord>, StoreError> {
        validate_limit(limit)?;
        load_json_rows(
            sqlx::query(
                "SELECT record_json FROM registered_watch_paths \
                 ORDER BY registered_at_ms, event_key LIMIT ?",
            )
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?,
            "record_json",
        )
    }

    /// Load bounded paths registered for one session.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for an invalid limit, corrupt record, or database failure.
    pub async fn registered_watch_paths_for(
        &self,
        session: SessionIdentity,
        limit: u32,
    ) -> Result<Vec<RegisteredWatchPathRecord>, StoreError> {
        validate_limit(limit)?;
        load_json_rows(
            sqlx::query(
                "SELECT record_json FROM registered_watch_paths WHERE session_id = ? \
                 ORDER BY registered_at_ms, event_key LIMIT ?",
            )
            .bind(session.session_id().to_string())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?,
            "record_json",
        )
    }

    /// Upsert bounded operator-facing metadata for an existing session.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for a missing session, numeric overflow, or
    /// `SQLite` failure.
    pub async fn save_session_metadata(
        &self,
        record: &SessionMetadataRecord,
    ) -> Result<(), StoreError> {
        let pull_request_number = record
            .pull_request_number()
            .map(|value| sqlite_integer("pull request number", value))
            .transpose()?;
        let result = sqlx::query(
            "UPDATE sessions SET title = ?, startup_directory = ?, repository_remote = ?, \
             branch = ?, pull_request_number = ?, pull_request_url = ?, updated_at_ms = ? \
             WHERE session_id = ?",
        )
        .bind(record.title())
        .bind(record.startup_directory())
        .bind(record.repository_remote())
        .bind(record.branch())
        .bind(pull_request_number)
        .bind(record.pull_request_url())
        .bind(record.updated_at().value())
        .bind(record.session().session_id().to_string())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::CorruptValue("metadata session is missing"));
        }
        Ok(())
    }

    /// Load bounded operator-facing metadata for an existing session.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt metadata or `SQLite` failure.
    pub async fn session_metadata(
        &self,
        session: SessionIdentity,
    ) -> Result<Option<SessionMetadataRecord>, StoreError> {
        let row = sqlx::query(
            "SELECT title, startup_directory, repository_remote, branch, \
             pull_request_number, pull_request_url, updated_at_ms FROM sessions WHERE session_id = ?",
        )
        .bind(session.session_id().to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| decode_session_metadata(&row, session))
            .transpose()
    }

    /// Load bounded metadata for nonterminal child sessions in one snapshot query.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for an invalid limit, corrupt identity/metadata,
    /// or `SQLite` failure.
    pub async fn active_child_metadata(
        &self,
        limit: u32,
    ) -> Result<Vec<SessionMetadataRecord>, StoreError> {
        validate_limit(limit)?;
        let rows = sqlx::query(
            "SELECT s.session_id, s.kind, s.root_session_id, s.runtime, s.native_id, \
             s.title, s.startup_directory, s.repository_remote, s.branch, \
             s.pull_request_number, s.pull_request_url, s.updated_at_ms \
             FROM sessions s JOIN session_snapshots ss ON ss.session_id = s.session_id \
             WHERE s.kind = 'child' AND s.startup_directory IS NOT NULL \
             AND ss.detailed_state NOT IN ('completed', 'failed', 'cancelled', 'disappeared') \
             ORDER BY s.created_at_ms, s.session_id LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let session = decode_session_row(row, None)?.session;
                decode_session_metadata(row, session)
            })
            .collect()
    }

    /// Load stable native identity and tree placement for one session.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt stored identities or `SQLite` failure.
    pub async fn session(
        &self,
        session: SessionIdentity,
    ) -> Result<Option<StoredSessionRecord>, StoreError> {
        let row = sqlx::query(
            "SELECT session_id, kind, root_session_id, runtime, native_id FROM sessions WHERE session_id = ?",
        )
        .bind(session.session_id().to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| decode_session_row(&row, Some(session)))
            .transpose()
    }

    /// Load stable native identity and role for a runtime-neutral session ID.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt stored identities or `SQLite` failure.
    pub async fn session_by_id(
        &self,
        session_id: SessionId,
    ) -> Result<Option<StoredSessionRecord>, StoreError> {
        let row = sqlx::query(
            "SELECT session_id, kind, root_session_id, runtime, native_id FROM sessions WHERE session_id = ?",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| decode_session_row(&row, None)).transpose()
    }

    /// Load a bounded stable session list for one main-session tree.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid limits, corrupt stored identities, or
    /// `SQLite` failure.
    pub async fn sessions_for_root(
        &self,
        root: MainSessionId,
        limit: u32,
    ) -> Result<Vec<StoredSessionRecord>, StoreError> {
        validate_limit(limit)?;
        let rows = sqlx::query(
            "SELECT session_id, kind, root_session_id, runtime, native_id FROM sessions \
             WHERE root_session_id = ? ORDER BY created_at_ms, session_id LIMIT ?",
        )
        .bind(root.session_id().to_string())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| decode_session_row(row, None))
            .collect()
    }

    /// Load a bounded stable list for one role across all trees.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid limits, corrupt identities, or
    /// `SQLite` failure.
    pub async fn sessions_by_kind(
        &self,
        kind: watchdog_domain::SessionKind,
        limit: u32,
    ) -> Result<Vec<StoredSessionRecord>, StoreError> {
        validate_limit(limit)?;
        let kind = match kind {
            watchdog_domain::SessionKind::Main => "main",
            watchdog_domain::SessionKind::Child => "child",
        };
        let rows = sqlx::query(
            "SELECT session_id, kind, root_session_id, runtime, native_id FROM sessions \
             WHERE kind = ? ORDER BY created_at_ms, session_id LIMIT ?",
        )
        .bind(kind)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| decode_session_row(row, None))
            .collect()
    }

    /// Load one idempotent observation envelope.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt data or `SQLite` failure.
    pub async fn observation(
        &self,
        observation_id: ObservationId,
    ) -> Result<Option<ObservationEnvelope>, StoreError> {
        load_optional_json(
            sqlx::query("SELECT envelope_json FROM observations WHERE observation_id = ?")
                .bind(observation_id.to_string())
                .fetch_optional(&self.pool)
                .await?,
            "envelope_json",
        )
    }

    /// Load the current reducer snapshot for one session.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt data or `SQLite` failure.
    pub async fn snapshot(
        &self,
        session: SessionIdentity,
    ) -> Result<Option<SnapshotUpdate>, StoreError> {
        load_optional_json(
            sqlx::query("SELECT snapshot_json FROM session_snapshots WHERE session_id = ?")
                .bind(session.session_id().to_string())
                .fetch_optional(&self.pool)
                .await?,
            "snapshot_json",
        )
    }

    /// Load durable events after an exclusive event cursor.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid bounds, corrupt data, or `SQLite`
    /// failure.
    pub async fn events_after(
        &self,
        root: watchdog_domain::MainSessionId,
        after: EventId,
        limit: u32,
    ) -> Result<Vec<DomainEvent>, StoreError> {
        validate_limit(limit)?;
        load_json_rows(
            sqlx::query(
                "SELECT event_json FROM state_transitions \
                 WHERE root_session_id = ? AND event_id > ? ORDER BY event_id LIMIT ?",
            )
            .bind(root.session_id().to_string())
            .bind(sqlite_integer("event cursor", after.value())?)
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?,
            "event_json",
        )
    }

    /// Insert or update a bounded hierarchy candidate.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when identities are missing or `SQLite` fails.
    pub async fn save_relation(&self, record: &RelationRecord) -> Result<(), StoreError> {
        let payload = bounded_json(record, "session relation")?;
        sqlx::query(
            "INSERT INTO session_relations \
             (child_session_id, parent_session_id, root_session_id, selected, provenance_json, valid_from_ms, valid_until_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(child_session_id, parent_session_id, valid_from_ms) DO UPDATE SET \
             selected = excluded.selected, provenance_json = excluded.provenance_json, \
             valid_until_ms = excluded.valid_until_ms",
        )
        .bind(record.child.session_id().to_string())
        .bind(record.parent.session_id().to_string())
        .bind(record.root.session_id().to_string())
        .bind(record.selected)
        .bind(payload)
        .bind(record.valid_from.value())
        .bind(record.valid_until.map(watchdog_domain::WallTimeMs::value))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Select one hierarchy relation, superseding a prior selected parent in a
    /// single transaction.
    ///
    /// Repeating the same selected relation is idempotent and returns `false`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when identities are missing, serialization is
    /// oversized, or the transaction fails.
    pub async fn select_relation(&self, record: &RelationRecord) -> Result<bool, StoreError> {
        let payload = bounded_json(record, "session relation")?;
        let mut transaction = self.pool.begin().await?;
        let current: Option<String> = sqlx::query_scalar(
            "SELECT parent_session_id FROM session_relations \
             WHERE child_session_id = ? AND selected = 1 AND valid_until_ms IS NULL \
             ORDER BY valid_from_ms DESC LIMIT 1",
        )
        .bind(record.child.session_id().to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let selected_parent = record.parent.session_id().to_string();
        if current.as_deref() == Some(selected_parent.as_str()) {
            transaction.commit().await?;
            return Ok(false);
        }
        sqlx::query(
            "UPDATE session_relations SET selected = 0, valid_until_ms = ? \
             WHERE child_session_id = ? AND selected = 1 AND valid_until_ms IS NULL",
        )
        .bind(record.valid_from.value())
        .bind(record.child.session_id().to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO session_relations \
             (child_session_id, parent_session_id, root_session_id, selected, provenance_json, valid_from_ms, valid_until_ms) \
             VALUES (?, ?, ?, 1, ?, ?, NULL) \
             ON CONFLICT(child_session_id, parent_session_id, valid_from_ms) DO UPDATE SET \
             selected = 1, provenance_json = excluded.provenance_json, valid_until_ms = NULL",
        )
        .bind(record.child.session_id().to_string())
        .bind(record.parent.session_id().to_string())
        .bind(record.root.session_id().to_string())
        .bind(payload)
        .bind(record.valid_from.value())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// Load hierarchy candidates for one main-session tree.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid limits, corrupt records, or `SQLite`
    /// failures.
    pub async fn relations_for_root(
        &self,
        root: watchdog_domain::MainSessionId,
        limit: u32,
    ) -> Result<Vec<RelationRecord>, StoreError> {
        validate_limit(limit)?;
        load_json_rows(
            sqlx::query(
                "SELECT provenance_json FROM session_relations \
                 WHERE root_session_id = ? ORDER BY valid_from_ms DESC LIMIT ?",
            )
            .bind(root.session_id().to_string())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?,
            "provenance_json",
        )
    }

    /// Append one bounded attributable activity sample.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the session is missing or persistence fails.
    pub async fn append_activity(&self, record: &ActivitySampleRecord) -> Result<i64, StoreError> {
        let payload = bounded_json(record, "activity sample")?;
        let result = sqlx::query(
            "INSERT INTO activity_samples \
             (session_id, observed_at_ms, signal_kind, sample_json) VALUES (?, ?, ?, ?)",
        )
        .bind(record.session.session_id().to_string())
        .bind(record.observed_at.value())
        .bind(record.evidence.kind())
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    /// Replace the latest diagnostic sample of one kind for a session.
    ///
    /// This keeps frequently sampled process diagnostics bounded without
    /// retaining a time series that v1 does not expose.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the session is missing or the transaction fails.
    pub async fn save_latest_activity(
        &self,
        record: &ActivitySampleRecord,
    ) -> Result<(), StoreError> {
        let payload = bounded_json(record, "activity sample")?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query("DELETE FROM activity_samples WHERE session_id = ? AND signal_kind = ?")
            .bind(record.session.session_id().to_string())
            .bind(record.evidence.kind())
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "INSERT INTO activity_samples \
             (session_id, observed_at_ms, signal_kind, sample_json) VALUES (?, ?, ?, ?)",
        )
        .bind(record.session.session_id().to_string())
        .bind(record.observed_at.value())
        .bind(record.evidence.kind())
        .bind(payload)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Load recent activity newest first.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid limits, corrupt records, or `SQLite`
    /// failures.
    pub async fn recent_activity(
        &self,
        session: SessionIdentity,
        limit: u32,
    ) -> Result<Vec<ActivitySampleRecord>, StoreError> {
        validate_limit(limit)?;
        load_json_rows(
            sqlx::query(
                "SELECT sample_json FROM activity_samples WHERE session_id = ? \
                 ORDER BY observed_at_ms DESC, sample_id DESC LIMIT ?",
            )
            .bind(session.session_id().to_string())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?,
            "sample_json",
        )
    }

    /// Upsert one incremental file cursor.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for integer overflow or persistence failure.
    pub async fn save_file_cursor(&self, record: &FileCursorRecord) -> Result<(), StoreError> {
        let payload = bounded_json(record, "file cursor")?;
        sqlx::query(
            "INSERT INTO file_cursors \
             (path_key, device_id, inode, byte_offset, complete_record_offset, parser_version, last_observation_id, cursor_json) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(path_key) DO UPDATE SET \
             device_id = excluded.device_id, inode = excluded.inode, \
             byte_offset = excluded.byte_offset, complete_record_offset = excluded.complete_record_offset, \
             parser_version = excluded.parser_version, last_observation_id = excluded.last_observation_id, \
             cursor_json = excluded.cursor_json",
        )
        .bind(record.path_key().as_str())
        .bind(sqlite_integer("cursor device ID", record.device_id())?)
        .bind(sqlite_integer("cursor inode", record.inode())?)
        .bind(sqlite_integer("cursor byte offset", record.byte_offset())?)
        .bind(sqlite_integer(
            "cursor complete record offset",
            record.complete_record_offset(),
        )?)
        .bind(record.parser_version().as_str())
        .bind(record.last_observation_id().map(|value| value.to_string()))
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Load one incremental file cursor.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt data or `SQLite` failure.
    pub async fn file_cursor(
        &self,
        path_key: &BoundedText<4_096>,
    ) -> Result<Option<FileCursorRecord>, StoreError> {
        load_optional_json(
            sqlx::query("SELECT cursor_json FROM file_cursors WHERE path_key = ?")
                .bind(path_key.as_str())
                .fetch_optional(&self.pool)
                .await?,
            "cursor_json",
        )
    }

    /// Upsert a parent-provided deadline.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the session is missing or persistence fails.
    pub async fn save_deadline(&self, record: &DeadlineRecord) -> Result<(), StoreError> {
        let payload = bounded_json(record, "deadline")?;
        sqlx::query(
            "INSERT INTO deadlines (session_id, deadline_ms, paused_at_ms, provenance_json) \
             VALUES (?, ?, ?, ?) ON CONFLICT(session_id) DO UPDATE SET \
             deadline_ms = excluded.deadline_ms, paused_at_ms = excluded.paused_at_ms, \
             provenance_json = excluded.provenance_json",
        )
        .bind(record.session.session_id().to_string())
        .bind(record.deadline.map(watchdog_domain::WallTimeMs::value))
        .bind(record.paused_at.map(watchdog_domain::WallTimeMs::value))
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Load a persisted deadline.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt data or `SQLite` failure.
    pub async fn deadline(
        &self,
        session: SessionIdentity,
    ) -> Result<Option<DeadlineRecord>, StoreError> {
        load_optional_json(
            sqlx::query("SELECT provenance_json FROM deadlines WHERE session_id = ?")
                .bind(session.session_id().to_string())
                .fetch_optional(&self.pool)
                .await?,
            "provenance_json",
        )
    }

    /// Upsert one child-only termination saga.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for stale/invalid revision or persistence failure.
    pub async fn save_termination_saga(
        &self,
        record: &TerminationSagaRecord,
    ) -> Result<(), StoreError> {
        let revision =
            crate::positive_sqlite_integer("termination saga revision", record.revision)?;
        let payload = bounded_json(record, "termination saga")?;
        let result = sqlx::query(
            "INSERT INTO termination_sagas \
             (child_session_id, stage, revision, next_action_at_ms, safety_json) \
             VALUES (?, ?, ?, ?, ?) ON CONFLICT(child_session_id) DO UPDATE SET \
             stage = excluded.stage, revision = excluded.revision, \
             next_action_at_ms = excluded.next_action_at_ms, safety_json = excluded.safety_json \
             WHERE excluded.revision > termination_sagas.revision",
        )
        .bind(record.child.session_id().to_string())
        .bind(record.stage.as_str())
        .bind(revision)
        .bind(
            record
                .next_action_at
                .map(watchdog_domain::WallTimeMs::value),
        )
        .bind(payload)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::StaleSnapshot);
        }
        Ok(())
    }

    /// Load one child-only termination saga.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt data or `SQLite` failure.
    pub async fn termination_saga(
        &self,
        child: watchdog_domain::ChildSessionId,
    ) -> Result<Option<TerminationSagaRecord>, StoreError> {
        load_optional_json(
            sqlx::query("SELECT safety_json FROM termination_sagas WHERE child_session_id = ?")
                .bind(child.session_id().to_string())
                .fetch_optional(&self.pool)
                .await?,
            "safety_json",
        )
    }

    /// Advance a parent inbox cursor monotonically.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for integer overflow or persistence failure.
    pub async fn save_inbox_offset(&self, record: InboxOffsetRecord) -> Result<bool, StoreError> {
        let event_id = sqlite_integer("inbox event ID", record.last_event_id.value())?;
        let result = sqlx::query(
            "INSERT INTO inbox_offsets (parent_session_id, last_event_id, updated_at_ms) \
             VALUES (?, ?, ?) ON CONFLICT(parent_session_id) DO UPDATE SET \
             last_event_id = excluded.last_event_id, updated_at_ms = excluded.updated_at_ms \
             WHERE excluded.last_event_id > inbox_offsets.last_event_id",
        )
        .bind(record.parent.session_id().to_string())
        .bind(event_id)
        .bind(record.updated_at.value())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Load one parent inbox cursor.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt data or `SQLite` failure.
    pub async fn inbox_offset(
        &self,
        parent: watchdog_domain::MainSessionId,
    ) -> Result<Option<InboxOffsetRecord>, StoreError> {
        let Some(row) = sqlx::query(
            "SELECT last_event_id, updated_at_ms FROM inbox_offsets WHERE parent_session_id = ?",
        )
        .bind(parent.session_id().to_string())
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let raw_event_id: i64 = row.try_get("last_event_id")?;
        Ok(Some(InboxOffsetRecord {
            parent,
            last_event_id: EventId::new(
                u64::try_from(raw_event_id)
                    .map_err(|_| StoreError::CorruptValue("negative inbox event ID"))?,
            ),
            updated_at: watchdog_domain::WallTimeMs::new(row.try_get("updated_at_ms")?),
        }))
    }

    /// Upsert one runtime adapter's bounded health record.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for oversized serialization or persistence failure.
    pub async fn save_adapter_health(
        &self,
        record: &AdapterHealthRecord,
    ) -> Result<(), StoreError> {
        let payload = bounded_json(record, "adapter health")?;
        sqlx::query(
            "INSERT INTO adapter_health \
             (adapter, runtime, version, status, last_success_ms, last_error_ms, affected_scope, message, health_json) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(adapter) DO UPDATE SET \
             version = excluded.version, status = excluded.status, \
             last_success_ms = excluded.last_success_ms, last_error_ms = excluded.last_error_ms, \
             affected_scope = excluded.affected_scope, message = excluded.message, health_json = excluded.health_json",
        )
        .bind(record.adapter.runtime().as_str())
        .bind(record.adapter.runtime().as_str())
        .bind(record.adapter.version())
        .bind(record.status.as_str())
        .bind(record.last_success.map(watchdog_domain::WallTimeMs::value))
        .bind(record.last_error.map(watchdog_domain::WallTimeMs::value))
        .bind(record.affected_scope.as_ref().map(BoundedText::as_str))
        .bind(record.message.as_ref().map(BoundedText::as_str))
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Load one runtime adapter health record.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for corrupt data or `SQLite` failure.
    pub async fn adapter_health(
        &self,
        runtime: RuntimeKind,
    ) -> Result<Option<AdapterHealthRecord>, StoreError> {
        load_optional_json(
            sqlx::query("SELECT health_json FROM adapter_health WHERE adapter = ?")
                .bind(runtime.as_str())
                .fetch_optional(&self.pool)
                .await?,
            "health_json",
        )
    }

    /// Record one terminal human-notification attempt without retry state.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for integer overflow or persistence failure.
    pub async fn record_notification_attempt(
        &self,
        record: &NotificationAttemptRecord,
    ) -> Result<i64, StoreError> {
        let payload = bounded_json(record, "notification attempt")?;
        let result = sqlx::query(
            "INSERT INTO notification_attempts \
             (event_id, channel, attempted_at_ms, outcome, message, attempt_json) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(sqlite_integer(
            "notification event ID",
            record.event_id.value(),
        )?)
        .bind(record.channel.as_str())
        .bind(record.attempted_at.value())
        .bind(record.outcome.as_str())
        .bind(record.message.as_ref().map(BoundedText::as_str))
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    /// Load notification attempts for one event, newest first.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid limits, corrupt data, or `SQLite`
    /// failure.
    pub async fn notification_attempts(
        &self,
        event_id: EventId,
        limit: u32,
    ) -> Result<Vec<NotificationAttemptRecord>, StoreError> {
        validate_limit(limit)?;
        load_json_rows(
            sqlx::query(
                "SELECT attempt_json FROM notification_attempts WHERE event_id = ? \
                 ORDER BY attempt_id DESC LIMIT ?",
            )
            .bind(sqlite_integer("notification event ID", event_id.value())?)
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?,
            "attempt_json",
        )
    }
}

fn validate_limit(limit: u32) -> Result<(), StoreError> {
    if limit == 0 {
        return Err(crate::StoreInputError::ZeroLimit.into());
    }
    Ok(())
}

fn decode_session_row(
    row: &sqlx::sqlite::SqliteRow,
    expected: Option<SessionIdentity>,
) -> Result<StoredSessionRecord, StoreError> {
    let runtime = match row.try_get::<String, _>("runtime")?.as_str() {
        "claude_code" => RuntimeKind::ClaudeCode,
        "codex_cli" => RuntimeKind::CodexCli,
        "codex_companion" => RuntimeKind::CodexCompanion,
        "opencode" => RuntimeKind::OpenCode,
        _ => return Err(StoreError::CorruptValue("unknown session runtime")),
    };
    let native = NativeSessionKey::new(runtime, row.try_get::<String, _>("native_id")?)
        .map_err(|_| StoreError::CorruptValue("invalid native session identity"))?;
    let derived = SessionId::from_native(&native);
    let stored_id: SessionId = serde_json::from_value(serde_json::Value::String(
        row.try_get::<String, _>("session_id")?,
    ))?;
    if stored_id != derived {
        return Err(StoreError::CorruptValue(
            "stored and derived session identities differ",
        ));
    }
    let session = match row.try_get::<String, _>("kind")?.as_str() {
        "main" => SessionIdentity::Main(MainSessionId::from(derived)),
        "child" => SessionIdentity::Child(ChildSessionId::from(derived)),
        _ => return Err(StoreError::CorruptValue("unknown session kind")),
    };
    if expected.is_some_and(|value| value != session) {
        return Err(StoreError::CorruptValue("stored session role differs"));
    }
    let root_id: SessionId = serde_json::from_value(serde_json::Value::String(
        row.try_get::<String, _>("root_session_id")?,
    ))?;
    Ok(StoredSessionRecord {
        session,
        root: MainSessionId::from(root_id),
        native,
    })
}

fn decode_session_metadata(
    row: &sqlx::sqlite::SqliteRow,
    session: SessionIdentity,
) -> Result<SessionMetadataRecord, StoreError> {
    let pull_request_number: Option<i64> = row.try_get("pull_request_number")?;
    let pull_request_number = pull_request_number
        .map(|value| {
            u64::try_from(value)
                .map_err(|_| StoreError::CorruptValue("invalid pull request number"))
        })
        .transpose()?;
    SessionMetadataRecord::new(
        session,
        row.try_get("title")?,
        row.try_get("startup_directory")?,
        row.try_get("repository_remote")?,
        row.try_get("branch")?,
        pull_request_number,
        row.try_get("pull_request_url")?,
        watchdog_domain::WallTimeMs::new(row.try_get("updated_at_ms")?),
    )
    .map_err(|_| StoreError::CorruptValue("invalid session metadata"))
}

fn load_json_rows<T: serde::de::DeserializeOwned>(
    rows: Vec<sqlx::sqlite::SqliteRow>,
    column: &str,
) -> Result<Vec<T>, StoreError> {
    rows.into_iter()
        .map(|row| {
            let payload: Vec<u8> = row.try_get(column)?;
            decode_json(&payload)
        })
        .collect()
}

fn load_optional_json<T: serde::de::DeserializeOwned>(
    row: Option<sqlx::sqlite::SqliteRow>,
    column: &str,
) -> Result<Option<T>, StoreError> {
    row.map(|row| {
        let payload: Vec<u8> = row.try_get(column)?;
        decode_json(&payload)
    })
    .transpose()
}

fn decode_json<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, StoreError> {
    if payload.len() > crate::MAX_RECORD_BYTES {
        return Err(StoreError::RecordTooLarge {
            record: "stored record",
        });
    }
    Ok(serde_json::from_slice(payload)?)
}
