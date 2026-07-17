use sqlx::Row;
use watchdog_domain::{
    BoundedText, DomainEvent, EventId, ObservationEnvelope, ObservationId, RuntimeKind,
    SessionIdentity,
};

use crate::{
    ActivitySampleRecord, AdapterHealthRecord, DeadlineRecord, FileCursorRecord, InboxOffsetRecord,
    NotificationAttemptRecord, RelationRecord, SnapshotUpdate, StoreError, TerminationSagaRecord,
    WatchdogStore, bounded_json, sqlite_integer,
};

impl WatchdogStore {
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
