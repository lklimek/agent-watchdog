CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('main', 'child')),
    root_session_id TEXT NOT NULL,
    runtime TEXT NOT NULL,
    native_id TEXT NOT NULL CHECK (length(native_id) BETWEEN 1 AND 512),
    title TEXT,
    startup_directory TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (runtime, native_id)
) STRICT;

CREATE TABLE session_relations (
    child_session_id TEXT NOT NULL,
    parent_session_id TEXT NOT NULL,
    root_session_id TEXT NOT NULL,
    selected INTEGER NOT NULL CHECK (selected IN (0, 1)),
    provenance_json BLOB NOT NULL CHECK (length(provenance_json) <= 16384),
    valid_from_ms INTEGER NOT NULL,
    valid_until_ms INTEGER,
    PRIMARY KEY (child_session_id, parent_session_id, valid_from_ms),
    FOREIGN KEY (child_session_id) REFERENCES sessions(session_id),
    FOREIGN KEY (parent_session_id) REFERENCES sessions(session_id)
) STRICT;

CREATE TABLE observations (
    observation_id TEXT PRIMARY KEY NOT NULL,
    runtime TEXT NOT NULL,
    native_id TEXT NOT NULL CHECK (length(native_id) BETWEEN 1 AND 512),
    observed_at_ms INTEGER NOT NULL,
    monotonic_ms INTEGER NOT NULL CHECK (monotonic_ms >= 0),
    envelope_json BLOB NOT NULL CHECK (length(envelope_json) <= 16384)
) STRICT;

CREATE TABLE session_snapshots (
    session_id TEXT PRIMARY KEY NOT NULL,
    root_session_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    detailed_state TEXT NOT NULL,
    compact_state TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    snapshot_json BLOB NOT NULL CHECK (length(snapshot_json) <= 16384),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
) STRICT;

CREATE TABLE state_transitions (
    event_id INTEGER PRIMARY KEY NOT NULL CHECK (event_id >= 0),
    root_session_id TEXT NOT NULL,
    subject_session_id TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    event_json BLOB NOT NULL CHECK (length(event_json) <= 16384),
    FOREIGN KEY (subject_session_id) REFERENCES sessions(session_id)
) STRICT;

CREATE TABLE activity_samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    signal_kind TEXT NOT NULL,
    sample_json BLOB NOT NULL CHECK (length(sample_json) <= 16384),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
) STRICT;

CREATE TABLE file_cursors (
    path_key TEXT PRIMARY KEY NOT NULL,
    device_id INTEGER NOT NULL,
    inode INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL CHECK (byte_offset >= 0),
    complete_record_offset INTEGER NOT NULL CHECK (complete_record_offset >= 0),
    parser_version TEXT NOT NULL,
    last_observation_id TEXT,
    cursor_json BLOB NOT NULL CHECK (length(cursor_json) <= 16384)
) STRICT;

CREATE TABLE deadlines (
    session_id TEXT PRIMARY KEY NOT NULL,
    deadline_ms INTEGER,
    paused_at_ms INTEGER,
    provenance_json BLOB NOT NULL CHECK (length(provenance_json) <= 16384),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
) STRICT;

CREATE TABLE termination_sagas (
    child_session_id TEXT PRIMARY KEY NOT NULL,
    stage TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    next_action_at_ms INTEGER,
    safety_json BLOB NOT NULL CHECK (length(safety_json) <= 16384),
    FOREIGN KEY (child_session_id) REFERENCES sessions(session_id)
) STRICT;

CREATE TABLE outbox (
    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    destination TEXT NOT NULL,
    payload_json BLOB NOT NULL CHECK (length(payload_json) <= 16384),
    delivered_at_ms INTEGER,
    UNIQUE (event_id, destination),
    FOREIGN KEY (event_id) REFERENCES state_transitions(event_id)
) STRICT;

CREATE TABLE inbox_offsets (
    parent_session_id TEXT PRIMARY KEY NOT NULL,
    last_event_id INTEGER NOT NULL CHECK (last_event_id >= 0),
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (parent_session_id) REFERENCES sessions(session_id)
) STRICT;

CREATE TABLE adapter_health (
    adapter TEXT PRIMARY KEY NOT NULL,
    runtime TEXT NOT NULL,
    version TEXT NOT NULL CHECK (length(version) BETWEEN 1 AND 128),
    status TEXT NOT NULL,
    last_success_ms INTEGER,
    last_error_ms INTEGER,
    affected_scope TEXT,
    message TEXT CHECK (message IS NULL OR length(message) <= 2048)
) STRICT;

CREATE TABLE notification_attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    channel TEXT NOT NULL,
    attempted_at_ms INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    message TEXT CHECK (message IS NULL OR length(message) <= 2048),
    FOREIGN KEY (event_id) REFERENCES state_transitions(event_id)
) STRICT;

CREATE INDEX observations_subject_time
    ON observations(runtime, native_id, observed_at_ms);
CREATE INDEX transitions_root_event
    ON state_transitions(root_session_id, event_id);
CREATE INDEX outbox_pending
    ON outbox(delivered_at_ms, outbox_id);
