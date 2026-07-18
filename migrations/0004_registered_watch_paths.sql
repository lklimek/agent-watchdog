CREATE TABLE registered_watch_paths (
    event_key TEXT PRIMARY KEY NOT NULL CHECK (length(event_key) BETWEEN 1 AND 512),
    session_id TEXT NOT NULL,
    root_session_id TEXT NOT NULL,
    native_path TEXT NOT NULL CHECK (length(native_path) BETWEEN 1 AND 4096),
    registered_at_ms INTEGER NOT NULL,
    record_json BLOB NOT NULL CHECK (length(record_json) <= 16384),
    UNIQUE (session_id, native_path),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
    FOREIGN KEY (root_session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX registered_watch_paths_root
    ON registered_watch_paths(root_session_id, session_id);
