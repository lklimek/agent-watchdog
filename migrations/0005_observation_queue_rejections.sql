CREATE TABLE observation_queue_rejections (
    observation_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL,
    rejected_at_ms INTEGER NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX observation_queue_rejections_session
    ON observation_queue_rejections(session_id);

CREATE TABLE observation_queue_rejection_overflow (
    session_id TEXT PRIMARY KEY NOT NULL,
    rejected_at_ms INTEGER NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
) STRICT;
