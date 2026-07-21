ALTER TABLE inbox_offsets RENAME TO inbox_offsets_legacy;

CREATE TABLE inbox_offsets (
    parent_session_id TEXT PRIMARY KEY NOT NULL,
    last_event_id INTEGER NOT NULL CHECK (last_event_id >= 0),
    last_delivered_event_id INTEGER NOT NULL
        CHECK (last_delivered_event_id >= last_event_id),
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (parent_session_id) REFERENCES sessions(session_id)
) STRICT;

INSERT INTO inbox_offsets (
    parent_session_id,
    last_event_id,
    last_delivered_event_id,
    updated_at_ms
)
SELECT parent_session_id, last_event_id, last_event_id, updated_at_ms
FROM inbox_offsets_legacy;

DROP TABLE inbox_offsets_legacy;
