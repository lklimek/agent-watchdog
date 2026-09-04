CREATE TABLE relation_events (
    child_session_id TEXT NOT NULL,
    event_fingerprint TEXT NOT NULL,
    relation_json BLOB NOT NULL CHECK (length(relation_json) <= 16384),
    PRIMARY KEY (child_session_id, event_fingerprint),
    FOREIGN KEY (child_session_id) REFERENCES sessions(session_id)
) STRICT;

INSERT OR IGNORE INTO relation_events (child_session_id, event_fingerprint, relation_json)
SELECT child_session_id,
       json_extract(CAST(provenance_json AS TEXT), '$.provenance.fingerprint'),
       provenance_json
FROM session_relations
WHERE json_type(CAST(provenance_json AS TEXT), '$.provenance.fingerprint') = 'text'
ORDER BY valid_from_ms ASC;
