CREATE TABLE discovery_aliases (
    alias_runtime TEXT NOT NULL,
    alias_native_id TEXT NOT NULL CHECK (length(alias_native_id) BETWEEN 1 AND 512),
    canonical_session_id TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (alias_runtime, alias_native_id, canonical_session_id),
    FOREIGN KEY (canonical_session_id) REFERENCES sessions(session_id)
) STRICT;

CREATE INDEX discovery_aliases_runtime
    ON discovery_aliases(alias_runtime, alias_native_id);
