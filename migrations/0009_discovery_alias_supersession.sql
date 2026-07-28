DELETE FROM discovery_aliases
WHERE NOT EXISTS (
    SELECT 1 FROM sessions
    WHERE sessions.session_id = discovery_aliases.canonical_session_id
      AND sessions.kind = 'main'
      AND sessions.root_session_id = sessions.session_id
);

DELETE FROM discovery_aliases
WHERE rowid NOT IN (
    SELECT rowid FROM (
        SELECT rowid, ROW_NUMBER() OVER (
            PARTITION BY alias_runtime, alias_native_id
            ORDER BY observed_at_ms DESC, rowid DESC
        ) AS recency
        FROM discovery_aliases
    )
    WHERE recency = 1
);

DROP INDEX discovery_aliases_runtime;

CREATE UNIQUE INDEX discovery_aliases_key
    ON discovery_aliases(alias_runtime, alias_native_id);
