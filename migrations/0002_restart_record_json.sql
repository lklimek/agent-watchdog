ALTER TABLE adapter_health
    ADD COLUMN health_json BLOB CHECK (health_json IS NULL OR length(health_json) <= 16384);

ALTER TABLE notification_attempts
    ADD COLUMN attempt_json BLOB CHECK (attempt_json IS NULL OR length(attempt_json) <= 16384);
