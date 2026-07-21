ALTER TABLE adapter_health
    ADD COLUMN version_richness INTEGER NOT NULL DEFAULT 1
    CHECK (version_richness IN (0, 1));
