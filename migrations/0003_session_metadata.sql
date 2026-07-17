ALTER TABLE sessions ADD COLUMN repository_remote TEXT;
ALTER TABLE sessions ADD COLUMN branch TEXT;
ALTER TABLE sessions ADD COLUMN pull_request_number INTEGER CHECK (pull_request_number IS NULL OR pull_request_number > 0);
ALTER TABLE sessions ADD COLUMN pull_request_url TEXT;
