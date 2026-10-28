-- Assessment 2: Versioned long polling and progress observation schema extension
ALTER TABLE jobs 
    ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS stage TEXT NOT NULL DEFAULT 'queued',
    ADD COLUMN IF NOT EXISTS variants_completed INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS simulated_progress TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS terminal_committed_at TIMESTAMPTZ;

-- Backfill existing jobs with valid initial version (VER-01)
UPDATE jobs SET version = 1 WHERE version IS NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_version ON jobs(version);
