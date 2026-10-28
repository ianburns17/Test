DROP INDEX IF EXISTS idx_jobs_version;
ALTER TABLE jobs
    DROP COLUMN IF EXISTS version,
    DROP COLUMN IF EXISTS stage,
    DROP COLUMN IF EXISTS variants_completed,
    DROP COLUMN IF EXISTS simulated_progress,
    DROP COLUMN IF EXISTS terminal_committed_at;
