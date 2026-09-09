-- ==============================================================================
-- EXTENSIONS
-- ==============================================================================
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ==============================================================================
-- CUSTOM ENUM TYPES
-- ==============================================================================
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status') THEN
        CREATE TYPE job_status AS ENUM ('queued', 'processing', 'completed', 'failed', 'cancelled');
    END IF;
END $$;

-- ==============================================================================
-- UUIDv7 GENERATOR FUNCTION (PostgreSQL native time-ordered UUIDv7)
-- ==============================================================================
CREATE OR REPLACE FUNCTION uuid_generate_v7()
RETURNS uuid
AS $$
DECLARE
  v_time timestamp with time zone := clock_timestamp();
  v_unix_time_ms bigint;
  v_rand bytea;
  v_uuid_bytes bytea;
BEGIN
  v_unix_time_ms := (EXTRACT(EPOCH FROM v_time) * 1000)::bigint;
  v_rand := gen_random_bytes(10);
  
  v_uuid_bytes := set_byte(
    set_byte(
      set_byte(
        set_byte(
          set_byte(
            set_byte(
              E'\\x00000000000000000000000000000000'::bytea,
              0, ((v_unix_time_ms >> 40) & 255)::int
            ),
            1, ((v_unix_time_ms >> 32) & 255)::int
          ),
          2, ((v_unix_time_ms >> 24) & 255)::int
        ),
        3, ((v_unix_time_ms >> 16) & 255)::int
      ),
      4, ((v_unix_time_ms >> 8) & 255)::int
    ),
    5, (v_unix_time_ms & 255)::int
  );
  
  -- Version 7: set 4 most significant bits of 7th byte to 0111 (0x70)
  v_uuid_bytes := set_byte(v_uuid_bytes, 6, (get_byte(v_rand, 0) & 15 | 112)::int);
  v_uuid_bytes := set_byte(v_uuid_bytes, 7, get_byte(v_rand, 1));
  
  -- Variant 1: set 2 most significant bits of 9th byte to 10 (0x80)
  v_uuid_bytes := set_byte(v_uuid_bytes, 8, (get_byte(v_rand, 2) & 63 | 128)::int);
  v_uuid_bytes := set_byte(v_uuid_bytes, 9, get_byte(v_rand, 3));
  v_uuid_bytes := set_byte(v_uuid_bytes, 10, get_byte(v_rand, 4));
  v_uuid_bytes := set_byte(v_uuid_bytes, 11, get_byte(v_rand, 5));
  v_uuid_bytes := set_byte(v_uuid_bytes, 12, get_byte(v_rand, 6));
  v_uuid_bytes := set_byte(v_uuid_bytes, 13, get_byte(v_rand, 7));
  v_uuid_bytes := set_byte(v_uuid_bytes, 14, get_byte(v_rand, 8));
  v_uuid_bytes := set_byte(v_uuid_bytes, 15, get_byte(v_rand, 9));
  
  RETURN encode(v_uuid_bytes, 'hex')::uuid;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- ==============================================================================
-- TRIGGER FUNCTION: updated_at
-- ==============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- TABLE 1: images
-- ==============================================================================
CREATE TABLE IF NOT EXISTS images (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    original_filename TEXT NOT NULL,
    storage_path TEXT NOT NULL UNIQUE,
    mime_type TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    width INTEGER,
    height INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_images_created_at ON images(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_images_mime_type ON images(mime_type);

DROP TRIGGER IF EXISTS trg_images_updated_at ON images;
CREATE TRIGGER trg_images_updated_at
BEFORE UPDATE ON images
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- ==============================================================================
-- TABLE 2: jobs (Asynchronous Image Processing Queue & Lifecycle State Machine)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    public_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    image_id UUID NOT NULL REFERENCES images(id) ON DELETE CASCADE,
    job_type TEXT NOT NULL DEFAULT 'image_transform',
    status job_status NOT NULL DEFAULT 'queued',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB,
    error_message TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobs_public_id ON jobs(public_id);
CREATE INDEX IF NOT EXISTS idx_jobs_image_id ON jobs(image_id);
CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);

DROP TRIGGER IF EXISTS trg_jobs_updated_at ON jobs;
CREATE TRIGGER trg_jobs_updated_at
BEFORE UPDATE ON jobs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- ==============================================================================
-- TABLE 3: image_variants (Prepared Variant Store for Week 2 Execution)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS image_variants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    image_id UUID NOT NULL REFERENCES images(id) ON DELETE CASCADE,
    variant_name TEXT NOT NULL,
    storage_path TEXT NOT NULL UNIQUE,
    mime_type TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    width INTEGER,
    height INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_image_variants_image_id ON image_variants(image_id);
CREATE INDEX IF NOT EXISTS idx_image_variants_variant_name ON image_variants(variant_name);

DROP TRIGGER IF EXISTS trg_image_variants_updated_at ON image_variants;
CREATE TRIGGER trg_image_variants_updated_at
BEFORE UPDATE ON image_variants
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
