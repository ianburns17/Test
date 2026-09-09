CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status') THEN
        CREATE TYPE job_status AS ENUM ('queued', 'processing', 'completed', 'failed', 'cancelled');
    END IF;
END $$;

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
  
  v_uuid_bytes := set_byte(v_uuid_bytes, 6, (get_byte(v_rand, 0) & 15 | 112)::int);
  v_uuid_bytes := set_byte(v_uuid_bytes, 7, get_byte(v_rand, 1));
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

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
