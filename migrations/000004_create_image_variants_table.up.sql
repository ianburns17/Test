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
