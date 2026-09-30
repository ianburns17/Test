package data

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

type ImageVariant struct {
	ID          uuid.UUID `json:"id"`
	ImageID     uuid.UUID `json:"image_id"`
	VariantName string    `json:"variant_name"`
	StoragePath string    `json:"storage_path"`
	MimeType    string    `json:"mime_type"`
	FileSize    int64     `json:"file_size"`
	Width       *int      `json:"width,omitempty"`
	Height      *int      `json:"height,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type VariantModel struct {
	DB *sql.DB
}

// Insert creates a new image variant record (prepared for Week 2 transformations).
func (m VariantModel) Insert(variant *ImageVariant) error {
	if m.DB == nil {
		return errors.New("database connection is not available")
	}
	query := `
		INSERT INTO image_variants (id, image_id, variant_name, storage_path, mime_type, file_size, width, height)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING created_at, updated_at
	`
	if variant.ID == uuid.Nil {
		var err error
		variant.ID, err = NewUUIDv7()
		if err != nil {
			variant.ID = uuid.New()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return m.DB.QueryRowContext(
		ctx,
		query,
		variant.ID,
		variant.ImageID,
		variant.VariantName,
		variant.StoragePath,
		variant.MimeType,
		variant.FileSize,
		variant.Width,
		variant.Height,
	).Scan(&variant.CreatedAt, &variant.UpdatedAt)
}

// GetForImage retrieves all generated variants for an image.
func (m VariantModel) GetForImage(imageID uuid.UUID) ([]*ImageVariant, error) {
	if m.DB == nil {
		return nil, errors.New("database connection is not available")
	}
	query := `
		SELECT id, image_id, variant_name, storage_path, mime_type, file_size, width, height, created_at, updated_at
		FROM image_variants
		WHERE image_id = $1
		ORDER BY created_at ASC
	`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rows, err := m.DB.QueryContext(ctx, query, imageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var variants []*ImageVariant
	for rows.Next() {
		var v ImageVariant
		if err := rows.Scan(
			&v.ID,
			&v.ImageID,
			&v.VariantName,
			&v.StoragePath,
			&v.MimeType,
			&v.FileSize,
			&v.Width,
			&v.Height,
			&v.CreatedAt,
			&v.UpdatedAt,
		); err != nil {
			return nil, err
		}
		variants = append(variants, &v)
	}

	return variants, rows.Err()
}
