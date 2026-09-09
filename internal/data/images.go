package data

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Image struct {
	ID               uuid.UUID `json:"id"`
	OriginalFilename string    `json:"original_filename"`
	StoragePath      string    `json:"storage_path"`
	MimeType         string    `json:"mime_type"`
	FileSize         int64     `json:"file_size"`
	Width            *int      `json:"width,omitempty"`
	Height           *int      `json:"height,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type ImageModel struct {
	DB *sql.DB
}

// Insert persists an accepted image record to PostgreSQL.
// Uses client-generated UUIDv7 for time-ordered locality or database default.
func (m ImageModel) Insert(image *Image) error {
	query := `
		INSERT INTO images (id, original_filename, storage_path, mime_type, file_size, width, height)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING created_at, updated_at
	`
	if image.ID == uuid.Nil {
		var err error
		image.ID, err = NewUUIDv7()
		if err != nil {
			image.ID = uuid.New()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return m.DB.QueryRowContext(
		ctx,
		query,
		image.ID,
		image.OriginalFilename,
		image.StoragePath,
		image.MimeType,
		image.FileSize,
		image.Width,
		image.Height,
	).Scan(&image.CreatedAt, &image.UpdatedAt)
}

// Get retrieves an image record by its primary key UUID.
func (m ImageModel) Get(id uuid.UUID) (*Image, error) {
	query := `
		SELECT id, original_filename, storage_path, mime_type, file_size, width, height, created_at, updated_at
		FROM images
		WHERE id = $1
	`

	var img Image
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, id).Scan(
		&img.ID,
		&img.OriginalFilename,
		&img.StoragePath,
		&img.MimeType,
		&img.FileSize,
		&img.Width,
		&img.Height,
		&img.CreatedAt,
		&img.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}

	return &img, nil
}
