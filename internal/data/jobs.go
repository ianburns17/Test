package data

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID           uuid.UUID        `json:"id"`
	PublicID     uuid.UUID        `json:"public_id"`
	ImageID      uuid.UUID        `json:"image_id"`
	JobType      string           `json:"job_type"`
	Status       string           `json:"status"`
	Payload      json.RawMessage  `json:"payload"`
	Result       *json.RawMessage `json:"result,omitempty"`
	ErrorMessage *string          `json:"error_message,omitempty"`
	StartedAt    *time.Time       `json:"started_at,omitempty"`
	CompletedAt  *time.Time       `json:"completed_at,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
}

type JobModel struct {
	DB *sql.DB
}

// Insert creates a new asynchronous job associated with an uploaded image.
// Defaults status to 'queued' and sets public_id to an unguessable UUIDv4.
func (m JobModel) Insert(job *Job) error {
	query := `
		INSERT INTO jobs (id, public_id, image_id, job_type, status, payload)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at, updated_at
	`
	if job.ID == uuid.Nil {
		var err error
		job.ID, err = NewUUIDv7()
		if err != nil {
			job.ID = uuid.New()
		}
	}
	if job.PublicID == uuid.Nil {
		job.PublicID = NewUUIDv4()
	}
	if job.JobType == "" {
		job.JobType = "image_transform"
	}
	if job.Status == "" {
		job.Status = "queued"
	}
	if len(job.Payload) == 0 {
		job.Payload = json.RawMessage("{}")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return m.DB.QueryRowContext(
		ctx,
		query,
		job.ID,
		job.PublicID,
		job.ImageID,
		job.JobType,
		job.Status,
		job.Payload,
	).Scan(&job.CreatedAt, &job.UpdatedAt)
}

// Get retrieves a job by internal UUID.
func (m JobModel) Get(id uuid.UUID) (*Job, error) {
	query := `
		SELECT id, public_id, image_id, job_type, status, payload, result, error_message, started_at, completed_at, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`

	var j Job
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var payloadBytes, resultBytes []byte
	err := m.DB.QueryRowContext(ctx, query, id).Scan(
		&j.ID,
		&j.PublicID,
		&j.ImageID,
		&j.JobType,
		&j.Status,
		&payloadBytes,
		&resultBytes,
		&j.ErrorMessage,
		&j.StartedAt,
		&j.CompletedAt,
		&j.CreatedAt,
		&j.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}

	j.Payload = payloadBytes
	if resultBytes != nil {
		raw := json.RawMessage(resultBytes)
		j.Result = &raw
	}

	return &j, nil
}

// GetByPublicID retrieves a job by client-facing unguessable public_id.
func (m JobModel) GetByPublicID(publicID uuid.UUID) (*Job, error) {
	query := `
		SELECT id, public_id, image_id, job_type, status, payload, result, error_message, started_at, completed_at, created_at, updated_at
		FROM jobs
		WHERE public_id = $1
	`

	var j Job
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var payloadBytes, resultBytes []byte
	err := m.DB.QueryRowContext(ctx, query, publicID).Scan(
		&j.ID,
		&j.PublicID,
		&j.ImageID,
		&j.JobType,
		&j.Status,
		&payloadBytes,
		&resultBytes,
		&j.ErrorMessage,
		&j.StartedAt,
		&j.CompletedAt,
		&j.CreatedAt,
		&j.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}

	j.Payload = payloadBytes
	if resultBytes != nil {
		raw := json.RawMessage(resultBytes)
		j.Result = &raw
	}

	return &j, nil
}

// ClaimNextJob atomically claims the oldest queued job using an atomic SQL CTE with FOR UPDATE SKIP LOCKED.
func (m JobModel) ClaimNextJob(jobType string) (*Job, error) {
	query := `
		WITH next_job AS (
			SELECT id
			FROM jobs
			WHERE status = 'queued' AND ($1 = '' OR job_type = $1)
			ORDER BY created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs j
		SET status = 'processing', started_at = CURRENT_TIMESTAMP
		FROM next_job
		WHERE j.id = next_job.id
		RETURNING j.id, j.public_id, j.image_id, j.job_type, j.status, j.payload, j.result, j.error_message, j.started_at, j.completed_at, j.created_at, j.updated_at
	`

	var j Job
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var payloadBytes, resultBytes []byte
	err := m.DB.QueryRowContext(ctx, query, jobType).Scan(
		&j.ID,
		&j.PublicID,
		&j.ImageID,
		&j.JobType,
		&j.Status,
		&payloadBytes,
		&resultBytes,
		&j.ErrorMessage,
		&j.StartedAt,
		&j.CompletedAt,
		&j.CreatedAt,
		&j.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}

	j.Payload = payloadBytes
	if resultBytes != nil {
		raw := json.RawMessage(resultBytes)
		j.Result = &raw
	}

	return &j, nil
}

// Update updates an existing job row with mutated status, timestamps, and results.
func (m JobModel) Update(j *Job) error {
	query := `
		UPDATE jobs
		SET status = $1, result = $2, error_message = $3, started_at = COALESCE($4, started_at), completed_at = $5
		WHERE id = $6
		RETURNING updated_at
	`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var resultArg any
	if j.Result != nil {
		resultArg = *j.Result
	}

	err := m.DB.QueryRowContext(
		ctx,
		query,
		j.Status,
		resultArg,
		j.ErrorMessage,
		j.StartedAt,
		j.CompletedAt,
		j.ID,
	).Scan(&j.UpdatedAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrRecordNotFound
		}
		return err
	}

	return nil
}

// MarkCompleted records successful completion of a job.
func (m JobModel) MarkCompleted(id uuid.UUID, resultJSON json.RawMessage) error {
	now := time.Now().UTC()
	job := &Job{
		ID:          id,
		Status:      "completed",
		Result:      &resultJSON,
		CompletedAt: &now,
	}
	return m.Update(job)
}

// MarkFailed records a failure for a job.
func (m JobModel) MarkFailed(id uuid.UUID, errorMessage string) error {
	now := time.Now().UTC()
	job := &Job{
		ID:           id,
		Status:       "failed",
		ErrorMessage: &errorMessage,
		CompletedAt:  &now,
	}
	return m.Update(job)
}
