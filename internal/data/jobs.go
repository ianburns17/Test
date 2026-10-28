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
	ID                  uuid.UUID        `json:"id"`
	PublicID            uuid.UUID        `json:"public_id"`
	ImageID             uuid.UUID        `json:"image_id"`
	JobType             string           `json:"job_type"`
	Status              string           `json:"status"`
	Version             int              `json:"version"`
	Stage               string           `json:"stage"`
	VariantsCompleted   int              `json:"variants_completed"`
	SimulatedProgress   string           `json:"simulated_progress,omitempty"`
	Payload             json.RawMessage  `json:"payload"`
	Result              *json.RawMessage `json:"result,omitempty"`
	ErrorMessage        *string          `json:"error_message,omitempty"`
	StartedAt           *time.Time       `json:"started_at,omitempty"`
	CompletedAt         *time.Time       `json:"completed_at,omitempty"`
	TerminalCommittedAt *time.Time       `json:"terminal_committed_at,omitempty"`
	CreatedAt           time.Time        `json:"created_at"`
	UpdatedAt           time.Time        `json:"updated_at"`
}

type JobModel struct {
	DB *sql.DB
}

// Insert creates a new asynchronous job associated with an uploaded image.
// Defaults status to 'queued', version to 1 (VER-01), stage to 'queued', and public_id to an unguessable UUIDv4.
func (m JobModel) Insert(job *Job) error {
	if m.DB == nil {
		return errors.New("database connection is not available")
	}
	query := `
		INSERT INTO jobs (id, public_id, image_id, job_type, status, version, stage, variants_completed, simulated_progress, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING version, stage, variants_completed, created_at, updated_at
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
	if job.Version <= 0 {
		job.Version = 1
	}
	if job.Stage == "" {
		job.Stage = "queued"
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
		job.Version,
		job.Stage,
		job.VariantsCompleted,
		job.SimulatedProgress,
		job.Payload,
	).Scan(&job.Version, &job.Stage, &job.VariantsCompleted, &job.CreatedAt, &job.UpdatedAt)
}

// Get retrieves a job by internal UUID.
func (m JobModel) Get(id uuid.UUID) (*Job, error) {
	if m.DB == nil {
		return nil, errors.New("database connection is not available")
	}
	query := `
		SELECT id, public_id, image_id, job_type, status, version, stage, variants_completed, COALESCE(simulated_progress, ''),
		       payload, result, error_message, started_at, completed_at, terminal_committed_at, created_at, updated_at
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
		&j.Version,
		&j.Stage,
		&j.VariantsCompleted,
		&j.SimulatedProgress,
		&payloadBytes,
		&resultBytes,
		&j.ErrorMessage,
		&j.StartedAt,
		&j.CompletedAt,
		&j.TerminalCommittedAt,
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
	if m.DB == nil {
		return nil, errors.New("database connection is not available")
	}
	query := `
		SELECT id, public_id, image_id, job_type, status, version, stage, variants_completed, COALESCE(simulated_progress, ''),
		       payload, result, error_message, started_at, completed_at, terminal_committed_at, created_at, updated_at
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
		&j.Version,
		&j.Stage,
		&j.VariantsCompleted,
		&j.SimulatedProgress,
		&payloadBytes,
		&resultBytes,
		&j.ErrorMessage,
		&j.StartedAt,
		&j.CompletedAt,
		&j.TerminalCommittedAt,
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
// Transitions status to 'processing', stage to 'starting', and increments version (VER-02).
func (m JobModel) ClaimNextJob(jobType string) (*Job, error) {
	if m.DB == nil {
		return nil, errors.New("database connection is not available")
	}
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
		SET status = 'processing', stage = 'starting', version = j.version + 1, started_at = CURRENT_TIMESTAMP
		FROM next_job
		WHERE j.id = next_job.id
		RETURNING j.id, j.public_id, j.image_id, j.job_type, j.status, j.version, j.stage, j.variants_completed,
		          COALESCE(j.simulated_progress, ''), j.payload, j.result, j.error_message, j.started_at, j.completed_at,
		          j.terminal_committed_at, j.created_at, j.updated_at
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
		&j.Version,
		&j.Stage,
		&j.VariantsCompleted,
		&j.SimulatedProgress,
		&payloadBytes,
		&resultBytes,
		&j.ErrorMessage,
		&j.StartedAt,
		&j.CompletedAt,
		&j.TerminalCommittedAt,
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

// UpdateProgress atomically updates stage, variants_completed, simulated_progress, and increments version (VER-02).
func (m JobModel) UpdateProgress(id uuid.UUID, stage string, variantsCompleted int, simulatedProgress string) (*Job, error) {
	if m.DB == nil {
		return nil, errors.New("database connection is not available")
	}
	query := `
		UPDATE jobs
		SET stage = $1, variants_completed = $2, simulated_progress = $3, version = version + 1
		WHERE id = $4
		RETURNING id, public_id, image_id, job_type, status, version, stage, variants_completed,
		          COALESCE(simulated_progress, ''), payload, result, error_message, started_at, completed_at,
		          terminal_committed_at, created_at, updated_at
	`

	var j Job
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var payloadBytes, resultBytes []byte
	err := m.DB.QueryRowContext(ctx, query, stage, variantsCompleted, simulatedProgress, id).Scan(
		&j.ID,
		&j.PublicID,
		&j.ImageID,
		&j.JobType,
		&j.Status,
		&j.Version,
		&j.Stage,
		&j.VariantsCompleted,
		&j.SimulatedProgress,
		&payloadBytes,
		&resultBytes,
		&j.ErrorMessage,
		&j.StartedAt,
		&j.CompletedAt,
		&j.TerminalCommittedAt,
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
	if m.DB == nil {
		return errors.New("database connection is not available")
	}
	query := `
		UPDATE jobs
		SET status = $1, result = $2, error_message = $3, started_at = COALESCE($4, started_at), completed_at = $5,
		    terminal_committed_at = COALESCE($6, terminal_committed_at), version = version + 1
		WHERE id = $7
		RETURNING version, updated_at
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
		j.TerminalCommittedAt,
		j.ID,
	).Scan(&j.Version, &j.UpdatedAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrRecordNotFound
		}
		return err
	}

	return nil
}

// MarkCompletedWithProgress atomically updates status to 'completed', stage to 'display_ready',
// variants_completed to 3, sets completion timestamps, and increments version (VER-02, PROG-02).
func (m JobModel) MarkCompletedWithProgress(id uuid.UUID, resultJSON json.RawMessage, stage string, variantsCompleted int) (*Job, error) {
	if m.DB == nil {
		return nil, errors.New("database connection is not available")
	}
	now := time.Now().UTC()
	query := `
		UPDATE jobs
		SET status = 'completed', stage = $1, variants_completed = $2, result = $3, completed_at = $4,
		    terminal_committed_at = $4, version = version + 1
		WHERE id = $5
		RETURNING id, public_id, image_id, job_type, status, version, stage, variants_completed,
		          COALESCE(simulated_progress, ''), payload, result, error_message, started_at, completed_at,
		          terminal_committed_at, created_at, updated_at
	`

	var j Job
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var payloadBytes, resultBytes []byte
	err := m.DB.QueryRowContext(ctx, query, stage, variantsCompleted, resultJSON, now, id).Scan(
		&j.ID,
		&j.PublicID,
		&j.ImageID,
		&j.JobType,
		&j.Status,
		&j.Version,
		&j.Stage,
		&j.VariantsCompleted,
		&j.SimulatedProgress,
		&payloadBytes,
		&resultBytes,
		&j.ErrorMessage,
		&j.StartedAt,
		&j.CompletedAt,
		&j.TerminalCommittedAt,
		&j.CreatedAt,
		&j.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	j.Payload = payloadBytes
	if resultBytes != nil {
		raw := json.RawMessage(resultBytes)
		j.Result = &raw
	}
	return &j, nil
}

// MarkCompleted records successful completion of a job.
func (m JobModel) MarkCompleted(id uuid.UUID, resultJSON json.RawMessage) error {
	_, err := m.MarkCompletedWithProgress(id, resultJSON, "display_ready", 3)
	return err
}

// MarkFailed records a failure for a job, setting terminal status and incrementing version (VER-02, PROG-03).
func (m JobModel) MarkFailed(id uuid.UUID, errorMessage string) error {
	if m.DB == nil {
		return errors.New("database connection is not available")
	}
	now := time.Now().UTC()
	query := `
		UPDATE jobs
		SET status = 'failed', error_message = $1, completed_at = $2, terminal_committed_at = $2, version = version + 1
		WHERE id = $3
		RETURNING updated_at
	`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var updatedAt time.Time
	err := m.DB.QueryRowContext(ctx, query, errorMessage, now, id).Scan(&updatedAt)
	return err
}
