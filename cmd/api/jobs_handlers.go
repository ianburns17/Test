package main

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"image-api/internal/data"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

// fetchJob retrieves a job record by either client-facing public_id (UUIDv4) or internal id (UUIDv7).
func (a *applicationDependencies) fetchJob(parsedUUID uuid.UUID) (*data.Job, error) {
	job, err := a.models.Jobs.GetByPublicID(parsedUUID)
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			return a.models.Jobs.Get(parsedUUID)
		}
		return nil, err
	}
	return job, nil
}

// buildJobResponse constructs the complete JSON snapshot representation for both short and long polling (API-03).
func (a *applicationDependencies) buildJobResponse(job *data.Job) envelope {
	type variantItem struct {
		Name   string `json:"name"`
		Width  int    `json:"width"`
		Height int    `json:"height"`
		URL    string `json:"url"`
		Size   int64  `json:"size,omitempty"`
	}

	var variantList []variantItem
	if job.Status == "completed" && a.models.Variants.DB != nil {
		dbVariants, err := a.models.Variants.GetForImage(job.ImageID)
		if err == nil {
			for _, v := range dbVariants {
				w := 0
				h := 0
				if v.Width != nil {
					w = *v.Width
				}
				if v.Height != nil {
					h = *v.Height
				}
				variantList = append(variantList, variantItem{
					Name:   v.VariantName,
					Width:  w,
					Height: h,
					URL:    fmt.Sprintf("/v1/images/%s/variants/%s", job.ImageID.String(), v.VariantName),
					Size:   v.FileSize,
				})
			}
		}
	}

	resp := envelope{
		"id":                 job.PublicID,
		"public_id":          job.PublicID,
		"image_id":           job.ImageID,
		"job_type":           job.JobType,
		"status":             job.Status,
		"version":            job.Version,
		"stage":              job.Stage,
		"variants_completed": job.VariantsCompleted,
		"queued_at":          job.CreatedAt,
		"started_at":         job.StartedAt,
		"completed_at":       job.CompletedAt,
		"error_message":      job.ErrorMessage,
	}

	if job.SimulatedProgress != "" {
		resp["simulated_progress"] = job.SimulatedProgress
	}
	if job.TerminalCommittedAt != nil {
		resp["terminal_committed_at"] = job.TerminalCommittedAt
	}
	if len(variantList) > 0 {
		resp["variants"] = variantList
	}

	// Nested job envelope for backward compatibility with UI and test fixtures
	jobEnvelope := envelope{
		"id":                 job.PublicID,
		"internal_id":        job.ID,
		"public_id":          job.PublicID,
		"image_id":           job.ImageID,
		"job_type":           job.JobType,
		"status":             job.Status,
		"version":            job.Version,
		"stage":              job.Stage,
		"variants_completed": job.VariantsCompleted,
		"queued_at":          job.CreatedAt,
		"started_at":         job.StartedAt,
		"completed_at":       job.CompletedAt,
		"error_message":      job.ErrorMessage,
		"variants":           variantList,
	}
	if job.SimulatedProgress != "" {
		jobEnvelope["simulated_progress"] = job.SimulatedProgress
	}
	if job.TerminalCommittedAt != nil {
		jobEnvelope["terminal_committed_at"] = job.TerminalCommittedAt
	}
	resp["job"] = jobEnvelope

	return resp
}

// getJobHandler implements the HTTP observation contract for both prompt snapshots and versioned long polling (API-01, API-02, API-03).
// Routes:
// - Prompt snapshot: GET /v1/jobs/{id}
// - Versioned long poll: GET /v1/jobs/{id}?after={n}&wait={seconds}
func (a *applicationDependencies) getJobHandler(w http.ResponseWriter, r *http.Request) {
	// API-03: Use Cache-Control: no-store for observation responses
	w.Header().Set("Cache-Control", "no-store")

	if a.models.Jobs.DB == nil {
		a.tryConnectDB()
	}
	if a.models.Jobs.DB == nil {
		a.serverErrorResponse(w, r, errors.New("database connection is not available"))
		return
	}

	params := httprouter.ParamsFromContext(r.Context())
	idStr := params.ByName("id")

	parsedUUID, err := uuid.Parse(idStr)
	if err != nil {
		a.badRequestResponse(w, r, errors.New("invalid job identifier"))
		return
	}

	// Parse query parameters
	q := r.URL.Query()
	afterStr := q.Get("after")
	waitStr := q.Get("wait")
	latencyStr := q.Get("latency")

	// Optional latency injection for C3 verification
	if latencyStr != "" {
		if d, err := time.ParseDuration(latencyStr); err == nil && d > 0 && d <= 5*time.Second {
			time.Sleep(d)
		}
	}

	// API-02: Reject wait without after with 400
	if afterStr == "" && waitStr != "" {
		a.badRequestResponse(w, r, errors.New("query parameter 'wait' cannot be supplied without 'after'"))
		return
	}

	// API-01: GET /v1/jobs/{id} without query parameters is a prompt snapshot response. It must never wait for a change.
	if afterStr == "" {
		job, err := a.fetchJob(parsedUUID)
		if err != nil {
			if errors.Is(err, data.ErrRecordNotFound) {
				a.notFoundResponse(w, r)
				return
			}
			a.serverErrorResponse(w, r, err)
			return
		}

		resp := a.buildJobResponse(job)
		err = a.writeJSON(w, http.StatusOK, resp, nil)
		if err != nil {
			a.serverErrorResponse(w, r, err)
		}
		return
	}

	// Validate after parameter
	after, err := strconv.Atoi(afterStr)
	if err != nil || after < 0 {
		a.badRequestResponse(w, r, errors.New("query parameter 'after' must be a nonnegative integer"))
		return
	}

	// Validate wait parameter (defaults to 20 when after is supplied; integer 1..20)
	wait := 20
	if waitStr != "" {
		parsedWait, err := strconv.Atoi(waitStr)
		if err != nil || parsedWait < 1 || parsedWait > 20 {
			a.badRequestResponse(w, r, errors.New("query parameter 'wait' must be an integer between 1 and 20 seconds"))
			return
		}
		wait = parsedWait
	}

	// WAIT-01: Counter + Version synchronization pattern.
	// Subscribe BEFORE checking the database to ensure no committed update is missed between checking and waiting.
	ch, unsubscribe := a.notifier.Subscribe(parsedUUID)
	defer unsubscribe()

	// Initial authoritative check of database
	job, err := a.fetchJob(parsedUUID)
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			a.notFoundResponse(w, r)
			return
		}
		a.serverErrorResponse(w, r, err)
		return
	}

	// Also link internal ID if client requested using public_id
	if job.ID != parsedUUID {
		chInternal, unsubInternal := a.notifier.Subscribe(job.ID)
		defer unsubInternal()
		go func() {
			select {
			case <-r.Context().Done():
			case <-chInternal:
				a.notifier.Notify(parsedUUID)
			}
		}()
	}

	// API-02 Condition: after exceeds current version -> 400 promptly; do not wait for an impossible client position.
	if after > job.Version {
		a.badRequestResponse(w, r, fmt.Errorf("requested after version %d exceeds current job version %d", after, job.Version))
		return
	}

	// API-02 Condition: Known terminal job and valid after -> 200 with terminal representation immediately, even when versions match.
	if job.Status == "completed" || job.Status == "failed" {
		resp := a.buildJobResponse(job)
		_ = a.writeJSON(w, http.StatusOK, resp, nil)
		return
	}

	// API-02 Condition: Current version is greater than after -> 200 with the current representation immediately.
	if job.Version > after {
		resp := a.buildJobResponse(job)
		_ = a.writeJSON(w, http.StatusOK, resp, nil)
		return
	}

	// API-02 Condition: Active job and versions match -> Hold until committed change, holding deadline, or cancellation.
	waitDuration := time.Duration(wait) * time.Second
	timer := time.NewTimer(waitDuration)
	defer timer.Stop()

	for {
		select {
		case <-r.Context().Done():
			// WAIT-02: Request-context cancellation ends observation, NOT the worker's job.
			return

		case <-timer.C:
			// Holding deadline reached with no newer version!
			// Requirement: "Recheck authoritative state before choosing 204."
			finalJob, err := a.fetchJob(parsedUUID)
			if err == nil && finalJob != nil {
				if finalJob.Version > after || finalJob.Status == "completed" || finalJob.Status == "failed" {
					resp := a.buildJobResponse(finalJob)
					_ = a.writeJSON(w, http.StatusOK, resp, nil)
					return
				}
			}

			// API-02: 204 No Content with no JSON body
			w.WriteHeader(http.StatusNoContent)
			return

		case <-ch:
			// Wake-up notification received! Recheck authoritative state from PostgreSQL (WAIT-01)
			updatedJob, err := a.fetchJob(parsedUUID)
			if err != nil {
				a.serverErrorResponse(w, r, err)
				return
			}

			if updatedJob.Version > after || updatedJob.Status == "completed" || updatedJob.Status == "failed" {
				resp := a.buildJobResponse(updatedJob)
				_ = a.writeJSON(w, http.StatusOK, resp, nil)
				return
			}
			// If versions still match (e.g. duplicate notification), keep waiting until deadline or next change
		}
	}
}
