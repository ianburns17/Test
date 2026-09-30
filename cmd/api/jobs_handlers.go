package main

import (
	"errors"
	"fmt"
	"net/http"

	"image-api/internal/data"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

// getJobHandler provides read-only inspection of an asynchronous job (Section 10).
// Accepts either the client-facing public_id (UUIDv4) or internal id (UUIDv7).
func (a *applicationDependencies) getJobHandler(w http.ResponseWriter, r *http.Request) {
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

	// Try lookup by public_id first (standard client token)
	job, err := a.models.Jobs.GetByPublicID(parsedUUID)
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			// Fallback to internal ID lookup
			job, err = a.models.Jobs.Get(parsedUUID)
			if err != nil {
				if errors.Is(err, data.ErrRecordNotFound) {
					a.notFoundResponse(w, r)
					return
				}
				a.serverErrorResponse(w, r, err)
				return
			}
		} else {
			a.serverErrorResponse(w, r, err)
			return
		}
	}

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

	// Formulate response supporting Section 10 contract and client envelopes
	resp := envelope{
		"id":            job.PublicID,
		"public_id":     job.PublicID,
		"image_id":      job.ImageID,
		"status":        job.Status,
		"queued_at":     job.CreatedAt,
		"started_at":    job.StartedAt,
		"completed_at":  job.CompletedAt,
		"error_message": job.ErrorMessage,
	}

	if len(variantList) > 0 {
		resp["variants"] = variantList
	}

	// Provide nested job key for backward compatibility with UI and test fixtures
	resp["job"] = envelope{
		"id":            job.PublicID,
		"internal_id":   job.ID,
		"public_id":     job.PublicID,
		"image_id":      job.ImageID,
		"job_type":      job.JobType,
		"status":        job.Status,
		"queued_at":     job.CreatedAt,
		"started_at":    job.StartedAt,
		"completed_at":  job.CompletedAt,
		"error_message": job.ErrorMessage,
		"variants":      variantList,
	}

	err = a.writeJSON(w, http.StatusOK, resp, nil)
	if err != nil {
		a.serverErrorResponse(w, r, err)
	}
}
