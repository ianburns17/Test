package main

import (
	"errors"
	"net/http"

	"image-api/internal/data"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

// getJobHandler provides read-only inspection of an asynchronous job.
// Accepts either the client-facing public_id (UUIDv4) or internal id (UUIDv7).
func (a *applicationDependencies) getJobHandler(w http.ResponseWriter, r *http.Request) {
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

	err = a.writeJSON(w, http.StatusOK, envelope{
		"job": job,
	}, nil)
	if err != nil {
		a.serverErrorResponse(w, r, err)
	}
}
