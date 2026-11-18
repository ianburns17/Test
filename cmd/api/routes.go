package main

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// routes registers URL handlers and configures middleware chains.
func (a *applicationDependencies) routes() http.Handler {
	router := httprouter.New()

	router.NotFound = http.HandlerFunc(a.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(a.methodNotAllowedResponse)

	// Web UI & Observability
	router.HandlerFunc(http.MethodGet, "/", a.uiConsoleHandler)
	router.HandlerFunc(http.MethodGet, "/lab", a.uiConsoleHandler)
	router.HandlerFunc(http.MethodGet, "/customer", a.uiConsoleHandler)
	router.HandlerFunc(http.MethodGet, "/v1/healthcheck", a.healthcheckHandler)

	// Image Acceptance & Retrieval Endpoints
	router.HandlerFunc(http.MethodPost, "/v1/images", a.uploadImageHandler)
	router.HandlerFunc(http.MethodGet, "/v1/images/:id", a.getImageHandler)
	router.HandlerFunc(http.MethodGet, "/v1/images/:id/variants/:name", a.getVariantHandler)

	// Job Observation Endpoints
	// SSE Route (Assessment 3 / SSE-01)
	router.HandlerFunc(http.MethodGet, "/v1/jobs/:id/events", a.getJobEventsHandler)
	// Prompt snapshot & Versioned Long Polling (Assessment 2 / API-01, API-02)
	router.HandlerFunc(http.MethodGet, "/v1/jobs/:id", a.getJobHandler)

	// Middleware chain: recoverPanic -> rateLimit -> corsMiddleware
	return a.recoverPanic(
		a.rateLimit(
			a.corsMiddleware(router),
		),
	)
}
