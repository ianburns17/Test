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
	router.HandlerFunc(http.MethodGet, "/v1/healthcheck", a.healthcheckHandler)

	// Image Acceptance Endpoints (Week 1 Acceptance Foundation)
	router.HandlerFunc(http.MethodPost, "/v1/images", a.uploadImageHandler)
	router.HandlerFunc(http.MethodGet, "/v1/images/:id", a.getImageHandler)

	// Job Observation Endpoints
	router.HandlerFunc(http.MethodGet, "/v1/jobs/:id", a.getJobHandler)

	// Middleware chain: recoverPanic -> rateLimit -> corsMiddleware
	return a.recoverPanic(
		a.rateLimit(
			a.corsMiddleware(router),
		),
	)
}
