package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"image-api/internal/data"
	"github.com/google/uuid"
)

// setupTestServer creates an in-memory/isolated application instance for unit testing observation logic.
func setupTestServer(t *testing.T) (*applicationDependencies, func()) {
	tempDir, err := os.MkdirTemp("", "imagelab_longpoll_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := serverConfig{
		port:        4000,
		environment: "testing",
		storageDir:  tempDir,
		variantsDir: filepath.Join(tempDir, "variants"),
	}
	cfg.limiter.enabled = false

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	notifier := NewJobNotifier()

	app := &applicationDependencies{
		config:   cfg,
		logger:   logger,
		models:   data.NewModels(nil),
		notifier: notifier,
	}

	cleanup := func() {
		_ = os.RemoveAll(tempDir)
	}

	return app, cleanup
}

// TestObservationContract_Validation tests parameter boundaries according to Section 4 / API-02.
func TestObservationContract_Validation(t *testing.T) {
	app, cleanup := setupTestServer(t)
	defer cleanup()

	tests := []struct {
		name           string
		url            string
		expectedStatus int
	}{
		{
			name:           "Wait without after returns 400 (API-02)",
			url:            "/v1/jobs/00000000-0000-0000-0000-000000000001?wait=10",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Malformed after parameter returns 400",
			url:            "/v1/jobs/00000000-0000-0000-0000-000000000001?after=invalid",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Negative after parameter returns 400",
			url:            "/v1/jobs/00000000-0000-0000-0000-000000000001?after=-5",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Wait value 0 returns 400 (out of range)",
			url:            "/v1/jobs/00000000-0000-0000-0000-000000000001?after=1&wait=0",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Wait value 25 returns 400 (out of range 1..20)",
			url:            "/v1/jobs/00000000-0000-0000-0000-000000000001?after=1&wait=25",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Invalid UUID identifier returns 400",
			url:            "/v1/jobs/not-a-valid-uuid?after=1&wait=10",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			rr := httptest.NewRecorder()

			app.routes().ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d. Body: %s", tt.expectedStatus, rr.Code, rr.Body.String())
			}

			// API-03: Cache-Control must be no-store
			cc := rr.Header().Get("Cache-Control")
			if cc != "no-store" {
				t.Errorf("expected Cache-Control: no-store, got %q", cc)
			}
		})
	}
}

// TestJobNotifier_CoalescedBroadcast tests the in-memory PubSub notifier (VER-03, WAIT-01).
func TestJobNotifier_CoalescedBroadcast(t *testing.T) {
	notifier := NewJobNotifier()
	jobID := uuid.New()

	// 1. Subscribe two distinct observers (checkpoint 86: two observers discover change)
	ch1, unsub1 := notifier.Subscribe(jobID)
	defer unsub1()

	ch2, unsub2 := notifier.Subscribe(jobID)
	defer unsub2()

	if notifier.ActiveObserversCount(jobID) != 2 {
		t.Fatalf("expected 2 active observers, got %d", notifier.ActiveObserversCount(jobID))
	}

	// 2. Broadcast change notification
	notifier.Notify(jobID)

	// Both observers must receive the wake-up signal (VER-03)
	select {
	case <-ch1:
		// Success
	case <-time.After(50 * time.Millisecond):
		t.Fatal("observer 1 timed out waiting for broadcast notification")
	}

	select {
	case <-ch2:
		// Success
	case <-time.After(50 * time.Millisecond):
		t.Fatal("observer 2 timed out waiting for broadcast notification")
	}

	// 3. Test non-blocking coalescing (duplicate notification does not block or leak)
	notifier.Notify(jobID)
	notifier.Notify(jobID) // Second rapid notification must coalesce safely into buffer

	select {
	case <-ch1:
		// Consumed coalesced notification
	default:
		t.Fatal("expected notification in channel 1")
	}

	// 4. Test unsubscribe
	unsub1()
	if notifier.ActiveObserversCount(jobID) != 1 {
		t.Fatalf("expected 1 active observer after unsub1, got %d", notifier.ActiveObserversCount(jobID))
	}
}

// TestBuildJobResponse_Fields verifies the Section 4 snapshot fields contract (API-03).
func TestBuildJobResponse_Fields(t *testing.T) {
	app, cleanup := setupTestServer(t)
	defer cleanup()

	now := time.Now().UTC()
	testJob := &data.Job{
		ID:                  uuid.New(),
		PublicID:            uuid.New(),
		ImageID:             uuid.New(),
		JobType:             "image_transform",
		Status:              "processing",
		Version:             4,
		Stage:               "preview_ready",
		VariantsCompleted:   2,
		SimulatedProgress:   "Step 2/5",
		StartedAt:           &now,
		TerminalCommittedAt: &now,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	resp := app.buildJobResponse(testJob)

	if resp["version"] != 4 {
		t.Errorf("expected version 4, got %v", resp["version"])
	}
	if resp["stage"] != "preview_ready" {
		t.Errorf("expected stage 'preview_ready', got %v", resp["stage"])
	}
	if resp["variants_completed"] != 2 {
		t.Errorf("expected variants_completed 2, got %v", resp["variants_completed"])
	}
	if resp["status"] != "processing" {
		t.Errorf("expected status 'processing', got %v", resp["status"])
	}
	if resp["simulated_progress"] != "Step 2/5" {
		t.Errorf("expected simulated_progress 'Step 2/5', got %v", resp["simulated_progress"])
	}
}
