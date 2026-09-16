package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"image-api/internal/data"
	"github.com/google/uuid"
)

// newTestApplication creates an applicationDependencies instance with in-memory / temporary storage
func newTestApplication(t *testing.T) (*applicationDependencies, string) {
	tempDir, err := os.MkdirTemp("", "image_test_*")
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
	models := data.NewModels(nil) // DB-nil for standalone unit testing of upload handler

	app := &applicationDependencies{
		config: cfg,
		logger: logger,
		models: models,
	}

	return app, tempDir
}

// createMultipartRequest constructs a multipart/form-data HTTP request with the given filename, field name, and body
func createMultipartRequest(t *testing.T, fieldName, filename string, content []byte) (*http.Request, string) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	part, err := writer.CreateFormFile(fieldName, filename)
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}

	if _, err := part.Write(content); err != nil {
		t.Fatalf("failed to write file content: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/images", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req, writer.FormDataContentType()
}

func TestUploadHandler_AcceptsValidJPEG(t *testing.T) {
	app, tempDir := newTestApplication(t)
	defer os.RemoveAll(tempDir)

	// Valid JPEG magic bytes header: FF D8 FF E0 ... JFIF
	jpegData := []byte{
		0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 'J', 'F', 'I', 'F', 0x00, 0x01, 0x01, 0x01, 0x00, 0x48,
		0x00, 0x48, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43, 0x00, 0x08, 0x06, 0x06, 0x07, 0x06, 0x05, 0x08,
	}
	// Pad to have some payload
	jpegData = append(jpegData, bytes.Repeat([]byte{0x00}, 200)...)
	jpegData = append(jpegData, 0xFF, 0xD9) // EOI

	req, _ := createMultipartRequest(t, "image", "user_vacation.jpg", jpegData)
	rr := httptest.NewRecorder()

	app.routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected status 202 Accepted, got %d: %s", rr.Code, rr.Body.String())
	}

	loc := rr.Header().Get("Location")
	if loc == "" || !strings.HasPrefix(loc, "/v1/jobs/") {
		t.Errorf("expected Location header starting with /v1/jobs/, got %q", loc)
	}

	var res struct {
		Status string `json:"status"`
		Job    struct {
			ID       string `json:"id"`
			PublicID string `json:"public_id"`
			Status   string `json:"status"`
		} `json:"job"`
		Image struct {
			ID          string `json:"id"`
			StoragePath string `json:"storage_path"`
			MimeType    string `json:"mime_type"`
			FileSize    int64  `json:"file_size"`
		} `json:"image"`
	}

	if err := json.NewDecoder(rr.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}

	if res.Status != "accepted" && res.Status != "queued" {
		t.Errorf("expected status 'accepted' or 'queued', got %q", res.Status)
	}

	if res.Job.Status != "queued" {
		t.Errorf("expected job status 'queued', got %q", res.Job.Status)
	}

	if res.Image.MimeType != "image/jpeg" {
		t.Errorf("expected mime_type 'image/jpeg', got %q", res.Image.MimeType)
	}

	// Verify server-controlled storage path:
	// Must NOT contain user filename 'user_vacation.jpg'
	if strings.Contains(res.Image.StoragePath, "user_vacation.jpg") {
		t.Errorf("storage path leaked user filename: %s", res.Image.StoragePath)
	}
	if !strings.HasSuffix(res.Image.StoragePath, ".jpg") {
		t.Errorf("storage path missing canonical .jpg extension: %s", res.Image.StoragePath)
	}

	// Verify file actually exists on filesystem
	if _, err := os.Stat(filepath.FromSlash(res.Image.StoragePath)); os.IsNotExist(err) {
		t.Errorf("file was not written to disk at %s", res.Image.StoragePath)
	}
}

func TestUploadHandler_AcceptsValidPNG(t *testing.T) {
	app, tempDir := newTestApplication(t)
	defer os.RemoveAll(tempDir)

	// Valid PNG magic bytes header: 89 50 4E 47 0D 0A 1A 0A
	pngData := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}
	pngData = append(pngData, bytes.Repeat([]byte{0x00}, 200)...)

	req, _ := createMultipartRequest(t, "image", "test_diagram.png", pngData)
	rr := httptest.NewRecorder()

	app.routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected status 202 Accepted, got %d: %s", rr.Code, rr.Body.String())
	}

	var res struct {
		Image struct {
			StoragePath string `json:"storage_path"`
			MimeType    string `json:"mime_type"`
		} `json:"image"`
	}
	_ = json.NewDecoder(rr.Body).Decode(&res)

	if res.Image.MimeType != "image/png" {
		t.Errorf("expected mime_type 'image/png', got %q", res.Image.MimeType)
	}
	if !strings.HasSuffix(res.Image.StoragePath, ".png") {
		t.Errorf("expected storage_path to have .png suffix: %s", res.Image.StoragePath)
	}
}

func TestUploadHandler_RejectsUnsupportedMimeType(t *testing.T) {
	app, tempDir := newTestApplication(t)
	defer os.RemoveAll(tempDir)

	// Plain text file disguised as .jpg
	fakeJPEG := []byte("This is plain text and not a real image at all.")

	req, _ := createMultipartRequest(t, "image", "malicious_script.jpg", fakeJPEG)
	rr := httptest.NewRecorder()

	app.routes().ServeHTTP(rr, req)

	// Must be rejected with 422 Unprocessable Entity
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected status 422 Unprocessable Entity, got %d: %s", rr.Code, rr.Body.String())
	}

	if !strings.Contains(rr.Body.String(), "unsupported file type") {
		t.Errorf("expected error message mentioning unsupported file type, got %s", rr.Body.String())
	}

	// Verify no files were stored in tempDir
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("failed to read temp dir: %v", err)
	}
	if len(entries) > 0 {
		t.Errorf("expected 0 files stored on rejection, found %d", len(entries))
	}
}

func TestUploadHandler_RejectsOversizedPayload(t *testing.T) {
	app, tempDir := newTestApplication(t)
	defer os.RemoveAll(tempDir)

	// Create payload > 10MB (10MB + 1KB)
	oversizedData := make([]byte, (10*1024*1024)+1024)
	copy(oversizedData[:4], []byte{0xFF, 0xD8, 0xFF, 0xE0})

	req, _ := createMultipartRequest(t, "image", "huge_image.jpg", oversizedData)
	rr := httptest.NewRecorder()

	app.routes().ServeHTTP(rr, req)

	// Must be rejected with 413 or 400
	if rr.Code != http.StatusRequestEntityTooLarge && rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 413 or 400, got %d: %s", rr.Code, rr.Body.String())
	}

	// Verify no file stored
	entries, _ := os.ReadDir(tempDir)
	if len(entries) > 0 {
		t.Errorf("expected 0 files stored on oversized rejection, found %d", len(entries))
	}
}

func TestGetJobHandler_InvalidUUID(t *testing.T) {
	app, tempDir := newTestApplication(t)
	defer os.RemoveAll(tempDir)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/not-a-uuid", nil)
	rr := httptest.NewRecorder()

	app.routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 Bad Request, got %d", rr.Code)
	}
}

func TestGetVariantHandler_RejectsUnknownVariantName(t *testing.T) {
	app, tempDir := newTestApplication(t)
	defer os.RemoveAll(tempDir)

	req := httptest.NewRequest(http.MethodGet, "/v1/images/01a0877a-6524-7570-a5b7-eb710244f9d0/variants/invalid_variant", nil)
	rr := httptest.NewRecorder()

	app.routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected status 404 Not Found, got %d", rr.Code)
	}
}

// Integration tests with PostgreSQL (runs when database is available)
func newIntegrationTestApplication(t *testing.T) (*applicationDependencies, string, func()) {
	dsn := os.Getenv("IMAGE_DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/image_db?sslmode=disable"
	}

	cfg := serverConfig{
		port:        4000,
		environment: "testing",
	}
	cfg.db.dsn = dsn
	cfg.db.maxOpenConns = 10
	cfg.db.maxIdleConns = 10
	cfg.db.maxIdleTime = time.Minute
	cfg.limiter.enabled = false

	db, err := openDB(cfg)
	if err != nil {
		t.Skipf("skipping database integration test: %v", err)
	}

	tempDir, err := os.MkdirTemp("", "image_integration_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	cfg.storageDir = filepath.Join(tempDir, "uploads")
	cfg.variantsDir = filepath.Join(tempDir, "variants")
	_ = os.MkdirAll(cfg.storageDir, 0755)
	_ = os.MkdirAll(cfg.variantsDir, 0755)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	models := data.NewModels(db)

	app := &applicationDependencies{
		config: cfg,
		logger: logger,
		models: models,
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tempDir)
	}

	return app, tempDir, cleanup
}

func TestIntegration_EndToEnd_WorkerProcessesJobAndGeneratesVariants(t *testing.T) {
	app, _, cleanup := newIntegrationTestApplication(t)
	defer cleanup()

	// 1. Submit a valid JPEG image
	// Generate genuine 100x100 JPEG
	imgBuf := &bytes.Buffer{}
	rawImg := image.NewRGBA(image.Rect(0, 0, 400, 300))
	for y := 0; y < 300; y++ {
		for x := 0; x < 400; x++ {
			rawImg.Set(x, y, color.RGBA{R: 120, G: 180, B: 240, A: 255})
		}
	}
	if err := jpeg.Encode(imgBuf, rawImg, &jpeg.Options{Quality: 90}); err != nil {
		t.Fatalf("failed to encode test jpeg: %v", err)
	}

	req, _ := createMultipartRequest(t, "image", "sample_landscape.jpg", imgBuf.Bytes())
	rr := httptest.NewRecorder()

	app.routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rr.Code, rr.Body.String())
	}

	var acceptRes struct {
		ImageID  string `json:"image_id"`
		JobID    string `json:"job_id"`
		Status   string `json:"status"`
		StatusURL string `json:"status_url"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&acceptRes); err != nil {
		t.Fatalf("failed to decode accept response: %v", err)
	}

	if acceptRes.JobID == "" || acceptRes.ImageID == "" {
		t.Fatalf("missing job_id or image_id in 202 response: %+v", acceptRes)
	}

	// 2. Fetch created job and execute worker processing
	parsedJobID, err := uuid.Parse(acceptRes.JobID)
	if err != nil {
		t.Fatalf("failed to parse job id: %v", err)
	}
	jobToProcess, err := app.models.Jobs.GetByPublicID(parsedJobID)
	if err != nil {
		t.Fatalf("failed to find job in db: %v", err)
	}

	app.executeImageJob(jobToProcess)

	// 3. Poll the job status via GET /v1/jobs/:id
	reqJob := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+acceptRes.JobID, nil)
	rrJob := httptest.NewRecorder()
	app.routes().ServeHTTP(rrJob, reqJob)

	if rrJob.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from GET /v1/jobs/:id, got %d: %s", rrJob.Code, rrJob.Body.String())
	}

	var jobRes struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		StartedAt   *time.Time `json:"started_at"`
		CompletedAt *time.Time `json:"completed_at"`
		Variants    []struct {
			Name   string `json:"name"`
			Width  int    `json:"width"`
			Height int    `json:"height"`
			URL    string `json:"url"`
		} `json:"variants"`
	}
	if err := json.NewDecoder(rrJob.Body).Decode(&jobRes); err != nil {
		t.Fatalf("failed to decode job response: %v", err)
	}

	if jobRes.Status != "completed" {
		t.Fatalf("expected job status 'completed', got %q", jobRes.Status)
	}

	if jobRes.CompletedAt == nil {
		t.Errorf("expected completed_at to be populated, got completed=%v", jobRes.CompletedAt)
	}

	if len(jobRes.Variants) != 3 {
		t.Fatalf("expected 3 variants, got %d", len(jobRes.Variants))
	}

	// 4. Verify variant endpoints GET /v1/images/:id/variants/:name
	for _, vName := range []string{"thumbnail", "preview", "display"} {
		vReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/images/%s/variants/%s", acceptRes.ImageID, vName), nil)
		vRR := httptest.NewRecorder()
		app.routes().ServeHTTP(vRR, vReq)

		if vRR.Code != http.StatusOK {
			t.Errorf("expected 200 OK for variant %s, got %d", vName, vRR.Code)
		}
		if contentType := vRR.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "image/") {
			t.Errorf("expected image Content-Type for variant %s, got %q", vName, contentType)
		}
	}
}

func TestIntegration_EndToEnd_WorkerHandlesDeliberateFailure(t *testing.T) {
	app, _, cleanup := newIntegrationTestApplication(t)
	defer cleanup()

	// 1. Submit an image whose filename triggers deliberate failure ("deliberate_fail_test.jpg")
	imgBuf := &bytes.Buffer{}
	rawImg := image.NewRGBA(image.Rect(0, 0, 100, 100))
	_ = jpeg.Encode(imgBuf, rawImg, &jpeg.Options{Quality: 80})

	req, _ := createMultipartRequest(t, "image", "deliberate_fail_test.jpg", imgBuf.Bytes())
	rr := httptest.NewRecorder()

	app.routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", rr.Code)
	}

	var acceptRes struct {
		JobID string `json:"job_id"`
	}
	_ = json.NewDecoder(rr.Body).Decode(&acceptRes)

	// 2. Fetch created job and execute worker processing
	parsedJobID, err := uuid.Parse(acceptRes.JobID)
	if err != nil {
		t.Fatalf("failed to parse job id: %v", err)
	}
	jobToProcess, err := app.models.Jobs.GetByPublicID(parsedJobID)
	if err != nil {
		t.Fatalf("failed to find job in db: %v", err)
	}

	app.executeImageJob(jobToProcess)

	// 3. Inspect job status
	reqJob := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+acceptRes.JobID, nil)
	rrJob := httptest.NewRecorder()
	app.routes().ServeHTTP(rrJob, reqJob)

	if rrJob.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from GET /v1/jobs, got %d", rrJob.Code)
	}

	var jobRes struct {
		Status       string  `json:"status"`
		ErrorMessage *string `json:"error_message"`
		Variants     []any   `json:"variants"`
	}
	_ = json.NewDecoder(rrJob.Body).Decode(&jobRes)

	if jobRes.Status != "failed" {
		t.Fatalf("expected job status 'failed', got %q", jobRes.Status)
	}

	if jobRes.ErrorMessage == nil || *jobRes.ErrorMessage == "" {
		t.Errorf("expected client-safe error message, got nil or empty")
	}

	if len(jobRes.Variants) > 0 {
		t.Errorf("expected 0 variants on failed job, got %d", len(jobRes.Variants))
	}
}

