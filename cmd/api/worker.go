package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"image-api/internal/data"
	"image-api/internal/transform"
	"github.com/google/uuid"
)

// startWorker launches exactly one background worker goroutine inside the application (WRK-01).
func (a *applicationDependencies) startWorker(ctx context.Context) {
	if a.models.Jobs.DB == nil {
		a.logger.Warn("database offline; background worker standing by")
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.logger.Info("background worker started (concurrency = 1)")

		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				a.logger.Info("background worker shutting down gracefully")
				return
			case <-ticker.C:
				if a.models.Jobs.DB == nil {
					if !a.tryConnectDB() {
						continue
					}
				}
				a.processNextQueuedJob(ctx)
			}
		}
	}()
}

// processNextQueuedJob finds and claims one queued job, then executes transformation (WRK-02).
func (a *applicationDependencies) processNextQueuedJob(ctx context.Context) {
	// Claim next queued job atomically via FOR UPDATE SKIP LOCKED
	job, err := a.models.Jobs.ClaimNextJob("image_transform")
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			// Queue is empty, nothing to do
			return
		}
		a.logger.Error("failed to claim next queued job", "error", err)
		return
	}

	a.logger.Info("worker claimed queued job",
		"job_id", job.ID.String(),
		"public_id", job.PublicID.String(),
		"image_id", job.ImageID.String(),
		"started_at", job.StartedAt,
	)

	a.executeImageJob(job)
}

// executeImageJob handles reading the original, generating all 3 variants, and updating authoritative state.
func (a *applicationDependencies) executeImageJob(job *data.Job) {
	// 1. Fetch original image record
	img, err := a.models.Images.Get(job.ImageID)
	if err != nil {
		errMsg := "authoritative image metadata not found"
		a.logger.Error("worker failed to find image record", "job_id", job.ID, "error", err)
		_ = a.models.Jobs.MarkFailed(job.ID, errMsg)
		return
	}

	// Apply configured simulated processing delay (e.g. -processing-delay=15s)
	// Keeps the job in 'processing' state so short polling and UI transitions can be observed.
	if a.config.processingDelay > 0 {
		a.logger.Info("applying simulated processing delay",
			"duration", a.config.processingDelay.String(),
			"job_id", job.ID.String(),
			"public_id", job.PublicID.String(),
		)
		time.Sleep(a.config.processingDelay)
	}

	// 2. Check for deliberate failure induction
	// If the original filename contains 'fail' or 'corrupt', or if payload requests intentional failure:
	var payloadMap map[string]any
	if len(job.Payload) > 0 {
		_ = json.Unmarshal(job.Payload, &payloadMap)
	}

	shouldFail := false
	if payloadMap != nil {
		if failVal, ok := payloadMap["fail_processing"].(bool); ok && failVal {
			shouldFail = true
		}
	}
	if strings.Contains(strings.ToLower(img.OriginalFilename), "fail") ||
		strings.Contains(strings.ToLower(img.OriginalFilename), "corrupt") {
		shouldFail = true
	}

	if shouldFail {
		safeErr := "processing failed: image transformation could not be completed (deliberate failure triggered)"
		a.logger.Warn("deliberate worker failure triggered", "job_id", job.ID, "filename", img.OriginalFilename)
		// Small simulated processing delay to show lifecycle transition
		time.Sleep(300 * time.Millisecond)
		if err := a.models.Jobs.MarkFailed(job.ID, safeErr); err != nil {
			a.logger.Error("failed to mark job as failed", "job_id", job.ID, "error", err)
		}
		return
	}

	// 3. Verify original file exists on filesystem
	diskPath := filepath.FromSlash(img.StoragePath)
	if _, err := os.Stat(diskPath); os.IsNotExist(err) {
		safeErr := "processing failed: original file not accessible on server storage"
		a.logger.Error("original file not found on disk", "job_id", job.ID, "path", diskPath)
		_ = a.models.Jobs.MarkFailed(job.ID, safeErr)
		return
	}

	variantsDir := a.config.variantsDir
	if variantsDir == "" {
		variantsDir = "./storage/variants"
	}

	// 4. Generate variants according to Section 9 contracts:
	// - thumbnail: 150x150 exact square center-crop
	// - preview: max 800x600 aspect-preserved
	// - display: max 1200x900 aspect-preserved
	variantResults, err := transform.GenerateAllVariants(diskPath, variantsDir, img.MimeType, img.ID.String())
	if err != nil {
		// Client-safe error message (never expose internal paths or stack traces - Section 13)
		safeErr := "processing failed: unable to decode or transform image format"
		a.logger.Error("worker variant generation error", "job_id", job.ID, "error", err)
		_ = a.models.Jobs.MarkFailed(job.ID, safeErr)
		return
	}

	// 5. Ensure all 3 variants are available before committing (IMG-03 All-or-complete contract)
	if len(variantResults) != 3 {
		safeErr := "processing failed: incomplete variant set generated"
		a.logger.Error("worker variant count mismatch", "job_id", job.ID, "count", len(variantResults))
		_ = a.models.Jobs.MarkFailed(job.ID, safeErr)
		return
	}

	// 6. Store variant records in database
	type variantSummary struct {
		Name   string `json:"name"`
		Width  int    `json:"width"`
		Height int    `json:"height"`
		URL    string `json:"url"`
		Size   int64  `json:"size"`
	}

	var summaries []variantSummary
	for _, vr := range variantResults {
		w := vr.Width
		h := vr.Height
		vRecord := &data.ImageVariant{
			ID:          uuid.Nil, // Let Insert generate UUIDv7
			ImageID:     img.ID,
			VariantName: vr.Name,
			StoragePath: vr.StoragePath,
			MimeType:    vr.MimeType,
			FileSize:    vr.FileSize,
			Width:       &w,
			Height:      &h,
		}

		if err := a.models.Variants.Insert(vRecord); err != nil {
			safeErr := "processing failed: could not commit variant metadata"
			a.logger.Error("failed to insert variant record", "job_id", job.ID, "variant", vr.Name, "error", err)
			_ = a.models.Jobs.MarkFailed(job.ID, safeErr)
			return
		}

		summaries = append(summaries, variantSummary{
			Name:   vr.Name,
			Width:  vr.Width,
			Height: vr.Height,
			URL:    fmt.Sprintf("/v1/images/%s/variants/%s", img.ID.String(), vr.Name),
			Size:   vr.FileSize,
		})
	}

	// 7. Mark the job completed only after all variants and metadata are verified available (IMG-03)
	resultBytes, _ := json.Marshal(map[string]any{
		"variants": summaries,
	})

	if err := a.models.Jobs.MarkCompleted(job.ID, json.RawMessage(resultBytes)); err != nil {
		a.logger.Error("failed to mark job completed", "job_id", job.ID, "error", err)
		return
	}

	a.logger.Info("worker successfully completed job",
		"job_id", job.ID.String(),
		"public_id", job.PublicID.String(),
		"variants_count", len(summaries),
	)
}
