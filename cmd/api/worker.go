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
		"version", job.Version,
		"stage", job.Stage,
		"started_at", job.StartedAt,
		"post_commit_time", time.Now().UTC().Format(time.RFC3339Nano),
	)

	// Notify observers that job transitioned to 'processing' / 'starting' (VER-03)
	if a.notifier != nil {
		a.notifier.Notify(job.ID)
		a.notifier.Notify(job.PublicID)
	}

	a.executeImageJob(job)
}

// executeImageJob handles reading the original, generating all 3 variants, publishing versioned milestones, and updating authoritative state (PROG-01, PROG-02, EXP-01).
func (a *applicationDependencies) executeImageJob(job *data.Job) {
	// 1. Fetch original image record
	img, err := a.models.Images.Get(job.ImageID)
	if err != nil {
		errMsg := "authoritative image metadata not found"
		a.logger.Error("worker failed to find image record", "job_id", job.ID, "error", err)
		_ = a.models.Jobs.MarkFailed(job.ID, errMsg)
		if a.notifier != nil {
			a.notifier.Notify(job.ID)
			a.notifier.Notify(job.PublicID)
		}
		return
	}

	// Extract condition and payload controls (Section 5 / EXP-01 / EXP-02)
	var payloadMap map[string]any
	if len(job.Payload) > 0 {
		_ = json.Unmarshal(job.Payload, &payloadMap)
	}

	condition := ""
	if payloadMap != nil {
		if c, ok := payloadMap["condition"].(string); ok {
			condition = strings.ToUpper(strings.TrimSpace(c))
		}
	}

	// Condition A: Quiet job (Add 30 seconds of delay while processing, with no intermediate updates)
	if condition == "A" {
		quietDelay := 30 * time.Second
		if a.config.processingDelay > 0 {
			quietDelay = a.config.processingDelay
		}
		a.logger.Info("Condition A [Quiet job]: applying 30s quiet processing delay with 0 intermediate updates",
			"job_id", job.ID.String(),
			"delay", quietDelay.String(),
		)
		time.Sleep(quietDelay)
	} else if a.config.processingDelay > 0 {
		a.logger.Info("applying configured simulated processing delay",
			"duration", a.config.processingDelay.String(),
			"job_id", job.ID.String(),
		)
		time.Sleep(a.config.processingDelay)
	}

	// Condition C1: Slow updates (10-second simulated phase with updates every 2 seconds -> 5 steps) (EXP-01, EXP-02, EXP-03)
	if condition == "C1" {
		a.logger.Info("Condition C1 [Slow updates]: starting 10s simulated phase (5 updates, 2s apart)", "job_id", job.ID.String())
		for step := 1; step <= 5; step++ {
			time.Sleep(2 * time.Second)
			simText := fmt.Sprintf("Step %d/5 (%ds)", step, step*2)
			updatedJob, err := a.models.Jobs.UpdateProgress(job.ID, "simulating", 0, simText)
			if err == nil {
				a.logger.Info("Condition C1 simulation step committed",
					"job_id", job.ID.String(),
					"version", updatedJob.Version,
					"step", step,
					"simulated_progress", simText,
					"post_commit_time", time.Now().UTC().Format(time.RFC3339Nano),
				)
				if a.notifier != nil {
					a.notifier.Notify(job.ID)
					a.notifier.Notify(job.PublicID)
				}
			}
		}
	}

	// Condition C2 & C3: Fast updates (10-second simulated phase with updates every 200 ms -> 50 steps) (EXP-01, EXP-02, EXP-03)
	if condition == "C2" || condition == "C3" {
		a.logger.Info(fmt.Sprintf("Condition %s [Fast updates]: starting 10s simulated phase (50 updates, 200ms apart)", condition), "job_id", job.ID.String())
		for step := 1; step <= 50; step++ {
			time.Sleep(200 * time.Millisecond)
			simText := fmt.Sprintf("Step %d/50 (%dms)", step, step*200)
			updatedJob, err := a.models.Jobs.UpdateProgress(job.ID, "simulating", 0, simText)
			if err == nil {
				a.logger.Info(fmt.Sprintf("Condition %s simulation step committed", condition),
					"job_id", job.ID.String(),
					"version", updatedJob.Version,
					"step", step,
					"simulated_progress", simText,
					"post_commit_time", time.Now().UTC().Format(time.RFC3339Nano),
				)
				if a.notifier != nil {
					a.notifier.Notify(job.ID)
					a.notifier.Notify(job.PublicID)
				}
			}
		}
	}

	// Check for deliberate failure induction
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
		time.Sleep(300 * time.Millisecond)
		if err := a.models.Jobs.MarkFailed(job.ID, safeErr); err != nil {
			a.logger.Error("failed to mark job as failed", "job_id", job.ID, "error", err)
		}
		if a.notifier != nil {
			a.notifier.Notify(job.ID)
			a.notifier.Notify(job.PublicID)
		}
		return
	}

	// Verify original file exists on filesystem
	diskPath := filepath.FromSlash(img.StoragePath)
	if _, err := os.Stat(diskPath); os.IsNotExist(err) {
		safeErr := "processing failed: original file not accessible on server storage"
		a.logger.Error("original file not found on disk", "job_id", job.ID, "path", diskPath)
		_ = a.models.Jobs.MarkFailed(job.ID, safeErr)
		if a.notifier != nil {
			a.notifier.Notify(job.ID)
			a.notifier.Notify(job.PublicID)
		}
		return
	}

	variantsDir := a.config.variantsDir
	if variantsDir == "" {
		variantsDir = "./storage/variants"
	}

	// Generate variants according to Section 9 contracts:
	// - thumbnail: 150x150 exact square center-crop
	// - preview: max 800x600 aspect-preserved
	// - display: max 1200x900 aspect-preserved
	variantResults, err := transform.GenerateAllVariants(diskPath, variantsDir, img.MimeType, img.ID.String())
	if err != nil {
		safeErr := "processing failed: unable to decode or transform image format"
		a.logger.Error("worker variant generation error", "job_id", job.ID, "error", err)
		_ = a.models.Jobs.MarkFailed(job.ID, safeErr)
		if a.notifier != nil {
			a.notifier.Notify(job.ID)
			a.notifier.Notify(job.PublicID)
		}
		return
	}

	if len(variantResults) != 3 {
		safeErr := "processing failed: incomplete variant set generated"
		a.logger.Error("worker variant count mismatch", "job_id", job.ID, "count", len(variantResults))
		_ = a.models.Jobs.MarkFailed(job.ID, safeErr)
		if a.notifier != nil {
			a.notifier.Notify(job.ID)
			a.notifier.Notify(job.PublicID)
		}
		return
	}

	// Store variants in database and publish versioned progress milestones (PROG-01, PROG-02)
	type variantSummary struct {
		Name   string `json:"name"`
		Width  int    `json:"width"`
		Height int    `json:"height"`
		URL    string `json:"url"`
		Size   int64  `json:"size"`
	}

	var summaries []variantSummary
	stageNames := []string{"thumbnail_ready", "preview_ready", "display_ready"}

	for idx, vr := range variantResults {
		// Provide slight natural milestone gap (150ms) so intermediate progress is observable
		time.Sleep(150 * time.Millisecond)

		w := vr.Width
		h := vr.Height
		vRecord := &data.ImageVariant{
			ID:          uuid.Nil,
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
			if a.notifier != nil {
				a.notifier.Notify(job.ID)
				a.notifier.Notify(job.PublicID)
			}
			return
		}

		summaries = append(summaries, variantSummary{
			Name:   vr.Name,
			Width:  vr.Width,
			Height: vr.Height,
			URL:    fmt.Sprintf("/v1/images/%s/variants/%s", img.ID.String(), vr.Name),
			Size:   vr.FileSize,
		})

		variantsCompleted := idx + 1
		stage := stageNames[idx]

		if variantsCompleted < 3 {
			// Intermediate milestone (thumbnail_ready at count 1, preview_ready at count 2)
			updatedJob, err := a.models.Jobs.UpdateProgress(job.ID, stage, variantsCompleted, "")
			if err != nil {
				a.logger.Error("failed to update progress milestone", "job_id", job.ID, "stage", stage, "error", err)
			} else {
				a.logger.Info("progress milestone committed",
					"job_id", job.ID.String(),
					"version", updatedJob.Version,
					"stage", updatedJob.Stage,
					"variants_completed", updatedJob.VariantsCompleted,
					"post_commit_time", time.Now().UTC().Format(time.RFC3339Nano),
				)
				if a.notifier != nil {
					a.notifier.Notify(job.ID)
					a.notifier.Notify(job.PublicID)
				}
			}
		} else {
			// Final milestone: display_ready + completed terminal state in one transaction (PROG-02)
			resultBytes, _ := json.Marshal(map[string]any{
				"variants": summaries,
			})

			updatedJob, err := a.models.Jobs.MarkCompletedWithProgress(job.ID, json.RawMessage(resultBytes), stage, 3)
			if err != nil {
				a.logger.Error("failed to mark job completed", "job_id", job.ID, "error", err)
				return
			}

			a.logger.Info("job terminal completion committed",
				"job_id", job.ID.String(),
				"public_id", job.PublicID.String(),
				"version", updatedJob.Version,
				"stage", updatedJob.Stage,
				"variants_completed", updatedJob.VariantsCompleted,
				"terminal_committed_at", updatedJob.TerminalCommittedAt,
				"post_commit_time", time.Now().UTC().Format(time.RFC3339Nano),
			)

			if a.notifier != nil {
				a.notifier.Notify(job.ID)
				a.notifier.Notify(job.PublicID)
			}
		}
	}
}
