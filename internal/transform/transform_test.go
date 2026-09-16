package transform

import (
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"testing"
)

func createTestImage(t *testing.T, dir, filename string, width, height int, isPNG bool) string {
	filePath := filepath.Join(dir, filename)
	img := image.NewRGBA(image.Rect(0, 0, width, height))

	// Fill with distinct test color
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y % 256), B: 180, A: 255})
		}
	}

	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create test image file: %v", err)
	}
	defer f.Close()

	if isPNG {
		if err := png.Encode(f, img); err != nil {
			t.Fatalf("failed to encode png: %v", err)
		}
	} else {
		if err := jpeg.Encode(f, img, &jpeg.Options{Quality: 90}); err != nil {
			t.Fatalf("failed to encode jpeg: %v", err)
		}
	}

	return filePath
}

func TestGenerateAllVariants_LandscapeJPEG(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "transform_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	variantsDir := filepath.Join(tempDir, "variants")
	srcPath := createTestImage(t, tempDir, "landscape.jpg", 1600, 1200, false)

	results, err := GenerateAllVariants(srcPath, variantsDir, "image/jpeg", "test_id_1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 variants, got %d", len(results))
	}

	// 1. Thumbnail: exact 150x150
	thumb := results[0]
	if thumb.Name != "thumbnail" || thumb.Width != 150 || thumb.Height != 150 {
		t.Errorf("expected thumbnail to be 150x150, got %dx%d (%s)", thumb.Width, thumb.Height, thumb.Name)
	}
	if _, err := os.Stat(thumb.StoragePath); err != nil {
		t.Errorf("thumbnail file missing on disk: %v", err)
	}

	// 2. Preview: 1600x1200 scaled to max 800x600 -> exactly 800x600 (4:3 ratio)
	prev := results[1]
	if prev.Name != "preview" || prev.Width != 800 || prev.Height != 600 {
		t.Errorf("expected preview 800x600, got %dx%d", prev.Width, prev.Height)
	}
	if _, err := os.Stat(prev.StoragePath); err != nil {
		t.Errorf("preview file missing on disk: %v", err)
	}

	// 3. Display: 1600x1200 scaled to max 1200x900 -> exactly 1200x900 (4:3 ratio)
	disp := results[2]
	if disp.Name != "display" || disp.Width != 1200 || disp.Height != 900 {
		t.Errorf("expected display 1200x900, got %dx%d", disp.Width, disp.Height)
	}
	if _, err := os.Stat(disp.StoragePath); err != nil {
		t.Errorf("display file missing on disk: %v", err)
	}
}

func TestGenerateAllVariants_PortraitPNG(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "transform_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	variantsDir := filepath.Join(tempDir, "variants")
	// 600x1200 portrait (1:2 aspect ratio)
	srcPath := createTestImage(t, tempDir, "portrait.png", 600, 1200, true)

	results, err := GenerateAllVariants(srcPath, variantsDir, "image/png", "test_id_2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 variants, got %d", len(results))
	}

	// Thumbnail: 150x150
	if results[0].Width != 150 || results[0].Height != 150 {
		t.Errorf("expected thumbnail 150x150, got %dx%d", results[0].Width, results[0].Height)
	}

	// Preview: max 800x600. For 600x1200 portrait, height dominates: 1200 -> 600 (scale 0.5), width 600 * 0.5 = 300
	if results[1].Width != 300 || results[1].Height != 600 {
		t.Errorf("expected preview 300x600, got %dx%d", results[1].Width, results[1].Height)
	}

	// Display: max 1200x900. For 600x1200 portrait, height dominates: 1200 -> 900 (scale 0.75), width 600 * 0.75 = 450
	if results[2].Width != 450 || results[2].Height != 900 {
		t.Errorf("expected display 450x900, got %dx%d", results[2].Width, results[2].Height)
	}
}
