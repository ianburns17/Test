package transform

import (
	"fmt"
	"image"
	"image/color"
	_ "image/gif"
	"image/jpeg"
	"image/png"
	"math"
	"os"
	"path/filepath"

	"golang.org/x/image/draw"
)

// VariantResult holds metadata for a successfully generated variant.
type VariantResult struct {
	Name        string
	StoragePath string
	MimeType    string
	FileSize    int64
	Width       int
	Height      int
}

// GenerateAllVariants processes the source image and generates thumbnail, preview, and display variants
// according to the required contracts in Section 9 of the specification.
func GenerateAllVariants(srcPath, variantsDir, mimeType, fileID string) ([]VariantResult, error) {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open source image: %w", err)
	}
	defer srcFile.Close()

	// Decode source image
	srcImg, format, err := image.Decode(srcFile)
	if err != nil {
		return nil, fmt.Errorf("failed to decode source image: %w", err)
	}

	// Ensure variants directory exists
	if err := os.MkdirAll(variantsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create variants directory: %w", err)
	}

	ext := ".jpg"
	targetMime := "image/jpeg"
	if format == "png" || mimeType == "image/png" {
		ext = ".png"
		targetMime = "image/png"
	}

	bounds := srcImg.Bounds()
	origW := bounds.Dx()
	origH := bounds.Dy()
	if origW <= 0 || origH <= 0 {
		return nil, fmt.Errorf("invalid image dimensions: %dx%d", origW, origH)
	}

	var results []VariantResult
	createdFiles := []string{}

	cleanupOnError := func() {
		for _, f := range createdFiles {
			_ = os.Remove(f)
		}
	}

	// 1. Thumbnail variant: Exact 150x150, center-cropped square (IMG-01, IMG-02)
	thumbImg := cropAndScaleSquare(srcImg, 150)
	thumbPath := filepath.Join(variantsDir, fmt.Sprintf("%s_thumbnail%s", fileID, ext))
	thumbSize, err := saveImage(thumbImg, thumbPath, targetMime)
	if err != nil {
		cleanupOnError()
		return nil, fmt.Errorf("failed to generate thumbnail: %w", err)
	}
	createdFiles = append(createdFiles, thumbPath)
	results = append(results, VariantResult{
		Name:        "thumbnail",
		StoragePath: filepath.ToSlash(thumbPath),
		MimeType:    targetMime,
		FileSize:    thumbSize,
		Width:       150,
		Height:      150,
	})

	// 2. Preview variant: Max 800x600, preserve aspect ratio, no distortion (IMG-02, IMG-04)
	prevW, prevH := fitWithinBounds(origW, origH, 800, 600)
	prevImg := scaleImage(srcImg, prevW, prevH)
	prevPath := filepath.Join(variantsDir, fmt.Sprintf("%s_preview%s", fileID, ext))
	prevSize, err := saveImage(prevImg, prevPath, targetMime)
	if err != nil {
		cleanupOnError()
		return nil, fmt.Errorf("failed to generate preview: %w", err)
	}
	createdFiles = append(createdFiles, prevPath)
	results = append(results, VariantResult{
		Name:        "preview",
		StoragePath: filepath.ToSlash(prevPath),
		MimeType:    targetMime,
		FileSize:    prevSize,
		Width:       prevW,
		Height:      prevH,
	})

	// 3. Display variant: Max 1200x900, preserve aspect ratio, no distortion (IMG-02, IMG-04)
	dispW, dispH := fitWithinBounds(origW, origH, 1200, 900)
	dispImg := scaleImage(srcImg, dispW, dispH)
	dispPath := filepath.Join(variantsDir, fmt.Sprintf("%s_display%s", fileID, ext))
	dispSize, err := saveImage(dispImg, dispPath, targetMime)
	if err != nil {
		cleanupOnError()
		return nil, fmt.Errorf("failed to generate display variant: %w", err)
	}
	createdFiles = append(createdFiles, dispPath)
	results = append(results, VariantResult{
		Name:        "display",
		StoragePath: filepath.ToSlash(dispPath),
		MimeType:    targetMime,
		FileSize:    dispSize,
		Width:       dispW,
		Height:      dispH,
	})

	return results, nil
}

// cropAndScaleSquare performs a center crop to a 1:1 aspect ratio, then resizes to targetSize x targetSize.
func cropAndScaleSquare(src image.Image, targetSize int) image.Image {
	bounds := src.Bounds()
	w := bounds.Dx()
	h := bounds.Dy()

	minDim := w
	if h < minDim {
		minDim = h
	}

	// Calculate center crop rectangle
	x0 := bounds.Min.X + (w-minDim)/2
	y0 := bounds.Min.Y + (h-minDim)/2
	cropRect := image.Rect(x0, y0, x0+minDim, y0+minDim)

	type subImager interface {
		SubImage(r image.Rectangle) image.Image
	}

	var cropped image.Image
	if si, ok := src.(subImager); ok {
		cropped = si.SubImage(cropRect)
	} else {
		cp := image.NewRGBA(image.Rect(0, 0, minDim, minDim))
		draw.Draw(cp, cp.Bounds(), src, cropRect.Min, draw.Src)
		cropped = cp
	}

	// Resample to targetSize x targetSize
	dst := image.NewRGBA(image.Rect(0, 0, targetSize, targetSize))
	draw.BiLinear.Scale(dst, dst.Bounds(), cropped, cropped.Bounds(), draw.Over, nil)
	return dst
}

// fitWithinBounds calculates actual dimensions constrained to maxWidth x maxHeight preserving aspect ratio.
// If the image is smaller than the target bounds, its original dimensions are preserved (no upscaling).
func fitWithinBounds(origW, origH, maxW, maxH int) (int, int) {
	if origW <= maxW && origH <= maxH {
		return origW, origH
	}

	scaleW := float64(maxW) / float64(origW)
	scaleH := float64(maxH) / float64(origH)
	scale := math.Min(scaleW, scaleH)

	targetW := int(math.Round(float64(origW) * scale))
	targetH := int(math.Round(float64(origH) * scale))

	if targetW < 1 {
		targetW = 1
	}
	if targetH < 1 {
		targetH = 1
	}

	return targetW, targetH
}

// scaleImage scales an image to targetW x targetH using bilinear interpolation.
func scaleImage(src image.Image, targetW, targetH int) image.Image {
	bounds := src.Bounds()
	if bounds.Dx() == targetW && bounds.Dy() == targetH {
		return src
	}

	dst := image.NewRGBA(image.Rect(0, 0, targetW, targetH))
	draw.BiLinear.Scale(dst, dst.Bounds(), src, bounds, draw.Over, nil)
	return dst
}

// saveImage encodes and saves the image to dstPath and returns the written file size.
func saveImage(img image.Image, dstPath, mimeType string) (int64, error) {
	out, err := os.Create(dstPath)
	if err != nil {
		return 0, err
	}
	defer out.Close()

	if mimeType == "image/png" {
		if err := png.Encode(out, img); err != nil {
			return 0, err
		}
	} else {
		// Default to JPEG with quality 85
		var opaqueImg image.Image = img
		if rgba, ok := img.(*image.RGBA); ok {
			hasAlpha := false
			for i := 3; i < len(rgba.Pix); i += 4 {
				if rgba.Pix[i] < 255 {
					hasAlpha = true
					break
				}
			}
			if hasAlpha {
				bg := image.NewRGBA(rgba.Bounds())
				draw.Draw(bg, bg.Bounds(), &image.Uniform{color.White}, image.Point{}, draw.Src)
				draw.Draw(bg, bg.Bounds(), rgba, image.Point{}, draw.Over)
				opaqueImg = bg
			}
		}

		if err := jpeg.Encode(out, opaqueImg, &jpeg.Options{Quality: 85}); err != nil {
			return 0, err
		}
	}

	stat, err := out.Stat()
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}
