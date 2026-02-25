package logger

import (
	"bytes"
	"strings"
	"testing"
)

// IMPORTANT: The skip depth values in rawLogger and slogEntry assume the standard wrapper chain:
// For logger methods: levelFilteringLogger -> secretMaskingLogger -> rawLogger (skip=3)
// For entry methods: levelFilteringEntry -> secretMaskingEntry -> slogEntry (skip=3)
//
// These tests verify the standard configuration. If you add or remove wrapper layers, you MUST update:
// - internal/logger/slog_logger.go: rawLogger methods (currently skip=3)
// - internal/logger/slog_logger.go: slogEntry methods (currently skip=3)

// TestSourceLocationWithLevelFiltering verifies that source location is correct
// with the standard wrapper chain: levelFilteringLogger -> secretMaskingLogger -> rawLogger
func TestSourceLocationWithLevelFiltering(t *testing.T) {
	innerLogger := newRawLogger()
	var buf bytes.Buffer
	innerLogger.SetOutput(&buf)
	_ = innerLogger.SetLogLevel("debug")

	// Build the standard wrapper chain
	masked := newSecretMaskingLogger(innerLogger)
	filtered := newLevelFilteringLogger(masked)

	filtered.Debug("test message") // Line 31 - This line should appear in source location

	output := buf.String()
	// Check that the source location points to this test file, not the wrappers
	if !strings.Contains(output, "source_location_test.go") {
		t.Errorf("Expected source location to contain 'source_location_test.go', got: %s", output)
	}
	if strings.Contains(output, "level_filtering.go") {
		t.Errorf("Source location should not contain 'level_filtering.go', got: %s", output)
	}
	if strings.Contains(output, "secret_masking.go") {
		t.Errorf("Source location should not contain 'secret_masking.go', got: %s", output)
	}
}

// TestSourceLocationWithDebugf verifies formatted logging also reports correct source
func TestSourceLocationWithDebugf(t *testing.T) {
	innerLogger := newRawLogger()
	var buf bytes.Buffer
	innerLogger.SetOutput(&buf)
	_ = innerLogger.SetLogLevel("debug")

	// Build the standard wrapper chain
	masked := newSecretMaskingLogger(innerLogger)
	filtered := newLevelFilteringLogger(masked)

	filtered.Debugf("formatted message: %s", "test") // Line 58 - This line should appear

	output := buf.String()
	if !strings.Contains(output, "source_location_test.go") {
		t.Errorf("Expected source location to contain 'source_location_test.go', got: %s", output)
	}
	if strings.Contains(output, "level_filtering.go") || strings.Contains(output, "secret_masking.go") {
		t.Errorf("Source location should not contain wrapper files, got: %s", output)
	}
}

// TestSourceLocationWithEntry verifies that structured logging (WithField) also works correctly
func TestSourceLocationWithEntry(t *testing.T) {
	innerLogger := newRawLogger()
	var buf bytes.Buffer
	innerLogger.SetOutput(&buf)
	_ = innerLogger.SetLogLevel("debug")

	// Build the standard wrapper chain
	masked := newSecretMaskingLogger(innerLogger)
	filtered := newLevelFilteringLogger(masked)

	filtered.WithField("key", "value").Debug("entry message") // Line 82 - This line should appear

	output := buf.String()
	if !strings.Contains(output, "source_location_test.go") {
		t.Errorf("Expected source location to contain 'source_location_test.go', got: %s", output)
	}
	// Also verify the field is present
	if !strings.Contains(output, "key=value") {
		t.Errorf("Expected output to contain 'key=value', got: %s", output)
	}
}

// TestSkipDepthWarning documents the skip depth assumption and fails if wrappers change
// This test intentionally checks implementation details to warn developers when skip depths need updating.
func TestSkipDepthWarning(t *testing.T) {
	innerLogger := newRawLogger()
	var buf bytes.Buffer
	innerLogger.SetOutput(&buf)
	_ = innerLogger.SetLogLevel("debug")

	// Build the expected standard wrapper chain
	masked := newSecretMaskingLogger(innerLogger)
	filtered := newLevelFilteringLogger(masked)

	// Log from this test
	filtered.Debug("skip depth test") // Line 102 - This line should appear in source location

	output := buf.String()

	if !strings.Contains(output, "source_location_test.go:102") {
		t.Errorf(`
Skip depth appears incorrect!

Expected source location: source_location_test.go:102
Got: %s

If you added/removed a wrapper layer, update the skip values in:
  - internal/logger/slog_logger.go: rawLogger methods (currently skip=3)
  - internal/logger/slog_logger.go: slogEntry methods (currently skip=3)

Current wrapper chain for logger methods:
  Driver code -> levelFilteringLogger -> secretMaskingLogger -> rawLogger

Current wrapper chain for entry methods:
  Driver code -> levelFilteringEntry -> secretMaskingEntry -> slogEntry
`, output)
	}
}
