package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestSourceLocationWithLevelFiltering(t *testing.T) {
	innerLogger := newRawLogger()
	var buf bytes.Buffer
	innerLogger.SetOutput(&buf)
	_ = innerLogger.SetLogLevel("debug")

	// Build the standard wrapper chain
	masked := newSecretMaskingLogger(innerLogger)
	filtered := newLevelFilteringLogger(masked)

	filtered.Debug("test message")

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

func TestSourceLocationWithDebugf(t *testing.T) {
	innerLogger := newRawLogger()
	var buf bytes.Buffer
	innerLogger.SetOutput(&buf)
	_ = innerLogger.SetLogLevel("debug")

	// Build the standard wrapper chain
	masked := newSecretMaskingLogger(innerLogger)
	filtered := newLevelFilteringLogger(masked)

	filtered.Debugf("formatted message: %s", "test")

	output := buf.String()
	if !strings.Contains(output, "source_location_test.go") {
		t.Errorf("Expected source location to contain 'source_location_test.go', got: %s", output)
	}
	if strings.Contains(output, "level_filtering.go") || strings.Contains(output, "secret_masking.go") {
		t.Errorf("Source location should not contain wrapper files, got: %s", output)
	}
}

func TestSourceLocationWithEntry(t *testing.T) {
	innerLogger := newRawLogger()
	var buf bytes.Buffer
	innerLogger.SetOutput(&buf)
	_ = innerLogger.SetLogLevel("debug")

	// Build the standard wrapper chain
	masked := newSecretMaskingLogger(innerLogger)
	filtered := newLevelFilteringLogger(masked)

	filtered.WithField("key", "value").Debug("entry message")

	output := buf.String()
	if !strings.Contains(output, "source_location_test.go") {
		t.Errorf("Expected source location to contain 'source_location_test.go', got: %s", output)
	}
	// Also verify the field is present
	if !strings.Contains(output, "key=value") {
		t.Errorf("Expected output to contain 'key=value', got: %s", output)
	}
}
