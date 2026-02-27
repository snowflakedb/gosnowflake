package logger_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2/internal/logger"
)

// TestLoggerConfiguration verifies configuration methods work
func TestLoggerConfiguration(t *testing.T) {
	log := logger.CreateDefaultLogger()

	// Get current level
	level := log.GetLogLevel()
	if level == "" {
		t.Error("Expected non-empty log level")
	}
	t.Logf("Current log level: %s", level)

	// Set log level
	err := log.SetLogLevel("debug")
	if err != nil {
		t.Errorf("SetLogLevel failed: %v", err)
	}

	// Verify it changed
	newLevel := log.GetLogLevel()
	if newLevel != "DEBUG" {
		t.Errorf("Expected 'debug', got '%s'", newLevel)
	}
}

// TestLoggerSecretMasking verifies secret masking works
func TestLoggerSecretMasking(t *testing.T) {
	log := logger.CreateDefaultLogger()

	var buf bytes.Buffer
	log.SetOutput(&buf)
	// Reset log level to ensure info is logged
	_ = log.SetLogLevel("info")

	// Log a secret
	log.Infof("password=%s", "secret12345")

	output := buf.String()
	t.Logf("Output: %s", output) // Debug output

	// The output should have a masked secret
	if strings.Contains(output, "secret12345") {
		t.Errorf("Secret masking FAILED: secret leaked in: %s", output)
	}

	// Verify the message was logged (check for "password=")
	if !strings.Contains(output, "password=") {
		t.Errorf("Message not logged: %s", output)
	}

	t.Log("Secret masking works with GetLogger")
}

// TestLoggerAllMethods verifies all logging methods are available and produce output
func TestLoggerAllMethods(t *testing.T) {
	log := logger.CreateDefaultLogger()

	var buf bytes.Buffer
	log.SetOutput(&buf)
	_ = log.SetLogLevel("trace")

	// Test all formatted methods
	log.Tracef("trace %s", "formatted")
	log.Debugf("debug %s", "formatted")
	log.Infof("info %s", "formatted")
	log.Warnf("warn %s", "formatted")
	log.Errorf("error %s", "formatted")
	// Fatalf would exit, so skip in test

	// Test all direct methods
	log.Trace("trace direct")
	log.Debug("debug direct")
	log.Info("info direct")
	log.Warn("warn direct")
	log.Error("error direct")
	// Fatal would exit, so skip in test

	output := buf.String()

	// Verify all messages appear in output
	expectedMessages := []string{
		"trace formatted", "debug formatted", "info formatted",
		"warn formatted", "error formatted",
		"trace direct", "debug direct", "info direct",
		"warn direct", "error direct",
	}

	for _, msg := range expectedMessages {
		if !strings.Contains(output, msg) {
			t.Errorf("Expected output to contain '%s', got: %s", msg, output)
		}
	}
}

// TestLoggerLevelFiltering verifies log level filtering works correctly
func TestLoggerLevelFiltering(t *testing.T) {
	log := logger.CreateDefaultLogger()

	var buf bytes.Buffer
	log.SetOutput(&buf)

	// Set to INFO level
	_ = log.SetLogLevel("info")

	// Log at different levels
	log.Debug("this should not appear")
	log.Info("this should appear")
	log.Warn("this should also appear")

	output := buf.String()

	// Debug should not appear
	if strings.Contains(output, "this should not appear") {
		t.Errorf("Debug message appeared when log level is INFO: %s", output)
	}

	// Info and Warn should appear
	if !strings.Contains(output, "this should appear") {
		t.Errorf("Info message did not appear: %s", output)
	}
	if !strings.Contains(output, "this should also appear") {
		t.Errorf("Warn message did not appear: %s", output)
	}

	t.Log("Log level filtering works correctly")
}

// TestLogEntry verifies log entry methods and field inclusion
func TestLogEntry(t *testing.T) {
	log := logger.CreateDefaultLogger()

	var buf bytes.Buffer
	log.SetOutput(&buf)
	_ = log.SetLogLevel("info")

	// Get entry with field
	entry := log.WithField("module", "test")

	// Log with the entry
	entry.Infof("info with field %s", "formatted")
	entry.Info("info with field direct")

	output := buf.String()

	// Verify messages appear
	if !strings.Contains(output, "info with field formatted") {
		t.Errorf("Expected formatted message in output: %s", output)
	}
	if !strings.Contains(output, "info with field direct") {
		t.Errorf("Expected direct message in output: %s", output)
	}

	// Verify field appears in output
	if !strings.Contains(output, "module") || !strings.Contains(output, "test") {
		t.Errorf("Expected field 'module=test' in output: %s", output)
	}

	t.Log("LogEntry methods work correctly")
}

// TestLogEntryWithFields verifies WithFields works correctly
func TestLogEntryWithFields(t *testing.T) {
	log := logger.CreateDefaultLogger()

	var buf bytes.Buffer
	log.SetOutput(&buf)
	_ = log.SetLogLevel("info")

	// Get entry with multiple fields
	entry := log.WithFields(map[string]any{
		"requestId": "123-456",
		"userId":    42,
	})

	entry.Info("processing request")

	output := buf.String()

	// Verify message appears
	if !strings.Contains(output, "processing request") {
		t.Errorf("Expected message in output: %s", output)
	}

	// Verify both fields appear
	if !strings.Contains(output, "requestId") {
		t.Errorf("Expected 'requestId' field in output: %s", output)
	}
	if !strings.Contains(output, "123-456") {
		t.Errorf("Expected '123-456' value in output: %s", output)
	}
	if !strings.Contains(output, "userId") {
		t.Errorf("Expected 'userId' field in output: %s", output)
	}

	t.Log("WithFields works correctly")
}

// TestSetOutput verifies output redirection works correctly
func TestSetOutput(t *testing.T) {
	log := logger.CreateDefaultLogger()

	// Test with first buffer
	var buf1 bytes.Buffer
	log.SetOutput(&buf1)
	_ = log.SetLogLevel("info")

	log.Info("message to buffer 1")

	if !strings.Contains(buf1.String(), "message to buffer 1") {
		t.Errorf("Expected message in buffer 1: %s", buf1.String())
	}

	// Switch to second buffer
	var buf2 bytes.Buffer
	log.SetOutput(&buf2)

	log.Info("message to buffer 2")

	// Should appear only in buf2
	if !strings.Contains(buf2.String(), "message to buffer 2") {
		t.Errorf("Expected message in buffer 2: %s", buf2.String())
	}

	// Should NOT appear in buf1
	if strings.Contains(buf1.String(), "message to buffer 2") {
		t.Errorf("Message should not appear in buffer 1: %s", buf1.String())
	}

	t.Log("SetOutput correctly redirects log output")
}

// TestLogEntryWithContext verifies WithContext works correctly
func TestLogEntryWithContext(t *testing.T) {
	log := logger.CreateDefaultLogger()

	var buf bytes.Buffer
	log.SetOutput(&buf)
	_ = log.SetLogLevel("info")

	// Create type to avoid collisions
	type contextKey string

	// Create context with values
	ctx := context.WithValue(context.Background(), contextKey("traceId"), "trace-123")

	// Get entry with context
	entry := log.WithContext(ctx)

	entry.Info("message with context")

	output := buf.String()

	// Verify message appears
	if !strings.Contains(output, "message with context") {
		t.Errorf("Expected message in output: %s", output)
	}
}
