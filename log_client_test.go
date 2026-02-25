package gosnowflake_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
)

// customLogger is a simple implementation of gosnowflake.SFLogger for testing
type customLogger struct {
	buf    *bytes.Buffer
	level  string
	fields map[string]interface{}
	mu     sync.Mutex
}

func newCustomLogger() *customLogger {
	return &customLogger{
		buf:    &bytes.Buffer{},
		level:  "info",
		fields: make(map[string]interface{}),
	}
}

func (l *customLogger) formatMessage(level, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf(format, args...)

	// Include fields if any
	fieldStr := ""
	if len(l.fields) > 0 {
		parts := []string{}
		for k, v := range l.fields {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		fieldStr = " " + strings.Join(parts, " ")
	}

	fmt.Fprintf(l.buf, "%s: %s%s\n", level, msg, fieldStr)
}

func (l *customLogger) Tracef(format string, args ...interface{}) {
	l.formatMessage("TRACE", format, args...)
}

func (l *customLogger) Debugf(format string, args ...interface{}) {
	l.formatMessage("DEBUG", format, args...)
}

func (l *customLogger) Infof(format string, args ...interface{}) {
	l.formatMessage("INFO", format, args...)
}

func (l *customLogger) Warnf(format string, args ...interface{}) {
	l.formatMessage("WARN", format, args...)
}

func (l *customLogger) Errorf(format string, args ...interface{}) {
	l.formatMessage("ERROR", format, args...)
}

func (l *customLogger) Fatalf(format string, args ...interface{}) {
	l.formatMessage("FATAL", format, args...)
}

func (l *customLogger) Trace(msg string) {
	l.formatMessage("TRACE", "%s", fmt.Sprint(msg))
}

func (l *customLogger) Debug(msg string) {
	l.formatMessage("DEBUG", "%s", fmt.Sprint(msg))
}

func (l *customLogger) Info(msg string) {
	l.formatMessage("INFO", "%s", fmt.Sprint(msg))
}

func (l *customLogger) Warn(msg string) {
	l.formatMessage("WARN", "%s", fmt.Sprint(msg))
}

func (l *customLogger) Error(msg string) {
	l.formatMessage("ERROR", "%s", fmt.Sprint(msg))
}

func (l *customLogger) Fatal(msg string) {
	l.formatMessage("FATAL", "%s", fmt.Sprint(msg))
}

func (l *customLogger) WithField(key string, value interface{}) gosnowflake.LogEntry {
	newFields := make(map[string]interface{})
	for k, v := range l.fields {
		newFields[k] = v
	}
	newFields[key] = value

	return &customLogEntry{
		logger: l,
		fields: newFields,
	}
}

func (l *customLogger) WithFields(fields map[string]any) gosnowflake.LogEntry {
	newFields := make(map[string]interface{})
	for k, v := range l.fields {
		newFields[k] = v
	}
	for k, v := range fields {
		newFields[k] = v
	}

	return &customLogEntry{
		logger: l,
		fields: newFields,
	}
}

func (l *customLogger) WithContext(ctx context.Context) gosnowflake.LogEntry {
	newFields := make(map[string]interface{})
	for k, v := range l.fields {
		newFields[k] = v
	}

	// Extract context fields
	if sessionID := ctx.Value(gosnowflake.SFSessionIDKey); sessionID != nil {
		newFields["LOG_SESSION_ID"] = sessionID
	}
	if user := ctx.Value(gosnowflake.SFSessionUserKey); user != nil {
		newFields["LOG_USER"] = user
	}

	return &customLogEntry{
		logger: l,
		fields: newFields,
	}
}

func (l *customLogger) SetLogLevel(level string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = strings.ToLower(level)
	return nil
}

func (l *customLogger) GetLogLevel() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

func (l *customLogger) SetOutput(output io.Writer) {
	// For this test logger, we keep using our internal buffer
}

func (l *customLogger) GetOutput() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *customLogger) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.Reset()
}

// customLogEntry implements gosnowflake.LogEntry
type customLogEntry struct {
	logger *customLogger
	fields map[string]interface{}
}

func (e *customLogEntry) formatMessage(level, format string, args ...interface{}) {
	e.logger.mu.Lock()
	defer e.logger.mu.Unlock()

	msg := fmt.Sprintf(format, args...)

	// Include fields
	fieldStr := ""
	if len(e.fields) > 0 {
		parts := []string{}
		for k, v := range e.fields {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		fieldStr = " " + strings.Join(parts, " ")
	}

	fmt.Fprintf(e.logger.buf, "%s: %s%s\n", level, msg, fieldStr)
}

func (e *customLogEntry) Tracef(format string, args ...interface{}) {
	e.formatMessage("TRACE", format, args...)
}

func (e *customLogEntry) Debugf(format string, args ...interface{}) {
	e.formatMessage("DEBUG", format, args...)
}

func (e *customLogEntry) Infof(format string, args ...interface{}) {
	e.formatMessage("INFO", format, args...)
}

func (e *customLogEntry) Warnf(format string, args ...interface{}) {
	e.formatMessage("WARN", format, args...)
}

func (e *customLogEntry) Errorf(format string, args ...interface{}) {
	e.formatMessage("ERROR", format, args...)
}

func (e *customLogEntry) Fatalf(format string, args ...interface{}) {
	e.formatMessage("FATAL", format, args...)
}

func (e *customLogEntry) Trace(msg string) {
	e.formatMessage("TRACE", "%s", fmt.Sprint(msg))
}

func (e *customLogEntry) Debug(msg string) {
	e.formatMessage("DEBUG", "%s", fmt.Sprint(msg))
}

func (e *customLogEntry) Info(msg string) {
	e.formatMessage("INFO", "%s", fmt.Sprint(msg))
}

func (e *customLogEntry) Warn(msg string) {
	e.formatMessage("WARN", "%s", fmt.Sprint(msg))
}

func (e *customLogEntry) Error(msg string) {
	e.formatMessage("ERROR", "%s", fmt.Sprint(msg))
}

func (e *customLogEntry) Fatal(msg string) {
	e.formatMessage("FATAL", "%s", fmt.Sprint(msg))
}

// Helper functions
func assertContains(t *testing.T, output, expected string) {
	t.Helper()
	if !strings.Contains(output, expected) {
		t.Errorf("Expected output to contain %q, got:\n%s", expected, output)
	}
}

func assertNotContains(t *testing.T, output, unexpected string) {
	t.Helper()
	if strings.Contains(output, unexpected) {
		t.Errorf("Expected output to NOT contain %q, got:\n%s", unexpected, output)
	}
}

func assertJSONFormat(t *testing.T, output string) {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		var js map[string]interface{}
		if err := json.Unmarshal([]byte(line), &js); err != nil {
			t.Errorf("Expected valid JSON, got error: %v, line: %s", err, line)
		}
	}
}

func TestCustomSlogHandler(t *testing.T) {
	// Save original logger
	originalLogger := gosnowflake.GetLogger()
	defer func() {
		gosnowflake.SetLogger(originalLogger)
	}()

	// Create a new default logger
	logger := gosnowflake.CreateDefaultLogger()

	// Set it as global logger first
	gosnowflake.SetLogger(logger)

	// Get the logger and try to set custom handler
	currentLogger := gosnowflake.GetLogger()

	// Type assert to SFSlogLogger
	slogLogger, ok := currentLogger.(gosnowflake.SFSlogLogger)
	if !ok {
		t.Fatal("Logger does not implement SFSlogLogger interface")
	}

	// Create custom JSON handler with buffer
	buf := &bytes.Buffer{}
	jsonHandler := slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	// Set the custom handler
	err := slogLogger.SetHandler(jsonHandler)
	if err != nil {
		t.Fatalf("Failed to set custom handler: %v", err)
	}

	// Log some messages
	_ = currentLogger.SetLogLevel("info")
	currentLogger.Info("Test message from custom JSON handler")
	currentLogger.Infof("Formatted message: %d", 42)

	// Verify output is in JSON format
	output := buf.String()
	assertJSONFormat(t, output)
	assertContains(t, output, "Test message from custom JSON handler")
	assertContains(t, output, "Formatted message: 42")
}

func TestCustomLoggerImplementation(t *testing.T) {
	// Save original logger
	originalLogger := gosnowflake.GetLogger()
	defer func() {
		gosnowflake.SetLogger(originalLogger)
	}()

	// Create custom logger
	customLog := newCustomLogger()
	var sfLogger gosnowflake.SFLogger = customLog

	// Set as global logger
	gosnowflake.SetLogger(sfLogger)

	// Get logger (should be proxied)
	logger := gosnowflake.GetLogger()

	// Log various messages
	logger.Info("Test info message")
	logger.Infof("Formatted: %s", "value")
	logger.Warn("Warning message")

	// Verify output
	output := customLog.GetOutput()
	assertContains(t, output, "INFO: Test info message")
	assertContains(t, output, "INFO: Formatted: value")
	assertContains(t, output, "WARN: Warning message")
}

func TestCustomLoggerSecretMasking(t *testing.T) {
	// Save original logger
	originalLogger := gosnowflake.GetLogger()
	defer func() {
		gosnowflake.SetLogger(originalLogger)
	}()

	// Create custom logger
	customLog := newCustomLogger()
	var sfLogger gosnowflake.SFLogger = customLog

	// Set as global logger
	gosnowflake.SetLogger(sfLogger)

	// Get logger
	logger := gosnowflake.GetLogger()

	// Log messages with secrets (use 8+ char secrets for detection)
	logger.Infof("Connection string: password='secret123'")
	logger.Info("Token: idToken:abc12345678")
	logger.Infof("Auth: token=def12345678")

	// Verify secrets are masked
	output := customLog.GetOutput()
	assertContains(t, output, "****")
	assertNotContains(t, output, "secret123")
	assertNotContains(t, output, "abc12345678") // pragma: allowlist secret
	assertNotContains(t, output, "def12345678") // pragma: allowlist secret
}

func TestCustomHandlerWithContext(t *testing.T) {
	// Save original logger
	originalLogger := gosnowflake.GetLogger()
	defer func() {
		gosnowflake.SetLogger(originalLogger)
	}()

	// Create a new default logger with JSON handler
	logger := gosnowflake.CreateDefaultLogger()
	gosnowflake.SetLogger(logger)

	currentLogger := gosnowflake.GetLogger()

	// Set custom JSON handler
	buf := &bytes.Buffer{}
	jsonHandler := slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	if slogLogger, ok := currentLogger.(gosnowflake.SFSlogLogger); ok {
		_ = slogLogger.SetHandler(jsonHandler)
	}

	// Create context with session info
	ctx := context.Background()
	ctx = context.WithValue(ctx, gosnowflake.SFSessionIDKey, "session-123")
	ctx = context.WithValue(ctx, gosnowflake.SFSessionUserKey, "test-user")

	// Log with context
	_ = currentLogger.SetLogLevel("info")
	currentLogger.WithContext(ctx).Info("Message with context")

	// Verify context fields in JSON output
	output := buf.String()
	assertJSONFormat(t, output)
	assertContains(t, output, "session-123")
	assertContains(t, output, "test-user")
}

func TestCustomLoggerWithFields(t *testing.T) {
	// Save original logger
	originalLogger := gosnowflake.GetLogger()
	defer func() {
		gosnowflake.SetLogger(originalLogger)
	}()

	// Create custom logger
	customLog := newCustomLogger()
	var sfLogger gosnowflake.SFLogger = customLog

	// Set as global logger
	gosnowflake.SetLogger(sfLogger)

	// Get logger
	logger := gosnowflake.GetLogger()

	// Use WithField
	logger.WithField("key1", "value1").Info("Message with field")

	// Use WithFields
	logger.WithFields(map[string]any{
		"key2": "value2",
		"key3": 123,
	}).Info("Message with multiple fields")

	// Verify fields in output
	output := customLog.GetOutput()
	assertContains(t, output, "key1=value1")
	assertContains(t, output, "key2=value2")
	assertContains(t, output, "key3=123")
}

func TestCustomLoggerLevelConfiguration(t *testing.T) {
	// Save original logger
	originalLogger := gosnowflake.GetLogger()
	defer func() {
		gosnowflake.SetLogger(originalLogger)
	}()

	// Create custom logger
	customLog := newCustomLogger()
	var sfLogger gosnowflake.SFLogger = customLog

	// Set as global logger
	gosnowflake.SetLogger(sfLogger)

	// Get logger
	logger := gosnowflake.GetLogger()

	// Set level to info
	err := logger.SetLogLevel("info")
	if err != nil {
		t.Fatalf("Failed to set log level: %v", err)
	}

	// Verify level
	if level := logger.GetLogLevel(); level != "info" {
		t.Errorf("Expected level 'info', got %q", level)
	}

	// Log at different levels
	logger.Debug("Debug message - should not appear at info level")
	logger.Info("Info message - should appear")

	// Check output
	output := customLog.GetOutput()

	// Note: Our custom logger doesn't implement level filtering
	// This test validates that the API works, actual filtering
	// would be implemented in a production custom logger
	assertContains(t, output, "INFO: Info message")
}

func TestCustomHandlerRestore(t *testing.T) {
	// Save original logger
	originalLogger := gosnowflake.GetLogger()
	defer func() {
		gosnowflake.SetLogger(originalLogger)
	}()

	// Create logger with JSON handler
	logger1 := gosnowflake.CreateDefaultLogger()
	gosnowflake.SetLogger(logger1)

	buf1 := &bytes.Buffer{}
	if slogLogger, ok := gosnowflake.GetLogger().(gosnowflake.SFSlogLogger); ok {
		jsonHandler := slog.NewJSONHandler(buf1, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
		_ = slogLogger.SetHandler(jsonHandler)
	}

	// Log with JSON handler
	_ = gosnowflake.GetLogger().SetLogLevel("info")
	gosnowflake.GetLogger().Info("JSON format message")

	// Verify JSON format
	output1 := buf1.String()
	assertJSONFormat(t, output1)
	assertContains(t, output1, "JSON format message")

	// Create new default logger (text format)
	logger2 := gosnowflake.CreateDefaultLogger()
	buf2 := &bytes.Buffer{}
	logger2.SetOutput(buf2)
	gosnowflake.SetLogger(logger2)

	// Log with default text handler
	_ = gosnowflake.GetLogger().SetLogLevel("info")
	gosnowflake.GetLogger().Info("Text format message")

	// Verify text format (not JSON)
	output2 := buf2.String()
	assertContains(t, output2, "Text format message")

	// Text format should have "level=" in it
	assertContains(t, output2, "level=")
}
