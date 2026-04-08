package logger

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2/sflog"
)

func newProxyTestLogger(t *testing.T) (SFLogger, *bytes.Buffer, func()) {
	t.Helper()

	buf := &bytes.Buffer{}
	opts := &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}
	handler := slog.NewTextHandler(buf, opts)
	snowHandler := newSnowflakeHandler(handler, sflog.LevelDebug)
	rawLog := &rawLogger{
		inner:   slog.New(snowHandler),
		handler: snowHandler,
		level:   sflog.LevelDebug,
		enabled: true,
		output:  buf,
	}

	masked := newSecretMaskingLogger(rawLog)
	filtered := newLevelFilteringLogger(masked)

	loggerAccessorMu.Lock()
	oldLogger := globalLogger
	globalLogger = filtered
	loggerAccessorMu.Unlock()

	cleanup := func() {
		loggerAccessorMu.Lock()
		globalLogger = oldLogger
		loggerAccessorMu.Unlock()
	}

	return NewLoggerProxy(), buf, cleanup
}

// TestProxyCorrectSourceLocation verifies that the Proxy reports the correct
// source file, not the proxy.go location.
func TestProxyCorrectSourceLocation(t *testing.T) {
	proxy, buf, cleanup := newProxyTestLogger(t)
	defer cleanup()
	// Log a message - this is the line we expect to see in the source
	proxy.Debug("test message from proxy") // This line number should appear in logs

	// Get the output
	output := buf.String()

	// Verify that the source contains "proxy_test.go"
	if !strings.Contains(output, "proxy_test.go:") {
		t.Errorf("Expected source to contain 'proxy_test.go', got: %s", output)
	}

	// Verify that the source does NOT contain "proxy.go"
	if strings.Contains(output, "proxy.go:") {
		t.Errorf("Source should not contain 'proxy.go', got: %s", output)
	}

	// Verify the message is present
	if !strings.Contains(output, "test message from proxy") {
		t.Errorf("Expected message 'test message from proxy', got: %s", output)
	}
}

// TestProxyWithContextSourceLocation verifies that WithContext still works correctly
func TestProxyWithContextSourceLocation(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer

	// Create a raw logger with the buffer as output
	opts := &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}
	handler := slog.NewTextHandler(&buf, opts)
	snowHandler := newSnowflakeHandler(handler, sflog.LevelDebug)
	rawLog := &rawLogger{
		inner:   slog.New(snowHandler),
		handler: snowHandler,
		level:   sflog.LevelDebug,
		enabled: true,
		output:  &buf,
	}

	// Wrap with secret masking and level filtering
	masked := newSecretMaskingLogger(rawLog)
	filtered := newLevelFilteringLogger(masked)

	// Set as global logger
	loggerAccessorMu.Lock()
	oldLogger := globalLogger
	globalLogger = filtered
	loggerAccessorMu.Unlock()

	defer func() {
		loggerAccessorMu.Lock()
		globalLogger = oldLogger
		loggerAccessorMu.Unlock()
	}()

	// Create a proxy and log through it with WithContext
	proxy := NewLoggerProxy()

	// Log a message with WithContext
	entry := proxy.WithContext(context.Background())
	entry.Debug("test message with context") // This line number should appear in logs

	// Get the output
	output := buf.String()

	// Verify that the source contains "proxy_test.go"
	if !strings.Contains(output, "proxy_test.go:") {
		t.Errorf("Expected source to contain 'proxy_test.go', got: %s", output)
	}

	// Verify the message is present
	if !strings.Contains(output, "test message with context") {
		t.Errorf("Expected message 'test message with context', got: %s", output)
	}
}
