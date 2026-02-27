package logger

import (
	"context"
	"github.com/snowflakedb/gosnowflake/v2/sflog"
	"io"
	"testing"
)

// mockLogger is a simple logger implementation for testing
type mockLogger struct {
	lastMessage string
}

func (m *mockLogger) Tracef(format string, args ...interface{}) {}
func (m *mockLogger) Debugf(format string, args ...interface{}) {}
func (m *mockLogger) Infof(format string, args ...interface{})  {}
func (m *mockLogger) Warnf(format string, args ...interface{})  {}
func (m *mockLogger) Errorf(format string, args ...interface{}) {}
func (m *mockLogger) Fatalf(format string, args ...interface{}) {}

func (m *mockLogger) Trace(msg string) {}
func (m *mockLogger) Debug(msg string) {}
func (m *mockLogger) Info(msg string)  { m.lastMessage = msg }
func (m *mockLogger) Warn(msg string)  {}
func (m *mockLogger) Error(msg string) {}
func (m *mockLogger) Fatal(msg string) {}

func (m *mockLogger) WithField(key string, value interface{}) LogEntry { return m }
func (m *mockLogger) WithFields(fields map[string]any) LogEntry        { return m }
func (m *mockLogger) WithContext(ctx context.Context) LogEntry         { return m }
func (m *mockLogger) SetLogLevel(level string) error                   { return nil }
func (m *mockLogger) SetLogLevelInt(level sflog.Level) error           { return nil }
func (m *mockLogger) GetLogLevel() string                              { return "info" }
func (m *mockLogger) GetLogLevelInt() sflog.Level                      { return sflog.LevelInfo }
func (m *mockLogger) SetOutput(output io.Writer)                       {}

// Compile-time verification that mockLogger implements SFLogger
var _ SFLogger = (*mockLogger)(nil)

func TestSecretMaskingLogger(t *testing.T) {
	mock := &mockLogger{}
	logger := newSecretMaskingLogger(mock)

	// Use a real password pattern that will be masked
	logger.Infof("test message with %s", "password:secret123")

	// Secret masking logger formats the message, masks it, then passes with "%s" format
	if mock.lastMessage != "test message with password:****" {
		t.Errorf("Expected format string to be '%%s', got %s", mock.lastMessage)
	}

	// The masked message should have been passed as the first arg
	// (We can't check this with the current mock, but we verified it works in other tests)
}
