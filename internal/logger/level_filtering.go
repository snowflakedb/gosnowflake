package logger

import (
	"context"
	"io"
	"strings"

	"github.com/snowflakedb/gosnowflake/v2/loginterface"
)

// levelFilteringLogger wraps any logger and filters log messages based on log level.
// This prevents expensive operations (like secret masking and formatting) from running
// when the message wouldn't be logged anyway.
type levelFilteringLogger struct {
	inner SFLogger
}

// Compile-time verification that levelFilteringLogger implements SFLogger
var _ SFLogger = (*levelFilteringLogger)(nil)

// Unwrap returns the inner logger (for introspection by easy_logging)
func (l *levelFilteringLogger) Unwrap() interface{} {
	return l.inner
}

// Level values for comparison (matching slog conventions)
const (
	levelTraceValue = -8
	levelDebugValue = -4
	levelInfoValue  = 0
	levelWarnValue  = 4
	levelErrorValue = 8
	levelFatalValue = 12
	levelOffValue   = 999 // Higher than any real level
)

// levelValue returns the numeric value of a log level
func levelValue(level string) int {
	switch strings.ToUpper(level) {
	case "TRACE":
		return levelTraceValue
	case "DEBUG":
		return levelDebugValue
	case "INFO":
		return levelInfoValue
	case "WARN", "WARNING":
		return levelWarnValue
	case "ERROR":
		return levelErrorValue
	case "FATAL":
		return levelFatalValue
	case "OFF":
		return levelOffValue
	default:
		return levelInfoValue
	}
}

// shouldLog determines if a message at messageLevel should be logged
// given the current configured level
func (l *levelFilteringLogger) shouldLog(messageLevel string) bool {
	currentLevel := l.inner.GetLogLevel()
	return levelValue(messageLevel) >= levelValue(currentLevel)
}

// NewLevelFilteringLogger creates a new level filtering wrapper around the provided logger
func NewLevelFilteringLogger(inner SFLogger) SFLogger {
	if inner == nil {
		panic("inner logger cannot be nil")
	}
	return &levelFilteringLogger{inner: inner}
}

// Implement all formatted logging methods (*f variants)
func (l *levelFilteringLogger) Tracef(format string, args ...interface{}) {
	if !l.shouldLog("trace") {
		return
	}
	l.inner.Tracef(format, args...)
}

func (l *levelFilteringLogger) Debugf(format string, args ...interface{}) {
	if !l.shouldLog("debug") {
		return
	}
	l.inner.Debugf(format, args...)
}

func (l *levelFilteringLogger) Infof(format string, args ...interface{}) {
	if !l.shouldLog("info") {
		return
	}
	l.inner.Infof(format, args...)
}

func (l *levelFilteringLogger) Warnf(format string, args ...interface{}) {
	if !l.shouldLog("warn") {
		return
	}
	l.inner.Warnf(format, args...)
}

func (l *levelFilteringLogger) Errorf(format string, args ...interface{}) {
	if !l.shouldLog("error") {
		return
	}
	l.inner.Errorf(format, args...)
}

func (l *levelFilteringLogger) Fatalf(format string, args ...interface{}) {
	if !l.shouldLog("fatal") {
		return
	}
	l.inner.Fatalf(format, args...)
}

// Implement all direct logging methods
func (l *levelFilteringLogger) Trace(msg string) {
	if !l.shouldLog("trace") {
		return
	}
	l.inner.Trace(msg)
}

func (l *levelFilteringLogger) Debug(msg string) {
	if !l.shouldLog("debug") {
		return
	}
	l.inner.Debug(msg)
}

func (l *levelFilteringLogger) Info(msg string) {
	if !l.shouldLog("info") {
		return
	}
	l.inner.Info(msg)
}

func (l *levelFilteringLogger) Warn(msg string) {
	if !l.shouldLog("warn") {
		return
	}
	l.inner.Warn(msg)
}

func (l *levelFilteringLogger) Error(msg string) {
	if !l.shouldLog("error") {
		return
	}
	l.inner.Error(msg)
}

func (l *levelFilteringLogger) Fatal(msg string) {
	if !l.shouldLog("fatal") {
		return
	}
	l.inner.Fatal(msg)
}

// Implement structured logging methods - these return wrapped entries
func (l *levelFilteringLogger) WithField(key string, value interface{}) loginterface.LogEntry {
	innerEntry := l.inner.WithField(key, value)
	return &levelFilteringEntry{
		parent: l,
		inner:  innerEntry,
	}
}

func (l *levelFilteringLogger) WithFields(fields map[string]any) loginterface.LogEntry {
	innerEntry := l.inner.WithFields(fields)
	return &levelFilteringEntry{
		parent: l,
		inner:  innerEntry,
	}
}

func (l *levelFilteringLogger) WithContext(ctx context.Context) loginterface.LogEntry {
	innerEntry := l.inner.WithContext(ctx)
	return &levelFilteringEntry{
		parent: l,
		inner:  innerEntry,
	}
}

// Delegate configuration methods to inner logger
func (l *levelFilteringLogger) SetLogLevel(level string) error {
	return l.inner.SetLogLevel(level)
}

func (l *levelFilteringLogger) GetLogLevel() string {
	return l.inner.GetLogLevel()
}

func (l *levelFilteringLogger) SetOutput(output io.Writer) {
	l.inner.SetOutput(output)
}

// SetHandler implements SFSlogLogger interface for advanced slog handler configuration
func (l *levelFilteringLogger) SetHandler(handler interface{}) error {
	// Try to delegate to inner logger if it supports SetHandler
	type setHandlerLogger interface{ SetHandler(interface{}) error }
	if sh, ok := l.inner.(setHandlerLogger); ok {
		return sh.SetHandler(handler)
	}
	return &loggerError{message: "underlying logger does not support SetHandler"}
}

// levelFilteringEntry wraps a log entry and filters by level
type levelFilteringEntry struct {
	parent *levelFilteringLogger
	inner  loginterface.LogEntry
}

// Implement all formatted logging methods for entry
func (e *levelFilteringEntry) Tracef(format string, args ...interface{}) {
	if !e.parent.shouldLog("trace") {
		return
	}
	e.inner.Tracef(format, args...)
}

func (e *levelFilteringEntry) Debugf(format string, args ...interface{}) {
	if !e.parent.shouldLog("debug") {
		return
	}
	e.inner.Debugf(format, args...)
}

func (e *levelFilteringEntry) Infof(format string, args ...interface{}) {
	if !e.parent.shouldLog("info") {
		return
	}
	e.inner.Infof(format, args...)
}

func (e *levelFilteringEntry) Warnf(format string, args ...interface{}) {
	if !e.parent.shouldLog("warn") {
		return
	}
	e.inner.Warnf(format, args...)
}

func (e *levelFilteringEntry) Errorf(format string, args ...interface{}) {
	if !e.parent.shouldLog("error") {
		return
	}
	e.inner.Errorf(format, args...)
}

func (e *levelFilteringEntry) Fatalf(format string, args ...interface{}) {
	if !e.parent.shouldLog("fatal") {
		return
	}
	e.inner.Fatalf(format, args...)
}

// Implement all direct logging methods for entry
func (e *levelFilteringEntry) Trace(msg string) {
	if !e.parent.shouldLog("trace") {
		return
	}
	e.inner.Trace(msg)
}

func (e *levelFilteringEntry) Debug(msg string) {
	if !e.parent.shouldLog("debug") {
		return
	}
	e.inner.Debug(msg)
}

func (e *levelFilteringEntry) Info(msg string) {
	if !e.parent.shouldLog("info") {
		return
	}
	e.inner.Info(msg)
}

func (e *levelFilteringEntry) Warn(msg string) {
	if !e.parent.shouldLog("warn") {
		return
	}
	e.inner.Warn(msg)
}

func (e *levelFilteringEntry) Error(msg string) {
	if !e.parent.shouldLog("error") {
		return
	}
	e.inner.Error(msg)
}

func (e *levelFilteringEntry) Fatal(msg string) {
	if !e.parent.shouldLog("fatal") {
		return
	}
	e.inner.Fatal(msg)
}
