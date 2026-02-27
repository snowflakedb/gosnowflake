package logger

import (
	"context"
	"fmt"
	"github.com/snowflakedb/gosnowflake/v2/sflog"
	"io"
	"log/slog"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"
)

// formatSource formats caller information for logging
func formatSource(frame *runtime.Frame) (string, string) {
	return path.Base(frame.Function), fmt.Sprintf("%s:%d", path.Base(frame.File), frame.Line)
}

// rawLogger implements SFLogger using slog
type rawLogger struct {
	inner   *slog.Logger
	handler *snowflakeHandler
	level   sflog.Level
	enabled bool // For OFF level support
	file    *os.File
	output  io.Writer
	mu      sync.Mutex
}

// Compile-time verification that rawLogger implements SFLogger
var _ SFLogger = (*rawLogger)(nil)

// newRawLogger creates the internal default logger using slog
func newRawLogger() SFLogger {
	level := sflog.LevelInfo

	opts := createOpts(slog.Level(level))

	textHandler := slog.NewTextHandler(os.Stderr, opts)
	handler := newSnowflakeHandler(textHandler, level)

	slogLogger := slog.New(handler)

	return &rawLogger{
		inner:   slogLogger,
		handler: handler,
		level:   level,
		enabled: true,
		output:  os.Stderr,
	}
}

// isEnabled checks if logging is enabled (for OFF level)
func (log *rawLogger) isEnabled() bool {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.enabled
}

// SetLogLevel sets the log level
func (log *rawLogger) SetLogLevel(level string) error {
	upperLevel, err := sflog.ParseLevel(strings.ToUpper(level))
	if err != nil {
		return fmt.Errorf("error while setting log level. %v", err)
	}

	if upperLevel == sflog.LevelOff {
		log.mu.Lock()
		log.level = sflog.LevelOff
		log.enabled = false
		log.mu.Unlock()
		return nil
	}

	log.mu.Lock()
	log.enabled = true
	log.level = upperLevel
	log.mu.Unlock()

	return nil
}

func (log *rawLogger) SetLogLevelInt(level sflog.Level) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	_, err := sflog.LevelToString(level)
	if err != nil {
		return fmt.Errorf("invalid log level: %d", level)
	}
	log.level = level
	return nil
}

// GetLogLevel returns the current log level
func (log *rawLogger) GetLogLevel() string {
	if levelStr, err := sflog.LevelToString(log.level); err == nil {
		return levelStr
	}
	return "unknown"
}

func (log *rawLogger) GetLogLevelInt() sflog.Level {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.level
}

// SetOutput sets the output writer
func (log *rawLogger) SetOutput(output io.Writer) {
	log.mu.Lock()
	defer log.mu.Unlock()

	log.output = output

	// Create new handler with new output
	opts := createOpts(slog.Level(log.level))

	textHandler := slog.NewTextHandler(output, opts)
	log.handler = newSnowflakeHandler(textHandler, log.level)
	log.inner = slog.New(log.handler)
}

func createOpts(level slog.Level) *slog.HandlerOptions {
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				if t, ok := a.Value.Any().(time.Time); ok {
					return slog.String(slog.TimeKey, t.Format(time.RFC3339Nano))
				}
			}
			if a.Key == slog.SourceKey {
				if src, ok := a.Value.Any().(*slog.Source); ok {
					frame := &runtime.Frame{
						File:     src.File,
						Line:     src.Line,
						Function: src.Function,
					}
					_, location := formatSource(frame)
					return slog.String(slog.SourceKey, location)
				}
			}
			return a
		},
	}
	return opts
}

// SetHandler sets a custom slog handler (implements SFSlogLogger interface)
// The provided handler will be wrapped with snowflakeHandler to preserve context extraction.
// Secret masking is handled at a higher level (secretMaskingLogger wrapper).
func (log *rawLogger) SetHandler(handler slog.Handler) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	// Wrap user's handler with snowflakeHandler to preserve context extraction
	log.handler = newSnowflakeHandler(handler, log.level)
	log.inner = slog.New(log.handler)

	return nil
}

// logWithSkip logs a message at the given level, skipping 'skip' frames when determining source location.
// This is used internally to skip wrapper frames (levelFilteringLogger -> secretMaskingLogger -> rawLogger)
// and report the actual caller's location.
func (log *rawLogger) logWithSkip(skip int, level sflog.Level, msg string) {
	if !log.isEnabled() {
		return
	}
	var pcs [1]uintptr
	// Skip: runtime.Callers itself + logWithSkip + specified skip
	runtime.Callers(skip+2, pcs[:])
	r := slog.NewRecord(time.Now(), slog.Level(level), msg, pcs[0])
	_ = log.handler.Handle(context.Background(), r)
}

// Implement all formatted logging methods (*f variants)
// Skip depth = 3 assumes standard wrapper chain: levelFilteringLogger -> secretMaskingLogger -> rawLogger
// If wrapper chain changes, update this value. See TestSkipDepthWarning test.
func (log *rawLogger) Tracef(format string, args ...interface{}) {
	log.logWithSkip(3, sflog.LevelTrace, fmt.Sprintf(format, args...))
}

func (log *rawLogger) Debugf(format string, args ...interface{}) {
	log.logWithSkip(3, sflog.LevelDebug, fmt.Sprintf(format, args...))
}

func (log *rawLogger) Infof(format string, args ...interface{}) {
	log.logWithSkip(3, sflog.LevelInfo, fmt.Sprintf(format, args...))
}

func (log *rawLogger) Warnf(format string, args ...interface{}) {
	log.logWithSkip(3, sflog.LevelWarn, fmt.Sprintf(format, args...))
}

func (log *rawLogger) Errorf(format string, args ...interface{}) {
	log.logWithSkip(3, sflog.LevelError, fmt.Sprintf(format, args...))
}

func (log *rawLogger) Fatalf(format string, args ...interface{}) {
	log.logWithSkip(3, sflog.LevelFatal, fmt.Sprintf(format, args...))
	os.Exit(1)
}

// Implement all direct logging methods
// Skip depth = 3 assumes standard wrapper chain: levelFilteringLogger -> secretMaskingLogger -> rawLogger
// If wrapper chain changes, update this value. See TestSkipDepthWarning test.
func (log *rawLogger) Trace(msg string) {
	log.logWithSkip(3, sflog.LevelTrace, msg)
}

func (log *rawLogger) Debug(msg string) {
	log.logWithSkip(3, sflog.LevelDebug, msg)
}

func (log *rawLogger) Info(msg string) {
	log.logWithSkip(3, sflog.LevelInfo, msg)
}

func (log *rawLogger) Warn(msg string) {
	log.logWithSkip(3, sflog.LevelWarn, msg)
}

func (log *rawLogger) Error(msg string) {
	log.logWithSkip(3, sflog.LevelError, msg)
}

func (log *rawLogger) Fatal(msg string) {
	log.logWithSkip(3, sflog.LevelFatal, msg)
	os.Exit(1)
}

// Structured logging methods
func (log *rawLogger) WithField(key string, value interface{}) LogEntry {
	return &slogEntry{
		logger:  log.inner.With(slog.Any(key, value)),
		enabled: &log.enabled,
		mu:      &log.mu,
	}
}

func (log *rawLogger) WithFields(fields map[string]any) LogEntry {
	attrs := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		attrs = append(attrs, k, v)
	}
	return &slogEntry{
		logger:  log.inner.With(attrs...),
		enabled: &log.enabled,
		mu:      &log.mu,
	}
}

func (log *rawLogger) WithContext(ctx context.Context) LogEntry {
	if ctx == nil {
		return log
	}

	// Extract fields from context
	attrs := extractContextFields(ctx)
	if len(attrs) == 0 {
		return log
	}

	// Convert []slog.Attr to []any for With()
	// slog.Logger.With() can accept slog.Attr directly
	args := make([]any, len(attrs))
	for i, attr := range attrs {
		args[i] = attr
	}

	newLogger := log.inner.With(args...)

	return &slogEntry{
		logger:  newLogger,
		enabled: &log.enabled,
		mu:      &log.mu,
	}
}

// slogEntry implements LogEntry
type slogEntry struct {
	logger  *slog.Logger
	enabled *bool
	mu      *sync.Mutex
}

// Compile-time verification that slogEntry implements LogEntry
var _ LogEntry = (*slogEntry)(nil)

func (e *slogEntry) isEnabled() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return *e.enabled
}

// logWithSkip logs a message at the given level, skipping 'skip' frames when determining source location.
func (e *slogEntry) logWithSkip(skip int, level sflog.Level, msg string) {
	if !e.isEnabled() {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(skip+2, pcs[:]) // +2: runtime.Callers itself + logWithSkip
	r := slog.NewRecord(time.Now(), slog.Level(level), msg, pcs[0])
	_ = e.logger.Handler().Handle(context.Background(), r)
}

// Implement all formatted logging methods (*f variants)
// Skip depth = 3 assumes standard wrapper chain: levelFilteringEntry -> secretMaskingEntry -> slogEntry
// If wrapper chain changes, update this value. See TestSkipDepthWarning test.
func (e *slogEntry) Tracef(format string, args ...interface{}) {
	e.logWithSkip(3, sflog.LevelTrace, fmt.Sprintf(format, args...))
}

func (e *slogEntry) Debugf(format string, args ...interface{}) {
	e.logWithSkip(3, sflog.LevelDebug, fmt.Sprintf(format, args...))
}

func (e *slogEntry) Infof(format string, args ...interface{}) {
	e.logWithSkip(3, sflog.LevelInfo, fmt.Sprintf(format, args...))
}

func (e *slogEntry) Warnf(format string, args ...interface{}) {
	e.logWithSkip(3, sflog.LevelWarn, fmt.Sprintf(format, args...))
}

func (e *slogEntry) Errorf(format string, args ...interface{}) {
	e.logWithSkip(3, sflog.LevelError, fmt.Sprintf(format, args...))
}

func (e *slogEntry) Fatalf(format string, args ...interface{}) {
	e.logWithSkip(3, sflog.LevelFatal, fmt.Sprintf(format, args...))
	os.Exit(1)
}

// Implement all direct logging methods
// Skip depth = 3 assumes standard wrapper chain: levelFilteringEntry -> secretMaskingEntry -> slogEntry
// If wrapper chain changes, update this value. See TestSkipDepthWarning test.
func (e *slogEntry) Trace(msg string) {
	e.logWithSkip(3, sflog.LevelTrace, msg)
}

func (e *slogEntry) Debug(msg string) {
	e.logWithSkip(3, sflog.LevelDebug, msg)
}

func (e *slogEntry) Info(msg string) {
	e.logWithSkip(3, sflog.LevelInfo, msg)
}

func (e *slogEntry) Warn(msg string) {
	e.logWithSkip(3, sflog.LevelWarn, msg)
}

func (e *slogEntry) Error(msg string) {
	e.logWithSkip(3, sflog.LevelError, msg)
}

func (e *slogEntry) Fatal(msg string) {
	e.logWithSkip(3, sflog.LevelFatal, msg)
	os.Exit(1)
}

// Helper methods for internal use and easy_logging support
func (log *rawLogger) closeFileOnLoggerReplace(file *os.File) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	if log.file != nil && log.file != file {
		return fmt.Errorf("could not set a file to close on logger reset because there were already set one")
	}
	log.file = file
	return nil
}

// CloseFileOnLoggerReplace is exported for easy_logging support
func (log *rawLogger) CloseFileOnLoggerReplace(file *os.File) error {
	return log.closeFileOnLoggerReplace(file)
}

// ReplaceGlobalLogger closes the current logger's file (for easy_logging support)
// The actual global logger replacement is handled by the main package
func (log *rawLogger) ReplaceGlobalLogger(newLogger interface{}) {
	if log.file != nil {
		_ = log.file.Close()
	}
}

// Ensure rawLogger implements SFLogger
var _ SFLogger = (*rawLogger)(nil)
