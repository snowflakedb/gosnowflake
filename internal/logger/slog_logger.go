package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"
)

// callerPrettyfier formats caller information for logging
func callerPrettyfier(frame *runtime.Frame) (string, string) {
	return path.Base(frame.Function), fmt.Sprintf("%s:%d", path.Base(frame.File), frame.Line)
}

// defaultLogger implements SFLogger using slog
type defaultLogger struct {
	inner    *slog.Logger
	handler  *snowflakeHandler
	levelVar *slog.LevelVar
	enabled  bool // For OFF level support
	file     *os.File
	output   io.Writer
	mu       sync.Mutex
}

// Compile-time verification that defaultLogger implements SFLogger
var _ SFLogger = (*defaultLogger)(nil)

// NewDefaultLogger creates the internal default logger using slog
func NewDefaultLogger() SFLogger {
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelInfo) // Default level to match logrus internal default

	opts := &slog.HandlerOptions{
		Level:     levelVar,
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Custom attribute replacement for formatting
			if a.Key == slog.TimeKey {
				// Format timestamp as RFC3339Nano like logrus
				if t, ok := a.Value.Any().(time.Time); ok {
					return slog.String(slog.TimeKey, t.Format(time.RFC3339Nano))
				}
			}
			if a.Key == slog.SourceKey {
				// Format source using callerPrettyfier
				if src, ok := a.Value.Any().(*slog.Source); ok {
					// Create a runtime.Frame equivalent
					frame := &runtime.Frame{
						File:     src.File,
						Line:     src.Line,
						Function: src.Function,
					}
					_, location := callerPrettyfier(frame)
					return slog.String(slog.SourceKey, location)
				}
			}
			return a
		},
	}

	textHandler := slog.NewTextHandler(os.Stderr, opts)
	handler := &snowflakeHandler{
		inner:    textHandler,
		levelVar: levelVar,
	}

	slogLogger := slog.New(handler)

	return &defaultLogger{
		inner:    slogLogger,
		handler:  handler,
		levelVar: levelVar,
		enabled:  true,
		output:   os.Stderr,
	}
}

// isEnabled checks if logging is enabled (for OFF level)
func (log *defaultLogger) isEnabled() bool {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.enabled
}

// SetLogLevel sets the log level
func (log *defaultLogger) SetLogLevel(level string) error {
	upperLevel := strings.ToUpper(level)

	if upperLevel == "OFF" {
		log.mu.Lock()
		log.enabled = false
		log.mu.Unlock()
		return nil
	}

	log.mu.Lock()
	log.enabled = true
	log.mu.Unlock()

	slogLevel, err := parseLevel(upperLevel)
	if err != nil {
		return err
	}

	log.levelVar.Set(slogLevel)
	return nil
}

// GetLogLevel returns the current log level
func (log *defaultLogger) GetLogLevel() string {
	if !log.isEnabled() {
		return "OFF"
	}

	level := log.levelVar.Level()
	return levelToString(level)
}

// SetOutput sets the output writer
func (log *defaultLogger) SetOutput(output io.Writer) {
	log.mu.Lock()
	defer log.mu.Unlock()

	log.output = output

	// Create new handler with new output
	opts := &slog.HandlerOptions{
		Level:     log.levelVar,
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
					_, location := callerPrettyfier(frame)
					return slog.String(slog.SourceKey, location)
				}
			}
			return a
		},
	}

	textHandler := slog.NewTextHandler(output, opts)
	log.handler = &snowflakeHandler{
		inner:    textHandler,
		levelVar: log.levelVar,
	}
	log.inner = slog.New(log.handler)
}

// SetHandler sets a custom slog handler (implements SFSlogLogger interface)
// The provided handler will be wrapped with snowflakeHandler to preserve context extraction.
// Secret masking is handled at a higher level (secretMaskingLogger wrapper).
func (log *defaultLogger) SetHandler(handler interface{}) error {
	// Type assert to slog.Handler
	slogHandler, ok := handler.(slog.Handler)
	if !ok {
		return fmt.Errorf("handler must be of type slog.Handler")
	}

	log.mu.Lock()
	defer log.mu.Unlock()

	// Wrap user's handler with snowflakeHandler to preserve context extraction
	log.handler = &snowflakeHandler{
		inner:    slogHandler,
		levelVar: log.levelVar,
	}
	log.inner = slog.New(log.handler)

	return nil
}

// logWithSkip logs a message at the given level, skipping 'skip' frames when determining source location.
// This is used internally to skip wrapper frames (levelFilteringLogger -> secretMaskingLogger -> defaultLogger)
// and report the actual caller's location.
func (log *defaultLogger) logWithSkip(skip int, level slog.Level, msg string) {
	if !log.isEnabled() {
		return
	}
	var pcs [1]uintptr
	// Skip: runtime.Callers itself + logWithSkip + specified skip
	runtime.Callers(skip+2, pcs[:])
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	_ = log.handler.Handle(context.Background(), r)
}

// Implement all formatted logging methods (*f variants)
// Skip depth = 3 assumes standard wrapper chain: levelFilteringLogger -> secretMaskingLogger -> defaultLogger
// If wrapper chain changes, update this value. See TestSkipDepthWarning test.
func (log *defaultLogger) Tracef(format string, args ...interface{}) {
	log.logWithSkip(3, LevelTrace, fmt.Sprintf(format, args...))
}

func (log *defaultLogger) Debugf(format string, args ...interface{}) {
	log.logWithSkip(3, LevelDebug, fmt.Sprintf(format, args...))
}

func (log *defaultLogger) Infof(format string, args ...interface{}) {
	log.logWithSkip(3, LevelInfo, fmt.Sprintf(format, args...))
}

func (log *defaultLogger) Warnf(format string, args ...interface{}) {
	log.logWithSkip(3, LevelWarn, fmt.Sprintf(format, args...))
}

func (log *defaultLogger) Errorf(format string, args ...interface{}) {
	log.logWithSkip(3, LevelError, fmt.Sprintf(format, args...))
}

func (log *defaultLogger) Fatalf(format string, args ...interface{}) {
	log.logWithSkip(3, LevelFatal, fmt.Sprintf(format, args...))
	os.Exit(1)
}

// Implement all direct logging methods
// Skip depth = 3 assumes standard wrapper chain: levelFilteringLogger -> secretMaskingLogger -> defaultLogger
// If wrapper chain changes, update this value. See TestSkipDepthWarning test.
func (log *defaultLogger) Trace(msg string) {
	log.logWithSkip(3, LevelTrace, msg)
}

func (log *defaultLogger) Debug(msg string) {
	log.logWithSkip(3, LevelDebug, msg)
}

func (log *defaultLogger) Info(msg string) {
	log.logWithSkip(3, LevelInfo, msg)
}

func (log *defaultLogger) Warn(msg string) {
	log.logWithSkip(3, LevelWarn, msg)
}

func (log *defaultLogger) Error(msg string) {
	log.logWithSkip(3, LevelError, msg)
}

func (log *defaultLogger) Fatal(msg string) {
	log.logWithSkip(3, LevelFatal, msg)
	os.Exit(1)
}

// Structured logging methods
func (log *defaultLogger) WithField(key string, value interface{}) LogEntry {
	return &slogEntry{
		logger:  log.inner.With(slog.Any(key, value)),
		enabled: &log.enabled,
		mu:      &log.mu,
	}
}

func (log *defaultLogger) WithFields(fields map[string]any) LogEntry {
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

func (log *defaultLogger) WithContext(ctx context.Context) LogEntry {
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
func (e *slogEntry) logWithSkip(skip int, level slog.Level, msg string) {
	if !e.isEnabled() {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(skip+2, pcs[:]) // +2: runtime.Callers itself + logWithSkip
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	_ = e.logger.Handler().Handle(context.Background(), r)
}

// Implement all formatted logging methods (*f variants)
// Skip depth = 3 assumes standard wrapper chain: levelFilteringEntry -> secretMaskingEntry -> slogEntry
// If wrapper chain changes, update this value. See TestSkipDepthWarning test.
func (e *slogEntry) Tracef(format string, args ...interface{}) {
	e.logWithSkip(3, LevelTrace, fmt.Sprintf(format, args...))
}

func (e *slogEntry) Debugf(format string, args ...interface{}) {
	e.logWithSkip(3, LevelDebug, fmt.Sprintf(format, args...))
}

func (e *slogEntry) Infof(format string, args ...interface{}) {
	e.logWithSkip(3, LevelInfo, fmt.Sprintf(format, args...))
}

func (e *slogEntry) Warnf(format string, args ...interface{}) {
	e.logWithSkip(3, LevelWarn, fmt.Sprintf(format, args...))
}

func (e *slogEntry) Errorf(format string, args ...interface{}) {
	e.logWithSkip(3, LevelError, fmt.Sprintf(format, args...))
}

func (e *slogEntry) Fatalf(format string, args ...interface{}) {
	e.logWithSkip(3, LevelFatal, fmt.Sprintf(format, args...))
	os.Exit(1)
}

// Implement all direct logging methods
// Skip depth = 3 assumes standard wrapper chain: levelFilteringEntry -> secretMaskingEntry -> slogEntry
// If wrapper chain changes, update this value. See TestSkipDepthWarning test.
func (e *slogEntry) Trace(msg string) {
	e.logWithSkip(3, LevelTrace, msg)
}

func (e *slogEntry) Debug(msg string) {
	e.logWithSkip(3, LevelDebug, msg)
}

func (e *slogEntry) Info(msg string) {
	e.logWithSkip(3, LevelInfo, msg)
}

func (e *slogEntry) Warn(msg string) {
	e.logWithSkip(3, LevelWarn, msg)
}

func (e *slogEntry) Error(msg string) {
	e.logWithSkip(3, LevelError, msg)
}

func (e *slogEntry) Fatal(msg string) {
	e.logWithSkip(3, LevelFatal, msg)
	os.Exit(1)
}

// Helper methods for internal use and easy_logging support
func (log *defaultLogger) closeFileOnLoggerReplace(file *os.File) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	if log.file != nil && log.file != file {
		return fmt.Errorf("could not set a file to close on logger reset because there were already set one")
	}
	log.file = file
	return nil
}

// CloseFileOnLoggerReplace is exported for easy_logging support
func (log *defaultLogger) CloseFileOnLoggerReplace(file *os.File) error {
	return log.closeFileOnLoggerReplace(file)
}

// ReplaceGlobalLogger closes the current logger's file (for easy_logging support)
// The actual global logger replacement is handled by the main package
func (log *defaultLogger) ReplaceGlobalLogger(newLogger interface{}) {
	if log.file != nil {
		_ = log.file.Close()
	}
}

// Ensure defaultLogger implements SFLogger
var _ SFLogger = (*defaultLogger)(nil)
