package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/snowflakedb/gosnowflake/v2/loginterface"
)

// SecretMaskingAdapter adapts the internal secret masking logger to implement SFLogger.
// This type is kept for backward compatibility with SetLogger() detection logic.
// It's exported so the main package can check if a logger is already wrapped.
type SecretMaskingAdapter struct {
	Inner loginterface.SFLogger
}

// Compile-time verification that SecretMaskingAdapter implements SFLogger
var _ loginterface.SFLogger = (*SecretMaskingAdapter)(nil)

// Unwrap returns the inner logger
func (a *SecretMaskingAdapter) Unwrap() any {
	if u, ok := a.Inner.(Unwrapper); ok {
		return u.Unwrap()
	}
	return a.Inner
}

// All methods delegate directly to inner with compile-time type safety

func (a *SecretMaskingAdapter) Tracef(format string, args ...interface{}) {
	a.Inner.Tracef(format, args...)
}

func (a *SecretMaskingAdapter) Debugf(format string, args ...interface{}) {
	a.Inner.Debugf(format, args...)
}

func (a *SecretMaskingAdapter) Infof(format string, args ...interface{}) {
	a.Inner.Infof(format, args...)
}

func (a *SecretMaskingAdapter) Warnf(format string, args ...interface{}) {
	a.Inner.Warnf(format, args...)
}

func (a *SecretMaskingAdapter) Errorf(format string, args ...interface{}) {
	a.Inner.Errorf(format, args...)
}

func (a *SecretMaskingAdapter) Fatalf(format string, args ...interface{}) {
	a.Inner.Fatalf(format, args...)
}

func (a *SecretMaskingAdapter) Trace(msg string) {
	a.Inner.Trace(msg)
}

func (a *SecretMaskingAdapter) Debug(msg string) {
	a.Inner.Debug(msg)
}

func (a *SecretMaskingAdapter) Info(msg string) {
	a.Inner.Info(msg)
}

func (a *SecretMaskingAdapter) Warn(msg string) {
	a.Inner.Warn(msg)
}

func (a *SecretMaskingAdapter) Error(msg string) {
	a.Inner.Error(msg)
}

func (a *SecretMaskingAdapter) Fatal(msg string) {
	a.Inner.Fatal(msg)
}

func (a *SecretMaskingAdapter) WithField(key string, value interface{}) loginterface.LogEntry {
	return a.Inner.WithField(key, value)
}

func (a *SecretMaskingAdapter) WithFields(fields map[string]any) loginterface.LogEntry {
	return a.Inner.WithFields(fields)
}

func (a *SecretMaskingAdapter) WithContext(ctx context.Context) loginterface.LogEntry {
	return a.Inner.WithContext(ctx)
}

func (a *SecretMaskingAdapter) SetLogLevel(level string) error {
	return a.Inner.SetLogLevel(level)
}

func (a *SecretMaskingAdapter) GetLogLevel() string {
	return a.Inner.GetLogLevel()
}

func (a *SecretMaskingAdapter) SetOutput(output io.Writer) {
	a.Inner.SetOutput(output)
}

// SetHandler implements SFSlogLogger interface for advanced slog handler configuration
func (a *SecretMaskingAdapter) SetHandler(handler slog.Handler) error {
	// The internal logger's SetHandler takes interface{} to avoid circular deps
	// So we use duck typing here - this is acceptable for optional interfaces
	type setHandlerLogger interface{ SetHandler(interface{}) error }
	if logger, ok := a.Inner.(setHandlerLogger); ok {
		return logger.SetHandler(handler)
	}
	return fmt.Errorf("inner logger does not support SetHandler")
}
