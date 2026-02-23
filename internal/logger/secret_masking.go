package logger

import (
	"context"
	"fmt"
	"io"
)

// secretMaskingLogger wraps any logger implementation and ensures
// all log messages have secrets masked before being passed to the inner logger.
type secretMaskingLogger struct {
	inner SFLogger
}

// Compile-time verification that secretMaskingLogger implements SFLogger
var _ SFLogger = (*secretMaskingLogger)(nil)

// Unwrap returns the inner logger (for introspection by easy_logging)
func (l *secretMaskingLogger) Unwrap() interface{} {
	return l.inner
}

// NewSecretMaskingLogger creates a new secret masking wrapper around the provided logger.
// Returns interface{} which the caller can type-assert to their SFLogger interface.
// The inner logger must implement the SFLogger interface.
func NewSecretMaskingLogger(inner interface{}) interface{} {
	if inner == nil {
		panic("inner logger cannot be nil")
	}

	// Type assert to ensure it implements SFLogger
	sfLogger, ok := inner.(SFLogger)
	if !ok {
		panic("inner logger must implement SFLogger interface")
	}

	return &secretMaskingLogger{inner: sfLogger}
}

// Helper methods for masking
func (l *secretMaskingLogger) maskValue(value interface{}) interface{} {
	if str, ok := value.(string); ok {
		return l.maskString(str)
	}
	// For other types, convert to string, mask, but return original type if no secrets
	strVal := fmt.Sprint(value)
	masked := l.maskString(strVal)
	if masked != strVal {
		return masked // Secrets found and masked
	}
	return value // No secrets, return original
}

func (l *secretMaskingLogger) maskString(value string) string {
	return MaskSecrets(value)
}

// Implement all formatted logging methods (*f variants)
func (l *secretMaskingLogger) Tracef(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := l.maskString(message)
	l.inner.Tracef("%s", maskedMessage)
}

func (l *secretMaskingLogger) Debugf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := l.maskString(message)
	l.inner.Debugf("%s", maskedMessage)
}

func (l *secretMaskingLogger) Infof(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := l.maskString(message)
	l.inner.Infof("%s", maskedMessage)
}

func (l *secretMaskingLogger) Warnf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := l.maskString(message)
	l.inner.Warnf("%s", maskedMessage)
}

func (l *secretMaskingLogger) Errorf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := l.maskString(message)
	l.inner.Errorf("%s", maskedMessage)
}

func (l *secretMaskingLogger) Fatalf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := l.maskString(message)
	l.inner.Fatalf("%s", maskedMessage)
}

// Implement all direct logging methods
func (l *secretMaskingLogger) Trace(msg string) {
	l.inner.Trace(l.maskString(msg))
}

func (l *secretMaskingLogger) Debug(msg string) {
	l.inner.Debug(l.maskString(msg))
}

func (l *secretMaskingLogger) Info(msg string) {
	l.inner.Info(l.maskString(msg))
}

func (l *secretMaskingLogger) Warn(msg string) {
	l.inner.Warn(l.maskString(msg))
}

func (l *secretMaskingLogger) Error(msg string) {
	l.inner.Error(l.maskString(msg))
}

func (l *secretMaskingLogger) Fatal(msg string) {
	l.inner.Fatal(l.maskString(msg))
}

// Implement structured logging methods
// Note: These return interface{} to maintain compatibility with the adapter layer
func (l *secretMaskingLogger) WithField(key string, value interface{}) LogEntry {
	maskedValue := l.maskValue(value)
	result := l.inner.WithField(key, maskedValue)
	return &secretMaskingEntry{
		inner:  result,
		parent: l,
	}
}

func (l *secretMaskingLogger) WithFields(fields map[string]any) LogEntry {
	maskedFields := make(map[string]any, len(fields))
	for k, v := range fields {
		maskedFields[k] = l.maskValue(v)
	}
	result := l.inner.WithFields(maskedFields)
	return &secretMaskingEntry{
		inner:  result,
		parent: l,
	}
}

func (l *secretMaskingLogger) WithContext(ctx context.Context) LogEntry {
	result := l.inner.WithContext(ctx)
	return &secretMaskingEntry{
		inner:  result,
		parent: l,
	}
}

// Delegate configuration methods
func (l *secretMaskingLogger) SetLogLevel(level string) error {
	return l.inner.SetLogLevel(level)
}

func (l *secretMaskingLogger) GetLogLevel() string {
	return l.inner.GetLogLevel()
}

func (l *secretMaskingLogger) SetOutput(output io.Writer) {
	l.inner.SetOutput(output)
}

// SetHandler delegates to inner logger's SetHandler (for slog handler configuration)
func (l *secretMaskingLogger) SetHandler(handler interface{}) error {
	type setHandlerLogger interface{ SetHandler(interface{}) error }
	if logger, ok := l.inner.(setHandlerLogger); ok {
		return logger.SetHandler(handler)
	}
	return fmt.Errorf("inner logger does not support SetHandler")
}

// secretMaskingEntry wraps a log entry and masks all secrets.
type secretMaskingEntry struct {
	inner  LogEntry
	parent *secretMaskingLogger
}

// Compile-time verification that secretMaskingEntry implements LogEntry
var _ LogEntry = (*secretMaskingEntry)(nil)

// Implement all formatted logging methods (*f variants)
func (e *secretMaskingEntry) Tracef(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := MaskSecrets(message)
	e.inner.Tracef("%s", maskedMessage)
}

func (e *secretMaskingEntry) Debugf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := MaskSecrets(message)
	e.inner.Debugf("%s", maskedMessage)
}

func (e *secretMaskingEntry) Infof(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := MaskSecrets(message)
	e.inner.Infof("%s", maskedMessage)
}

func (e *secretMaskingEntry) Warnf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := MaskSecrets(message)
	e.inner.Warnf("%s", maskedMessage)
}

func (e *secretMaskingEntry) Errorf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := MaskSecrets(message)
	e.inner.Errorf("%s", maskedMessage)
}

func (e *secretMaskingEntry) Fatalf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	maskedMessage := MaskSecrets(message)
	e.inner.Fatalf("%s", maskedMessage)
}

// Implement all direct logging methods
func (e *secretMaskingEntry) Trace(msg string) {
	e.inner.Trace(e.parent.maskString(msg))
}

func (e *secretMaskingEntry) Debug(msg string) {
	e.inner.Debug(e.parent.maskString(msg))
}

func (e *secretMaskingEntry) Info(msg string) {
	e.inner.Info(e.parent.maskString(msg))
}

func (e *secretMaskingEntry) Warn(msg string) {
	e.inner.Warn(e.parent.maskString(msg))
}

func (e *secretMaskingEntry) Error(msg string) {
	e.inner.Error(e.parent.maskString(msg))
}

func (e *secretMaskingEntry) Fatal(msg string) {
	e.inner.Fatal(e.parent.maskString(msg))
}
