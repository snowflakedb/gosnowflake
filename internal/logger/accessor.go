package logger

import (
	"sync"

	"github.com/snowflakedb/gosnowflake/v2/loginterface"
)

// LoggerAccessor allows internal packages to access the global logger
// without importing the main gosnowflake package (avoiding circular dependencies)
var (
	globalLogger       loginterface.SFLogger
	loggerAccessorOnce sync.Once
	loggerAccessorMu   sync.RWMutex
)

// SetGlobalLoggerAccessor sets the logger accessor for internal packages
// This should be called by the main package during initialization
func SetGlobalLoggerAccessor(logger loginterface.SFLogger) {
	loggerAccessorMu.Lock()
	defer loggerAccessorMu.Unlock()
	globalLogger = logger
}

// GetLogger returns the global logger for use by internal packages
// Returns a default logger if not yet initialized
//
// Example usage:
//   log := logger.GetLogger()
//   log.Info("Message from internal package")
//   log.WithField("key", "value").Info("Structured log")
func GetLogger() loginterface.SFLogger {
	loggerAccessorMu.RLock()
	defer loggerAccessorMu.RUnlock()

	if globalLogger == nil {
		// Return a default logger if not initialized yet
		loggerAccessorOnce.Do(func() {
			inner := NewDefaultLogger()
			wrapped := NewSecretMaskingLogger(inner)
			if sfLogger, ok := wrapped.(loginterface.SFLogger); ok {
				globalLogger = sfLogger
			}
		})
	}

	return globalLogger
}

// SetLoggerWithMasking wraps any logger with secret masking and sets it as the global logger.
// If the logger is already wrapped with SecretMaskingAdapter, it uses the inner logger directly.
// This centralizes the wrapping logic that was duplicated across SetLogger and CreateDefaultLogger.
// LoggerProxy instances are rejected to prevent infinite recursion.
func SetLoggerWithMasking(logger interface{}) error {
	loggerAccessorMu.Lock()
	defer loggerAccessorMu.Unlock()

	// Reject LoggerProxy - it should never be set as the global logger
	// because it delegates to GetLogger(), which would create infinite recursion
	if _, isProxy := logger.(*LoggerProxy); isProxy {
		return &loggerError{message: "cannot set LoggerProxy as global logger - it would create infinite recursion"}
	}

	// Check if already a SecretMaskingAdapter
	if adapter, ok := logger.(*SecretMaskingAdapter); ok {
		// Already wrapped - use the inner logger directly
		globalLogger = adapter.Inner
		return nil
	}

	// Unwrap if the logger is wrapping a LoggerProxy
	type unwrapper interface {
		Unwrap() interface{}
	}
	if u, ok := logger.(unwrapper); ok {
		if inner, ok := u.Unwrap().(loginterface.SFLogger); ok {
			if _, isProxy := inner.(*LoggerProxy); isProxy {
				return &loggerError{message: "cannot set logger wrapping LoggerProxy - it would create infinite recursion"}
			}
		}
	}

	// Wrap with secret masking
	wrappedInterface := NewSecretMaskingLogger(logger)
	wrapped, ok := wrappedInterface.(loginterface.SFLogger)
	if !ok {
		return &loggerError{message: "wrapped logger does not implement SFLogger interface"}
	}

	globalLogger = wrapped
	return nil
}

// CreateAndSetDefaultLogger creates a new default logger, wraps it with secret masking,
// and sets it as the global logger. This centralizes the initialization logic.
func CreateAndSetDefaultLogger() {
	loggerAccessorMu.Lock()
	defer loggerAccessorMu.Unlock()

	inner := NewDefaultLogger()
	wrappedInterface := NewSecretMaskingLogger(inner)
	wrapped, _ := wrappedInterface.(loginterface.SFLogger)
	globalLogger = wrapped
}

// loggerError is a simple error type for logger-related errors
type loggerError struct {
	message string
}

func (e *loggerError) Error() string {
	return e.message
}
