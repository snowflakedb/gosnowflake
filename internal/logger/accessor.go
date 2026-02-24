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
//
//	log := logger.GetLogger()
//	log.Info("Message from internal package")
//	log.WithField("key", "value").Info("Structured log")
func GetLogger() loginterface.SFLogger {
	loggerAccessorMu.RLock()
	defer loggerAccessorMu.RUnlock()

	if globalLogger == nil {
		// Return a default logger if not initialized yet
		loggerAccessorOnce.Do(func() {
			inner := NewDefaultLogger()
			// Wrap with secret masking
			maskedInterface := NewSecretMaskingLogger(inner)
			masked, ok := maskedInterface.(loginterface.SFLogger)
			if !ok {
				panic("wrapped logger does not implement SFLogger interface")
			}
			// Wrap with level filtering
			filtered := NewLevelFilteringLogger(masked)
			globalLogger = filtered
		})
	}

	return globalLogger
}

// SetLoggerWithMasking wraps any logger with secret masking and level filtering, then sets it as the global logger.
// If the logger is already wrapped with SecretMaskingAdapter, it uses the inner logger directly.
// This centralizes the wrapping logic that was duplicated across SetLogger and CreateDefaultLogger.
// LoggerProxy instances are rejected to prevent infinite recursion.
//
// The wrapping chain is: levelFilteringLogger → secretMaskingLogger → actualLogger
// This ensures level filtering happens first (performance optimization) before expensive masking operations.
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

	// Build the wrapping chain: levelFiltering → secretMasking → actualLogger

	// Step 1: Wrap with secret masking
	maskedInterface := NewSecretMaskingLogger(logger)
	masked, ok := maskedInterface.(loginterface.SFLogger)
	if !ok {
		return &loggerError{message: "wrapped logger does not implement SFLogger interface"}
	}

	// Step 2: Wrap with level filtering (outermost layer)
	filtered := NewLevelFilteringLogger(masked)

	globalLogger = filtered
	return nil
}

// CreateAndSetDefaultLogger creates a new default logger, wraps it with secret masking and level filtering,
// and sets it as the global logger. This centralizes the initialization logic.
//
// The wrapping chain is: levelFilteringLogger → secretMaskingLogger → defaultLogger
// This ensures level filtering happens first (performance optimization) before expensive masking operations.
func CreateAndSetDefaultLogger() {
	loggerAccessorMu.Lock()
	defer loggerAccessorMu.Unlock()

	// Create the actual logger
	actualLogger := NewDefaultLogger()

	// Step 1: Wrap with secret masking
	maskedInterface := NewSecretMaskingLogger(actualLogger)
	masked, _ := maskedInterface.(loginterface.SFLogger)

	// Step 2: Wrap with level filtering (outermost layer)
	filtered := NewLevelFilteringLogger(masked)

	globalLogger = filtered
}

// loggerError is a simple error type for logger-related errors
type loggerError struct {
	message string
}

func (e *loggerError) Error() string {
	return e.message
}
