package logger

import (
	"errors"
	"log"
	"sync"

	"github.com/snowflakedb/gosnowflake/v2/loginterface"
)

// LoggerAccessor allows internal packages to access the global logger
// without importing the main gosnowflake package (avoiding circular dependencies)
var (
	loggerAccessorMu sync.Mutex
	// globalLogger is the actual logger that provides all features (secret masking, level filtering, etc.)
	globalLogger loginterface.SFLogger
)

// GetLogger returns the global logger for use by internal packages
func GetLogger() loginterface.SFLogger {
	loggerAccessorMu.Lock()
	defer loggerAccessorMu.Unlock()

	return globalLogger
}

// SetLogger sets the raw (base) logger implementation and wraps it with the standard protection layers.
// This function ALWAYS wraps the provided logger with:
//  1. Secret masking (to protect sensitive data)
//  2. Level filtering (for performance optimization)
//
// There is no way to bypass these protective layers. The globalLogger structure is:
//
//	globalLogger = levelFilteringLogger → secretMaskingLogger → rawLogger
//
// If the provided logger is already wrapped (e.g., from CreateDefaultLogger), this function
// automatically extracts the raw logger to prevent double-wrapping.
//
// Internal wrapper types that would cause issues are rejected:
//   - Proxy (would cause infinite recursion)
func SetLogger(providedLogger SFLogger) error {
	loggerAccessorMu.Lock()
	defer loggerAccessorMu.Unlock()

	// Reject Proxy to prevent infinite recursion
	if _, isProxy := providedLogger.(*Proxy); isProxy {
		return errors.New("cannot set Proxy as raw logger - it would create infinite recursion")
	}

	// Unwrap if the logger is one of our own wrapper types
	// This allows SetLogger to accept both raw loggers and fully-wrapped loggers
	rawLogger := providedLogger

	// If it's a level filtering logger, unwrap to get the secret masking layer
	if levelFiltering, ok := rawLogger.(*levelFilteringLogger); ok {
		rawLogger = levelFiltering.inner
	}

	// If it's a secret masking logger, unwrap to get the raw logger
	if secretMasking, ok := rawLogger.(*secretMaskingLogger); ok {
		rawLogger = secretMasking.inner
	}

	// Build the standard protection chain: levelFiltering → secretMasking → rawLogger
	masked := newSecretMaskingLogger(rawLogger)
	filtered := newLevelFilteringLogger(masked)

	globalLogger = filtered
	return nil
}

func init() {
	rawLogger := newRawLogger()
	if err := SetLogger(rawLogger); err != nil {
		log.Panicf("cannot set default logger. %v", err)
	}
}

// CreateDefaultLogger function creates a new instance of the default logger with the standard protection layers.
func CreateDefaultLogger() loginterface.SFLogger {
	return newLevelFilteringLogger(newSecretMaskingLogger(newRawLogger()))
}
