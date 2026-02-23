package gosnowflake

import (
	loggerinternal "github.com/snowflakedb/gosnowflake/v2/internal/logger"
	"github.com/snowflakedb/gosnowflake/v2/loginterface"
)

// SFSessionIDKey is context key of session id
const SFSessionIDKey contextKey = "LOG_SESSION_ID"

// SFSessionUserKey is context key of user id of a session
const SFSessionUserKey contextKey = "LOG_USER"

func init() {
	// Set default log keys in internal package
	SetLogKeys(SFSessionIDKey, SFSessionUserKey)

	// Initialize the default logger in internal package
	loggerinternal.CreateAndSetDefaultLogger()

	// Set default log level
	_ = logger.SetLogLevel("error")
	if runningOnGithubAction() {
		_ = logger.SetLogLevel("fatal")
	}
}

// Re-export types from loginterface package for backward compatibility
type (
	// ClientLogContextHook is a client-defined hook that can be used to insert log
	// fields based on the Context.
	ClientLogContextHook = loginterface.ClientLogContextHook

	// LogEntry allows for logging using a snapshot of field values.
	// No implementation-specific logging details should be placed into this interface.
	LogEntry = loginterface.LogEntry

	// SFLogger Snowflake logger interface which abstracts away the underlying logging mechanism.
	// No implementation-specific logging details should be placed into this interface.
	SFLogger = loginterface.SFLogger

	// SFSlogLogger is an optional interface for advanced slog handler configuration.
	// This interface is separate from SFLogger to maintain framework-agnostic design.
	// Users can type-assert the logger to check if slog handler configuration is supported.
	SFSlogLogger = loginterface.SFSlogLogger
)

// SetLogKeys sets the context keys to be written to logs when logger.WithContext is used.
// This function is thread-safe and can be called at runtime.
func SetLogKeys(keys ...contextKey) {
	// Convert contextKey to []interface{} for internal package
	ikeys := make([]interface{}, len(keys))
	for i, k := range keys {
		ikeys[i] = k
	}
	loggerinternal.SetLogKeys(ikeys)
}

// GetLogKeys returns the currently configured context keys.
func GetLogKeys() []contextKey {
	ikeys := loggerinternal.GetLogKeys()

	// Convert []interface{} back to []contextKey
	keys := make([]contextKey, 0, len(ikeys))
	for _, k := range ikeys {
		if ck, ok := k.(contextKey); ok {
			keys = append(keys, ck)
		}
	}
	return keys
}

// RegisterLogContextHook registers a hook that can be used to extract fields
// from the Context and associated with log messages using the provided key.
// This function is thread-safe and can be called at runtime.
func RegisterLogContextHook(contextKey string, ctxExtractor ClientLogContextHook) {
	// Delegate directly to internal package
	loggerinternal.RegisterLogContextHook(contextKey, ctxExtractor)
}

// GetClientLogContextHooks returns the registered log context hooks.
func GetClientLogContextHooks() map[string]ClientLogContextHook {
	return loggerinternal.GetClientLogContextHooks()
}

// logger is a proxy that delegates all calls to the internal global logger
// This ensures a single source of truth for the current logger
var logger SFLogger = loggerinternal.NewLoggerProxy()

// SetLogger set a new logger of SFLogger interface for gosnowflake
// The provided logger will automatically be wrapped with secret masking.
func SetLogger(inLogger *SFLogger) {
	_ = loggerinternal.SetLoggerWithMasking(*inLogger)
}

// GetLogger return logger that is not public
func GetLogger() SFLogger {
	return logger
}

// CreateDefaultLogger creates and returns a new instance of SFLogger with default config.
// The returned logger is automatically wrapped with secret masking.
// This is a pure factory function and does NOT modify global state.
// If you want to set it as the global logger, call SetLogger(&newLogger).
func CreateDefaultLogger() SFLogger {
	inner := loggerinternal.NewDefaultLogger()
	wrappedInterface := loggerinternal.NewSecretMaskingLogger(inner)
	wrapped, _ := wrappedInterface.(loginterface.SFLogger)
	return wrapped
}
