package logger

import (
	"github.com/snowflakedb/gosnowflake/v2/loginterface"
)

// Re-export types from loginterface package to avoid circular dependencies
// while maintaining a clean internal API
type (
	// LogEntry reexports the LogEntry interface from loginterface package.
	LogEntry = loginterface.LogEntry
	// SFLogger reexports the SFLogger interface from loginterface package.
	SFLogger = loginterface.SFLogger
	// ClientLogContextHook reexports the ClientLogContextHook type from loginterface package.
	ClientLogContextHook = loginterface.ClientLogContextHook
)
