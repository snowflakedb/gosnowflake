package logger

import (
	"github.com/snowflakedb/gosnowflake/v2/loginterface"
)

// Re-export types from loginterface package to avoid circular dependencies
// while maintaining a clean internal API
type (
	LogEntry             = loginterface.LogEntry
	SFLogger             = loginterface.SFLogger
	ClientLogContextHook = loginterface.ClientLogContextHook
)
