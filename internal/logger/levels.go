package logger

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
)

// Custom level constants that extend slog's standard levels
const (
	LevelTrace = slog.Level(-8)          // Below DEBUG
	LevelDebug = slog.LevelDebug         // -4
	LevelInfo  = slog.LevelInfo          // 0
	LevelWarn  = slog.LevelWarn          // 4
	LevelError = slog.LevelError         // 8
	LevelFatal = slog.Level(12)          // Above ERROR
	LevelOff   = slog.Level(math.MaxInt) // Disable all logging
)

// parseLevel converts a string level to slog.Level
func parseLevel(level string) (slog.Level, error) {
	switch strings.ToUpper(level) {
	case "TRACE":
		return LevelTrace, nil
	case "DEBUG":
		return LevelDebug, nil
	case "INFO":
		return LevelInfo, nil
	case "WARN":
		return LevelWarn, nil
	case "ERROR":
		return LevelError, nil
	case "FATAL":
		return LevelFatal, nil
	case "OFF":
		return LevelOff, nil
	default:
		return LevelInfo, fmt.Errorf("unknown log level: %s", level)
	}
}

// levelToString converts slog.Level to string
func levelToString(level slog.Level) string {
	switch level {
	case LevelTrace:
		return "trace"
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	case LevelFatal:
		return "fatal"
	case LevelOff:
		return "off"
	default:
		return "info"
	}
}
