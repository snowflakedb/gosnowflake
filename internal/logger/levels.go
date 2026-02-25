package logger

import (
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
func parseLevel(level string) slog.Level {
	switch strings.ToUpper(level) {
	case "TRACE":
		return LevelTrace
	case "DEBUG":
		return LevelDebug
	case "INFO":
		return LevelInfo
	case "WARN":
		return LevelWarn
	case "ERROR":
		return LevelError
	case "FATAL":
		return LevelFatal
	case "OFF":
		return LevelOff
	default:
		return LevelInfo
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
