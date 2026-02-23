package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/snowflakedb/gosnowflake/v2/loginterface"
)

// LoggerProxy is a proxy that delegates all calls to the global logger.
// This ensures a single source of truth for the current logger.
type LoggerProxy struct{}

// Compile-time verification that LoggerProxy implements SFLogger
var _ loginterface.SFLogger = (*LoggerProxy)(nil)

// All logging methods delegate to the global logger

func (p *LoggerProxy) Tracef(format string, args ...interface{}) {
	GetLogger().Tracef(format, args...)
}

func (p *LoggerProxy) Debugf(format string, args ...interface{}) {
	GetLogger().Debugf(format, args...)
}

func (p *LoggerProxy) Infof(format string, args ...interface{}) {
	GetLogger().Infof(format, args...)
}

func (p *LoggerProxy) Warnf(format string, args ...interface{}) {
	GetLogger().Warnf(format, args...)
}

func (p *LoggerProxy) Errorf(format string, args ...interface{}) {
	GetLogger().Errorf(format, args...)
}

func (p *LoggerProxy) Fatalf(format string, args ...interface{}) {
	GetLogger().Fatalf(format, args...)
}

func (p *LoggerProxy) Trace(msg string) {
	GetLogger().Trace(msg)
}

func (p *LoggerProxy) Debug(msg string) {
	GetLogger().Debug(msg)
}

func (p *LoggerProxy) Info(msg string) {
	GetLogger().Info(msg)
}

func (p *LoggerProxy) Warn(msg string) {
	GetLogger().Warn(msg)
}

func (p *LoggerProxy) Error(msg string) {
	GetLogger().Error(msg)
}

func (p *LoggerProxy) Fatal(msg string) {
	GetLogger().Fatal(msg)
}

func (p *LoggerProxy) WithField(key string, value interface{}) loginterface.LogEntry {
	return GetLogger().WithField(key, value)
}

func (p *LoggerProxy) WithFields(fields map[string]any) loginterface.LogEntry {
	return GetLogger().WithFields(fields)
}

func (p *LoggerProxy) WithContext(ctx context.Context) loginterface.LogEntry {
	return GetLogger().WithContext(ctx)
}

func (p *LoggerProxy) SetLogLevel(level string) error {
	return GetLogger().SetLogLevel(level)
}

func (p *LoggerProxy) GetLogLevel() string {
	return GetLogger().GetLogLevel()
}

func (p *LoggerProxy) SetOutput(output io.Writer) {
	GetLogger().SetOutput(output)
}

// SetHandler implements SFSlogLogger interface for advanced slog handler configuration.
// This delegates to the underlying logger if it supports SetHandler.
func (p *LoggerProxy) SetHandler(handler slog.Handler) error {
	logger := GetLogger()

	// Try to set handler via duck typing
	type setHandlerLogger interface{ SetHandler(interface{}) error }
	if sh, ok := logger.(setHandlerLogger); ok {
		return sh.SetHandler(handler)
	}

	return fmt.Errorf("underlying logger does not support SetHandler")
}

// NewLoggerProxy creates a new logger proxy that delegates all calls
// to the global logger managed by the internal package.
func NewLoggerProxy() loginterface.SFLogger {
	return &LoggerProxy{}
}
