// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	rlog "github.com/sirupsen/logrus"
)

// SFSessionIDKey is context key of session id
const SFSessionIDKey contextKey = "LOG_SESSION_ID"

// SFSessionUserKey is context key of  user id of a session
const SFSessionUserKey contextKey = "LOG_USER"

// map which stores a string which will be used as a log key to the function which
// will be called to get the log value out of the context
var clientLogContextHooks = map[string]ClientLogContextHook{}

// ClientLogContextHook is a client-defined hook that can be used to insert log
// fields based on the Context.
type ClientLogContextHook func(context.Context) string

// RegisterLogContextHook registers a hook that can be used to extract fields
// from the Context and associated with log messages using the provided key. This
// function is not thread-safe and should only be called on startup.
func RegisterLogContextHook(contextKey string, ctxExtractor ClientLogContextHook) {
	clientLogContextHooks[contextKey] = ctxExtractor
}

// LogKeys registers string-typed context keys to be written to the logs when
// logger.WithContext is used
var LogKeys = [...]contextKey{SFSessionIDKey, SFSessionUserKey}

// Fields
type Fields map[string]any

type LogEntry interface {
	Tracef(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Printf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})

	Trace(args ...interface{})
	Debug(args ...interface{})
	Info(args ...interface{})
	Print(args ...interface{})
	Warn(args ...interface{})
	Warning(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})
	Panic(args ...interface{})

	Traceln(args ...interface{})
	Debugln(args ...interface{})
	Infoln(args ...interface{})
	Println(args ...interface{})
	Warnln(args ...interface{})
	Warningln(args ...interface{})
	Errorln(args ...interface{})
	Fatalln(args ...interface{})
	Panicln(args ...interface{})
}

// SFLogger Snowflake logger interface to expose FieldLogger defined in logrus
type SFLogger interface {
	LogEntry
	WithField(key string, value interface{}) LogEntry
	WithFields(fields Fields) LogEntry
	WithError(err error) LogEntry
	WithTime(t time.Time) LogEntry

	SetLogLevel(level string) error
	GetLogLevel() string
	WithContext(ctx ...context.Context) LogEntry
	SetOutput(output io.Writer)
	CloseFileOnLoggerReplace(file *os.File) error
	Replace(newLogger *SFLogger)
}

// SFCallerPrettyfier to provide base file name and function name from calling frame used in SFLogger
func SFCallerPrettyfier(frame *runtime.Frame) (string, string) {
	return path.Base(frame.Function), fmt.Sprintf("%s:%d", path.Base(frame.File), frame.Line)
}

var _ SFLogger = &defaultLogger{} // ensure defaultLogger isa SFLogger.

type defaultLogger struct {
	inner   *rlog.Logger
	enabled bool
	file    *os.File
}

type sfTextFormatter struct {
	rlog.TextFormatter
}

func (f *sfTextFormatter) Format(entry *rlog.Entry) ([]byte, error) {
	// mask all secrets before calling the default Format method
	entry.Message = maskSecrets(entry.Message)
	return f.TextFormatter.Format(entry)
}

// SetLogLevel set logging level for calling defaultLogger
func (log *defaultLogger) SetLogLevel(level string) error {
	newEnabled := strings.ToUpper(level) != "OFF"
	log.enabled = newEnabled
	if newEnabled {
		actualLevel, err := rlog.ParseLevel(level)
		if err != nil {
			return err
		}
		log.inner.Level = actualLevel
	}
	return nil
}

// GetLogLevel return current log level
func (log *defaultLogger) GetLogLevel() string {
	if !log.enabled {
		return "OFF"
	}
	return log.inner.Level.String()
}

// CloseFileOnLoggerReplace set a file to be closed when releasing resources occupied by the logger
func (log *defaultLogger) CloseFileOnLoggerReplace(file *os.File) error {
	if log.file != nil && log.file != file {
		return fmt.Errorf("could not set a file to close on logger reset because there were already set one")
	}
	log.file = file
	return nil
}

// Replace substitute logger by a given one
func (log *defaultLogger) Replace(newLogger *SFLogger) {
	SetLogger(newLogger)
	closeLogFile(log.file)
}

func closeLogFile(file *os.File) {
	if file != nil {
		err := file.Close()
		if err != nil {
			logger.Errorf("failed to close log file: %s", err)
		}
	}
}

// WithContext return Entry to include fields in context
func (log *defaultLogger) WithContext(ctxs ...context.Context) LogEntry {
	if !log.enabled {
		return log
	}
	fields := context2Fields(ctxs...)
	return log.WithFields(*fields)
}

// CreateDefaultLogger return a new instance of SFLogger with default config
func CreateDefaultLogger() SFLogger {
	var rLogger = rlog.New()
	var formatter = new(sfTextFormatter)
	formatter.CallerPrettyfier = SFCallerPrettyfier
	rLogger.SetFormatter(formatter)
	rLogger.SetReportCaller(true)
	var ret = defaultLogger{inner: rLogger, enabled: true}
	return &ret
}

var _ LogEntry = &entryBridge{} // ensure entryBridge isa LogEntry.

type entryBridge struct {
	*rlog.Entry
}

// WithTime overrides the time of the log entry.
func (log *defaultLogger) WithTime(t time.Time) LogEntry {
	return &entryBridge{log.inner.WithTime(t)}
}

// WithError overrides the error of the log entry.
func (log *defaultLogger) WithError(err error) LogEntry {
	return &entryBridge{log.inner.WithError(err)}
}

// WithFields add a map of fields to the log entry.
func (log *defaultLogger) WithFields(fields Fields) LogEntry {
	m := map[string]any(fields)
	return &entryBridge{log.inner.WithFields(m)}
}

// WithField adds a single key/value pair to the log entry.
func (log *defaultLogger) WithField(key string, value interface{}) LogEntry {
	return &entryBridge{log.inner.WithField(key, value)}
}

func (log *defaultLogger) Tracef(format string, args ...interface{}) {
	if log.enabled {
		log.inner.Tracef(format, args...)
	}
}

func (log *defaultLogger) Debugf(format string, args ...interface{}) {
	if log.enabled {
		log.inner.Debugf(format, args...)
	}
}

func (log *defaultLogger) Infof(format string, args ...interface{}) {
	if log.enabled {
		log.inner.Infof(format, args...)
	}
}

func (log *defaultLogger) Printf(format string, args ...interface{}) {
	if log.enabled {
		log.inner.Printf(format, args...)
	}
}

func (log *defaultLogger) Warnf(format string, args ...interface{}) {
	if log.enabled {
		log.inner.Warnf(format, args...)
	}
}

func (log *defaultLogger) Warningf(format string, args ...interface{}) {
	if log.enabled {
		log.inner.Warningf(format, args...)
	}
}

func (log *defaultLogger) Errorf(format string, args ...interface{}) {
	if log.enabled {
		log.inner.Errorf(format, args...)
	}
}

func (log *defaultLogger) Fatalf(format string, args ...interface{}) {
	if log.enabled {
		log.inner.Fatalf(format, args...)
	}
}

func (log *defaultLogger) Panicf(format string, args ...interface{}) {
	if log.enabled {
		log.inner.Panicf(format, args...)
	}
}

func (log *defaultLogger) Trace(args ...interface{}) {
	if log.enabled {
		log.inner.Trace(args...)
	}
}

func (log *defaultLogger) Debug(args ...interface{}) {
	if log.enabled {
		log.inner.Debug(args...)
	}
}

func (log *defaultLogger) Info(args ...interface{}) {
	if log.enabled {
		log.inner.Info(args...)
	}
}

func (log *defaultLogger) Print(args ...interface{}) {
	if log.enabled {
		log.inner.Print(args...)
	}
}

func (log *defaultLogger) Warn(args ...interface{}) {
	if log.enabled {
		log.inner.Warn(args...)
	}
}

func (log *defaultLogger) Warning(args ...interface{}) {
	if log.enabled {
		log.inner.Warning(args...)
	}
}

func (log *defaultLogger) Error(args ...interface{}) {
	if log.enabled {
		log.inner.Error(args...)
	}
}

func (log *defaultLogger) Fatal(args ...interface{}) {
	if log.enabled {
		log.inner.Fatal(args...)
	}
}

func (log *defaultLogger) Panic(args ...interface{}) {
	if log.enabled {
		log.inner.Panic(args...)
	}
}

func (log *defaultLogger) Traceln(args ...interface{}) {
	if log.enabled {
		log.inner.Traceln(args...)
	}
}

func (log *defaultLogger) Debugln(args ...interface{}) {
	if log.enabled {
		log.inner.Debugln(args...)
	}
}

func (log *defaultLogger) Infoln(args ...interface{}) {
	if log.enabled {
		log.inner.Infoln(args...)
	}
}

func (log *defaultLogger) Println(args ...interface{}) {
	if log.enabled {
		log.inner.Println(args...)
	}
}

func (log *defaultLogger) Warnln(args ...interface{}) {
	if log.enabled {
		log.inner.Warnln(args...)
	}
}

func (log *defaultLogger) Warningln(args ...interface{}) {
	if log.enabled {
		log.inner.Warningln(args...)
	}
}

func (log *defaultLogger) Errorln(args ...interface{}) {
	if log.enabled {
		log.inner.Errorln(args...)
	}
}

func (log *defaultLogger) Fatalln(args ...interface{}) {
	if log.enabled {
		log.inner.Fatalln(args...)
	}
}

func (log *defaultLogger) Panicln(args ...interface{}) {
	if log.enabled {
		log.inner.Panicln(args...)
	}
}

// SetOutput sets the logger output.
func (log *defaultLogger) SetOutput(output io.Writer) {
	log.inner.SetOutput(output)
}

// SetLogger set a new logger of SFLogger interface for gosnowflake
func SetLogger(inLogger *SFLogger) {
	logger = *inLogger //.(*defaultLogger)
}

// GetLogger return logger that is not public
func GetLogger() SFLogger {
	return logger
}

func context2Fields(ctxs ...context.Context) *Fields {
	var fields = Fields{}
	if len(ctxs) <= 0 {
		return &fields
	}

	for i := 0; i < len(LogKeys); i++ {
		for _, ctx := range ctxs {
			if ctx.Value(LogKeys[i]) != nil {
				fields[string(LogKeys[i])] = ctx.Value(LogKeys[i])
			}
		}
	}

	for key, hook := range clientLogContextHooks {
		for _, ctx := range ctxs {
			if value := hook(ctx); value != "" {
				fields[key] = value
			}
		}
	}

	return &fields
}
