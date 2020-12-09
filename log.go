package gosnowflake

import (
	"fmt"
	rlog "github.com/sirupsen/logrus"
	"io"
	"path"
	"runtime"
)

// SFLogger Snowflake logger wrapper interface to expose the same interface as FieldLogger defined in logrus
type SFLogger interface {
	rlog.FieldLogger
	SetOutput(output io.Writer)
	SetLogLevel(level string) error
}

// SFCallerPrettyfier to provide base file name and function name from calling frame used in SFLogger
func SFCallerPrettyfier(frame *runtime.Frame) (string, string) {
	return path.Base(frame.Function), fmt.Sprintf("%s:%d", path.Base(frame.File), frame.Line)
}

type defaultLogger struct {
	*rlog.Logger
}

func (log *defaultLogger) SetLogLevel(level string) error {
	actualLevel, err := rlog.ParseLevel(level)
	if err != nil {
		return err
	}
	log.Level = actualLevel
	return nil
}

// DefaultLogger return a new instance of SFLogger with default config
func DefaultLogger() SFLogger {
	var rLogger = rlog.New()
	var formatter = rlog.TextFormatter{CallerPrettyfier: SFCallerPrettyfier}
	rLogger.SetReportCaller(true)
	rLogger.SetFormatter(&formatter)
	return &defaultLogger{Logger: rLogger}
}
