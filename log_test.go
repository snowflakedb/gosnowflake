// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	rlog "github.com/sirupsen/logrus"
)

func createTestLogger() defaultLogger {
	var rLogger = rlog.New()
	var ret = defaultLogger{inner: rLogger}
	return ret
}

func TestIsLevelEnabled(t *testing.T) {
	logger := createTestLogger()
	logger.SetLevel(rlog.TraceLevel)
	if !logger.IsLevelEnabled(rlog.TraceLevel) {
		t.Fatalf("log level should be trace but is %v", logger.GetLevel())
	}
}

func TestLogFunction(t *testing.T) {
	logger := createTestLogger()
	buf := &bytes.Buffer{}
	var formatter = rlog.TextFormatter{CallerPrettyfier: SFCallerPrettyfier}
	logger.SetFormatter(&formatter)
	logger.SetReportCaller(true)
	logger.SetOutput(buf)
	logger.SetLevel(rlog.TraceLevel)

	logger.Log(rlog.TraceLevel, "hello world")
	logger.Logf(rlog.TraceLevel, "log %v", "format")
	logger.Logln(rlog.TraceLevel, "log line")

	var strbuf = buf.String()
	if !strings.Contains(strbuf, "hello world") &&
		!strings.Contains(strbuf, "log format") &&
		!strings.Contains(strbuf, "log line") {
		t.Fatalf("unexpected output in log %v", strbuf)
	}
}

func TestSetLogLevelError(t *testing.T) {
	logger := CreateDefaultLogger()
	err := logger.SetLogLevel("unknown")
	if err == nil {
		t.Fatal("should have thrown an error")
	}
}

func TestDefaultLogLevel(t *testing.T) {
	logger := CreateDefaultLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)
	SetLogger(&logger)

	// default logger level is info
	logger.Info("info")
	logger.Infof("info%v", "f")
	logger.Infoln("infoln")

	// debug and trace won't write to log since they are higher than info level
	logger.Debug("debug")
	logger.Debugf("debug%v", "f")
	logger.Debugln("debugln")

	logger.Trace("trace")
	logger.Tracef("trace%v", "f")
	logger.Traceln("traceln")

	// print, warning and error should write to log since they are lower than info
	logger.Print("print")
	logger.Printf("print%v", "f")
	logger.Println("println")

	logger.Warn("warn")
	logger.Warnf("warn%v", "f")
	logger.Warnln("warnln")

	logger.Warning("warning")
	logger.Warningf("warning%v", "f")
	logger.Warningln("warningln")

	logger.Error("error")
	logger.Errorf("error%v", "f")
	logger.Errorln("errorln")

	// verify output
	var strbuf = buf.String()

	if strings.Contains(strbuf, "debug") &&
		strings.Contains(strbuf, "trace") &&
		!strings.Contains(strbuf, "info") &&
		!strings.Contains(strbuf, "print") &&
		!strings.Contains(strbuf, "warn") &&
		!strings.Contains(strbuf, "warning") &&
		!strings.Contains(strbuf, "error") {
		t.Fatalf("unexpected output in log: %v", strbuf)
	}
}

func TestLogSetLevel(t *testing.T) {
	logger := GetLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)
	logger.SetLogLevel("trace")

	logger.Trace("should print at trace level")
	logger.Debug("should print at debug level")

	var strbuf = buf.String()

	if !strings.Contains(strbuf, "trace level") &&
		!strings.Contains(strbuf, "debug level") {
		t.Fatalf("unexpected output in log: %v", strbuf)
	}
}

func TestLogWithError(t *testing.T) {
	logger := CreateDefaultLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)

	err := errors.New("error")
	logger.WithError(err).Info("hello world")

	var strbuf = buf.String()
	if !strings.Contains(strbuf, "error=error") {
		t.Fatalf("unexpected output in log: %v", strbuf)
	}
}

func TestLogWithTime(t *testing.T) {
	logger := createTestLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)

	ti := time.Now()
	logger.WithTime(ti).Info("hello")
	time.Sleep(3 * time.Second)

	var strbuf = buf.String()
	if !strings.Contains(strbuf, ti.Format(time.RFC3339)) {
		t.Fatalf("unexpected string in output: %v", strbuf)
	}
}

func TestLogWithField(t *testing.T) {
	logger := CreateDefaultLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)

	logger.WithField("field", "test").Info("hello")
	var strbuf = buf.String()
	if !strings.Contains(strbuf, "field=test") {
		t.Fatalf("unexpected string in output: %v", strbuf)
	}
}

func TestLogLevelFunctions(t *testing.T) {
	logger := createTestLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)

	logger.TraceFn(func() []interface{} {
		return []interface{}{
			"trace function",
		}
	})

	logger.DebugFn(func() []interface{} {
		return []interface{}{
			"debug function",
		}
	})

	logger.InfoFn(func() []interface{} {
		return []interface{}{
			"info function",
		}
	})

	logger.PrintFn(func() []interface{} {
		return []interface{}{
			"print function",
		}
	})

	logger.WarningFn(func() []interface{} {
		return []interface{}{
			"warning function",
		}
	})

	logger.ErrorFn(func() []interface{} {
		return []interface{}{
			"error function",
		}
	})

	// check that info, print, warning and error were outputted to the log.
	var strbuf = buf.String()

	if strings.Contains(strbuf, "debug") &&
		strings.Contains(strbuf, "trace") &&
		!strings.Contains(strbuf, "info") &&
		!strings.Contains(strbuf, "print") &&
		!strings.Contains(strbuf, "warning") &&
		!strings.Contains(strbuf, "error") {
		t.Fatalf("unexpected output in log: %v", strbuf)
	}
}
