package gosnowflake

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestLogLevelEnabled(t *testing.T) {
	log := CreateDefaultLogger() // via the SFLogger interface.
	err := log.SetLogLevel("info")
	if err != nil {
		t.Fatalf("log level could not be set %v", err)
	}
	if log.GetLogLevel() != "info" {
		t.Fatalf("log level should be info but is %v", log.GetLogLevel())
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

	// default logger level is info
	logger.Info("info")
	logger.Infof("info%v", "f")

	// debug and trace won't write to log since they are higher than info level
	logger.Debug("debug")
	logger.Debugf("debug%v", "f")

	logger.Trace("trace")
	logger.Tracef("trace%v", "f")

	logger.Warn("warn")
	logger.Warnf("warn%v", "f")

	logger.Error("error")
	logger.Errorf("error%v", "f")

	// verify output
	var strbuf = buf.String()

	if !strings.Contains(strbuf, "info") ||
		!strings.Contains(strbuf, "warn") ||
		!strings.Contains(strbuf, "error") {
		t.Fatalf("unexpected output in log: %v", strbuf)
	}
	if strings.Contains(strbuf, "debug") ||
		strings.Contains(strbuf, "trace") {
		t.Fatalf("debug/trace should not be in log: %v", strbuf)
	}
}

func TestOffLogLevel(t *testing.T) {
	logger := CreateDefaultLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)
	err := logger.SetLogLevel("OFF")
	assertNilF(t, err)

	logger.Info("info")
	logger.Infof("info%v", "f")
	logger.Debug("debug")
	logger.Debugf("debug%v", "f")
	logger.Trace("trace")
	logger.Tracef("trace%v", "f")
	logger.Warn("warn")
	logger.Warnf("warn%v", "f")
	logger.Error("error")
	logger.Errorf("error%v", "f")

	assertEqualE(t, buf.Len(), 0, "log messages count")
	assertEqualE(t, logger.GetLogLevel(), "OFF", "log level")
}

func TestLogSetLevel(t *testing.T) {
	logger := CreateDefaultLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)
	_ = logger.SetLogLevel("trace")

	logger.Trace("should print at trace level")
	logger.Debug("should print at debug level")

	var strbuf = buf.String()

	if !strings.Contains(strbuf, "trace level") ||
		!strings.Contains(strbuf, "debug level") {
		t.Fatalf("unexpected output in log: %v", strbuf)
	}
}

func TestLowerLevelsAreSuppressed(t *testing.T) {
	logger := CreateDefaultLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)
	_ = logger.SetLogLevel("info")

	logger.Trace("should print at trace level")
	logger.Debug("should print at debug level")
	logger.Info("should print at info level")
	logger.Warn("should print at warn level")
	logger.Error("should print at error level")

	var strbuf = buf.String()

	if strings.Contains(strbuf, "trace level") ||
		strings.Contains(strbuf, "debug level") {
		t.Fatalf("unexpected debug and trace are not present in log: %v", strbuf)
	}

	if !strings.Contains(strbuf, "info level") ||
		!strings.Contains(strbuf, "warn level") ||
		!strings.Contains(strbuf, "error level") {
		t.Fatalf("expected info, warn, error output in log: %v", strbuf)
	}
}

func TestLogWithField(t *testing.T) {
	logger := CreateDefaultLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)

	logger.WithField("field", "test").Info("hello")
	var strbuf = buf.String()
	if !strings.Contains(strbuf, "field") || !strings.Contains(strbuf, "test") {
		t.Fatalf("expected field and test in output: %v", strbuf)
	}
}

type testRequestIDCtxKey struct{}

func TestLogKeysDefault(t *testing.T) {
	logger := CreateDefaultLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)

	ctx := context.Background()

	// set the sessionID on the context to see if we have it in the logs
	sessionIDContextValue := "sessionID"
	ctx = context.WithValue(ctx, SFSessionIDKey, sessionIDContextValue)

	userContextValue := "madison"
	ctx = context.WithValue(ctx, SFSessionUserKey, userContextValue)

	// base case (not using RegisterContextVariableToLog to add additional types )
	logger.WithContext(ctx).Info("test")
	var strbuf = buf.String()
	if !strings.Contains(strbuf, string(SFSessionIDKey)) || !strings.Contains(strbuf, sessionIDContextValue) {
		t.Fatalf("expected that sfSessionIdKey would be in logs if logger.WithContext was used, but got: %v", strbuf)
	}
	if !strings.Contains(strbuf, string(SFSessionUserKey)) || !strings.Contains(strbuf, userContextValue) {
		t.Fatalf("expected that SFSessionUserKey would be in logs if logger.WithContext was used, but got: %v", strbuf)
	}
}

func TestLogKeysWithRegisterContextVariableToLog(t *testing.T) {
	logger := CreateDefaultLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)

	ctx := context.Background()

	// set the sessionID on the context to see if we have it in the logs
	sessionIDContextValue := "sessionID"
	ctx = context.WithValue(ctx, SFSessionIDKey, sessionIDContextValue)

	userContextValue := "testUser"
	ctx = context.WithValue(ctx, SFSessionUserKey, userContextValue)

	// test that RegisterContextVariableToLog works with non string keys
	logKey := "REQUEST_ID"
	contextIntVal := 123
	ctx = context.WithValue(ctx, testRequestIDCtxKey{}, contextIntVal)

	getRequestKeyFunc := func(ctx context.Context) string {
		if requestContext, ok := ctx.Value(testRequestIDCtxKey{}).(int); ok {
			return fmt.Sprint(requestContext)
		}
		return ""
	}

	RegisterLogContextHook(logKey, getRequestKeyFunc)

	// base case (not using RegisterContextVariableToLog to add additional types )
	logger.WithContext(ctx).Info("test")
	var strbuf = buf.String()

	if !strings.Contains(strbuf, string(SFSessionIDKey)) || !strings.Contains(strbuf, sessionIDContextValue) {
		t.Fatalf("expected that sfSessionIdKey would be in logs if logger.WithContext and RegisterContextVariableToLog was used, but got: %v", strbuf)
	}
	if !strings.Contains(strbuf, string(SFSessionUserKey)) || !strings.Contains(strbuf, userContextValue) {
		t.Fatalf("expected that SFSessionUserKey would be in logs if logger.WithContext and RegisterContextVariableToLog was used, but got: %v", strbuf)
	}
	if !strings.Contains(strbuf, logKey) || !strings.Contains(strbuf, fmt.Sprint(contextIntVal)) {
		t.Fatalf("expected that REQUEST_ID would be in logs if logger.WithContext and RegisterContextVariableToLog was used, but got: %v", strbuf)
	}
}

func TestLogMaskSecrets(t *testing.T) {
	logger := CreateDefaultLogger()
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)

	ctx := context.Background()
	query := "create user testuser password='testpassword'"
	logger.WithContext(ctx).Infof("Query: %#v", query)

	// verify output
	expected := "create user testuser password='****"
	var strbuf = buf.String()
	if !strings.Contains(strbuf, expected) {
		t.Fatalf("expected that password would be masked. WithContext was used, but got: %v", strbuf)
	}
}
