package gosnowflake

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestCtxVal(t *testing.T) {
	type favContextKey string

	f := func(ctx context.Context, k favContextKey) error {
		if v := ctx.Value(k); v != nil {
			return nil
		}
		return fmt.Errorf("key not found: %v", k)
	}

	k := favContextKey("language")
	ctx := context.WithValue(context.Background(), k, "Go")

	k2 := favContextKey("data")
	ctx2 := context.WithValue(ctx, k2, "Snowflake")
	if err := f(ctx, k); err != nil {
		t.Error(err)
	}
	if err := f(ctx, "color"); err == nil {
		t.Error("should not have been found in context")
	}

	if err := f(ctx2, k); err != nil {
		t.Error(err)
	}
	if err := f(ctx2, k2); err != nil {
		t.Error(err)
	}
}

func TestLogCtx(t *testing.T) {
	log := CreateDefaultLogger()
	var ctx1 = context.WithValue(context.Background(), SFSessionIDKey, "sessID1")
	var ctx2 = context.WithValue(context.Background(), SFSessionUserKey, "admin")

	var b bytes.Buffer
	log.SetOutput(&b)
	assertNilF(t, log.SetLogLevel("trace"), "could not set log level")
	l := log.WithContext(ctx1, ctx2)
	l.Info("Hello 1")
	l.Warn("Hello 2")
	s := b.String()
	if len(s) <= 0 {
		t.Error("nothing written")
	}
	if !strings.Contains(s, "LOG_SESSION_ID=sessID1") {
		t.Error("context ctx1 keys/values not logged")
	}
	if !strings.Contains(s, "LOG_USER=admin") {
		t.Error("context ctx2 keys/values not logged")
	}
}
