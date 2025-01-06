// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"fmt"
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
	var log = logger
	var ctx1 = context.WithValue(context.Background(), SFSessionIDKey, "sessID1")
	var ctx2 = context.WithValue(context.Background(), SFSessionUserKey, "admin")

	l := log.WithContext(ctx1, ctx2)
	l.Info("Hello 1")
	l.Warn("Hello 2")
	// what purpose does this test serve? ... nothing is being validated except that it compiles and runs.
}
