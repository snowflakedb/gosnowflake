package gosnowflake

import (
	"context"
	"fmt"
	"testing"
)

func TestCtxVal(t *testing.T) {
	type favContextKey string

	f := func(ctx context.Context, k favContextKey) {
		if v := ctx.Value(k); v != nil {
			fmt.Println("found value:", v)
			return
		}
		fmt.Println("key not found:", k)
	}

	k := favContextKey("language")
	ctx := context.WithValue(context.Background(), k, "Go")

	k2 := favContextKey("data")
	ctx2 := context.WithValue(ctx, k2, "Snowflake")
	f(ctx, k)
	f(ctx, favContextKey("color"))

	f(ctx2, k)
	f(ctx2, k2)
}

func TestLogEntryCtx(t *testing.T) {
	var log = logger
	var ctx1 = context.WithValue(context.TODO(), SFSessionIDKey, "sessID1")
	var ctx2 = context.WithValue(context.TODO(), SFSessionUserKey, "admin")

	fs1 := context2Fields(ctx1)
	fs2 := context2Fields(ctx2)
	l1 := log.WithFields(*fs1)
	l2 := log.WithFields(*fs2)
	l1.Info("Hello 1")
	l2.Warning("Hello 2")

	//log.WithContext(ctx).Info("new log text 1")
	//log.WithContext(ctx2).Info("new log text 2")

}
