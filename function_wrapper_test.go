package gosnowflake

import (
	"context"
	"testing"
)

var (
	goWrapperCalled = false
)

// this is the go wrapper function we are going to pass into GoroutineWrapper.
// we will know that this has been called if the channel is closed
var closeGoWrapperCalledChannel = func(ctx context.Context, f func()) {
	goWrapperCalled = true

	f()
}

func TestGoWrapper(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		oldGoroutineWrapper := GoroutineWrapper
		t.Cleanup(func() {
			GoroutineWrapper = oldGoroutineWrapper
		})

		GoroutineWrapper = closeGoWrapperCalledChannel

		ctx := WithAsyncMode(context.Background())
		rows := dbt.mustQueryContext(ctx, "SELECT 1")
		defer rows.Close()

		assertTrueF(t, goWrapperCalled, "channel should be closed, indicating our wrapper worked")
	})
}
