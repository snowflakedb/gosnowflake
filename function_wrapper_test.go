package gosnowflake

import (
	"context"
	"fmt"
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

		numrows := 100000
		withCancelCtx, cancel := context.WithCancel(context.Background())
		// using async mode because I know that will trigger a goroutine to be fired off
		ctx := WithAsyncMode(withCancelCtx)
		dbt.mustQueryContext(ctx, fmt.Sprintf(selectRandomGenerator, numrows))
		cancel()

		assertTrueF(t, goWrapperCalled, "channel should be closed, indicating our wrapper worked")
	})
}
