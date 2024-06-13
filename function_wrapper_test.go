package gosnowflake

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

var (
	goWrapperCalled chan struct{}
	once       sync.Once
)

// Initialize the goWrapperCalled channel which we will use for testing the goroutine wrap function
func initGoWrapperCalledChannel() {
	once.Do(func() {
		goWrapperCalled = make(chan struct{})
	})
}

// Check if the goWrapperCalled by making sure channel is clsoed
func isGoWrapperChannelClosed() bool {
	select {
	case <-goWrapperCalled:
		return true
	default:
		return false
	}
}

// this is the go wrapper function we are going to pass into GoroutineWrapper.
// we will know that this has been called if the channel is closed
var closeGoWrapperCalledChannel = func(f func()) {
	if !isGoWrapperChannelClosed() {
		close(goWrapperCalled)
	}
	
	f()
}

func TestGoWrapper(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		initGoWrapperCalledChannel()
		GoroutineWrapper = closeGoWrapperCalledChannel

		numrows := 100000
		withCancelCtx, cancel := context.WithCancel(context.Background())
		// using async mode because I know that will trigger a goroutine to be fired off
		ctx := WithAsyncMode(withCancelCtx)
		dbt.mustQueryContext(ctx, fmt.Sprintf(selectRandomGenerator, numrows))
		cancel()

		assertTrueF(t, isGoWrapperChannelClosed(), "channel should be closed, indicating our wrapper worked")
	})
}