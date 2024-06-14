package gosnowflake

import "context"

// Define a type for the function that wraps goroutines
type GoroutineWrapperFunc func(ctx context.Context, f func())

// Global variable to hold the function pointer
var defaultDoesNothing = func(_ context.Context, f func()) {
	f()
}
var GoroutineWrapper GoroutineWrapperFunc = defaultDoesNothing
