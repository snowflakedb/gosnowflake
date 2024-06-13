package gosnowflake

// Define a type for the function that wraps goroutines
type GoroutineWrapperFunc func(f func())

// Global variable to hold the function pointer
var defaultDoesNothing = func(f func()){
	f()
}
var GoroutineWrapper GoroutineWrapperFunc = defaultDoesNothing
