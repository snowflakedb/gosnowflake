package gosnowflake

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"time"
)

func reportAsyncErrorFromContext(ctx context.Context) bool {
	val := ctx.Value(reportAsyncError)
	if val == nil {
		return false
	}
	a, ok := val.(bool)
	return a && ok
}

// panic message for no response from get func
type panicMessageType = struct {
	deadlineSet bool
	deadline    time.Time
	startTime   time.Time
	timeout     time.Duration
	statusCode  string
	stack       *[]uintptr
}

func newPanicMessage(
	ctx context.Context,
	resp *http.Response,
	startTime time.Time,
	timeout time.Duration,
) panicMessageType {

	var pcs [32]uintptr
	stackEntries := runtime.Callers(1, pcs[:])
	stackTrace := pcs[0:stackEntries]

	deadline, ok := ctx.Deadline()

	statusCode := "nil"
	if resp != nil {
		statusCode = fmt.Sprint(resp.StatusCode)
	}

	panicMessage := panicMessageType{
		deadlineSet: ok,
		deadline:    deadline,
		startTime:   startTime,
		timeout:     timeout,
		statusCode:  statusCode,
		stack:       &stackTrace,
	}
	return panicMessage
}
