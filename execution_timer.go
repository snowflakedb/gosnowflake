package gosnowflake

import (
	"math"
	"time"
)

// executionTimer is a utility to measure code execution time.
type executionTimer struct {
	startTime *time.Time
	endTime   *time.Time
}

// newExecutionTimer creates a new instance of an executionTimer.
func newExecutionTimer() *executionTimer {
	return &executionTimer{}
}

func (tm *executionTimer) wasStarted() bool {
	return tm.startTime != nil
}

func (tm *executionTimer) start() *executionTimer {
	now := time.Now()
	tm.startTime = &now
	tm.endTime = nil
	return tm
}

func (tm *executionTimer) stop() *executionTimer {
	if !tm.wasStarted() {
		logger.Debug("Tried to stop timer, that was not started.")
	}
	now := time.Now()
	tm.endTime = &now
	return tm
}

func (tm *executionTimer) getDuration() float64 {
	if !tm.wasStarted() {
		return 0
	}
	if tm.endTime == nil {
		now := time.Now()
		tm.endTime = &now
	}
	duration := tm.endTime.Sub(*tm.startTime)
	millis := float64(duration) / float64(time.Millisecond)
	return math.Round(millis*100) / 100
}
