package gosnowflake

import (
	"math"
	"time"
)

type ExecutionTimer struct {
	startTime *time.Time
	endTime   *time.Time
}

func NewExecutionTimer() *ExecutionTimer {
	return &ExecutionTimer{}
}

func (tm *ExecutionTimer) wasStarted() bool {
	return tm.startTime != nil
}

func (tm *ExecutionTimer) start() *ExecutionTimer {
	now := time.Now()
	tm.startTime = &now
	tm.endTime = nil
	return tm
}

func (tm *ExecutionTimer) stop() *ExecutionTimer {
	if !tm.wasStarted() {
		logger.Debug("Tried to stop timer, that was not started.")
	}
	now := time.Now()
	tm.endTime = &now
	return tm
}

func (tm *ExecutionTimer) getDuration() float64 {
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
