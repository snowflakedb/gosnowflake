// Package sfloc is a timezone utility package for Go Snowflake Driver
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package sfloc

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
)

const (
	// ErrInvalidOffsetStr is an error code for the case where a offset string is invalid. The input string must
	// consist of sHHMI where one sign character '+'/'-' followed by zero filled hours and minutes
	ErrInvalidOffsetStr = 268002

	errMsgInvalidOffsetStr = "offset must be a string consist of sHHMI where one sign character '+'/'-' followed by zero filled hours and minutes: %v"
)

// SnowflakeError is a error type including various Snowflake specific information.
type SnowflakeError struct {
	Number      int
	Message     string
	MessageArgs []interface{}
}

func (se *SnowflakeError) Error() string {
	message := se.Message
	if len(se.MessageArgs) > 0 {
		message = fmt.Sprintf(se.Message, se.MessageArgs)
	}
	return fmt.Sprintf("%06d (): %s", se.Number, message)
}

var timezones map[int]*time.Location
var updateTimezoneMutex *sync.Mutex

// WithOffset returns an offset (minutes) based Location object.
func WithOffset(offset int) *time.Location {
	updateTimezoneMutex.Lock()
	defer updateTimezoneMutex.Unlock()
	loc := timezones[offset]
	if loc != nil {
		return loc
	}
	loc = genTimezone(offset)
	timezones[offset] = loc
	return loc
}

// WithOffsetString returns an offset based Location object. The offset string must consist of sHHMI where one sign
// character '+'/'-' followed by zero filled hours and minutes
func WithOffsetString(offsets string) (loc *time.Location, err error) {
	if len(offsets) != 5 {
		return nil, &SnowflakeError{
			Number:      ErrInvalidOffsetStr,
			Message:     errMsgInvalidOffsetStr,
			MessageArgs: []interface{}{offsets},
		}
	}
	if offsets[0] != '-' && offsets[0] != '+' {
		return nil, &SnowflakeError{
			Number:      ErrInvalidOffsetStr,
			Message:     errMsgInvalidOffsetStr,
			MessageArgs: []interface{}{offsets},
		}
	}
	s := 1
	if offsets[0] == '-' {
		s = -1
	}
	var h, m int64
	h, err = strconv.ParseInt(offsets[1:3], 10, 64)
	if err != nil {
		return
	}
	m, err = strconv.ParseInt(offsets[3:], 10, 64)
	if err != nil {
		return
	}
	offset := s * (int(h)*60 + int(m))
	loc = WithOffset(offset)
	return
}

func genTimezone(offset int) *time.Location {
	var offsetSign string
	var toffset int
	if offset < 0 {
		offsetSign = "-"
		toffset = -offset
	} else {
		offsetSign = "+"
		toffset = offset
	}
	glog.V(2).Infof("offset: %v", offset)
	return time.FixedZone(fmt.Sprintf("%v%02d%02d", offsetSign, toffset/60, toffset%60), int(offset)*60)
}

func init() {
	updateTimezoneMutex = &sync.Mutex{}
	timezones = make(map[int]*time.Location, 48)
	// pre-generate all common timezones
	for i := -720; i <= 720; i += 30 {
		glog.V(2).Infof("offset: %v", i)
		timezones[i] = genTimezone(i)
	}
}
