// Package sftz is a timezone utility package for Go Snowflake Driver
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package sfloc

import (
	"testing"
)

type testcase struct {
	ss  string
	tt  string
	err error
}

func TestWithOffsetString(t *testing.T) {
	testcases := []testcase{
		{
			ss:  "+0700",
			tt:  "+0700",
			err: nil,
		},
		{
			ss:  "-1200",
			tt:  "-1200",
			err: nil,
		},
		{
			ss: "1200",
			tt: "-1200",
			err: &SnowflakeError{
				Number:      ErrInvalidTimezoneStr,
				Message:     errInvalidOffsetStr,
				MessageArgs: []interface{}{"1200"},
			},
		},
		{
			ss: "+12001",
			tt: "-1200",
			err: &SnowflakeError{
				Number:      ErrInvalidTimezoneStr,
				Message:     errInvalidOffsetStr,
				MessageArgs: []interface{}{"+12001"},
			},
		},
	}
	for _, t0 := range testcases {
		loc, err := WithOffsetString(t0.ss)
		if t0.err != nil {
			if t0.err != err {
				driverError1, ok1 := t0.err.(*SnowflakeError)
				driverError2, ok2 := err.(*SnowflakeError)
				if !(ok1 && ok2) {
					t.Fatalf("error expected: %v, got: %v", t0.err, err)
				}
				if driverError1.Number != driverError2.Number {
					t.Fatalf("error expected: %v, got: %v", t0.err, err)
				}
			}
		} else {
			if err != nil {
				t.Fatalf("%v", err)
			}
			if t0.tt != loc.String() {
				t.Fatalf("location string didn't match. expected: %v, got: %v", t0.tt, loc)
			}
		}
	}
}
