// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type tcLocation struct {
	ss  string
	tt  string
	err error
}

func TestWithOffsetString(t *testing.T) {
	testcases := []tcLocation{
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
			ss:  "+0710",
			tt:  "+0710",
			err: nil,
		},
		{
			ss: "1200",
			tt: "",
			err: &SnowflakeError{
				Number:      ErrInvalidOffsetStr,
				Message:     errMsgInvalidOffsetStr,
				MessageArgs: []interface{}{"1200"},
			},
		},
		{
			ss: "x1200",
			tt: "",
			err: &SnowflakeError{
				Number:      ErrInvalidOffsetStr,
				Message:     errMsgInvalidOffsetStr,
				MessageArgs: []interface{}{"x1200"},
			},
		},
		{
			ss: "+12001",
			tt: "",
			err: &SnowflakeError{
				Number:      ErrInvalidOffsetStr,
				Message:     errMsgInvalidOffsetStr,
				MessageArgs: []interface{}{"+12001"},
			},
		},
		{
			ss: "x12001",
			tt: "",
			err: &SnowflakeError{
				Number:      ErrInvalidOffsetStr,
				Message:     errMsgInvalidOffsetStr,
				MessageArgs: []interface{}{"x12001"},
			},
		},
		{
			ss:  "-12CD",
			tt:  "",
			err: errors.New("parse int error"), // can this be more specific?
		},
		{
			ss:  "+ABCD",
			tt:  "",
			err: errors.New("parse int error"), // can this be more specific?
		},
	}
	for _, t0 := range testcases {
		t.Run(t0.ss, func(t *testing.T) {
			loc, err := LocationWithOffsetString(t0.ss)
			if t0.err != nil {
				if t0.err != err {
					driverError1, ok1 := t0.err.(*SnowflakeError)
					driverError2, ok2 := err.(*SnowflakeError)
					if ok1 && ok2 && driverError1.Number != driverError2.Number {
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
		})
	}
}

func TestGetCurrentLocation(t *testing.T) {
	specificTz := "Pacific/Honolulu"
	specificLoc, err := time.LoadLocation(specificTz)
	if err != nil {
		t.Fatalf("Cannot initialize specific timezone location")
	}
	incorrectTz := "Not/exists"
	testcases := []struct {
		params map[string]*string
		loc    *time.Location
	}{
		{
			params: map[string]*string{},
			loc:    time.Now().Location(),
		},
		{
			params: map[string]*string{
				"timezone": nil,
			},
			loc: time.Now().Location(),
		},
		{
			params: map[string]*string{
				"timezone": &specificTz,
			},
			loc: specificLoc,
		},
		{
			params: map[string]*string{
				"timezone": &incorrectTz,
			},
			loc: time.Now().Location(),
		},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%v", tc.loc), func(t *testing.T) {
			loc := getCurrentLocation(tc.params)
			if !reflect.DeepEqual(*loc, *tc.loc) {
				t.Fatalf("location mismatch. expected: %v, got: %v", tc.loc, loc)
			}
		})
	}
}
