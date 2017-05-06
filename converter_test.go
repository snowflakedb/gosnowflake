// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"database/sql/driver"
	"math/cmplx"
	"testing"
	"time"
)

type tcGoTypeToSnowflake struct {
	in    interface{}
	tmode string
	out   string
}

func TestGoTypeToSnowflake(t *testing.T) {
	testcases := []tcGoTypeToSnowflake{
		{in: int64(123), tmode: "", out: "FIXED"},
		{in: float64(234.56), tmode: "", out: "REAL"},
		{in: true, tmode: "", out: "BOOLEAN"},
		{in: "teststring", tmode: "", out: "TEXT"},
		{in: nil, tmode: "", out: "TEXT"}, // nil is taken as TEXT
		{in: DataTypeBinary, tmode: "", out: "CHANGE_TYPE"},
		{in: DataTypeTimestampLtz, tmode: "", out: "CHANGE_TYPE"},
		{in: DataTypeTimestampNtz, tmode: "", out: "CHANGE_TYPE"},
		{in: DataTypeTimestampTz, tmode: "", out: "CHANGE_TYPE"},
		{in: time.Now(), tmode: "TIMESTAMP_NTZ", out: "TIMESTAMP_NTZ"},
		{in: time.Now(), tmode: "TIMESTAMP_TZ", out: "TIMESTAMP_TZ"},
		{in: time.Now(), tmode: "TIMESTAMP_LTZ", out: "TIMESTAMP_LTZ"},
		{in: []byte{1, 2, 3}, tmode: "BINARY", out: "BINARY"},
		// negative
		{in: 123, tmode: "", out: "TEXT"},
		{in: int8(12), tmode: "", out: "TEXT"},
		{in: int32(456), tmode: "", out: "TEXT"},
		{in: uint(456), tmode: "", out: "TEXT"},
		{in: uint8(12), tmode: "", out: "TEXT"},
		{in: uint64(456), tmode: "", out: "TEXT"},
		{in: []byte{100}, tmode: "", out: "TEXT"},
	}
	for _, test := range testcases {
		a := goTypeToSnowflake(test.in, test.tmode)
		if a != test.out {
			t.Errorf("failed. in: %v, tmode: %v, expected: %v, got: %v", test.in, test.tmode, test.out, a)
		}
	}
}

func TestValueToString(t *testing.T) {
	v := cmplx.Sqrt(-5 + 12i) // should never happen as Go sql package must have already validated.
	_, err := valueToString(v, "")
	if err == nil {
		t.Errorf("should raise error: %v", v)
	}
}

func TestExtractTimestamp(t *testing.T) {
	s := "1234abcdef"
	_, _, err := extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
	s = "1234abc.def"
	_, _, err = extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
	s = "1234.def"
	_, _, err = extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
}

func TestStringToValue(t *testing.T) {
	var source string
	var dest driver.Value
	var err error
	var rowType *execResponseRowType
	source = "abcdefg"

	types := []string{
		"date", "time", "timestamp_ntz", "timestamp_ltz", "timestamp_tz", "binary",
	}

	for _, tt := range types {
		rowType = &execResponseRowType{
			Type: tt,
		}
		err = stringToValue(&dest, *rowType, &source)
		if err == nil {
			t.Errorf("should raise error. type: %v, value:%v", tt, source)
		}
	}

	sources := []string{
		"12345K78 2020",
		"12345678 20T0",
	}

	types = []string{
		"timestamp_tz",
	}

	for _, ss := range sources {
		for _, tt := range types {
			rowType = &execResponseRowType{
				Type: tt,
			}
			err = stringToValue(&dest, *rowType, &ss)
			if err == nil {
				t.Errorf("should raise error. type: %v, value:%v", tt, source)
			}
		}

	}
}
