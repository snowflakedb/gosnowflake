// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
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
