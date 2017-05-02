// Package gosnowflake is a utility package for Go Snowflake Driver
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"fmt"
	"testing"
)

type tc struct {
	tp    []byte
	tmode string
	err   error
}

func TestDataTypeMode(t *testing.T) {
	var testcases = []tc{
		{tp: DataTypeTimestampLtz, tmode: "TIMESTAMP_LTZ", err: nil},
		{tp: DataTypeTimestampNtz, tmode: "TIMESTAMP_NTZ", err: nil},
		{tp: DataTypeTimestampTz, tmode: "TIMESTAMP_TZ", err: nil},
		{tp: DataTypeDate, tmode: "DATE", err: nil},
		{tp: DataTypeTime, tmode: "TIME", err: nil},
		{tp: DataTypeBinary, tmode: "BINARY", err: nil},
		{tp: DataTypeFixed, tmode: "FIXED",
			err: fmt.Errorf(errMsgInvalidByteArray, DataTypeFixed)},
		{tp: DataTypeReal, tmode: "REAL",
			err: fmt.Errorf(errMsgInvalidByteArray, DataTypeFixed)},
	}
	for _, ts := range testcases {
		tmode, err := DataTypeMode(ts.tp)
		if ts.err == nil {
			if err != nil {
				t.Errorf("failed to get datatype mode: %v", err)
			}
			if tmode != ts.tmode {
				t.Errorf("wrong data type: %v", tmode)
			}
		} else {
			if err == nil {
				t.Errorf("should raise an error: %v", ts.err)
			}
		}
	}
}
