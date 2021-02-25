// Copyright (c) 2017-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"database/sql/driver"
	"fmt"
	"testing"
)

type tcDataTypeMode struct {
	tp    driver.Value
	tmode snowflakeType
	err   error
}

func TestDataTypeMode(t *testing.T) {
	var testcases = []tcDataTypeMode{
		{tp: DataTypeTimestampLtz, tmode: timestampLtzType, err: nil},
		{tp: DataTypeTimestampNtz, tmode: timestampNtzType, err: nil},
		{tp: DataTypeTimestampTz, tmode: timestampTzType, err: nil},
		{tp: DataTypeDate, tmode: dateType, err: nil},
		{tp: DataTypeTime, tmode: timeType, err: nil},
		{tp: DataTypeBinary, tmode: binaryType, err: nil},
		{tp: DataTypeFixed, tmode: fixedType,
			err: fmt.Errorf(errMsgInvalidByteArray, DataTypeFixed)},
		{tp: DataTypeReal, tmode: realType,
			err: fmt.Errorf(errMsgInvalidByteArray, DataTypeFixed)},
		{tp: 123, tmode: nullType,
			err: fmt.Errorf(errMsgInvalidByteArray, 123)},
	}
	for _, ts := range testcases {
		tmode, err := dataTypeMode(ts.tp)
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
