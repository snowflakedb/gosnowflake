// Copyright (c) 2017-2018 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"database/sql/driver"
	"fmt"
)

const (
	fixedType byte = iota
	realType
	textType
	dateType
	variantType
	timestampLtzType
	timestampNtzType
	timestampTzType
	objectType
	arrayType
	binaryType
	timeType
	booleanType
)

var (
	// DataTypeFixed is a FIXED datatype.
	DataTypeFixed = []byte{fixedType}
	// DataTypeReal is a REAL datatype.
	DataTypeReal = []byte{realType}
	// DataTypeText is a TEXT datatype.
	DataTypeText = []byte{textType}
	// DataTypeDate is a Date datatype.
	DataTypeDate = []byte{dateType}
	// DataTypeVariant is a TEXT datatype.
	DataTypeVariant = []byte{variantType}
	// DataTypeTimestampLtz is a TIMESTAMP_LTZ datatype.
	DataTypeTimestampLtz = []byte{timestampLtzType}
	// DataTypeTimestampNtz is a TIMESTAMP_NTZ datatype.
	DataTypeTimestampNtz = []byte{timestampNtzType}
	// DataTypeTimestampTz is a TIMESTAMP_TZ datatype.
	DataTypeTimestampTz = []byte{timestampTzType}
	// DataTypeObject is a OBJECT datatype.
	DataTypeObject = []byte{objectType}
	// DataTypeArray is a ARRAY datatype.
	DataTypeArray = []byte{arrayType}
	// DataTypeBinary is a BINARY datatype.
	DataTypeBinary = []byte{binaryType}
	// DataTypeTime is a TIME datatype.
	DataTypeTime = []byte{timeType}
	// DataTypeBoolean is a BOOLEAN datatype.
	DataTypeBoolean = []byte{booleanType}
)

// dataTypeMode returns the subsequent data type in a string representation.
func dataTypeMode(v driver.Value) (tsmode string, err error) {
	if bd, ok := v.([]byte); ok {
		switch {
		case bytes.Equal(bd, DataTypeDate):
			tsmode = "DATE"
		case bytes.Equal(bd, DataTypeTime):
			tsmode = "TIME"
		case bytes.Equal(bd, DataTypeTimestampLtz):
			tsmode = "TIMESTAMP_LTZ"
		case bytes.Equal(bd, DataTypeTimestampNtz):
			tsmode = "TIMESTAMP_NTZ"
		case bytes.Equal(bd, DataTypeTimestampTz):
			tsmode = "TIMESTAMP_TZ"
		case bytes.Equal(bd, DataTypeBinary):
			tsmode = "BINARY"
		default:
			return "", fmt.Errorf(errMsgInvalidByteArray, v)
		}
	} else {
		return "", fmt.Errorf(errMsgInvalidByteArray, v)
	}
	return tsmode, nil
}
