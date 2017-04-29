// Package sfutil is a utility package for Go Snowflake Driver
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package sfutil

// DataTypeIdx indicates Snowflake Datatype index.
type DataTypeIdx int

const (
	// Fixed is a FIXED datatype.
	Fixed DataTypeIdx = iota
	// Real is a REAL datatype.
	Real
	// Text is a TEXT datatype.
	Text
	// Date is a TEXT datatype.
	Date
	// Timestamp is a TEXT datatype.
	Timestamp
	// Variant is a TEXT datatype.
	Variant
	// TimestampLtz is a TIMESTAMP_LTZ datatype.
	TimestampLtz
	// TimestampNtz is a TIMESTAMP_NTZ datatype.
	TimestampNtz
	// TimestampTz is a TIMESTAMP_TZ datatype.
	TimestampTz
	// Object is a OBJECT datatype.
	Object
	// Array is a ARRAY datatype.
	Array
	// Binary is a BINARY datatype.
	Binary
	// Time is a TIME datatype.
	Time
	// Boolean is a BOOLEAN datatype.
	Boolean
	lastDummyType
)

var idxToName = []string{
	"FIXED",
	"REAL",
	"TEXT",
	"DATE",
	"TIMESTAMP",
	"VARIANT",
	"TIMESTAMP_LTZ",
	"TIMESTAMP_NTZ",
	"TIMESTAMP_TZ",
	"OBJECT",
	"ARRAY",
	"BINARY",
	"TIME",
	"BOOLEAN",
}

// DataType is Snowflake DataType that wraps an actual value. This is mainly used when
// binding Timestamp data
type DataType struct {
	idx   DataTypeIdx
	value interface{}
}

// NewDataType creates an instance of DataType and returns the pointer. If an invalid
// idx is specified, nil is returned
func NewDataType(idx DataTypeIdx, value interface{}) *DataType {
	if idx < 0 || idx >= lastDummyType {
		return nil
	}
	return &DataType{
		idx:   idx,
		value: value,
	}
}

// Value returns the value wrapped by DataType object.
func (sdt DataType) Value() interface{} {
	return sdt.value
}

// String returns a string representation of DataType.
func (sdt DataType) String() string {
	return idxToName[sdt.idx]
}
