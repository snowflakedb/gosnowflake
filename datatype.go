// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"database/sql"
	"fmt"
)

type snowflakeType int

const (
	fixedType snowflakeType = iota
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
	nullType
	// the following are not snowflake types per se but internal types
	sliceType
	changeType
	unSupportedType
)

var snowflakeTypes = [...]string{"FIXED", "REAL", "TEXT", "DATE", "VARIANT",
	"TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "OBJECT", "ARRAY",
	"BINARY", "TIME", "BOOLEAN", "NULL", "SLICE", "CHANGE_TYPE", "NOT_SUPPORTED"}

func (st snowflakeType) String() string {
	return snowflakeTypes[st]
}

func (st snowflakeType) Byte() byte {
	return byte(st)
}

func getSnowflakeType(typ string) snowflakeType {
	for i, sft := range snowflakeTypes {
		if sft == typ {
			return snowflakeType(i)
		} else if snowflakeType(i) == nullType {
			break
		}
	}
	return nullType
}

// SnowflakeDataType is the type used by clients to explicitly indicate the type
// of an argument to ExecContext and friends. We use a separate public-facing
// type rather than a Go primitive type so that we can always differentiate
// between args that indicate type and args that are values.
type SnowflakeDataType []byte

// Equals checks if dt and o represent the same type indicator
func (dt SnowflakeDataType) Equals(o SnowflakeDataType) bool {
	return bytes.Equal(([]byte)(dt), ([]byte)(o))
}

var (
	// DataTypeFixed is a FIXED datatype.
	DataTypeFixed = SnowflakeDataType{fixedType.Byte()}
	// DataTypeReal is a REAL datatype.
	DataTypeReal = SnowflakeDataType{realType.Byte()}
	// DataTypeText is a TEXT datatype.
	DataTypeText = SnowflakeDataType{textType.Byte()}
	// DataTypeDate is a Date datatype.
	DataTypeDate = SnowflakeDataType{dateType.Byte()}
	// DataTypeVariant is a TEXT datatype.
	DataTypeVariant = SnowflakeDataType{variantType.Byte()}
	// DataTypeTimestampLtz is a TIMESTAMP_LTZ datatype.
	DataTypeTimestampLtz = SnowflakeDataType{timestampLtzType.Byte()}
	// DataTypeTimestampNtz is a TIMESTAMP_NTZ datatype.
	DataTypeTimestampNtz = SnowflakeDataType{timestampNtzType.Byte()}
	// DataTypeTimestampTz is a TIMESTAMP_TZ datatype.
	DataTypeTimestampTz = SnowflakeDataType{timestampTzType.Byte()}
	// DataTypeObject is a OBJECT datatype.
	DataTypeObject = SnowflakeDataType{objectType.Byte()}
	// DataTypeArray is a ARRAY datatype.
	DataTypeArray = SnowflakeDataType{arrayType.Byte()}
	// DataTypeBinary is a BINARY datatype.
	DataTypeBinary = SnowflakeDataType{binaryType.Byte()}
	// DataTypeTime is a TIME datatype.
	DataTypeTime = SnowflakeDataType{timeType.Byte()}
	// DataTypeBoolean is a BOOLEAN datatype.
	DataTypeBoolean = SnowflakeDataType{booleanType.Byte()}
	// DataTypeNull is a NULL datatype.
	DataTypeNull = SnowflakeDataType{nullType.Byte()}
)

func clientTypeToInternal(cType SnowflakeDataType) (iType snowflakeType, err error) {
	if cType != nil {
		switch {
		case cType.Equals(DataTypeFixed):
			iType = fixedType
		case cType.Equals(DataTypeReal):
			iType = realType
		case cType.Equals(DataTypeText):
			iType = textType
		case cType.Equals(DataTypeDate):
			iType = dateType
		case cType.Equals(DataTypeVariant):
			iType = variantType
		case cType.Equals(DataTypeTimestampLtz):
			iType = timestampLtzType
		case cType.Equals(DataTypeTimestampNtz):
			iType = timestampNtzType
		case cType.Equals(DataTypeTimestampTz):
			iType = timestampTzType
		case cType.Equals(DataTypeObject):
			iType = objectType
		case cType.Equals(DataTypeArray):
			iType = arrayType
		case cType.Equals(DataTypeBinary):
			iType = binaryType
		case cType.Equals(DataTypeTime):
			iType = timeType
		case cType.Equals(DataTypeBoolean):
			iType = booleanType
		case cType.Equals(DataTypeNull):
			iType = nullType
		default:
			return nullType, fmt.Errorf(errMsgInvalidByteArray, ([]byte)(cType))
		}
	} else {
		return nullType, fmt.Errorf(errMsgInvalidByteArray, nil)
	}
	return iType, nil
}

// SnowflakeParameter includes the columns output from SHOW PARAMETER command.
type SnowflakeParameter struct {
	Key                       string
	Value                     string
	Default                   string
	Level                     string
	Description               string
	SetByUser                 string
	SetInJob                  string
	SetOn                     string
	SetByThreadID             string
	SetByThreadName           string
	SetByClass                string
	ParameterComment          string
	Type                      string
	IsExpired                 string
	ExpiresAt                 string
	SetByControllingParameter string
	ActivateVersion           string
	PartialRollout            string
	Unknown                   string // Reserve for added parameter
}

func populateSnowflakeParameter(colname string, p *SnowflakeParameter) interface{} {
	switch colname {
	case "key":
		return &p.Key
	case "value":
		return &p.Value
	case "default":
		return &p.Default
	case "level":
		return &p.Level
	case "description":
		return &p.Description
	case "set_by_user":
		return &p.SetByUser
	case "set_in_job":
		return &p.SetInJob
	case "set_on":
		return &p.SetOn
	case "set_by_thread_id":
		return &p.SetByThreadID
	case "set_by_thread_name":
		return &p.SetByThreadName
	case "set_by_class":
		return &p.SetByClass
	case "parameter_comment":
		return &p.ParameterComment
	case "type":
		return &p.Type
	case "is_expired":
		return &p.IsExpired
	case "expires_at":
		return &p.ExpiresAt
	case "set_by_controlling_parameter":
		return &p.SetByControllingParameter
	case "activate_version":
		return &p.ActivateVersion
	case "partial_rollout":
		return &p.PartialRollout
	default:
		debugPanicf("unknown type: %v", colname)
		return &p.Unknown
	}
}

// ScanSnowflakeParameter binds SnowflakeParameter variable with an array of column buffer.
func ScanSnowflakeParameter(rows *sql.Rows) (*SnowflakeParameter, error) {
	var err error
	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return nil, err
	}
	colNum := len(columns)
	p := SnowflakeParameter{}
	cols := make([]interface{}, colNum)
	for i := 0; i < colNum; i++ {
		cols[i] = populateSnowflakeParameter(columns[i], &p)
	}
	err = rows.Scan(cols...)
	return &p, err
}
