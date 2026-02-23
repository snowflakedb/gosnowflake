package gosnowflake

import (
	"database/sql/driver"
	"fmt"
	"github.com/snowflakedb/gosnowflake/v2/internal/types"
	"testing"
)

func TestDataTypeMode(t *testing.T) {
	var testcases = []struct {
		tp    driver.Value
		tmode types.SnowflakeType
		err   error
	}{
		{tp: DataTypeTimestampLtz, tmode: types.TimestampLtzType, err: nil},
		{tp: DataTypeTimestampNtz, tmode: types.TimestampNtzType, err: nil},
		{tp: DataTypeTimestampTz, tmode: types.TimestampTzType, err: nil},
		{tp: DataTypeDate, tmode: types.DateType, err: nil},
		{tp: DataTypeTime, tmode: types.TimeType, err: nil},
		{tp: DataTypeBinary, tmode: types.BinaryType, err: nil},
		{tp: DataTypeObject, tmode: types.ObjectType, err: nil},
		{tp: DataTypeArray, tmode: types.ArrayType, err: nil},
		{tp: DataTypeVariant, tmode: types.VariantType, err: nil},
		{tp: DataTypeFixed, tmode: types.FixedType,
			err: fmt.Errorf(errMsgInvalidByteArray, DataTypeFixed)},
		{tp: DataTypeReal, tmode: types.RealType,
			err: fmt.Errorf(errMsgInvalidByteArray, DataTypeFixed)},
		{tp: 123, tmode: types.NullType,
			err: fmt.Errorf(errMsgInvalidByteArray, 123)},
	}
	for _, ts := range testcases {
		t.Run(fmt.Sprintf("%v_%v", ts.tp, ts.tmode), func(t *testing.T) {
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
		})
	}
}

func TestPopulateSnowflakeParameter(t *testing.T) {
	columns := []string{"key", "value", "default", "level", "description", "set_by_user", "set_in_job", "set_on", "set_by_thread_id", "set_by_thread_name", "set_by_class", "parameter_comment", "type", "is_expired", "expires_at", "set_by_controlling_parameter", "activate_version", "partial_rollout"}
	p := SnowflakeParameter{}
	cols := make([]interface{}, len(columns))
	for i := 0; i < len(columns); i++ {
		cols[i] = populateSnowflakeParameter(columns[i], &p)
	}
	for i := 0; i < len(cols); i++ {
		if cols[i] == nil {
			t.Fatal("failed to populate parameter")
		}
	}
}
