// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//

package gosnowflake

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

func goTypeToSnowflake(v interface{}) string {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "FIXED"
	case bool:
		return "BOOLEAN"
	case float32, float64:
		return "REAL"
	case time.Time:
		// TODO: timestamp support?
		return "DATE"
	case string:
		return "TEXT"
	default:
		return "TEXT"
	}
}

func valueToString(v interface{}) (string, error) {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		if v1, ok := v.(int64); ok {
			return strconv.FormatInt(v1, 10), nil
		} else {
			return "", errors.New(fmt.Sprintf("Failed to convert %s to string", v))
		}
	case uint, uint8, uint16, uint32, uint64:
		if v1, ok := v.(uint64); ok {
			return strconv.FormatUint(v1, 10), nil
		} else {
			return "", errors.New(fmt.Sprintf("Failed to convert %s to string", v))
		}
	case float32:
		if v1, ok := v.(float64); ok {
			return strconv.FormatFloat(v1, 'g', -1, 32), nil
		} else {
			return "", errors.New(fmt.Sprintf("Failed to convert %s to string", v))
		}
	case float64:
		if v1, ok := v.(float64); ok {
			return strconv.FormatFloat(v1, 'g', -1, 64), nil
		} else {
			return "", errors.New(fmt.Sprintf("Failed to convert %s to string", v))
		}
	case time.Time:
		// TODO: convert time to string
		return "", nil
	case bool:
		if v1, ok := v.(bool); ok {
			return strconv.FormatBool(v1), nil
		} else {
			return "", errors.New(fmt.Sprintf("Failed to convert %s to string", v))
		}
	case string:
		if v1, ok := v.(string); ok {
			return v1, nil
		} else {
			return "", errors.New(fmt.Sprintf("Failed to convert %s to string", v))
		}
	default:
		return "0", nil
	}
}
