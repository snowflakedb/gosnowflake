// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//

package gosnowflake

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
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

func stringToValue(srcColumnMeta ExecResponseRowType, srcValue string) (interface{}, error) {
	log.Printf("DATA TYPE: %s", srcColumnMeta.Type)
	switch srcColumnMeta.Type {
	case "date":
		v, err := strconv.ParseInt(srcValue, 10, 64)
		if err != nil {
			return nil, err
		}
		return time.Unix(v*86400, 0).UTC(), nil
	case "time":
		var i int
		var sec, nsec int64
		var err error
		log.Printf("SRC: %s", srcValue)
		for i = 0; i < len(srcValue); i++ {
			if srcValue[i] == '.' {
				sec, err = strconv.ParseInt(srcValue[0:i], 10, 64)
				if err != nil {
					return nil, err
				}
				break
			}
		}
		if i == len(srcValue) {
			// no fraction
			sec, err = strconv.ParseInt(srcValue, 10, 64)
			if err != nil {
				return nil, err
			}
			nsec = 0
		} else {
			s := srcValue[i+1:]
			nsec, err = strconv.ParseInt(s+strings.Repeat("0", 9-len(s)), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		log.Printf("SEC: %s, NSEC: %s", sec, nsec)
		t0 := time.Time{}
		return t0.Add(time.Duration(sec * 1e9 + nsec)), nil
	case "timestamp_ntz":
		var i int
		var sec, nsec int64
		var err error
		log.Printf("SRC: %s", srcValue)
		for i = 0; i < len(srcValue); i++ {
			if srcValue[i] == '.' {
				sec, err = strconv.ParseInt(srcValue[0:i], 10, 64)
				if err != nil {
					return nil, err
				}
				break
			}
		}
		if i == len(srcValue) {
			// no fraction
			sec, err = strconv.ParseInt(srcValue, 10, 64)
			if err != nil {
				return nil, err
			}
			nsec = 0
		} else {
			s := srcValue[i+1:]
			nsec, err = strconv.ParseInt(s+strings.Repeat("0", 9-len(s)), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		log.Printf("SEC: %s, NSEC: %s", sec, nsec)
		return time.Unix(sec, nsec).UTC(),nil
	case "timestamp_ltz":
	case "timestamp_tz":
	default:
		return srcValue, nil
	}
	return srcValue, nil
}
