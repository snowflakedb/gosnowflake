// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
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

// valueToString converts arbitrary golang type to a string. This is mainly used in binding data with placeholders
// in queries.
func valueToString(v interface{}) (*string, error) {
	glog.V(2).Infof("TYPE: %v, %v", reflect.TypeOf(v), reflect.ValueOf(v))
	if v == nil {
		return nil, nil
	}
	v1 := reflect.ValueOf(v)
	switch v1.Kind() {
	case reflect.Bool:
		s := strconv.FormatBool(v1.Bool())
		return &s, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s := strconv.FormatInt(v1.Int(), 10)
		return &s, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s := strconv.FormatUint(v1.Uint(), 10)
		return &s, nil
	case reflect.Float32, reflect.Float64:
		s := strconv.FormatFloat(v1.Float(), 'g', -1, 32)
		return &s, nil
	case reflect.String:
		s := v1.String()
		return &s, nil
	case reflect.Slice, reflect.Map, reflect.Struct:
		if v1.IsNil() {
			return nil, nil
		}
		s := v1.String()
		return &s, nil
	}
	return nil, fmt.Errorf("Unexpected type is given: %v", v1.Kind())
}

// stringToValue converts a pointer of string data to an arbitrary golang variable. This is mainly used in fetching
// data.
func stringToValue(dest *driver.Value, srcColumnMeta execResponseRowType, srcValue *string) error {
	// glog.V(2).Infof("DATA TYPE: %s, VALUE: % s", srcColumnMeta.Type, srcValue)
	if srcValue == nil {
		dest = nil
		return nil
	}
	switch srcColumnMeta.Type {
	case "text":
		*dest = *srcValue
		return nil
	case "date":
		v, err := strconv.ParseInt(*srcValue, 10, 64)
		if err != nil {
			return err
		}
		*dest = time.Unix(v*86400, 0).UTC()
		return nil
	case "time":
		var i int
		var sec, nsec int64
		var err error
		glog.V(2).Infof("SRC: %v", srcValue)
		for i = 0; i < len(*srcValue); i++ {
			if (*srcValue)[i] == '.' {
				sec, err = strconv.ParseInt((*srcValue)[0:i], 10, 64)
				if err != nil {
					return err
				}
				break
			}
		}
		if i == len(*srcValue) {
			// no fraction
			sec, err = strconv.ParseInt(*srcValue, 10, 64)
			if err != nil {
				return err
			}
			nsec = 0
		} else {
			s := (*srcValue)[i+1:]
			nsec, err = strconv.ParseInt(s+strings.Repeat("0", 9-len(s)), 10, 64)
			if err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
		glog.V(2).Infof("SEC: %v, NSEC: %v", sec, nsec)
		t0 := time.Time{}
		*dest = t0.Add(time.Duration(sec*1e9 + nsec))
		return nil
	case "timestamp_ntz":
		var i int
		var sec, nsec int64
		var err error
		glog.V(2).Infof("SRC: %v", srcValue)
		for i = 0; i < len(*srcValue); i++ {
			if (*srcValue)[i] == '.' {
				sec, err = strconv.ParseInt((*srcValue)[0:i], 10, 64)
				if err != nil {
					return err
				}
				break
			}
		}
		if i == len(*srcValue) {
			// no fraction
			sec, err = strconv.ParseInt(*srcValue, 10, 64)
			if err != nil {
				return err
			}
			nsec = 0
		} else {
			s := (*srcValue)[i+1:]
			nsec, err = strconv.ParseInt(s+strings.Repeat("0", 9-len(s)), 10, 64)
			if err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
		glog.V(2).Infof("SEC: %v, NSEC: %v", sec, nsec)
		*dest = time.Unix(sec, nsec).UTC()
		return nil
	case "timestamp_ltz":
	case "timestamp_tz":
		// TODO: implement
		return nil
	default:
		*dest = *srcValue
		return nil
	}
	*dest = *srcValue
	return nil
}
