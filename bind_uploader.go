// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"database/sql/driver"
	"reflect"
)

func supportedArrayBind(nv *driver.NamedValue) bool {
	switch reflect.TypeOf(nv.Value) {
	case reflect.TypeOf(&intArray{}), reflect.TypeOf(&int32Array{}),
		reflect.TypeOf(&int64Array{}), reflect.TypeOf(&float64Array{}),
		reflect.TypeOf(&float32Array{}), reflect.TypeOf(&boolArray{}),
		reflect.TypeOf(&stringArray{}), reflect.TypeOf(&byteArray{}):
		return true
	default:
		// TODO SNOW-292862 date, timestamp, time
		// TODO SNOW-176486 variant, object, array
		return false
	}
}
