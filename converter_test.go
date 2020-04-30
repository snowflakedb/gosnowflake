// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"database/sql/driver"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"math/cmplx"
	"reflect"
	"strings"
	"testing"
	"time"
)

type tcGoTypeToSnowflake struct {
	in    interface{}
	tmode string
	out   string
}

func TestGoTypeToSnowflake(t *testing.T) {
	testcases := []tcGoTypeToSnowflake{
		{in: int64(123), tmode: "", out: "FIXED"},
		{in: float64(234.56), tmode: "", out: "REAL"},
		{in: true, tmode: "", out: "BOOLEAN"},
		{in: "teststring", tmode: "", out: "TEXT"},
		{in: nil, tmode: "", out: "TEXT"}, // nil is taken as TEXT
		{in: DataTypeBinary, tmode: "", out: "CHANGE_TYPE"},
		{in: DataTypeTimestampLtz, tmode: "", out: "CHANGE_TYPE"},
		{in: DataTypeTimestampNtz, tmode: "", out: "CHANGE_TYPE"},
		{in: DataTypeTimestampTz, tmode: "", out: "CHANGE_TYPE"},
		{in: time.Now(), tmode: "TIMESTAMP_NTZ", out: "TIMESTAMP_NTZ"},
		{in: time.Now(), tmode: "TIMESTAMP_TZ", out: "TIMESTAMP_TZ"},
		{in: time.Now(), tmode: "TIMESTAMP_LTZ", out: "TIMESTAMP_LTZ"},
		{in: []byte{1, 2, 3}, tmode: "BINARY", out: "BINARY"},
		// negative
		{in: 123, tmode: "", out: "TEXT"},
		{in: int8(12), tmode: "", out: "TEXT"},
		{in: int32(456), tmode: "", out: "TEXT"},
		{in: uint(456), tmode: "", out: "TEXT"},
		{in: uint8(12), tmode: "", out: "TEXT"},
		{in: uint64(456), tmode: "", out: "TEXT"},
		{in: []byte{100}, tmode: "", out: "TEXT"},
	}
	for _, test := range testcases {
		a := goTypeToSnowflake(test.in, test.tmode)
		if a != test.out {
			t.Errorf("failed. in: %v, tmode: %v, expected: %v, got: %v", test.in, test.tmode, test.out, a)
		}
	}
}

type tcSnowflakeTypeToGo struct {
	in    string
	scale int64
	out   reflect.Type
}

func TestSnowflakeTypeToGo(t *testing.T) {
	testcases := []tcSnowflakeTypeToGo{
		{in: "fixed", scale: 0, out: reflect.TypeOf(int64(0))},
		{in: "fixed", scale: 2, out: reflect.TypeOf(float64(0))},
		{in: "real", scale: 0, out: reflect.TypeOf(float64(0))},
		{in: "text", scale: 0, out: reflect.TypeOf("")},
		{in: "date", scale: 0, out: reflect.TypeOf(time.Now())},
		{in: "time", scale: 0, out: reflect.TypeOf(time.Now())},
		{in: "timestamp_ltz", scale: 0, out: reflect.TypeOf(time.Now())},
		{in: "timestamp_ntz", scale: 0, out: reflect.TypeOf(time.Now())},
		{in: "timestamp_tz", scale: 0, out: reflect.TypeOf(time.Now())},
		{in: "object", scale: 0, out: reflect.TypeOf("")},
		{in: "variant", scale: 0, out: reflect.TypeOf("")},
		{in: "array", scale: 0, out: reflect.TypeOf("")},
		{in: "binary", scale: 0, out: reflect.TypeOf([]byte{})},
		{in: "boolean", scale: 0, out: reflect.TypeOf(true)},
	}
	for _, test := range testcases {
		a := snowflakeTypeToGo(test.in, test.scale)
		if a != test.out {
			t.Errorf("failed. in: %v, scale: %v, expected: %v, got: %v",
				test.in, test.scale, test.out, a)
		}
	}
}

func TestValueToString(t *testing.T) {
	v := cmplx.Sqrt(-5 + 12i) // should never happen as Go sql package must have already validated.
	_, err := valueToString(v, "")
	if err == nil {
		t.Errorf("should raise error: %v", v)
	}

	// both localTime and utcTime should yield the same unix timestamp
	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))
	utcTime := time.Date(2019, 2, 6, 22, 17, 31, 123456789, time.UTC)
	expectedUnixTime := "1549491451123456789" // time.Unix(1549491451, 123456789).Format(time.RFC3339) == "2019-02-06T14:17:31-08:00"

	if s, err := valueToString(localTime, "TIMESTAMP_LTZ"); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedUnixTime, s)
	} else if *s != expectedUnixTime {
		t.Errorf("expected '%v', got '%v'", expectedUnixTime, *s)
	}

	if s, err := valueToString(utcTime, "TIMESTAMP_LTZ"); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedUnixTime, s)
	} else if *s != expectedUnixTime {
		t.Errorf("expected '%v', got '%v'", expectedUnixTime, *s)
	}
}

func TestExtractTimestamp(t *testing.T) {
	s := "1234abcdef"
	_, _, err := extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
	s = "1234abc.def"
	_, _, err = extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
	s = "1234.def"
	_, _, err = extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
}

func TestStringToValue(t *testing.T) {
	var source string
	var dest driver.Value
	var err error
	var rowType *execResponseRowType
	source = "abcdefg"

	types := []string{
		"date", "time", "timestamp_ntz", "timestamp_ltz", "timestamp_tz", "binary",
	}

	for _, tt := range types {
		rowType = &execResponseRowType{
			Type: tt,
		}
		err = stringToValue(&dest, *rowType, &source)
		if err == nil {
			t.Errorf("should raise error. type: %v, value:%v", tt, source)
		}
	}

	sources := []string{
		"12345K78 2020",
		"12345678 20T0",
	}

	types = []string{
		"timestamp_tz",
	}

	for _, ss := range sources {
		for _, tt := range types {
			rowType = &execResponseRowType{
				Type: tt,
			}
			err = stringToValue(&dest, *rowType, &ss)
			if err == nil {
				t.Errorf("should raise error. type: %v, value:%v", tt, source)
			}
		}
	}

	src := "1549491451.123456789"
	if err = stringToValue(&dest, execResponseRowType{Type: "timestamp_ltz"}, &src); err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if ts, ok := dest.(time.Time); !ok {
		t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
	} else if ts.UnixNano() != 1549491451123456789 {
		t.Errorf("expected unix timestamp: 1549491451123456789, got %v", ts.UnixNano())
	}
}

func nanosSinceMidnight(t time.Time) int32 {
	return int32(t.Hour())*3600*1e9 + int32(t.Minute())*60*1e9 + int32(t.Second())*1e9 + int32(t.Nanosecond())
}

func TestArrowToValue(t *testing.T) {
	dest := make([]snowflakeValue, 2)

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	var valids []bool // AppendValues() with an empty valid array adds every value by default

	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))
	//utcTime := time.Date(2019, 2, 6, 22, 17, 31, 123456789, time.UTC)

	type testObj struct {
		field1 int
		field2 string
	}

	for _, tc := range []struct {
		logical  string
		physical string
		values   interface{}
		builder  array.Builder
		append   func(b array.Builder, vs interface{})
		compare  func(src interface{}, dst []snowflakeValue) int
	}{
		{
			logical:  "fixed",
			physical: "number",
			values:   []int32{1, 2},
			builder:  array.NewInt32Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int32Builder).AppendValues(vs.([]int32), valids) },
		},
		{
			logical:  "fixed",
			physical: "integer",
			values:   []int64{1, 2},
			builder:  array.NewInt64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
		},
		{
			logical:  "fixed",
			physical: "number(38,0)",
			values:   []uint64{1, 2},
			builder:  array.NewUint64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Uint64Builder).AppendValues(vs.([]uint64), valids) },
		},
		{
			logical:  "fixed",
			physical: "number(38,37)",
			values:   []int16{1, 2},
			builder:  array.NewInt16Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int16Builder).AppendValues(vs.([]int16), valids) },
		},
		{
			logical:  "fixed",
			physical: "number(3,0)",
			values:   []int64{1, 2},
			builder:  array.NewInt64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
		},
		{
			logical: "boolean",
			values:  []bool{true, false},
			builder: array.NewBooleanBuilder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.BooleanBuilder).AppendValues(vs.([]bool), valids) },
		},
		{
			logical:  "real",
			physical: "float",
			values:   []float64{1, 2},
			builder:  array.NewFloat64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Float64Builder).AppendValues(vs.([]float64), valids) },
		},
		{
			logical:  "text",
			physical: "string",
			values:   []string{"foo", "bar"},
			builder:  array.NewStringBuilder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.StringBuilder).AppendValues(vs.([]string), valids) },
		},
		{
			logical: "binary",
			values:  [][]byte{[]byte("foo"), []byte("bar")},
			builder: array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary),
			append:  func(b array.Builder, vs interface{}) { b.(*array.BinaryBuilder).AppendValues(vs.([][]byte), valids) },
		},
		{
			logical: "date",
			values:  []int32{1, 2},
			builder: array.NewDate32Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, d := range vs.([]int32) {
					b.(*array.Date32Builder).Append(arrow.Date32(d))
				}
			},
		},
		{
			logical: "time",
			values:  []time.Time{time.Now(), time.Now()},
			builder: array.NewInt32Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int32Builder).Append(nanosSinceMidnight(t))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i, _ := range srcvs {
					if nanosSinceMidnight(srcvs[i]) != dst[i].(int32) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ntz",
			values:  []time.Time{time.Now(), localTime},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i, _ := range srcvs {
					if srcvs[i].Unix() != dst[i].(time.Time).Unix() {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ltz",
			values:  []time.Time{time.Now(), localTime},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i, _ := range srcvs {
					if srcvs[i].Unix() != dst[i].(time.Time).Unix() {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_tz",
			values:  []time.Time{time.Now(), localTime},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i, _ := range srcvs {
					return i
					//if nanosSinceMidnight(srcvs[i]) != dst[i].([]uint8) {
					//	return i
					//}
				}
				return -1
			},
		},
		{
			logical: "array",
			values:  [][]string{{"foo", "bar"}, {"baz", "quz", "quux"}},
			builder: array.NewStringBuilder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, a := range vs.([][]string) {
					b.(*array.StringBuilder).Append(fmt.Sprint(a))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([][]string)
				for i, o := range srcvs {
					if fmt.Sprint(o) != dst[i].(string) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "object",
			values:  []testObj{{0, "foo"}, {1, "bar"}},
			builder: array.NewStringBuilder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, o := range vs.([]testObj) {
					b.(*array.StringBuilder).Append(fmt.Sprint(o))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]testObj)
				for i, o := range srcvs {
					if fmt.Sprint(o) != dst[i].(string) {
						return i
					}
				}
				return -1
			},
		},
	} {
		testName := tc.logical
		if tc.physical != "" {
			testName += " " + tc.physical
		}
		t.Run(testName, func(t *testing.T) {
			b := tc.builder
			tc.append(b, tc.values)
			arr := b.NewArray()
			defer arr.Release()

			meta := execResponseRowType{Type: strings.ToUpper(tc.logical)}
			if tc.physical == "fixed" && tc.logical == "integer" {
				meta.Scale = 0
			}

			err := arrowToValue(&dest, meta, arr)
			if err != nil {
				t.Fatalf("error: %s", err)
			}
			elemType := reflect.TypeOf(tc.values).Elem()
			if tc.compare != nil {
				idx := tc.compare(tc.values, dest)
				if idx != -1 {
					t.Fatalf("error: column array value mistmatch at index %v", idx)
				}
			} else {
				for _, d := range dest {
					if reflect.TypeOf(d) != elemType {
						t.Fatalf("error: expected type %s, got type %s", reflect.TypeOf(d), elemType)
					}
				}
			}
		})
	}
}
