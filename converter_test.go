// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"math/big"
	"math/cmplx"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

func stringIntToDecimal(src string) (decimal128.Num, bool) {
	b, ok := new(big.Int).SetString(src, 10)
	if !ok {
		return decimal128.Num{}, ok
	}
	var high, low big.Int
	high.QuoRem(b, decimalShift, &low)
	return decimal128.New(high.Int64(), low.Uint64()), ok
}

func stringFloatToDecimal(src string, scale int64) (decimal128.Num, bool) {
	b, ok := new(big.Float).SetString(src)
	if !ok {
		return decimal128.Num{}, ok
	}
	s := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
	n := new(big.Float).Mul(b, s)
	if !n.IsInt() {
		return decimal128.Num{}, false
	}
	var high, low, z big.Int
	n.Int(&z)
	high.QuoRem(&z, decimalShift, &low)
	return decimal128.New(high.Int64(), low.Uint64()), ok
}

type tcGoTypeToSnowflake struct {
	in    interface{}
	tmode SnowflakeDataType
	out   snowflakeType
}

func TestGoTypeToSnowflake(t *testing.T) {
	testcases := []tcGoTypeToSnowflake{
		{in: int64(123), tmode: nil, out: fixedType},
		{in: float64(234.56), tmode: nil, out: realType},
		{in: true, tmode: nil, out: booleanType},
		{in: "teststring", tmode: nil, out: textType},
		{in: Array([]int{1}), tmode: nil, out: sliceType},
		{in: time.Now(), tmode: nil, out: timestampNtzType},
		{in: time.Now(), tmode: DataTypeTimestampNtz, out: timestampNtzType},
		{in: time.Now(), tmode: DataTypeTimestampTz, out: timestampTzType},
		{in: time.Now(), tmode: DataTypeTimestampLtz, out: timestampLtzType},
		{in: []byte{1, 2, 3}, tmode: DataTypeBinary, out: binaryType},
		// Every explicit DataType should return changeType
		{in: DataTypeFixed, tmode: nil, out: changeType},
		{in: DataTypeReal, tmode: nil, out: changeType},
		{in: DataTypeText, tmode: nil, out: changeType},
		{in: DataTypeDate, tmode: nil, out: changeType},
		{in: DataTypeVariant, tmode: nil, out: changeType},
		{in: DataTypeTimestampLtz, tmode: nil, out: changeType},
		{in: DataTypeTimestampNtz, tmode: nil, out: changeType},
		{in: DataTypeTimestampTz, tmode: nil, out: changeType},
		{in: DataTypeObject, tmode: nil, out: changeType},
		{in: DataTypeArray, tmode: nil, out: changeType},
		{in: DataTypeBinary, tmode: nil, out: changeType},
		{in: DataTypeTime, tmode: nil, out: changeType},
		{in: DataTypeBoolean, tmode: nil, out: changeType},
		{in: DataTypeNull, tmode: nil, out: changeType},
		// negative
		{in: 123, tmode: nil, out: unSupportedType},
		{in: int8(12), tmode: nil, out: unSupportedType},
		{in: int32(456), tmode: nil, out: unSupportedType},
		{in: uint(456), tmode: nil, out: unSupportedType},
		{in: uint8(12), tmode: nil, out: unSupportedType},
		{in: uint64(456), tmode: nil, out: unSupportedType},
		{in: []byte{100}, tmode: nil, out: unSupportedType},
		{in: nil, tmode: nil, out: unSupportedType},
		{in: []int{1}, tmode: nil, out: unSupportedType},
	}
	for _, test := range testcases {
		a := goTypeToSnowflake(test.in, test.tmode)
		if a != test.out {
			t.Errorf("failed. in: %v, tmode: %v, expected: %v, got: %v", test.in, test.tmode, test.out, a)
		}
	}
}

type tcSnowflakeTypeToGo struct {
	in    snowflakeType
	scale int64
	out   reflect.Type
}

func TestSnowflakeTypeToGo(t *testing.T) {
	testcases := []tcSnowflakeTypeToGo{
		{in: fixedType, scale: 0, out: reflect.TypeOf(int64(0))},
		{in: fixedType, scale: 2, out: reflect.TypeOf(float64(0))},
		{in: realType, scale: 0, out: reflect.TypeOf(float64(0))},
		{in: textType, scale: 0, out: reflect.TypeOf("")},
		{in: dateType, scale: 0, out: reflect.TypeOf(time.Now())},
		{in: timeType, scale: 0, out: reflect.TypeOf(time.Now())},
		{in: timestampLtzType, scale: 0, out: reflect.TypeOf(time.Now())},
		{in: timestampNtzType, scale: 0, out: reflect.TypeOf(time.Now())},
		{in: timestampTzType, scale: 0, out: reflect.TypeOf(time.Now())},
		{in: objectType, scale: 0, out: reflect.TypeOf("")},
		{in: variantType, scale: 0, out: reflect.TypeOf("")},
		{in: arrayType, scale: 0, out: reflect.TypeOf("")},
		{in: binaryType, scale: 0, out: reflect.TypeOf([]byte{})},
		{in: booleanType, scale: 0, out: reflect.TypeOf(true)},
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
	_, err := valueToString(v, nil)
	if err == nil {
		t.Errorf("should raise error: %v", v)
	}

	// both localTime and utcTime should yield the same unix timestamp
	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))
	utcTime := time.Date(2019, 2, 6, 22, 17, 31, 123456789, time.UTC)
	expectedUnixTime := "1549491451123456789" // time.Unix(1549491451, 123456789).Format(time.RFC3339) == "2019-02-06T14:17:31-08:00"

	if s, err := valueToString(localTime, DataTypeTimestampLtz); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedUnixTime, s)
	} else if *s != expectedUnixTime {
		t.Errorf("expected '%v', got '%v'", expectedUnixTime, *s)
	}

	if s, err := valueToString(utcTime, DataTypeTimestampLtz); err != nil {
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
		if err = stringToValue(&dest, *rowType, &source, nil); err == nil {
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
			if err = stringToValue(&dest, *rowType, &ss, nil); err == nil {
				t.Errorf("should raise error. type: %v, value:%v", tt, source)
			}
		}
	}

	src := "1549491451.123456789"
	if err = stringToValue(&dest, execResponseRowType{Type: "timestamp_ltz"}, &src, nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if ts, ok := dest.(time.Time); !ok {
		t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
	} else if ts.UnixNano() != 1549491451123456789 {
		t.Errorf("expected unix timestamp: 1549491451123456789, got %v", ts.UnixNano())
	}
}

type tcArrayToString struct {
	in  driver.NamedValue
	typ snowflakeType
	out []string
}

func TestArrayToString(t *testing.T) {
	testcases := []tcArrayToString{
		{in: driver.NamedValue{Value: &intArray{1, 2}}, typ: fixedType, out: []string{"1", "2"}},
		{in: driver.NamedValue{Value: &int64Array{3, 4, 5}}, typ: fixedType, out: []string{"3", "4", "5"}},
		{in: driver.NamedValue{Value: &float64Array{6.7}}, typ: realType, out: []string{"6.7"}},
		{in: driver.NamedValue{Value: &boolArray{true, false}}, typ: booleanType, out: []string{"true", "false"}},
		{in: driver.NamedValue{Value: &stringArray{"foo", "bar", "baz"}}, typ: textType, out: []string{"foo", "bar", "baz"}},
	}
	for _, test := range testcases {
		s, a := snowflakeArrayToString(&test.in, false)
		if s != test.typ {
			t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.typ, s)
		}
		for i, v := range a {
			if *v != test.out[i] {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.out[i], a)
			}
		}
	}
}

func TestArrowToValue(t *testing.T) {
	dest := make([]snowflakeValue, 2)

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	var valids []bool // AppendValues() with an empty valid array adds every value by default

	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))

	field1 := arrow.Field{Name: "epoch", Type: &arrow.Int64Type{}}
	field2 := arrow.Field{Name: "timezone", Type: &arrow.Int32Type{}}
	tzStruct := arrow.StructOf(field1, field2)

	type testObj struct {
		field1 int
		field2 string
	}

	for _, tc := range []struct {
		logical  string
		physical string
		rowType  execResponseRowType
		values   interface{}
		builder  array.Builder
		append   func(b array.Builder, vs interface{})
		compare  func(src interface{}, dst []snowflakeValue) int
	}{
		{
			logical:  "fixed",
			physical: "number", // default: number(38, 0)
			values:   []int64{1, 2},
			builder:  array.NewInt64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
		},
		{
			logical:  "fixed",
			physical: "number(38,0)",
			values:   []string{"10000000000000000000000000000000000000", "-12345678901234567890123456789012345678"},
			builder:  array.NewDecimal128Builder(pool, &arrow.Decimal128Type{Precision: 30, Scale: 2}),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringIntToDecimal(s)
					if !ok {
						t.Fatalf("failed to convert to big.Int")
					}
					b.(*array.Decimal128Builder).Append(num)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringIntToDecimal(srcvs[i])
					if !ok {
						return i
					}
					srcDec := decimalToBigInt(num)
					dstDec := dst[i].(*big.Int)
					if srcDec.Cmp(dstDec) != 0 {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "number(38,37)",
			rowType:  execResponseRowType{Scale: 37},
			values:   []string{"1.2345678901234567890123456789012345678", "-9.9999999999999999999999999999999999999"},
			builder:  array.NewDecimal128Builder(pool, &arrow.Decimal128Type{Precision: 38, Scale: 37}),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToDecimal(s, 37)
					if !ok {
						t.Fatalf("failed to convert to big.Rat")
					}
					b.(*array.Decimal128Builder).Append(num)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToDecimal(srcvs[i], 37)
					if !ok {
						return i
					}
					srcDec := decimalToBigFloat(num, 37)
					dstDec := dst[i].(*big.Float)
					if srcDec.Cmp(dstDec) != 0 {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "int8",
			values:   []int8{1, 2},
			builder:  array.NewInt8Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int8Builder).AppendValues(vs.([]int8), valids) },
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]int8)
				for i := range srcvs {
					if int64(srcvs[i]) != dst[i].(int64) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "int16",
			values:   []int16{1, 2},
			builder:  array.NewInt16Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int16Builder).AppendValues(vs.([]int16), valids) },
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]int16)
				for i := range srcvs {
					if int64(srcvs[i]) != dst[i].(int64) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "int32",
			values:   []int32{1, 2},
			builder:  array.NewInt32Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int32Builder).AppendValues(vs.([]int32), valids) },
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]int32)
				for i := range srcvs {
					if int64(srcvs[i]) != dst[i].(int64) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "int64",
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
			values:  []time.Time{time.Now(), localTime},
			builder: array.NewDate32Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, d := range vs.([]time.Time) {
					b.(*array.Date32Builder).Append(arrow.Date32(d.Unix()))
				}
			},
		},
		{
			logical: "time",
			values:  []time.Time{time.Now(), time.Now()},
			rowType: execResponseRowType{Scale: 9},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].Nanosecond() != dst[i].(time.Time).Nanosecond() {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ntz",
			values:  []time.Time{time.Now(), localTime},
			rowType: execResponseRowType{Scale: 9},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].UnixNano() != dst[i].(time.Time).UnixNano() {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ntz",
			values:  []time.Time{time.Now(), localTime},
			rowType: execResponseRowType{Scale: 3},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano() / 1000000)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].UnixNano()/1000000 != dst[i].(time.Time).UnixNano()/1000000 {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ltz",
			values:  []time.Time{time.Now(), localTime},
			rowType: execResponseRowType{Scale: 9},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].UnixNano() != dst[i].(time.Time).UnixNano() {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ltz",
			values:  []time.Time{time.Now(), localTime},
			rowType: execResponseRowType{Scale: 3},
			builder: array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano() / 1000000)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].UnixNano()/1000000 != dst[i].(time.Time).UnixNano()/1000000 {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_tz",
			values:  []time.Time{time.Now(), localTime},
			builder: array.NewStructBuilder(pool, tzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.UnixNano()))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]time.Time)
				for i := range srcvs {
					if srcvs[i].Unix() != dst[i].(time.Time).Unix() {
						return i
					}
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

			meta := tc.rowType
			meta.Type = tc.logical

			if err := arrowToValue(dest, meta, arr, localTime.Location(), true); err != nil {
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

func TestArrowToRecord(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0) // ensure no arrow memory leaks
	var valids []bool           // AppendValues() with an empty valid array adds every value by default

	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))

	field1 := arrow.Field{Name: "epoch", Type: &arrow.Int64Type{}}
	field2 := arrow.Field{Name: "timezone", Type: &arrow.Int32Type{}}
	tzStruct := arrow.StructOf(field1, field2)

	type testObj struct {
		field1 int
		field2 string
	}

	for _, tc := range []struct {
		logical  string
		physical string
		sc       *arrow.Schema
		rowType  execResponseRowType
		values   interface{}
		nrows    int
		builder  array.Builder
		append   func(b array.Builder, vs interface{})
		compare  func(src interface{}, rec arrow.Record) int
	}{
		{
			logical:  "fixed",
			physical: "number", // default: number(38, 0)
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			values:   []int64{1, 2},
			nrows:    2,
			builder:  array.NewInt64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
		},
		{
			logical:  "fixed",
			physical: "number(38,0)",
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Decimal128Type{Precision: 38, Scale: 0}}}, nil),
			values:   []string{"10000000000000000000000000000000000000", "-12345678901234567890123456789012345678"},
			nrows:    2,
			builder:  array.NewDecimal128Builder(pool, &arrow.Decimal128Type{Precision: 38, Scale: 0}),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringIntToDecimal(s)
					if !ok {
						t.Fatalf("failed to convert to Int64")
					}
					b.(*array.Decimal128Builder).Append(num)
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]string)
				for i, dec := range convertedRec.Column(0).(*array.Int64).Int64Values() {
					num, ok := stringIntToDecimal(srcvs[i])
					if !ok {
						return i
					}
					srcDec := decimalToBigInt(num).Int64()
					if srcDec != dec {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "number(38,37)",
			rowType:  execResponseRowType{Scale: 37},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Decimal128Type{Precision: 38, Scale: 37}}}, nil),
			values:   []string{"1.2345678901234567890123456789012345678", "-9.999999999999999"},
			nrows:    2,
			builder:  array.NewDecimal128Builder(pool, &arrow.Decimal128Type{Precision: 38, Scale: 37}),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, err := decimal128.FromString(s, 38, 37)
					if err != nil {
						t.Fatalf("failed to convert to decimal: %s", err)
					}
					b.(*array.Decimal128Builder).Append(num)
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]string)
				for i, dec := range convertedRec.Column(0).(*array.Float64).Float64Values() {
					num, err := decimal128.FromString(srcvs[i], 38, 37)
					if err != nil {
						return i
					}
					srcDec := num.ToFloat64(37)
					if srcDec != dec {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "int8",
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int8Type{}}}, nil),
			values:   []int8{1, 2},
			nrows:    2,
			builder:  array.NewInt8Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int8Builder).AppendValues(vs.([]int8), valids) },
		},
		{
			logical:  "fixed",
			physical: "int16",
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int16Type{}}}, nil),
			values:   []int16{1, 2},
			nrows:    2,
			builder:  array.NewInt16Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int16Builder).AppendValues(vs.([]int16), valids) },
		},
		{
			logical:  "fixed",
			physical: "int32",
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int32Type{}}}, nil),
			values:   []int32{1, 2},
			nrows:    2,
			builder:  array.NewInt32Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int32Builder).AppendValues(vs.([]int32), valids) },
		},
		{
			logical:  "fixed",
			physical: "int64",
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			values:   []int64{1, 2},
			nrows:    2,
			builder:  array.NewInt64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
		},
		{
			logical:  "fixed",
			physical: "float8",
			rowType:  execResponseRowType{Scale: 1},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int8Type{}}}, nil),
			values:   []int8{10, 16},
			nrows:    2,
			builder:  array.NewInt8Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int8Builder).AppendValues(vs.([]int8), valids) },
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]int8)
				for i, f := range convertedRec.Column(0).(*array.Float64).Float64Values() {
					rawFloat, _ := intToBigFloat(int64(srcvs[i]), 1).Float64()
					if rawFloat != f {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "float16",
			rowType:  execResponseRowType{Scale: 1},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int16Type{}}}, nil),
			values:   []int16{20, 26},
			nrows:    2,
			builder:  array.NewInt16Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int16Builder).AppendValues(vs.([]int16), valids) },
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]int16)
				for i, f := range convertedRec.Column(0).(*array.Float64).Float64Values() {
					rawFloat, _ := intToBigFloat(int64(srcvs[i]), 1).Float64()
					if rawFloat != f {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "float32",
			rowType:  execResponseRowType{Scale: 2},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int32Type{}}}, nil),
			values:   []int32{200, 265},
			nrows:    2,
			builder:  array.NewInt32Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int32Builder).AppendValues(vs.([]int32), valids) },
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]int32)
				for i, f := range convertedRec.Column(0).(*array.Float64).Float64Values() {
					rawFloat, _ := intToBigFloat(int64(srcvs[i]), 2).Float64()
					if rawFloat != f {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "float64",
			rowType:  execResponseRowType{Scale: 5},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			values:   []int64{12345, 234567},
			nrows:    2,
			builder:  array.NewInt64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]int64)
				for i, f := range convertedRec.Column(0).(*array.Float64).Float64Values() {
					rawFloat, _ := intToBigFloat(srcvs[i], 5).Float64()
					if rawFloat != f {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "boolean",
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.BooleanType{}}}, nil),
			values:  []bool{true, false},
			nrows:   2,
			builder: array.NewBooleanBuilder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.BooleanBuilder).AppendValues(vs.([]bool), valids) },
		},
		{
			logical:  "real",
			physical: "float",
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Float64Type{}}}, nil),
			values:   []float64{1, 2},
			nrows:    2,
			builder:  array.NewFloat64Builder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.Float64Builder).AppendValues(vs.([]float64), valids) },
		},
		{
			logical:  "text",
			physical: "string",
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.StringType{}}}, nil),
			values:   []string{"foo", "bar"},
			nrows:    2,
			builder:  array.NewStringBuilder(pool),
			append:   func(b array.Builder, vs interface{}) { b.(*array.StringBuilder).AppendValues(vs.([]string), valids) },
		},
		{
			logical: "binary",
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.BinaryType{}}}, nil),
			values:  [][]byte{[]byte("foo"), []byte("bar")},
			nrows:   2,
			builder: array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary),
			append:  func(b array.Builder, vs interface{}) { b.(*array.BinaryBuilder).AppendValues(vs.([][]byte), valids) },
		},
		{
			logical: "date",
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.Date32Type{}}}, nil),
			values:  []time.Time{time.Now(), localTime},
			nrows:   2,
			builder: array.NewDate32Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, d := range vs.([]time.Time) {
					b.(*array.Date32Builder).Append(arrow.Date32(d.Unix()))
				}
			},
		},
		{
			logical: "time",
			sc:      arrow.NewSchema([]arrow.Field{{Type: arrow.FixedWidthTypes.Time64ns}}, nil),
			values:  []time.Time{time.Now(), time.Now()},
			nrows:   2,
			builder: array.NewTime64Builder(pool, arrow.FixedWidthTypes.Time64ns.(*arrow.Time64Type)),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Time64Builder).Append(arrow.Time64(t.UnixNano()))
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				arr := convertedRec.Column(0).(*array.Time64)
				for i := 0; i < arr.Len(); i++ {
					if srcvs[i].UnixNano() != int64(arr.Value(i)) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ntz",
			values:  []time.Time{time.Now(), localTime},
			nrows:   2,
			rowType: execResponseRowType{Scale: 9},
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.TimestampType{}}}, nil),
			builder: array.NewTimestampBuilder(pool, &arrow.TimestampType{}),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixNano()))
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if srcvs[i].UnixNano() != int64(t) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_ntz",
			physical: "int64",
			values:   []time.Time{time.Now(), localTime},
			nrows:    2,
			rowType:  execResponseRowType{Scale: 9},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixNano())
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if srcvs[i].UnixNano() != int64(t) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_ltz",
			values:  []time.Time{time.Now(), localTime},
			nrows:   2,
			rowType: execResponseRowType{Scale: 9},
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.TimestampType{}}}, nil),
			builder: array.NewTimestampBuilder(pool, &arrow.TimestampType{}),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixNano()))
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if srcvs[i].UnixNano() != int64(t) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "timestamp_tz",
			values:  []time.Time{time.Now(), localTime},
			nrows:   2,
			sc:      arrow.NewSchema([]arrow.Field{{Type: arrow.StructOf(field1, field2)}}, nil),
			builder: array.NewStructBuilder(pool, tzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.UnixNano()))
				}
			},
			compare: func(src interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if srcvs[i].Unix() != time.Unix(0, int64(t)).Unix() {
						return i
					}
				}
				return -1
			},
		},
		{
			logical: "array",
			values:  [][]string{{"foo", "bar"}, {"baz", "quz", "quux"}},
			nrows:   2,
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.StringType{}}}, nil),
			builder: array.NewStringBuilder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, a := range vs.([][]string) {
					b.(*array.StringBuilder).Append(fmt.Sprint(a))
				}
			},
		},
		{
			logical: "object",
			values:  []testObj{{0, "foo"}, {1, "bar"}},
			nrows:   2,
			sc:      arrow.NewSchema([]arrow.Field{{Type: &arrow.StringType{}}}, nil),
			builder: array.NewStringBuilder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, o := range vs.([]testObj) {
					b.(*array.StringBuilder).Append(fmt.Sprint(o))
				}
			},
		},
	} {
		testName := tc.logical
		if tc.physical != "" {
			testName += " " + tc.physical
		}
		t.Run(testName, func(t *testing.T) {
			scope := memory.NewCheckedAllocatorScope(pool)
			defer scope.CheckSize(t)

			b := tc.builder
			defer b.Release()
			tc.append(b, tc.values)
			arr := b.NewArray()
			defer arr.Release()
			rawRec := array.NewRecord(tc.sc, []arrow.Array{arr}, int64(tc.nrows))
			defer rawRec.Release()

			meta := tc.rowType
			meta.Type = tc.logical

			transformedRec, err := arrowToRecord(rawRec, pool, []execResponseRowType{meta}, localTime.Location())
			if err != nil {
				t.Fatalf("error: %s", err)
			}
			defer transformedRec.Release()

			if tc.compare != nil {
				idx := tc.compare(tc.values, transformedRec)
				if idx != -1 {
					t.Fatalf("error: column array value mismatch at index %v", idx)
				}
			} else {
				for i, c := range transformedRec.Columns() {
					rawCol := rawRec.Column(i)
					if rawCol != c {
						t.Fatalf("error: expected column %s, got column %s", rawCol, c)
					}
				}
			}
		})
	}
}

func TestTimestampLTZLocation(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	sc, err := buildSnowflakeConn(ctx, *config)
	if err != nil {
		t.Error(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Error(err)
	}

	src := "1549491451.123456789"
	var dest driver.Value
	loc, _ := time.LoadLocation(PSTLocation)
	if err = stringToValue(&dest, execResponseRowType{Type: "timestamp_ltz"}, &src, loc); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	ts, ok := dest.(time.Time)
	if !ok {
		t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
	}
	if ts.Location() != loc {
		t.Errorf("expected location to be %v, got '%v'", loc, ts.Location())
	}

	if err = stringToValue(&dest, execResponseRowType{Type: "timestamp_ltz"}, &src, nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	ts, ok = dest.(time.Time)
	if !ok {
		t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
	}
	if ts.Location() != time.Local {
		t.Errorf("expected location to be local, got '%v'", ts.Location())
	}
}

func TestSmallTimestampBinding(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	sc, err := buildSnowflakeConn(ctx, *config)
	if err != nil {
		t.Error(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Error(err)
	}
	timeValue, err := time.Parse("2006-01-02 15:04:05", "1600-10-10 10:10:10")
	if err != nil {
		t.Fatalf("failed to parse time: %v", err)
	}
	parameters := []driver.NamedValue{
		{Ordinal: 1, Value: DataTypeTimestampNtz},
		{Ordinal: 2, Value: timeValue},
	}

	rows, err := sc.QueryContext(ctx, "SELECT ?", parameters)
	if err != nil {
		t.Fatalf("failed to run query: %v", err)
	}
	defer rows.Close()

	scanValues := make([]driver.Value, 1)
	for {
		if err := rows.Next(scanValues); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("failed to run query: %v", err)
		}
		if scanValues[0] != timeValue {
			t.Fatalf("unexpected result. expected: %v, got: %v", timeValue, scanValues[0])
		}
	}
}

func TestLargeTimestampBinding(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	sc, err := buildSnowflakeConn(ctx, *config)
	if err != nil {
		t.Error(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Error(err)
	}
	timeValue, err := time.Parse("2006-01-02 15:04:05", "9000-10-10 10:10:10")
	if err != nil {
		t.Fatalf("failed to parse time: %v", err)
	}
	parameters := []driver.NamedValue{
		{Ordinal: 1, Value: DataTypeTimestampNtz},
		{Ordinal: 2, Value: timeValue},
	}

	rows, err := sc.QueryContext(ctx, "SELECT ?", parameters)
	if err != nil {
		t.Fatalf("failed to run query: %v", err)
	}
	defer rows.Close()

	scanValues := make([]driver.Value, 1)
	for {
		if err := rows.Next(scanValues); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("failed to run query: %v", err)
		}
		if scanValues[0] != timeValue {
			t.Fatalf("unexpected result. expected: %v, got: %v", timeValue, scanValues[0])
		}
	}
}
