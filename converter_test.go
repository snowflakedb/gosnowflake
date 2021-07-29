// Copyright (c) 2017-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"database/sql/driver"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"math/big"
	"math/cmplx"
	"reflect"
	"testing"
	"time"
)

type tcGoTypeToSnowflake struct {
	in    interface{}
	tmode snowflakeType
	out   snowflakeType
}

func TestGoTypeToSnowflake(t *testing.T) {
	testcases := []tcGoTypeToSnowflake{
		{in: int64(123), tmode: nullType, out: fixedType},
		{in: float64(234.56), tmode: nullType, out: realType},
		{in: true, tmode: nullType, out: booleanType},
		{in: "teststring", tmode: nullType, out: textType},
		{in: Array([]int{1}), tmode: nullType, out: sliceType},
		{in: DataTypeBinary, tmode: nullType, out: changeType},
		{in: DataTypeTimestampLtz, tmode: nullType, out: changeType},
		{in: DataTypeTimestampNtz, tmode: nullType, out: changeType},
		{in: DataTypeTimestampTz, tmode: nullType, out: changeType},
		{in: time.Now(), tmode: timestampNtzType, out: timestampNtzType},
		{in: time.Now(), tmode: timestampTzType, out: timestampTzType},
		{in: time.Now(), tmode: timestampLtzType, out: timestampLtzType},
		{in: []byte{1, 2, 3}, tmode: binaryType, out: binaryType},
		// negative
		{in: 123, tmode: nullType, out: unSupportedType},
		{in: int8(12), tmode: nullType, out: unSupportedType},
		{in: int32(456), tmode: nullType, out: unSupportedType},
		{in: uint(456), tmode: nullType, out: unSupportedType},
		{in: uint8(12), tmode: nullType, out: unSupportedType},
		{in: uint64(456), tmode: nullType, out: unSupportedType},
		{in: []byte{100}, tmode: nullType, out: unSupportedType},
		{in: nil, tmode: nullType, out: unSupportedType},
		{in: []int{1}, tmode: nullType, out: unSupportedType},
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
	_, err := valueToString(v, nullType)
	if err == nil {
		t.Errorf("should raise error: %v", v)
	}

	// both localTime and utcTime should yield the same unix timestamp
	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))
	utcTime := time.Date(2019, 2, 6, 22, 17, 31, 123456789, time.UTC)
	expectedUnixTime := "1549491451123456789" // time.Unix(1549491451, 123456789).Format(time.RFC3339) == "2019-02-06T14:17:31-08:00"

	if s, err := valueToString(localTime, timestampLtzType); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedUnixTime, s)
	} else if *s != expectedUnixTime {
		t.Errorf("expected '%v', got '%v'", expectedUnixTime, *s)
	}

	if s, err := valueToString(utcTime, timestampLtzType); err != nil {
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

			err := arrowToValue(&dest, meta, arr, true)
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
