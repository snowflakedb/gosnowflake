// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"math/big"
	"math/cmplx"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

func stringFloatToInt(src string, scale int64) (int64, bool) {
	b, ok := new(big.Float).SetString(src)
	if !ok {
		return 0, ok
	}
	s := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
	n := new(big.Float).Mul(b, s)
	var z big.Int
	n.Int(&z)
	if !z.IsInt64() {
		return 0, false
	}
	return z.Int64(), true
}

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
		{in: Array(&[]int{1}), tmode: nullType, out: sliceType},
		{in: Array([]int32{1}), tmode: nullType, out: sliceType},
		{in: Array(&[]int32{1}), tmode: nullType, out: sliceType},
		{in: Array([]int64{1}), tmode: nullType, out: sliceType},
		{in: Array(&[]int64{1}), tmode: nullType, out: sliceType},
		{in: Array([]float64{1.1}), tmode: nullType, out: sliceType},
		{in: Array(&[]float64{1.1}), tmode: nullType, out: sliceType},
		{in: Array([]float32{1.1}), tmode: nullType, out: sliceType},
		{in: Array(&[]float32{1.1}), tmode: nullType, out: sliceType},
		{in: Array([]bool{true}), tmode: nullType, out: sliceType},
		{in: Array([]string{"test string"}), tmode: nullType, out: sliceType},
		{in: Array([][]byte{}), tmode: nullType, out: sliceType},
		{in: Array([]time.Time{time.Now()}, TimestampNTZType), tmode: timestampNtzType, out: sliceType},
		{in: Array([]time.Time{time.Now()}, TimestampLTZType), tmode: timestampLtzType, out: sliceType},
		{in: Array([]time.Time{time.Now()}, TimestampTZType), tmode: timestampTzType, out: sliceType},
		{in: Array([]time.Time{time.Now()}, DateType), tmode: dateType, out: sliceType},
		{in: Array([]time.Time{time.Now()}, TimeType), tmode: timeType, out: sliceType},
		{in: DataTypeBinary, tmode: nullType, out: changeType},
		{in: DataTypeTimestampLtz, tmode: nullType, out: changeType},
		{in: DataTypeTimestampNtz, tmode: nullType, out: changeType},
		{in: DataTypeTimestampTz, tmode: nullType, out: changeType},
		{in: time.Now(), tmode: timestampNtzType, out: timestampNtzType},
		{in: time.Now(), tmode: timestampTzType, out: timestampTzType},
		{in: time.Now(), tmode: timestampLtzType, out: timestampLtzType},
		{in: []byte{1, 2, 3}, tmode: binaryType, out: binaryType},
		{in: []int{1}, tmode: nullType, out: arrayType},
		{in: Array([]interface{}{int64(123)}), tmode: nullType, out: sliceType},
		{in: Array([]interface{}{float64(234.56)}), tmode: nullType, out: sliceType},
		{in: Array([]interface{}{true}), tmode: nullType, out: sliceType},
		{in: Array([]interface{}{"teststring"}), tmode: nullType, out: sliceType},
		{in: Array([]interface{}{[]byte{1, 2, 3}}), tmode: nullType, out: sliceType},
		{in: Array([]interface{}{time.Now()}), tmode: timestampNtzType, out: sliceType},
		{in: Array([]interface{}{time.Now()}), tmode: timestampTzType, out: sliceType},
		{in: Array([]interface{}{time.Now()}), tmode: timestampLtzType, out: sliceType},
		{in: Array([]interface{}{time.Now()}), tmode: dateType, out: sliceType},
		{in: Array([]interface{}{time.Now()}), tmode: timeType, out: sliceType},
		{in: Array([]interface{}{time.Now()}, TimestampNTZType), tmode: timestampLtzType, out: sliceType},
		{in: Array([]interface{}{time.Now()}, TimestampLTZType), tmode: dateType, out: sliceType},
		{in: Array([]interface{}{time.Now()}, TimestampTZType), tmode: timeType, out: sliceType},
		{in: Array([]interface{}{time.Now()}, DateType), tmode: timestampNtzType, out: sliceType},
		{in: Array([]interface{}{time.Now()}, TimeType), tmode: timestampTzType, out: sliceType},
		{in: nil, tmode: nullType, out: nullType},
		// negative
		{in: 123, tmode: nullType, out: unSupportedType},
		{in: int8(12), tmode: nullType, out: unSupportedType},
		{in: int32(456), tmode: nullType, out: unSupportedType},
		{in: uint(456), tmode: nullType, out: unSupportedType},
		{in: uint8(12), tmode: nullType, out: unSupportedType},
		{in: uint64(456), tmode: nullType, out: unSupportedType},
		{in: []byte{100}, tmode: nullType, out: unSupportedType},
	}
	for _, test := range testcases {
		t.Run(fmt.Sprintf("%v_%v_%v", test.in, test.out, test.tmode), func(t *testing.T) {
			a := goTypeToSnowflake(test.in, test.tmode)
			if a != test.out {
				t.Errorf("failed. in: %v, tmode: %v, expected: %v, got: %v", test.in, test.tmode, test.out, a)
			}
		})
	}
}

func TestSnowflakeTypeToGo(t *testing.T) {
	testcases := []struct {
		in     snowflakeType
		scale  int64
		fields []fieldMetadata
		out    reflect.Type
		ctx    context.Context
	}{
		{in: fixedType, scale: 0, out: reflect.TypeOf(int64(0)), ctx: context.Background()},
		{in: fixedType, scale: 2, out: reflect.TypeOf(float64(0)), ctx: context.Background()},
		{in: realType, scale: 0, out: reflect.TypeOf(float64(0)), ctx: context.Background()},
		{in: textType, scale: 0, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: dateType, scale: 0, out: reflect.TypeOf(time.Now()), ctx: context.Background()},
		{in: timeType, scale: 0, out: reflect.TypeOf(time.Now()), ctx: context.Background()},
		{in: timestampLtzType, scale: 0, out: reflect.TypeOf(time.Now()), ctx: context.Background()},
		{in: timestampNtzType, scale: 0, out: reflect.TypeOf(time.Now()), ctx: context.Background()},
		{in: timestampTzType, scale: 0, out: reflect.TypeOf(time.Now()), ctx: context.Background()},
		{in: objectType, scale: 0, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: objectType, scale: 0, fields: []fieldMetadata{}, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: objectType, scale: 0, fields: []fieldMetadata{{}}, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: objectType, scale: 0, fields: []fieldMetadata{{}}, out: reflect.TypeOf(ObjectType{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: variantType, scale: 0, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: arrayType, scale: 0, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: binaryType, scale: 0, out: reflect.TypeOf([]byte{}), ctx: context.Background()},
		{in: booleanType, scale: 0, out: reflect.TypeOf(true), ctx: context.Background()},
		{in: sliceType, scale: 0, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "fixed", Scale: 0}}, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "fixed", Scale: 0}}, out: reflect.TypeOf([]int64{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "fixed", Scale: 1}}, out: reflect.TypeOf([]float64{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "fixed", Scale: 0}}, out: reflect.TypeOf([]*big.Int{}), ctx: WithStructuredTypesEnabled(WithHigherPrecision(context.Background()))},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "fixed", Scale: 1}}, out: reflect.TypeOf([]*big.Float{}), ctx: WithStructuredTypesEnabled(WithHigherPrecision(context.Background()))},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "real", Scale: 1}}, out: reflect.TypeOf([]float64{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "text"}}, out: reflect.TypeOf([]string{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "date"}}, out: reflect.TypeOf([]time.Time{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "time"}}, out: reflect.TypeOf([]time.Time{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "timestamp_ntz"}}, out: reflect.TypeOf([]time.Time{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "timestamp_ltz"}}, out: reflect.TypeOf([]time.Time{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "timestamp_tz"}}, out: reflect.TypeOf([]time.Time{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "boolean"}}, out: reflect.TypeOf([]bool{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "binary"}}, out: reflect.TypeOf([][]byte{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: arrayType, scale: 0, fields: []fieldMetadata{{Type: "object"}}, out: reflect.TypeOf([]ObjectType{}), ctx: WithStructuredTypesEnabled(context.Background())},
		{in: mapType, fields: nil, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: mapType, fields: []fieldMetadata{}, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: mapType, fields: []fieldMetadata{{}, {}}, out: reflect.TypeOf(""), ctx: context.Background()},
		{in: mapType, fields: []fieldMetadata{{Type: "text"}, {Type: "text"}}, out: reflect.TypeOf(map[string]string{}), ctx: WithStructuredTypesEnabled(context.Background())},
	}
	for _, test := range testcases {
		t.Run(fmt.Sprintf("%v_%v", test.in, test.out), func(t *testing.T) {
			a := snowflakeTypeToGo(test.ctx, test.in, test.scale, test.fields)
			if a != test.out {
				t.Errorf("failed. in: %v, scale: %v, expected: %v, got: %v",
					test.in, test.scale, test.out, a)
			}
		})
	}
}

type testValueToStringStructuredObject struct {
	s    string
	i    int32
	date time.Time
}

func (o *testValueToStringStructuredObject) Write(sowc StructuredObjectWriterContext) error {
	if err := sowc.WriteString("s", o.s); err != nil {
		return err
	}
	if err := sowc.WriteInt32("i", o.i); err != nil {
		return err
	}
	if err := sowc.WriteTime("date", o.date, DataTypeDate); err != nil {
		return err
	}
	return nil
}

func TestValueToString(t *testing.T) {
	v := cmplx.Sqrt(-5 + 12i) // should never happen as Go sql package must have already validated.
	_, err := valueToString(v, nullType, nil)
	if err == nil {
		t.Errorf("should raise error: %v", v)
	}
	params := make(map[string]*string)
	dateFormat := "YYYY-MM-DD"
	params["date_output_format"] = &dateFormat

	// both localTime and utcTime should yield the same unix timestamp
	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))
	utcTime := time.Date(2019, 2, 6, 22, 17, 31, 123456789, time.UTC)
	expectedUnixTime := "1549491451123456789" // time.Unix(1549491451, 123456789).Format(time.RFC3339) == "2019-02-06T14:17:31-08:00"
	expectedBool := "true"
	expectedInt64 := "1"
	expectedFloat64 := "1.1"
	expectedString := "teststring"

	bv, err := valueToString(localTime, timestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedUnixTime)

	bv, err = valueToString(utcTime, timestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedUnixTime)

	bv, err = valueToString(sql.NullBool{Bool: true, Valid: true}, timestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedBool)

	bv, err = valueToString(sql.NullInt64{Int64: 1, Valid: true}, timestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedInt64)

	bv, err = valueToString(sql.NullFloat64{Float64: 1.1, Valid: true}, timestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedFloat64)

	bv, err = valueToString(sql.NullString{String: "teststring", Valid: true}, timestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedString)

	t.Run("SQL Time", func(t *testing.T) {
		bv, err := valueToString(sql.NullTime{Time: localTime, Valid: true}, timestampLtzType, nil)
		assertNilF(t, err)
		assertEmptyStringE(t, bv.format)
		assertNilE(t, bv.schema)
		assertEqualE(t, *bv.value, expectedUnixTime)
	})

	t.Run("arrays", func(t *testing.T) {
		bv, err := valueToString([2]int{1, 2}, objectType, nil)
		assertNilF(t, err)
		assertEqualE(t, bv.format, jsonFormatStr)
		assertEqualE(t, *bv.value, "[1,2]")
	})
	t.Run("slices", func(t *testing.T) {
		bv, err := valueToString([]int{1, 2}, objectType, nil)
		assertNilF(t, err)
		assertEqualE(t, bv.format, jsonFormatStr)
		assertEqualE(t, *bv.value, "[1,2]")
	})

	t.Run("UUID - should return string", func(t *testing.T) {
		u := NewUUID()
		bv, err := valueToString(u, textType, nil)
		assertNilF(t, err)
		assertEmptyStringE(t, bv.format)
		assertEqualE(t, *bv.value, u.String())
	})

	t.Run("database/sql/driver - Valuer interface", func(t *testing.T) {
		u := newTestUUID()
		bv, err := valueToString(u, textType, nil)
		assertNilF(t, err)
		assertEmptyStringE(t, bv.format)
		assertEqualE(t, *bv.value, u.String())
	})

	t.Run("testUUID", func(t *testing.T) {
		u := newTestUUID()
		assertEqualE(t, u.String(), parseTestUUID(u.String()).String())

		bv, err := valueToString(u, textType, nil)
		assertNilF(t, err)
		assertEmptyStringE(t, bv.format)
		assertEqualE(t, *bv.value, u.String())
	})

	bv, err = valueToString(&testValueToStringStructuredObject{s: "some string", i: 123, date: time.Date(2024, time.May, 24, 0, 0, 0, 0, time.UTC)}, timestampLtzType, params)
	assertNilF(t, err)
	assertEqualE(t, bv.format, jsonFormatStr)
	assertDeepEqualE(t, *bv.schema, bindingSchema{
		Typ:      "object",
		Nullable: true,
		Fields: []fieldMetadata{
			{
				Name:     "s",
				Type:     "text",
				Nullable: true,
				Length:   134217728,
			},
			{
				Name:      "i",
				Type:      "fixed",
				Nullable:  true,
				Precision: 38,
				Scale:     0,
			},
			{
				Name:     "date",
				Type:     "date",
				Nullable: true,
				Scale:    9,
			},
		},
	})
	assertEqualIgnoringWhitespaceE(t, *bv.value, `{"date": "2024-05-24", "i": 123, "s": "some string"}`)
}

func TestExtractTimestamp(t *testing.T) {
	s := "1234abcdef" // pragma: allowlist secret
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
		t.Run(tt, func(t *testing.T) {
			rowType = &execResponseRowType{
				Type: tt,
			}
			if err = stringToValue(context.Background(), &dest, *rowType, &source, nil, nil); err == nil {
				t.Errorf("should raise error. type: %v, value:%v", tt, source)
			}
		})
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
			t.Run(ss+tt, func(t *testing.T) {
				rowType = &execResponseRowType{
					Type: tt,
				}
				if err = stringToValue(context.Background(), &dest, *rowType, &ss, nil, nil); err == nil {
					t.Errorf("should raise error. type: %v, value:%v", tt, source)
				}
			})
		}
	}

	src := "1549491451.123456789"
	if err = stringToValue(context.Background(), &dest, execResponseRowType{Type: "timestamp_ltz"}, &src, nil, nil); err != nil {
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
		{in: driver.NamedValue{Value: &int32Array{1, 2}}, typ: fixedType, out: []string{"1", "2"}},
		{in: driver.NamedValue{Value: &int64Array{3, 4, 5}}, typ: fixedType, out: []string{"3", "4", "5"}},
		{in: driver.NamedValue{Value: &float64Array{6.7}}, typ: realType, out: []string{"6.7"}},
		{in: driver.NamedValue{Value: &float32Array{1.5}}, typ: realType, out: []string{"1.5"}},
		{in: driver.NamedValue{Value: &boolArray{true, false}}, typ: booleanType, out: []string{"true", "false"}},
		{in: driver.NamedValue{Value: &stringArray{"foo", "bar", "baz"}}, typ: textType, out: []string{"foo", "bar", "baz"}},
	}
	for _, test := range testcases {
		t.Run(strings.Join(test.out, "_"), func(t *testing.T) {
			s, a, err := snowflakeArrayToString(&test.in, false)
			assertNilF(t, err)
			if s != test.typ {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.typ, s)
			}
			for i, v := range a {
				if *v != test.out[i] {
					t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.out[i], a)
				}
			}
		})
	}
}

func TestArrowToValues(t *testing.T) {
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
		logical         string
		physical        string
		rowType         execResponseRowType
		values          interface{}
		builder         array.Builder
		append          func(b array.Builder, vs interface{})
		compare         func(src interface{}, dst []snowflakeValue) int
		higherPrecision bool
	}{
		{
			logical:         "fixed",
			physical:        "number", // default: number(38, 0)
			values:          []int64{1, 2},
			builder:         array.NewInt64Builder(pool),
			append:          func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "number(38,5)",
			rowType:  execResponseRowType{Scale: 5},
			values:   []string{"1.05430", "2.08983"},
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 5)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int64Builder).Append(num)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 5)
					if !ok {
						return i
					}
					srcDec := intToBigFloat(num, 5)
					dstDec := dst[i].(*big.Float)
					if srcDec.Cmp(dstDec) != 0 {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "number(38,5)",
			rowType:  execResponseRowType{Scale: 5},
			values:   []string{"1.05430", "2.08983"},
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 5)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int64Builder).Append(num)
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 5)
					if !ok {
						return i
					}
					srcDec := fmt.Sprintf("%.*f", 5, float64(num)/math.Pow10(int(5)))
					dstDec := dst[i]
					if srcDec != dstDec {
						return i
					}
				}
				return -1
			},
			higherPrecision: false,
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
			higherPrecision: true,
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
			higherPrecision: true,
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
			higherPrecision: true,
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
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "int16",
			values:   []string{"1.2345", "2.3456"},
			rowType:  execResponseRowType{Scale: 4},
			builder:  array.NewInt16Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 4)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int16Builder).Append(int16(num))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 4)
					if !ok {
						return i
					}
					srcDec := intToBigFloat(num, 4)
					dstDec := dst[i].(*big.Float)
					if srcDec.Cmp(dstDec) != 0 {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "int16",
			values:   []string{"1.2345", "2.3456"},
			rowType:  execResponseRowType{Scale: 4},
			builder:  array.NewInt16Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 4)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int16Builder).Append(int16(num))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 4)
					if !ok {
						return i
					}
					srcDec := fmt.Sprintf("%.*f", 4, float64(num)/math.Pow10(int(4)))
					dstDec := dst[i]
					if srcDec != dstDec {
						return i
					}
				}
				return -1
			},
			higherPrecision: false,
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
					if int64(srcvs[i]) != dst[i] {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "int32",
			values:   []string{"1.23456", "2.34567"},
			rowType:  execResponseRowType{Scale: 5},
			builder:  array.NewInt32Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 5)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int32Builder).Append(int32(num))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 5)
					if !ok {
						return i
					}
					srcDec := intToBigFloat(num, 5)
					dstDec := dst[i].(*big.Float)
					if srcDec.Cmp(dstDec) != 0 {
						return i
					}
				}
				return -1
			},
			higherPrecision: true,
		},
		{
			logical:  "fixed",
			physical: "int32",
			values:   []string{"1.23456", "2.34567"},
			rowType:  execResponseRowType{Scale: 5},
			builder:  array.NewInt32Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringFloatToInt(s, 5)
					if !ok {
						t.Fatalf("failed to convert to int")
					}
					b.(*array.Int32Builder).Append(int32(num))
				}
			},
			compare: func(src interface{}, dst []snowflakeValue) int {
				srcvs := src.([]string)
				for i := range srcvs {
					num, ok := stringFloatToInt(srcvs[i], 5)
					if !ok {
						return i
					}
					srcDec := fmt.Sprintf("%.*f", 5, float64(num)/math.Pow10(int(5)))
					dstDec := dst[i]
					if srcDec != dstDec {
						return i
					}
				}
				return -1
			},
			higherPrecision: false,
		},
		{
			logical:         "fixed",
			physical:        "int64",
			values:          []int64{1, 2},
			builder:         array.NewInt64Builder(pool),
			append:          func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
			higherPrecision: true,
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
			higherPrecision: true,
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

			withHigherPrecision := tc.higherPrecision

			if err := arrowToValues(context.Background(), dest, meta, arr, localTime.Location(), withHigherPrecision, nil); err != nil { // TODO
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

	localTime := time.Date(2019, 1, 1, 1, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))
	localTimeFarIntoFuture := time.Date(9000, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))

	epochField := arrow.Field{Name: "epoch", Type: &arrow.Int64Type{}}
	timezoneField := arrow.Field{Name: "timezone", Type: &arrow.Int32Type{}}
	fractionField := arrow.Field{Name: "fraction", Type: &arrow.Int32Type{}}
	timestampTzStructWithoutFraction := arrow.StructOf(epochField, timezoneField)
	timestampTzStructWithFraction := arrow.StructOf(epochField, fractionField, timezoneField)
	timestampNtzStruct := arrow.StructOf(epochField, fractionField)
	timestampLtzStruct := arrow.StructOf(epochField, fractionField)

	type testObj struct {
		field1 int
		field2 string
	}

	for _, tc := range []struct {
		logical                          string
		physical                         string
		sc                               *arrow.Schema
		rowType                          execResponseRowType
		values                           interface{}
		expected                         interface{}
		error                            string
		arrowBatchesTimestampOption      snowflakeArrowBatchesTimestampOption
		enableArrowBatchesUtf8Validation bool
		withHigherPrecision              bool
		nrows                            int
		builder                          array.Builder
		append                           func(b array.Builder, vs interface{})
		compare                          func(src interface{}, expected interface{}, rec arrow.Record) int
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
			physical: "int64",
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
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
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
			logical:             "fixed",
			physical:            "number(38,0)",
			sc:                  arrow.NewSchema([]arrow.Field{{Type: &arrow.Decimal128Type{Precision: 38, Scale: 0}}}, nil),
			values:              []string{"10000000000000000000000000000000000000", "-12345678901234567890123456789012345678"},
			withHigherPrecision: true,
			nrows:               2,
			builder:             array.NewDecimal128Builder(pool, &arrow.Decimal128Type{Precision: 38, Scale: 0}),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, ok := stringIntToDecimal(s)
					if !ok {
						t.Fatalf("failed to convert to Int64")
					}
					b.(*array.Decimal128Builder).Append(num)
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]string)
				for i, dec := range convertedRec.Column(0).(*array.Decimal128).Values() {
					srcDec, ok := stringIntToDecimal(srcvs[i])
					if !ok {
						return i
					}
					if srcDec != dec {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "fixed",
			physical: "float64",
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
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
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
			logical:             "fixed",
			physical:            "number(38,37)",
			rowType:             execResponseRowType{Scale: 37},
			sc:                  arrow.NewSchema([]arrow.Field{{Type: &arrow.Decimal128Type{Precision: 38, Scale: 37}}}, nil),
			values:              []string{"1.2345678901234567890123456789012345678", "-9.999999999999999"},
			withHigherPrecision: true,
			nrows:               2,
			builder:             array.NewDecimal128Builder(pool, &arrow.Decimal128Type{Precision: 38, Scale: 37}),
			append: func(b array.Builder, vs interface{}) {
				for _, s := range vs.([]string) {
					num, err := decimal128.FromString(s, 38, 37)
					if err != nil {
						t.Fatalf("failed to convert to decimal: %s", err)
					}
					b.(*array.Decimal128Builder).Append(num)
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]string)
				for i, dec := range convertedRec.Column(0).(*array.Decimal128).Values() {
					srcDec, err := decimal128.FromString(srcvs[i], 38, 37)
					if err != nil {
						return i
					}
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
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
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
			logical:             "fixed",
			physical:            "int8",
			rowType:             execResponseRowType{Scale: 1},
			sc:                  arrow.NewSchema([]arrow.Field{{Type: &arrow.Int8Type{}}}, nil),
			values:              []int8{10, 16},
			withHigherPrecision: true,
			nrows:               2,
			builder:             array.NewInt8Builder(pool),
			append:              func(b array.Builder, vs interface{}) { b.(*array.Int8Builder).AppendValues(vs.([]int8), valids) },
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]int8)
				for i, f := range convertedRec.Column(0).(*array.Int8).Int8Values() {
					if srcvs[i] != f {
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
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
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
			logical:             "fixed",
			physical:            "int16",
			rowType:             execResponseRowType{Scale: 1},
			sc:                  arrow.NewSchema([]arrow.Field{{Type: &arrow.Int16Type{}}}, nil),
			values:              []int16{20, 26},
			withHigherPrecision: true,
			nrows:               2,
			builder:             array.NewInt16Builder(pool),
			append:              func(b array.Builder, vs interface{}) { b.(*array.Int16Builder).AppendValues(vs.([]int16), valids) },
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]int16)
				for i, f := range convertedRec.Column(0).(*array.Int16).Int16Values() {
					if srcvs[i] != f {
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
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
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
			logical:             "fixed",
			physical:            "int32",
			rowType:             execResponseRowType{Scale: 2},
			sc:                  arrow.NewSchema([]arrow.Field{{Type: &arrow.Int32Type{}}}, nil),
			values:              []int32{200, 265},
			withHigherPrecision: true,
			nrows:               2,
			builder:             array.NewInt32Builder(pool),
			append:              func(b array.Builder, vs interface{}) { b.(*array.Int32Builder).AppendValues(vs.([]int32), valids) },
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]int32)
				for i, f := range convertedRec.Column(0).(*array.Int32).Int32Values() {
					if srcvs[i] != f {
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
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
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
			logical:             "fixed",
			physical:            "int64",
			rowType:             execResponseRowType{Scale: 5},
			sc:                  arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			values:              []int64{12345, 234567},
			withHigherPrecision: true,
			nrows:               2,
			builder:             array.NewInt64Builder(pool),
			append:              func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]int64)
				for i, f := range convertedRec.Column(0).(*array.Int64).Int64Values() {
					if srcvs[i] != f {
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
			logical:                          "text",
			physical:                         "string with invalid utf8",
			sc:                               arrow.NewSchema([]arrow.Field{{Type: &arrow.StringType{}}}, nil),
			rowType:                          execResponseRowType{Type: "TEXT"},
			values:                           []string{"\xFF", "bar", "baz\xFF\xFF"},
			expected:                         []string{"�", "bar", "baz��"},
			enableArrowBatchesUtf8Validation: true,
			nrows:                            2,
			builder:                          array.NewStringBuilder(pool),
			append:                           func(b array.Builder, vs interface{}) { b.(*array.StringBuilder).AppendValues(vs.([]string), valids) },
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				arr := convertedRec.Column(0).(*array.String)
				for i := 0; i < arr.Len(); i++ {
					if expected.([]string)[i] != string(arr.Value(i)) {
						return i
					}
				}
				return -1
			},
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
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
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
			logical:  "timestamp_ntz",
			physical: "int64",                                                                                  // timestamp_ntz with scale 0..3 -> int64
			values:   []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)}, // Millisecond for scale = 3
			nrows:    2,
			rowType:  execResponseRowType{Scale: 3},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli()) // Millisecond for scale = 3
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Nanosecond)) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_ntz",
			physical: "struct", // timestamp_ntz with scale 4..9 -> int64 + int32
			values:   []time.Time{time.Now(), localTime},
			nrows:    2,
			rowType:  execResponseRowType{Scale: 9},
			sc:       arrow.NewSchema([]arrow.Field{{Type: timestampNtzStruct}}, nil),
			builder:  array.NewStructBuilder(pool, timestampNtzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Nanosecond)) {
						return i
					}
				}
				return -1
			},
		},
		// microsecond timestamp_ntz
		{
			logical:                     "timestamp_ntz",
			physical:                    "struct", // timestamp_ntz with scale 4..9 -> int64 + int32
			values:                      []time.Time{time.Now().Truncate(time.Microsecond), localTime.Truncate(time.Microsecond)},
			arrowBatchesTimestampOption: UseMicrosecondTimestamp,
			nrows:                       2,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampNtzStruct}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampNtzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Microsecond)) {
						return i
					}
				}
				return -1
			},
		},
		// millisecond timestamp_ntz
		{
			logical:                     "timestamp_ntz",
			physical:                    "struct", // timestamp_ntz with scale 4..9 -> int64 + int32
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: UseMillisecondTimestamp,
			nrows:                       2,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampNtzStruct}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampNtzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Millisecond)) {
						return i
					}
				}
				return -1
			},
		},
		// second timestamp_ntz
		{
			logical:                     "timestamp_ntz",
			physical:                    "struct", // timestamp_ntz with scale 4..9 -> int64 + int32
			values:                      []time.Time{time.Now().Truncate(time.Second), localTime.Truncate(time.Second)},
			arrowBatchesTimestampOption: UseSecondTimestamp,
			nrows:                       2,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampNtzStruct}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampNtzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Second)) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_ntz",
			physical: "error",
			values:   []time.Time{localTimeFarIntoFuture},
			error:    "Cannot convert timestamp",
			nrows:    1,
			rowType:  execResponseRowType{Scale: 3},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli())
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int { return 0 },
		},
		{
			logical:                     "timestamp_ntz",
			physical:                    "int64 with original timestamp", // timestamp_ntz with scale 0..3 -> int64
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond), localTimeFarIntoFuture.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     execResponseRowType{Scale: 3},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:                     array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli()) // Millisecond for scale = 3
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i := 0; i < convertedRec.Column(0).Len(); i++ {
					ts := arrowSnowflakeTimestampToTime(convertedRec.Column(0), timestampNtzType, 3, i, nil)
					if !srcvs[i].Equal(*ts) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:                     "timestamp_ntz",
			physical:                    "struct with original timestamp", // timestamp_ntz with scale 4..9 -> int64 + int32
			values:                      []time.Time{time.Now(), localTime, localTimeFarIntoFuture},
			arrowBatchesTimestampOption: UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampNtzStruct}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampNtzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i := 0; i < convertedRec.Column(0).Len(); i++ {
					ts := arrowSnowflakeTimestampToTime(convertedRec.Column(0), timestampNtzType, 9, i, nil)
					if !srcvs[i].Equal(*ts) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_ltz",
			physical: "int64", // timestamp_ntz with scale 0..3 -> int64
			values:   []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			nrows:    2,
			rowType:  execResponseRowType{Scale: 3},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli()) // Millisecond for scale = 3
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Nanosecond)) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_ltz",
			physical: "struct", // timestamp_ntz with scale 4..9 -> int64 + int32
			values:   []time.Time{time.Now(), localTime},
			nrows:    2,
			rowType:  execResponseRowType{Scale: 9},
			sc:       arrow.NewSchema([]arrow.Field{{Type: timestampNtzStruct}}, nil),
			builder:  array.NewStructBuilder(pool, timestampNtzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Nanosecond)) {
						return i
					}
				}
				return -1
			},
		},
		// microsecond timestamp_ltz
		{
			logical:                     "timestamp_ltz",
			physical:                    "struct", // timestamp_ntz with scale 4..9 -> int64 + int32
			values:                      []time.Time{time.Now().Truncate(time.Microsecond), localTime.Truncate(time.Microsecond)},
			arrowBatchesTimestampOption: UseMicrosecondTimestamp,
			nrows:                       2,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampNtzStruct}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampNtzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Microsecond)) {
						return i
					}
				}
				return -1
			},
		},
		// millisecond timestamp_ltz
		{
			logical:                     "timestamp_ltz",
			physical:                    "struct", // timestamp_ntz with scale 4..9 -> int64 + int32
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: UseMillisecondTimestamp,
			nrows:                       2,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampNtzStruct}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampNtzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Millisecond)) {
						return i
					}
				}
				return -1
			},
		},
		// second timestamp_ltz
		{
			logical:                     "timestamp_ltz",
			physical:                    "struct", // timestamp_ntz with scale 4..9 -> int64 + int32
			values:                      []time.Time{time.Now().Truncate(time.Second), localTime.Truncate(time.Second)},
			arrowBatchesTimestampOption: UseSecondTimestamp,
			nrows:                       2,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampNtzStruct}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampNtzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Second)) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_ltz",
			physical: "error",
			values:   []time.Time{localTimeFarIntoFuture},
			error:    "Cannot convert timestamp",
			nrows:    1,
			rowType:  execResponseRowType{Scale: 3},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli()) // Millisecond for scale = 3
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int { return 0 },
		},
		{
			logical:                     "timestamp_ltz",
			physical:                    "int64 with original timestamp", // timestamp_ntz with scale 0..3 -> int64
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond), localTimeFarIntoFuture.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     execResponseRowType{Scale: 3},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:                     array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli()) // Millisecond for scale = 3
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i := 0; i < convertedRec.Column(0).Len(); i++ {
					ts := arrowSnowflakeTimestampToTime(convertedRec.Column(0), timestampLtzType, 3, i, localTime.Location())
					if !srcvs[i].Equal(*ts) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:                     "timestamp_ltz",
			physical:                    "struct with original timestamp", // timestamp_ntz with scale 4..9 -> int64 + int32
			values:                      []time.Time{time.Now(), localTime, localTimeFarIntoFuture},
			arrowBatchesTimestampOption: UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampLtzStruct}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampLtzStruct),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i := 0; i < convertedRec.Column(0).Len(); i++ {
					ts := arrowSnowflakeTimestampToTime(convertedRec.Column(0), timestampLtzType, 9, i, localTime.Location())
					if !srcvs[i].Equal(*ts) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_tz",
			physical: "struct2", // timestamp_tz with scale 0..3 -> int64 + int32
			values:   []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			nrows:    2,
			rowType:  execResponseRowType{Scale: 3},
			sc:       arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithoutFraction}}, nil),
			builder:  array.NewStructBuilder(pool, timestampTzStructWithoutFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.UnixMilli()) // Millisecond for scale = 3
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(0))      // timezone index - not important in tests
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Nanosecond)) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_tz",
			physical: "struct3", // timestamp_tz with scale 4..9 -> int64 + int32 + int32
			values:   []time.Time{time.Now(), localTime},
			nrows:    2,
			rowType:  execResponseRowType{Scale: 9},
			sc:       arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithFraction}}, nil),
			builder:  array.NewStructBuilder(pool, timestampTzStructWithFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
					sb.FieldBuilder(2).(*array.Int32Builder).Append(int32(0)) // timezone index - not important in tests
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Nanosecond)) {
						return i
					}
				}
				return -1
			},
		},
		// microsecond timestamp_tz
		{
			logical:                     "timestamp_tz",
			physical:                    "struct3", // timestamp_tz with scale 4..9 -> int64 + int32 + int32
			values:                      []time.Time{time.Now().Truncate(time.Microsecond), localTime.Truncate(time.Microsecond)},
			arrowBatchesTimestampOption: UseMicrosecondTimestamp,
			nrows:                       2,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithFraction}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampTzStructWithFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
					sb.FieldBuilder(2).(*array.Int32Builder).Append(int32(0)) // timezone index - not important in tests
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Microsecond)) {
						return i
					}
				}
				return -1
			},
		},
		// millisecond timestamp_tz
		{
			logical:                     "timestamp_tz",
			physical:                    "struct3", // timestamp_tz with scale 4..9 -> int64 + int32 + int32
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: UseMillisecondTimestamp,
			nrows:                       2,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithFraction}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampTzStructWithFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
					sb.FieldBuilder(2).(*array.Int32Builder).Append(int32(0)) // timezone index - not important in tests
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Millisecond)) {
						return i
					}
				}
				return -1
			},
		},
		// second timestamp_tz
		{
			logical:                     "timestamp_tz",
			physical:                    "struct3", // timestamp_tz with scale 4..9 -> int64 + int32 + int32
			values:                      []time.Time{time.Now().Truncate(time.Second), localTime.Truncate(time.Second)},
			arrowBatchesTimestampOption: UseSecondTimestamp,
			nrows:                       2,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithFraction}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampTzStructWithFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
					sb.FieldBuilder(2).(*array.Int32Builder).Append(int32(0)) // timezone index - not important in tests
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i, t := range convertedRec.Column(0).(*array.Timestamp).TimestampValues() {
					if !srcvs[i].Equal(t.ToTime(arrow.Second)) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:                     "timestamp_tz",
			physical:                    "struct2 with original timestamp", // timestamp_ntz with scale 0..3 -> int64 + int32
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond), localTimeFarIntoFuture.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     execResponseRowType{Scale: 3},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithoutFraction}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampTzStructWithoutFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.UnixMilli()) // Millisecond for scale = 3
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(0))      // timezone index - not important in tests
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i := 0; i < convertedRec.Column(0).Len(); i++ {
					ts := arrowSnowflakeTimestampToTime(convertedRec.Column(0), timestampTzType, 3, i, nil)
					if !srcvs[i].Equal(*ts) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:                     "timestamp_tz",
			physical:                    "struct3 with original timestamp", // timestamp_ntz with scale 4..9 -> int64 + int32 + int32
			values:                      []time.Time{time.Now(), localTime, localTimeFarIntoFuture},
			arrowBatchesTimestampOption: UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     execResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithFraction}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampTzStructWithFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
					sb.FieldBuilder(2).(*array.Int32Builder).Append(int32(0)) // timezone index - not important in tests
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i := 0; i < convertedRec.Column(0).Len(); i++ {
					ts := arrowSnowflakeTimestampToTime(convertedRec.Column(0), timestampTzType, 9, i, nil)
					if !srcvs[i].Equal(*ts) {
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

			ctx := context.Background()
			switch tc.arrowBatchesTimestampOption {
			case UseOriginalTimestamp:
				ctx = WithArrowBatchesTimestampOption(ctx, UseOriginalTimestamp)
			case UseSecondTimestamp:
				ctx = WithArrowBatchesTimestampOption(ctx, UseSecondTimestamp)
			case UseMillisecondTimestamp:
				ctx = WithArrowBatchesTimestampOption(ctx, UseMillisecondTimestamp)
			case UseMicrosecondTimestamp:
				ctx = WithArrowBatchesTimestampOption(ctx, UseMicrosecondTimestamp)
			default:
				ctx = WithArrowBatchesTimestampOption(ctx, UseNanosecondTimestamp)
			}

			if tc.enableArrowBatchesUtf8Validation {
				ctx = WithArrowBatchesUtf8Validation(ctx)
			}

			if tc.withHigherPrecision {
				ctx = WithHigherPrecision(ctx)
			}

			transformedRec, err := arrowToRecord(ctx, rawRec, pool, []execResponseRowType{meta}, localTime.Location())
			if err != nil {
				if tc.error == "" || !strings.Contains(err.Error(), tc.error) {
					t.Fatalf("error: %s", err)
				}
			} else {
				defer transformedRec.Release()
				if tc.error != "" {
					t.Fatalf("expected error: %s", tc.error)
				}

				if tc.compare != nil {
					idx := tc.compare(tc.values, tc.expected, transformedRec)
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
			}
		})
	}
}

func TestTimestampLTZLocation(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		src := "1549491451.123456789"
		var dest driver.Value
		loc, _ := time.LoadLocation(PSTLocation)
		if err := stringToValue(context.Background(), &dest, execResponseRowType{Type: "timestamp_ltz"}, &src, loc, nil); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		ts, ok := dest.(time.Time)
		if !ok {
			t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
		}
		if ts.Location() != loc {
			t.Errorf("expected location to be %v, got '%v'", loc, ts.Location())
		}

		if err := stringToValue(context.Background(), &dest, execResponseRowType{Type: "timestamp_ltz"}, &src, nil, nil); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		ts, ok = dest.(time.Time)
		if !ok {
			t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
		}
		if ts.Location() != time.Local {
			t.Errorf("expected location to be local, got '%v'", ts.Location())
		}
	})
}

func TestSmallTimestampBinding(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		ctx := context.Background()
		timeValue, err := time.Parse("2006-01-02 15:04:05", "1600-10-10 10:10:10")
		if err != nil {
			t.Fatalf("failed to parse time: %v", err)
		}
		parameters := []driver.NamedValue{
			{Ordinal: 1, Value: DataTypeTimestampNtz},
			{Ordinal: 2, Value: timeValue},
		}

		rows := sct.mustQueryContext(ctx, "SELECT ?", parameters)
		defer func() {
			assertNilF(t, rows.Close())
		}()

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
	})
}

func TestTimestampConversionWithoutArrowBatches(t *testing.T) {
	timestamps := [3]string{
		"2000-10-10 10:10:10.123456789", // neutral
		"9999-12-12 23:59:59.999999999", // max
		"0001-01-01 00:00:00.000000000"} // min
	types := [3]string{"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}

	runDBTest(t, func(sct *DBTest) {
		ctx := context.Background()

		for _, tsStr := range timestamps {
			ts, err := time.Parse("2006-01-02 15:04:05", tsStr)
			if err != nil {
				t.Fatalf("failed to parse time: %v", err)
			}
			for _, tp := range types {
				for scale := 0; scale <= 9; scale++ {
					t.Run(tp+"("+strconv.Itoa(scale)+")_"+tsStr, func(t *testing.T) {
						query := fmt.Sprintf("SELECT '%s'::%s(%v)", tsStr, tp, scale)
						rows := sct.mustQueryContext(ctx, query, nil)
						defer func() {
							assertNilF(t, rows.Close())
						}()

						if rows.Next() {
							var act time.Time
							assertNilF(t, rows.Scan(&act))
							exp := ts.Truncate(time.Duration(math.Pow10(9 - scale)))
							if !exp.Equal(act) {
								t.Fatalf("unexpected result. expected: %v, got: %v", exp, act)
							}
						} else {
							t.Fatalf("failed to run query: %v", query)
						}
					})
				}
			}
		}
	})
}

func TestTimestampConversionWithArrowBatchesNanosecondFailsForDistantDates(t *testing.T) {
	timestamps := [2]string{
		"9999-12-12 23:59:59.999999999", // max
		"0001-01-01 00:00:00.000000000"} // min
	types := [3]string{"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}

	expectedError := "Cannot convert timestamp"

	runSnowflakeConnTest(t, func(sct *SCTest) {
		ctx := WithArrowBatches(sct.sc.ctx)

		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx = WithArrowAllocator(ctx, pool)

		for _, tsStr := range timestamps {
			for _, tp := range types {
				for scale := 0; scale <= 9; scale++ {
					t.Run(tp+"("+strconv.Itoa(scale)+")_"+tsStr, func(t *testing.T) {

						query := fmt.Sprintf("SELECT '%s'::%s(%v)", tsStr, tp, scale)
						_, err := sct.sc.QueryContext(ctx, query, []driver.NamedValue{})
						if err != nil {
							if !strings.Contains(err.Error(), expectedError) {
								t.Fatalf("improper error, expected: %v, got: %v", expectedError, err.Error())
							}
						} else {
							t.Fatalf("no error, expected: %v ", expectedError)

						}
					})
				}
			}
		}
	})
}

// use arrow.Timestamp with microsecond precision and below should not encounter overflow issue.
func TestTimestampConversionWithArrowBatchesMicrosecondPassesForDistantDates(t *testing.T) {
	timestamps := [2]string{
		"9999-12-12 23:59:59.999999999", // max
		"0001-01-01 00:00:00.000000000"} // min
	types := [3]string{"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}

	runSnowflakeConnTest(t, func(sct *SCTest) {
		ctx := WithArrowBatchesTimestampOption(WithArrowBatches(sct.sc.ctx), UseMicrosecondTimestamp)

		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx = WithArrowAllocator(ctx, pool)

		for _, tsStr := range timestamps {
			for _, tp := range types {
				for scale := 0; scale <= 9; scale++ {
					t.Run(tp+"("+strconv.Itoa(scale)+")_"+tsStr, func(t *testing.T) {

						query := fmt.Sprintf("SELECT '%s'::%s(%v)", tsStr, tp, scale)
						rows, err := sct.sc.QueryContext(ctx, query, []driver.NamedValue{})
						if err != nil {
							t.Fatalf("failed to query: %v", err)
						}
						defer func() {
							assertNilF(t, rows.Close())
						}()

						// getting result batches
						batches, err := rows.(*snowflakeRows).GetArrowBatches()
						if err != nil {
							t.Error(err)
						}

						rec, err := batches[0].Fetch()
						if err != nil {
							t.Error(err)
						}

						records := *rec
						r := records[0]
						defer r.Release()
						actual := r.Column(0).(*array.Timestamp).TimestampValues()[0]
						actualYear := actual.ToTime(arrow.Microsecond).Year()

						ts, err := time.Parse("2006-01-02 15:04:05", tsStr)
						if err != nil {
							t.Fatalf("failed to parse time: %v", err)
						}
						exp := ts.Truncate(time.Duration(math.Pow10(9 - scale)))

						if actualYear != exp.Year() {
							t.Fatalf("unexpected year in timestamp, expected: %v, got: %v", exp.Year(), actualYear)
						}
					})
				}
			}
		}
	})
}

func TestTimestampConversionWithArrowBatchesAndWithOriginalTimestamp(t *testing.T) {
	timestamps := [3]string{
		"2000-10-10 10:10:10.123456789", // neutral
		"9999-12-12 23:59:59.999999999", // max
		"0001-01-01 00:00:00.000000000"} // min
	types := [3]string{"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}

	runSnowflakeConnTest(t, func(sct *SCTest) {
		ctx := WithArrowBatchesTimestampOption(WithArrowBatches(sct.sc.ctx), UseOriginalTimestamp)
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx = WithArrowAllocator(ctx, pool)

		for _, tsStr := range timestamps {
			ts, err := time.Parse("2006-01-02 15:04:05", tsStr)
			if err != nil {
				t.Fatalf("failed to parse time: %v", err)
			}
			for _, tp := range types {
				for scale := 0; scale <= 9; scale++ {
					t.Run(tp+"("+strconv.Itoa(scale)+")_"+tsStr, func(t *testing.T) {

						query := fmt.Sprintf("SELECT '%s'::%s(%v)", tsStr, tp, scale)
						rows := sct.mustQueryContext(ctx, query, []driver.NamedValue{})
						defer func() {
							assertNilF(t, rows.Close())
						}()

						// getting result batches
						batches, err := rows.(*snowflakeRows).GetArrowBatches()
						if err != nil {
							t.Error(err)
						}

						numBatches := len(batches)
						if numBatches != 1 {
							t.Errorf("incorrect number of batches, expected: 1, got: %v", numBatches)
						}

						rec, err := batches[0].Fetch()
						if err != nil {
							t.Error(err)
						}
						exp := ts.Truncate(time.Duration(math.Pow10(9 - scale)))
						for _, r := range *rec {
							defer r.Release()
							act := batches[0].ArrowSnowflakeTimestampToTime(r, 0, 0)
							if act == nil {
								t.Fatalf("unexpected result. expected: %v, got: nil", exp)
							} else if !exp.Equal(*act) {
								t.Fatalf("unexpected result. expected: %v, got: %v", exp, act)
							}
						}
					})
				}
			}
		}
	})
}

func TestTimeTypeValueToString(t *testing.T) {
	timeValue, err := time.Parse("2006-01-02 15:04:05", "2020-01-02 10:11:12")
	if err != nil {
		t.Fatal(err)
	}
	offsetTimeValue, err := time.ParseInLocation("2006-01-02 15:04:05", "2020-01-02 10:11:12", Location(6*60))
	if err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		in     time.Time
		tsmode snowflakeType
		out    string
	}{
		{timeValue, dateType, "1577959872000"},
		{timeValue, timeType, "36672000000000"},
		{timeValue, timestampNtzType, "1577959872000000000"},
		{timeValue, timestampLtzType, "1577959872000000000"},
		{timeValue, timestampTzType, "1577959872000000000 1440"},
		{offsetTimeValue, timestampTzType, "1577938272000000000 1800"},
	}

	for _, tc := range testcases {
		t.Run(tc.out, func(t *testing.T) {
			bv, err := timeTypeValueToString(tc.in, tc.tsmode)
			assertNilF(t, err)
			assertEmptyStringE(t, bv.format)
			assertNilE(t, bv.schema)
			assertEqualE(t, tc.out, *bv.value)
		})
	}
}

func TestIsArrayOfStructs(t *testing.T) {
	testcases := []struct {
		value    any
		expected bool
	}{
		{[]simpleObject{}, true},
		{[]*simpleObject{}, true},
		{[]int{1}, false},
		{[]string{"abc"}, false},
		{&[]bool{true}, false},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%v", tc.value), func(t *testing.T) {
			res := isArrayOfStructs(tc.value)
			if res != tc.expected {
				t.Errorf("expected %v to result in %v", tc.value, tc.expected)
			}
		})
	}
}

func TestSqlNull(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQuery("SELECT 1, NULL UNION SELECT 2, 'test' ORDER BY 1")
		defer rows.Close()
		var rowID int
		var nullStr sql.Null[string]
		assertTrueF(t, rows.Next())
		assertNilF(t, rows.Scan(&rowID, &nullStr))
		assertEqualE(t, nullStr, sql.Null[string]{Valid: false})
		assertTrueF(t, rows.Next())
		assertNilF(t, rows.Scan(&rowID, &nullStr))
		assertEqualE(t, nullStr, sql.Null[string]{Valid: true, V: "test"})
	})
}
