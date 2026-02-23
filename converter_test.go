package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/snowflakedb/gosnowflake/v2/internal/query"
	"github.com/snowflakedb/gosnowflake/v2/internal/types"
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
	_, err := valueToString(v, types.NullType, nil)
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

	bv, err := valueToString(localTime, types.TimestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedUnixTime)

	bv, err = valueToString(utcTime, types.TimestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedUnixTime)

	bv, err = valueToString(sql.NullBool{Bool: true, Valid: true}, types.TimestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedBool)

	bv, err = valueToString(sql.NullInt64{Int64: 1, Valid: true}, types.TimestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedInt64)

	bv, err = valueToString(sql.NullFloat64{Float64: 1.1, Valid: true}, types.TimestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedFloat64)

	bv, err = valueToString(sql.NullString{String: "teststring", Valid: true}, types.TimestampLtzType, nil)
	assertNilF(t, err)
	assertEmptyStringE(t, bv.format)
	assertNilE(t, bv.schema)
	assertEqualE(t, *bv.value, expectedString)

	t.Run("SQL Time", func(t *testing.T) {
		bv, err := valueToString(sql.NullTime{Time: localTime, Valid: true}, types.TimestampLtzType, nil)
		assertNilF(t, err)
		assertEmptyStringE(t, bv.format)
		assertNilE(t, bv.schema)
		assertEqualE(t, *bv.value, expectedUnixTime)
	})

	t.Run("arrays", func(t *testing.T) {
		bv, err := valueToString([2]int{1, 2}, types.ObjectType, nil)
		assertNilF(t, err)
		assertEqualE(t, bv.format, jsonFormatStr)
		assertEqualE(t, *bv.value, "[1,2]")
	})
	t.Run("slices", func(t *testing.T) {
		bv, err := valueToString([]int{1, 2}, types.ObjectType, nil)
		assertNilF(t, err)
		assertEqualE(t, bv.format, jsonFormatStr)
		assertEqualE(t, *bv.value, "[1,2]")
	})

	t.Run("UUID - should return string", func(t *testing.T) {
		u := NewUUID()
		bv, err := valueToString(u, types.TextType, nil)
		assertNilF(t, err)
		assertEmptyStringE(t, bv.format)
		assertEqualE(t, *bv.value, u.String())
	})

	t.Run("database/sql/driver - Valuer interface", func(t *testing.T) {
		u := newTestUUID()
		bv, err := valueToString(u, types.TextType, nil)
		assertNilF(t, err)
		assertEmptyStringE(t, bv.format)
		assertEqualE(t, *bv.value, u.String())
	})

	t.Run("testUUID", func(t *testing.T) {
		u := newTestUUID()
		assertEqualE(t, u.String(), parseTestUUID(u.String()).String())

		bv, err := valueToString(u, types.TextType, nil)
		assertNilF(t, err)
		assertEmptyStringE(t, bv.format)
		assertEqualE(t, *bv.value, u.String())
	})

	bv, err = valueToString(&testValueToStringStructuredObject{s: "some string", i: 123, date: time.Date(2024, time.May, 24, 0, 0, 0, 0, time.UTC)}, types.TimestampLtzType, params)
	assertNilF(t, err)
	assertEqualE(t, bv.format, jsonFormatStr)
	assertDeepEqualE(t, *bv.schema, bindingSchema{
		Typ:      "object",
		Nullable: true,
		Fields: []query.FieldMetadata{
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
	var rowType *query.ExecResponseRowType
	source = "abcdefg"

	types := []string{
		"date", "time", "timestamp_ntz", "timestamp_ltz", "timestamp_tz", "binary",
	}

	for _, tt := range types {
		t.Run(tt, func(t *testing.T) {
			rowType = &query.ExecResponseRowType{
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
				rowType = &query.ExecResponseRowType{
					Type: tt,
				}
				if err = stringToValue(context.Background(), &dest, *rowType, &ss, nil, nil); err == nil {
					t.Errorf("should raise error. type: %v, value:%v", tt, source)
				}
			})
		}
	}

	src := "1549491451.123456789"
	if err = stringToValue(context.Background(), &dest, query.ExecResponseRowType{Type: "timestamp_ltz"}, &src, nil, nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if ts, ok := dest.(time.Time); !ok {
		t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
	} else if ts.UnixNano() != 1549491451123456789 {
		t.Errorf("expected unix timestamp: 1549491451123456789, got %v", ts.UnixNano())
	}
}

type tcArrayToString struct {
	in  driver.NamedValue
	typ types.SnowflakeType
	out []string
}

func TestArrayToString(t *testing.T) {
	testcases := []tcArrayToString{
		{in: driver.NamedValue{Value: &intArray{1, 2}}, typ: types.FixedType, out: []string{"1", "2"}},
		{in: driver.NamedValue{Value: &int32Array{1, 2}}, typ: types.FixedType, out: []string{"1", "2"}},
		{in: driver.NamedValue{Value: &int64Array{3, 4, 5}}, typ: types.FixedType, out: []string{"3", "4", "5"}},
		{in: driver.NamedValue{Value: &float64Array{6.7}}, typ: types.RealType, out: []string{"6.7"}},
		{in: driver.NamedValue{Value: &float32Array{1.5}}, typ: types.RealType, out: []string{"1.5"}},
		{in: driver.NamedValue{Value: &boolArray{true, false}}, typ: types.BooleanType, out: []string{"true", "false"}},
		{in: driver.NamedValue{Value: &stringArray{"foo", "bar", "baz"}}, typ: types.TextType, out: []string{"foo", "bar", "baz"}},
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
		rowType         query.ExecResponseRowType
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
			rowType:  query.ExecResponseRowType{Scale: 5},
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
			rowType:  query.ExecResponseRowType{Scale: 5},
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
			rowType:  query.ExecResponseRowType{Scale: 37},
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
			rowType:  query.ExecResponseRowType{Scale: 4},
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
			rowType:  query.ExecResponseRowType{Scale: 4},
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
			rowType:  query.ExecResponseRowType{Scale: 5},
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
			rowType:  query.ExecResponseRowType{Scale: 5},
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
			rowType: query.ExecResponseRowType{Scale: 9},
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
			rowType: query.ExecResponseRowType{Scale: 9},
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
			rowType: query.ExecResponseRowType{Scale: 9},
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

// TestArrowToRecord has been moved to arrowbatches/converter_test.go
// (all test case data removed from this file)

func TestTimestampLTZLocation(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		src := "1549491451.123456789"
		var dest driver.Value
		loc, _ := time.LoadLocation(PSTLocation)
		if err := stringToValue(context.Background(), &dest, query.ExecResponseRowType{Type: "timestamp_ltz"}, &src, loc, nil); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		ts, ok := dest.(time.Time)
		if !ok {
			t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
		}
		if ts.Location() != loc {
			t.Errorf("expected location to be %v, got '%v'", loc, ts.Location())
		}

		if err := stringToValue(context.Background(), &dest, query.ExecResponseRowType{Type: "timestamp_ltz"}, &src, nil, nil); err != nil {
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
		tsmode types.SnowflakeType
		out    string
	}{
		{timeValue, types.DateType, "1577959872000"},
		{timeValue, types.TimeType, "36672000000000"},
		{timeValue, types.TimestampNtzType, "1577959872000000000"},
		{timeValue, types.TimestampLtzType, "1577959872000000000"},
		{timeValue, types.TimestampTzType, "1577959872000000000 1440"},
		{offsetTimeValue, types.TimestampTzType, "1577938272000000000 1800"},
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

func TestNumbersScanType(t *testing.T) {
	for _, forceFormat := range []string{forceJSON, forceARROW} {
		t.Run(forceFormat, func(t *testing.T) {
			runDBTest(t, func(dbt *DBTest) {
				dbt.mustExecT(t, forceFormat)

				t.Run("scale == 0", func(t *testing.T) {
					t.Run("without higher precision", func(t *testing.T) {
						rows := dbt.mustQueryContext(context.Background(), "SELECT 1, 300::NUMBER(15, 0), 600::NUMBER(18, 0), 700::NUMBER(19, 0), 900::NUMBER(38, 0), 123456789012345678901234567890")
						defer rows.Close()
						rows.mustNext()
						var i1, i2, i3 int64
						var i4, i5, i6 string
						rows.mustScan(&i1, &i2, &i3, &i4, &i5, &i6)
						assertEqualE(t, i1, int64(1))
						assertEqualE(t, i2, int64(300))
						assertEqualE(t, i3, int64(600))
						assertEqualE(t, i4, "700")
						assertEqualE(t, i5, "900")
						assertEqualE(t, i6, "123456789012345678901234567890") // pragma: allowlist secret

						types, err := rows.ColumnTypes()
						assertNilF(t, err)
						assertEqualE(t, types[0].ScanType(), reflect.TypeOf(int64(1)))
						assertEqualE(t, types[1].ScanType(), reflect.TypeOf(int64(1)))
						assertEqualE(t, types[2].ScanType(), reflect.TypeOf(int64(1)))
						assertEqualE(t, types[3].ScanType(), reflect.TypeOf(""))
						assertEqualE(t, types[4].ScanType(), reflect.TypeOf(""))
						assertEqualE(t, types[5].ScanType(), reflect.TypeOf(""))
					})

					t.Run("without higher precision - regardless of scan type, int parsing should still work", func(t *testing.T) {
						rows := dbt.mustQueryContext(context.Background(), "SELECT 1, 300::NUMBER(15, 0), 600::NUMBER(18, 0), 700::NUMBER(19, 0), 900::NUMBER(38, 0), 123456789012345678901234567890")
						defer rows.Close()
						rows.mustNext()
						var i1, i2, i3, i4, i5 int64
						var i6 string
						rows.mustScan(&i1, &i2, &i3, &i4, &i5, &i6)
						assertEqualE(t, i1, int64(1))
						assertEqualE(t, i2, int64(300))
						assertEqualE(t, i3, int64(600))
						assertEqualE(t, i4, int64(700))
						assertEqualE(t, i5, int64(900))
						assertEqualE(t, i6, "123456789012345678901234567890") // pragma: allowlist secret

						types, err := rows.ColumnTypes()
						assertNilF(t, err)
						assertEqualE(t, types[0].ScanType(), reflect.TypeOf(int64(1)))
						assertEqualE(t, types[1].ScanType(), reflect.TypeOf(int64(1)))
						assertEqualE(t, types[2].ScanType(), reflect.TypeOf(int64(1)))
						assertEqualE(t, types[3].ScanType(), reflect.TypeOf(""))
						assertEqualE(t, types[4].ScanType(), reflect.TypeOf(""))
						assertEqualE(t, types[5].ScanType(), reflect.TypeOf(""))
					})

					t.Run("with higher precision", func(t *testing.T) {
						rows := dbt.mustQueryContext(WithHigherPrecision(context.Background()), "SELECT 1::NUMBER(1, 0), 300::NUMBER(15, 0), 600::NUMBER(19, 0), 700::NUMBER(20, 0), 900::NUMBER(38, 0), 123456789012345678901234567890")
						defer rows.Close()
						rows.mustNext()
						var i1, i2 int64
						var i3, i4, i5, i6 *big.Int
						rows.mustScan(&i1, &i2, &i3, &i4, &i5, &i6)
						assertEqualE(t, i1, int64(1))
						assertEqualE(t, i2, int64(300))
						assertEqualE(t, i3.Cmp(big.NewInt(600)), 0)
						assertEqualE(t, i4.Cmp(big.NewInt(700)), 0)
						assertEqualE(t, i5.Cmp(big.NewInt(900)), 0)
						bigInt123456789012345678901234567890 := &big.Int{}
						bigInt123456789012345678901234567890.SetString("123456789012345678901234567890", 10) // pragma: allowlist secret
						assertEqualE(t, i6.Cmp(bigInt123456789012345678901234567890), 0)

						types, err := rows.ColumnTypes()
						assertNilF(t, err)
						assertEqualE(t, types[0].ScanType(), reflect.TypeOf(int64(1)))
						assertEqualE(t, types[1].ScanType(), reflect.TypeOf(int64(1)))
						assertEqualE(t, types[2].ScanType(), reflect.TypeOf(&big.Int{}))
						assertEqualE(t, types[3].ScanType(), reflect.TypeOf(&big.Int{}))
						assertEqualE(t, types[4].ScanType(), reflect.TypeOf(&big.Int{}))
						assertEqualE(t, types[5].ScanType(), reflect.TypeOf(&big.Int{}))
					})
				})

				t.Run("scale != 0", func(t *testing.T) {
					t.Run("without higher precision", func(t *testing.T) {
						rows := dbt.mustQueryContext(context.Background(), "SELECT 1.5, 300.5::NUMBER(15, 1), 600.5::NUMBER(18, 1), 700.5::NUMBER(19, 1), 900.5::NUMBER(38, 1), 123456789012345678901234567890.5")
						defer rows.Close()
						rows.mustNext()
						var i1, i2, i3, i4, i5, i6 float64
						rows.mustScan(&i1, &i2, &i3, &i4, &i5, &i6)
						assertEqualE(t, i1, 1.5)
						assertEqualE(t, i2, 300.5)
						assertEqualE(t, i3, 600.5)
						assertEqualE(t, i4, 700.5)
						assertEqualE(t, i5, 900.5)
						assertEqualE(t, i6, 123456789012345678901234567890.5)

						types, err := rows.ColumnTypes()
						assertNilF(t, err)
						assertEqualE(t, types[0].ScanType(), reflect.TypeOf(1.5))
						assertEqualE(t, types[1].ScanType(), reflect.TypeOf(1.5))
						assertEqualE(t, types[2].ScanType(), reflect.TypeOf(1.5))
						assertEqualE(t, types[3].ScanType(), reflect.TypeOf(1.5))
						assertEqualE(t, types[4].ScanType(), reflect.TypeOf(1.5))
						assertEqualE(t, types[5].ScanType(), reflect.TypeOf(1.5))
					})

					t.Run("with higher precision", func(t *testing.T) {
						rows := dbt.mustQueryContext(WithHigherPrecision(context.Background()), "SELECT 1.5, 300.5::NUMBER(15, 1), 600.5::NUMBER(18, 1), 700.5::NUMBER(19, 1), 900.5::NUMBER(38, 1), 123456789012345678901234567890.5")
						defer rows.Close()
						rows.mustNext()
						var i1, i2, i3, i4, i5, i6 *big.Float
						rows.mustScan(&i1, &i2, &i3, &i4, &i5, &i6)
						assertEqualE(t, i1.Cmp(big.NewFloat(1.5)), 0)
						assertEqualE(t, i2.Cmp(big.NewFloat(300.5)), 0)
						assertEqualE(t, i3.Cmp(big.NewFloat(600.5)), 0)
						assertEqualE(t, i4.Cmp(big.NewFloat(700.5)), 0)
						assertEqualE(t, i5.Cmp(big.NewFloat(900.5)), 0)
						bigInt123456789012345678901234567890, _, err := big.ParseFloat("123456789012345678901234567890.5", 10, numberMaxPrecisionInBits, big.AwayFromZero)
						assertNilF(t, err)
						assertEqualE(t, i6.Cmp(bigInt123456789012345678901234567890), 0)

						types, err := rows.ColumnTypes()
						assertNilF(t, err)
						assertEqualE(t, types[0].ScanType(), reflect.TypeOf(&big.Float{}))
						assertEqualE(t, types[1].ScanType(), reflect.TypeOf(&big.Float{}))
						assertEqualE(t, types[2].ScanType(), reflect.TypeOf(&big.Float{}))
						assertEqualE(t, types[3].ScanType(), reflect.TypeOf(&big.Float{}))
						assertEqualE(t, types[4].ScanType(), reflect.TypeOf(&big.Float{}))
						assertEqualE(t, types[5].ScanType(), reflect.TypeOf(&big.Float{}))
					})
				})
			})
		})
	}
}

func mustArray(v interface{}, typ ...any) driver.Value {
	array, err := Array(v, typ...)
	if err != nil {
		panic(fmt.Sprintf("failed to convert to array: %v", err))
	}
	return array
}
