package arrowbatches

import (
	"context"
	"fmt"
	"github.com/snowflakedb/gosnowflake/v2/internal/query"
	"github.com/snowflakedb/gosnowflake/v2/internal/types"
	"math/big"
	"strings"
	"testing"
	"time"

	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var decimalShift = new(big.Int).Exp(big.NewInt(2), big.NewInt(64), nil)

func stringIntToDecimal(src string) (decimal128.Num, bool) {
	b, ok := new(big.Int).SetString(src, 10)
	if !ok {
		return decimal128.Num{}, ok
	}
	var high, low big.Int
	high.QuoRem(b, decimalShift, &low)
	return decimal128.New(high.Int64(), low.Uint64()), true
}

func decimalToBigInt(num decimal128.Num) *big.Int {
	high := new(big.Int).SetInt64(num.HighBits())
	low := new(big.Int).SetUint64(num.LowBits())
	return new(big.Int).Add(new(big.Int).Mul(high, decimalShift), low)
}

func TestArrowToRecord(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	var valids []bool

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
		rowType                          query.ExecResponseRowType
		values                           interface{}
		expected                         interface{}
		error                            string
		arrowBatchesTimestampOption      ia.TimestampOption
		enableArrowBatchesUtf8Validation bool
		withHigherPrecision              bool
		nrows                            int
		builder                          array.Builder
		append                           func(b array.Builder, vs interface{})
		compare                          func(src interface{}, expected interface{}, rec arrow.Record) int
	}{
		{
			logical:  "fixed",
			physical: "number",
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
			rowType:  query.ExecResponseRowType{Scale: 37},
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
			rowType:             query.ExecResponseRowType{Scale: 37},
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
			rowType:  query.ExecResponseRowType{Scale: 1},
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
			rowType:             query.ExecResponseRowType{Scale: 1},
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
			rowType:  query.ExecResponseRowType{Scale: 1},
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
			rowType:             query.ExecResponseRowType{Scale: 1},
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
			rowType:  query.ExecResponseRowType{Scale: 2},
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
			rowType:             query.ExecResponseRowType{Scale: 2},
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
			rowType:  query.ExecResponseRowType{Scale: 5},
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
			rowType:             query.ExecResponseRowType{Scale: 5},
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
			rowType:                          query.ExecResponseRowType{Type: "TEXT"},
			values:                           []string{"\xFF", "bar", "baz\xFF\xFF"},
			expected:                         []string{"�", "bar", "baz��"},
			enableArrowBatchesUtf8Validation: true,
			nrows:                            2,
			builder:                          array.NewStringBuilder(pool),
			append:                           func(b array.Builder, vs interface{}) { b.(*array.StringBuilder).AppendValues(vs.([]string), valids) },
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				arr := convertedRec.Column(0).(*array.String)
				for i := 0; i < arr.Len(); i++ {
					if expected.([]string)[i] != arr.Value(i) {
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
			physical: "int64",
			values:   []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			nrows:    2,
			rowType:  query.ExecResponseRowType{Scale: 3},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli())
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
			physical: "struct",
			values:   []time.Time{time.Now(), localTime},
			nrows:    2,
			rowType:  query.ExecResponseRowType{Scale: 9},
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
		{
			logical:                     "timestamp_ntz",
			physical:                    "struct",
			values:                      []time.Time{time.Now().Truncate(time.Microsecond), localTime.Truncate(time.Microsecond)},
			arrowBatchesTimestampOption: ia.UseMicrosecondTimestamp,
			nrows:                       2,
			rowType:                     query.ExecResponseRowType{Scale: 9},
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
		{
			logical:                     "timestamp_ntz",
			physical:                    "struct",
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: ia.UseMillisecondTimestamp,
			nrows:                       2,
			rowType:                     query.ExecResponseRowType{Scale: 9},
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
		{
			logical:                     "timestamp_ntz",
			physical:                    "struct",
			values:                      []time.Time{time.Now().Truncate(time.Second), localTime.Truncate(time.Second)},
			arrowBatchesTimestampOption: ia.UseSecondTimestamp,
			nrows:                       2,
			rowType:                     query.ExecResponseRowType{Scale: 9},
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
			rowType:  query.ExecResponseRowType{Scale: 3},
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
			physical:                    "int64 with original timestamp",
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond), localTimeFarIntoFuture.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: ia.UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     query.ExecResponseRowType{Scale: 3},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:                     array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli())
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i := 0; i < convertedRec.Column(0).Len(); i++ {
					ts := ArrowSnowflakeTimestampToTime(convertedRec.Column(0), types.GetSnowflakeType("timestamp_ntz"), 3, i, nil)
					if !srcvs[i].Equal(*ts) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:                     "timestamp_ntz",
			physical:                    "struct with original timestamp",
			values:                      []time.Time{time.Now(), localTime, localTimeFarIntoFuture},
			arrowBatchesTimestampOption: ia.UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     query.ExecResponseRowType{Scale: 9},
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
					ts := ArrowSnowflakeTimestampToTime(convertedRec.Column(0), types.GetSnowflakeType("timestamp_ntz"), 9, i, nil)
					if !srcvs[i].Equal(*ts) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_ltz",
			physical: "int64",
			values:   []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			nrows:    2,
			rowType:  query.ExecResponseRowType{Scale: 3},
			sc:       arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:  array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli())
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
			physical: "struct",
			values:   []time.Time{time.Now(), localTime},
			nrows:    2,
			rowType:  query.ExecResponseRowType{Scale: 9},
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
		{
			logical:                     "timestamp_ltz",
			physical:                    "struct",
			values:                      []time.Time{time.Now().Truncate(time.Microsecond), localTime.Truncate(time.Microsecond)},
			arrowBatchesTimestampOption: ia.UseMicrosecondTimestamp,
			nrows:                       2,
			rowType:                     query.ExecResponseRowType{Scale: 9},
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
		{
			logical:                     "timestamp_ltz",
			physical:                    "struct",
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: ia.UseMillisecondTimestamp,
			nrows:                       2,
			rowType:                     query.ExecResponseRowType{Scale: 9},
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
		{
			logical:                     "timestamp_ltz",
			physical:                    "struct",
			values:                      []time.Time{time.Now().Truncate(time.Second), localTime.Truncate(time.Second)},
			arrowBatchesTimestampOption: ia.UseSecondTimestamp,
			nrows:                       2,
			rowType:                     query.ExecResponseRowType{Scale: 9},
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
			rowType:  query.ExecResponseRowType{Scale: 3},
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
			logical:                     "timestamp_ltz",
			physical:                    "int64 with original timestamp",
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond), localTimeFarIntoFuture.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: ia.UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     query.ExecResponseRowType{Scale: 3},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: &arrow.Int64Type{}}}, nil),
			builder:                     array.NewInt64Builder(pool),
			append: func(b array.Builder, vs interface{}) {
				for _, t := range vs.([]time.Time) {
					b.(*array.Int64Builder).Append(t.UnixMilli())
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i := 0; i < convertedRec.Column(0).Len(); i++ {
					ts := ArrowSnowflakeTimestampToTime(convertedRec.Column(0), types.GetSnowflakeType("timestamp_ltz"), 3, i, localTime.Location())
					if !srcvs[i].Equal(*ts) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:                     "timestamp_ltz",
			physical:                    "struct with original timestamp",
			values:                      []time.Time{time.Now(), localTime, localTimeFarIntoFuture},
			arrowBatchesTimestampOption: ia.UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     query.ExecResponseRowType{Scale: 9},
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
					ts := ArrowSnowflakeTimestampToTime(convertedRec.Column(0), types.GetSnowflakeType("timestamp_ltz"), 9, i, localTime.Location())
					if !srcvs[i].Equal(*ts) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:  "timestamp_tz",
			physical: "struct2",
			values:   []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			nrows:    2,
			rowType:  query.ExecResponseRowType{Scale: 3},
			sc:       arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithoutFraction}}, nil),
			builder:  array.NewStructBuilder(pool, timestampTzStructWithoutFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.UnixMilli())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(0))
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
			physical: "struct3",
			values:   []time.Time{time.Now(), localTime},
			nrows:    2,
			rowType:  query.ExecResponseRowType{Scale: 9},
			sc:       arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithFraction}}, nil),
			builder:  array.NewStructBuilder(pool, timestampTzStructWithFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
					sb.FieldBuilder(2).(*array.Int32Builder).Append(int32(0))
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
			logical:                     "timestamp_tz",
			physical:                    "struct3",
			values:                      []time.Time{time.Now().Truncate(time.Microsecond), localTime.Truncate(time.Microsecond)},
			arrowBatchesTimestampOption: ia.UseMicrosecondTimestamp,
			nrows:                       2,
			rowType:                     query.ExecResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithFraction}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampTzStructWithFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
					sb.FieldBuilder(2).(*array.Int32Builder).Append(int32(0))
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
		{
			logical:                     "timestamp_tz",
			physical:                    "struct3",
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: ia.UseMillisecondTimestamp,
			nrows:                       2,
			rowType:                     query.ExecResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithFraction}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampTzStructWithFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
					sb.FieldBuilder(2).(*array.Int32Builder).Append(int32(0))
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
		{
			logical:                     "timestamp_tz",
			physical:                    "struct3",
			values:                      []time.Time{time.Now().Truncate(time.Second), localTime.Truncate(time.Second)},
			arrowBatchesTimestampOption: ia.UseSecondTimestamp,
			nrows:                       2,
			rowType:                     query.ExecResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithFraction}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampTzStructWithFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
					sb.FieldBuilder(2).(*array.Int32Builder).Append(int32(0))
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
			physical:                    "struct2 with original timestamp",
			values:                      []time.Time{time.Now().Truncate(time.Millisecond), localTime.Truncate(time.Millisecond), localTimeFarIntoFuture.Truncate(time.Millisecond)},
			arrowBatchesTimestampOption: ia.UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     query.ExecResponseRowType{Scale: 3},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithoutFraction}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampTzStructWithoutFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.UnixMilli())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(0))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i := 0; i < convertedRec.Column(0).Len(); i++ {
					ts := ArrowSnowflakeTimestampToTime(convertedRec.Column(0), types.GetSnowflakeType("timestamp_tz"), 3, i, nil)
					if !srcvs[i].Equal(*ts) {
						return i
					}
				}
				return -1
			},
		},
		{
			logical:                     "timestamp_tz",
			physical:                    "struct3 with original timestamp",
			values:                      []time.Time{time.Now(), localTime, localTimeFarIntoFuture},
			arrowBatchesTimestampOption: ia.UseOriginalTimestamp,
			nrows:                       3,
			rowType:                     query.ExecResponseRowType{Scale: 9},
			sc:                          arrow.NewSchema([]arrow.Field{{Type: timestampTzStructWithFraction}}, nil),
			builder:                     array.NewStructBuilder(pool, timestampTzStructWithFraction),
			append: func(b array.Builder, vs interface{}) {
				sb := b.(*array.StructBuilder)
				valids = []bool{true, true, true}
				sb.AppendValues(valids)
				for _, t := range vs.([]time.Time) {
					sb.FieldBuilder(0).(*array.Int64Builder).Append(t.Unix())
					sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(t.Nanosecond()))
					sb.FieldBuilder(2).(*array.Int32Builder).Append(int32(0))
				}
			},
			compare: func(src interface{}, expected interface{}, convertedRec arrow.Record) int {
				srcvs := src.([]time.Time)
				for i := 0; i < convertedRec.Column(0).Len(); i++ {
					ts := ArrowSnowflakeTimestampToTime(convertedRec.Column(0), types.GetSnowflakeType("timestamp_tz"), 9, i, nil)
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
			case ia.UseOriginalTimestamp:
				ctx = ia.WithTimestampOption(ctx, ia.UseOriginalTimestamp)
			case ia.UseSecondTimestamp:
				ctx = ia.WithTimestampOption(ctx, ia.UseSecondTimestamp)
			case ia.UseMillisecondTimestamp:
				ctx = ia.WithTimestampOption(ctx, ia.UseMillisecondTimestamp)
			case ia.UseMicrosecondTimestamp:
				ctx = ia.WithTimestampOption(ctx, ia.UseMicrosecondTimestamp)
			default:
				ctx = ia.WithTimestampOption(ctx, ia.UseNanosecondTimestamp)
			}

			if tc.enableArrowBatchesUtf8Validation {
				ctx = ia.EnableUtf8Validation(ctx)
			}

			if tc.withHigherPrecision {
				ctx = ia.WithHigherPrecision(ctx)
			}

			transformedRec, err := arrowToRecord(ctx, rawRec, pool, []query.ExecResponseRowType{meta}, localTime.Location())
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
