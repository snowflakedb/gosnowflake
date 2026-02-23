package arrowbatches

import (
	"context"
	"fmt"
	"github.com/snowflakedb/gosnowflake/v2/internal/query"
	"github.com/snowflakedb/gosnowflake/v2/internal/types"
	"math"
	"math/big"
	"strings"
	"time"
	"unicode/utf8"

	sf "github.com/snowflakedb/gosnowflake/v2"
	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// arrowToRecord transforms a raw arrow.Record from Snowflake into a record
// with standard Arrow types (e.g., converting struct-based timestamps to
// arrow.Timestamp, decimal128 to int64/float64, etc.)
func arrowToRecord(ctx context.Context, record arrow.Record, pool memory.Allocator, rowType []query.ExecResponseRowType, loc *time.Location) (arrow.Record, error) {
	timestampOption := ia.GetTimestampOption(ctx)
	higherPrecision := ia.HigherPrecisionEnabled(ctx)

	s, err := recordToSchema(record.Schema(), rowType, loc, timestampOption, higherPrecision)
	if err != nil {
		return nil, err
	}

	var cols []arrow.Array
	numRows := record.NumRows()
	ctxAlloc := compute.WithAllocator(ctx, pool)

	for i, col := range record.Columns() {
		fieldMetadata := rowType[i].ToFieldMetadata()

		newCol, err := arrowToRecordSingleColumn(ctxAlloc, s.Field(i), col, fieldMetadata, higherPrecision, timestampOption, pool, loc, numRows)
		if err != nil {
			return nil, err
		}
		cols = append(cols, newCol)
		defer newCol.Release()
	}
	newRecord := array.NewRecord(s, cols, numRows)
	return newRecord, nil
}

func arrowToRecordSingleColumn(ctx context.Context, field arrow.Field, col arrow.Array, fieldMetadata query.FieldMetadata, higherPrecisionEnabled bool, timestampOption ia.TimestampOption, pool memory.Allocator, loc *time.Location, numRows int64) (arrow.Array, error) {
	var err error
	newCol := col
	snowflakeType := types.GetSnowflakeType(fieldMetadata.Type)
	switch snowflakeType {
	case types.FixedType:
		if higherPrecisionEnabled {
			col.Retain()
		} else if col.DataType().ID() == arrow.DECIMAL || col.DataType().ID() == arrow.DECIMAL256 {
			var toType arrow.DataType
			if fieldMetadata.Scale == 0 {
				toType = arrow.PrimitiveTypes.Int64
			} else {
				toType = arrow.PrimitiveTypes.Float64
			}
			newCol, err = compute.CastArray(ctx, col, compute.UnsafeCastOptions(toType))
			if err != nil {
				return nil, err
			}
		} else if fieldMetadata.Scale != 0 && col.DataType().ID() != arrow.INT64 {
			result, err := compute.Divide(ctx, compute.ArithmeticOptions{NoCheckOverflow: true},
				&compute.ArrayDatum{Value: newCol.Data()},
				compute.NewDatum(math.Pow10(int(fieldMetadata.Scale))))
			if err != nil {
				return nil, err
			}
			defer result.Release()
			newCol = result.(*compute.ArrayDatum).MakeArray()
		} else if fieldMetadata.Scale != 0 && col.DataType().ID() == arrow.INT64 {
			values := col.(*array.Int64).Int64Values()
			floatValues := make([]float64, len(values))
			for i, val := range values {
				floatValues[i], _ = intToBigFloat(val, int64(fieldMetadata.Scale)).Float64()
			}
			builder := array.NewFloat64Builder(pool)
			builder.AppendValues(floatValues, nil)
			newCol = builder.NewArray()
			builder.Release()
		} else {
			col.Retain()
		}
	case types.TimeType:
		newCol, err = compute.CastArray(ctx, col, compute.SafeCastOptions(arrow.FixedWidthTypes.Time64ns))
		if err != nil {
			return nil, err
		}
	case types.TimestampNtzType, types.TimestampLtzType, types.TimestampTzType:
		if timestampOption == ia.UseOriginalTimestamp {
			col.Retain()
		} else {
			var unit arrow.TimeUnit
			switch timestampOption {
			case ia.UseMicrosecondTimestamp:
				unit = arrow.Microsecond
			case ia.UseMillisecondTimestamp:
				unit = arrow.Millisecond
			case ia.UseSecondTimestamp:
				unit = arrow.Second
			case ia.UseNanosecondTimestamp:
				unit = arrow.Nanosecond
			}
			var tb *array.TimestampBuilder
			if snowflakeType == types.TimestampLtzType {
				tb = array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: unit, TimeZone: loc.String()})
			} else {
				tb = array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: unit})
			}
			defer tb.Release()

			for i := 0; i < int(numRows); i++ {
				ts := ArrowSnowflakeTimestampToTime(col, snowflakeType, int(fieldMetadata.Scale), i, loc)
				if ts != nil {
					var ar arrow.Timestamp
					switch timestampOption {
					case ia.UseMicrosecondTimestamp:
						ar = arrow.Timestamp(ts.UnixMicro())
					case ia.UseMillisecondTimestamp:
						ar = arrow.Timestamp(ts.UnixMilli())
					case ia.UseSecondTimestamp:
						ar = arrow.Timestamp(ts.Unix())
					case ia.UseNanosecondTimestamp:
						ar = arrow.Timestamp(ts.UnixNano())
						if ts.UTC().Year() != ar.ToTime(arrow.Nanosecond).Year() {
							return nil, &sf.SnowflakeError{
								Number:   sf.ErrTooHighTimestampPrecision,
								SQLState: sf.SQLStateInvalidDataTimeFormat,
								Message:  fmt.Sprintf("Cannot convert timestamp %v in column %v to Arrow.Timestamp data type due to too high precision. Please use context with WithOriginalTimestamp.", ts.UTC(), fieldMetadata.Name),
							}
						}
					}
					tb.Append(ar)
				} else {
					tb.AppendNull()
				}
			}
			newCol = tb.NewArray()
		}
	case types.TextType:
		if stringCol, ok := col.(*array.String); ok {
			newCol = arrowStringRecordToColumn(ctx, stringCol, pool, numRows)
		}
	case types.ObjectType:
		if structCol, ok := col.(*array.Struct); ok {
			var internalCols []arrow.Array
			for i := 0; i < structCol.NumField(); i++ {
				internalCol := structCol.Field(i)
				newInternalCol, err := arrowToRecordSingleColumn(ctx, field.Type.(*arrow.StructType).Field(i), internalCol, fieldMetadata.Fields[i], higherPrecisionEnabled, timestampOption, pool, loc, numRows)
				if err != nil {
					return nil, err
				}
				internalCols = append(internalCols, newInternalCol)
				defer newInternalCol.Release()
			}
			var fieldNames []string
			for _, f := range field.Type.(*arrow.StructType).Fields() {
				fieldNames = append(fieldNames, f.Name)
			}
			nullBitmap := memory.NewBufferBytes(structCol.NullBitmapBytes())
			numberOfNulls := structCol.NullN()
			return array.NewStructArrayWithNulls(internalCols, fieldNames, nullBitmap, numberOfNulls, 0)
		} else if stringCol, ok := col.(*array.String); ok {
			newCol = arrowStringRecordToColumn(ctx, stringCol, pool, numRows)
		}
	case types.ArrayType:
		if listCol, ok := col.(*array.List); ok {
			newCol, err = arrowToRecordSingleColumn(ctx, field.Type.(*arrow.ListType).ElemField(), listCol.ListValues(), fieldMetadata.Fields[0], higherPrecisionEnabled, timestampOption, pool, loc, numRows)
			if err != nil {
				return nil, err
			}
			defer newCol.Release()
			newData := array.NewData(arrow.ListOf(newCol.DataType()), listCol.Len(), listCol.Data().Buffers(), []arrow.ArrayData{newCol.Data()}, listCol.NullN(), 0)
			defer newData.Release()
			return array.NewListData(newData), nil
		} else if stringCol, ok := col.(*array.String); ok {
			newCol = arrowStringRecordToColumn(ctx, stringCol, pool, numRows)
		}
	case types.MapType:
		if mapCol, ok := col.(*array.Map); ok {
			keyCol, err := arrowToRecordSingleColumn(ctx, field.Type.(*arrow.MapType).KeyField(), mapCol.Keys(), fieldMetadata.Fields[0], higherPrecisionEnabled, timestampOption, pool, loc, numRows)
			if err != nil {
				return nil, err
			}
			defer keyCol.Release()
			valueCol, err := arrowToRecordSingleColumn(ctx, field.Type.(*arrow.MapType).ItemField(), mapCol.Items(), fieldMetadata.Fields[1], higherPrecisionEnabled, timestampOption, pool, loc, numRows)
			if err != nil {
				return nil, err
			}
			defer valueCol.Release()

			structArr, err := array.NewStructArray([]arrow.Array{keyCol, valueCol}, []string{"k", "v"})
			if err != nil {
				return nil, err
			}
			defer structArr.Release()
			newData := array.NewData(arrow.MapOf(keyCol.DataType(), valueCol.DataType()), mapCol.Len(), mapCol.Data().Buffers(), []arrow.ArrayData{structArr.Data()}, mapCol.NullN(), 0)
			defer newData.Release()
			return array.NewMapData(newData), nil
		} else if stringCol, ok := col.(*array.String); ok {
			newCol = arrowStringRecordToColumn(ctx, stringCol, pool, numRows)
		}
	default:
		col.Retain()
	}
	return newCol, nil
}

func arrowStringRecordToColumn(
	ctx context.Context,
	stringCol *array.String,
	mem memory.Allocator,
	numRows int64,
) arrow.Array {
	if ia.Utf8ValidationEnabled(ctx) && stringCol.DataType().ID() == arrow.STRING {
		tb := array.NewStringBuilder(mem)
		defer tb.Release()

		for i := 0; i < int(numRows); i++ {
			if stringCol.IsValid(i) {
				stringValue := stringCol.Value(i)
				if !utf8.ValidString(stringValue) {
					stringValue = strings.ToValidUTF8(stringValue, "�")
				}
				tb.Append(stringValue)
			} else {
				tb.AppendNull()
			}
		}
		arr := tb.NewArray()
		return arr
	}
	stringCol.Retain()
	return stringCol
}

func intToBigFloat(val int64, scale int64) *big.Float {
	f := new(big.Float).SetInt64(val)
	s := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
	return new(big.Float).Quo(f, s)
}

// ArrowSnowflakeTimestampToTime converts original timestamp returned by Snowflake to time.Time.
func ArrowSnowflakeTimestampToTime(
	column arrow.Array,
	sfType types.SnowflakeType,
	scale int,
	recIdx int,
	loc *time.Location) *time.Time {

	if column.IsNull(recIdx) {
		return nil
	}
	var ret time.Time
	switch sfType {
	case types.TimestampNtzType:
		if column.DataType().ID() == arrow.STRUCT {
			structData := column.(*array.Struct)
			epoch := structData.Field(0).(*array.Int64).Int64Values()
			fraction := structData.Field(1).(*array.Int32).Int32Values()
			ret = time.Unix(epoch[recIdx], int64(fraction[recIdx])).UTC()
		} else {
			intData := column.(*array.Int64)
			value := intData.Value(recIdx)
			epoch := extractEpoch(value, scale)
			fraction := extractFraction(value, scale)
			ret = time.Unix(epoch, fraction).UTC()
		}
	case types.TimestampLtzType:
		if column.DataType().ID() == arrow.STRUCT {
			structData := column.(*array.Struct)
			epoch := structData.Field(0).(*array.Int64).Int64Values()
			fraction := structData.Field(1).(*array.Int32).Int32Values()
			ret = time.Unix(epoch[recIdx], int64(fraction[recIdx])).In(loc)
		} else {
			intData := column.(*array.Int64)
			value := intData.Value(recIdx)
			epoch := extractEpoch(value, scale)
			fraction := extractFraction(value, scale)
			ret = time.Unix(epoch, fraction).In(loc)
		}
	case types.TimestampTzType:
		structData := column.(*array.Struct)
		if structData.NumField() == 2 {
			value := structData.Field(0).(*array.Int64).Int64Values()
			timezone := structData.Field(1).(*array.Int32).Int32Values()
			epoch := extractEpoch(value[recIdx], scale)
			fraction := extractFraction(value[recIdx], scale)
			locTz := sf.Location(int(timezone[recIdx]) - 1440)
			ret = time.Unix(epoch, fraction).In(locTz)
		} else {
			epoch := structData.Field(0).(*array.Int64).Int64Values()
			fraction := structData.Field(1).(*array.Int32).Int32Values()
			timezone := structData.Field(2).(*array.Int32).Int32Values()
			locTz := sf.Location(int(timezone[recIdx]) - 1440)
			ret = time.Unix(epoch[recIdx], int64(fraction[recIdx])).In(locTz)
		}
	}
	return &ret
}

func extractEpoch(value int64, scale int) int64 {
	return value / int64(math.Pow10(scale))
}

func extractFraction(value int64, scale int) int64 {
	return (value % int64(math.Pow10(scale))) * int64(math.Pow10(9-scale))
}
