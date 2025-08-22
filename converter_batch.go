//go:build !nobatch
// +build !nobatch

package gosnowflake

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type snowflakeArrowBatchesTimestampOption int

const (
	// UseNanosecondTimestamp uses arrow.Timestamp in nanosecond precision, could cause ErrTooHighTimestampPrecision if arrow.Timestamp cannot fit original timestamp values.
	UseNanosecondTimestamp snowflakeArrowBatchesTimestampOption = iota
	// UseMicrosecondTimestamp uses arrow.Timestamp in microsecond precision
	UseMicrosecondTimestamp
	// UseMillisecondTimestamp uses arrow.Timestamp in millisecond precision
	UseMillisecondTimestamp
	// UseSecondTimestamp uses arrow.Timestamp in second precision
	UseSecondTimestamp
	// UseOriginalTimestamp uses original timestamp struct returned by Snowflake. It can be used in case arrow.Timestamp cannot fit original timestamp values.
	UseOriginalTimestamp
)

// ArrowSnowflakeTimestampToTime converts original timestamp returned by Snowflake to time.Time
func (rb *ArrowBatch) ArrowSnowflakeTimestampToTime(rec arrow.Record, colIdx int, recIdx int) *time.Time {
	scale := int(rb.scd.RowSet.RowType[colIdx].Scale)
	dbType := rb.scd.RowSet.RowType[colIdx].Type
	return arrowSnowflakeTimestampToTime(rec.Column(colIdx), getSnowflakeType(dbType), scale, recIdx, rb.loc)
}

func arrowBatchesUtf8ValidationEnabled(ctx context.Context) bool {
	v := ctx.Value(enableArrowBatchesUtf8Validation)
	if v == nil {
		return false
	}
	d, ok := v.(bool)
	return ok && d
}

func getArrowBatchesTimestampOption(ctx context.Context) snowflakeArrowBatchesTimestampOption {
	v := ctx.Value(arrowBatchesTimestampOption)
	if v == nil {
		return UseNanosecondTimestamp
	}
	o, ok := v.(snowflakeArrowBatchesTimestampOption)
	if !ok {
		return UseNanosecondTimestamp
	}
	return o
}

func arrowToRecord(ctx context.Context, record arrow.Record, pool memory.Allocator, rowType []execResponseRowType, loc *time.Location) (arrow.Record, error) {
	arrowBatchesTimestampOption := getArrowBatchesTimestampOption(ctx)
	higherPrecisionEnabled := higherPrecisionEnabled(ctx)

	s, err := recordToSchema(record.Schema(), rowType, loc, arrowBatchesTimestampOption, higherPrecisionEnabled)
	if err != nil {
		return nil, err
	}

	var cols []arrow.Array
	numRows := record.NumRows()
	ctxAlloc := compute.WithAllocator(ctx, pool)

	for i, col := range record.Columns() {
		fieldMetadata := rowType[i].toFieldMetadata()

		newCol, err := arrowToRecordSingleColumn(ctxAlloc, s.Field(i), col, fieldMetadata, higherPrecisionEnabled, arrowBatchesTimestampOption, pool, loc, numRows)
		if err != nil {
			return nil, err
		}
		cols = append(cols, newCol)
		defer newCol.Release()
	}
	newRecord := array.NewRecord(s, cols, numRows)
	return newRecord, nil
}

func arrowToRecordSingleColumn(ctx context.Context, field arrow.Field, col arrow.Array, fieldMetadata fieldMetadata, higherPrecisionEnabled bool, timestampOption snowflakeArrowBatchesTimestampOption, pool memory.Allocator, loc *time.Location, numRows int64) (arrow.Array, error) {
	var err error
	newCol := col
	snowflakeType := getSnowflakeType(fieldMetadata.Type)
	switch snowflakeType {
	case fixedType:
		if higherPrecisionEnabled {
			// do nothing - return decimal as is
			col.Retain()
		} else if col.DataType().ID() == arrow.DECIMAL || col.DataType().ID() == arrow.DECIMAL256 {
			var toType arrow.DataType
			if fieldMetadata.Scale == 0 {
				toType = arrow.PrimitiveTypes.Int64
			} else {
				toType = arrow.PrimitiveTypes.Float64
			}
			// we're fine truncating so no error for data loss here.
			// so we use UnsafeCastOptions.
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
			// gosnowflake driver uses compute.Divide() which could bring `integer value not in range: -9007199254740992 to 9007199254740992` error
			// if we convert int64 to BigDecimal and then use compute.CastArray to convert BigDecimal to float64, we won't have enough precision.
			// e.g 0.1 as (38,19) will result 0.09999999999999999
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
	case timeType:
		newCol, err = compute.CastArray(ctx, col, compute.SafeCastOptions(arrow.FixedWidthTypes.Time64ns))
		if err != nil {
			return nil, err
		}
	case timestampNtzType, timestampLtzType, timestampTzType:
		if timestampOption == UseOriginalTimestamp {
			// do nothing - return timestamp as is
			col.Retain()
		} else {
			var unit arrow.TimeUnit
			switch timestampOption {
			case UseMicrosecondTimestamp:
				unit = arrow.Microsecond
			case UseMillisecondTimestamp:
				unit = arrow.Millisecond
			case UseSecondTimestamp:
				unit = arrow.Second
			case UseNanosecondTimestamp:
				unit = arrow.Nanosecond
			}
			var tb *array.TimestampBuilder
			if snowflakeType == timestampLtzType {
				tb = array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: unit, TimeZone: loc.String()})
			} else {
				tb = array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: unit})
			}
			defer tb.Release()

			for i := 0; i < int(numRows); i++ {
				ts := arrowSnowflakeTimestampToTime(col, snowflakeType, int(fieldMetadata.Scale), i, loc)
				if ts != nil {
					var ar arrow.Timestamp
					switch timestampOption {
					case UseMicrosecondTimestamp:
						ar = arrow.Timestamp(ts.UnixMicro())
					case UseMillisecondTimestamp:
						ar = arrow.Timestamp(ts.UnixMilli())
					case UseSecondTimestamp:
						ar = arrow.Timestamp(ts.Unix())
					case UseNanosecondTimestamp:
						ar = arrow.Timestamp(ts.UnixNano())
						// in case of overflow in arrow timestamp return error
						// this could only happen for nanosecond case
						if ts.UTC().Year() != ar.ToTime(arrow.Nanosecond).Year() {
							return nil, &SnowflakeError{
								Number:   ErrTooHighTimestampPrecision,
								SQLState: SQLStateInvalidDataTimeFormat,
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
	case textType:
		if stringCol, ok := col.(*array.String); ok {
			newCol = arrowStringRecordToColumn(ctx, stringCol, pool, numRows, fieldMetadata)
		}
	case objectType:
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
			newCol = arrowStringRecordToColumn(ctx, stringCol, pool, numRows, fieldMetadata)
		}
	case arrayType:
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
			newCol = arrowStringRecordToColumn(ctx, stringCol, pool, numRows, fieldMetadata)
		}
	case mapType:
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
			newCol = arrowStringRecordToColumn(ctx, stringCol, pool, numRows, fieldMetadata)
		}
	default:
		col.Retain()
	}
	return newCol, nil
}

// returns n arrow array which will be new and populated if we converted the array to valid utf8
// or if we didn't covnert it, it will return the original column.
func arrowStringRecordToColumn(
	ctx context.Context,
	stringCol *array.String,
	mem memory.Allocator,
	numRows int64,
	fieldMetadata fieldMetadata,
) arrow.Array {
	if arrowBatchesUtf8ValidationEnabled(ctx) && stringCol.DataType().ID() == arrow.STRING {
		tb := array.NewStringBuilder(mem)
		defer tb.Release()

		for i := 0; i < int(numRows); i++ {
			if stringCol.IsValid(i) {
				stringValue := stringCol.Value(i)
				if !utf8.ValidString(stringValue) {
					logger.WithContext(ctx).Error("Invalid UTF-8 characters detected while reading query response, column: ", fieldMetadata.Name)
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

func recordToSchema(sc *arrow.Schema, rowType []execResponseRowType, loc *time.Location, timestampOption snowflakeArrowBatchesTimestampOption, withHigherPrecision bool) (*arrow.Schema, error) {
	fields := recordToSchemaRecursive(sc.Fields(), rowType, loc, timestampOption, withHigherPrecision)
	meta := sc.Metadata()
	return arrow.NewSchema(fields, &meta), nil
}

func recordToSchemaRecursive(inFields []arrow.Field, rowType []execResponseRowType, loc *time.Location, timestampOption snowflakeArrowBatchesTimestampOption, withHigherPrecision bool) []arrow.Field {
	var outFields []arrow.Field
	for i, f := range inFields {
		fieldMetadata := rowType[i].toFieldMetadata()
		converted, t := recordToSchemaSingleField(fieldMetadata, f, withHigherPrecision, timestampOption, loc)

		newField := f
		if converted {
			newField = arrow.Field{
				Name:     f.Name,
				Type:     t,
				Nullable: f.Nullable,
				Metadata: f.Metadata,
			}
		}
		outFields = append(outFields, newField)
	}
	return outFields
}

func recordToSchemaSingleField(fieldMetadata fieldMetadata, f arrow.Field, withHigherPrecision bool, timestampOption snowflakeArrowBatchesTimestampOption, loc *time.Location) (bool, arrow.DataType) {
	t := f.Type
	converted := true
	switch getSnowflakeType(fieldMetadata.Type) {
	case fixedType:
		switch f.Type.ID() {
		case arrow.DECIMAL:
			if withHigherPrecision {
				converted = false
			} else if fieldMetadata.Scale == 0 {
				t = &arrow.Int64Type{}
			} else {
				t = &arrow.Float64Type{}
			}
		default:
			if withHigherPrecision {
				converted = false
			} else if fieldMetadata.Scale != 0 {
				t = &arrow.Float64Type{}
			} else {
				converted = false
			}
		}
	case timeType:
		t = &arrow.Time64Type{Unit: arrow.Nanosecond}
	case timestampNtzType, timestampTzType:
		if timestampOption == UseOriginalTimestamp {
			// do nothing - return timestamp as is
			converted = false
		} else if timestampOption == UseMicrosecondTimestamp {
			t = &arrow.TimestampType{Unit: arrow.Microsecond}
		} else if timestampOption == UseMillisecondTimestamp {
			t = &arrow.TimestampType{Unit: arrow.Millisecond}
		} else if timestampOption == UseSecondTimestamp {
			t = &arrow.TimestampType{Unit: arrow.Second}
		} else {
			t = &arrow.TimestampType{Unit: arrow.Nanosecond}
		}
	case timestampLtzType:
		if timestampOption == UseOriginalTimestamp {
			// do nothing - return timestamp as is
			converted = false
		} else if timestampOption == UseMicrosecondTimestamp {
			t = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: loc.String()}
		} else if timestampOption == UseMillisecondTimestamp {
			t = &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: loc.String()}
		} else if timestampOption == UseSecondTimestamp {
			t = &arrow.TimestampType{Unit: arrow.Second, TimeZone: loc.String()}
		} else {
			t = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
		}
	case objectType:
		converted = false
		if f.Type.ID() == arrow.STRUCT {
			var internalFields []arrow.Field
			for idx, internalField := range f.Type.(*arrow.StructType).Fields() {
				internalConverted, convertedDataType := recordToSchemaSingleField(fieldMetadata.Fields[idx], internalField, withHigherPrecision, timestampOption, loc)
				converted = converted || internalConverted
				if internalConverted {
					newInternalField := arrow.Field{
						Name:     internalField.Name,
						Type:     convertedDataType,
						Metadata: internalField.Metadata,
						Nullable: internalField.Nullable,
					}
					internalFields = append(internalFields, newInternalField)
				} else {
					internalFields = append(internalFields, internalField)
				}
			}
			t = arrow.StructOf(internalFields...)
		}
	case arrayType:
		if _, ok := f.Type.(*arrow.ListType); ok {
			converted, dataType := recordToSchemaSingleField(fieldMetadata.Fields[0], f.Type.(*arrow.ListType).ElemField(), withHigherPrecision, timestampOption, loc)
			if converted {
				t = arrow.ListOf(dataType)
			}
		} else {
			t = f.Type
		}
	case mapType:
		convertedKey, keyDataType := recordToSchemaSingleField(fieldMetadata.Fields[0], f.Type.(*arrow.MapType).KeyField(), withHigherPrecision, timestampOption, loc)
		convertedValue, valueDataType := recordToSchemaSingleField(fieldMetadata.Fields[1], f.Type.(*arrow.MapType).ItemField(), withHigherPrecision, timestampOption, loc)
		converted = convertedKey || convertedValue
		if converted {
			t = arrow.MapOf(keyDataType, valueDataType)
		}
	default:
		converted = false
	}
	return converted, t
}
