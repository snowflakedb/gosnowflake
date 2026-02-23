package arrowbatches

import (
	"github.com/snowflakedb/gosnowflake/v2/internal/query"
	"github.com/snowflakedb/gosnowflake/v2/internal/types"
	"time"

	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"

	"github.com/apache/arrow-go/v18/arrow"
)

func recordToSchema(sc *arrow.Schema, rowType []query.ExecResponseRowType, loc *time.Location, timestampOption ia.TimestampOption, withHigherPrecision bool) (*arrow.Schema, error) {
	fields := recordToSchemaRecursive(sc.Fields(), rowType, loc, timestampOption, withHigherPrecision)
	meta := sc.Metadata()
	return arrow.NewSchema(fields, &meta), nil
}

func recordToSchemaRecursive(inFields []arrow.Field, rowType []query.ExecResponseRowType, loc *time.Location, timestampOption ia.TimestampOption, withHigherPrecision bool) []arrow.Field {
	var outFields []arrow.Field
	for i, f := range inFields {
		fieldMetadata := rowType[i].ToFieldMetadata()
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

func recordToSchemaSingleField(fieldMetadata query.FieldMetadata, f arrow.Field, withHigherPrecision bool, timestampOption ia.TimestampOption, loc *time.Location) (bool, arrow.DataType) {
	t := f.Type
	converted := true
	switch types.GetSnowflakeType(fieldMetadata.Type) {
	case types.FixedType:
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
	case types.TimeType:
		t = &arrow.Time64Type{Unit: arrow.Nanosecond}
	case types.TimestampNtzType, types.TimestampTzType:
		switch timestampOption {
		case ia.UseOriginalTimestamp:
			converted = false
		case ia.UseMicrosecondTimestamp:
			t = &arrow.TimestampType{Unit: arrow.Microsecond}
		case ia.UseMillisecondTimestamp:
			t = &arrow.TimestampType{Unit: arrow.Millisecond}
		case ia.UseSecondTimestamp:
			t = &arrow.TimestampType{Unit: arrow.Second}
		default:
			t = &arrow.TimestampType{Unit: arrow.Nanosecond}
		}
	case types.TimestampLtzType:
		switch timestampOption {
		case ia.UseOriginalTimestamp:
			converted = false
		case ia.UseMicrosecondTimestamp:
			t = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: loc.String()}
		case ia.UseMillisecondTimestamp:
			t = &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: loc.String()}
		case ia.UseSecondTimestamp:
			t = &arrow.TimestampType{Unit: arrow.Second, TimeZone: loc.String()}
		default:
			t = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
		}
	case types.ObjectType:
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
	case types.ArrayType:
		if _, ok := f.Type.(*arrow.ListType); ok {
			converted, dataType := recordToSchemaSingleField(fieldMetadata.Fields[0], f.Type.(*arrow.ListType).ElemField(), withHigherPrecision, timestampOption, loc)
			if converted {
				t = arrow.ListOf(dataType)
			}
		} else {
			t = f.Type
		}
	case types.MapType:
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
