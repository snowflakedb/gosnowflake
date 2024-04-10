package gosnowflake

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"
)

// ObjectType Empty marker of an object used in column type ScanType function
type ObjectType struct {
}

// StructuredObject representation of structured type.
type StructuredObject interface {
	GetString(fieldName string) (string, error)
	GetNullString(fieldName string) (sql.NullString, error)
	GetByte(fieldName string) (byte, error)
	GetNullByte(fieldName string) (sql.NullByte, error)
	GetInt16(fieldName string) (int16, error)
	GetNullInt16(fieldName string) (sql.NullInt16, error)
	GetInt32(fieldName string) (int32, error)
	GetNullInt32(fieldName string) (sql.NullInt32, error)
	GetInt64(fieldName string) (int64, error)
	GetNullInt64(fieldName string) (sql.NullInt64, error)
	GetBigInt(fieldName string) (*big.Int, error)
	GetFloat32(fieldName string) (float32, error)
	GetFloat64(fieldName string) (float64, error)
	GetNullFloat64(fieldName string) (sql.NullFloat64, error)
	GetBigFloat(fieldName string) (*big.Float, error)
	GetBool(fieldName string) (bool, error)
	GetNullBool(fieldName string) (sql.NullBool, error)
	GetBytes(fieldName string) ([]byte, error)
	GetTime(fieldName string) (time.Time, error)
	GetNullTime(fieldName string) (sql.NullTime, error)
	GetStruct(fieldName string, scanner sql.Scanner) (sql.Scanner, error)
}

// ArrayOfScanners Helper type for scanning array of sql.Scanner values.
type ArrayOfScanners[T sql.Scanner] []T

func (st *ArrayOfScanners[T]) Scan(val any) error {
	sts := val.([]*structuredType)
	*st = make([]T, len(sts))
	var t T
	for i, s := range sts {
		(*st)[i] = reflect.New(reflect.TypeOf(t).Elem()).Interface().(T)
		if err := (*st)[i].Scan(s); err != nil {
			return err
		}
	}
	return nil
}

// ScanArrayOfScanners is a helper function for scanning arrays of sql.Scanner values.
// Example:
// var res []*simpleObject
// err := rows.Scan(ScanArrayOfScanners(&res))
func ScanArrayOfScanners[T sql.Scanner](value *[]T) *ArrayOfScanners[T] {
	return (*ArrayOfScanners[T])(value)
}

type structuredType struct {
	values        map[string]any
	fieldMetadata []fieldMetadata
	params        map[string]*string
}

func getType[T any](st *structuredType, fieldName string, emptyValue T) (T, bool, error) {
	v, ok := st.values[fieldName]
	if !ok {
		return emptyValue, false, errors.New("field " + fieldName + " does not exist")
	}
	if v == nil {
		return emptyValue, true, nil
	}
	v, ok = v.(T)
	if !ok {
		return emptyValue, false, fmt.Errorf("cannot convert field %v to %T", fieldName, emptyValue)
	}
	return v.(T), false, nil
}

func (st *structuredType) GetString(fieldName string) (string, error) {
	nullString, err := st.GetNullString(fieldName)
	if err != nil {
		return "", err
	}
	if !nullString.Valid {
		return "", fmt.Errorf("nil value for %v, use GetNullString instead", fieldName)
	}
	return nullString.String, nil
}

func (st *structuredType) GetNullString(fieldName string) (sql.NullString, error) {
	s, wasNil, err := getType[string](st, fieldName, "")
	if err != nil {
		return sql.NullString{}, err
	}
	if wasNil {
		return sql.NullString{Valid: false}, err
	}
	return sql.NullString{Valid: true, String: s}, nil
}

func (st *structuredType) GetByte(fieldName string) (byte, error) {
	nullByte, err := st.GetNullByte(fieldName)
	if err != nil {
		return 0, nil
	}
	if !nullByte.Valid {
		return 0, fmt.Errorf("nil value for %v, use GetNullByte instead", fieldName)
	}
	return nullByte.Byte, nil
}

func (st *structuredType) GetNullByte(fieldName string) (sql.NullByte, error) {
	b, err := st.GetNullInt64(fieldName)
	if err != nil {
		return sql.NullByte{}, err
	}
	if !b.Valid {
		return sql.NullByte{Valid: false}, nil
	}
	return sql.NullByte{Valid: true, Byte: byte(b.Int64)}, nil
}

func (st *structuredType) GetInt16(fieldName string) (int16, error) {
	nullInt16, err := st.GetNullInt16(fieldName)
	if err != nil {
		return 0, nil
	}
	if !nullInt16.Valid {
		return 0, fmt.Errorf("nil value for %v, use GetNullInt16 instead", fieldName)
	}
	return nullInt16.Int16, nil
}

func (st *structuredType) GetNullInt16(fieldName string) (sql.NullInt16, error) {
	b, err := st.GetNullInt64(fieldName)
	if err != nil {
		return sql.NullInt16{}, err
	}
	if !b.Valid {
		return sql.NullInt16{Valid: false}, nil
	}
	return sql.NullInt16{Valid: true, Int16: int16(b.Int64)}, nil
}

func (st *structuredType) GetInt32(fieldName string) (int32, error) {
	nullInt32, err := st.GetNullInt32(fieldName)
	if err != nil {
		return 0, nil
	}
	if !nullInt32.Valid {
		return 0, fmt.Errorf("nil value for %v, use GetNullInt32 instead", fieldName)
	}
	return nullInt32.Int32, nil
}

func (st *structuredType) GetNullInt32(fieldName string) (sql.NullInt32, error) {
	b, err := st.GetNullInt64(fieldName)
	if err != nil {
		return sql.NullInt32{}, err
	}
	if !b.Valid {
		return sql.NullInt32{Valid: false}, nil
	}
	return sql.NullInt32{Valid: true, Int32: int32(b.Int64)}, nil
}

func (st *structuredType) GetInt64(fieldName string) (int64, error) {
	nullInt64, err := st.GetNullInt64(fieldName)
	if err != nil {
		return 0, nil
	}
	if !nullInt64.Valid {
		return 0, fmt.Errorf("nil value for %v, use GetNullInt64 instead", fieldName)
	}
	return nullInt64.Int64, nil
}

func (st *structuredType) GetNullInt64(fieldName string) (sql.NullInt64, error) {
	i64, wasNil, err := getType[int64](st, fieldName, 0)
	if wasNil {
		return sql.NullInt64{Valid: false}, err
	}
	if err == nil {
		return sql.NullInt64{Valid: true, Int64: i64}, nil
	}
	if s, _, err := getType[string](st, fieldName, ""); err == nil {
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return sql.NullInt64{Valid: false}, err
		}
		return sql.NullInt64{Valid: true, Int64: i}, nil
	} else if b, _, err := getType[float64](st, fieldName, 0); err == nil {
		return sql.NullInt64{Valid: true, Int64: int64(b)}, nil
	} else if b, _, err := getType[json.Number](st, fieldName, ""); err == nil {
		i, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return sql.NullInt64{Valid: false}, err
		}
		return sql.NullInt64{Valid: true, Int64: i}, err
	} else {
		return sql.NullInt64{Valid: false}, fmt.Errorf("cannot cast column %v to byte", fieldName)
	}
}

func (st *structuredType) GetBigInt(fieldName string) (*big.Int, error) {
	b, wasNull, err := getType[*big.Int](st, fieldName, new(big.Int))
	if wasNull {
		return nil, nil
	}
	return b, err
}

func (st *structuredType) GetFloat32(fieldName string) (float32, error) {
	f32, err := st.GetFloat64(fieldName)
	if err != nil {
		return 0, err
	}
	return float32(f32), err
}

func (st *structuredType) GetFloat64(fieldName string) (float64, error) {
	nullFloat64, err := st.GetNullFloat64(fieldName)
	if err != nil {
		return 0, nil
	}
	if !nullFloat64.Valid {
		return 0, fmt.Errorf("nil value for %v, use GetNullFloat64 instead", fieldName)
	}
	return nullFloat64.Float64, nil
}

func (st *structuredType) GetNullFloat64(fieldName string) (sql.NullFloat64, error) {
	float64, wasNull, err := getType[float64](st, fieldName, 0)
	if wasNull {
		return sql.NullFloat64{Valid: false}, nil
	}
	if err == nil {
		return sql.NullFloat64{Valid: true, Float64: float64}, nil
	}
	s, _, err := getType[string](st, fieldName, "")
	if err == nil {
		f64, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return sql.NullFloat64{}, err
		}
		return sql.NullFloat64{Valid: true, Float64: f64}, err
	}
	jsonNumber, _, err := getType[json.Number](st, fieldName, "")
	if err != nil {
		return sql.NullFloat64{}, err
	}
	f64, err := strconv.ParseFloat(string(jsonNumber), 64)
	if err != nil {
		return sql.NullFloat64{}, err
	}
	return sql.NullFloat64{Valid: true, Float64: f64}, nil
}

func (st *structuredType) GetBigFloat(fieldName string) (*big.Float, error) {
	float, wasNull, err := getType[*big.Float](st, fieldName, new(big.Float))
	if wasNull {
		return nil, nil
	}
	return float, err
}

func (st *structuredType) GetBool(fieldName string) (bool, error) {
	nullBool, err := st.GetNullBool(fieldName)
	if err != nil {
		return false, err
	}
	if !nullBool.Valid {
		return false, fmt.Errorf("nil value for %v, use GetNullBool instead", fieldName)
	}
	return nullBool.Bool, err
}

func (st *structuredType) GetNullBool(fieldName string) (sql.NullBool, error) {
	b, wasNull, err := getType[bool](st, fieldName, false)
	if wasNull {
		return sql.NullBool{Valid: false}, nil
	}
	if err != nil {
		return sql.NullBool{}, err
	}
	return sql.NullBool{Valid: true, Bool: b}, nil
}

func (st *structuredType) GetBytes(fieldName string) ([]byte, error) {
	if bi, _, err := getType[[]byte](st, fieldName, []byte{}); err == nil {
		return bi, nil
	} else if bi, _, err := getType[string](st, fieldName, ""); err == nil {
		return hex.DecodeString(bi)
	}
	bytes, _, err := getType[[]byte](st, fieldName, []byte{})
	return bytes, err
}

func (st *structuredType) GetTime(fieldName string) (time.Time, error) {
	nullTime, err := st.GetNullTime(fieldName)
	if err != nil {
		return time.Time{}, nil
	}
	if !nullTime.Valid {
		return time.Time{}, fmt.Errorf("nil value for %v, use GetNullBool instead", fieldName)
	}
	return nullTime.Time, nil
}

func (st *structuredType) GetNullTime(fieldName string) (sql.NullTime, error) {
	s, wasNull, err := getType[string](st, fieldName, "")
	if wasNull {
		return sql.NullTime{Valid: false}, nil
	}
	if err == nil {
		fieldMetadata, err := st.fieldMetadataByFieldName(fieldName)
		if err != nil {
			return sql.NullTime{}, err
		}
		format, err := dateTimeFormatByType(fieldMetadata.Type, st.params)
		if err != nil {
			return sql.NullTime{}, err
		}
		goFormat, err := snowflakeFormatToGoFormat(format)
		if err != nil {
			return sql.NullTime{}, err
		}
		time, err := time.Parse(goFormat, s)
		return sql.NullTime{Valid: true, Time: time}, err
	}
	time, _, err := getType[time.Time](st, fieldName, time.Time{})
	if err != nil {
		return sql.NullTime{}, err
	}
	return sql.NullTime{Valid: true, Time: time}, nil
}

func (st *structuredType) GetStruct(fieldName string, scanner sql.Scanner) (sql.Scanner, error) {
	childSt, wasNull, err := getType[*structuredType](st, fieldName, &structuredType{})
	if wasNull {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	err = scanner.Scan(childSt)
	return scanner, err
}

func (st *structuredType) fieldMetadataByFieldName(fieldName string) (fieldMetadata, error) {
	for _, fm := range st.fieldMetadata {
		if fm.Name == fieldName {
			return fm, nil
		}
	}
	return fieldMetadata{}, errors.New("no metadata for field " + fieldName)
}

func mapValuesNullableEnabled(ctx context.Context) bool {
	v := ctx.Value(mapValuesNullable)
	if v == nil {
		return false
	}
	d, ok := v.(bool)
	return ok && d
}
