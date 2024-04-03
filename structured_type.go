package gosnowflake

import (
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

// StructuredType representation of structured type.
type StructuredType interface {
	GetString(fieldName string) (string, error)
	GetByte(fieldName string) (byte, error)
	GetInt16(fieldName string) (int16, error)
	GetInt(fieldName string) (int, error)
	GetInt64(fieldName string) (int64, error)
	GetBigInt(fieldName string) (*big.Int, error)
	GetFloat32(fieldName string) (float32, error)
	GetFloat64(fieldName string) (float64, error)
	GetBigFloat(fieldName string) (*big.Float, error)
	GetBool(fieldName string) (bool, error)
	GetBytes(fieldName string) ([]byte, error)
	GetTime(fieldName string) (time.Time, error)
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

func getType[T any](st *structuredType, fieldName string, emptyValue T) (T, error) {
	v, ok := st.values[fieldName]
	if !ok {
		return emptyValue, errors.New("field " + fieldName + " does not exist")
	}
	v, ok = v.(T)
	if !ok {
		return emptyValue, fmt.Errorf("cannot convert field %v to %T", fieldName, emptyValue)
	}
	return v.(T), nil
}

func (st *structuredType) GetString(fieldName string) (string, error) {
	return getType[string](st, fieldName, "")
}

func (st *structuredType) GetByte(fieldName string) (byte, error) {
	b, err := st.GetInt64(fieldName)
	if err != nil {
		return 0, err
	}
	return byte(b), err
}

func (st *structuredType) GetInt16(fieldName string) (int16, error) {
	i16, err := st.GetInt64(fieldName)
	if err != nil {
		return 0, err
	}
	return int16(i16), err
}

func (st *structuredType) GetInt(fieldName string) (int, error) {
	i, err := st.GetInt64(fieldName)
	if err != nil {
		return 0, err
	}
	return int(i), err
}

func (st *structuredType) GetInt64(fieldName string) (int64, error) {
	if b, err := getType[int64](st, fieldName, 0); err == nil {
		return b, err
	} else if b, err := getType[string](st, fieldName, ""); err == nil {
		return strconv.ParseInt(b, 10, 64)
	} else if b, err := getType[float64](st, fieldName, 0); err == nil {
		return int64(b), nil
	} else if b, err := getType[json.Number](st, fieldName, ""); err == nil {
		return strconv.ParseInt(string(b), 10, 64)
	} else {
		return 0, fmt.Errorf("cannot cast column %v to byte", fieldName)
	}
}

func (st *structuredType) GetBigInt(fieldName string) (*big.Int, error) {
	return getType[*big.Int](st, fieldName, new(big.Int))
}

func (st *structuredType) GetFloat32(fieldName string) (float32, error) {
	f32, err := st.GetFloat64(fieldName)
	if err != nil {
		return 0, err
	}
	return float32(f32), err
}

func (st *structuredType) GetFloat64(fieldName string) (float64, error) {
	if b, err := getType[json.Number](st, fieldName, ""); err == nil {
		return strconv.ParseFloat(string(b), 64)
	}
	return getType[float64](st, fieldName, 0)
}

func (st *structuredType) GetBigFloat(fieldName string) (*big.Float, error) {
	return getType[*big.Float](st, fieldName, new(big.Float))
}

func (st *structuredType) GetBool(fieldName string) (bool, error) {
	return getType[bool](st, fieldName, false)
}

func (st *structuredType) GetBytes(fieldName string) ([]byte, error) {
	if bi, err := getType[[]byte](st, fieldName, []byte{}); err == nil {
		return bi, nil
	} else if bi, err := getType[string](st, fieldName, ""); err == nil {
		return hex.DecodeString(bi)
	}
	return getType[[]byte](st, fieldName, []byte{})
}

func (st *structuredType) GetTime(fieldName string) (time.Time, error) {
	s, err := getType[string](st, fieldName, "")
	if err == nil {
		fieldMetadata, err := st.fieldMetadataByFieldName(fieldName)
		if err != nil {
			return time.Time{}, err
		}
		format, err := dateTimeFormatByType(fieldMetadata.Type, st.params)
		if err != nil {
			return time.Time{}, err
		}
		goFormat, err := snowflakeFormatToGoFormat(format)
		if err != nil {
			return time.Time{}, err
		}
		return time.Parse(goFormat, s)
	}
	dt, err := getType[time.Time](st, fieldName, time.Time{})
	if err != nil {
		return time.Time{}, err
	}
	return dt, nil
}

func (st *structuredType) GetStruct(fieldName string, scanner sql.Scanner) (sql.Scanner, error) {
	childSt, err := getType[*structuredType](st, fieldName, &structuredType{})
	if err != nil {
		return nil, err
	}
	scanner.Scan(childSt)
	return scanner, nil
}

func (st *structuredType) fieldMetadataByFieldName(fieldName string) (fieldMetadata, error) {
	for _, fm := range st.fieldMetadata {
		if fm.Name == fieldName {
			return fm, nil
		}
	}
	return fieldMetadata{}, errors.New("no metadata for field " + fieldName)
}
