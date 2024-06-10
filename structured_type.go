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
	"strings"
	"time"
	"unicode"
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
	GetRaw(fieldName string) (any, error)
	ScanTo(sc sql.Scanner) error
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
//
//	var res []*simpleObject
//	err := rows.Scan(ScanArrayOfScanners(&res))
func ScanArrayOfScanners[T sql.Scanner](value *[]T) *ArrayOfScanners[T] {
	return (*ArrayOfScanners[T])(value)
}

// MapOfScanners Helper type for scanning map of sql.Scanner values.
type MapOfScanners[K comparable, V sql.Scanner] map[K]V

func (st *MapOfScanners[K, V]) Scan(val any) error {
	sts := val.(map[K]*structuredType)
	*st = make(map[K]V)
	var someV V
	for k := range sts {
		(*st)[k] = reflect.New(reflect.TypeOf(someV).Elem()).Interface().(V)
		if err := (*st)[k].Scan(sts[k]); err != nil {
			return err
		}
	}
	return nil
}

// ScanMapOfScanners is a helper function for scanning maps of sql.Scanner values.
// Example:
//
//	var res map[string]*simpleObject
//	err := rows.Scan(ScanMapOfScanners(&res))
func ScanMapOfScanners[K comparable, V sql.Scanner](m *map[K]V) *MapOfScanners[K, V] {
	return (*MapOfScanners[K, V])(m)
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
func (st *structuredType) GetRaw(fieldName string) (any, error) {
	return st.values[fieldName], nil
}

func (st *structuredType) ScanTo(sc sql.Scanner) error {
	v := reflect.Indirect(reflect.ValueOf(sc))
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if st.shouldIgnoreField(field) {
			continue
		}
		switch field.Type.Kind() {
		case reflect.String:
			s, err := st.GetString(st.getFieldName(field))
			if err != nil {
				return err
			}
			v.FieldByName(field.Name).SetString(s)
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			i, err := st.GetInt64(st.getFieldName(field))
			if err != nil {
				return err
			}
			v.FieldByName(field.Name).SetInt(i)
		case reflect.Uint8:
			b, err := st.GetByte(st.getFieldName(field))
			if err != nil {
				return err
			}
			v.FieldByName(field.Name).SetUint(uint64(int64(b)))
		case reflect.Float32, reflect.Float64:
			f, err := st.GetFloat64(st.getFieldName(field))
			if err != nil {
				return err
			}
			v.FieldByName(field.Name).SetFloat(f)
		case reflect.Bool:
			b, err := st.GetBool(st.getFieldName(field))
			if err != nil {
				return err
			}
			v.FieldByName(field.Name).SetBool(b)
		case reflect.Slice, reflect.Array:
			switch field.Type.Elem().Kind() {
			case reflect.Uint8:
				b, err := st.GetBytes(st.getFieldName(field))
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).SetBytes(b)
			default:
				raw, err := st.GetRaw(st.getFieldName(field))
				if err != nil {
					return err
				}
				if raw != nil {
					v.FieldByName(field.Name).Set(reflect.ValueOf(raw))
				}
			}
		case reflect.Map:
			raw, err := st.GetRaw(st.getFieldName(field))
			if err != nil {
				return err
			}
			if raw != nil {
				v.FieldByName(field.Name).Set(reflect.ValueOf(raw))
			}
		case reflect.Struct:
			a := v.FieldByName(field.Name).Interface()
			if _, ok := a.(time.Time); ok {
				time, err := st.GetTime(st.getFieldName(field))
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).Set(reflect.ValueOf(time))
			} else if _, ok := a.(sql.Scanner); ok {
				scanner := reflect.New(reflect.TypeOf(a)).Interface().(sql.Scanner)
				s, err := st.GetStruct(st.getFieldName(field), scanner)
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).Set(reflect.Indirect(reflect.ValueOf(s)))
			} else if _, ok := a.(sql.NullString); ok {
				ns, err := st.GetNullString(st.getFieldName(field))
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).Set(reflect.ValueOf(ns))
			} else if _, ok := a.(sql.NullByte); ok {
				nb, err := st.GetNullByte(st.getFieldName(field))
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).Set(reflect.ValueOf(nb))
			} else if _, ok := a.(sql.NullBool); ok {
				nb, err := st.GetNullBool(st.getFieldName(field))
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).Set(reflect.ValueOf(nb))
			} else if _, ok := a.(sql.NullInt16); ok {
				ni, err := st.GetNullInt16(st.getFieldName(field))
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).Set(reflect.ValueOf(ni))
			} else if _, ok := a.(sql.NullInt32); ok {
				ni, err := st.GetNullInt32(st.getFieldName(field))
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).Set(reflect.ValueOf(ni))
			} else if _, ok := a.(sql.NullInt64); ok {
				ni, err := st.GetNullInt64(st.getFieldName(field))
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).Set(reflect.ValueOf(ni))
			} else if _, ok := a.(sql.NullFloat64); ok {
				nf, err := st.GetNullFloat64(st.getFieldName(field))
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).Set(reflect.ValueOf(nf))
			} else if _, ok := a.(sql.NullTime); ok {
				nt, err := st.GetNullTime(st.getFieldName(field))
				if err != nil {
					return err
				}
				v.FieldByName(field.Name).Set(reflect.ValueOf(nt))
			}
		case reflect.Pointer:
			switch field.Type.Elem().Kind() {
			case reflect.Struct:
				a := reflect.New(field.Type.Elem()).Interface()
				s, err := st.GetStruct(st.getFieldName(field), a.(sql.Scanner))
				if err != nil {
					return err
				}
				if s != nil {
					v.FieldByName(field.Name).Set(reflect.ValueOf(s))
				}
			default:
				return errors.New("only struct pointers are supported")
			}
		}
	}
	return nil
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

func (st *structuredType) getFieldName(field reflect.StructField) string {
	sfTag := field.Tag.Get("sf")
	if sfTag != "" {
		return strings.Split(sfTag, ",")[0]
	}
	r := []rune(field.Name)
	r[0] = unicode.ToLower(r[0])
	return string(r)
}

func (st *structuredType) shouldIgnoreField(field reflect.StructField) bool {
	sfTag := field.Tag.Get("sf")
	if sfTag == "" {
		return false
	}
	return contains(strings.Split(sfTag, ",")[1:], "ignore")
}
