package gosnowflake

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestBindingVariant(t *testing.T) {
	t.Skip("binding variant is currently not supported")
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test_variant_binding (var VARIANT)")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_variant_binding")
		}()
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("INSERT INTO test_variant_binding SELECT (?)", DataTypeVariant, nil)
		dbt.mustExec("INSERT INTO test_variant_binding SELECT (?)", DataTypeVariant, sql.NullString{Valid: false})
		dbt.mustExec("INSERT INTO test_variant_binding SELECT (?)", DataTypeVariant, "{'s': 'some string'}")
		dbt.mustExec("INSERT INTO test_variant_binding SELECT (?)", DataTypeVariant, sql.NullString{Valid: true, String: "{'s': 'some string2'}"})
		rows := dbt.mustQuery("SELECT * FROM test_variant_binding")
		defer rows.Close()
		var res sql.NullString

		assertTrueF(t, rows.Next())
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `{"s": "some string"}`)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `{"s": "some string2"}`)
	})
}

func TestBindingObjectWithoutSchema(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test_object_binding (obj OBJECT)")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_object_binding")
		}()
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", DataTypeObject, nil)
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", DataTypeObject, sql.NullString{Valid: false})
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", DataTypeObject, "{'s': 'some string'}")
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", DataTypeObject, sql.NullString{Valid: true, String: "{'s': 'some string2'}"})
		rows := dbt.mustQuery("SELECT * FROM test_object_binding")
		defer rows.Close()
		var res sql.NullString

		assertTrueF(t, rows.Next())
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `{"s": "some string"}`)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `{"s": "some string2"}`)
	})
}

func TestBindingArrayWithoutSchema(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test_array_binding (arr ARRAY)")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_array_binding")
		}()
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", DataTypeArray, nil)
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", DataTypeArray, sql.NullString{Valid: false})
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", DataTypeArray, "[1, 2, 3]")
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", DataTypeArray, sql.NullString{Valid: true, String: "[1, 2, 3]"})
		rows := dbt.mustQuery("SELECT * FROM test_array_binding")
		defer rows.Close()
		var res sql.NullString

		assertTrueF(t, rows.Next())
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertFalseF(t, res.Valid)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `[1, 2, 3]`)

		assertTrueF(t, rows.Next())
		err = rows.Scan(&res)
		assertNilF(t, err)
		assertTrueE(t, res.Valid)
		assertEqualIgnoringWhitespaceE(t, res.String, `[1, 2, 3]`)
	})
}

func TestBindingObjectWithSchema(t *testing.T) {
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	ctx := WithStructuredTypesEnabled(context.Background())
	assertNilF(t, err)
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE test_object_binding (obj OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f32 FLOAT, f64 DOUBLE, nfraction NUMBER(38, 9), bo boolean, bi BINARY, date DATE, time TIME, ltz TIMESTAMPLTZ, ntz TIMESTAMPNTZ, tz TIMESTAMPTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_object_binding")
		}()
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		dbt.mustExec("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'")
		o := objectWithAllTypes{
			s:         "some string",
			b:         1,
			i16:       2,
			i32:       3,
			i64:       4,
			f32:       1.1,
			f64:       2.2,
			nfraction: 3.3,
			bo:        true,
			bi:        []byte{'a', 'b', 'c'},
			date:      time.Date(2024, time.May, 24, 0, 0, 0, 0, time.UTC),
			time:      time.Date(1, 1, 1, 11, 22, 33, 0, time.UTC),
			ltz:       time.Date(2025, time.May, 24, 11, 22, 33, 44, warsawTz),
			ntz:       time.Date(2026, time.May, 24, 11, 22, 33, 0, time.UTC),
			tz:        time.Date(2027, time.May, 24, 11, 22, 33, 44, warsawTz),
			so:        &simpleObject{s: "another string", i: 123},
			sArr:      []string{"a", "b"},
			f64Arr:    []float64{1.1, 2.2},
			someMap:   map[string]bool{"a": true, "b": false},
			uuid:      newTestUUID(),
		}
		dbt.mustExecT(t, "INSERT INTO test_object_binding SELECT (?)", o)
		rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_object_binding WHERE obj = ?", o)
		defer rows.Close()

		assertTrueE(t, rows.Next())
		var res objectWithAllTypes
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertEqualE(t, res.s, o.s)
		assertEqualE(t, res.b, o.b)
		assertEqualE(t, res.i16, o.i16)
		assertEqualE(t, res.i32, o.i32)
		assertEqualE(t, res.i64, o.i64)
		assertEqualE(t, res.f32, o.f32)
		assertEqualE(t, res.f64, o.f64)
		assertEqualE(t, res.nfraction, o.nfraction)
		assertEqualE(t, res.bo, o.bo)
		assertDeepEqualE(t, res.bi, o.bi)
		assertTrueE(t, res.date.Equal(o.date))
		assertEqualE(t, res.time.Hour(), o.time.Hour())
		assertEqualE(t, res.time.Minute(), o.time.Minute())
		assertEqualE(t, res.time.Second(), o.time.Second())
		assertTrueE(t, res.ltz.Equal(o.ltz))
		assertTrueE(t, res.tz.Equal(o.tz))
		assertTrueE(t, res.ntz.Equal(o.ntz))
		assertDeepEqualE(t, res.so, o.so)
		assertDeepEqualE(t, res.sArr, o.sArr)
		assertDeepEqualE(t, res.f64Arr, o.f64Arr)
		assertDeepEqualE(t, res.someMap, o.someMap)
		assertEqualE(t, res.uuid.String(), o.uuid.String())
	})
}

func TestBindingObjectWithNullableFieldsWithSchema(t *testing.T) {
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE test_object_binding (obj OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f64 DOUBLE, bo boolean, bi BINARY, date DATE, time TIME, ltz TIMESTAMPLTZ, ntz TIMESTAMPNTZ, tz TIMESTAMPTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_object_binding")
		}()
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		dbt.mustExec("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'")
		t.Run("not null", func(t *testing.T) {
			o := &objectWithAllTypesNullable{
				s:       sql.NullString{String: "some string", Valid: true},
				b:       sql.NullByte{Byte: 1, Valid: true},
				i16:     sql.NullInt16{Int16: 2, Valid: true},
				i32:     sql.NullInt32{Int32: 3, Valid: true},
				i64:     sql.NullInt64{Int64: 4, Valid: true},
				f64:     sql.NullFloat64{Float64: 2.2, Valid: true},
				bo:      sql.NullBool{Bool: true, Valid: true},
				bi:      []byte{'a', 'b', 'c'},
				date:    sql.NullTime{Time: time.Date(2024, time.May, 24, 0, 0, 0, 0, time.UTC), Valid: true},
				time:    sql.NullTime{Time: time.Date(1, 1, 1, 11, 22, 33, 0, time.UTC), Valid: true},
				ltz:     sql.NullTime{Time: time.Date(2025, time.May, 24, 11, 22, 33, 44, warsawTz), Valid: true},
				ntz:     sql.NullTime{Time: time.Date(2026, time.May, 24, 11, 22, 33, 0, time.UTC), Valid: true},
				tz:      sql.NullTime{Time: time.Date(2027, time.May, 24, 11, 22, 33, 44, warsawTz), Valid: true},
				so:      &simpleObject{s: "another string", i: 123},
				sArr:    []string{"a", "b"},
				f64Arr:  []float64{1.1, 2.2},
				someMap: map[string]bool{"a": true, "b": false},
				uuid:    newTestUUID(),
			}
			dbt.mustExecT(t, "INSERT INTO test_object_binding SELECT (?)", o)
			rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_object_binding WHERE obj = ?", o)
			defer rows.Close()

			assertTrueE(t, rows.Next())
			var res objectWithAllTypesNullable
			err := rows.Scan(&res)
			assertNilF(t, err)
			assertEqualE(t, res.s, o.s)
			assertEqualE(t, res.b, o.b)
			assertEqualE(t, res.i16, o.i16)
			assertEqualE(t, res.i32, o.i32)
			assertEqualE(t, res.i64, o.i64)
			assertEqualE(t, res.f64, o.f64)
			assertEqualE(t, res.bo, o.bo)
			assertDeepEqualE(t, res.bi, o.bi)
			assertTrueE(t, res.date.Time.Equal(o.date.Time))
			assertEqualE(t, res.time.Time.Hour(), o.time.Time.Hour())
			assertEqualE(t, res.time.Time.Minute(), o.time.Time.Minute())
			assertEqualE(t, res.time.Time.Second(), o.time.Time.Second())
			assertTrueE(t, res.ltz.Time.Equal(o.ltz.Time))
			assertTrueE(t, res.tz.Time.Equal(o.tz.Time))
			assertTrueE(t, res.ntz.Time.Equal(o.ntz.Time))
			assertDeepEqualE(t, res.so, o.so)
			assertDeepEqualE(t, res.sArr, o.sArr)
			assertDeepEqualE(t, res.f64Arr, o.f64Arr)
			assertDeepEqualE(t, res.someMap, o.someMap)
			assertEqualE(t, res.uuid.String(), o.uuid.String())
		})
		t.Run("null", func(t *testing.T) {
			o := &objectWithAllTypesNullable{
				s:       sql.NullString{},
				b:       sql.NullByte{},
				i16:     sql.NullInt16{},
				i32:     sql.NullInt32{},
				i64:     sql.NullInt64{},
				f64:     sql.NullFloat64{},
				bo:      sql.NullBool{},
				bi:      nil,
				date:    sql.NullTime{},
				time:    sql.NullTime{},
				ltz:     sql.NullTime{},
				ntz:     sql.NullTime{},
				tz:      sql.NullTime{},
				so:      nil,
				sArr:    nil,
				f64Arr:  nil,
				someMap: nil,
			}
			dbt.mustExecT(t, "INSERT INTO test_object_binding SELECT (?)", o)
			rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_object_binding WHERE obj = ?", o)
			defer rows.Close()

			assertTrueE(t, rows.Next())
			var res objectWithAllTypesNullable
			err := rows.Scan(&res)
			assertNilF(t, err)
			assertEqualE(t, res.s, o.s)
			assertEqualE(t, res.b, o.b)
			assertEqualE(t, res.i16, o.i16)
			assertEqualE(t, res.i32, o.i32)
			assertEqualE(t, res.i64, o.i64)
			assertEqualE(t, res.f64, o.f64)
			assertEqualE(t, res.bo, o.bo)
			assertDeepEqualE(t, res.bi, o.bi)
			assertTrueE(t, res.date.Time.Equal(o.date.Time))
			assertEqualE(t, res.time.Time.Hour(), o.time.Time.Hour())
			assertEqualE(t, res.time.Time.Minute(), o.time.Time.Minute())
			assertEqualE(t, res.time.Time.Second(), o.time.Time.Second())
			assertTrueE(t, res.ltz.Time.Equal(o.ltz.Time))
			assertTrueE(t, res.tz.Time.Equal(o.tz.Time))
			assertTrueE(t, res.ntz.Time.Equal(o.ntz.Time))
			assertDeepEqualE(t, res.so, o.so)
			assertDeepEqualE(t, res.sArr, o.sArr)
			assertDeepEqualE(t, res.f64Arr, o.f64Arr)
			assertDeepEqualE(t, res.someMap, o.someMap)
		})
	})
}

func TestBindingObjectWithSchemaSimpleWrite(t *testing.T) {
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE test_object_binding (obj OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f32 FLOAT, f64 DOUBLE, nfraction NUMBER(38, 9), bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN)))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_object_binding")
		}()
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		dbt.mustExec("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'")
		o := &objectWithAllTypesSimpleScan{
			S:         "some string",
			B:         1,
			I16:       2,
			I32:       3,
			I64:       4,
			F32:       1.1,
			F64:       2.2,
			Nfraction: 3.3,
			Bo:        true,
			Bi:        []byte{'a', 'b', 'c'},
			Date:      time.Date(2024, time.May, 24, 0, 0, 0, 0, time.UTC),
			Time:      time.Date(1, 1, 1, 11, 22, 33, 0, time.UTC),
			Ltz:       time.Date(2025, time.May, 24, 11, 22, 33, 44, warsawTz),
			Ntz:       time.Date(2026, time.May, 24, 11, 22, 33, 0, time.UTC),
			Tz:        time.Date(2027, time.May, 24, 11, 22, 33, 44, warsawTz),
			So:        &simpleObject{s: "another string", i: 123},
			SArr:      []string{"a", "b"},
			F64Arr:    []float64{1.1, 2.2},
			SomeMap:   map[string]bool{"a": true, "b": false},
		}
		dbt.mustExecT(t, "INSERT INTO test_object_binding SELECT (?)", o)
		rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_object_binding WHERE obj = ?", o)
		defer rows.Close()

		assertTrueE(t, rows.Next())
		var res objectWithAllTypesSimpleScan
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertEqualE(t, res.S, o.S)
		assertEqualE(t, res.B, o.B)
		assertEqualE(t, res.I16, o.I16)
		assertEqualE(t, res.I32, o.I32)
		assertEqualE(t, res.I64, o.I64)
		assertEqualE(t, res.F32, o.F32)
		assertEqualE(t, res.F64, o.F64)
		assertEqualE(t, res.Nfraction, o.Nfraction)
		assertEqualE(t, res.Bo, o.Bo)
		assertDeepEqualE(t, res.Bi, o.Bi)
		assertTrueE(t, res.Date.Equal(o.Date))
		assertEqualE(t, res.Time.Hour(), o.Time.Hour())
		assertEqualE(t, res.Time.Minute(), o.Time.Minute())
		assertEqualE(t, res.Time.Second(), o.Time.Second())
		assertTrueE(t, res.Ltz.Equal(o.Ltz))
		assertTrueE(t, res.Tz.Equal(o.Tz))
		assertTrueE(t, res.Ntz.Equal(o.Ntz))
		assertDeepEqualE(t, res.So, o.So)
		assertDeepEqualE(t, res.SArr, o.SArr)
		assertDeepEqualE(t, res.F64Arr, o.F64Arr)
		assertDeepEqualE(t, res.SomeMap, o.SomeMap)
	})
}

func TestBindingObjectWithNullableFieldsWithSchemaSimpleWrite(t *testing.T) {
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.forceJSON()
		dbt.mustExec("CREATE OR REPLACE TABLE test_object_binding (obj OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f64 DOUBLE, bo boolean, bi BINARY, date DATE, time TIME, ltz TIMESTAMPLTZ, tz TIMESTAMPTZ, ntz TIMESTAMPNTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN)))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_object_binding")
		}()
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		dbt.mustExec("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'")
		t.Run("not null", func(t *testing.T) {
			o := &objectWithAllTypesNullableSimpleScan{
				S:       sql.NullString{String: "some string", Valid: true},
				B:       sql.NullByte{Byte: 1, Valid: true},
				I16:     sql.NullInt16{Int16: 2, Valid: true},
				I32:     sql.NullInt32{Int32: 3, Valid: true},
				I64:     sql.NullInt64{Int64: 4, Valid: true},
				F64:     sql.NullFloat64{Float64: 2.2, Valid: true},
				Bo:      sql.NullBool{Bool: true, Valid: true},
				Bi:      []byte{'a', 'b', 'c'},
				Date:    sql.NullTime{Time: time.Date(2024, time.May, 24, 0, 0, 0, 0, time.UTC), Valid: true},
				Time:    sql.NullTime{Time: time.Date(1, 1, 1, 11, 22, 33, 0, time.UTC), Valid: true},
				Ltz:     sql.NullTime{Time: time.Date(2025, time.May, 24, 11, 22, 33, 44, warsawTz), Valid: true},
				Ntz:     sql.NullTime{Time: time.Date(2026, time.May, 24, 11, 22, 33, 0, time.UTC), Valid: true},
				Tz:      sql.NullTime{Time: time.Date(2027, time.May, 24, 11, 22, 33, 44, warsawTz), Valid: true},
				So:      &simpleObject{s: "another string", i: 123},
				SArr:    []string{"a", "b"},
				F64Arr:  []float64{1.1, 2.2},
				SomeMap: map[string]bool{"a": true, "b": false},
			}
			dbt.mustExecT(t, "INSERT INTO test_object_binding SELECT (?)", o)
			rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_object_binding WHERE obj = ?", o)
			defer rows.Close()

			assertTrueE(t, rows.Next())
			var res objectWithAllTypesNullableSimpleScan
			err := rows.Scan(&res)
			assertNilF(t, err)
			assertEqualE(t, res.S, o.S)
			assertEqualE(t, res.B, o.B)
			assertEqualE(t, res.I16, o.I16)
			assertEqualE(t, res.I32, o.I32)
			assertEqualE(t, res.I64, o.I64)
			assertEqualE(t, res.F64, o.F64)
			assertEqualE(t, res.Bo, o.Bo)
			assertDeepEqualE(t, res.Bi, o.Bi)
			assertTrueE(t, res.Date.Time.Equal(o.Date.Time))
			assertEqualE(t, res.Time.Time.Hour(), o.Time.Time.Hour())
			assertEqualE(t, res.Time.Time.Minute(), o.Time.Time.Minute())
			assertEqualE(t, res.Time.Time.Second(), o.Time.Time.Second())
			assertTrueE(t, res.Ltz.Time.Equal(o.Ltz.Time))
			assertTrueE(t, res.Tz.Time.Equal(o.Tz.Time))
			assertTrueE(t, res.Ntz.Time.Equal(o.Ntz.Time))
			assertDeepEqualE(t, res.So, o.So)
			assertDeepEqualE(t, res.SArr, o.SArr)
			assertDeepEqualE(t, res.F64Arr, o.F64Arr)
			assertDeepEqualE(t, res.SomeMap, o.SomeMap)
		})
		t.Run("null", func(t *testing.T) {
			o := &objectWithAllTypesNullableSimpleScan{
				S:       sql.NullString{},
				B:       sql.NullByte{},
				I16:     sql.NullInt16{},
				I32:     sql.NullInt32{},
				I64:     sql.NullInt64{},
				F64:     sql.NullFloat64{},
				Bo:      sql.NullBool{},
				Bi:      nil,
				Date:    sql.NullTime{},
				Time:    sql.NullTime{},
				Ltz:     sql.NullTime{},
				Ntz:     sql.NullTime{},
				Tz:      sql.NullTime{},
				So:      nil,
				SArr:    nil,
				F64Arr:  nil,
				SomeMap: nil,
			}
			dbt.mustExecT(t, "INSERT INTO test_object_binding SELECT (?)", o)
			rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_object_binding WHERE obj = ?", o)
			defer rows.Close()

			assertTrueE(t, rows.Next())
			var res objectWithAllTypesNullableSimpleScan
			err := rows.Scan(&res)
			assertNilF(t, err)
			assertEqualE(t, res.S, o.S)
			assertEqualE(t, res.B, o.B)
			assertEqualE(t, res.I16, o.I16)
			assertEqualE(t, res.I32, o.I32)
			assertEqualE(t, res.I64, o.I64)
			assertEqualE(t, res.F64, o.F64)
			assertEqualE(t, res.Bo, o.Bo)
			assertDeepEqualE(t, res.Bi, o.Bi)
			assertTrueE(t, res.Date.Time.Equal(o.Date.Time))
			assertEqualE(t, res.Time.Time.Hour(), o.Time.Time.Hour())
			assertEqualE(t, res.Time.Time.Minute(), o.Time.Time.Minute())
			assertEqualE(t, res.Time.Time.Second(), o.Time.Time.Second())
			assertTrueE(t, res.Ltz.Time.Equal(o.Ltz.Time))
			assertTrueE(t, res.Tz.Time.Equal(o.Tz.Time))
			assertTrueE(t, res.Ntz.Time.Equal(o.Ntz.Time))
			assertDeepEqualE(t, res.So, o.So)
			assertDeepEqualE(t, res.SArr, o.SArr)
			assertDeepEqualE(t, res.F64Arr, o.F64Arr)
			assertDeepEqualE(t, res.SomeMap, o.SomeMap)
		})
	})
}

type objectWithAllTypesWrapper struct {
	o *objectWithAllTypes
}

func (o *objectWithAllTypesWrapper) Scan(val any) error {
	st := val.(StructuredObject)
	var owat *objectWithAllTypes
	_, err := st.GetStruct("o", owat)
	if err == nil {
		return err
	}
	o.o = owat
	return err
}

func (o *objectWithAllTypesWrapper) Write(sowc StructuredObjectWriterContext) error {
	return sowc.WriteNullableStruct("o", o.o, reflect.TypeOf(objectWithAllTypes{}))
}

func TestBindingObjectWithAllTypesNullable(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.forceJSON()
		dbt.mustExec("CREATE OR REPLACE TABLE test_object_binding (o OBJECT(o OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f32 FLOAT, f64 DOUBLE, nfraction NUMBER(38, 9), bo boolean, bi BINARY, date DATE, time TIME, ltz TIMESTAMPLTZ, tz TIMESTAMPTZ, ntz TIMESTAMPNTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR)))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_object_binding")
		}()
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		dbt.mustExec("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'")
		o := &objectWithAllTypesWrapper{}
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", o)
		rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_object_binding WHERE o = ?", o)
		defer rows.Close()

		assertTrueE(t, rows.Next())
		var res objectWithAllTypesWrapper
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertDeepEqualE(t, o, &res)
	})
}

func TestBindingObjectWithSchemaWithCustomNameAndIgnoredField(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE test_object_binding (obj OBJECT(anotherName VARCHAR))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_object_binding")
		}()
		dbt.enableStructuredTypesBinding()
		o := &objectWithCustomNameAndIgnoredField{
			SomeString: "some string",
			IgnoreMe:   "ignore me",
		}
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", o)
		rows := dbt.mustQueryContext(ctx, "SELECT * FROM test_object_binding WHERE obj = ?", o)
		defer rows.Close()

		assertTrueE(t, rows.Next())
		var res objectWithCustomNameAndIgnoredField
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertEqualE(t, res.SomeString, "some string")
		assertEqualE(t, res.IgnoreMe, "")
	})
}

func TestBindingNullStructuredObjects(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE test_object_binding (obj OBJECT(s VARCHAR, i INTEGER))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_object_binding")
		}()
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("INSERT INTO test_object_binding SELECT (?)", DataTypeNilObject, reflect.TypeOf(simpleObject{}))

		rows := dbt.mustQueryContext(ctx, "SELECT * FROM test_object_binding")
		defer rows.Close()

		assertTrueE(t, rows.Next())
		var res *simpleObject
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertNilE(t, res)
	})
}

func TestBindingArrayWithSchema(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		testcases := []struct {
			name      string
			arrayType string
			values    []any
			expected  any
		}{
			{
				name:      "byte - empty",
				arrayType: "TINYINT",
				values:    []any{[]byte{}},
				expected:  []int64{},
			},
			{
				name:      "byte - not empty",
				arrayType: "TINYINT",
				values:    []any{[]byte{1, 2, 3}},
				expected:  []int64{1, 2, 3},
			},
			{
				name:      "int16",
				arrayType: "SMALLINT",
				values:    []any{[]int16{1, 2, 3}},
				expected:  []int64{1, 2, 3},
			},
			{
				name:      "int16 - empty",
				arrayType: "SMALLINT",
				values:    []any{[]int16{}},
				expected:  []int64{},
			},
			{
				name:      "int32",
				arrayType: "INTEGER",
				values:    []any{[]int32{1, 2, 3}},
				expected:  []int64{1, 2, 3},
			},
			{
				name:      "int64",
				arrayType: "BIGINT",
				values:    []any{[]int64{1, 2, 3}},
				expected:  []int64{1, 2, 3},
			},
			{
				name:      "float32",
				arrayType: "FLOAT",
				values:    []any{[]float32{1.2, 3.4}},
				expected:  []float64{1.2, 3.4},
			},
			{
				name:      "float64",
				arrayType: "FLOAT",
				values:    []any{[]float64{1.2, 3.4}},
				expected:  []float64{1.2, 3.4},
			},
			{
				name:      "bool",
				arrayType: "BOOLEAN",
				values:    []any{[]bool{true, false}},
				expected:  []bool{true, false},
			},
			{
				name:      "binary",
				arrayType: "BINARY",
				values:    []any{DataTypeBinary, [][]byte{{'a', 'b'}, {'c', 'd'}}},
				expected:  [][]byte{{'a', 'b'}, {'c', 'd'}},
			},
			{
				name:      "binary - empty",
				arrayType: "BINARY",
				values:    []any{DataTypeBinary, [][]byte{}},
				expected:  [][]byte{},
			},
			{
				name:      "date",
				arrayType: "DATE",
				values:    []any{DataTypeDate, []time.Time{time.Date(2024, time.June, 4, 0, 0, 0, 0, time.UTC)}},
				expected:  []time.Time{time.Date(2024, time.June, 4, 0, 0, 0, 0, time.UTC)},
			},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				dbt.mustExecT(t, fmt.Sprintf("CREATE OR REPLACE TABLE test_array_binding (arr ARRAY(%s))", tc.arrayType))
				defer func() {
					dbt.mustExecT(t, "DROP TABLE IF EXISTS test_array_binding")
				}()

				dbt.mustExecT(t, "INSERT INTO test_array_binding SELECT (?)", tc.values...)

				rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_array_binding")
				defer rows.Close()

				assertTrueE(t, rows.Next())
				var res any
				err := rows.Scan(&res)
				assertNilF(t, err)
				assertDeepEqualE(t, res, tc.expected)
			})
		}
	})
}

func TestBindingArrayOfObjects(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_array_binding (arr ARRAY(OBJECT(s VARCHAR, i INTEGER)))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_array_binding")
		}()

		arr := []*simpleObject{{s: "some string", i: 123}}
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", arr)

		rows := dbt.mustQueryContext(ctx, "SELECT * FROM test_array_binding WHERE arr = ?", arr)
		defer rows.Close()

		assertTrueE(t, rows.Next())
		var res []*simpleObject
		err := rows.Scan(ScanArrayOfScanners(&res))
		assertNilF(t, err)
		assertDeepEqualE(t, res, arr)
	})
}

func TestBindingEmptyArrayOfObjects(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_array_binding (arr ARRAY(OBJECT(s VARCHAR, i INTEGER)))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_array_binding")
		}()

		arr := []*simpleObject{}
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", arr)

		rows := dbt.mustQueryContext(ctx, "SELECT * FROM test_array_binding WHERE arr = ?", arr)
		defer rows.Close()

		assertTrueF(t, rows.Next())
		var res []*simpleObject
		err := rows.Scan(ScanArrayOfScanners(&res))
		assertNilF(t, err)
		assertDeepEqualE(t, res, arr)
	})
}

func TestBindingNilArrayOfObjects(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_array_binding (arr ARRAY(OBJECT(s VARCHAR, i INTEGER)))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_array_binding")
		}()

		var arr []*simpleObject
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", DataTypeNilArray, reflect.TypeOf(simpleObject{}))

		rows := dbt.mustQueryContext(ctx, "SELECT * FROM test_array_binding")
		defer rows.Close()

		assertTrueF(t, rows.Next())
		var res []*simpleObject
		err := rows.Scan(ScanArrayOfScanners(&res))
		assertNilF(t, err)
		assertDeepEqualE(t, res, arr)
	})
}

func TestBindingNilArrayOfInts(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_array_binding (arr ARRAY(INTEGER))")
		defer func() {
			dbt.mustExec("DROP TABLE IF EXISTS test_array_binding")
		}()

		var arr *[]int64
		dbt.mustExec("INSERT INTO test_array_binding SELECT (?)", DataTypeNilArray, reflect.TypeOf(1))

		rows := dbt.mustQueryContext(ctx, "SELECT * FROM test_array_binding")
		defer rows.Close()

		assertTrueF(t, rows.Next())
		var res *[]int64
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertDeepEqualE(t, res, arr)
	})
}

func TestBindingMap(t *testing.T) {
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	ctx := WithStructuredTypesEnabled(context.Background())
	testcases := []struct {
		tableDefinition string
		values          []any
		expected        any
		isTimeOnly      bool
	}{
		{
			tableDefinition: "VARCHAR, VARCHAR",
			values: []any{map[string]string{
				"a": "b",
				"c": "d",
			}},
			expected: map[string]string{
				"a": "b",
				"c": "d",
			},
		},
		{
			tableDefinition: "INTEGER, VARCHAR",
			values: []any{map[int64]string{
				1: "b",
				2: "d",
			}},
			expected: map[int64]string{
				1: "b",
				2: "d",
			},
		},
		{
			tableDefinition: "VARCHAR, BOOLEAN",
			values: []any{map[string]bool{
				"a": true,
				"c": false,
			}},
			expected: map[string]bool{
				"a": true,
				"c": false,
			},
		},
		{
			tableDefinition: "VARCHAR, INTEGER",
			values: []any{map[string]int64{
				"a": 1,
				"b": 2,
			}},
			expected: map[string]int64{
				"a": 1,
				"b": 2,
			},
		},
		{
			tableDefinition: "VARCHAR, DOUBLE",
			values: []any{map[string]float64{
				"a": 1.1,
				"b": 2.2,
			}},
			expected: map[string]float64{
				"a": 1.1,
				"b": 2.2,
			},
		},
		{
			tableDefinition: "INTEGER, BINARY",
			values: []any{DataTypeBinary, map[int64][]byte{
				1: {'a', 'b'},
				2: {'c', 'd'},
			}},
			expected: map[int64][]byte{
				1: {'a', 'b'},
				2: {'c', 'd'},
			},
		},
		{
			tableDefinition: "VARCHAR, BINARY",
			values: []any{DataTypeBinary, map[string][]byte{
				"a": {'a', 'b'},
				"b": {'c', 'd'},
			}},
			expected: map[string][]byte{
				"a": {'a', 'b'},
				"b": {'c', 'd'},
			},
		},
		{
			tableDefinition: "VARCHAR, DATE",
			values: []any{DataTypeDate, map[string]time.Time{
				"a": time.Date(2024, time.June, 25, 0, 0, 0, 0, time.UTC),
				"b": time.Date(2024, time.June, 26, 0, 0, 0, 0, time.UTC),
			}},
			expected: map[string]time.Time{
				"a": time.Date(2024, time.June, 25, 0, 0, 0, 0, time.UTC),
				"b": time.Date(2024, time.June, 26, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			tableDefinition: "VARCHAR, TIME",
			values: []any{DataTypeTime, map[string]time.Time{
				"a": time.Date(1, time.January, 1, 11, 22, 33, 0, time.UTC),
				"b": time.Date(2, time.January, 1, 22, 11, 44, 0, time.UTC),
			}},
			expected: map[string]time.Time{
				"a": time.Date(1, time.January, 1, 11, 22, 33, 0, time.UTC),
				"b": time.Date(2, time.January, 1, 22, 11, 44, 0, time.UTC),
			},
			isTimeOnly: true,
		},
		{
			tableDefinition: "VARCHAR, TIMESTAMPNTZ",
			values: []any{DataTypeTimestampNtz, map[string]time.Time{
				"a": time.Date(2024, time.June, 25, 11, 22, 33, 0, time.UTC),
				"b": time.Date(2024, time.June, 26, 11, 22, 33, 0, time.UTC),
			}},
			expected: map[string]time.Time{
				"a": time.Date(2024, time.June, 25, 11, 22, 33, 0, time.UTC),
				"b": time.Date(2024, time.June, 26, 11, 22, 33, 0, time.UTC),
			},
		},
		{
			tableDefinition: "VARCHAR, TIMESTAMPTZ",
			values: []any{DataTypeTimestampTz, map[string]time.Time{
				"a": time.Date(2024, time.June, 25, 11, 22, 33, 0, warsawTz),
				"b": time.Date(2024, time.June, 26, 11, 22, 33, 0, warsawTz),
			}},
			expected: map[string]time.Time{
				"a": time.Date(2024, time.June, 25, 11, 22, 33, 0, warsawTz),
				"b": time.Date(2024, time.June, 26, 11, 22, 33, 0, warsawTz),
			},
		},
		{
			tableDefinition: "VARCHAR, TIMESTAMPLTZ",
			values: []any{DataTypeTimestampLtz, map[string]time.Time{
				"a": time.Date(2024, time.June, 25, 11, 22, 33, 0, warsawTz),
				"b": time.Date(2024, time.June, 26, 11, 22, 33, 0, warsawTz),
			}},
			expected: map[string]time.Time{
				"a": time.Date(2024, time.June, 25, 11, 22, 33, 0, warsawTz),
				"b": time.Date(2024, time.June, 26, 11, 22, 33, 0, warsawTz),
			},
		},
	}
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExecT(t, "ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		dbt.enableStructuredTypesBinding()
		for _, tc := range testcases {
			t.Run(tc.tableDefinition, func(t *testing.T) {
				dbt.mustExecT(t, fmt.Sprintf("CREATE OR REPLACE TABLE test_map_binding (m MAP(%v))", tc.tableDefinition))
				defer func() {
					dbt.mustExecT(t, "DROP TABLE IF EXISTS test_map_binding")
				}()

				dbt.mustExecT(t, "INSERT INTO test_map_binding SELECT (?)", tc.values...)

				rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_map_binding WHERE m = ?", tc.values...)
				defer rows.Close()

				assertTrueE(t, rows.Next())
				var res any
				err := rows.Scan(&res)
				assertNilF(t, err)
				if m, ok := tc.expected.(map[string]time.Time); ok {
					resTimes := res.(map[string]time.Time)
					for k, v := range m {
						if tc.isTimeOnly {
							assertEqualE(t, resTimes[k].Hour(), v.Hour())
							assertEqualE(t, resTimes[k].Minute(), v.Minute())
							assertEqualE(t, resTimes[k].Second(), v.Second())
						} else {
							assertTrueE(t, resTimes[k].Equal(v))
						}
					}
				} else {
					assertDeepEqualE(t, res, tc.expected)
				}
			})
		}
	})
}

func TestBindingMapOfStructs(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_map_binding (m MAP(VARCHAR, OBJECT(s VARCHAR, i INTEGER)))")
		defer func() {
			dbt.mustExecT(t, "DROP TABLE IF EXISTS test_map_binding")
		}()
		m := map[string]*simpleObject{
			"a": {"abc", 1},
			"b": nil,
			"c": {"def", 2},
		}

		dbt.mustExecT(t, "INSERT INTO test_map_binding SELECT ?", m)
		rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_map_binding WHERE m = ?", m)
		defer rows.Close()

		rows.Next()
		var res map[string]*simpleObject
		err := rows.Scan(ScanMapOfScanners(&res))
		assertNilF(t, err)
		assertDeepEqualE(t, res, m)
	})
}

func TestBindingMapOfWithAllValuesNil(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_map_binding (m MAP(VARCHAR, OBJECT(s VARCHAR, i INTEGER)))")
		defer func() {
			dbt.mustExecT(t, "DROP TABLE IF EXISTS test_map_binding")
		}()
		m := map[string]*simpleObject{
			"a": nil,
		}

		dbt.mustExecT(t, "INSERT INTO test_map_binding SELECT ?", m)
		rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_map_binding WHERE m = ?", m)
		defer rows.Close()

		rows.Next()
		var res map[string]*simpleObject
		err := rows.Scan(ScanMapOfScanners(&res))
		assertNilF(t, err)
		assertDeepEqualE(t, res, m)
	})
}

func TestBindingEmptyMapOfStructs(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_map_binding (m MAP(VARCHAR, OBJECT(s VARCHAR, i INTEGER)))")
		defer func() {
			dbt.mustExecT(t, "DROP TABLE IF EXISTS test_map_binding")
		}()

		m := map[string]*simpleObject{}
		dbt.mustExecT(t, "INSERT INTO test_map_binding SELECT ?", m)
		rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_map_binding WHERE m = ?", m)
		defer rows.Close()

		assertTrueF(t, rows.Next())
		var res map[string]*simpleObject
		err := rows.Scan(ScanMapOfScanners(&res))
		assertNilF(t, err)
		assertDeepEqualE(t, res, m)
	})
}

func TestBindingEmptyMapOfInts(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_map_binding (m MAP(VARCHAR, INTEGER))")
		defer func() {
			dbt.mustExecT(t, "DROP TABLE IF EXISTS test_map_binding")
		}()

		m := map[string]int64{}
		dbt.mustExecT(t, "INSERT INTO test_map_binding SELECT ?", m)
		rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_map_binding WHERE m = ?", m)
		defer rows.Close()

		assertTrueF(t, rows.Next())
		var res map[string]int64
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertDeepEqualE(t, res, m)
	})
}

func TestBindingNilMapOfStructs(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_map_binding (m MAP(VARCHAR, OBJECT(s VARCHAR, i INTEGER)))")
		defer func() {
			dbt.mustExecT(t, "DROP TABLE IF EXISTS test_map_binding")
		}()

		var m map[string]*simpleObject
		dbt.mustExecT(t, "INSERT INTO test_map_binding SELECT ?", DataTypeNilMap, NilMapTypes{Key: reflect.TypeOf(""), Value: reflect.TypeOf(&simpleObject{})})
		rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_map_binding", DataTypeNilMap, NilMapTypes{Key: reflect.TypeOf(""), Value: reflect.TypeOf(&simpleObject{})})
		defer rows.Close()

		assertTrueF(t, rows.Next())
		var res map[string]*simpleObject
		err := rows.Scan(ScanMapOfScanners(&res))
		assertNilF(t, err)
		assertDeepEqualE(t, res, m)
	})
}

func TestBindingNilMapOfInts(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_map_binding (m MAP(VARCHAR, INTEGER))")
		defer func() {
			dbt.mustExecT(t, "DROP TABLE IF EXISTS test_map_binding")
		}()

		var m *map[string]int64
		dbt.mustExecT(t, "INSERT INTO test_map_binding SELECT ?", DataTypeNilMap, NilMapTypes{Key: reflect.TypeOf(""), Value: reflect.TypeOf(1)})
		rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_map_binding", DataTypeNilMap, NilMapTypes{Key: reflect.TypeOf(""), Value: reflect.TypeOf(1)})
		defer rows.Close()

		assertTrueF(t, rows.Next())
		var res *map[string]int64
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertDeepEqualE(t, res, m)
	})
}

func TestBindingMapOfArrays(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypesBinding()
		dbt.mustExec("CREATE OR REPLACE TABLE test_map_binding (m MAP(VARCHAR, ARRAY(INTEGER)))")
		defer func() {
			dbt.mustExecT(t, "DROP TABLE IF EXISTS test_map_binding")
		}()

		m := map[string][]int64{
			"a": {1, 2},
			"b": nil,
		}
		dbt.mustExecT(t, "INSERT INTO test_map_binding SELECT ?", m)
		rows := dbt.mustQueryContextT(ctx, t, "SELECT * FROM test_map_binding", m)
		defer rows.Close()

		assertTrueF(t, rows.Next())
		var res map[string][]int64
		err := rows.Scan(&res)
		assertNilF(t, err)
		assertDeepEqualE(t, res, m)
	})
}

func TestBindingMapWithNillableValues(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	var testcases = []struct {
		tableDefinition string
		values          []any
		expected        any
		isTimeOnly      bool
	}{
		{
			tableDefinition: "VARCHAR, VARCHAR",
			values: []any{map[string]sql.NullString{
				"a": {String: "b", Valid: true},
				"c": {},
			}},
			expected: map[string]sql.NullString{
				"a": {String: "b", Valid: true},
				"c": {},
			},
		},
		{
			tableDefinition: "INTEGER, VARCHAR",
			values: []any{map[int64]sql.NullString{
				1: {String: "b", Valid: true},
				2: {},
			}},
			expected: map[int64]sql.NullString{
				1: {String: "b", Valid: true},
				2: {},
			},
		},
		{
			tableDefinition: "VARCHAR, BOOLEAN",
			values: []any{map[string]sql.NullBool{
				"a": {Bool: true, Valid: true},
				"c": {},
			}},
			expected: map[string]sql.NullBool{
				"a": {Bool: true, Valid: true},
				"c": {},
			},
		},
		{
			tableDefinition: "VARCHAR, INTEGER",
			values: []any{map[string]sql.NullInt64{
				"a": {Int64: 1, Valid: true},
				"b": {},
			}},
			expected: map[string]sql.NullInt64{
				"a": {Int64: 1, Valid: true},
				"b": {},
			},
		},
		{
			tableDefinition: "VARCHAR, DOUBLE",
			values: []any{map[string]sql.NullFloat64{
				"a": {Float64: 1.1, Valid: true},
				"b": {},
			}},
			expected: map[string]sql.NullFloat64{
				"a": {Float64: 1.1, Valid: true},
				"b": {},
			},
		},
		{
			tableDefinition: "INTEGER, BINARY",
			values: []any{DataTypeBinary, map[int64][]byte{
				1: {'a', 'b'},
				2: nil,
			}},
			expected: map[int64][]byte{
				1: {'a', 'b'},
				2: nil,
			},
		},
		{
			tableDefinition: "VARCHAR, BINARY",
			values: []any{DataTypeBinary, map[string][]byte{
				"a": {'a', 'b'},
				"b": nil,
			}},
			expected: map[string][]byte{
				"a": {'a', 'b'},
				"b": nil,
			},
		},
		{
			tableDefinition: "VARCHAR, DATE",
			values: []any{DataTypeDate, map[string]sql.NullTime{
				"a": {Time: time.Date(2024, time.June, 25, 0, 0, 0, 0, time.UTC), Valid: true},
				"b": {},
			}},
			expected: map[string]sql.NullTime{
				"a": {Time: time.Date(2024, time.June, 25, 0, 0, 0, 0, time.UTC), Valid: true},
				"b": {},
			},
		},
		{
			tableDefinition: "VARCHAR, TIME",
			values: []any{DataTypeTime, map[string]sql.NullTime{
				"a": {Time: time.Date(1, time.January, 1, 11, 22, 33, 0, time.UTC), Valid: true},
				"b": {},
			}},
			expected: map[string]sql.NullTime{
				"a": {Time: time.Date(1, time.January, 1, 11, 22, 33, 0, time.UTC), Valid: true},
				"b": {},
			},
			isTimeOnly: true,
		},
		{
			tableDefinition: "VARCHAR, TIMESTAMPNTZ",
			values: []any{DataTypeTimestampNtz, map[string]sql.NullTime{
				"a": {Time: time.Date(2024, time.June, 25, 11, 22, 33, 0, time.UTC), Valid: true},
				"b": {},
			}},
			expected: map[string]sql.NullTime{
				"a": {Time: time.Date(2024, time.June, 25, 11, 22, 33, 0, time.UTC), Valid: true},
				"b": {},
			},
		},
		{
			tableDefinition: "VARCHAR, TIMESTAMPTZ",
			values: []any{DataTypeTimestampTz, map[string]sql.NullTime{
				"a": {Time: time.Date(2024, time.June, 25, 11, 22, 33, 0, warsawTz), Valid: true},
				"b": {},
			}},
			expected: map[string]sql.NullTime{
				"a": {Time: time.Date(2024, time.June, 25, 11, 22, 33, 0, warsawTz), Valid: true},
				"b": {},
			},
		},
		{
			tableDefinition: "VARCHAR, TIMESTAMPLTZ",
			values: []any{DataTypeTimestampLtz, map[string]sql.NullTime{
				"a": {Time: time.Date(2024, time.June, 25, 11, 22, 33, 0, warsawTz), Valid: true},
				"b": {},
			}},
			expected: map[string]sql.NullTime{
				"a": {Time: time.Date(2024, time.June, 25, 11, 22, 33, 0, warsawTz), Valid: true},
				"b": {},
			},
		},
	}
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExecT(t, "ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		dbt.enableStructuredTypesBinding()
		for _, tc := range testcases {
			t.Run(tc.tableDefinition, func(t *testing.T) {
				dbt.mustExecT(t, fmt.Sprintf("CREATE OR REPLACE TABLE test_map_binding (m MAP(%v))", tc.tableDefinition))
				defer func() {
					dbt.mustExecT(t, "DROP TABLE IF EXISTS test_map_binding")
				}()

				dbt.mustExecT(t, "INSERT INTO test_map_binding SELECT (?)", tc.values...)

				rows := dbt.mustQueryContextT(WithMapValuesNullable(ctx), t, "SELECT * FROM test_map_binding WHERE m = ?", tc.values...)
				defer rows.Close()

				assertTrueE(t, rows.Next())
				var res any
				err := rows.Scan(&res)
				assertNilF(t, err)
				if m, ok := tc.expected.(map[string]sql.NullTime); ok {
					resTimes := res.(map[string]sql.NullTime)
					for k, v := range m {
						if tc.isTimeOnly {
							assertEqualE(t, resTimes[k].Valid, v.Valid)
							assertEqualE(t, resTimes[k].Time.Hour(), v.Time.Hour())
							assertEqualE(t, resTimes[k].Time.Minute(), v.Time.Minute())
							assertEqualE(t, resTimes[k].Time.Second(), v.Time.Second())
						} else {
							assertEqualE(t, resTimes[k].Valid, v.Valid)
							if v.Valid {
								assertTrueE(t, resTimes[k].Time.Equal(v.Time))
							}
						}
					}
				} else {
					assertDeepEqualE(t, res, tc.expected)
				}
			})
		}
	})
}
