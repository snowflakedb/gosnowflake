package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type objectWithAllTypes struct {
	s         string
	b         byte
	i16       int16
	i32       int32
	i64       int64
	f32       float32
	f64       float64
	nfraction float64
	bo        bool
	bi        []byte
	date      time.Time `sf:"date,date"`
	time      time.Time `sf:"time,time"`
	ltz       time.Time `sf:"ltz,ltz"`
	tz        time.Time `sf:"tz,tz"`
	ntz       time.Time `sf:"ntz,ntz"`
	so        *simpleObject
	sArr      []string
	f64Arr    []float64
	someMap   map[string]bool
	uuid      testUUID
}

func (o *objectWithAllTypes) Scan(val any) error {
	st, ok := val.(StructuredObject)
	if !ok {
		return fmt.Errorf("expected StructuredObject, got %T", val)
	}

	var err error
	if o.s, err = st.GetString("s"); err != nil {
		return err
	}
	if o.b, err = st.GetByte("b"); err != nil {
		return err
	}
	if o.i16, err = st.GetInt16("i16"); err != nil {
		return err
	}
	if o.i32, err = st.GetInt32("i32"); err != nil {
		return err
	}
	if o.i64, err = st.GetInt64("i64"); err != nil {
		return err
	}
	if o.f32, err = st.GetFloat32("f32"); err != nil {
		return err
	}
	if o.f64, err = st.GetFloat64("f64"); err != nil {
		return err
	}
	if o.nfraction, err = st.GetFloat64("nfraction"); err != nil {
		return err
	}
	if o.bo, err = st.GetBool("bo"); err != nil {
		return err
	}
	if o.bi, err = st.GetBytes("bi"); err != nil {
		return err
	}
	if o.date, err = st.GetTime("date"); err != nil {
		return err
	}
	if o.time, err = st.GetTime("time"); err != nil {
		return err
	}
	if o.ltz, err = st.GetTime("ltz"); err != nil {
		return err
	}
	if o.tz, err = st.GetTime("tz"); err != nil {
		return err
	}
	if o.ntz, err = st.GetTime("ntz"); err != nil {
		return err
	}
	so, err := st.GetStruct("so", &simpleObject{})
	if err != nil {
		return err
	}
	o.so = so.(*simpleObject)
	sArr, err := st.GetRaw("sArr")
	if err != nil {
		return err
	}
	if sArr != nil {
		o.sArr = sArr.([]string)
	}
	f64Arr, err := st.GetRaw("f64Arr")
	if err != nil {
		return err
	}
	if f64Arr != nil {
		o.f64Arr = f64Arr.([]float64)
	}
	someMap, err := st.GetRaw("someMap")
	if err != nil {
		return err
	}
	if someMap != nil {
		o.someMap = someMap.(map[string]bool)
	}
	uuidStr, err := st.GetString("uuid")
	if err != nil {
		return err
	}

	o.uuid = parseTestUUID(uuidStr)

	return nil
}

func (o objectWithAllTypes) Write(sowc StructuredObjectWriterContext) error {
	if err := sowc.WriteString("s", o.s); err != nil {
		return err
	}
	if err := sowc.WriteByt("b", o.b); err != nil {
		return err
	}
	if err := sowc.WriteInt16("i16", o.i16); err != nil {
		return err
	}
	if err := sowc.WriteInt32("i32", o.i32); err != nil {
		return err
	}
	if err := sowc.WriteInt64("i64", o.i64); err != nil {
		return err
	}
	if err := sowc.WriteFloat32("f32", o.f32); err != nil {
		return err
	}
	if err := sowc.WriteFloat64("f64", o.f64); err != nil {
		return err
	}
	if err := sowc.WriteFloat64("nfraction", o.nfraction); err != nil {
		return err
	}
	if err := sowc.WriteBool("bo", o.bo); err != nil {
		return err
	}
	if err := sowc.WriteBytes("bi", o.bi); err != nil {
		return err
	}
	if err := sowc.WriteTime("date", o.date, DataTypeDate); err != nil {
		return err
	}
	if err := sowc.WriteTime("time", o.time, DataTypeTime); err != nil {
		return err
	}
	if err := sowc.WriteTime("ltz", o.ltz, DataTypeTimestampLtz); err != nil {
		return err
	}
	if err := sowc.WriteTime("ntz", o.ntz, DataTypeTimestampNtz); err != nil {
		return err
	}
	if err := sowc.WriteTime("tz", o.tz, DataTypeTimestampTz); err != nil {
		return err
	}
	if err := sowc.WriteStruct("so", o.so); err != nil {
		return err
	}
	if err := sowc.WriteRaw("sArr", o.sArr); err != nil {
		return err
	}
	if err := sowc.WriteRaw("f64Arr", o.f64Arr); err != nil {
		return err
	}
	if err := sowc.WriteRaw("someMap", o.someMap); err != nil {
		return err
	}
	if err := sowc.WriteString("uuid", o.uuid.String()); err != nil {
		return err
	}
	return nil
}

type simpleObject struct {
	s string
	i int32
}

func (so *simpleObject) Scan(val any) error {
	st, ok := val.(StructuredObject)
	if !ok {
		return fmt.Errorf("expected StructuredObject, got %T", val)
	}

	var err error
	if so.s, err = st.GetString("s"); err != nil {
		return err
	}
	if so.i, err = st.GetInt32("i"); err != nil {
		return err
	}
	return nil
}

func (so *simpleObject) Write(sowc StructuredObjectWriterContext) error {
	if err := sowc.WriteString("s", so.s); err != nil {
		return err
	}
	if err := sowc.WriteInt32("i", so.i); err != nil {
		return err
	}
	return nil
}

func TestObjectWithAllTypesAsString(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			skipForStringingNativeArrow(t, format)
			rows := dbt.mustQuery("SELECT {'s': 'some string', 'i32': 3}::OBJECT(s VARCHAR, i32 INTEGER)")
			defer rows.Close()
			assertTrueF(t, rows.Next())
			var res string
			err := rows.Scan(&res)
			assertNilF(t, err)
			assertEqualIgnoringWhitespaceE(t, res, `{"s": "some string", "i32": 3}`)
		})
	})
}

func TestObjectWithAllTypesAsObject(t *testing.T) {
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			uid := newTestUUID()
			rows := dbt.mustQueryContextT(ctx, t, fmt.Sprintf("SELECT 1, {'s': 'some string', 'b': 1, 'i16': 2, 'i32': 3, 'i64': 9223372036854775807, 'f32': '1.1', 'f64': 2.2, 'nfraction': 3.3, 'bo': true, 'bi': TO_BINARY('616263', 'HEX'), 'date': '2024-03-21'::DATE, 'time': '13:03:02'::TIME, 'ltz': '2021-07-21 11:22:33'::TIMESTAMP_LTZ, 'tz': '2022-08-31 13:43:22 +0200'::TIMESTAMP_TZ, 'ntz': '2023-05-22 01:17:19'::TIMESTAMP_NTZ, 'so': {'s': 'child', 'i': 9}, 'sArr': ARRAY_CONSTRUCT('x', 'y', 'z'), 'f64Arr': ARRAY_CONSTRUCT(1.1, 2.2, 3.3), 'someMap': {'x': true, 'y': false}, 'uuid': '%s'}::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f32 FLOAT, f64 DOUBLE, nfraction NUMBER(38, 19), bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR)", uid))
			defer rows.Close()
			rows.Next()
			var ignore int
			var res objectWithAllTypes
			err := rows.Scan(&ignore, &res)
			assertNilF(t, err)
			assertEqualE(t, res.s, "some string")
			assertEqualE(t, res.b, byte(1))
			assertEqualE(t, res.i16, int16(2))
			assertEqualE(t, res.i32, int32(3))
			assertEqualE(t, res.i64, int64(9223372036854775807))
			assertEqualE(t, res.f32, float32(1.1))
			assertEqualE(t, res.f64, 2.2)
			assertEqualE(t, res.nfraction, 3.3)
			assertEqualE(t, res.bo, true)
			assertBytesEqualE(t, res.bi, []byte{'a', 'b', 'c'})
			assertEqualE(t, res.date, time.Date(2024, time.March, 21, 0, 0, 0, 0, time.UTC))
			assertEqualE(t, res.time.Hour(), 13)
			assertEqualE(t, res.time.Minute(), 3)
			assertEqualE(t, res.time.Second(), 2)
			assertTrueE(t, res.ltz.Equal(time.Date(2021, time.July, 21, 11, 22, 33, 0, warsawTz)))
			assertTrueE(t, res.tz.Equal(time.Date(2022, time.August, 31, 13, 43, 22, 0, warsawTz)))
			assertTrueE(t, res.ntz.Equal(time.Date(2023, time.May, 22, 1, 17, 19, 0, time.UTC)))
			assertDeepEqualE(t, res.so, &simpleObject{s: "child", i: 9})
			assertDeepEqualE(t, res.sArr, []string{"x", "y", "z"})
			assertDeepEqualE(t, res.f64Arr, []float64{1.1, 2.2, 3.3})
			assertDeepEqualE(t, res.someMap, map[string]bool{"x": true, "y": false})
			assertEqualE(t, res.uuid.String(), uid.String())
		})
	})
}

func TestNullObject(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			t.Run("null", func(t *testing.T) {
				rows := dbt.mustQueryContextT(ctx, t, "SELECT null::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f32 FLOAT, f64 DOUBLE, nfraction NUMBER(38, 19), bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR)")
				defer rows.Close()
				assertTrueF(t, rows.Next())
				var res *objectWithAllTypes
				err := rows.Scan(&res)
				assertNilF(t, err)
				assertNilE(t, res)
			})
			t.Run("not null", func(t *testing.T) {
				uid := newTestUUID()
				rows := dbt.mustQueryContextT(ctx, t, fmt.Sprintf("SELECT {'s': 'some string', 'b': 1, 'i16': 2, 'i32': 3, 'i64': 9223372036854775807, 'f32': '1.1', 'f64': 2.2, 'nfraction': 3.3, 'bo': true, 'bi': TO_BINARY('616263', 'HEX'), 'date': '2024-03-21'::DATE, 'time': '13:03:02'::TIME, 'ltz': '2021-07-21 11:22:33'::TIMESTAMP_LTZ, 'tz': '2022-08-31 13:43:22 +0200'::TIMESTAMP_TZ, 'ntz': '2023-05-22 01:17:19'::TIMESTAMP_NTZ, 'so': {'s': 'child', 'i': 9}, 'sArr': ARRAY_CONSTRUCT('x', 'y', 'z'), 'f64Arr': ARRAY_CONSTRUCT(1.1, 2.2, 3.3), 'someMap': {'x': true, 'y': false}, 'uuid': '%s'}::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f32 FLOAT, f64 DOUBLE, nfraction NUMBER(38, 19), bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR)", uid))
				defer rows.Close()
				assertTrueF(t, rows.Next())
				var res *objectWithAllTypes
				err := rows.Scan(&res)
				assertNilF(t, err)
				assertEqualE(t, res.s, "some string")
			})
		})
	})
}

type objectWithAllTypesNullable struct {
	s       sql.NullString
	b       sql.NullByte
	i16     sql.NullInt16
	i32     sql.NullInt32
	i64     sql.NullInt64
	f64     sql.NullFloat64
	bo      sql.NullBool
	bi      []byte
	date    sql.NullTime
	time    sql.NullTime
	ltz     sql.NullTime
	tz      sql.NullTime
	ntz     sql.NullTime
	so      *simpleObject
	sArr    []string
	f64Arr  []float64
	someMap map[string]bool
	uuid    testUUID
}

func (o *objectWithAllTypesNullable) Scan(val any) error {
	st, ok := val.(StructuredObject)
	if !ok {
		return fmt.Errorf("expected StructuredObject, got %T", val)
	}

	var err error
	if o.s, err = st.GetNullString("s"); err != nil {
		return err
	}
	if o.b, err = st.GetNullByte("b"); err != nil {
		return err
	}
	if o.i16, err = st.GetNullInt16("i16"); err != nil {
		return err
	}
	if o.i32, err = st.GetNullInt32("i32"); err != nil {
		return err
	}
	if o.i64, err = st.GetNullInt64("i64"); err != nil {
		return err
	}
	if o.f64, err = st.GetNullFloat64("f64"); err != nil {
		return err
	}
	if o.bo, err = st.GetNullBool("bo"); err != nil {
		return err
	}
	if o.bi, err = st.GetBytes("bi"); err != nil {
		return err
	}
	if o.date, err = st.GetNullTime("date"); err != nil {
		return err
	}
	if o.time, err = st.GetNullTime("time"); err != nil {
		return err
	}
	if o.ltz, err = st.GetNullTime("ltz"); err != nil {
		return err
	}
	if o.tz, err = st.GetNullTime("tz"); err != nil {
		return err
	}
	if o.ntz, err = st.GetNullTime("ntz"); err != nil {
		return err
	}
	so, err := st.GetStruct("so", &simpleObject{})
	if err != nil {
		return err
	}
	if so != nil {
		o.so = so.(*simpleObject)
	} else {
		o.so = nil
	}
	sArr, err := st.GetRaw("sArr")
	if err != nil {
		return err
	}
	if sArr != nil {
		o.sArr = sArr.([]string)
	}
	f64Arr, err := st.GetRaw("f64Arr")
	if err != nil {
		return err
	}
	if f64Arr != nil {
		o.f64Arr = f64Arr.([]float64)
	}
	someMap, err := st.GetRaw("someMap")
	if err != nil {
		return err
	}
	if someMap != nil {
		o.someMap = someMap.(map[string]bool)
	}
	uuidStr, err := st.GetNullString("uuid")
	if err != nil {
		return err
	}

	o.uuid = parseTestUUID(uuidStr.String)

	return nil
}

func (o *objectWithAllTypesNullable) Write(sowc StructuredObjectWriterContext) error {
	if err := sowc.WriteNullString("s", o.s); err != nil {
		return err
	}
	if err := sowc.WriteNullByte("b", o.b); err != nil {
		return err
	}
	if err := sowc.WriteNullInt16("i16", o.i16); err != nil {
		return err
	}
	if err := sowc.WriteNullInt32("i32", o.i32); err != nil {
		return err
	}
	if err := sowc.WriteNullInt64("i64", o.i64); err != nil {
		return err
	}
	if err := sowc.WriteNullFloat64("f64", o.f64); err != nil {
		return err
	}
	if err := sowc.WriteNullBool("bo", o.bo); err != nil {
		return err
	}
	if err := sowc.WriteBytes("bi", o.bi); err != nil {
		return err
	}
	if err := sowc.WriteNullTime("date", o.date, DataTypeDate); err != nil {
		return err
	}
	if err := sowc.WriteNullTime("time", o.time, DataTypeTime); err != nil {
		return err
	}
	if err := sowc.WriteNullTime("ltz", o.ltz, DataTypeTimestampLtz); err != nil {
		return err
	}
	if err := sowc.WriteNullTime("ntz", o.ntz, DataTypeTimestampNtz); err != nil {
		return err
	}
	if err := sowc.WriteNullTime("tz", o.tz, DataTypeTimestampTz); err != nil {
		return err
	}
	if err := sowc.WriteNullableStruct("so", o.so, reflect.TypeOf(simpleObject{})); err != nil {
		return err
	}
	if err := sowc.WriteRaw("sArr", o.sArr); err != nil {
		return err
	}
	if err := sowc.WriteRaw("f64Arr", o.f64Arr); err != nil {
		return err
	}
	if err := sowc.WriteRaw("someMap", o.someMap); err != nil {
		return err
	}
	if err := sowc.WriteNullString("uuid", sql.NullString{String: o.uuid.String(), Valid: true}); err != nil {
		return err
	}
	return nil
}

func TestObjectWithAllTypesNullable(t *testing.T) {
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			t.Run("null", func(t *testing.T) {
				rows := dbt.mustQueryContextT(ctx, t, "select null, object_construct_keep_null('s', null, 'b', null, 'i16', null, 'i32', null, 'i64', null, 'f64', null, 'bo', null, 'bi', null, 'date', null, 'time', null, 'ltz', null, 'tz', null, 'ntz', null, 'so', null, 'sArr', null, 'f64Arr', null, 'someMap', null, 'uuid', null)::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f64 DOUBLE, bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR)")
				defer rows.Close()
				assertTrueF(t, rows.Next())
				var ignore sql.NullInt32
				var res objectWithAllTypesNullable
				err := rows.Scan(&ignore, &res)
				assertNilF(t, err)
				assertEqualE(t, ignore, sql.NullInt32{Valid: false})
				assertEqualE(t, res.s, sql.NullString{Valid: false})
				assertEqualE(t, res.b, sql.NullByte{Valid: false})
				assertEqualE(t, res.i16, sql.NullInt16{Valid: false})
				assertEqualE(t, res.i32, sql.NullInt32{Valid: false})
				assertEqualE(t, res.i64, sql.NullInt64{Valid: false})
				assertEqualE(t, res.f64, sql.NullFloat64{Valid: false})
				assertEqualE(t, res.bo, sql.NullBool{Valid: false})
				assertBytesEqualE(t, res.bi, nil)
				assertEqualE(t, res.date, sql.NullTime{Valid: false})
				assertEqualE(t, res.time, sql.NullTime{Valid: false})
				assertEqualE(t, res.ltz, sql.NullTime{Valid: false})
				assertEqualE(t, res.tz, sql.NullTime{Valid: false})
				assertEqualE(t, res.ntz, sql.NullTime{Valid: false})
				var so *simpleObject
				assertDeepEqualE(t, res.so, so)
				assertEqualE(t, res.uuid, testUUID{})
			})
			t.Run("not null", func(t *testing.T) {
				uuid := newTestUUID()
				rows := dbt.mustQueryContextT(ctx, t, fmt.Sprintf("select 1, object_construct_keep_null('s', 'abc', 'b', 1, 'i16', 2, 'i32', 3, 'i64', 9223372036854775807, 'f64', 2.2, 'bo', true, 'bi', TO_BINARY('616263', 'HEX'), 'date', '2024-03-21'::DATE, 'time', '13:03:02'::TIME, 'ltz', '2021-07-21 11:22:33'::TIMESTAMP_LTZ, 'tz', '2022-08-31 13:43:22 +0200'::TIMESTAMP_TZ, 'ntz', '2023-05-22 01:17:19'::TIMESTAMP_NTZ, 'so', {'s': 'child', 'i': 9}::OBJECT, 'sArr', ARRAY_CONSTRUCT('x', 'y', 'z'), 'f64Arr', ARRAY_CONSTRUCT(1.1, 2.2, 3.3), 'someMap', {'x': true, 'y': false}, 'uuid', '%s')::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f64 DOUBLE, bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR)", uuid))
				defer rows.Close()
				rows.Next()
				var ignore sql.NullInt32
				var res objectWithAllTypesNullable
				err := rows.Scan(&ignore, &res)
				assertNilF(t, err)
				assertEqualE(t, ignore, sql.NullInt32{Valid: true, Int32: 1})
				assertEqualE(t, res.s, sql.NullString{Valid: true, String: "abc"})
				assertEqualE(t, res.b, sql.NullByte{Valid: true, Byte: byte(1)})
				assertEqualE(t, res.i16, sql.NullInt16{Valid: true, Int16: int16(2)})
				assertEqualE(t, res.i32, sql.NullInt32{Valid: true, Int32: 3})
				assertEqualE(t, res.i64, sql.NullInt64{Valid: true, Int64: 9223372036854775807})
				assertEqualE(t, res.f64, sql.NullFloat64{Valid: true, Float64: 2.2})
				assertEqualE(t, res.bo, sql.NullBool{Valid: true, Bool: true})
				assertBytesEqualE(t, res.bi, []byte{'a', 'b', 'c'})
				assertEqualE(t, res.date, sql.NullTime{Valid: true, Time: time.Date(2024, time.March, 21, 0, 0, 0, 0, time.UTC)})
				assertTrueE(t, res.time.Valid)
				assertEqualE(t, res.time.Time.Hour(), 13)
				assertEqualE(t, res.time.Time.Minute(), 3)
				assertEqualE(t, res.time.Time.Second(), 2)
				assertTrueE(t, res.ltz.Valid)
				assertTrueE(t, res.ltz.Time.Equal(time.Date(2021, time.July, 21, 11, 22, 33, 0, warsawTz)))
				assertTrueE(t, res.tz.Valid)
				assertTrueE(t, res.tz.Time.Equal(time.Date(2022, time.August, 31, 13, 43, 22, 0, warsawTz)))
				assertTrueE(t, res.ntz.Valid)
				assertTrueE(t, res.ntz.Time.Equal(time.Date(2023, time.May, 22, 1, 17, 19, 0, time.UTC)))
				assertDeepEqualE(t, res.so, &simpleObject{s: "child", i: 9})
				assertDeepEqualE(t, res.sArr, []string{"x", "y", "z"})
				assertDeepEqualE(t, res.f64Arr, []float64{1.1, 2.2, 3.3})
				assertDeepEqualE(t, res.someMap, map[string]bool{"x": true, "y": false})
				assertEqualE(t, res.uuid.String(), uuid.String())
			})
		})
	})
}

type objectWithAllTypesSimpleScan struct {
	S         string
	B         byte
	I16       int16
	I32       int32
	I64       int64
	F32       float32
	F64       float64
	Nfraction float64
	Bo        bool
	Bi        []byte
	Date      time.Time `sf:"date,date"`
	Time      time.Time `sf:"time,time"`
	Ltz       time.Time `sf:"ltz,ltz"`
	Tz        time.Time `sf:"tz,tz"`
	Ntz       time.Time `sf:"ntz,ntz"`
	So        *simpleObject
	SArr      []string
	F64Arr    []float64
	SomeMap   map[string]bool
}

func (so *objectWithAllTypesSimpleScan) Scan(val any) error {
	st, ok := val.(StructuredObject)
	if !ok {
		return fmt.Errorf("expected StructuredObject, got %T", val)
	}

	return st.ScanTo(so)
}

func (so *objectWithAllTypesSimpleScan) Write(sowc StructuredObjectWriterContext) error {
	return sowc.WriteAll(so)
}

func TestObjectWithAllTypesSimpleScan(t *testing.T) {
	uid := newTestUUID()
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQueryContextT(ctx, t, fmt.Sprintf("SELECT 1, {'s': 'some string', 'b': 1, 'i16': 2, 'i32': 3, 'i64': 9223372036854775807, 'f32': '1.1', 'f64': 2.2, 'nfraction': 3.3, 'bo': true, 'bi': TO_BINARY('616263', 'HEX'), 'date': '2024-03-21'::DATE, 'time': '13:03:02'::TIME, 'ltz': '2021-07-21 11:22:33'::TIMESTAMP_LTZ, 'tz': '2022-08-31 13:43:22 +0200'::TIMESTAMP_TZ, 'ntz': '2023-05-22 01:17:19'::TIMESTAMP_NTZ, 'so': {'s': 'child', 'i': 9}, 'sArr': ARRAY_CONSTRUCT('x', 'y', 'z'), 'f64Arr': ARRAY_CONSTRUCT(1.1, 2.2, 3.3), 'someMap': {'x': true, 'y': false}, 'uuid': '%s'}::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f32 FLOAT, f64 DOUBLE, nfraction NUMBER(38, 19), bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR)", uid))
			defer rows.Close()
			rows.Next()
			var ignore int
			var res objectWithAllTypesSimpleScan
			err := rows.Scan(&ignore, &res)
			assertNilF(t, err)
			assertEqualE(t, res.S, "some string")
			assertEqualE(t, res.B, byte(1))
			assertEqualE(t, res.I16, int16(2))
			assertEqualE(t, res.I32, int32(3))
			assertEqualE(t, res.I64, int64(9223372036854775807))
			assertEqualE(t, res.F32, float32(1.1))
			assertEqualE(t, res.F64, 2.2)
			assertEqualE(t, res.Nfraction, 3.3)
			assertEqualE(t, res.Bo, true)
			assertBytesEqualE(t, res.Bi, []byte{'a', 'b', 'c'})
			assertEqualE(t, res.Date, time.Date(2024, time.March, 21, 0, 0, 0, 0, time.UTC))
			assertEqualE(t, res.Time.Hour(), 13)
			assertEqualE(t, res.Time.Minute(), 3)
			assertEqualE(t, res.Time.Second(), 2)
			assertTrueE(t, res.Ltz.Equal(time.Date(2021, time.July, 21, 11, 22, 33, 0, warsawTz)))
			assertTrueE(t, res.Tz.Equal(time.Date(2022, time.August, 31, 13, 43, 22, 0, warsawTz)))
			assertTrueE(t, res.Ntz.Equal(time.Date(2023, time.May, 22, 1, 17, 19, 0, time.UTC)))
			assertDeepEqualE(t, res.So, &simpleObject{s: "child", i: 9})
			assertDeepEqualE(t, res.SArr, []string{"x", "y", "z"})
			assertDeepEqualE(t, res.F64Arr, []float64{1.1, 2.2, 3.3})
			assertDeepEqualE(t, res.SomeMap, map[string]bool{"x": true, "y": false})
		})
	})
}

func TestNullObjectSimpleScan(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			t.Run("null", func(t *testing.T) {
				rows := dbt.mustQueryContextT(ctx, t, "SELECT null::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f32 FLOAT, f64 DOUBLE, nfraction NUMBER(38, 19), bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR)")
				defer rows.Close()
				assertTrueF(t, rows.Next())
				var res *objectWithAllTypesSimpleScan
				err := rows.Scan(&res)
				assertNilF(t, err)
				assertNilE(t, res)
			})
			t.Run("not null", func(t *testing.T) {
				uid := newTestUUID()
				rows := dbt.mustQueryContextT(ctx, t, fmt.Sprintf("SELECT {'s': 'some string', 'b': 1, 'i16': 2, 'i32': 3, 'i64': 9223372036854775807, 'f32': '1.1', 'f64': 2.2, 'nfraction': 3.3, 'bo': true, 'bi': TO_BINARY('616263', 'HEX'), 'date': '2024-03-21'::DATE, 'time': '13:03:02'::TIME, 'ltz': '2021-07-21 11:22:33'::TIMESTAMP_LTZ, 'tz': '2022-08-31 13:43:22 +0200'::TIMESTAMP_TZ, 'ntz': '2023-05-22 01:17:19'::TIMESTAMP_NTZ, 'so': {'s': 'child', 'i': 9}, 'sArr': ARRAY_CONSTRUCT('x', 'y', 'z'), 'f64Arr': ARRAY_CONSTRUCT(1.1, 2.2, 3.3), 'someMap': {'x': true, 'y': false}, 'uuid': '%s'}::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f32 FLOAT, f64 DOUBLE, nfraction NUMBER(38, 19), bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR)", uid))
				defer rows.Close()
				assertTrueF(t, rows.Next())
				var res *objectWithAllTypesSimpleScan
				err := rows.Scan(&res)
				assertNilF(t, err)
				assertEqualE(t, res.S, "some string")
			})
		})
	})
}

type objectWithAllTypesNullableSimpleScan struct {
	S       sql.NullString
	B       sql.NullByte
	I16     sql.NullInt16
	I32     sql.NullInt32
	I64     sql.NullInt64
	F64     sql.NullFloat64
	Bo      sql.NullBool
	Bi      []byte
	Date    sql.NullTime `sf:"date,date"`
	Time    sql.NullTime `sf:"time,time"`
	Ltz     sql.NullTime `sf:"ltz,ltz"`
	Tz      sql.NullTime `sf:"tz,tz"`
	Ntz     sql.NullTime `sf:"ntz,ntz"`
	So      *simpleObject
	SArr    []string
	F64Arr  []float64
	SomeMap map[string]bool
}

func (o *objectWithAllTypesNullableSimpleScan) Scan(val any) error {
	st, ok := val.(StructuredObject)
	if !ok {
		return fmt.Errorf("expected StructuredObject, got %T", val)
	}

	return st.ScanTo(o)
}

func (o *objectWithAllTypesNullableSimpleScan) Write(sowc StructuredObjectWriterContext) error {
	return sowc.WriteAll(o)
}

func TestObjectWithAllTypesSimpleScanNullable(t *testing.T) {
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			t.Run("null", func(t *testing.T) {
				rows := dbt.mustQueryContextT(ctx, t, "select null, object_construct_keep_null('s', null, 'b', null, 'i16', null, 'i32', null, 'i64', null, 'f64', null, 'bo', null, 'bi', null, 'date', null, 'time', null, 'ltz', null, 'tz', null, 'ntz', null, 'so', null, 'sArr', null, 'f64Arr', null, 'someMap', null)::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f64 DOUBLE, bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN))")
				defer rows.Close()
				rows.Next()
				var ignore sql.NullInt32
				var res objectWithAllTypesNullableSimpleScan
				err := rows.Scan(&ignore, &res)
				assertNilF(t, err)
				assertEqualE(t, ignore, sql.NullInt32{Valid: false})
				assertEqualE(t, res.S, sql.NullString{Valid: false})
				assertEqualE(t, res.B, sql.NullByte{Valid: false})
				assertEqualE(t, res.I16, sql.NullInt16{Valid: false})
				assertEqualE(t, res.I32, sql.NullInt32{Valid: false})
				assertEqualE(t, res.I64, sql.NullInt64{Valid: false})
				assertEqualE(t, res.F64, sql.NullFloat64{Valid: false})
				assertEqualE(t, res.Bo, sql.NullBool{Valid: false})
				assertBytesEqualE(t, res.Bi, nil)
				assertEqualE(t, res.Date, sql.NullTime{Valid: false})
				assertEqualE(t, res.Time, sql.NullTime{Valid: false})
				assertEqualE(t, res.Ltz, sql.NullTime{Valid: false})
				assertEqualE(t, res.Tz, sql.NullTime{Valid: false})
				assertEqualE(t, res.Ntz, sql.NullTime{Valid: false})
				var so *simpleObject
				assertDeepEqualE(t, res.So, so)
			})
			t.Run("not null", func(t *testing.T) {
				uuid := newTestUUID()
				rows := dbt.mustQueryContextT(ctx, t, fmt.Sprintf("select 1, object_construct_keep_null('s', 'abc', 'b', 1, 'i16', 2, 'i32', 3, 'i64', 9223372036854775807, 'f64', 2.2, 'bo', true, 'bi', TO_BINARY('616263', 'HEX'), 'date', '2024-03-21'::DATE, 'time', '13:03:02'::TIME, 'ltz', '2021-07-21 11:22:33'::TIMESTAMP_LTZ, 'tz', '2022-08-31 13:43:22 +0200'::TIMESTAMP_TZ, 'ntz', '2023-05-22 01:17:19'::TIMESTAMP_NTZ, 'so', {'s': 'child', 'i': 9}::OBJECT, 'sArr', ARRAY_CONSTRUCT('x', 'y', 'z'), 'f64Arr', ARRAY_CONSTRUCT(1.1, 2.2, 3.3), 'someMap', {'x': true, 'y': false}, 'uuid', '%s')::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, f64 DOUBLE, bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER), sArr ARRAY(VARCHAR), f64Arr ARRAY(DOUBLE), someMap MAP(VARCHAR, BOOLEAN), uuid VARCHAR)", uuid))
				defer rows.Close()
				rows.Next()
				var ignore sql.NullInt32
				var res objectWithAllTypesNullableSimpleScan
				err := rows.Scan(&ignore, &res)
				assertNilF(t, err)
				assertEqualE(t, ignore, sql.NullInt32{Valid: true, Int32: 1})
				assertEqualE(t, res.S, sql.NullString{Valid: true, String: "abc"})
				assertEqualE(t, res.B, sql.NullByte{Valid: true, Byte: byte(1)})
				assertEqualE(t, res.I16, sql.NullInt16{Valid: true, Int16: int16(2)})
				assertEqualE(t, res.I32, sql.NullInt32{Valid: true, Int32: 3})
				assertEqualE(t, res.I64, sql.NullInt64{Valid: true, Int64: 9223372036854775807})
				assertEqualE(t, res.F64, sql.NullFloat64{Valid: true, Float64: 2.2})
				assertEqualE(t, res.Bo, sql.NullBool{Valid: true, Bool: true})
				assertBytesEqualE(t, res.Bi, []byte{'a', 'b', 'c'})
				assertEqualE(t, res.Date, sql.NullTime{Valid: true, Time: time.Date(2024, time.March, 21, 0, 0, 0, 0, time.UTC)})
				assertTrueE(t, res.Time.Valid)
				assertEqualE(t, res.Time.Time.Hour(), 13)
				assertEqualE(t, res.Time.Time.Minute(), 3)
				assertEqualE(t, res.Time.Time.Second(), 2)
				assertTrueE(t, res.Ltz.Valid)
				assertTrueE(t, res.Ltz.Time.Equal(time.Date(2021, time.July, 21, 11, 22, 33, 0, warsawTz)))
				assertTrueE(t, res.Tz.Valid)
				assertTrueE(t, res.Tz.Time.Equal(time.Date(2022, time.August, 31, 13, 43, 22, 0, warsawTz)))
				assertTrueE(t, res.Ntz.Valid)
				assertTrueE(t, res.Ntz.Time.Equal(time.Date(2023, time.May, 22, 1, 17, 19, 0, time.UTC)))
				assertDeepEqualE(t, res.So, &simpleObject{s: "child", i: 9})
				assertDeepEqualE(t, res.SArr, []string{"x", "y", "z"})
				assertDeepEqualE(t, res.F64Arr, []float64{1.1, 2.2, 3.3})
				assertDeepEqualE(t, res.SomeMap, map[string]bool{"x": true, "y": false})
			})
		})
	})
}

type objectWithCustomNameAndIgnoredField struct {
	SomeString string `sf:"anotherName"`
	IgnoreMe   string `sf:"ignoreMe,ignore"`
}

func (o *objectWithCustomNameAndIgnoredField) Scan(val any) error {
	st, ok := val.(StructuredObject)
	if !ok {
		return fmt.Errorf("expected StructuredObject, got %T", val)
	}

	return st.ScanTo(o)
}

func (o *objectWithCustomNameAndIgnoredField) Write(sowc StructuredObjectWriterContext) error {
	return sowc.WriteAll(o)
}

func TestObjectWithCustomName(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQueryContextT(ctx, t, "SELECT {'anotherName': 'some string'}::OBJECT(anotherName VARCHAR)")
			defer rows.Close()
			rows.Next()
			var res objectWithCustomNameAndIgnoredField
			err := rows.Scan(&res)
			assertNilF(t, err)
			assertEqualE(t, res.SomeString, "some string")
			assertEqualE(t, res.IgnoreMe, "")
		})
	})
}

func TestObjectMetadataAsObject(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQueryContextT(ctx, t, "SELECT {'a': 'b'}::OBJECT(a VARCHAR) as structured_type")
			defer rows.Close()
			columnTypes, err := rows.ColumnTypes()
			assertNilF(t, err)
			assertEqualE(t, len(columnTypes), 1)
			assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(ObjectType{}))
			assertEqualE(t, columnTypes[0].DatabaseTypeName(), "OBJECT")
			assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
		})
	})
}

func TestObjectMetadataAsString(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			skipForStringingNativeArrow(t, format)
			rows := dbt.mustQueryT(t, "SELECT {'a': 'b'}::OBJECT(a VARCHAR) as structured_type")
			defer rows.Close()
			columnTypes, err := rows.ColumnTypes()
			assertNilF(t, err)
			assertEqualE(t, len(columnTypes), 1)
			assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(""))
			assertEqualE(t, columnTypes[0].DatabaseTypeName(), "OBJECT")
			assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
		})
	})
}

func TestObjectWithoutSchema(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			if format == "NATIVE_ARROW" {
				t.Skip("Native arrow is not supported in objects without schema")
			}
			rows := dbt.mustQuery("SELECT {'a': 'b'}::OBJECT AS STRUCTURED_TYPE")
			defer rows.Close()
			rows.Next()
			var v string
			err := rows.Scan(&v)
			assertNilF(t, err)
			assertStringContainsE(t, v, `"a": "b"`)
		})
	})
}

func TestObjectWithoutSchemaMetadata(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			if format == "NATIVE_ARROW" {
				t.Skip("Native arrow is not supported in objects without schema")
			}
			rows := dbt.mustQuery("SELECT {'a': 'b'}::OBJECT AS structured_type")
			defer rows.Close()
			columnTypes, err := rows.ColumnTypes()
			assertNilF(t, err)
			assertEqualE(t, len(columnTypes), 1)
			assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(""))
			assertEqualE(t, columnTypes[0].DatabaseTypeName(), "OBJECT")
			assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
		})
	})
}

func TestArrayAndMetadataAsString(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			skipForStringingNativeArrow(t, format)
			rows := dbt.mustQueryT(t, "SELECT ARRAY_CONSTRUCT(1, 2)::ARRAY(INTEGER) AS STRUCTURED_TYPE")
			defer rows.Close()
			assertTrueF(t, rows.Next())
			var res string
			err := rows.Scan(&res)
			assertNilF(t, err)
			assertEqualIgnoringWhitespaceE(t, "[1, 2]", res)

			columnTypes, err := rows.ColumnTypes()
			assertNilF(t, err)
			assertEqualE(t, len(columnTypes), 1)
			assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(""))
			assertEqualE(t, columnTypes[0].DatabaseTypeName(), "ARRAY")
			assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
		})
	})
}

func TestArrayAndMetadataAsArray(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			testcases := []struct {
				name      string
				query     string
				expected1 any
				expected2 any
				actual    any
			}{
				{
					name:      "integer",
					query:     "SELECT ARRAY_CONSTRUCT(1, 2)::ARRAY(INTEGER) as structured_type UNION SELECT ARRAY_CONSTRUCT(4, 5, 6)::ARRAY(INTEGER)",
					expected1: []int64{1, 2},
					expected2: []int64{4, 5, 6},
					actual:    []int64{},
				},
				{
					name:      "double",
					query:     "SELECT ARRAY_CONSTRUCT(1.1, 2.2)::ARRAY(DOUBLE) as structured_type UNION SELECT ARRAY_CONSTRUCT(3.3)::ARRAY(DOUBLE) ORDER BY 1",
					expected1: []float64{1.1, 2.2},
					expected2: []float64{3.3},
					actual:    []float64{},
				},
				{
					name:      "number - fixed integer",
					query:     "SELECT ARRAY_CONSTRUCT(1, 2)::ARRAY(NUMBER(38, 0)) as structured_type UNION SELECT ARRAY_CONSTRUCT(3)::ARRAY(NUMBER(38, 0))",
					expected1: []int64{1, 2},
					expected2: []int64{3},
					actual:    []int64{},
				},
				{
					name:      "number - fixed fraction",
					query:     "SELECT ARRAY_CONSTRUCT(1.1, 2.2)::ARRAY(NUMBER(38, 19)) as structured_type UNION SELECT ARRAY_CONSTRUCT()::ARRAY(NUMBER(38, 19))",
					expected1: []float64{1.1, 2.2},
					expected2: []float64{},
					actual:    []float64{},
				},
				{
					name:      "string",
					query:     "SELECT ARRAY_CONSTRUCT('a', 'b')::ARRAY(VARCHAR) as structured_type",
					expected1: []string{"a", "b"},
					actual:    []string{},
				},
				{
					name:      "time",
					query:     "SELECT ARRAY_CONSTRUCT('13:03:02'::TIME, '05:13:22'::TIME)::ARRAY(TIME) as structured_type",
					expected1: []time.Time{time.Date(0, 1, 1, 13, 3, 2, 0, time.UTC), time.Date(0, 1, 1, 5, 13, 22, 0, time.UTC)},
					actual:    []time.Time{},
				},
				{
					name:      "date",
					query:     "SELECT ARRAY_CONSTRUCT('2024-01-05'::DATE, '2001-11-12'::DATE)::ARRAY(DATE) as structured_type",
					expected1: []time.Time{time.Date(2024, time.January, 5, 0, 0, 0, 0, time.UTC), time.Date(2001, time.November, 12, 0, 0, 0, 0, time.UTC)},
					actual:    []time.Time{},
				},
				{
					name:      "timestamp_ntz",
					query:     "SELECT ARRAY_CONSTRUCT('2024-01-05 11:22:33'::TIMESTAMP_NTZ, '2001-11-12 11:22:33'::TIMESTAMP_NTZ)::ARRAY(TIMESTAMP_NTZ) as structured_type",
					expected1: []time.Time{time.Date(2024, time.January, 5, 11, 22, 33, 0, time.UTC), time.Date(2001, time.November, 12, 11, 22, 33, 0, time.UTC)},
					actual:    []time.Time{},
				},
				{
					name:      "timestamp_ltz",
					query:     "SELECT ARRAY_CONSTRUCT('2024-01-05 11:22:33'::TIMESTAMP_LTZ, '2001-11-12 11:22:33'::TIMESTAMP_LTZ)::ARRAY(TIMESTAMP_LTZ) as structured_type",
					expected1: []time.Time{time.Date(2024, time.January, 5, 11, 22, 33, 0, warsawTz), time.Date(2001, time.November, 12, 11, 22, 33, 0, warsawTz)},
					actual:    []time.Time{},
				},
				{
					name:      "timestamp_tz",
					query:     "SELECT ARRAY_CONSTRUCT('2024-01-05 11:22:33 +0100'::TIMESTAMP_TZ, '2001-11-12 11:22:33 +0100'::TIMESTAMP_TZ)::ARRAY(TIMESTAMP_TZ) as structured_type",
					expected1: []time.Time{time.Date(2024, time.January, 5, 11, 22, 33, 0, warsawTz), time.Date(2001, time.November, 12, 11, 22, 33, 0, warsawTz)},
					actual:    []time.Time{},
				},
				{
					name:      "bool",
					query:     "SELECT ARRAY_CONSTRUCT(true, false)::ARRAY(boolean) as structured_type",
					expected1: []bool{true, false},
					actual:    []bool{},
				},
				{
					name:      "binary",
					query:     "SELECT ARRAY_CONSTRUCT(TO_BINARY('616263', 'HEX'), TO_BINARY('646566', 'HEX'))::ARRAY(BINARY) as structured_type",
					expected1: [][]byte{{'a', 'b', 'c'}, {'d', 'e', 'f'}},
					actual:    [][]byte{},
				},
			}
			for _, tc := range testcases {
				t.Run(tc.name, func(t *testing.T) {
					rows := dbt.mustQueryContextT(ctx, t, tc.query)
					defer rows.Close()
					rows.Next()
					err := rows.Scan(&tc.actual)
					assertNilF(t, err)
					if _, ok := tc.actual.([]time.Time); ok {
						assertEqualE(t, len(tc.actual.([]time.Time)), len(tc.expected1.([]time.Time)))
						for i := range tc.actual.([]time.Time) {
							if tc.name == "time" {
								assertEqualE(t, tc.actual.([]time.Time)[i].Hour(), tc.expected1.([]time.Time)[i].Hour())
								assertEqualE(t, tc.actual.([]time.Time)[i].Minute(), tc.expected1.([]time.Time)[i].Minute())
								assertEqualE(t, tc.actual.([]time.Time)[i].Second(), tc.expected1.([]time.Time)[i].Second())
							} else {
								assertTrueE(t, tc.actual.([]time.Time)[i].UTC().Equal(tc.expected1.([]time.Time)[i].UTC()))
							}
						}
					} else {
						assertDeepEqualE(t, tc.actual, tc.expected1)
					}
					if tc.expected2 != nil {
						rows.Next()
						err := rows.Scan(&tc.actual)
						assertNilF(t, err)
						if _, ok := tc.actual.([]time.Time); ok {
							assertEqualE(t, len(tc.actual.([]time.Time)), len(tc.expected2.([]time.Time)))
							for i := range tc.actual.([]time.Time) {
								assertTrueE(t, tc.actual.([]time.Time)[i].UTC().Equal(tc.expected2.([]time.Time)[i].UTC()))
							}
						} else {
							assertDeepEqualE(t, tc.actual, tc.expected2)
						}
					}
					columnTypes, err := rows.ColumnTypes()
					assertNilF(t, err)
					assertEqualE(t, len(columnTypes), 1)
					assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(tc.expected1))
					assertEqualE(t, columnTypes[0].DatabaseTypeName(), "ARRAY")
					assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
				})
			}
		})
	})
}

func TestArrayWithoutSchema(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			if format == "NATIVE_ARROW" {
				t.Skip("Native arrow is not supported in arrays without schema")
			}
			rows := dbt.mustQuery("SELECT ARRAY_CONSTRUCT(1, 2)")
			defer rows.Close()
			rows.Next()
			var v string
			err := rows.Scan(&v)
			assertNilF(t, err)
			assertEqualIgnoringWhitespaceE(t, v, "[1, 2]")
		})
	})
}

func TestEmptyArraysAndNullArrays(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQueryContextT(ctx, t, "SELECT ARRAY_CONSTRUCT(1, 2)::ARRAY(INTEGER) as structured_type UNION SELECT ARRAY_CONSTRUCT()::ARRAY(INTEGER) UNION SELECT NULL UNION SELECT ARRAY_CONSTRUCT(4, 5, 6)::ARRAY(INTEGER) ORDER BY 1")
			defer rows.Close()
			checkRow := func(rows *RowsExtended, expected *[]int64) {
				var res *[]int64
				rows.Next()
				err := rows.Scan(&res)
				assertNilF(t, err)
				assertDeepEqualE(t, res, expected)
			}

			checkRow(rows, &[]int64{})
			checkRow(rows, &[]int64{1, 2})
			checkRow(rows, &[]int64{4, 5, 6})
			checkRow(rows, nil)
		})
	})
}

func TestArrayWithoutSchemaMetadata(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			if format == "NATIVE_ARROW" {
				t.Skip("Native arrow is not supported in arrays without schema")
			}
			rows := dbt.mustQuery("SELECT ARRAY_CONSTRUCT(1, 2) AS structured_type")
			defer rows.Close()
			columnTypes, err := rows.ColumnTypes()
			assertNilF(t, err)
			assertEqualE(t, len(columnTypes), 1)
			assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(""))
			assertEqualE(t, columnTypes[0].DatabaseTypeName(), "ARRAY")
			assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
		})
	})
}

func TestArrayOfObjects(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQueryContextT(ctx, t, "SELECT ARRAY_CONSTRUCT({'s': 's1', 'i': 9}, {'s': 's2', 'i': 8})::ARRAY(OBJECT(s VARCHAR, i INTEGER)) as structured_type UNION SELECT ARRAY_CONSTRUCT({'s': 's3', 'i': 7})::ARRAY(OBJECT(s VARCHAR, i INTEGER))")
			defer rows.Close()
			rows.Next()
			var res []*simpleObject
			err := rows.Scan(ScanArrayOfScanners(&res))
			assertNilF(t, err)
			assertDeepEqualE(t, res, []*simpleObject{{s: "s1", i: 9}, {s: "s2", i: 8}})
			rows.Next()
			err = rows.Scan(ScanArrayOfScanners(&res))
			assertNilF(t, err)
			assertDeepEqualE(t, res, []*simpleObject{{s: "s3", i: 7}})
			columnTypes, err := rows.ColumnTypes()
			assertNilF(t, err)
			assertEqualE(t, len(columnTypes), 1)
			assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf([]ObjectType{}))
			assertEqualE(t, columnTypes[0].DatabaseTypeName(), "ARRAY")
			assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
		})
	})
}

func TestArrayOfArrays(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	testcases := []struct {
		name     string
		query    string
		actual   any
		expected any
	}{
		{
			name:     "string",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('a', 'b', 'c'), ARRAY_CONSTRUCT('d', 'e'))::ARRAY(ARRAY(VARCHAR))",
			actual:   make([][]string, 2),
			expected: [][]string{{"a", "b", "c"}, {"d", "e"}},
		},
		{
			name:     "int64",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT(1, 2), ARRAY_CONSTRUCT(3, 4))::ARRAY(ARRAY(INTEGER))",
			actual:   make([][]int64, 2),
			expected: [][]int64{{1, 2}, {3, 4}},
		},
		{
			name:     "float64 - fixed",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT(1.1, 2.2), ARRAY_CONSTRUCT(3.3, 4.4))::ARRAY(ARRAY(NUMBER(38, 19)))",
			actual:   make([][]float64, 2),
			expected: [][]float64{{1.1, 2.2}, {3.3, 4.4}},
		},
		{
			name:     "float64 - real",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT(1.1, 2.2), ARRAY_CONSTRUCT(3.3, 4.4))::ARRAY(ARRAY(DOUBLE))",
			actual:   make([][]float64, 2),
			expected: [][]float64{{1.1, 2.2}, {3.3, 4.4}},
		},
		{
			name:     "bool",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT(true, false), ARRAY_CONSTRUCT(false, true, false))::ARRAY(ARRAY(BOOLEAN))",
			actual:   make([][]bool, 2),
			expected: [][]bool{{true, false}, {false, true, false}},
		},
		{
			name:     "binary",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT(TO_BINARY('6162'), TO_BINARY('6364')), ARRAY_CONSTRUCT(TO_BINARY('6566'), TO_BINARY('6768')))::ARRAY(ARRAY(BINARY))",
			actual:   make([][][]byte, 2),
			expected: [][][]byte{{{'a', 'b'}, {'c', 'd'}}, {{'e', 'f'}, {'g', 'h'}}},
		},
		{
			name:     "date",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('2024-01-01'::DATE, '2024-02-02'::DATE), ARRAY_CONSTRUCT('2024-03-03'::DATE, '2024-04-04'::DATE))::ARRAY(ARRAY(DATE))",
			actual:   make([][]time.Time, 2),
			expected: [][]time.Time{{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 2, 2, 0, 0, 0, 0, time.UTC)}, {time.Date(2024, 3, 3, 0, 0, 0, 0, time.UTC), time.Date(2024, 4, 4, 0, 0, 0, 0, time.UTC)}},
		},
		{
			name:     "time",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('01:01:01'::TIME, '02:02:02'::TIME), ARRAY_CONSTRUCT('03:03:03'::TIME, '04:04:04'::TIME))::ARRAY(ARRAY(TIME))",
			actual:   make([][]time.Time, 2),
			expected: [][]time.Time{{time.Date(0, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(0, 1, 1, 2, 2, 2, 0, time.UTC)}, {time.Date(0, 1, 1, 3, 3, 3, 0, time.UTC), time.Date(0, 1, 1, 4, 4, 4, 0, time.UTC)}},
		},
		{
			name:     "timestamp_ltz",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('2024-01-05 11:22:33'::TIMESTAMP_LTZ), ARRAY_CONSTRUCT('2001-11-12 11:22:33'::TIMESTAMP_LTZ))::ARRAY(ARRAY(TIMESTAMP_LTZ))",
			actual:   make([][]time.Time, 2),
			expected: [][]time.Time{{time.Date(2024, time.January, 5, 11, 22, 33, 0, warsawTz)}, {time.Date(2001, time.November, 12, 11, 22, 33, 0, warsawTz)}},
		},
		{
			name:     "timestamp_ntz",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('2024-01-05 11:22:33'::TIMESTAMP_NTZ), ARRAY_CONSTRUCT('2001-11-12 11:22:33'::TIMESTAMP_NTZ))::ARRAY(ARRAY(TIMESTAMP_NTZ))",
			actual:   make([][]time.Time, 2),
			expected: [][]time.Time{{time.Date(2024, time.January, 5, 11, 22, 33, 0, time.UTC)}, {time.Date(2001, time.November, 12, 11, 22, 33, 0, time.UTC)}},
		},
		{
			name:     "timestamp_tz",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT('2024-01-05 11:22:33 +0100'::TIMESTAMP_TZ), ARRAY_CONSTRUCT('2001-11-12 11:22:33 +0100'::TIMESTAMP_TZ))::ARRAY(ARRAY(TIMESTAMP_TZ))",
			actual:   make([][]time.Time, 2),
			expected: [][]time.Time{{time.Date(2024, time.January, 5, 11, 22, 33, 0, warsawTz)}, {time.Date(2001, time.November, 12, 11, 22, 33, 0, warsawTz)}},
		},
	}
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			for _, tc := range testcases {
				t.Run(tc.name, func(t *testing.T) {
					rows := dbt.mustQueryContextT(ctx, t, tc.query)
					defer rows.Close()
					rows.Next()
					err := rows.Scan(&tc.actual)
					assertNilF(t, err)
					if timesOfTimes, ok := tc.expected.([][]time.Time); ok {
						for i, timeOfTimes := range timesOfTimes {
							for j, tm := range timeOfTimes {
								if tc.name == "time" {
									assertEqualE(t, tm.Hour(), tc.actual.([][]time.Time)[i][j].Hour())
									assertEqualE(t, tm.Minute(), tc.actual.([][]time.Time)[i][j].Minute())
									assertEqualE(t, tm.Second(), tc.actual.([][]time.Time)[i][j].Second())
								} else {
									assertTrueE(t, tm.Equal(tc.actual.([][]time.Time)[i][j]))
								}
							}
						}
					} else {
						assertDeepEqualE(t, tc.actual, tc.expected)
					}
				})
			}
		})
	})
}

func TestMapAndMetadataAsString(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			if format == "NATIVE_ARROW" {
				t.Skip("Native arrow is not supported in maps without schema")
			}
			rows := dbt.mustQuery("SELECT {'a': 'b', 'c': 'd'}::MAP(VARCHAR, VARCHAR) AS STRUCTURED_TYPE")
			defer rows.Close()
			assertTrueF(t, rows.Next())
			var v string
			err := rows.Scan(&v)
			assertNilF(t, err)
			assertEqualIgnoringWhitespaceE(t, v, `{"a": "b", "c": "d"}`)

			columnTypes, err := rows.ColumnTypes()
			assertNilF(t, err)
			assertEqualE(t, len(columnTypes), 1)
			assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(""))
			assertEqualE(t, columnTypes[0].DatabaseTypeName(), "MAP")
			assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
		})
	})
}

func TestMapAndMetadataAsMap(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		testcases := []struct {
			name      string
			query     string
			expected1 any
			expected2 any
			actual    any
		}{
			{
				name:      "string string",
				query:     "SELECT {'a': 'x', 'b': 'y'}::MAP(VARCHAR, VARCHAR) as structured_type UNION SELECT {'c': 'z'}::MAP(VARCHAR, VARCHAR) ORDER BY 1 DESC",
				expected1: map[string]string{"a": "x", "b": "y"},
				expected2: map[string]string{"c": "z"},
				actual:    make(map[string]string),
			},
			{
				name:      "integer string",
				query:     "SELECT {'1': 'x', '2': 'y'}::MAP(INTEGER, VARCHAR) as structured_type UNION SELECT {'3': 'z'}::MAP(INTEGER, VARCHAR) ORDER BY 1 DESC",
				expected1: map[int64]string{int64(1): "x", int64(2): "y"},
				expected2: map[int64]string{int64(3): "z"},
				actual:    make(map[int64]string),
			},
			{
				name:      "string bool",
				query:     "SELECT {'a': true, 'b': false}::MAP(VARCHAR, BOOLEAN) as structured_type UNION SELECT {'c': true}::MAP(VARCHAR, BOOLEAN) ORDER BY 1 DESC",
				expected1: map[string]bool{"a": true, "b": false},
				expected2: map[string]bool{"c": true},
				actual:    make(map[string]bool),
			},
			{
				name:      "integer bool",
				query:     "SELECT {'1': true, '2': false}::MAP(INTEGER, BOOLEAN) as structured_type UNION SELECT {'3': true}::MAP(INTEGER, BOOLEAN) ORDER BY 1 DESC",
				expected1: map[int64]bool{int64(1): true, int64(2): false},
				expected2: map[int64]bool{int64(3): true},
				actual:    make(map[int64]bool),
			},
			{
				name:      "string integer",
				query:     "SELECT {'a': 11, 'b': 22}::MAP(VARCHAR, INTEGER) as structured_type UNION SELECT {'c': 33}::MAP(VARCHAR, INTEGER) ORDER BY 1 DESC",
				expected1: map[string]int64{"a": 11, "b": 22},
				expected2: map[string]int64{"c": 33},
				actual:    make(map[string]int64),
			},
			{
				name:      "integer integer",
				query:     "SELECT {'1': 11, '2': 22}::MAP(INTEGER, INTEGER) as structured_type UNION SELECT {'3': 33}::MAP(INTEGER, INTEGER) ORDER BY 1 DESC",
				expected1: map[int64]int64{int64(1): int64(11), int64(2): int64(22)},
				expected2: map[int64]int64{int64(3): int64(33)},
				actual:    make(map[int64]int64),
			},
			{
				name:      "string double",
				query:     "SELECT {'a': 11.1, 'b': 22.2}::MAP(VARCHAR, DOUBLE) as structured_type UNION SELECT {'c': 33.3}::MAP(VARCHAR, DOUBLE) ORDER BY 1 DESC",
				expected1: map[string]float64{"a": 11.1, "b": 22.2},
				expected2: map[string]float64{"c": 33.3},
				actual:    make(map[string]float64),
			},
			{
				name:      "integer double",
				query:     "SELECT {'1': 11.1, '2': 22.2}::MAP(INTEGER, DOUBLE) as structured_type UNION SELECT {'3': 33.3}::MAP(INTEGER, DOUBLE) ORDER BY 1 DESC",
				expected1: map[int64]float64{int64(1): 11.1, int64(2): 22.2},
				expected2: map[int64]float64{int64(3): 33.3},
				actual:    make(map[int64]float64),
			},
			{
				name:      "string number integer",
				query:     "SELECT {'a': 11, 'b': 22}::MAP(VARCHAR, NUMBER(38, 0)) as structured_type UNION SELECT {'c': 33}::MAP(VARCHAR, NUMBER(38, 0)) ORDER BY 1 DESC",
				expected1: map[string]int64{"a": 11, "b": 22},
				expected2: map[string]int64{"c": 33},
				actual:    make(map[string]int64),
			},
			{
				name:      "integer number integer",
				query:     "SELECT {'1': 11, '2': 22}::MAP(INTEGER, NUMBER(38, 0)) as structured_type UNION SELECT {'3': 33}::MAP(INTEGER, NUMBER(38, 0)) ORDER BY 1 DESC",
				expected1: map[int64]int64{int64(1): int64(11), int64(2): int64(22)},
				expected2: map[int64]int64{int64(3): int64(33)},
				actual:    make(map[int64]int64),
			},
			{
				name:      "string number fraction",
				query:     "SELECT {'a': 11.1, 'b': 22.2}::MAP(VARCHAR, NUMBER(38, 19)) as structured_type UNION SELECT {'c': 33.3}::MAP(VARCHAR, NUMBER(38, 19)) ORDER BY 1 DESC",
				expected1: map[string]float64{"a": 11.1, "b": 22.2},
				expected2: map[string]float64{"c": 33.3},
				actual:    make(map[string]float64),
			},
			{
				name:      "integer number fraction",
				query:     "SELECT {'1': 11.1, '2': 22.2}::MAP(INTEGER, NUMBER(38, 19)) as structured_type UNION SELECT {'3': 33.3}::MAP(INTEGER, NUMBER(38, 19)) ORDER BY 1 DESC",
				expected1: map[int64]float64{int64(1): 11.1, int64(2): 22.2},
				expected2: map[int64]float64{int64(3): 33.3},
				actual:    make(map[int64]float64),
			},
			{
				name:      "string binary",
				query:     "SELECT {'a': TO_BINARY('616263', 'HEX'), 'b': TO_BINARY('646566', 'HEX')}::MAP(VARCHAR, BINARY) as structured_type UNION SELECT {'c': TO_BINARY('676869', 'HEX')}::MAP(VARCHAR, BINARY) ORDER BY 1 DESC",
				expected1: map[string][]byte{"a": {'a', 'b', 'c'}, "b": {'d', 'e', 'f'}},
				expected2: map[string][]byte{"c": {'g', 'h', 'i'}},
				actual:    make(map[string][]byte),
			},
			{
				name:      "integer binary",
				query:     "SELECT {'1': TO_BINARY('616263', 'HEX'), '2': TO_BINARY('646566', 'HEX')}::MAP(INTEGER, BINARY) as structured_type UNION SELECT {'3': TO_BINARY('676869', 'HEX')}::MAP(INTEGER, BINARY) ORDER BY 1 DESC",
				expected1: map[int64][]byte{1: {'a', 'b', 'c'}, 2: {'d', 'e', 'f'}},
				expected2: map[int64][]byte{3: {'g', 'h', 'i'}},
				actual:    make(map[int64][]byte),
			},
			{
				name:      "string date",
				query:     "SELECT {'a': '2024-04-02'::DATE, 'b': '2024-04-03'::DATE}::MAP(VARCHAR, DATE) as structured_type UNION SELECT {'c': '2024-04-04'::DATE}::MAP(VARCHAR, DATE) ORDER BY 1 DESC",
				expected1: map[string]time.Time{"a": time.Date(2024, time.April, 2, 0, 0, 0, 0, time.UTC), "b": time.Date(2024, time.April, 3, 0, 0, 0, 0, time.UTC)},
				expected2: map[string]time.Time{"c": time.Date(2024, time.April, 4, 0, 0, 0, 0, time.UTC)},
				actual:    make(map[string]time.Time),
			},
			{
				name:      "integer date",
				query:     "SELECT {'1': '2024-04-02'::DATE, '2': '2024-04-03'::DATE}::MAP(INTEGER, DATE) as structured_type UNION SELECT {'3': '2024-04-04'::DATE}::MAP(INTEGER, DATE) ORDER BY 1 DESC",
				expected1: map[int64]time.Time{1: time.Date(2024, time.April, 2, 0, 0, 0, 0, time.UTC), 2: time.Date(2024, time.April, 3, 0, 0, 0, 0, time.UTC)},
				expected2: map[int64]time.Time{3: time.Date(2024, time.April, 4, 0, 0, 0, 0, time.UTC)},
				actual:    make(map[int64]time.Time),
			},
			{
				name:      "string time",
				query:     "SELECT {'a': '13:03:02'::TIME, 'b': '13:03:03'::TIME}::MAP(VARCHAR, TIME) as structured_type UNION SELECT {'c': '13:03:04'::TIME}::MAP(VARCHAR, TIME) ORDER BY 1 DESC",
				expected1: map[string]time.Time{"a": time.Date(0, 0, 0, 13, 3, 2, 0, time.UTC), "b": time.Date(0, 0, 0, 13, 3, 3, 0, time.UTC)},
				expected2: map[string]time.Time{"c": time.Date(0, 0, 0, 13, 3, 4, 0, time.UTC)},
				actual:    make(map[string]time.Time),
			},
			{
				name:      "integer time",
				query:     "SELECT {'1': '13:03:02'::TIME, '2': '13:03:03'::TIME}::MAP(VARCHAR, TIME) as structured_type UNION SELECT {'3': '13:03:04'::TIME}::MAP(VARCHAR, TIME) ORDER BY 1 DESC",
				expected1: map[string]time.Time{"1": time.Date(0, 0, 0, 13, 3, 2, 0, time.UTC), "2": time.Date(0, 0, 0, 13, 3, 3, 0, time.UTC)},
				expected2: map[string]time.Time{"3": time.Date(0, 0, 0, 13, 3, 4, 0, time.UTC)},
				actual:    make(map[int64]time.Time),
			},
			{
				name:      "string timestamp_ntz",
				query:     "SELECT {'a': '2024-01-05 11:22:33'::TIMESTAMP_NTZ, 'b': '2024-01-06 11:22:33'::TIMESTAMP_NTZ}::MAP(VARCHAR, TIMESTAMP_NTZ) as structured_type UNION SELECT {'c': '2024-01-07 11:22:33'::TIMESTAMP_NTZ}::MAP(VARCHAR, TIMESTAMP_NTZ) ORDER BY 1 DESC",
				expected1: map[string]time.Time{"a": time.Date(2024, time.January, 5, 11, 22, 33, 0, time.UTC), "b": time.Date(2024, time.January, 6, 11, 22, 33, 0, time.UTC)},
				expected2: map[string]time.Time{"c": time.Date(2024, time.January, 7, 11, 22, 33, 0, time.UTC)},
				actual:    make(map[string]time.Time),
			},
			{
				name:      "integer timestamp_ntz",
				query:     "SELECT {'1': '2024-01-05 11:22:33'::TIMESTAMP_NTZ, '2': '2024-01-06 11:22:33'::TIMESTAMP_NTZ}::MAP(INTEGER, TIMESTAMP_NTZ) as structured_type UNION SELECT {'3': '2024-01-07 11:22:33'::TIMESTAMP_NTZ}::MAP(INTEGER, TIMESTAMP_NTZ) ORDER BY 1 DESC",
				expected1: map[int64]time.Time{1: time.Date(2024, time.January, 5, 11, 22, 33, 0, time.UTC), 2: time.Date(2024, time.January, 6, 11, 22, 33, 0, time.UTC)},
				expected2: map[int64]time.Time{3: time.Date(2024, time.January, 7, 11, 22, 33, 0, time.UTC)},
				actual:    make(map[int64]time.Time),
			},
			{
				name:      "string timestamp_tz",
				query:     "SELECT {'a': '2024-01-05 11:22:33 +0100'::TIMESTAMP_TZ, 'b': '2024-01-06 11:22:33 +0100'::TIMESTAMP_TZ}::MAP(VARCHAR, TIMESTAMP_TZ) as structured_type UNION SELECT {'c': '2024-01-07 11:22:33 +0100'::TIMESTAMP_TZ}::MAP(VARCHAR, TIMESTAMP_TZ) ORDER BY 1 DESC",
				expected1: map[string]time.Time{"a": time.Date(2024, time.January, 5, 11, 22, 33, 0, warsawTz), "b": time.Date(2024, time.January, 6, 11, 22, 33, 0, warsawTz)},
				expected2: map[string]time.Time{"c": time.Date(2024, time.January, 7, 11, 22, 33, 0, warsawTz)},
				actual:    make(map[string]time.Time),
			},
			{
				name:      "integer timestamp_tz",
				query:     "SELECT {'1': '2024-01-05 11:22:33 +0100'::TIMESTAMP_TZ, '2': '2024-01-06 11:22:33 +0100'::TIMESTAMP_TZ}::MAP(INTEGER, TIMESTAMP_TZ) as structured_type UNION SELECT {'3': '2024-01-07 11:22:33 +0100'::TIMESTAMP_TZ}::MAP(INTEGER, TIMESTAMP_TZ) ORDER BY 1 DESC",
				expected1: map[int64]time.Time{1: time.Date(2024, time.January, 5, 11, 22, 33, 0, time.UTC), 2: time.Date(2024, time.January, 6, 11, 22, 33, 0, time.UTC)},
				expected2: map[int64]time.Time{3: time.Date(2024, time.January, 7, 11, 22, 33, 0, time.UTC)},
				actual:    make(map[int64]time.Time),
			},
			{
				name:      "string timestamp_ltz",
				query:     "SELECT {'a': '2024-01-05 11:22:33'::TIMESTAMP_LTZ, 'b': '2024-01-06 11:22:33'::TIMESTAMP_LTZ}::MAP(VARCHAR, TIMESTAMP_LTZ) as structured_type UNION SELECT {'c': '2024-01-07 11:22:33'::TIMESTAMP_LTZ}::MAP(VARCHAR, TIMESTAMP_LTZ) ORDER BY 1 DESC",
				expected1: map[string]time.Time{"a": time.Date(2024, time.January, 5, 11, 22, 33, 0, warsawTz), "b": time.Date(2024, time.January, 6, 11, 22, 33, 0, warsawTz)},
				expected2: map[string]time.Time{"c": time.Date(2024, time.January, 7, 11, 22, 33, 0, warsawTz)},
				actual:    make(map[string]time.Time),
			},
			{
				name:      "integer timestamp_ltz",
				query:     "SELECT {'1': '2024-01-05 11:22:33'::TIMESTAMP_LTZ, '2': '2024-01-06 11:22:33'::TIMESTAMP_LTZ}::MAP(INTEGER, TIMESTAMP_LTZ) as structured_type UNION SELECT {'3': '2024-01-07 11:22:33'::TIMESTAMP_LTZ}::MAP(INTEGER, TIMESTAMP_LTZ) ORDER BY 1 DESC",
				expected1: map[int64]time.Time{1: time.Date(2024, time.January, 5, 11, 22, 33, 0, time.UTC), 2: time.Date(2024, time.January, 6, 11, 22, 33, 0, time.UTC)},
				expected2: map[int64]time.Time{3: time.Date(2024, time.January, 7, 11, 22, 33, 0, time.UTC)},
				actual:    make(map[int64]time.Time),
			},
		}
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			for _, tc := range testcases {
				t.Run(tc.name, func(t *testing.T) {
					rows := dbt.mustQueryContextT(ctx, t, tc.query)
					defer rows.Close()

					checkRow := func(expected any) {
						rows.Next()
						err := rows.Scan(&tc.actual)
						assertNilF(t, err)
						if _, ok := expected.(map[string]time.Time); ok {
							assertEqualE(t, len(tc.actual.(map[string]time.Time)), len(expected.(map[string]time.Time)))
							for k, v := range expected.(map[string]time.Time) {
								if strings.Contains(tc.name, "time") {
									assertEqualE(t, v.Hour(), tc.actual.(map[string]time.Time)[k].Hour())
									assertEqualE(t, v.Minute(), tc.actual.(map[string]time.Time)[k].Minute())
									assertEqualE(t, v.Second(), tc.actual.(map[string]time.Time)[k].Second())
								} else {
									assertTrueE(t, v.UTC().Equal(tc.actual.(map[string]time.Time)[k].UTC()))
								}
							}
						} else if _, ok := expected.(map[int64]time.Time); ok {
							assertEqualE(t, len(tc.actual.(map[int64]time.Time)), len(expected.(map[int64]time.Time)))
							for k, v := range expected.(map[int64]time.Time) {
								if strings.Contains(tc.name, "time") {

								} else {
									assertTrueE(t, v.UTC().Equal(tc.actual.(map[int64]time.Time)[k].UTC()))
								}
							}
						} else {
							assertDeepEqualE(t, tc.actual, expected)
						}
					}

					checkRow(tc.expected1)
					checkRow(tc.expected2)

					columnTypes, err := rows.ColumnTypes()
					assertNilF(t, err)
					assertEqualE(t, len(columnTypes), 1)
					assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(tc.expected1))
					assertEqualE(t, columnTypes[0].DatabaseTypeName(), "MAP")
					assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
				})
			}
		})
	})
}

func TestMapOfObjects(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQueryContextT(ctx, t, "SELECT {'x': {'s': 'abc', 'i': 1}, 'y': {'s': 'def', 'i': 2}}::MAP(VARCHAR, OBJECT(s VARCHAR, i INTEGER))")
			defer rows.Close()
			var res map[string]*simpleObject
			rows.Next()
			err := rows.Scan(ScanMapOfScanners(&res))
			assertNilF(t, err)
			assertDeepEqualE(t, res, map[string]*simpleObject{"x": {s: "abc", i: 1}, "y": {s: "def", i: 2}})
		})
	})
}

func TestMapOfArrays(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	testcases := []struct {
		name     string
		query    string
		actual   any
		expected any
	}{
		{
			name:     "string",
			query:    "SELECT {'x': ARRAY_CONSTRUCT('ab', 'cd'), 'y': ARRAY_CONSTRUCT('ef')}::MAP(VARCHAR, ARRAY(VARCHAR))",
			actual:   make(map[string][]string),
			expected: map[string][]string{"x": {"ab", "cd"}, "y": {"ef"}},
		},
		{
			name:     "fixed - scale == 0",
			query:    "SELECT {'x': ARRAY_CONSTRUCT(1, 2), 'y': ARRAY_CONSTRUCT(3, 4)}::MAP(VARCHAR, ARRAY(INTEGER))",
			actual:   make(map[string][]int64),
			expected: map[string][]int64{"x": {1, 2}, "y": {3, 4}},
		},
		{
			name:     "fixed - scale != 0",
			query:    "SELECT {'x': ARRAY_CONSTRUCT(1.1, 2.2), 'y': ARRAY_CONSTRUCT(3.3, 4.4)}::MAP(VARCHAR, ARRAY(NUMBER(38, 19)))",
			actual:   make(map[string][]float64),
			expected: map[string][]float64{"x": {1.1, 2.2}, "y": {3.3, 4.4}},
		},
		{
			name:     "real",
			query:    "SELECT {'x': ARRAY_CONSTRUCT(1.1, 2.2), 'y': ARRAY_CONSTRUCT(3.3, 4.4)}::MAP(VARCHAR, ARRAY(DOUBLE))",
			actual:   make(map[string][]float64),
			expected: map[string][]float64{"x": {1.1, 2.2}, "y": {3.3, 4.4}},
		},
		{
			name:     "binary",
			query:    "SELECT {'x': ARRAY_CONSTRUCT(TO_BINARY('6162')), 'y': ARRAY_CONSTRUCT(TO_BINARY('6364'), TO_BINARY('6566'))}::MAP(VARCHAR, ARRAY(BINARY))",
			actual:   make(map[string][][]byte),
			expected: map[string][][]byte{"x": {[]byte{'a', 'b'}}, "y": {[]byte{'c', 'd'}, []byte{'e', 'f'}}},
		},
		{
			name:     "boolean",
			query:    "SELECT {'x': ARRAY_CONSTRUCT(true, false), 'y': ARRAY_CONSTRUCT(false, true)}::MAP(VARCHAR, ARRAY(BOOLEAN))",
			actual:   make(map[string][]bool),
			expected: map[string][]bool{"x": {true, false}, "y": {false, true}},
		},
		{
			name:     "date",
			query:    "SELECT {'a': ARRAY_CONSTRUCT('2024-04-02'::DATE, '2024-04-03'::DATE)}::MAP(VARCHAR, ARRAY(DATE))",
			expected: map[string][]time.Time{"a": {time.Date(2024, time.April, 2, 0, 0, 0, 0, time.UTC), time.Date(2024, time.April, 3, 0, 0, 0, 0, time.UTC)}},
			actual:   make(map[string]time.Time),
		},
		{
			name:     "time",
			query:    "SELECT {'a': ARRAY_CONSTRUCT('13:03:02'::TIME, '13:03:03'::TIME)}::MAP(VARCHAR, ARRAY(TIME))",
			expected: map[string][]time.Time{"a": {time.Date(0, 0, 0, 13, 3, 2, 0, time.UTC), time.Date(0, 0, 0, 13, 3, 3, 0, time.UTC)}},
			actual:   make(map[string]time.Time),
		},
		{
			name:     "timestamp_ntz",
			query:    "SELECT {'a': ARRAY_CONSTRUCT('2024-01-05 11:22:33'::TIMESTAMP_NTZ, '2024-01-06 11:22:33'::TIMESTAMP_NTZ)}::MAP(VARCHAR, ARRAY(TIMESTAMP_NTZ))",
			expected: map[string][]time.Time{"a": {time.Date(2024, time.January, 5, 11, 22, 33, 0, time.UTC), time.Date(2024, time.January, 6, 11, 22, 33, 0, time.UTC)}},
			actual:   make(map[string]time.Time),
		},
		{
			name:     "string timestamp_tz",
			query:    "SELECT {'a': ARRAY_CONSTRUCT('2024-01-05 11:22:33 +0100'::TIMESTAMP_TZ, '2024-01-06 11:22:33 +0100'::TIMESTAMP_TZ)}::MAP(VARCHAR, ARRAY(TIMESTAMP_TZ))",
			expected: map[string][]time.Time{"a": {time.Date(2024, time.January, 5, 11, 22, 33, 0, warsawTz), time.Date(2024, time.January, 6, 11, 22, 33, 0, warsawTz)}},
			actual:   make(map[string]time.Time),
		},
		{
			name:     "string timestamp_ltz",
			query:    "SELECT {'a': ARRAY_CONSTRUCT('2024-01-05 11:22:33'::TIMESTAMP_LTZ, '2024-01-06 11:22:33'::TIMESTAMP_LTZ)}::MAP(VARCHAR, ARRAY(TIMESTAMP_LTZ))",
			expected: map[string][]time.Time{"a": {time.Date(2024, time.January, 5, 11, 22, 33, 0, warsawTz), time.Date(2024, time.January, 6, 11, 22, 33, 0, warsawTz)}},
			actual:   make(map[string]time.Time),
		},
	}
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			for _, tc := range testcases {
				t.Run(tc.name, func(t *testing.T) {
					rows := dbt.mustQueryContextT(ctx, t, tc.query)
					defer rows.Close()
					rows.Next()
					err := rows.Scan(&tc.actual)
					assertNilF(t, err)
					if expected, ok := tc.expected.(map[string][]time.Time); ok {
						for k, v := range expected {
							for i, expectedTime := range v {
								if tc.name == "time" {
									assertEqualE(t, expectedTime.Hour(), tc.actual.(map[string][]time.Time)[k][i].Hour())
									assertEqualE(t, expectedTime.Minute(), tc.actual.(map[string][]time.Time)[k][i].Minute())
									assertEqualE(t, expectedTime.Second(), tc.actual.(map[string][]time.Time)[k][i].Second())
								} else {
									assertTrueE(t, expectedTime.Equal(tc.actual.(map[string][]time.Time)[k][i]))
								}
							}
						}
					} else {
						assertDeepEqualE(t, tc.actual, tc.expected)
					}
				})
			}
		})
	})
}

func TestNullAndEmptyMaps(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQueryContextT(ctx, t, "SELECT {'a': 1}::MAP(VARCHAR, INTEGER) UNION SELECT NULL UNION SELECT {}::MAP(VARCHAR, INTEGER) UNION SELECT {'d': 4}::MAP(VARCHAR, INTEGER) ORDER BY 1")
			defer rows.Close()
			checkRow := func(rows *RowsExtended, expected *map[string]int64) {
				rows.Next()
				var res *map[string]int64
				err := rows.Scan(&res)
				assertNilF(t, err)
				assertDeepEqualE(t, res, expected)
			}
			checkRow(rows, &map[string]int64{})
			checkRow(rows, &map[string]int64{"d": 4})
			checkRow(rows, &map[string]int64{"a": 1})
			checkRow(rows, nil)
		})
	})
}

func TestMapWithNullValues(t *testing.T) {
	ctx := WithStructuredTypesEnabled(context.Background())
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	testcases := []struct {
		name     string
		query    string
		actual   any
		expected any
	}{
		{
			name:     "string",
			query:    "SELECT object_construct_keep_null('x', 'abc', 'y', null)::MAP(VARCHAR, VARCHAR)",
			actual:   make(map[string]sql.NullString),
			expected: map[string]sql.NullString{"x": {Valid: true, String: "abc"}, "y": {Valid: false}},
		},
		{
			name:     "bool",
			query:    "SELECT object_construct_keep_null('x', true, 'y', null)::MAP(VARCHAR, BOOLEAN)",
			actual:   make(map[string]sql.NullBool),
			expected: map[string]sql.NullBool{"x": {Valid: true, Bool: true}, "y": {Valid: false}},
		},
		{
			name:     "fixed - scale == 0",
			query:    "SELECT object_construct_keep_null('x', 1, 'y', null)::MAP(VARCHAR, BIGINT)",
			actual:   make(map[string]sql.NullInt64),
			expected: map[string]sql.NullInt64{"x": {Valid: true, Int64: 1}, "y": {Valid: false}},
		},
		{
			name:     "fixed - scale != 0",
			query:    "SELECT object_construct_keep_null('x', 1.1, 'y', null)::MAP(VARCHAR, NUMBER(38, 19))",
			actual:   make(map[string]sql.NullFloat64),
			expected: map[string]sql.NullFloat64{"x": {Valid: true, Float64: 1.1}, "y": {Valid: false}},
		},
		{
			name:     "real",
			query:    "SELECT object_construct_keep_null('x', 1.1, 'y', null)::MAP(VARCHAR, DOUBLE)",
			actual:   make(map[string]sql.NullFloat64),
			expected: map[string]sql.NullFloat64{"x": {Valid: true, Float64: 1.1}, "y": {Valid: false}},
		},
		{
			name:     "binary",
			query:    "SELECT object_construct_keep_null('x', TO_BINARY('616263'), 'y', null)::MAP(VARCHAR, BINARY)",
			actual:   make(map[string][]byte),
			expected: map[string][]byte{"x": {'a', 'b', 'c'}, "y": nil},
		},
		{
			name:     "date",
			query:    "SELECT object_construct_keep_null('x', '2024-04-05'::DATE, 'y', null)::MAP(VARCHAR, DATE)",
			actual:   make(map[string]sql.NullTime),
			expected: map[string]sql.NullTime{"x": {Valid: true, Time: time.Date(2024, time.April, 5, 0, 0, 0, 0, time.UTC)}, "y": {Valid: false}},
		},
		{
			name:     "time",
			query:    "SELECT object_construct_keep_null('x', '13:14:15'::TIME, 'y', null)::MAP(VARCHAR, TIME)",
			actual:   make(map[string]sql.NullTime),
			expected: map[string]sql.NullTime{"x": {Valid: true, Time: time.Date(1, 0, 0, 13, 14, 15, 0, time.UTC)}, "y": {Valid: false}},
		},
		{
			name:     "timestamp_tz",
			query:    "SELECT object_construct_keep_null('x', '2022-08-31 13:43:22 +0200'::TIMESTAMP_TZ, 'y', null)::MAP(VARCHAR, TIMESTAMP_TZ)",
			actual:   make(map[string]sql.NullTime),
			expected: map[string]sql.NullTime{"x": {Valid: true, Time: time.Date(2022, 8, 31, 13, 43, 22, 0, warsawTz)}, "y": {Valid: false}},
		},
		{
			name:     "timestamp_ntz",
			query:    "SELECT object_construct_keep_null('x', '2022-08-31 13:43:22'::TIMESTAMP_NTZ, 'y', null)::MAP(VARCHAR, TIMESTAMP_NTZ)",
			actual:   make(map[string]sql.NullTime),
			expected: map[string]sql.NullTime{"x": {Valid: true, Time: time.Date(2022, 8, 31, 13, 43, 22, 0, time.UTC)}, "y": {Valid: false}},
		},
		{
			name:     "timestamp_ltz",
			query:    "SELECT object_construct_keep_null('x', '2022-08-31 13:43:22'::TIMESTAMP_LTZ, 'y', null)::MAP(VARCHAR, TIMESTAMP_LTZ)",
			actual:   make(map[string]sql.NullTime),
			expected: map[string]sql.NullTime{"x": {Valid: true, Time: time.Date(2022, 8, 31, 13, 43, 22, 0, warsawTz)}, "y": {Valid: false}},
		},
	}
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			for _, tc := range testcases {
				t.Run(tc.name, func(t *testing.T) {
					rows := dbt.mustQueryContextT(WithMapValuesNullable(ctx), t, tc.query)
					defer rows.Close()
					rows.Next()
					err := rows.Scan(&tc.actual)
					assertNilF(t, err)
					if tc.name == "time" {
						for i, nt := range tc.actual.(map[string]sql.NullTime) {
							assertEqualE(t, nt.Valid, tc.expected.(map[string]sql.NullTime)[i].Valid)
							assertEqualE(t, nt.Time.Hour(), tc.expected.(map[string]sql.NullTime)[i].Time.Hour())
							assertEqualE(t, nt.Time.Minute(), tc.expected.(map[string]sql.NullTime)[i].Time.Minute())
							assertEqualE(t, nt.Time.Second(), tc.expected.(map[string]sql.NullTime)[i].Time.Second())
						}
					} else if tc.name == "timestamp_tz" || tc.name == "timestamp_ltz" || tc.name == "timestamp_ntz" {
						for i, nt := range tc.actual.(map[string]sql.NullTime) {
							assertEqualE(t, nt.Valid, tc.expected.(map[string]sql.NullTime)[i].Valid)
							assertTrueE(t, nt.Time.Equal(tc.expected.(map[string]sql.NullTime)[i].Time))
						}
					} else {
						assertDeepEqualE(t, tc.actual, tc.expected)
					}
				})
			}
		})
	})
}

func TestArraysWithNullValues(t *testing.T) {
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	testcases := []struct {
		name     string
		query    string
		actual   any
		expected any
	}{
		{
			name:     "string",
			query:    "SELECT ARRAY_CONSTRUCT('x', null, 'yz', null)::ARRAY(STRING)",
			actual:   []sql.NullString{},
			expected: []sql.NullString{{Valid: true, String: "x"}, {Valid: false}, {Valid: true, String: "yz"}, {Valid: false}},
		},
		{
			name:     "bool",
			query:    "SELECT ARRAY_CONSTRUCT(true, null, false)::ARRAY(BOOLEAN)",
			actual:   []sql.NullBool{},
			expected: []sql.NullBool{{Valid: true, Bool: true}, {Valid: false}, {Valid: true, Bool: false}},
		},
		{
			name:     "fixed - scale == 0",
			query:    "SELECT ARRAY_CONSTRUCT(null, 2, 3)::ARRAY(BIGINT)",
			actual:   []sql.NullInt64{},
			expected: []sql.NullInt64{{Valid: false}, {Valid: true, Int64: 2}, {Valid: true, Int64: 3}},
		},
		{
			name:     "fixed - scale == 0",
			query:    "SELECT ARRAY_CONSTRUCT(1.3, 2.0, null, null)::ARRAY(NUMBER(38, 19))",
			actual:   []sql.NullFloat64{},
			expected: []sql.NullFloat64{{Valid: true, Float64: 1.3}, {Valid: true, Float64: 2.0}, {Valid: false}, {Valid: false}},
		},
		{
			name:     "real",
			query:    "SELECT ARRAY_CONSTRUCT(1.9, 0.2, null)::ARRAY(DOUBLE)",
			actual:   []sql.NullFloat64{},
			expected: []sql.NullFloat64{{Valid: true, Float64: 1.9}, {Valid: true, Float64: 0.2}, {Valid: false}},
		},
		{
			name:     "binary",
			query:    "SELECT ARRAY_CONSTRUCT(null, TO_BINARY('616263'))::ARRAY(BINARY)",
			actual:   [][]byte{},
			expected: [][]byte{nil, {'a', 'b', 'c'}},
		},
		{
			name:     "date",
			query:    "SELECT ARRAY_CONSTRUCT('2024-04-05'::DATE, null)::ARRAY(DATE)",
			actual:   []sql.NullTime{},
			expected: []sql.NullTime{{Valid: true, Time: time.Date(2024, time.April, 5, 0, 0, 0, 0, time.UTC)}, {Valid: false}},
		},
		{
			name:     "time",
			query:    "SELECT ARRAY_CONSTRUCT('13:14:15'::TIME, null)::ARRAY(TIME)",
			actual:   []sql.NullTime{},
			expected: []sql.NullTime{{Valid: true, Time: time.Date(1, 0, 0, 13, 14, 15, 0, time.UTC)}, {Valid: false}},
		},
		{
			name:     "timestamp_tz",
			query:    "SELECT ARRAY_CONSTRUCT('2022-08-31 13:43:22 +0200'::TIMESTAMP_TZ, null)::ARRAY(TIMESTAMP_TZ)",
			actual:   []sql.NullTime{},
			expected: []sql.NullTime{{Valid: true, Time: time.Date(2022, 8, 31, 13, 43, 22, 0, warsawTz)}, {Valid: false}},
		},
		{
			name:     "timestamp_ntz",
			query:    "SELECT ARRAY_CONSTRUCT('2022-08-31 13:43:22'::TIMESTAMP_NTZ, null)::ARRAY(TIMESTAMP_NTZ)",
			actual:   []sql.NullTime{},
			expected: []sql.NullTime{{Valid: true, Time: time.Date(2022, 8, 31, 13, 43, 22, 0, time.UTC)}, {Valid: false}},
		},
		{
			name:     "timestamp_ltz",
			query:    "SELECT ARRAY_CONSTRUCT('2022-08-31 13:43:22'::TIMESTAMP_LTZ, null)::ARRAY(TIMESTAMP_LTZ)",
			actual:   []sql.NullTime{},
			expected: []sql.NullTime{{Valid: true, Time: time.Date(2022, 8, 31, 13, 43, 22, 0, warsawTz)}, {Valid: false}},
		},
		{
			name:     "array",
			query:    "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT(true, null), null, ARRAY_CONSTRUCT(null, false, true))::ARRAY(ARRAY(BOOLEAN))",
			actual:   [][]sql.NullBool{},
			expected: [][]sql.NullBool{{{Valid: true, Bool: true}, {Valid: false}}, nil, {{Valid: false}, {Valid: true, Bool: false}, {Valid: true, Bool: true}}},
		},
	}
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		dbt.forceNativeArrow()
		dbt.enableStructuredTypes()
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				rows := dbt.mustQueryContext(WithStructuredTypesEnabled(WithArrayValuesNullable(context.Background())), tc.query)
				defer rows.Close()
				rows.Next()
				err := rows.Scan(&tc.actual)
				assertNilF(t, err)
				if tc.name == "time" {
					for i, nt := range tc.actual.([]sql.NullTime) {
						assertEqualE(t, nt.Valid, tc.expected.([]sql.NullTime)[i].Valid)
						assertEqualE(t, nt.Time.Hour(), tc.expected.([]sql.NullTime)[i].Time.Hour())
						assertEqualE(t, nt.Time.Minute(), tc.expected.([]sql.NullTime)[i].Time.Minute())
						assertEqualE(t, nt.Time.Second(), tc.expected.([]sql.NullTime)[i].Time.Second())
					}
				} else if tc.name == "timestamp_tz" || tc.name == "timestamp_ltz" || tc.name == "timestamp_ntz" {
					for i, nt := range tc.actual.([]sql.NullTime) {
						assertEqualE(t, nt.Valid, tc.expected.([]sql.NullTime)[i].Valid)
						assertTrueE(t, nt.Time.Equal(tc.expected.([]sql.NullTime)[i].Time))
					}
				} else {
					assertDeepEqualE(t, tc.actual, tc.expected)
				}
			})
		}
	})

}

func TestArraysWithNullValuesHigherPrecision(t *testing.T) {
	testcases := []struct {
		name     string
		query    string
		actual   any
		expected any
	}{
		{
			name:   "fixed - scale == 0",
			query:  "SELECT ARRAY_CONSTRUCT(null, 2)::ARRAY(BIGINT)",
			actual: []*big.Int{},
		},
	}
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		dbt.forceNativeArrow()
		dbt.enableStructuredTypes()
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := WithHigherPrecision(WithStructuredTypesEnabled(WithArrayValuesNullable(context.Background())))
				rows := dbt.mustQueryContext(ctx, tc.query)
				defer rows.Close()
				rows.Next()
				err := rows.Scan(&tc.actual)
				assertNilF(t, err)
				assertNilF(t, tc.actual.([]*big.Int)[0])
				bigInt, _ := new(big.Int).SetString("2", 10)
				assertEqualE(t, tc.actual.([]*big.Int)[1].Cmp(bigInt), 0)
			})
		}
	})

}

type HigherPrecisionStruct struct {
	i *big.Int
	f *big.Float
}

func (hps *HigherPrecisionStruct) Scan(val any) error {
	st, ok := val.(StructuredObject)
	if !ok {
		return fmt.Errorf("expected StructuredObject, got %T", val)
	}

	var err error
	if hps.i, err = st.GetBigInt("i"); err != nil {
		return err
	}
	if hps.f, err = st.GetBigFloat("f"); err != nil {
		return err
	}
	return nil
}

func TestWithHigherPrecision(t *testing.T) {
	ctx := WithHigherPrecision(WithStructuredTypesEnabled(context.Background()))
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			if format != "NATIVE_ARROW" {
				t.Skip("JSON structured type does not support higher precision")
			}
			t.Run("object", func(t *testing.T) {
				rows := dbt.mustQueryContext(ctx, "SELECT {'i': 10000000000000000000000000000000000000::DECIMAL(38, 0), 'f': 1.2345678901234567890123456789012345678::DECIMAL(38, 37)}::OBJECT(i DECIMAL(38, 0), f DECIMAL(38, 37)) as structured_type")
				defer rows.Close()
				rows.Next()
				var v HigherPrecisionStruct
				err := rows.Scan(&v)
				assertNilF(t, err)
				bigInt, b := new(big.Int).SetString("10000000000000000000000000000000000000", 10)
				assertTrueF(t, b)
				assertEqualE(t, bigInt.Cmp(v.i), 0)
				bigFloat, b := new(big.Float).SetPrec(v.f.Prec()).SetString("1.2345678901234567890123456789012345678")
				assertTrueE(t, b)
				assertEqualE(t, bigFloat.Cmp(v.f), 0)
				columnTypes, err := rows.ColumnTypes()
				assertNilF(t, err)
				assertEqualE(t, len(columnTypes), 1)
				assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(ObjectType{}))
				assertEqualE(t, columnTypes[0].DatabaseTypeName(), "OBJECT")
				assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
			})
			t.Run("array of big ints", func(t *testing.T) {
				rows := dbt.mustQueryContext(ctx, "SELECT ARRAY_CONSTRUCT(10000000000000000000000000000000000000)::ARRAY(DECIMAL(38, 0)) as structured_type")
				defer rows.Close()
				rows.Next()
				var v *[]*big.Int
				err := rows.Scan(&v)
				assertNilF(t, err)
				bigInt, b := new(big.Int).SetString("10000000000000000000000000000000000000", 10)
				assertTrueF(t, b)
				assertEqualE(t, bigInt.Cmp((*v)[0]), 0)
				columnTypes, err := rows.ColumnTypes()
				assertNilF(t, err)
				assertEqualE(t, len(columnTypes), 1)
				assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf([]*big.Int{}))
				assertEqualE(t, columnTypes[0].DatabaseTypeName(), "ARRAY")
				assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
			})
			t.Run("array of big floats", func(t *testing.T) {
				rows := dbt.mustQueryContext(ctx, "SELECT ARRAY_CONSTRUCT(1.2345678901234567890123456789012345678)::ARRAY(DECIMAL(38, 37)) as structured_type")
				defer rows.Close()
				rows.Next()
				var v *[]*big.Float
				err := rows.Scan(&v)
				assertNilF(t, err)
				bigFloat, b := new(big.Float).SetPrec((*v)[0].Prec()).SetString("1.2345678901234567890123456789012345678")
				assertTrueE(t, b)
				assertEqualE(t, bigFloat.Cmp((*v)[0]), 0)
				columnTypes, err := rows.ColumnTypes()
				assertNilF(t, err)
				assertEqualE(t, len(columnTypes), 1)
				assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf([]*big.Float{}))
				assertEqualE(t, columnTypes[0].DatabaseTypeName(), "ARRAY")
				assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
			})
			t.Run("map of string to big ints", func(t *testing.T) {
				rows := dbt.mustQueryContext(ctx, "SELECT object_construct_keep_null('x', 10000000000000000000000000000000000000, 'y', null)::MAP(VARCHAR, DECIMAL(38, 0)) as structured_type")
				defer rows.Close()
				rows.Next()
				var v *map[string]*big.Int
				err := rows.Scan(&v)
				assertNilF(t, err)
				bigInt, b := new(big.Int).SetString("10000000000000000000000000000000000000", 10)
				assertTrueF(t, b)
				assertEqualE(t, bigInt.Cmp((*v)["x"]), 0)
				assertEqualE(t, (*v)["y"], (*big.Int)(nil))
				columnTypes, err := rows.ColumnTypes()
				assertNilF(t, err)
				assertEqualE(t, len(columnTypes), 1)
				assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(map[string]*big.Int{}))
				assertEqualE(t, columnTypes[0].DatabaseTypeName(), "MAP")
				assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
			})
			t.Run("map of string to big floats", func(t *testing.T) {
				rows := dbt.mustQueryContext(ctx, "SELECT {'x': 1.2345678901234567890123456789012345678, 'y': null}::MAP(VARCHAR, DECIMAL(38, 37)) as structured_type")
				defer rows.Close()
				rows.Next()
				var v *map[string]*big.Float
				err := rows.Scan(&v)
				assertNilF(t, err)
				bigFloat, b := new(big.Float).SetPrec((*v)["x"].Prec()).SetString("1.2345678901234567890123456789012345678")
				assertTrueE(t, b)
				assertEqualE(t, bigFloat.Cmp((*v)["x"]), 0)
				assertEqualE(t, (*v)["y"], (*big.Float)(nil))
				columnTypes, err := rows.ColumnTypes()
				assertNilF(t, err)
				assertEqualE(t, len(columnTypes), 1)
				assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(map[string]*big.Float{}))
				assertEqualE(t, columnTypes[0].DatabaseTypeName(), "MAP")
				assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
			})
			t.Run("map of int64 to big ints", func(t *testing.T) {
				rows := dbt.mustQueryContext(ctx, "SELECT {'1': 10000000000000000000000000000000000000}::MAP(INTEGER, DECIMAL(38, 0)) as structured_type")
				defer rows.Close()
				rows.Next()
				var v *map[int64]*big.Int
				err := rows.Scan(&v)
				assertNilF(t, err)
				bigInt, b := new(big.Int).SetString("10000000000000000000000000000000000000", 10)
				assertTrueF(t, b)
				assertEqualE(t, bigInt.Cmp((*v)[1]), 0)
				columnTypes, err := rows.ColumnTypes()
				assertNilF(t, err)
				assertEqualE(t, len(columnTypes), 1)
				assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(map[int64]*big.Int{}))
				assertEqualE(t, columnTypes[0].DatabaseTypeName(), "MAP")
				assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
			})
			t.Run("map of int64 to big floats", func(t *testing.T) {
				rows := dbt.mustQueryContext(ctx, "SELECT {'1': 1.2345678901234567890123456789012345678}::MAP(INTEGER, DECIMAL(38, 37)) as structured_type")
				defer rows.Close()
				rows.Next()
				var v *map[int64]*big.Float
				err := rows.Scan(&v)
				assertNilF(t, err)
				bigFloat, b := new(big.Float).SetPrec((*v)[1].Prec()).SetString("1.2345678901234567890123456789012345678")
				assertTrueE(t, b)
				assertEqualE(t, bigFloat.Cmp((*v)[1]), 0)
				columnTypes, err := rows.ColumnTypes()
				assertNilF(t, err)
				assertEqualE(t, len(columnTypes), 1)
				assertEqualE(t, columnTypes[0].ScanType(), reflect.TypeOf(map[int64]*big.Float{}))
				assertEqualE(t, columnTypes[0].DatabaseTypeName(), "MAP")
				assertEqualE(t, columnTypes[0].Name(), "STRUCTURED_TYPE")
			})
		})
	})
}

func TestStructuredTypeInArrowBatchesSimple(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypes()
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx := WithArrowBatches(WithArrowAllocator(context.Background(), pool))

		dbt.forceNativeArrow()
		var err error
		var rows driver.Rows
		err = dbt.conn.Raw(func(sc any) error {
			rows, err = sc.(driver.QueryerContext).QueryContext(ctx, "SELECT 1, {'s': 'some string'}::OBJECT(s VARCHAR)", nil)
			return err
		})
		assertNilF(t, err)
		defer rows.Close()
		batches, err := rows.(SnowflakeRows).GetArrowBatches()
		assertNilF(t, err)
		assertNotEqualF(t, len(batches), 0)
		batch, err := batches[0].Fetch()
		assertNilF(t, err)
		assertNotEqualF(t, len(*batch), 0)
		for _, record := range *batch {
			assertEqualE(t, record.Column(0).(*array.Int8).Value(0), int8(1))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(0).(*array.String).Value(0), "some string")
			record.Release()
		}
	})
}

func TestStructuredTypeInArrowBatches(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypes()
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx := WithArrowBatches(WithArrowAllocator(context.Background(), pool))

		dbt.forceNativeArrow()
		var err error
		var rows driver.Rows
		err = dbt.conn.Raw(func(sc any) error {
			rows, err = sc.(driver.QueryerContext).QueryContext(ctx, "SELECT 1, {'s': 'some string', 'i': 1, 'time': '11:22:33'::TIME, 'date': '2024-04-16'::DATE, 'ltz': '2024-04-16 11:22:33'::TIMESTAMPLTZ, 'tz': '2025-04-16 22:33:11 +0100'::TIMESTAMPTZ, 'ntz': '2026-04-16 15:22:31'::TIMESTAMPNTZ}::OBJECT(s VARCHAR, i INTEGER, time TIME, date DATE, ltz TIMESTAMPLTZ, tz TIMESTAMPTZ, ntz TIMESTAMPNTZ)", nil)
			return err
		})
		assertNilF(t, err)
		defer rows.Close()
		batches, err := rows.(SnowflakeRows).GetArrowBatches()
		assertNilF(t, err)
		assertNotEqualF(t, len(batches), 0)
		batch, err := batches[0].Fetch()
		assertNilF(t, err)
		assertNotEqualF(t, len(*batch), 0)
		for _, record := range *batch {
			assertEqualE(t, record.Column(0).(*array.Int8).Value(0), int8(1))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(0).(*array.String).Value(0), "some string")
			assertEqualE(t, record.Column(1).(*array.Struct).Field(1).(*array.Int64).Value(0), int64(1))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(2).(*array.Time64).Value(0).ToTime(arrow.Nanosecond), time.Date(1970, 1, 1, 11, 22, 33, 0, time.UTC))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(3).(*array.Date32).Value(0).ToTime(), time.Date(2024, 4, 16, 0, 0, 0, 0, time.UTC))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(4).(*array.Timestamp).Value(0).ToTime(arrow.Nanosecond), time.Date(2024, 4, 16, 11, 22, 33, 0, time.UTC))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(5).(*array.Timestamp).Value(0).ToTime(arrow.Nanosecond), time.Date(2025, 4, 16, 21, 33, 11, 0, time.UTC))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(6).(*array.Timestamp).Value(0).ToTime(arrow.Nanosecond), time.Date(2026, 4, 16, 15, 22, 31, 0, time.UTC))
			record.Release()
		}
	})
}

func TestStructuredTypeInArrowBatchesWithTimestampOptionAndHigherPrecisionAndUtf8Validation(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypes()
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		dbt.forceNativeArrow()
		var err error
		var rows driver.Rows
		err = dbt.conn.Raw(func(sc any) error {
			ctx := WithArrowBatchesUtf8Validation(WithHigherPrecision(WithArrowBatchesTimestampOption(WithArrowBatches(WithArrowAllocator(context.Background(), pool)), UseOriginalTimestamp)))
			rows, err = sc.(driver.QueryerContext).QueryContext(ctx, "SELECT 1, {'i': 123, 'f': 12.34, 'n0': 321, 'n19': 1.5, 's': 'some string', 'bi': TO_BINARY('616263', 'HEX'), 'bool': true, 'time': '11:22:33', 'date': '2024-04-18', 'ntz': '2024-04-01 11:22:33', 'tz': '2024-04-02 11:22:33 +0100', 'ltz': '2024-04-03 11:22:33'}::OBJECT(i INTEGER, f DOUBLE, n0 NUMBER(38, 0), n19 NUMBER(38, 19), s VARCHAR, bi BINARY, bool BOOLEAN, time TIME, date DATE, ntz TIMESTAMP_NTZ, tz TIMESTAMP_TZ, ltz TIMESTAMP_LTZ)", nil)
			return err
		})
		assertNilF(t, err)
		defer rows.Close()
		batches, err := rows.(SnowflakeRows).GetArrowBatches()
		assertNilF(t, err)
		assertNotEqualF(t, len(batches), 0)
		batch, err := batches[0].Fetch()
		assertNilF(t, err)
		assertNotEqualF(t, len(*batch), 0)
		for _, record := range *batch {
			assertEqualE(t, record.Column(0).(*array.Int8).Value(0), int8(1))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(0).(*array.Decimal128).Value(0).LowBits(), uint64(123))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(1).(*array.Float64).Value(0), 12.34)
			assertEqualE(t, record.Column(1).(*array.Struct).Field(2).(*array.Decimal128).Value(0).LowBits(), uint64(321))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(3).(*array.Decimal128).Value(0).LowBits(), uint64(15000000000000000000))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(4).(*array.String).Value(0), "some string")
			assertDeepEqualE(t, record.Column(1).(*array.Struct).Field(5).(*array.Binary).Value(0), []byte{'a', 'b', 'c'})
			assertEqualE(t, record.Column(1).(*array.Struct).Field(6).(*array.Boolean).Value(0), true)
			assertEqualE(t, record.Column(1).(*array.Struct).Field(7).(*array.Time64).Value(0).ToTime(arrow.Nanosecond), time.Date(1970, 1, 1, 11, 22, 33, 0, time.UTC))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(8).(*array.Date32).Value(0).ToTime(), time.Date(2024, 4, 18, 0, 0, 0, 0, time.UTC))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(9).(*array.Struct).Field(0).(*array.Int64).Value(0), int64(1711970553))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(9).(*array.Struct).Field(1).(*array.Int32).Value(0), int32(0))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(10).(*array.Struct).Field(0).(*array.Int64).Value(0), int64(1712053353))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(10).(*array.Struct).Field(1).(*array.Int32).Value(0), int32(0))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(11).(*array.Struct).Field(0).(*array.Int64).Value(0), int64(1712143353))
			assertEqualE(t, record.Column(1).(*array.Struct).Field(11).(*array.Struct).Field(1).(*array.Int32).Value(0), int32(0))
			record.Release()
		}
	})
}

func TestStructuredTypeInArrowBatchesWithEmbeddedObject(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypes()
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		dbt.forceNativeArrow()
		var err error
		var rows driver.Rows
		err = dbt.conn.Raw(func(sc any) error {
			ctx := WithArrowBatches(WithArrowAllocator(context.Background(), pool))
			rows, err = sc.(driver.QueryerContext).QueryContext(ctx, "SELECT {'o': {'s': 'some string'}}::OBJECT(o OBJECT(s VARCHAR))", nil)
			return err
		})
		assertNilF(t, err)
		defer rows.Close()
		batches, err := rows.(SnowflakeRows).GetArrowBatches()
		assertNilF(t, err)
		assertNotEqualF(t, len(batches), 0)
		batch, err := batches[0].Fetch()
		assertNilF(t, err)
		assertNotEqualF(t, len(*batch), 0)
		for _, record := range *batch {
			assertEqualE(t, record.Column(0).(*array.Struct).Field(0).(*array.Struct).Field(0).(*array.String).Value(0), "some string")
			record.Release()
		}
	})
}

func TestStructuredTypeInArrowBatchesAsNull(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypes()
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		dbt.forceNativeArrow()
		var err error
		var rows driver.Rows
		err = dbt.conn.Raw(func(sc any) error {
			ctx := WithArrowBatches(WithArrowAllocator(context.Background(), pool))
			rows, err = sc.(driver.QueryerContext).QueryContext(ctx, "SELECT {'s': 'some string'}::OBJECT(s VARCHAR) UNION SELECT null", nil)
			return err
		})
		assertNilF(t, err)
		defer rows.Close()
		batches, err := rows.(SnowflakeRows).GetArrowBatches()
		assertNilF(t, err)
		assertNotEqualF(t, len(batches), 0)
		batch, err := batches[0].Fetch()
		assertNilF(t, err)
		assertNotEqualF(t, len(*batch), 0)
		for _, record := range *batch {
			assertFalseF(t, record.Column(0).IsNull(0))
			assertTrueE(t, record.Column(0).IsNull(1))
			record.Release()
		}
	})
}

func TestStructuredArrayInArrowBatches(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypes()
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx := WithArrowBatches(WithArrowAllocator(context.Background(), pool))

		dbt.forceNativeArrow()
		var err error
		var rows driver.Rows
		err = dbt.conn.Raw(func(sc any) error {
			rows, err = sc.(driver.QueryerContext).QueryContext(ctx, "SELECT [1, 2, 3]::ARRAY(INTEGER) UNION SELECT [4, 5, 6]::ARRAY(INTEGER)", nil)
			return err
		})
		assertNilF(t, err)
		defer rows.Close()
		batches, err := rows.(SnowflakeRows).GetArrowBatches()
		assertNilF(t, err)
		assertNotEqualF(t, len(batches), 0)
		batch, err := batches[0].Fetch()
		assertNilF(t, err)
		assertNotEqualF(t, len(*batch), 0)
		record := (*batch)[0]
		defer record.Release()
		assertEqualE(t, record.Column(0).(*array.List).ListValues().(*array.Int64).Value(0), int64(1))
		assertEqualE(t, record.Column(0).(*array.List).ListValues().(*array.Int64).Value(1), int64(2))
		assertEqualE(t, record.Column(0).(*array.List).ListValues().(*array.Int64).Value(2), int64(3))
		assertEqualE(t, record.Column(0).(*array.List).ListValues().(*array.Int64).Value(3), int64(4))
		assertEqualE(t, record.Column(0).(*array.List).ListValues().(*array.Int64).Value(4), int64(5))
		assertEqualE(t, record.Column(0).(*array.List).ListValues().(*array.Int64).Value(5), int64(6))
		assertEqualE(t, record.Column(0).(*array.List).Offsets()[0], int32(0))
		assertEqualE(t, record.Column(0).(*array.List).Offsets()[1], int32(3))
		assertEqualE(t, record.Column(0).(*array.List).Offsets()[2], int32(6))
	})
}

func TestStructuredMapInArrowBatches(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.enableStructuredTypes()
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx := WithArrowBatches(WithArrowAllocator(context.Background(), pool))

		dbt.forceNativeArrow()
		var err error
		var rows driver.Rows
		err = dbt.conn.Raw(func(sc any) error {
			rows, err = sc.(driver.QueryerContext).QueryContext(ctx, "SELECT {'a': 'b', 'c': 'd'}::MAP(VARCHAR, VARCHAR)", nil)
			return err
		})
		assertNilF(t, err)
		defer rows.Close()
		batches, err := rows.(SnowflakeRows).GetArrowBatches()
		assertNilF(t, err)
		assertNotEqualF(t, len(batches), 0)
		batch, err := batches[0].Fetch()
		assertNilF(t, err)
		assertNotEqualF(t, len(*batch), 0)
		for _, record := range *batch {
			assertEqualE(t, record.Column(0).(*array.Map).Keys().(*array.String).Value(0), "a")
			assertEqualE(t, record.Column(0).(*array.Map).Keys().(*array.String).Value(1), "c")
			assertEqualE(t, record.Column(0).(*array.Map).Items().(*array.String).Value(0), "b")
			assertEqualE(t, record.Column(0).(*array.Map).Items().(*array.String).Value(1), "d")
			record.Release()
		}
	})
}

func forAllStructureTypeFormats(dbt *DBTest, f func(t *testing.T, format string)) {
	for _, tc := range []struct {
		name        string
		forceFormat func(test *DBTest)
	}{
		{
			name: "JSON",
			forceFormat: func(test *DBTest) {
				dbt.forceJSON()
			},
		},
		{
			name: "ARROW",
			forceFormat: func(test *DBTest) {
				dbt.forceArrow()
			},
		},
		{
			name: "NATIVE_ARROW",
			forceFormat: func(test *DBTest) {
				dbt.forceNativeArrow()
			},
		},
	} {
		dbt.T.Run(tc.name, func(t *testing.T) {
			tc.forceFormat(dbt)
			dbt.enableStructuredTypes()
			f(t, tc.name)
		})
	}
}

func TestSelectingNullObjectsInArrowBatches(t *testing.T) {
	testcases := []string{
		"select null::object(v VARCHAR)",
		"select null::object",
	}
	runDBTest(t, func(dbt *DBTest) {
		for _, tc := range testcases {
			t.Run(tc, func(t *testing.T) {
				pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer pool.AssertSize(t, 0)
				ctx := WithArrowBatches(WithArrowAllocator(context.Background(), pool))

				var err error
				var rows driver.Rows
				err = dbt.conn.Raw(func(sc any) error {
					queryer, implementsQueryContext := sc.(driver.QueryerContext)
					assertTrueF(t, implementsQueryContext, "snowflake conn does not implement QueryerContext but needs to")
					rows, err = queryer.QueryContext(ctx, tc, nil)
					return err
				})
				assertNilF(t, err, fmt.Sprintf("test failed to run the following query: %s", tc))
				defer rows.Close()

				sfRows, isSfRows := rows.(SnowflakeRows)
				assertTrueF(t, isSfRows, "rows are not snowflakeRows")

				batches, err := sfRows.GetArrowBatches()
				assertNilF(t, err)
				assertNotEqualF(t, len(batches), 0)
				batch, err := batches[0].Fetch()
				assertNilF(t, err)
				assertNotEqualF(t, len(*batch), 0)
				for _, record := range *batch {
					// check number of cols/rows so we dont get index out of range
					assertEqualF(t, record.NumRows(), int64(1), "wrong number of rows")
					assertEqualF(t, record.NumCols(), int64(1), "wrong number of cols")

					colIndex := 0
					rowIndex := 0

					srtCol, isStrCol := record.Column(colIndex).(*array.String)
					assertTrueF(t, isStrCol, "column is not type string")

					assertEqualE(t, srtCol.Value(rowIndex), "")
					isNull := srtCol.IsNull(rowIndex)
					assertTrueF(t, isNull)
					record.Release()
				}

			})
		}
	})
}

func TestSelectingSemistructuredTypesInArrowBatches(t *testing.T) {
	testcases := []struct {
		name               string
		query              string
		expected           string
		withUtf8Validation bool
	}{
		{
			name:               "test semistructured object in arrow batch with utf8 validation, snowflakeType = objectType",
			withUtf8Validation: true,
			expected:           `{"s":"someString"}`,
			query:              "SELECT {'s':'someString'}::OBJECT",
		},
		{
			name:               "test semistructured object in arrow batch without utf8 validation, snowflakeType = objectType",
			withUtf8Validation: false,
			expected:           `{"s":"someString"}`,
			query:              "SELECT {'s':'someString'}::OBJECT",
		},
		{
			name:               "test semistructured object in arrow batch without utf8 validation, snowflakeType = arrayType",
			withUtf8Validation: false,
			expected:           `[1,2,3]`,
			query:              "SELECT [1, 2, 3]::ARRAY",
		},
		{
			name:               "test semistructured object in arrow batch with utf8 validation, snowflakeType = arrayType",
			withUtf8Validation: true,
			expected:           `[1,2,3]`,
			query:              "SELECT [1, 2, 3]::ARRAY",
		},
	}
	runDBTest(t, func(dbt *DBTest) {
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {

				pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer pool.AssertSize(t, 0)
				ctx := WithArrowBatches(WithArrowAllocator(context.Background(), pool))
				if tc.withUtf8Validation {
					ctx = WithArrowBatchesUtf8Validation(ctx)
				}

				var err error
				var rows driver.Rows
				err = dbt.conn.Raw(func(sc any) error {
					queryer, implementsQueryContext := sc.(driver.QueryerContext)
					assertTrueF(t, implementsQueryContext, "snowflake conn does not implement QueryerContext but needs to")
					rows, err = queryer.QueryContext(ctx, tc.query, nil)
					return err
				})
				assertNilF(t, err)
				defer rows.Close()

				sfRows, isSfRows := rows.(SnowflakeRows)
				assertTrueF(t, isSfRows, "rows are not snowflakeRows")

				batches, err := sfRows.GetArrowBatches()
				assertNilF(t, err)
				assertNotEqualF(t, len(batches), 0)
				batch, err := batches[0].Fetch()
				assertNilF(t, err)
				assertNotEqualF(t, len(*batch), 0)
				for _, record := range *batch {
					assertEqualF(t, record.NumCols(), int64(1), "unexpected number of columns")
					assertEqualF(t, record.NumRows(), int64(1), "unexpected number of rows")

					curColIndex := 0
					rowIndex := 0

					// The underlying data may be struct, or it could be string. Either way is ok but lets not fail
					stringCol, isString := record.Column(curColIndex).(*array.String)

					assertTrueF(t, isString, "wrong type for column, expected string")
					assertEqualIgnoringWhitespaceE(t, stringCol.Value(rowIndex), tc.expected)

					record.Release()
				}
			})
		}

	})
}

func skipForStringingNativeArrow(t *testing.T, format string) {
	if format == "NATIVE_ARROW" {
		t.Skip("returning native arrow structured types as string is currently not supported")
	}
}
