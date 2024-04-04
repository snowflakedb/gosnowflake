package gosnowflake

import (
	"context"
	"math/big"
	"reflect"
	"testing"
	"time"
)

type objectWithAllTypes struct {
	s    string
	b    byte
	i16  int16
	i    int
	i64  int64
	f32  float32
	f64  float64
	bo   bool
	bi   []byte
	date time.Time
	time time.Time
	ltz  time.Time
	tz   time.Time
	ntz  time.Time
	so   simpleObject
}

func (o *objectWithAllTypes) Scan(val any) error {
	st := val.(StructuredType)
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
	if o.i, err = st.GetInt("i"); err != nil {
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
	o.so = *so.(*simpleObject)
	return nil
}

type simpleObject struct {
	s string
	i int
}

func (so *simpleObject) Scan(val any) error {
	st := val.(StructuredType)
	var err error
	if so.s, err = st.GetString("s"); err != nil {
		return err
	}
	if so.i, err = st.GetInt("i"); err != nil {
		return err
	}
	return nil
}

func skipStructuredTypesTestsOnGHActions(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("Structured types are not available on GH Actions")
	}
}

func TestObjectWithAllTypes(t *testing.T) {
	skipStructuredTypesTestsOnGHActions(t)
	warsawTz, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
			rows := dbt.mustQuery("SELECT 1, {'s': 'some string', 'b': 1, 'i16': 2, 'i': 3, 'i64': 9223372036854775807, 'f32': '1.1', 'f64': 2.2, 'bo': true, 'bi': TO_BINARY('616263', 'HEX'), 'date': '2024-03-21'::DATE, 'time': '13:03:02'::TIME, 'ltz': '2021-07-21 11:22:33'::TIMESTAMP_LTZ, 'tz': '2022-08-31 13:43:22 +0200'::TIMESTAMP_TZ, 'ntz': '2023-05-22 01:17:19'::TIMESTAMP_NTZ, 'so': {'s': 'child', 'i': 9}}::OBJECT(s VARCHAR, b TINYINT, i16 SMALLINT, i INTEGER, i64 BIGINT, f32 FLOAT, f64 DOUBLE, bo BOOLEAN, bi BINARY, date DATE, time TIME, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, ntz TIMESTAMP_NTZ, so OBJECT(s VARCHAR, i INTEGER))")
			defer rows.Close()
			rows.Next()
			var ignore int
			var res objectWithAllTypes
			err := rows.Scan(&ignore, &res)
			assertNilF(t, err)
			assertEqualE(t, res.s, "some string")
			assertEqualE(t, res.b, byte(1))
			assertEqualE(t, res.i16, int16(2))
			assertEqualE(t, res.i, 3)
			assertEqualE(t, res.i64, int64(9223372036854775807))
			assertEqualE(t, res.f32, float32(1.1))
			assertEqualE(t, res.f64, 2.2)
			assertEqualE(t, res.bo, true)
			assertBytesEqualE(t, res.bi, []byte{'a', 'b', 'c'})
			assertEqualE(t, res.date, time.Date(2024, time.March, 21, 0, 0, 0, 0, time.UTC))
			assertEqualE(t, res.time.Hour(), 13)
			assertEqualE(t, res.time.Minute(), 3)
			assertEqualE(t, res.time.Second(), 2)
			assertTrueE(t, res.ltz.Equal(time.Date(2021, time.July, 21, 11, 22, 33, 0, warsawTz)))
			assertTrueE(t, res.tz.Equal(time.Date(2022, time.August, 31, 13, 43, 22, 0, warsawTz)))
			assertTrueE(t, res.ntz.Equal(time.Date(2023, time.May, 22, 1, 17, 19, 0, time.UTC)))
			assertEqualE(t, res.so, simpleObject{s: "child", i: 9})
		})
	})
}

type HigherPrecisionStruct struct {
	i *big.Int
	f *big.Float
}

func (hps *HigherPrecisionStruct) Scan(val any) error {
	st := val.(StructuredType)
	var err error
	if hps.i, err = st.GetBigInt("i"); err != nil {
		return err
	}
	if hps.f, err = st.GetBigFloat("f"); err != nil {
		return err
	}
	return nil
}

func TestObjectMetadata(t *testing.T) {
	skipStructuredTypesTestsOnGHActions(t)
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQuery("SELECT {'a': 'b'}::OBJECT(a VARCHAR) as structured_type")
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

func TestObjectWithoutSchema(t *testing.T) {
	skipStructuredTypesTestsOnGHActions(t)
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			if format == "NATIVE_ARROW" {
				t.Skip("Native arrow is not supported in objects without schema")
			}
			rows := dbt.mustQuery("SELECT {'a': 'b'}::OBJECT")
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
	skipStructuredTypesTestsOnGHActions(t)
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

func TestArrayAndMetadata(t *testing.T) {
	skipStructuredTypesTestsOnGHActions(t)
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
					query:     "SELECT ARRAY_CONSTRUCT(1.1, 2.2)::ARRAY(DOUBLE) as structured_type UNION SELECT ARRAY_CONSTRUCT(3.3)::ARRAY(DOUBLE)",
					expected1: []float64{1.1, 2.2},
					expected2: []float64{3.3},
					actual:    []float64{},
				},
				{
					name:      "number - fixed",
					query:     "SELECT ARRAY_CONSTRUCT(1, 2)::ARRAY(NUMBER(38, 0)) as structured_type UNION SELECT ARRAY_CONSTRUCT(3)::ARRAY(NUMBER(38, 0))",
					expected1: []int64{1, 2},
					expected2: []int64{3},
					actual:    []int64{},
				},
				{
					name:      "number - real",
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
					rows := dbt.mustQuery(tc.query)
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
	skipStructuredTypesTestsOnGHActions(t)
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQuery("SELECT ARRAY_CONSTRUCT(1, 2)")
			defer rows.Close()
			rows.Next()
			var v string
			err := rows.Scan(&v)
			assertNilF(t, err)
			assertStringContainsE(t, v, "1,\n  2")
		})
	})
}

func TestEmptyArraysAndNullArrays(t *testing.T) {
	skipStructuredTypesTestsOnGHActions(t)
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQuery("SELECT ARRAY_CONSTRUCT(1, 2)::ARRAY(INTEGER) as structured_type UNION SELECT ARRAY_CONSTRUCT()::ARRAY(INTEGER) UNION SELECT NULL UNION SELECT ARRAY_CONSTRUCT(4, 5, 6)::ARRAY(INTEGER)")
			defer rows.Close()
			checkRow := func(rows *RowsExtended, expected *[]int64) {
				var res *[]int64
				rows.Next()
				err := rows.Scan(&res)
				assertNilF(t, err)
				assertDeepEqualE(t, res, expected)
			}

			checkRow(rows, &[]int64{1, 2})
			checkRow(rows, &[]int64{})
			checkRow(rows, nil)
			checkRow(rows, &[]int64{4, 5, 6})
		})
	})
}

func TestArrayWithoutSchemaMetadata(t *testing.T) {
	skipStructuredTypesTestsOnGHActions(t)
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
	skipStructuredTypesTestsOnGHActions(t)
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			rows := dbt.mustQuery("SELECT ARRAY_CONSTRUCT({'s': 's1', 'i': 9}, {'s': 's2', 'i': 8})::ARRAY(OBJECT(s VARCHAR, i INTEGER)) as structured_type UNION SELECT ARRAY_CONSTRUCT({'s': 's3', 'i': 7})::ARRAY(OBJECT(s VARCHAR, i INTEGER))")
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

func TestWithHigherPrecision(t *testing.T) {
	skipStructuredTypesTestsOnGHActions(t)
	runDBTest(t, func(dbt *DBTest) {
		forAllStructureTypeFormats(dbt, func(t *testing.T, format string) {
			if format != "NATIVE_ARROW" {
				t.Skip("JSON structured type does not support higher precision")
			}
			ctx := WithHigherPrecision(context.Background())
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
		})
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
