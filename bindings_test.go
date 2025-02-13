// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	createTableSQL = `create or replace table test_prep_statement(c1 INTEGER,
		c2 FLOAT, c3 BOOLEAN, c4 STRING, C5 BINARY, C6 TIMESTAMP_NTZ,
		C7 TIMESTAMP_LTZ, C8 TIMESTAMP_TZ, C9 DATE, C10 TIME)`
	deleteTableSQL = "drop table if exists TEST_PREP_STATEMENT"
	insertSQL      = "insert into TEST_PREP_STATEMENT values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	selectAllSQL   = "select * from TEST_PREP_STATEMENT ORDER BY 1"

	createTableSQLBulkArray = `create or replace table test_bulk_array(c1 INTEGER,
		c2 FLOAT, c3 BOOLEAN, c4 STRING, C5 BINARY, C6 INTEGER)`
	deleteTableSQLBulkArray = "drop table if exists test_bulk_array"
	insertSQLBulkArray      = "insert into test_bulk_array values(?, ?, ?, ?, ?, ?)"
	selectAllSQLBulkArray   = "select * from test_bulk_array ORDER BY 1"

	createTableSQLBulkArrayDateTimeTimestamp = `create or replace table test_bulk_array_DateTimeTimestamp(
		C1 TIMESTAMP_NTZ, C2 TIMESTAMP_LTZ, C3 TIMESTAMP_TZ, C4 DATE, C5 TIME)`
	deleteTableSQLBulkArrayDateTimeTimestamp = "drop table if exists test_bulk_array_DateTimeTimestamp"
	insertSQLBulkArrayDateTimeTimestamp      = "insert into test_bulk_array_DateTimeTimestamp values(?, ?, ?, ?, ?)"
	selectAllSQLBulkArrayDateTimeTimestamp   = "select * from test_bulk_array_DateTimeTimestamp ORDER BY 1"

	enableFeatureMaxLOBSize      = "ALTER SESSION SET FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY='ENABLED'"
	unsetFeatureMaxLOBSize       = "ALTER SESSION UNSET FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY"
	enableLargeVarcharAndBinary  = "ALTER SESSION SET ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=TRUE"
	disableLargeVarcharAndBinary = "ALTER SESSION SET ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=FALSE"
	unsetLargeVarcharAndBinary   = "ALTER SESSION UNSET ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT"

	maxVarcharAndBinarySizeParam = "varchar_and_binary_max_size_in_result"

	originSize = 16 * 1024 * 1024
	smallSize  = 16
	// range to use for generating random numbers
	lobRandomRange = 100000
)

var (
	// maxLOBSize = 128 * 1024 * 1024 // new max LOB size
	maxLOBSize = 16 * 1024 * 1024 // current max LOB size
	largeSize  = maxLOBSize / 2
	mediumSize = largeSize / 2
)

func TestBindingFloat64(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		expected := 42.23
		var out float64
		var rows *RowsExtended
		for _, v := range types {
			t.Run(v, func(t *testing.T) {
				dbt.mustExec(fmt.Sprintf("CREATE OR REPLACE TABLE test (id int, value %v)", v))
				dbt.mustExec("INSERT INTO test VALUES (1, ?)", expected)
				rows = dbt.mustQuery("SELECT value FROM test WHERE id = ?", 1)
				defer func() {
					assertNilF(t, rows.Close())
				}()
				if rows.Next() {
					assertNilF(t, rows.Scan(&out))
					if expected != out {
						dbt.Errorf("%s: %g != %g", v, expected, out)
					}
				} else {
					dbt.Errorf("%s: no data", v)
				}
			})
		}
		dbt.mustExec("DROP TABLE IF EXISTS test")
	})
}

// TestBindingUint64 tests uint64 binding. Should fail as unit64 is not a
// supported binding value by Go's sql package.
func TestBindingUint64(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		expected := uint64(18446744073709551615)
		dbt.mustExec("CREATE OR REPLACE TABLE test (id int, value INTEGER)")
		if _, err := dbt.exec("INSERT INTO test VALUES (1, ?)", expected); err == nil {
			dbt.Fatal("should fail as uint64 values with high bit set are not supported.")
		} else {
			logger.Infof("expected err: %v", err)
		}
		dbt.mustExec("DROP TABLE IF EXISTS test")
	})
}

func TestBindingDateTimeTimestamp(t *testing.T) {
	createDSN(PSTLocation)
	runDBTest(t, func(dbt *DBTest) {
		expected := time.Now()
		dbt.mustExec(
			"CREATE OR REPLACE TABLE tztest (id int, ntz timestamp_ntz, ltz timestamp_ltz, dt date, tm time)")
		stmt, err := dbt.prepare("INSERT INTO tztest(id,ntz,ltz,dt,tm) VALUES(1,?,?,?,?)")
		if err != nil {
			dbt.Fatal(err.Error())
		}
		defer stmt.Close()
		if _, err = stmt.Exec(
			DataTypeTimestampNtz, expected,
			DataTypeTimestampLtz, expected,
			DataTypeDate, expected,
			DataTypeTime, expected); err != nil {
			dbt.Fatal(err)
		}
		rows := dbt.mustQuery("SELECT ntz,ltz,dt,tm FROM tztest WHERE id=?", 1)
		defer rows.Close()
		var ntz, vltz, dt, tm time.Time
		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			dbt.Errorf("column type error. err: %v", err)
		}
		if columnTypes[0].Name() != "NTZ" {
			dbt.Errorf("expected column name: %v, got: %v", "TEST", columnTypes[0])
		}
		canNull := dbt.mustNullable(columnTypes[0])
		if !canNull {
			dbt.Errorf("expected nullable: %v, got: %v", true, canNull)
		}
		if columnTypes[0].DatabaseTypeName() != "TIMESTAMP_NTZ" {
			dbt.Errorf("expected database type: %v, got: %v", "TIMESTAMP_NTZ", columnTypes[0].DatabaseTypeName())
		}
		dbt.mustFailDecimalSize(columnTypes[0])
		dbt.mustFailLength(columnTypes[0])
		cols, err := rows.Columns()
		if err != nil {
			dbt.Errorf("failed to get columns. err: %v", err)
		}
		if len(cols) != 4 || cols[0] != "NTZ" || cols[1] != "LTZ" || cols[2] != "DT" || cols[3] != "TM" {
			dbt.Errorf("failed to get columns. got: %v", cols)
		}
		if rows.Next() {
			assertNilF(t, rows.Scan(&ntz, &vltz, &dt, &tm))
			if expected.UnixNano() != ntz.UnixNano() {
				dbt.Errorf("returned TIMESTAMP_NTZ value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, ntz.UnixNano(), ntz)
			}
			if expected.UnixNano() != vltz.UnixNano() {
				dbt.Errorf("returned TIMESTAMP_LTZ value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, vltz.UnixNano(), vltz)
			}
			if expected.Year() != dt.Year() || expected.Month() != dt.Month() || expected.Day() != dt.Day() {
				dbt.Errorf("returned DATE value didn't match. expected: %v:%v, got: %v:%v",
					expected.Unix()*1000, expected, dt.Unix()*1000, dt)
			}
			if expected.Hour() != tm.Hour() || expected.Minute() != tm.Minute() || expected.Second() != tm.Second() || expected.Nanosecond() != tm.Nanosecond() {
				dbt.Errorf("returned TIME value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, tm.UnixNano(), tm)
			}
		} else {
			dbt.Error("no data")
		}
		dbt.mustExec("DROP TABLE tztest")
	})

	createDSN("UTC")
}

func TestBindingBinary(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE bintest (id int, b binary)")
		var b = []byte{0x01, 0x02, 0x03}
		dbt.mustExec("INSERT INTO bintest(id,b) VALUES(1, ?)", DataTypeBinary, b)
		rows := dbt.mustQuery("SELECT b FROM bintest WHERE id=?", 1)
		defer rows.Close()
		if rows.Next() {
			var rb []byte
			if err := rows.Scan(&rb); err != nil {
				dbt.Errorf("failed to scan data. err: %v", err)
			}
			if !bytes.Equal(b, rb) {
				dbt.Errorf("failed to match data. expected: %v, got: %v", b, rb)
			}
		} else {
			dbt.Errorf("no data")
		}
		dbt.mustExec("DROP TABLE bintest")
	})
}

func TestBindingTimestampTZ(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		expected := time.Now()
		dbt.mustExec("CREATE OR REPLACE TABLE tztest (id int, tz timestamp_tz)")
		stmt, err := dbt.prepare("INSERT INTO tztest(id,tz) VALUES(1, ?)")
		if err != nil {
			dbt.Fatal(err.Error())
		}
		defer func() {
			assertNilF(t, stmt.Close())
		}()
		if _, err = stmt.Exec(DataTypeTimestampTz, expected); err != nil {
			dbt.Fatal(err)
		}
		rows := dbt.mustQuery("SELECT tz FROM tztest WHERE id=?", 1)
		defer func() {
			assertNilF(t, rows.Close())
		}()
		var v time.Time
		if rows.Next() {
			assertNilF(t, rows.Scan(&v))
			if expected.UnixNano() != v.UnixNano() {
				dbt.Errorf("returned value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, v.UnixNano(), v)
			}
		} else {
			dbt.Error("no data")
		}
		dbt.mustExec("DROP TABLE tztest")
	})
}

// SNOW-755844: Test the use of a pointer *time.Time type in user-defined structures to perform updates/inserts
func TestBindingTimePtrInStruct(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		type timePtrStruct struct {
			id      *int
			timeVal *time.Time
		}
		expectedID := 1
		expectedTime := time.Now()
		testStruct := timePtrStruct{id: &expectedID, timeVal: &expectedTime}
		dbt.mustExec("CREATE OR REPLACE TABLE timeStructTest (id int, tz timestamp_tz)")

		runInsertQuery := false
		for i := 0; i < 2; i++ {
			if !runInsertQuery {
				_, err := dbt.exec("INSERT INTO timeStructTest(id,tz) VALUES(?, ?)", testStruct.id, testStruct.timeVal)
				if err != nil {
					dbt.Fatal(err.Error())
				}
				runInsertQuery = true
			} else {
				// Update row with a new time value
				expectedTime = time.Now().Add(1)
				testStruct.timeVal = &expectedTime
				_, err := dbt.exec("UPDATE timeStructTest SET tz = ? where id = ?", testStruct.timeVal, testStruct.id)
				if err != nil {
					dbt.Fatal(err.Error())
				}
			}

			rows := dbt.mustQuery("SELECT tz FROM timeStructTest WHERE id=?", &expectedID)
			defer func() {
				assertNilF(t, rows.Close())
			}()
			var v time.Time
			if rows.Next() {
				assertNilF(t, rows.Scan(&v))
				if expectedTime.UnixNano() != v.UnixNano() {
					dbt.Errorf("returned value didn't match. expected: %v:%v, got: %v:%v",
						expectedTime.UnixNano(), expectedTime, v.UnixNano(), v)
				}
			} else {
				dbt.Error("no data")
			}
		}
		dbt.mustExec("DROP TABLE timeStructTest")
	})
}

// SNOW-755844: Test the use of a time.Time type in user-defined structures to perform updates/inserts
func TestBindingTimeInStruct(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		type timeStruct struct {
			id      int
			timeVal time.Time
		}
		expectedID := 1
		expectedTime := time.Now()
		testStruct := timeStruct{id: expectedID, timeVal: expectedTime}
		dbt.mustExec("CREATE OR REPLACE TABLE timeStructTest (id int, tz timestamp_tz)")

		runInsertQuery := false
		for i := 0; i < 2; i++ {
			if !runInsertQuery {
				_, err := dbt.exec("INSERT INTO timeStructTest(id,tz) VALUES(?, ?)", testStruct.id, testStruct.timeVal)
				if err != nil {
					dbt.Fatal(err.Error())
				}
				runInsertQuery = true
			} else {
				// Update row with a new time value
				expectedTime = time.Now().Add(1)
				testStruct.timeVal = expectedTime
				_, err := dbt.exec("UPDATE timeStructTest SET tz = ? where id = ?", testStruct.timeVal, testStruct.id)
				if err != nil {
					dbt.Fatal(err.Error())
				}
			}

			rows := dbt.mustQuery("SELECT tz FROM timeStructTest WHERE id=?", &expectedID)
			defer func() {
				assertNilF(t, rows.Close())
			}()
			var v time.Time
			if rows.Next() {
				assertNilF(t, rows.Scan(&v))
				if expectedTime.UnixNano() != v.UnixNano() {
					dbt.Errorf("returned value didn't match. expected: %v:%v, got: %v:%v",
						expectedTime.UnixNano(), expectedTime, v.UnixNano(), v)
				}
			} else {
				dbt.Error("no data")
			}
		}
		dbt.mustExec("DROP TABLE timeStructTest")
	})
}

func TestBindingInterface(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(
			WithHigherPrecision(context.Background()), selectVariousTypes)
		defer func() {
			assertNilF(t, rows.Close())
		}()
		if !rows.Next() {
			dbt.Error("failed to query")
		}
		var v1, v2, v3, v4, v5, v6 any
		if err := rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6); err != nil {
			dbt.Errorf("failed to scan: %#v", err)
		}
		if s1, ok := v1.(*big.Float); !ok || s1.Cmp(big.NewFloat(1.0)) != 0 {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v1)
		}
		if s2, ok := v2.(int64); !ok || s2 != 2 {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v2)
		}
		if s3, ok := v3.(string); !ok || s3 != "t3" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v3)
		}
		if s4, ok := v4.(float64); !ok || s4 != 4.2 {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v4)
		}
	})
}

func TestBindingInterfaceString(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQuery(selectVariousTypes)
		defer func() {
			assertNilF(t, rows.Close())
		}()
		if !rows.Next() {
			dbt.Error("failed to query")
		}
		var v1, v2, v3, v4, v5, v6 any
		if err := rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6); err != nil {
			dbt.Errorf("failed to scan: %#v", err)
		}
		if s, ok := v1.(string); !ok {
			dbt.Error("failed to convert to string")
		} else if d, err := strconv.ParseFloat(s, 64); err != nil {
			dbt.Errorf("failed to convert to float. value: %v, err: %v", v1, err)
		} else if d != 1.00 {
			dbt.Errorf("failed to fetch. expected: 1.00, value: %v", v1)
		}
		if s, ok := v2.(string); !ok || s != "2" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v2)
		}
		if s, ok := v3.(string); !ok || s != "t3" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v3)
		}
	})
}

func TestBulkArrayBindingUUID(t *testing.T) {
	max := math.Pow10(5) // 100K because my power is maximum
	expectedUuids := make([]any, int(max))

	createTable := "CREATE OR REPLACE TABLE TEST_PREP_STATEMENT (uuid VARCHAR)"
	insert := "INSERT INTO TEST_PREP_STATEMENT (uuid) VALUES (?)"

	for i := range expectedUuids {
		expectedUuids[i] = newTestUUID()
	}

	slices.SortStableFunc(expectedUuids, func(i, j any) int {
		return strings.Compare(i.(testUUID).String(), j.(testUUID).String())
	})

	runDBTest(t, func(dbt *DBTest) {
		var rows *RowsExtended
		t.Cleanup(func() {
			if rows != nil {
				assertNilF(t, rows.Close())
			}

			_, err := dbt.exec(deleteTableSQL)
			if err != nil {
				t.Logf("failed to drop table. err: %s", err)
			}
		})

		dbt.mustExec(createTable)

		res := dbt.mustExec(insert, Array(&expectedUuids))

		affected, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("failed to get affected rows. err: %s", err)
		} else if affected != int64(max) {
			t.Fatalf("failed to insert all rows. expected: %f.0, got: %v", max, affected)
		}

		rows = dbt.mustQuery("SELECT * FROM TEST_PREP_STATEMENT ORDER BY uuid")
		if rows == nil {
			t.Fatal("failed to query")
		}

		if rows.Err() != nil {
			t.Fatalf("failed to query. err: %s", rows.Err())
		}

		var actual = make([]testUUID, len(expectedUuids))

		for i := 0; rows.Next(); i++ {
			var (
				out testUUID
			)
			if err := rows.Scan(&out); err != nil {
				t.Fatal(err)
			}

			actual[i] = out
		}

		for i := range expectedUuids {
			assertEqualE(t, actual[i], expectedUuids[i])
		}
	})

}

func TestBulkArrayBindingInterfaceNil(t *testing.T) {
	nilArray := make([]any, 1)

	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(createTableSQL)
		defer dbt.mustExec(deleteTableSQL)

		dbt.mustExec(insertSQL, Array(&nilArray), Array(&nilArray),
			Array(&nilArray), Array(&nilArray), Array(&nilArray),
			Array(&nilArray, TimestampNTZType), Array(&nilArray, TimestampTZType),
			Array(&nilArray, TimestampTZType), Array(&nilArray, DateType),
			Array(&nilArray, TimeType))
		rows := dbt.mustQuery(selectAllSQL)
		defer func() {
			assertNilF(t, rows.Close())
		}()

		var v0 sql.NullInt32
		var v1 sql.NullFloat64
		var v2 sql.NullBool
		var v3 sql.NullString
		var v4 []byte
		var v5, v6, v7, v8, v9 sql.NullTime

		cnt := 0
		for i := 0; rows.Next(); i++ {
			if err := rows.Scan(&v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9); err != nil {
				t.Fatal(err)
			}
			if v0.Valid {
				t.Fatalf("failed to fetch the sql.NullInt32 column v0. expected %v, got: %v", nilArray[i], v0)
			}
			if v1.Valid {
				t.Fatalf("failed to fetch the sql.NullFloat64 column v1. expected %v, got: %v", nilArray[i], v1)
			}
			if v2.Valid {
				t.Fatalf("failed to fetch the sql.NullBool column v2. expected %v, got: %v", nilArray[i], v2)
			}
			if v3.Valid {
				t.Fatalf("failed to fetch the sql.NullString column v3. expected %v, got: %v", nilArray[i], v3)
			}
			if v4 != nil {
				t.Fatalf("failed to fetch the []byte column v4. expected %v, got: %v", nilArray[i], v4)
			}
			if v5.Valid {
				t.Fatalf("failed to fetch the sql.NullTime column v5. expected %v, got: %v", nilArray[i], v5)
			}
			if v6.Valid {
				t.Fatalf("failed to fetch the sql.NullTime column v6. expected %v, got: %v", nilArray[i], v6)
			}
			if v7.Valid {
				t.Fatalf("failed to fetch the sql.NullTime column v7. expected %v, got: %v", nilArray[i], v7)
			}
			if v8.Valid {
				t.Fatalf("failed to fetch the sql.NullTime column v8. expected %v, got: %v", nilArray[i], v8)
			}
			if v9.Valid {
				t.Fatalf("failed to fetch the sql.NullTime column v9. expected %v, got: %v", nilArray[i], v9)
			}
			cnt++
		}
		if cnt != len(nilArray) {
			t.Fatal("failed to query")
		}
	})
}

func TestBulkArrayBindingInterface(t *testing.T) {
	intArray := make([]any, 3)
	intArray[0] = int32(100)
	intArray[1] = int32(200)

	fltArray := make([]any, 3)
	fltArray[0] = float64(0.1)
	fltArray[2] = float64(5.678)

	boolArray := make([]any, 3)
	boolArray[1] = false
	boolArray[2] = true

	strArray := make([]any, 3)
	strArray[2] = "test3"

	byteArray := make([]any, 3)
	byteArray[0] = []byte{0x01, 0x02, 0x03}
	byteArray[2] = []byte{0x07, 0x08, 0x09}

	int64Array := make([]any, 3)
	int64Array[0] = int64(100)
	int64Array[1] = int64(200)

	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(createTableSQLBulkArray)
		defer dbt.mustExec(deleteTableSQLBulkArray)

		dbt.mustExec(insertSQLBulkArray, Array(&intArray), Array(&fltArray),
			Array(&boolArray), Array(&strArray), Array(&byteArray), Array(&int64Array))
		rows := dbt.mustQuery(selectAllSQLBulkArray)
		defer func() {
			assertNilF(t, rows.Close())
		}()

		var v0 sql.NullInt32
		var v1 sql.NullFloat64
		var v2 sql.NullBool
		var v3 sql.NullString
		var v4 []byte
		var v5 sql.NullInt64

		cnt := 0
		for i := 0; rows.Next(); i++ {
			if err := rows.Scan(&v0, &v1, &v2, &v3, &v4, &v5); err != nil {
				t.Fatal(err)
			}
			if v0.Valid {
				if v0.Int32 != intArray[i] {
					t.Fatalf("failed to fetch the sql.NullInt32 column v0. expected %v, got: %v", intArray[i], v0.Int32)
				}
			} else if intArray[i] != nil {
				t.Fatalf("failed to fetch the sql.NullInt32 column v0. expected %v, got: %v", intArray[i], v0)
			}
			if v1.Valid {
				if v1.Float64 != fltArray[i] {
					t.Fatalf("failed to fetch the sql.NullFloat64 column v1. expected %v, got: %v", fltArray[i], v1.Float64)
				}
			} else if fltArray[i] != nil {
				t.Fatalf("failed to fetch the sql.NullFloat64 column v1. expected %v, got: %v", fltArray[i], v1)
			}
			if v2.Valid {
				if v2.Bool != boolArray[i] {
					t.Fatalf("failed to fetch the sql.NullBool column v2. expected %v, got: %v", boolArray[i], v2.Bool)
				}
			} else if boolArray[i] != nil {
				t.Fatalf("failed to fetch the sql.NullBool column v2. expected %v, got: %v", boolArray[i], v2)
			}
			if v3.Valid {
				if v3.String != strArray[i] {
					t.Fatalf("failed to fetch the sql.NullString column v3. expected %v, got: %v", strArray[i], v3.String)
				}
			} else if strArray[i] != nil {
				t.Fatalf("failed to fetch the sql.NullString column v3. expected %v, got: %v", strArray[i], v3)
			}
			if byteArray[i] != nil {
				if !bytes.Equal(v4, byteArray[i].([]byte)) {
					t.Fatalf("failed to fetch the []byte column v4. expected %v, got: %v", byteArray[i], v4)
				}
			} else if v4 != nil {
				t.Fatalf("failed to fetch the []byte column v4. expected %v, got: %v", byteArray[i], v4)
			}
			if v5.Valid {
				if v5.Int64 != int64Array[i] {
					t.Fatalf("failed to fetch the sql.NullInt64 column v5. expected %v, got: %v", int64Array[i], v5.Int64)
				}
			} else if int64Array[i] != nil {
				t.Fatalf("failed to fetch the sql.NullInt64 column v5. expected %v, got: %v", int64Array[i], v5)
			}
			cnt++
		}
		if cnt != len(intArray) {
			t.Fatal("failed to query")
		}
	})
}

func TestBulkArrayBindingInterfaceDateTimeTimestamp(t *testing.T) {
	tz := time.Now()
	createDSN(PSTLocation)

	now := time.Now()
	loc, err := time.LoadLocation(PSTLocation)
	if err != nil {
		t.Error(err)
	}
	ntzArray := make([]any, 3)
	ntzArray[0] = now
	ntzArray[1] = now.Add(1)

	ltzArray := make([]any, 3)
	ltzArray[1] = now.Add(2).In(loc)
	ltzArray[2] = now.Add(3).In(loc)

	tzArray := make([]any, 3)
	tzArray[0] = tz.Add(4).In(loc)
	tzArray[2] = tz.Add(5).In(loc)

	dtArray := make([]any, 3)
	dtArray[0] = tz.Add(6).In(loc)
	dtArray[1] = now.Add(7).In(loc)

	tmArray := make([]any, 3)
	tmArray[1] = now.Add(8).In(loc)
	tmArray[2] = now.Add(9).In(loc)

	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(createTableSQLBulkArrayDateTimeTimestamp)
		defer dbt.mustExec(deleteTableSQLBulkArrayDateTimeTimestamp)

		dbt.mustExec(insertSQLBulkArrayDateTimeTimestamp,
			Array(&ntzArray, TimestampNTZType), Array(&ltzArray, TimestampLTZType),
			Array(&tzArray, TimestampTZType), Array(&dtArray, DateType),
			Array(&tmArray, TimeType))

		rows := dbt.mustQuery(selectAllSQLBulkArrayDateTimeTimestamp)
		defer func() {
			assertNilF(t, rows.Close())
		}()

		var v0, v1, v2, v3, v4 sql.NullTime

		cnt := 0
		for i := 0; rows.Next(); i++ {
			if err := rows.Scan(&v0, &v1, &v2, &v3, &v4); err != nil {
				t.Fatal(err)
			}
			if v0.Valid {
				if v0.Time.UnixNano() != ntzArray[i].(time.Time).UnixNano() {
					t.Fatalf("failed to fetch the column v0. expected %v, got: %v", ntzArray[i], v0)
				}
			} else if ntzArray[i] != nil {
				t.Fatalf("failed to fetch the column v0. expected %v, got: %v", ntzArray[i], v0)
			}
			if v1.Valid {
				if v1.Time.UnixNano() != ltzArray[i].(time.Time).UnixNano() {
					t.Fatalf("failed to fetch the column v1. expected %v, got: %v", ltzArray[i], v1)
				}
			} else if ltzArray[i] != nil {
				t.Fatalf("failed to fetch the column v1. expected %v, got: %v", ltzArray[i], v1)
			}
			if v2.Valid {
				if v2.Time.UnixNano() != tzArray[i].(time.Time).UnixNano() {
					t.Fatalf("failed to fetch the column v2. expected %v, got: %v", tzArray[i], v2)
				}
			} else if tzArray[i] != nil {
				t.Fatalf("failed to fetch the column v2. expected %v, got: %v", tzArray[i], v2)
			}
			if v3.Valid {
				if v3.Time.Year() != dtArray[i].(time.Time).Year() ||
					v3.Time.Month() != dtArray[i].(time.Time).Month() ||
					v3.Time.Day() != dtArray[i].(time.Time).Day() {
					t.Fatalf("failed to fetch the column v3. expected %v, got: %v", dtArray[i], v3)
				}
			} else if dtArray[i] != nil {
				t.Fatalf("failed to fetch the column v3. expected %v, got: %v", dtArray[i], v3)
			}
			if v4.Valid {
				if v4.Time.Hour() != tmArray[i].(time.Time).Hour() ||
					v4.Time.Minute() != tmArray[i].(time.Time).Minute() ||
					v4.Time.Second() != tmArray[i].(time.Time).Second() {
					t.Fatalf("failed to fetch the column v4. expected %v, got: %v", tmArray[i], v4)
				}
			} else if tmArray[i] != nil {
				t.Fatalf("failed to fetch the column v4. expected %v, got: %v", tmArray[i], v4)
			}
			cnt++
		}
		if cnt != len(ntzArray) {
			t.Fatal("failed to query")
		}
	})
	createDSN("UTC")
}

// TestBindingArray tests basic array binding via the usage of the Array
// function that converts the passed Golang slice to a Snowflake array type
func TestBindingArray(t *testing.T) {
	testBindingArray(t, false)
}

// TestBindingBulkArray tests bulk array binding via the usage of the Array
// function that converts the passed Golang slice to a Snowflake array type
func TestBindingBulkArray(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("client_stage_array_binding_threshold value is internal")
	}
	testBindingArray(t, true)
}

func testBindingArray(t *testing.T, bulk bool) {
	tz := time.Now()
	createDSN(PSTLocation)
	intArray := []int{1, 2, 3}
	fltArray := []float64{0.1, 2.34, 5.678}
	boolArray := []bool{true, false, true}
	strArray := []string{"test1", "test2", "test3"}
	byteArray := [][]byte{{0x01, 0x02, 0x03}, {0x04, 0x05, 0x06}, {0x07, 0x08, 0x09}}

	now := time.Now()
	loc, err := time.LoadLocation(PSTLocation)
	if err != nil {
		t.Error(err)
	}
	ntzArray := []time.Time{now, now.Add(1), now.Add(2)}
	ltzArray := []time.Time{now.Add(3).In(loc), now.Add(4).In(loc), now.Add(5).In(loc)}
	tzArray := []time.Time{tz.Add(6).In(loc), tz.Add(7).In(loc), tz.Add(8).In(loc)}
	dtArray := []time.Time{now.Add(9), now.Add(10), now.Add(11)}
	tmArray := []time.Time{now.Add(12), now.Add(13), now.Add(14)}

	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(createTableSQL)
		defer dbt.mustExec(deleteTableSQL)
		if bulk {
			if _, err := dbt.exec("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); err != nil {
				t.Error(err)
			}
		}

		dbt.mustExec(insertSQL, Array(&intArray), Array(&fltArray),
			Array(&boolArray), Array(&strArray), Array(&byteArray),
			Array(&ntzArray, TimestampNTZType), Array(&ltzArray, TimestampLTZType),
			Array(&tzArray, TimestampTZType), Array(&dtArray, DateType),
			Array(&tmArray, TimeType))
		rows := dbt.mustQuery(selectAllSQL)
		defer func() {
			assertNilF(t, rows.Close())
		}()

		var v0 int
		var v1 float64
		var v2 bool
		var v3 string
		var v4 []byte
		var v5, v6, v7, v8, v9 time.Time
		cnt := 0
		for i := 0; rows.Next(); i++ {
			if err := rows.Scan(&v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9); err != nil {
				t.Fatal(err)
			}
			if v0 != intArray[i] {
				t.Fatalf("failed to fetch. expected %v, got: %v", intArray[i], v0)
			}
			if v1 != fltArray[i] {
				t.Fatalf("failed to fetch. expected %v, got: %v", fltArray[i], v1)
			}
			if v2 != boolArray[i] {
				t.Fatalf("failed to fetch. expected %v, got: %v", boolArray[i], v2)
			}
			if v3 != strArray[i] {
				t.Fatalf("failed to fetch. expected %v, got: %v", strArray[i], v3)
			}
			if !bytes.Equal(v4, byteArray[i]) {
				t.Fatalf("failed to fetch. expected %v, got: %v", byteArray[i], v4)
			}
			if v5.UnixNano() != ntzArray[i].UnixNano() {
				t.Fatalf("failed to fetch. expected %v, got: %v", ntzArray[i], v5)
			}
			if v6.UnixNano() != ltzArray[i].UnixNano() {
				t.Fatalf("failed to fetch. expected %v, got: %v", ltzArray[i], v6)
			}
			if v7.UnixNano() != tzArray[i].UnixNano() {
				t.Fatalf("failed to fetch. expected %v, got: %v", tzArray[i], v7)
			}
			if v8.Year() != dtArray[i].Year() || v8.Month() != dtArray[i].Month() || v8.Day() != dtArray[i].Day() {
				t.Fatalf("failed to fetch. expected %v, got: %v", dtArray[i], v8)
			}
			if v9.Hour() != tmArray[i].Hour() || v9.Minute() != tmArray[i].Minute() || v9.Second() != tmArray[i].Second() {
				t.Fatalf("failed to fetch. expected %v, got: %v", tmArray[i], v9)
			}
			cnt++
		}
		if cnt != len(intArray) {
			t.Fatal("failed to query")
		}
	})
	createDSN("UTC")
}

func TestBulkArrayBinding(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(fmt.Sprintf("create or replace table %v (c1 integer, c2 string, c3 timestamp_ltz, c4 timestamp_tz, c5 timestamp_ntz, c6 date, c7 time, c8 binary)", dbname))
		now := time.Now()
		someTime := time.Date(1, time.January, 1, 12, 34, 56, 123456789, time.UTC)
		someDate := time.Date(2024, time.March, 18, 0, 0, 0, 0, time.UTC)
		someBinary := []byte{0x01, 0x02, 0x03}
		numRows := 100000
		intArr := make([]int, numRows)
		strArr := make([]string, numRows)
		ltzArr := make([]time.Time, numRows)
		tzArr := make([]time.Time, numRows)
		ntzArr := make([]time.Time, numRows)
		dateArr := make([]time.Time, numRows)
		timeArr := make([]time.Time, numRows)
		binArr := make([][]byte, numRows)
		for i := 0; i < numRows; i++ {
			intArr[i] = i
			strArr[i] = "test" + strconv.Itoa(i)
			ltzArr[i] = now
			tzArr[i] = now.Add(time.Hour).UTC()
			ntzArr[i] = now.Add(2 * time.Hour)
			dateArr[i] = someDate
			timeArr[i] = someTime
			binArr[i] = someBinary
		}
		dbt.mustExec(fmt.Sprintf("insert into %v values (?, ?, ?, ?, ?, ?, ?, ?)", dbname), Array(&intArr), Array(&strArr), Array(&ltzArr, TimestampLTZType), Array(&tzArr, TimestampTZType), Array(&ntzArr, TimestampNTZType), Array(&dateArr, DateType), Array(&timeArr, TimeType), Array(&binArr))
		rows := dbt.mustQuery("select * from " + dbname + " order by c1")
		defer func() {
			assertNilF(t, rows.Close())
		}()
		cnt := 0
		var i int
		var s string
		var ltz, tz, ntz, date, tt time.Time
		var b []byte
		for rows.Next() {
			if err := rows.Scan(&i, &s, &ltz, &tz, &ntz, &date, &tt, &b); err != nil {
				t.Fatal(err)
			}
			assertEqualE(t, i, cnt)
			assertEqualE(t, "test"+strconv.Itoa(cnt), s)
			assertEqualE(t, ltz.UTC(), now.UTC())
			assertEqualE(t, tz.UTC(), now.Add(time.Hour).UTC())
			assertEqualE(t, ntz.UTC(), now.Add(2*time.Hour).UTC())
			assertEqualE(t, date, someDate)
			assertEqualE(t, tt, someTime)
			assertBytesEqualE(t, b, someBinary)
			cnt++
		}
		if cnt != numRows {
			t.Fatalf("expected %v rows, got %v", numRows, cnt)
		}
	})
}

func TestBindingsWithSameValue(t *testing.T) {
	arrayInsertTable := "test_array_binding_insert"
	stageBindingTable := "test_stage_binding_insert"
	interfaceArrayTable := "test_interface_binding_insert"

	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(fmt.Sprintf("create or replace table %v (c1 integer, c2 string, c3 timestamp_ltz, c4 timestamp_tz, c5 timestamp_ntz, c6 date, c7 time, c9 boolean, c10 double)", arrayInsertTable))
		dbt.mustExec(fmt.Sprintf("create or replace table %v (c1 integer, c2 string, c3 timestamp_ltz, c4 timestamp_tz, c5 timestamp_ntz, c6 date, c7 time, c9 boolean, c10 double)", stageBindingTable))
		dbt.mustExec(fmt.Sprintf("create or replace table %v (c1 integer, c2 string, c3 timestamp_ltz, c4 timestamp_tz, c5 timestamp_ntz, c6 date, c7 time, c9 boolean, c10 double)", interfaceArrayTable))

		defer func() {
			dbt.mustExec(fmt.Sprintf("drop table if exists %v", arrayInsertTable))
			dbt.mustExec(fmt.Sprintf("drop table if exists %v", stageBindingTable))
			dbt.mustExec(fmt.Sprintf("drop table if exists %v", interfaceArrayTable))
		}()

		numRows := 5

		intArr := make([]int, numRows)
		strArr := make([]string, numRows)
		timeArr := make([]time.Time, numRows)
		boolArr := make([]bool, numRows)
		doubleArr := make([]float64, numRows)

		intAnyArr := make([]any, numRows)
		strAnyArr := make([]any, numRows)
		timeAnyArr := make([]any, numRows)
		boolAnyArr := make([]bool, numRows)
		doubleAnyArr := make([]float64, numRows)

		for i := 0; i < numRows; i++ {
			intArr[i] = i
			intAnyArr[i] = i

			double := rand.Float64()
			doubleArr[i] = double
			doubleAnyArr[i] = double

			strArr[i] = "test" + strconv.Itoa(i)
			strAnyArr[i] = "test" + strconv.Itoa(i)

			b := getRandomBool()
			boolArr[i] = b
			boolAnyArr[i] = b

			date := getRandomDate()
			timeArr[i] = date
			timeAnyArr[i] = date
		}

		dbt.mustExec(fmt.Sprintf("insert into %v values (?, ?, ?, ?, ?, ?, ?, ?, ?)", interfaceArrayTable), Array(&intAnyArr), Array(&strAnyArr), Array(&timeAnyArr, TimestampLTZType), Array(&timeAnyArr, TimestampTZType), Array(&timeAnyArr, TimestampNTZType), Array(&timeAnyArr, DateType), Array(&timeAnyArr, TimeType), Array(&boolArr), Array(&doubleArr))
		dbt.mustExec(fmt.Sprintf("insert into %v values (?, ?, ?, ?, ?, ?, ?, ?, ?)", arrayInsertTable), Array(&intArr), Array(&strArr), Array(&timeArr, TimestampLTZType), Array(&timeArr, TimestampTZType), Array(&timeArr, TimestampNTZType), Array(&timeArr, DateType), Array(&timeArr, TimeType), Array(&boolArr), Array(&doubleArr))
		dbt.mustExec("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1")
		dbt.mustExec(fmt.Sprintf("insert into %v values (?, ?, ?, ?, ?, ?, ?, ?, ?)", stageBindingTable), Array(&intArr), Array(&strArr), Array(&timeArr, TimestampLTZType), Array(&timeArr, TimestampTZType), Array(&timeArr, TimestampNTZType), Array(&timeArr, DateType), Array(&timeArr, TimeType), Array(&boolArr), Array(&doubleArr))

		insertRows := dbt.mustQuery("select * from " + arrayInsertTable + " order by c1")
		bindingRows := dbt.mustQuery("select * from " + stageBindingTable + " order by c1")
		interfaceRows := dbt.mustQuery("select * from " + interfaceArrayTable + " order by c1")

		defer func() {
			assertNilF(t, insertRows.Close())
			assertNilF(t, bindingRows.Close())
			assertNilF(t, interfaceRows.Close())
		}()
		var i, bi, ii int
		var s, bs, is string
		var ltz, bltz, iltz, itz, btz, tz, intz, ntz, bntz, iDate, date, bDate, itt, tt, btt time.Time
		var b, bb, ib bool
		var d, bd, id float64

		timeFormat := "15:04:05"
		for k := 0; k < numRows; k++ {
			assertTrueF(t, insertRows.Next())
			assertNilF(t, insertRows.Scan(&i, &s, &ltz, &tz, &ntz, &date, &tt, &b, &d))

			assertTrueF(t, bindingRows.Next())
			assertNilF(t, bindingRows.Scan(&bi, &bs, &bltz, &btz, &bntz, &bDate, &btt, &bb, &bd))

			assertTrueF(t, interfaceRows.Next())
			assertNilF(t, interfaceRows.Scan(&ii, &is, &iltz, &itz, &intz, &iDate, &itt, &ib, &id))

			assertEqualE(t, k, i)
			assertEqualE(t, k, bi)
			assertEqualE(t, k, ii)

			assertEqualE(t, "test"+strconv.Itoa(k), s)
			assertEqualE(t, "test"+strconv.Itoa(k), bs)
			assertEqualE(t, "test"+strconv.Itoa(k), is)

			utcTime := timeArr[k].UTC()
			assertEqualE(t, ltz.UTC(), utcTime)
			assertEqualE(t, bltz.UTC(), utcTime)
			assertEqualE(t, iltz.UTC(), utcTime)

			assertEqualE(t, tz.UTC(), utcTime)
			assertEqualE(t, btz.UTC(), utcTime)
			assertEqualE(t, itz.UTC(), utcTime)

			assertEqualE(t, ntz.UTC(), utcTime)
			assertEqualE(t, bntz.UTC(), utcTime)
			assertEqualE(t, intz.UTC(), utcTime)

			testingDate := timeArr[k].Truncate(24 * time.Hour)
			assertEqualE(t, date, testingDate)
			assertEqualE(t, bDate, testingDate)
			assertEqualE(t, iDate, testingDate)

			testingTime := timeArr[k].Format(timeFormat)
			assertEqualE(t, tt.Format(timeFormat), testingTime)
			assertEqualE(t, btt.Format(timeFormat), testingTime)
			assertEqualE(t, itt.Format(timeFormat), testingTime)

			assertEqualE(t, b, boolArr[k])
			assertEqualE(t, bb, boolArr[k])
			assertEqualE(t, ib, boolArr[k])

			assertEqualE(t, d, doubleArr[k])
			assertEqualE(t, bd, doubleArr[k])
			assertEqualE(t, id, doubleArr[k])

		}
	})
}

func TestBulkArrayBindingTimeWithPrecision(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(fmt.Sprintf("create or replace table %v (s time(0), ms time(3), us time(6), ns time(9))", dbname))
		someTimeWithSeconds := time.Date(1, time.January, 1, 1, 1, 1, 0, time.UTC)
		someTimeWithMilliseconds := time.Date(1, time.January, 1, 2, 2, 2, 123000000, time.UTC)
		someTimeWithMicroseconds := time.Date(1, time.January, 1, 3, 3, 3, 123456000, time.UTC)
		someTimeWithNanoseconds := time.Date(1, time.January, 1, 4, 4, 4, 123456789, time.UTC)
		numRows := 100000
		secondsArr := make([]time.Time, numRows)
		millisecondsArr := make([]time.Time, numRows)
		microsecondsArr := make([]time.Time, numRows)
		nanosecondsArr := make([]time.Time, numRows)
		for i := 0; i < numRows; i++ {
			secondsArr[i] = someTimeWithSeconds
			millisecondsArr[i] = someTimeWithMilliseconds
			microsecondsArr[i] = someTimeWithMicroseconds
			nanosecondsArr[i] = someTimeWithNanoseconds
		}
		dbt.mustExec(fmt.Sprintf("insert into %v values (?, ?, ?, ?)", dbname), Array(&secondsArr, TimeType), Array(&millisecondsArr, TimeType), Array(&microsecondsArr, TimeType), Array(&nanosecondsArr, TimeType))
		rows := dbt.mustQuery("select * from " + dbname)
		defer func() {
			assertNilF(t, rows.Close())
		}()
		cnt := 0
		var s, ms, us, ns time.Time
		for rows.Next() {
			if err := rows.Scan(&s, &ms, &us, &ns); err != nil {
				t.Fatal(err)
			}
			assertEqualE(t, s, someTimeWithSeconds)
			assertEqualE(t, ms, someTimeWithMilliseconds)
			assertEqualE(t, us, someTimeWithMicroseconds)
			assertEqualE(t, ns, someTimeWithNanoseconds)
			cnt++
		}
		if cnt != numRows {
			t.Fatalf("expected %v rows, got %v", numRows, cnt)
		}
	})
}

func TestBulkArrayMultiPartBinding(t *testing.T) {
	rowCount := 1000000 // large enough to be partitioned into multiple files
	randomIter := rand.Intn(3) + 2
	randomStrings := make([]string, rowCount)
	str := randomString(30)
	for i := 0; i < rowCount; i++ {
		randomStrings[i] = str
	}
	tempTableName := fmt.Sprintf("test_table_%v", randomString(5))
	ctx := context.Background()

	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(fmt.Sprintf("CREATE TABLE %s (C VARCHAR(64) NOT NULL)", tempTableName))
		defer dbt.mustExec("drop table " + tempTableName)

		for i := 0; i < randomIter; i++ {
			dbt.mustExecContext(ctx,
				fmt.Sprintf("INSERT INTO %s VALUES (?)", tempTableName),
				Array(&randomStrings))
			rows := dbt.mustQuery("select count(*) from " + tempTableName)
			defer func() {
				assertNilF(t, rows.Close())
			}()
			if rows.Next() {
				var count int
				if err := rows.Scan(&count); err != nil {
					t.Error(err)
				}
			}
		}

		rows := dbt.mustQuery("select count(*) from " + tempTableName)
		defer func() {
			assertNilF(t, rows.Close())
		}()
		if rows.Next() {
			var count int
			if err := rows.Scan(&count); err != nil {
				t.Error(err)
			}
			if count != randomIter*rowCount {
				t.Errorf("expected %v rows, got %v rows intead", randomIter*rowCount, count)
			}
		}
	})
}

func TestBulkArrayMultiPartBindingInt(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("create or replace table binding_test (c1 integer)")
		startNum := 1000000
		endNum := 3000000
		numRows := endNum - startNum
		intArr := make([]int, numRows)
		for i := startNum; i < endNum; i++ {
			intArr[i-startNum] = i
		}
		_, err := dbt.exec("insert into binding_test values (?)", Array(&intArr))
		if err != nil {
			t.Errorf("Should have succeeded to insert. err: %v", err)
		}

		rows := dbt.mustQuery("select * from binding_test order by c1")
		defer func() {
			assertNilF(t, rows.Close())
		}()
		cnt := startNum
		var i int
		for rows.Next() {
			if err := rows.Scan(&i); err != nil {
				t.Fatal(err)
			}
			if i != cnt {
				t.Errorf("expected: %v, got: %v", cnt, i)
			}
			cnt++
		}
		if cnt != endNum {
			t.Fatalf("expected %v rows, got %v", numRows, cnt-startNum)
		}
		dbt.mustExec("DROP TABLE binding_test")
	})
}

func TestBulkArrayMultiPartBindingWithNull(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("create or replace table binding_test (c1 integer, c2 string)")
		startNum := 1000000
		endNum := 2000000
		numRows := endNum - startNum

		// Define the integer and string arrays
		intArr := make([]any, numRows)
		stringArr := make([]any, numRows)
		for i := startNum; i < endNum; i++ {
			intArr[i-startNum] = i
			stringArr[i-startNum] = fmt.Sprint(i)
		}

		// Set some of the rows to NULL
		intArr[numRows-1] = nil
		intArr[numRows-2] = nil
		intArr[numRows-3] = nil
		stringArr[1] = nil
		stringArr[2] = nil
		stringArr[3] = nil

		_, err := dbt.exec("insert into binding_test values (?, ?)", Array(&intArr), Array(&stringArr))
		if err != nil {
			t.Errorf("Should have succeeded to insert. err: %v", err)
		}

		rows := dbt.mustQuery("select * from binding_test order by c1,c2")
		defer func() {
			assertNilF(t, rows.Close())
		}()
		cnt := startNum
		var i sql.NullInt32
		var s sql.NullString
		for rows.Next() {
			if err := rows.Scan(&i, &s); err != nil {
				t.Fatal(err)
			}
			// Verify integer column c1
			if i.Valid {
				if int(i.Int32) != intArr[cnt-startNum] {
					t.Fatalf("expected: %v, got: %v", cnt, int(i.Int32))
				}
			} else if !(cnt == startNum+numRows-1 || cnt == startNum+numRows-2 || cnt == startNum+numRows-3) {
				t.Fatalf("expected NULL in column c1 at index: %v", cnt-startNum)
			}
			// Verify string column c2
			if s.Valid {
				if s.String != stringArr[cnt-startNum] {
					t.Fatalf("expected: %v, got: %v", cnt, s.String)
				}
			} else if !(cnt == startNum+1 || cnt == startNum+2 || cnt == startNum+3) {
				t.Fatalf("expected NULL in column c2 at index: %v", cnt-startNum)
			}
			cnt++
		}
		if cnt != endNum {
			t.Fatalf("expected %v rows, got %v", numRows, cnt-startNum)
		}
		dbt.mustExec("DROP TABLE binding_test")
	})
}

func TestFunctionParameters(t *testing.T) {
	testcases := []struct {
		testDesc   string
		paramType  string
		input      any
		nullResult bool
	}{
		{"textAndNullStringResultInNull", "text", sql.NullString{}, true},
		{"numberAndNullInt64ResultInNull", "number", sql.NullInt64{}, true},
		{"floatAndNullFloat64ResultInNull", "float", sql.NullFloat64{}, true},
		{"booleanAndAndNullBoolResultInNull", "boolean", sql.NullBool{}, true},
		{"dateAndTypedNullTimeResultInNull", "date", TypedNullTime{sql.NullTime{}, DateType}, true},
		{"datetimeAndTypedNullTimeResultInNull", "datetime", TypedNullTime{sql.NullTime{}, TimestampNTZType}, true},
		{"timeAndTypedNullTimeResultInNull", "time", TypedNullTime{sql.NullTime{}, TimeType}, true},
		{"timestampAndTypedNullTimeResultInNull", "timestamp", TypedNullTime{sql.NullTime{}, TimestampNTZType}, true},
		{"timestamp_ntzAndTypedNullTimeResultInNull", "timestamp_ntz", TypedNullTime{sql.NullTime{}, TimestampNTZType}, true},
		{"timestamp_ltzAndTypedNullTimeResultInNull", "timestamp_ltz", TypedNullTime{sql.NullTime{}, TimestampLTZType}, true},
		{"timestamp_tzAndTypedNullTimeResultInNull", "timestamp_tz", TypedNullTime{sql.NullTime{}, TimestampTZType}, true},
		{"textAndStringResultInNotNull", "text", "string", false},
		{"numberAndIntegerResultInNotNull", "number", 123, false},
		{"floatAndFloatResultInNotNull", "float", 123.01, false},
		{"booleanAndBooleanResultInNotNull", "boolean", true, false},
		{"dateAndTimeResultInNotNull", "date", time.Now(), false},
		{"datetimeAndTimeResultInNotNull", "datetime", time.Now(), false},
		{"timeAndTimeResultInNotNull", "time", time.Now(), false},
		{"timestampAndTimeResultInNotNull", "timestamp", time.Now(), false},
		{"timestamp_ntzAndTimeResultInNotNull", "timestamp_ntz", time.Now(), false},
		{"timestamp_ltzAndTimeResultInNotNull", "timestamp_ltz", time.Now(), false},
		{"timestamp_tzAndTimeResultInNotNull", "timestamp_tz", time.Now(), false},
	}

	runDBTest(t, func(dbt *DBTest) {
		_, err := dbt.exec("ALTER SESSION SET BIND_NULL_VALUE_USE_NULL_DATATYPE=false")
		if err != nil {
			log.Println(err)
		}
		for _, tc := range testcases {
			t.Run(tc.testDesc, func(t *testing.T) {
				query := fmt.Sprintf(`
				CREATE OR REPLACE FUNCTION NULLPARAMFUNCTION("param1" %v)
				RETURNS TABLE("r1" %v)
				LANGUAGE SQL
				AS 'select param1';`, tc.paramType, tc.paramType)
				dbt.mustExec(query)
				rows, err := dbt.query("select * from table(NULLPARAMFUNCTION(?))", tc.input)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					assertNilF(t, rows.Close())
				}()
				if rows.Err() != nil {
					t.Fatal(err)
				}
				if !rows.Next() {
					t.Fatal("no rows fetched")
				}
				var r1 any
				err = rows.Scan(&r1)
				if err != nil {
					t.Fatal(err)
				}
				if tc.nullResult && r1 != nil {
					t.Fatalf("the result for %v is of type %v but should be null", tc.paramType, reflect.TypeOf(r1))
				}
				if !tc.nullResult && r1 == nil {
					t.Fatalf("the result for %v should not be null", tc.paramType)
				}
			})
		}
	})
}

func TestVariousBindingModes(t *testing.T) {
	testcases := []struct {
		testDesc  string
		paramType string
		input     any
		isNil     bool
	}{
		{"textAndString", "text", "string", false},
		{"numberAndInteger", "number", 123, false},
		{"floatAndFloat", "float", 123.01, false},
		{"booleanAndBoolean", "boolean", true, false},
		{"dateAndTime", "date", time.Now().Truncate(24 * time.Hour), false},
		{"datetimeAndTime", "datetime", time.Now(), false},
		{"timeAndTime", "time", "12:34:56", false},
		{"timestampAndTime", "timestamp", time.Now(), false},
		{"timestamp_ntzAndTime", "timestamp_ntz", time.Now(), false},
		{"timestamp_ltzAndTime", "timestamp_ltz", time.Now(), false},
		{"timestamp_tzAndTime", "timestamp_tz", time.Now(), false},
		{"textAndNullString", "text", sql.NullString{}, true},
		{"numberAndNullInt64", "number", sql.NullInt64{}, true},
		{"floatAndNullFloat64", "float", sql.NullFloat64{}, true},
		{"booleanAndAndNullBool", "boolean", sql.NullBool{}, true},
		{"dateAndTypedNullTime", "date", TypedNullTime{sql.NullTime{}, DateType}, true},
		{"datetimeAndTypedNullTime", "datetime", TypedNullTime{sql.NullTime{}, TimestampNTZType}, true},
		{"timeAndTypedNullTime", "time", TypedNullTime{sql.NullTime{}, TimeType}, true},
		{"timestampAndTypedNullTime", "timestamp", TypedNullTime{sql.NullTime{}, TimestampNTZType}, true},
		{"timestamp_ntzAndTypedNullTime", "timestamp_ntz", TypedNullTime{sql.NullTime{}, TimestampNTZType}, true},
		{"timestamp_ltzAndTypedNullTime", "timestamp_ltz", TypedNullTime{sql.NullTime{}, TimestampLTZType}, true},
		{"timestamp_tzAndTypedNullTime", "timestamp_tz", TypedNullTime{sql.NullTime{}, TimestampTZType}, true},
		{"LOBSmallSize", fmt.Sprintf("varchar(%v)", smallSize), randomString(smallSize), false},
		{"LOBOriginSize", fmt.Sprintf("varchar(%v)", originSize), randomString(originSize), false},
		{"LOBMediumSize", fmt.Sprintf("varchar(%v)", mediumSize), randomString(mediumSize), false},
		{"LOBLargeSize", fmt.Sprintf("varchar(%v)", largeSize), randomString(largeSize), false},
		{"LOBMaxSize", fmt.Sprintf("varchar(%v)", maxLOBSize), randomString(maxLOBSize), false},
	}

	bindingModes := []struct {
		param     string
		query     string
		transform func(any) any
	}{
		{
			param:     "?",
			transform: func(v any) any { return v },
		},
		{
			param:     ":1",
			transform: func(v any) any { return v },
		},
		{
			param:     ":param",
			transform: func(v any) any { return sql.Named("param", v) },
		},
	}

	runDBTest(t, func(dbt *DBTest) {
		for _, tc := range testcases {
			// TODO SNOW-1264687
			if strings.Contains(tc.testDesc, "LOB") {
				skipOnJenkins(t, "skipped until SNOW-1264687 is fixed")
			}
			for _, bindingMode := range bindingModes {
				t.Run(tc.testDesc+" "+bindingMode.param, func(t *testing.T) {
					query := fmt.Sprintf(`CREATE OR REPLACE TABLE BINDING_MODES(param1 %v)`, tc.paramType)
					dbt.mustExec(query)
					if _, err := dbt.exec(fmt.Sprintf("INSERT INTO BINDING_MODES VALUES (%v)", bindingMode.param), bindingMode.transform(tc.input)); err != nil {
						t.Fatal(err)
					}
					if tc.isNil {
						query = "SELECT * FROM BINDING_MODES WHERE param1 IS NULL"
					} else {
						query = fmt.Sprintf("SELECT * FROM BINDING_MODES WHERE param1 = %v", bindingMode.param)
					}
					rows, err := dbt.query(query, bindingMode.transform(tc.input))
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						assertNilF(t, rows.Close())
					}()
					if !rows.Next() {
						t.Fatal("Expected to return a row")
					}
				})
			}
		}
	})
}

func skipMaxLobSizeTestOnGithubActions(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("Max Lob Size parameters are not available on GH Actions")
	}
}

func TestLOBRetrievalWithArrow(t *testing.T) {
	testLOBRetrieval(t, true)
}

func TestLOBRetrievalWithJSON(t *testing.T) {
	testLOBRetrieval(t, false)
}

func testLOBRetrieval(t *testing.T, useArrowFormat bool) {
	runDBTest(t, func(dbt *DBTest) {
		parameters := dbt.connParams()
		varcharBinaryMaxSizeRaw := parameters[maxVarcharAndBinarySizeParam]
		if varcharBinaryMaxSizeRaw != nil && *varcharBinaryMaxSizeRaw != "" {
			varcharBinaryMaxSize, err := strconv.ParseFloat(*varcharBinaryMaxSizeRaw, 64)
			assertNilF(t, err, "error during varcharBinaryMaxSize conversion")
			maxLOBSize = int(varcharBinaryMaxSize)
		}

		dbt.Logf("using %v as max LOB size", maxLOBSize)
		if useArrowFormat {
			dbt.mustExec(forceARROW)
		} else {
			dbt.mustExec(forceJSON)
		}

		var res string
		// the LOB sizes to be tested
		testSizes := [5]int{smallSize, originSize, mediumSize, largeSize, maxLOBSize}
		for _, testSize := range testSizes {
			t.Run(fmt.Sprintf("testLOB_%v_useArrowFormat=%v", strconv.Itoa(testSize), strconv.FormatBool(useArrowFormat)), func(t *testing.T) {
				rows, err := dbt.query(fmt.Sprintf("SELECT randstr(%v, 124)", testSize))
				assertNilF(t, err)
				defer func() {
					assertNilF(t, rows.Close())
				}()
				assertTrueF(t, rows.Next(), fmt.Sprintf("no rows returned for the LOB size %v", testSize))

				// retrieve the result
				err = rows.Scan(&res)
				assertNilF(t, err)

				// verify the length of the result
				assertEqualF(t, len(res), testSize)
			})
		}
		dbt.mustExec(unsetFeatureMaxLOBSize)
	})
}

func TestMaxLobSize(t *testing.T) {
	skipMaxLobSizeTestOnGithubActions(t)
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(enableFeatureMaxLOBSize)
		defer dbt.mustExec(unsetLargeVarcharAndBinary)
		t.Run("Max Lob Size disabled", func(t *testing.T) {
			dbt.mustExec(disableLargeVarcharAndBinary)
			_, err := dbt.query("select randstr(20000000, random())")
			assertNotNilF(t, err)
			assertStringContainsF(t, err.Error(), "Actual length 20000000 exceeds supported length")
		})

		t.Run("Max Lob Size enabled", func(t *testing.T) {
			dbt.mustExec(enableLargeVarcharAndBinary)
			rows, err := dbt.query("select randstr(20000000, random())")
			assertNilF(t, err)
			defer func() {
				assertNilF(t, rows.Close())
			}()
		})
	})
}

func TestInsertLobDataWithLiteralArrow(t *testing.T) {
	// TODO SNOW-1264687
	skipOnJenkins(t, "skipped until SNOW-1264687 is fixed")
	testInsertLOBData(t, true, true)
}

func TestInsertLobDataWithLiteralJSON(t *testing.T) {
	// TODO SNOW-1264687
	skipOnJenkins(t, "skipped until SNOW-1264687 is fixed")
	testInsertLOBData(t, false, true)
}

func TestInsertLobDataWithBindingsArrow(t *testing.T) {
	// TODO SNOW-1264687
	skipOnJenkins(t, "skipped until SNOW-1264687 is fixed")
	testInsertLOBData(t, true, false)
}

func TestInsertLobDataWithBindingsJSON(t *testing.T) {
	// TODO SNOW-1264687
	skipOnJenkins(t, "skipped until SNOW-1264687 is fixed")
	testInsertLOBData(t, false, false)
}

func testInsertLOBData(t *testing.T, useArrowFormat bool, isLiteral bool) {
	expectedNumCols := 3
	columnMeta := []struct {
		columnName string
		columnType reflect.Type
	}{
		{"C1", reflect.TypeOf("")},
		{"C2", reflect.TypeOf("")},
		{"C3", reflect.TypeOf(int64(0))},
	}
	testCases := []struct {
		testDesc string
		c1Size   int
		c2Size   int
		c3Size   int
	}{
		{"testLOBInsertSmallSize", smallSize, smallSize, lobRandomRange},
		{"testLOBInsertOriginSize", originSize, originSize, lobRandomRange},
		{"testLOBInsertMediumSize", mediumSize, originSize, lobRandomRange},
		{"testLOBInsertLargeSize", largeSize, originSize, lobRandomRange},
		{"testLOBInsertMaxSize", maxLOBSize, originSize, lobRandomRange},
	}

	runDBTest(t, func(dbt *DBTest) {
		var c1 string
		var c2 string
		var c3 int

		dbt.mustExec(enableFeatureMaxLOBSize)
		if useArrowFormat {
			dbt.mustExec(forceARROW)
		} else {
			dbt.mustExec(forceJSON)
		}

		for _, tc := range testCases {
			t.Run(tc.testDesc, func(t *testing.T) {
				// initialize test data
				c1Data := randomString(tc.c1Size)
				c2Data := randomString(tc.c2Size)
				c3Data := rand.Intn(tc.c3Size)

				dbt.mustExec(fmt.Sprintf("CREATE OR REPLACE TABLE lob_test_table (c1 varchar(%v), c2 varchar(%v), c3 int)", tc.c1Size, tc.c2Size))
				if isLiteral {
					dbt.mustExec(fmt.Sprintf("INSERT INTO lob_test_table VALUES ('%s', '%s', %v)", c1Data, c2Data, c3Data))
				} else {
					dbt.mustExec("INSERT INTO lob_test_table VALUES (?, ?, ?)", c1Data, c2Data, c3Data)
				}
				rows, err := dbt.query("SELECT * FROM lob_test_table")
				assertNilF(t, err)
				defer func() {
					assertNilF(t, rows.Close())
				}()
				assertTrueF(t, rows.Next(), fmt.Sprintf("%s: no rows returned", tc.testDesc))

				err = rows.Scan(&c1, &c2, &c3)
				assertNilF(t, err)

				// check the number of columns
				columnTypes, err := rows.ColumnTypes()
				assertNilF(t, err)
				assertEqualF(t, len(columnTypes), expectedNumCols)

				// verify the column metadata: name, type and length
				for colIdx := 0; colIdx < expectedNumCols; colIdx++ {
					colName := columnTypes[colIdx].Name()
					assertEqualF(t, colName, columnMeta[colIdx].columnName)

					colType := columnTypes[colIdx].ScanType()
					assertEqualF(t, colType, columnMeta[colIdx].columnType)

					colLength, ok := columnTypes[colIdx].Length()

					switch colIdx {
					case 0:
						assertTrueF(t, ok)
						assertEqualF(t, colLength, int64(tc.c1Size))
						// verify the data
						assertEqualF(t, c1, c1Data)
					case 1:
						assertTrueF(t, ok)
						assertEqualF(t, colLength, int64(tc.c2Size))
						// verify the data
						assertEqualF(t, c2, c2Data)
					case 2:
						assertFalseF(t, ok)
						// verify the data
						assertEqualF(t, c3, c3Data)
					}
				}
			})
			dbt.mustExec("DROP TABLE IF EXISTS lob_test_table")
		}
		dbt.mustExec(unsetFeatureMaxLOBSize)
	})
}

func getRandomDate() time.Time {
	return time.Date(rand.Intn(1582)+1, time.January, rand.Intn(40), rand.Intn(40), rand.Intn(40), rand.Intn(40), rand.Intn(40), time.UTC)
}

func getRandomBool() bool {
	return rand.Int63n(time.Now().Unix())%2 == 0
}
