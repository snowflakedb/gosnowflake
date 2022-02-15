// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
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
		c2 FLOAT, c3 BOOLEAN, c4 STRING, C5 BINARY)`
	deleteTableSQLBulkArray = "drop table if exists test_bulk_array"
	insertSQLBulkArray      = "insert into test_bulk_array values(?, ?, ?, ?, ?)"
	selectAllSQLBulkArray   = "select * from test_bulk_array ORDER BY 1"

	createTableSQLBulkArrayDateTimeTimestamp = `create or replace table test_bulk_array_DateTimeTimestamp(
		C1 TIMESTAMP_NTZ, C2 TIMESTAMP_LTZ, C3 TIMESTAMP_TZ, C4 DATE, C5 TIME)`
	deleteTableSQLBulkArrayDateTimeTimestamp = "drop table if exists test_bulk_array_DateTimeTimestamp"
	insertSQLBulkArrayDateTimeTimestamp      = "insert into test_bulk_array_DateTimeTimestamp values(?, ?, ?, ?, ?)"
	selectAllSQLBulkArrayDateTimeTimestamp   = "select * from test_bulk_array_DateTimeTimestamp ORDER BY 1"
)

func TestBindingNull(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (id int, c1 STRING, c2 BOOLEAN)")
		_, err := dbt.db.Exec("INSERT INTO test VALUES (1, ?, ?)",
			DataTypeText, "hello",
			DataTypeNull, nil,
		)
		if err != nil {
			dbt.Fatal(err)
		}
		dbt.mustExec("DROP TABLE IF EXISTS test")
	})
}

func TestBindingFloat64(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		expected := 42.23
		var out float64
		var rows *RowsExtended
		for _, v := range types {
			dbt.mustExec(fmt.Sprintf("CREATE TABLE test (id int, value %v)", v))
			dbt.mustExec("INSERT INTO test VALUES (1, ?)", expected)
			rows = dbt.mustQuery("SELECT value FROM test WHERE id = ?", 1)
			defer rows.Close()
			if rows.Next() {
				rows.Scan(&out)
				if expected != out {
					dbt.Errorf("%s: %g != %g", v, expected, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

// TestBindingUint64 tests uint64 binding. Should fail as unit64 is not a
// supported binding value by Go's sql package.
func TestBindingUint64(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := []string{"INTEGER"}
		expected := uint64(18446744073709551615)
		for _, v := range types {
			dbt.mustExec(fmt.Sprintf("CREATE TABLE test (id int, value %v)", v))
			if _, err := dbt.db.Exec("INSERT INTO test VALUES (1, ?)", expected); err == nil {
				dbt.Fatal("should fail as uint64 values with high bit set are not supported.")
			} else {
				logger.Infof("expected err: %v", err)
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestBindingDateTimeTimestamp(t *testing.T) {
	createDSN(PSTLocation)
	runTests(t, dsn, func(dbt *DBTest) {
		expected := time.Now()
		dbt.mustExec(
			"CREATE OR REPLACE TABLE tztest (id int, ntz timestamp_ntz, ltz timestamp_ltz, dt date, tm time)")
		stmt, err := dbt.db.Prepare("INSERT INTO tztest(id,ntz,ltz,dt,tm) VALUES(1,?,?,?,?)")
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
			rows.Scan(&ntz, &vltz, &dt, &tm)
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
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE bintest (id int, b binary, c binary)")
		var b = []byte{0x01, 0x02, 0x03}
		dbt.mustExec("INSERT INTO bintest(id,b,c) VALUES(1, ?, ?)", DataTypeBinary, b, DataTypeBinary, b)
		rows := dbt.mustQuery("SELECT b, c FROM bintest WHERE id=?", 1)
		defer rows.Close()
		if rows.Next() {
			var rb []byte
			var rc []byte
			if err := rows.Scan(&rb, &rc); err != nil {
				dbt.Errorf("failed to scan data. err: %v", err)
			}
			if !bytes.Equal(b, rb) {
				dbt.Errorf("failed to match data. expected: %v, got: %v", b, rb)
			}
			if !bytes.Equal(b, rc) {
				dbt.Errorf("failed to match data. expected: %v, got: %v", b, rc)
			}
		} else {
			dbt.Errorf("no data")
		}
		dbt.mustExec("DROP TABLE bintest")
	})
}

func TestBindingTimestampTZ(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		expected := time.Now()
		dbt.mustExec("CREATE OR REPLACE TABLE tztest (id int, tz timestamp_tz)")
		stmt, err := dbt.db.Prepare("INSERT INTO tztest(id,tz) VALUES(1, ?)")
		if err != nil {
			dbt.Fatal(err.Error())
		}
		defer stmt.Close()
		if _, err = stmt.Exec(DataTypeTimestampTz, expected); err != nil {
			dbt.Fatal(err)
		}
		rows := dbt.mustQuery("SELECT tz FROM tztest WHERE id=?", 1)
		defer rows.Close()
		var v time.Time
		if rows.Next() {
			rows.Scan(&v)
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

func TestBindingInterface(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(
			WithHigherPrecision(context.Background()), selectVariousTypes)
		defer rows.Close()
		if !rows.Next() {
			dbt.Error("failed to query")
		}
		var v1, v2, v3, v4, v5, v6 interface{}
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
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery(selectVariousTypes)
		defer rows.Close()
		if !rows.Next() {
			dbt.Error("failed to query")
		}
		var v1, v2, v3, v4, v5, v6 interface{}
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
		if s, ok := v4.(string); !ok {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v4)
		} else if d, err := strconv.ParseFloat(s, 64); err != nil {
			dbt.Errorf("failed to convert to float. value: %v, err: %v", v1, err)
		} else if d != 4.2 {
			dbt.Errorf("failed to fetch. expected: 4.2, value: %v", v1)
		}
	})
}

func TestBulkArrayBindingInterfaceNil(t *testing.T) {
	nilArray := make([]interface{}, 1)

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(createTableSQLBulkArray)
		defer dbt.mustExec(deleteTableSQLBulkArray)

		dbt.mustExec(insertSQLBulkArray, Array(&nilArray), Array(&nilArray),
			Array(&nilArray), Array(&nilArray), Array(&nilArray))
		rows := dbt.mustQuery(selectAllSQLBulkArray)
		defer rows.Close()

		var v0 sql.NullInt32
		var v1 sql.NullFloat64
		var v2 sql.NullBool
		var v3 sql.NullString
		var v4 []byte

		cnt := 0
		for i := 0; rows.Next(); i++ {
			if err := rows.Scan(&v0, &v1, &v2, &v3, &v4); err != nil {
				t.Fatal(err)
			}
			if v0.Valid {
				t.Fatalf("failed to fetch the INTEGER column V0. expected %v, got: %v", nilArray[i], v0)
			}
			if v1.Valid {
				t.Fatalf("failed to fetch the FLOAT column. expected %v, got: %v", nilArray[i], v1)
			}
			if v2.Valid {
				t.Fatalf("failed to fetch the BOOLEAN column. expected %v, got: %v", nilArray[i], v2)
			}
			if v3.Valid {
				t.Fatalf("failed to fetch the STRING column. expected %v, got: %v", nilArray[i], v3)
			}
			if v4 != nil {
				t.Fatalf("failed to fetch the BINARY column. expected %v, got: %v", nilArray[i], v4)
			}
			cnt++
		}
		if cnt != len(nilArray) {
			t.Fatal("failed to query")
		}
	})
}

func TestBulkArrayBindingInterface(t *testing.T) {
	intArray := make([]interface{}, 3)
	intArray[0] = int32(100)
	intArray[1] = int32(200)

	fltArray := make([]interface{}, 3)
	fltArray[0] = float64(0.1)
	fltArray[2] = float64(5.678)

	boolArray := make([]interface{}, 3)
	boolArray[1] = false
	boolArray[2] = true

	strArray := make([]interface{}, 3)
	strArray[2] = "test3"

	byteArray := make([]interface{}, 3)
	byteArray[0] = []byte{0x01, 0x02, 0x03}
	byteArray[2] = []byte{0x07, 0x08, 0x09}

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(createTableSQLBulkArray)
		defer dbt.mustExec(deleteTableSQLBulkArray)

		dbt.mustExec(insertSQLBulkArray, Array(&intArray), Array(&fltArray),
			Array(&boolArray), Array(&strArray), Array(&byteArray))
		rows := dbt.mustQuery(selectAllSQLBulkArray)
		defer rows.Close()

		var v0 sql.NullInt32
		var v1 sql.NullFloat64
		var v2 sql.NullBool
		var v3 sql.NullString
		var v4 []byte

		cnt := 0
		for i := 0; rows.Next(); i++ {
			if err := rows.Scan(&v0, &v1, &v2, &v3, &v4); err != nil {
				t.Fatal(err)
			}
			if v0.Valid {
				if v0.Int32 != intArray[i] {
					t.Fatalf("failed to fetch. expected %v, got: %v", intArray[i], v0.Int32)
				}
			} else if intArray[i] != nil {
				t.Fatalf("failed to fetch. expected %v, got: %v", intArray[i], v0)
			}
			if v1.Valid {
				if v1.Float64 != fltArray[i] {
					t.Fatalf("failed to fetch. expected %v, got: %v", fltArray[i], v1.Float64)
				}
			} else if fltArray[i] != nil {
				t.Fatalf("failed to fetch. expected %v, got: %v", fltArray[i], v1)
			}
			if v2.Valid {
				if v2.Bool != boolArray[i] {
					t.Fatalf("failed to fetch. expected %v, got: %v", boolArray[i], v2.Bool)
				}
			} else if boolArray[i] != nil {
				t.Fatalf("failed to fetch. expected %v, got: %v", boolArray[i], v2)
			}
			if v3.Valid {
				if v3.String != strArray[i] {
					t.Fatalf("failed to fetch. expected %v, got: %v", strArray[i], v3.String)
				}
			} else if strArray[i] != nil {
				t.Fatalf("failed to fetch. expected %v, got: %v", strArray[i], v3)
			}
			if byteArray[i] != nil {
				if !bytes.Equal(v4, byteArray[i].([]byte)) {
					t.Fatalf("failed to fetch. expected %v, got: %v", byteArray[i], v4)
				}
			} else if v4 != nil {
				t.Fatalf("failed to fetch. expected %v, got: %v", byteArray[i], v4)
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
	ntzArray := make([]interface{}, 3)
	ntzArray[0] = now
	ntzArray[1] = now.Add(1)

	ltzArray := make([]interface{}, 3)
	ltzArray[1] = now.Add(2).In(loc)
	ltzArray[2] = now.Add(3).In(loc)

	tzArray := make([]interface{}, 3)
	tzArray[0] = tz.Add(4).In(loc)
	tzArray[2] = tz.Add(5).In(loc)

	dtArray := make([]interface{}, 3)
	dtArray[0] = tz.Add(6).In(loc)
	dtArray[1] = now.Add(7).In(loc)

	tmArray := make([]interface{}, 3)
	tmArray[1] = now.Add(8).In(loc)
	tmArray[2] = now.Add(9).In(loc)

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(createTableSQLBulkArrayDateTimeTimestamp)
		defer dbt.mustExec(deleteTableSQLBulkArrayDateTimeTimestamp)

		_, err := dbt.db.Exec(insertSQLBulkArrayDateTimeTimestamp,
			Array(&ntzArray, TimestampNTZType), Array(&ltzArray, TimestampLTZType),
			Array(&tzArray, TimestampTZType), Array(&dtArray, DateType),
			Array(&tmArray, TimeType))
		if err == nil {
			t.Fatal("Date, time and timestamp are not supported in array binding using []interface{}")
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

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(createTableSQL)
		defer dbt.mustExec(deleteTableSQL)
		if bulk {
			if _, err := dbt.db.Exec("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); err != nil {
				t.Error(err)
			}
		}

		dbt.mustExec(insertSQL, Array(&intArray), Array(&fltArray),
			Array(&boolArray), Array(&strArray), Array(&byteArray),
			Array(&ntzArray, TimestampNTZType), Array(&ltzArray, TimestampLTZType),
			Array(&tzArray, TimestampTZType), Array(&dtArray, DateType),
			Array(&tmArray, TimeType))
		rows := dbt.mustQuery(selectAllSQL)
		defer rows.Close()

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
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(fmt.Sprintf("create or replace table %v (c1 integer, c2 string)", dbname))
		numRows := 100000
		intArr := make([]int, numRows)
		strArr := make([]string, numRows)
		for i := 0; i < numRows; i++ {
			intArr[i] = i
			strArr[i] = "test" + strconv.Itoa(i)
		}
		dbt.mustExec(fmt.Sprintf("insert into %v values (?, ?)", dbname), Array(&intArr), Array(&strArr))
		rows := dbt.mustQuery("select * from " + dbname)
		cnt := 0
		var i int
		var s string
		for rows.Next() {
			if err := rows.Scan(&i, &s); err != nil {
				t.Fatal(err)
			}
			if i != cnt {
				t.Errorf("expected: %v, got: %v", cnt, i)
			}
			if exp := "test" + strconv.Itoa(cnt); s != exp {
				t.Errorf("expected: %v, got: %v", exp, s)
			}
			cnt++
		}
		if cnt != numRows {
			t.Fatalf("expected %v rows, got %v", numRows, cnt)
		}
	})
}

func TestBulkArrayMultiPartBinding(t *testing.T) {
	rowCount := 1000000 // large enough to be partitioned into multiple files
	rand.Seed(time.Now().UnixNano())
	randomIter := rand.Intn(3) + 2
	randomStrings := make([]string, rowCount)
	str := randomString(30)
	for i := 0; i < rowCount; i++ {
		randomStrings[i] = str
	}
	tempTableName := fmt.Sprintf("test_table_%v", randomString(5))
	ctx := context.Background()

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(fmt.Sprintf("CREATE TABLE %s (C VARCHAR(64) NOT NULL)", tempTableName))
		defer dbt.mustExec("drop table " + tempTableName)

		for i := 0; i < randomIter; i++ {
			dbt.mustExecContext(ctx,
				fmt.Sprintf("INSERT INTO %s VALUES (?)", tempTableName),
				Array(&randomStrings))
			rows := dbt.mustQuery("select count(*) from " + tempTableName)
			if rows.Next() {
				var count int
				if err := rows.Scan(&count); err != nil {
					t.Error(err)
				}
			}
		}

		rows := dbt.mustQuery("select count(*) from " + tempTableName)
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
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("create or replace table binding_test (c1 integer)")
		startNum := 1000000
		endNum := 3000000
		numRows := endNum - startNum
		intArr := make([]int, numRows)
		for i := startNum; i < endNum; i++ {
			intArr[i-startNum] = i
		}
		_, err := dbt.db.Exec("insert into binding_test values (?)", Array(&intArr))
		if err != nil {
			t.Errorf("Should have succeeded to insert. err: %v", err)
		}

		rows := dbt.mustQuery("select * from binding_test order by c1")
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
			t.Fatalf("expected %v rows, got %v", numRows, (cnt - startNum))
		}
		dbt.mustExec("DROP TABLE binding_test")
	})
}

func TestBulkArrayMultiPartBindingWithNull(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("create or replace table binding_test (c1 integer, c2 string)")
		startNum := 1000000
		endNum := 2000000
		numRows := endNum - startNum

		// Define the integer and string arrays
		intArr := make([]interface{}, numRows)
		stringArr := make([]interface{}, numRows)
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

		_, err := dbt.db.Exec("insert into binding_test values (?, ?)", Array(&intArr), Array(&stringArr))
		if err != nil {
			t.Errorf("Should have succeeded to insert. err: %v", err)
		}

		rows := dbt.mustQuery("select * from binding_test order by c1,c2")
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
			t.Fatalf("expected %v rows, got %v", numRows, (cnt - startNum))
		}
		dbt.mustExec("DROP TABLE binding_test")
	})
}
