// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"
	"time"
)

const (
	createTableSQL = "create or replace table test_prep_statement(c1 INTEGER, " +
		"c2 FLOAT, c3 BOOLEAN, c4 STRING)"
	deleteTableSQL   = "drop table if exists TEST_PREP_STATEMENT"
	insertSQL        = "insert into TEST_PREP_STATEMENT values(?, ?, ?, ?)"
	selectAllSQL     = "select * from TEST_PREP_STATEMENT ORDER BY 1"
	updateSQL        = "update TEST_PREP_STATEMENT set C4 = 'newString' where C1 = ?"
	deleteSQL        = "delete from TEST_PREP_STATEMENT where C1 = ?"
	selectSQL        = "select * from TEST_PREP_STATEMENT where C1 = ?"
	enableCacheReuse = "alter session set USE_CACHED_RESULT=true"
	tableFuncSQL     = "select 1 from table(generator(rowCount => ?))"
)

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
			_, err := dbt.db.Exec("INSERT INTO test VALUES (1, ?)", expected)
			if err == nil {
				dbt.Fatal("should fail as uint64 values with high bit set are not supported.")
			} else {
				logger.Infof("expected err: %v", err)
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestBindingDateTimeTimestamp(t *testing.T) {
	createDSN("America/Los_Angeles")
	runTests(t, dsn, func(dbt *DBTest) {
		expected := time.Now()
		dbt.mustExec(
			"CREATE OR REPLACE TABLE tztest (id int, ntz timestamp_ntz, ltz timestamp_ltz, dt date, tm time)")
		stmt, err := dbt.db.Prepare("INSERT INTO tztest(id,ntz,ltz,dt,tm) VALUES(1,?,?,?,?)")
		if err != nil {
			dbt.Fatal(err.Error())
		}
		defer stmt.Close()
		_, err = stmt.Exec(
			DataTypeTimestampNtz, expected,
			DataTypeTimestampLtz, expected,
			DataTypeDate, expected,
			DataTypeTime, expected)
		if err != nil {
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
	runTests(t, dsn, func(dbt *DBTest) {
		expected := time.Now()
		dbt.mustExec("CREATE OR REPLACE TABLE tztest (id int, tz timestamp_tz)")
		stmt, err := dbt.db.Prepare("INSERT INTO tztest(id,tz) VALUES(1, ?)")
		if err != nil {
			dbt.Fatal(err.Error())
		}
		defer stmt.Close()
		_, err = stmt.Exec(DataTypeTimestampTz, expected)
		if err != nil {
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
			// fmt.Printf("returned value: %v, %v, %v\n", v, v.UnixNano(), expected.UnixNano())
		} else {
			dbt.Error("no data")
		}
		dbt.mustExec("DROP TABLE tztest")
	})
}

func TestBindingInterface(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		var err error
		rows := dbt.mustQuery(
			"SELECT 1.0::NUMBER(30,2) as C1, 2::NUMBER(38,0) AS C2, 't3' AS C3, 4.2::DOUBLE AS C4, 'abcd'::BINARY AS C5, true AS C6")
		defer rows.Close()
		if !rows.Next() {
			dbt.Error("failed to query")
		}
		var v1, v2, v3, v4, v5, v6 interface{}
		err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6)
		if err != nil {
			dbt.Errorf("failed to scan: %#v", err)
		}
		var s string
		var ok bool
		s, ok = v1.(string)
		if !ok || s != "1.00" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v1)
		}
		s, ok = v2.(string)
		if !ok || s != "2" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v2)
		}
		s, ok = v3.(string)
		if !ok || s != "t3" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v3)
		}
		s, ok = v4.(string)
		if !ok || s != "4.2" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v4)
		}
	})
}

func TestBindingArrowInterface(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(enableArrow)
		var err error
		rows := dbt.mustQuery(
			"SELECT 1.0::NUMBER(30,2) as C1, 2::NUMBER(38,0) AS C2, 't3' AS C3, 4.2::DOUBLE AS C4, 'abcd'::BINARY AS C5, true AS C6")
		defer rows.Close()
		if !rows.Next() {
			dbt.Error("failed to query")
		}
		var v1, v2, v3, v4, v5, v6 interface{}
		err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6)
		if err != nil {
			dbt.Errorf("failed to scan: %#v", err)
		}
		var s1 *big.Float
		var s2 int64
		var s3 string
		var s4 float64
		var ok bool
		s1, ok = v1.(*big.Float)
		if !ok || s1.Cmp(big.NewFloat(1.0)) != 0 {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v1)
		}
		s2, ok = v2.(int64)
		if !ok || s2 != 2 {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v2)
		}
		s3, ok = v3.(string)
		if !ok || s3 != "t3" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v3)
		}
		s4, ok = v4.(float64)
		if !ok || s4 != 4.2 {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v4)
		}
	})
}

// TestBindingArray tests basic array binding via the usage of the Array
// function that converts the passed Golang slice to a Snowflake array type
func TestBindingArray(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(createTableSQL)
		defer dbt.mustExec(deleteTableSQL)

		intArray := []int{1, 2, 3}
		fltArray := []float64{0.1, 2.34, 5.678}
		boolArray := []bool{true, false, true}
		strArray := []string{"test1", "test2", "test3"}
		dbt.mustExec(insertSQL, Array(&intArray), Array(&fltArray), Array(&boolArray), Array(&strArray))
		rows := dbt.mustQuery(selectAllSQL)
		defer rows.Close()

		var v1 int
		var v2 float64
		var v3 bool
		var v4 string
		if rows.Next() {
			err := rows.Scan(&v1, &v2, &v3, &v4)
			if err != nil {
				t.Fatal(err)
			}
			if v1 != 1 && v2 != 0.1 && v3 != true && v4 != "test1" {
				t.Fatalf("failed to fetch. expected: 1, 0.1, true, test1. got: %v, %v, %v, %v", v1, v2, v3, v4)
			}
		} else {
			t.Error("failed to query")
		}

		if rows.Next() {
			err := rows.Scan(&v1, &v2, &v3, &v4)
			if err != nil {
				t.Fatal(err)
			}
			if v1 != 2 && v2 != 2.34 && v3 != false && v4 != "test2" {
				t.Fatalf("failed to fetch. expected: 2, 2.34, false, test2. got: %v, %v, %v, %v", v1, v2, v3, v4)
			}
		} else {
			t.Error("failed to query")
		}

		if rows.Next() {
			err := rows.Scan(&v1, &v2, &v3, &v4)
			if err != nil {
				t.Fatal(err)
			}
			if v1 != 3 && v2 != 5.678 && v3 != true && v4 != "test3" {
				t.Fatalf("failed to fetch. expected: 3, test3. got: %v, %v, %v, %v", v1, v2, v3, v4)
			}
		} else {
			t.Error("failed to query")
		}
	})
}

// TestBindingBulkArray tests array binding via streaming PUT using the staging
// process
func TestBindingBulkArray(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1")
		dbt.mustExec(createTableSQL)
		defer dbt.mustExec(deleteTableSQL)

		intArray := []int{1, 2, 3}
		fltArray := []float64{0.1, 2.34, 5.678}
		boolArray := []bool{true, false, true}
		strArray := []string{"test1", "test2", "test3"}
		res := dbt.mustExec(insertSQL, Array(&intArray), Array(&fltArray), Array(&boolArray), Array(&strArray))
		count, err := res.RowsAffected()
		if err != nil {
			t.Error(err)
		}
		if count != 3 {
			t.Fatal("")
		}

		rows := dbt.mustQuery(selectAllSQL)
		defer rows.Close()

		var v1 int
		var v2 float64
		var v3 bool
		var v4 string
		if rows.Next() {
			err := rows.Scan(&v1, &v2, &v3, &v4)
			if err != nil {
				t.Fatal(err)
			}
			if v1 != 1 && v2 != 0.1 && v3 != true && v4 != "test1" {
				t.Fatalf("failed to fetch. expected: 1, 0.1, true, test1. got: %v, %v, %v, %v", v1, v2, v3, v4)
			}
		} else {
			t.Error("failed to query")
		}

		if rows.Next() {
			err := rows.Scan(&v1, &v2, &v3, &v4)
			if err != nil {
				t.Fatal(err)
			}
			if v1 != 2 && v2 != 2.34 && v3 != false && v4 != "test2" {
				t.Fatalf("failed to fetch. expected: 2, 2.34, false, test2. got: %v, %v, %v, %v", v1, v2, v3, v4)
			}
		} else {
			t.Error("failed to query")
		}

		if rows.Next() {
			err := rows.Scan(&v1, &v2, &v3, &v4)
			if err != nil {
				t.Fatal(err)
			}
			if v1 != 3 && v2 != 5.678 && v3 != true && v4 != "test3" {
				t.Fatalf("failed to fetch. expected: 3, test3. got: %v, %v, %v, %v", v1, v2, v3, v4)
			}
		} else {
			t.Error("failed to query")
		}
	})
}
