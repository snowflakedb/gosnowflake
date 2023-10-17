// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"testing"
)

// TestPreparedStatement creates a basic prepared statement, inserting values
// after the statement has been prepared
func TestPreparedStatement(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("create or replace table test_prep_statement(c1 INTEGER, c2 FLOAT, c3 BOOLEAN, c4 STRING)")
		defer dbt.mustExec(deleteTableSQL)

		intArray := []int{1, 2, 3}
		fltArray := []float64{0.1, 2.34, 5.678}
		boolArray := []bool{true, false, true}
		strArray := []string{"test1", "test2", "test3"}
		stmt := dbt.mustPrepare("insert into TEST_PREP_STATEMENT values(?, ?, ?, ?)")
		if _, err := stmt.Exec(Array(&intArray), Array(&fltArray), Array(&boolArray), Array(&strArray)); err != nil {
			t.Fatal(err)
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
