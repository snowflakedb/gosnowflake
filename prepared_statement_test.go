// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"database/sql"
	"testing"
)

// TestPreparedStatement creates a basic prepared statement, inserting values
// after the statement has been prepared
func TestPreparedStatement(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	db.Exec("create or replace table test(c1 int, c2 string)")
	intArray := []int{1, 2, 3}
	strArray := []string{"test1", "test2", "test3"}

	stmt, err := db.Prepare("insert into test values (?, ?)")
	if err != nil {
		t.Error(err)
	}
	defer stmt.Close()
	stmt.Exec(intArray, strArray)

	cnt := 0
	rows, _ := db.Query("select * from test order by 1")
	defer rows.Close()
	var v1 int
	var v2 string
	for rows.Next() {
		err := rows.Scan(&v1, &v2)
		if err != nil {
			t.Fatal(err)
		}
		if intArray[cnt] != v1 {
			t.Errorf("failed to scan. expected: %v, got %v", intArray[cnt], v1)
		}
		if strArray[cnt] != v2 {
			t.Errorf("failed to scan. expected: %v, got %v", strArray[cnt], v2)
		}
		cnt++
	}
	if cnt != len(intArray) {
		t.Errorf("number of rows didn't match. expected: %v, got: %v", len(intArray), cnt)
	}
}
