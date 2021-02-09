// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

func TestAsyncMode(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx, _ := WithAsyncMode(context.Background())
	numrows := 100000
	rows, _ := db.QueryContext(ctx, fmt.Sprintf("SELECT SEQ8(), RANDSTR(1000, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT=>%v))", numrows))
	defer rows.Close()

	cnt := 0
	var idx int
	var v string
	// Next() will block and wait until results are available
	for rows.Next() {
		err := rows.Scan(&idx, &v)
		if err != nil {
			t.Fatal(err)
		}
		cnt++
	}
	logger.Infof("NextResultSet: %v", rows.NextResultSet())

	if cnt != numrows {
		t.Errorf("number of rows didn't match. expected: %v, got: %v", numrows, cnt)
	}

	_, err = db.Exec("create or replace table test_async_exec (value boolean)")
	if err != nil {
		t.Error(err)
	}
	res, _ := db.ExecContext(ctx, "insert into test_async_exec values (true)")
	// RowsAffected() will block and wait until results are available
	count, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 affected row, got %d", count)
	}
}

func TestAsyncQueryFail(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx, _ := WithAsyncMode(context.Background())
	rows, err := db.QueryContext(ctx, "selectt 1")
	if err != nil {
		t.Fatal("asynchronous query should always return nil error")
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("should have no rows available")
	} else {
		if err = rows.Err(); err == nil {
			t.Fatal("should return a syntax error")
		}
	}
}

func TestMultipleAsyncQueries(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx, _ := WithAsyncMode(context.Background())
	s1 := "foo"
	s2 := "bar"
	rows1, _ := db.QueryContext(ctx, fmt.Sprintf("select distinct '%v' from table (generator(timelimit=>%v))", s1, 30))
	defer rows1.Close()
	rows2, _ := db.QueryContext(ctx, fmt.Sprintf("select distinct '%v' from table (generator(timelimit=>%v))", s2, 10))
	defer rows2.Close()

	ch1 := make(chan string)
	ch2 := make(chan string)
	go retrieveRows(rows1, ch1)
	go retrieveRows(rows2, ch2)
	select {
	case res := <-ch1:
		t.Fatalf("value %v should not have been called earlier.", res)
	case res := <-ch2:
		if res != s2 {
			t.Fatalf("query failed. expected: %v, got: %v", s2, res)
		}
	}
}

func retrieveRows(rows *sql.Rows, ch chan string) {
	var s string
	for rows.Next() {
		if err := rows.Scan(&s); err != nil {
			ch <- err.Error()
			close(ch)
			return
		}
	}
	ch <- s
	close(ch)
}
