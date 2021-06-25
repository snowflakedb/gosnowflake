// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"fmt"
	"testing"
)

func TestAsyncMode(t *testing.T) {
	ctx := WithAsyncMode(context.Background())
	numrows := 100000
	cnt := 0
	var idx int
	var v string

	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(ctx, fmt.Sprintf(selectRandomGenerator, numrows))
		defer rows.Close()

		// Next() will block and wait until results are available
		for rows.Next() {
			if err := rows.Scan(&idx, &v); err != nil {
				t.Fatal(err)
			}
			cnt++
		}
		logger.Infof("NextResultSet: %v", rows.NextResultSet())

		if cnt != numrows {
			t.Errorf("number of rows didn't match. expected: %v, got: %v", numrows, cnt)
		}

		dbt.mustExec("create or replace table test_async_exec (value boolean)")
		res := dbt.mustExecContext(ctx, "insert into test_async_exec values (true)")
		count, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("res.RowsAffected() returned error: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected 1 affected row, got %d", count)
		}
	})
}

func TestAsyncQueryFail(t *testing.T) {
	ctx := WithAsyncMode(context.Background())
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(ctx, "selectt 1")
		defer rows.Close()

		if rows.Next() {
			t.Fatal("should have no rows available")
		} else {
			if err := rows.Err(); err == nil {
				t.Fatal("should return a syntax error")
			}
		}
	})
}

func TestMultipleAsyncQueries(t *testing.T) {
	ctx := WithAsyncMode(context.Background())
	s1 := "foo"
	s2 := "bar"
	ch1 := make(chan string)
	ch2 := make(chan string)

	runTests(t, dsn, func(dbt *DBTest) {
		rows1 := dbt.mustQueryContext(ctx, fmt.Sprintf("select distinct '%v' from table (generator(timelimit=>%v))", s1, 30))
		defer rows1.Close()
		rows2 := dbt.mustQueryContext(ctx, fmt.Sprintf("select distinct '%v' from table (generator(timelimit=>%v))", s2, 10))
		defer rows2.Close()

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
	})
}

func retrieveRows(rows *RowsExtended, ch chan string) {
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
