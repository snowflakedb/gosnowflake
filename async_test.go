// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"fmt"
	"testing"
	"time"
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

func TestAsyncModeCancel(t *testing.T) {
	withCancelCtx, cancel := context.WithCancel(context.Background())
	ctx := WithAsyncMode(withCancelCtx)
	numrows := 100000

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustQueryContext(ctx, fmt.Sprintf(selectRandomGenerator, numrows))
		cancel()
	})
}

const (
	//selectTimelineGenerator = "SELECT COUNT(*) FROM TABLE(GENERATOR(TIMELIMIT=>%v))"
	selectTimelineGenerator = "SELECT SYSTEM$WAIT(%v, 'SECONDS')"
)

func TestAsyncModeNoFetch(t *testing.T) {
	ctx := WithAsyncMode(WithAsyncModeNoFetch(context.Background()))
	// the default behavior of the async wait is to wait for 45s. We want to make sure we wait until the query actually
	// completes, so we make the test take longer than 45s
	secondsToRun := 50

	runTests(t, dsn, func(dbt *DBTest) {
		start := time.Now()
		rows := dbt.mustQueryContext(ctx, fmt.Sprintf(selectTimelineGenerator, secondsToRun))
		defer rows.Close()

		// Next() will block and wait until results are available
		if rows.Next() == true {
			t.Fatalf("next should have returned no rows")
		}
		columns, err := rows.Columns()
		if columns != nil {
			t.Fatalf("there should be no column array returned")
		}
		if err == nil {
			t.Fatalf("we should have an error thrown")
		}
		if rows.Scan(nil) == nil {
			t.Fatalf("we should have an error thrown")
		}
		if (time.Second * time.Duration(secondsToRun)) > time.Since(start) {
			t.Fatalf("tset should should have run for %d seconds", secondsToRun)
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

func TestMultipleAsyncSuccessAndFailedQueries(t *testing.T) {
	ctx := WithAsyncMode(context.Background())
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	s1 := "foo"
	s2 := "bar"
	ch1 := make(chan string)
	ch2 := make(chan string)

	runTests(t, dsn, func(dbt *DBTest) {
		rows1 := dbt.mustQueryContext(ctx, fmt.Sprintf("select distinct '%s' from table (generator(timelimit=>3))", s1))
		defer rows1.Close()

		rows2 := dbt.mustQueryContext(ctx, fmt.Sprintf("select distinct '%s' from table (generator(timelimit=>7))", s2))
		defer rows2.Close()

		go retrieveRows(rows1, ch1)
		go retrieveRows(rows2, ch2)

		res1 := <-ch1
		if res1 != s1 {
			t.Fatalf("query failed. expected: %v, got: %v", s1, res1)
		}

		// wait until rows2 is done
		<-ch2
		driverErr, ok := rows2.Err().(*SnowflakeError)
		if !ok || driverErr == nil || driverErr.Number != ErrAsync {
			t.Fatalf("Snowflake ErrAsync expected. got: %T, %v", rows2.Err(), rows2.Err())
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
