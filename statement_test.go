// Copyright (c) 2020-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"
)

func openDB(t *testing.T) *sql.DB {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	return db
}

func TestGetQueryID(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	ctx := context.TODO()
	conn, _ := db.Conn(ctx)

	err := conn.Raw(func(x interface{}) error {
		rows, err := x.(driver.QueryerContext).QueryContext(ctx, "select 1", nil)
		if err != nil {
			return err
		}
		defer rows.Close()

		_, err = x.(driver.ConnPrepareContext).PrepareContext(ctx, "selectt 1")
		if err == nil {
			t.Fatal("should have failed to execute query")
		}
		if driverErr, ok := err.(*SnowflakeError); ok {
			if driverErr.Number != 1003 {
				t.Fatalf("incorrect error code. expected: 1003, got: %v", driverErr.Number)
			}
			if driverErr.QueryID == "" {
				t.Fatal("should have an associated query ID")
			}
		} else {
			t.Fatal("should have been able to cast to Snowflake Error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to prepare statement. err: %v", err)
	}
}

func TestEmitQueryID(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	queryIDChan := make(chan string, 1)
	numrows := 100000
	ctx := WithAsyncMode(context.Background())
	ctx = WithQueryIDChan(ctx, queryIDChan)

	goRoutineChan := make(chan string)
	go func(grCh chan string, qIDch chan string) {
		queryID := <-queryIDChan
		grCh <- queryID
	}(goRoutineChan, queryIDChan)

	cnt := 0
	var idx int
	var v string
	rows, _ := db.QueryContext(ctx, fmt.Sprintf("SELECT SEQ8(), RANDSTR(1000, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT=>%v))", numrows))
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&idx, &v)
		if err != nil {
			t.Fatal(err)
		}
		cnt++
	}
	logger.Infof("NextResultSet: %v", rows.NextResultSet())

	queryID := <-goRoutineChan
	if queryID == "" {
		t.Fatal("expected a nonempty query ID")
	}
	if cnt != numrows {
		t.Errorf("number of rows didn't match. expected: %v, got: %v", numrows, cnt)
	}
}

// End-to-end test to fetch result with queryID
func TestE2EFetchResultByID(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("create or replace table test_fetch_result(" +
		"c1 number, c2 string) as select 10, 'z'"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	err := conn.Raw(func(x interface{}) error {
		stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, "select * from test_fetch_result")
		if err != nil {
			return err
		}

		rows1, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
		if err != nil {
			return err
		}
		qid := rows1.(SnowflakeResult).GetQueryID()

		newCtx := context.WithValue(context.Background(), fetchResultByID, qid)
		rows2, err := db.QueryContext(newCtx, "")
		if err != nil {
			t.Fatalf("Fetch Query Result by ID failed: %v", err)
		}
		var c1 sql.NullInt64
		var c2 sql.NullString
		for rows2.Next() {
			err = rows2.Scan(&c1, &c2)
		}
		if c1.Int64 != 10 || c2.String != "z" {
			t.Fatalf("Query result is not expected: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	_, err = db.Exec("drop table if exists test_fetch_result")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
}

func TestWithDescribeOnly(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx := WithDescribeOnly(context.Background())
		rows := dbt.mustQueryContext(
			ctx,
			"SELECT 1.0::NUMBER(30,2) as C1, 2::NUMBER(38,0) AS C2, 't3' AS C3, 4.2::DOUBLE AS C4, 'abcd'::BINARY AS C5, true AS C6")
		cols, err := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		types, err := rows.ColumnTypes()
		if err != nil {
			t.Error(err)
		}
		for i, col := range cols {
			if types[i].Name() != col {
				t.Fatalf("column name mismatch. expected: %v, got: %v", col, types[i].Name())
			}
		}
		if rows.Next() {
			t.Fatal("there should not be any rows in describe only mode")
		}
	})
}

func TestWithMonitoring(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	err := conn.Raw(func(x interface{}) error {
		ctx = WithMonitoring(ctx)
		stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, "SELECT current_timestamp, system$wait(100, 'MILLISECONDS')")
		if err != nil {
			return err
		}

		rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
		if err != nil {
			return err
		}
		qid := rows.(SnowflakeResult).GetQueryID()
		mData := rows.(SnowflakeResult).GetMonitoring()

		dest := make([]driver.Value, 2)
		err = rows.Next(dest)
		if err != nil && err != io.EOF {
			t.Error(err)
		}

		// Get millisecond-level resolution to match the timestamps in the metadata
		ts := dest[0].(time.Time).UnixNano() / 1000000
		startDiff := ts - mData.StartTime
		// TODO: Is this fudge factor (in case current_timestamp gets executed
		// slightly after StartTime is recorded) necessary? If current_timestamp is
		// definitionally set to StartTime, we can just test for equality.
		if startDiff > 5 {
			t.Fatalf("Too large a skew between current_timestamp (%d) and StartTime (%d)", ts, mData.StartTime)
		}

		if _, ok := mData.Stats["compilationTime"]; !ok {
			t.Fatalf("Expected stats to include compilationTime")
		}

		if mData.TotalDuration < 100 {
			t.Fatalf("Duration (%d) wasn't greater than sleep time (100)", mData.TotalDuration)
		}
		if mData.TotalDuration != mData.EndTime-mData.StartTime {
			t.Fatalf("Expected TotalDuration %d - %d = %d but got %d", mData.EndTime, mData.StartTime, mData.EndTime-mData.StartTime, mData.TotalDuration)
		}

		type stringTest struct {
			key   string
			value string
		}

		cases := [4]stringTest{
			{"ID", qid},
			{"Status", "SUCCESS"},
			{"State", "SUCCEEDED"},
			{"WarehouseName", warehouse},
			// TODO(greg): add WithQueryTag here once that PR merges
		}

		m := reflect.ValueOf(mData).Elem()
		for _, c := range cases {
			v := m.FieldByName(c.key)
			if v.String() != c.value {
				t.Fatalf("Expected %s %s but got %s", c.key, c.value, v)
			}
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}
}
