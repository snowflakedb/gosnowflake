// Copyright (c) 2020-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/url"
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
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Error(err)
	}

	if err = conn.Raw(func(x interface{}) error {
		rows, err := x.(driver.QueryerContext).QueryContext(ctx, "select 1", nil)
		if err != nil {
			return err
		}
		defer rows.Close()

		if _, err = x.(driver.QueryerContext).QueryContext(ctx, "selectt 1", nil); err == nil {
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
	}); err != nil {
		t.Fatalf("failed to prepare statement. err: %v", err)
	}
}

func TestEmitQueryID(t *testing.T) {
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
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(ctx, fmt.Sprintf(selectRandomGenerator, numrows))
		defer rows.Close()

		for rows.Next() {
			if err := rows.Scan(&idx, &v); err != nil {
				t.Fatal(err)
			}
			cnt++
		}
		logger.Infof("NextResultSet: %v", rows.NextResultSet())
	})

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

	if _, err := db.Exec(`create or replace table test_fetch_result(c1 number,
		c2 string) as select 10, 'z'`); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Error(err)
	}
	if err = conn.Raw(func(x interface{}) error {
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
	}); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	if _, err := db.Exec("drop table if exists test_fetch_result"); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
}

func TestWithDescribeOnly(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx := WithDescribeOnly(context.Background())
		rows := dbt.mustQueryContext(ctx, selectVariousTypes)
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

func TestCallStatement(t *testing.T) {
	t.Skip("USE_STATEMENT_TYPE_CALL_FOR_STORED_PROC_CALLS is not yet supported")

	runTests(t, dsn, func(dbt *DBTest) {
		in1 := float64(1)
		in2 := string("[2,3]")
		expected := "1 \"[2,3]\" [2,3]"
		var out string

		dbt.db.Exec("ALTER SESSION SET USE_STATEMENT_TYPE_CALL_FOR_STORED_PROC_CALLS = true")

		dbt.mustExec("create or replace procedure " +
			"TEST_SP_CALL_STMT_ENABLED(in1 float, in2 variant) " +
			"returns string language javascript as $$ " +
			"let res = snowflake.execute({sqlText: 'select ? c1, ? c2', binds:[IN1, JSON.stringify(IN2)]}); " +
			"res.next(); " +
			"return res.getColumnValueAsString(1) + ' ' + res.getColumnValueAsString(2) + ' ' + IN2; " +
			"$$;")

		stmt, err := dbt.db.Prepare("call TEST_SP_CALL_STMT_ENABLED(?, to_variant(?))")
		if err != nil {
			dbt.Errorf("failed to prepare query: %v", err)
		}
		defer stmt.Close()
		err = stmt.QueryRow(in1, in2).Scan(&out)
		if err != nil {
			dbt.Errorf("failed to scan: %v", err)
		}

		if expected != out {
			dbt.Errorf("expected: %s, got: %s", expected, out)
		}

		dbt.mustExec("drop procedure if exists TEST_SP_CALL_STMT_ENABLED(float, variant)")
	})
}

func TestStmtExec(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec(`create or replace table test_table(col1 int, col2 int)`); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Error(err)
	}
	if err = conn.Raw(func(x interface{}) error {
		stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, "insert into test_table values (1, 2)")
		if err != nil {
			t.Error(err)
		}
		_, err = stmt.(*snowflakeStmt).Exec(nil)
		if err != nil {
			t.Error(err)
		}
		_, err = stmt.(*snowflakeStmt).Query(nil)
		if err != nil {
			t.Error(err)
		}
		return nil
	}); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	if _, err := db.Exec("drop table if exists test_table"); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
}

func getStatusSuccessButInvalidJSONfunc(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func TestUnitCheckQueryStatus(t *testing.T) {
	sc := getDefaultSnowflakeConn()
	ctx := context.Background()
	qid := NewUUID()

	sr := &snowflakeRestful{
		FuncGet:       getStatusSuccessButInvalidJSONfunc,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	sc.rest = sr
	_, err := sc.checkQueryStatus(ctx, qid.String())
	if err == nil {
		t.Fatal("invalid json. should have failed")
	}
	sc.rest.FuncGet = funcGetQueryRespFail
	_, err = sc.checkQueryStatus(ctx, qid.String())
	if err == nil {
		t.Fatal("should have failed")
	}

	sc.rest.FuncGet = funcGetQueryRespError
	_, err = sc.checkQueryStatus(ctx, qid.String())
	if err == nil {
		t.Fatal("should have failed")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("should be snowflake error. err: %v", err)
	}
	if driverErr.Number != ErrQueryStatus {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrQueryStatus, driverErr.Number)
	}
}
