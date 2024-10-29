// Copyright (c) 2020-2023 Snowflake Computing Inc. All rights reserved.
//lint:file-ignore SA1019 Ignore deprecated methods. We should leave them as-is to keep backward compatibility.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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
		t.Fatalf("failed to open db. %v", err)
	}

	return db
}

func openConn(t *testing.T) *sql.Conn {
	var db *sql.DB
	var conn *sql.Conn
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	if conn, err = db.Conn(context.Background()); err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	return conn
}

func TestExecStmt(t *testing.T) {
	dqlQuery := "SELECT 1"
	dmlQuery := "INSERT INTO TestDDLExec VALUES (1)"
	ddlQuery := "CREATE OR REPLACE TABLE TestDDLExec (num NUMBER)"
	multiStmtQuery := "DELETE FROM TestDDLExec;\n" +
		"SELECT 1;\n" +
		"SELECT 2;"
	ctx := context.Background()
	multiStmtCtx, err := WithMultiStatement(ctx, 3)
	if err != nil {
		t.Error(err)
	}
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(ddlQuery)
		defer dbt.mustExec("DROP TABLE IF EXISTS TestDDLExec")
		testcases := []struct {
			name  string
			query string
			f     func(stmt driver.Stmt) (any, error)
		}{
			{
				name:  "dql Exec",
				query: dqlQuery,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.Exec(nil)
				},
			},
			{
				name:  "dql ExecContext",
				query: dqlQuery,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.(driver.StmtExecContext).ExecContext(ctx, nil)
				},
			},
			{
				name:  "ddl Exec",
				query: ddlQuery,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.Exec(nil)
				},
			},
			{
				name:  "ddl ExecContext",
				query: ddlQuery,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.(driver.StmtExecContext).ExecContext(ctx, nil)
				},
			},
			{
				name:  "dml Exec",
				query: dmlQuery,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.Exec(nil)
				},
			},
			{
				name:  "dml ExecContext",
				query: dmlQuery,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.(driver.StmtExecContext).ExecContext(ctx, nil)
				},
			},
			{
				name:  "multistmt ExecContext",
				query: multiStmtQuery,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.(driver.StmtExecContext).ExecContext(multiStmtCtx, nil)
				},
			},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				err := dbt.conn.Raw(func(x any) error {
					stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, tc.query)
					if err != nil {
						t.Error(err)
					}
					if stmt.(SnowflakeStmt).GetQueryID() != "" {
						t.Error("queryId should be empty before executing any query")
					}
					if _, err := tc.f(stmt); err != nil {
						t.Errorf("should have not failed to execute the query, err: %s\n", err)
					}
					if stmt.(SnowflakeStmt).GetQueryID() == "" {
						t.Error("should have set the query id")
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

func TestFailedQueryIdInSnowflakeError(t *testing.T) {
	failingQuery := "SELECTT 1"
	failingExec := "INSERT 1 INTO NON_EXISTENT_TABLE"

	runDBTest(t, func(dbt *DBTest) {
		testcases := []struct {
			name  string
			query string
			f     func(dbt *DBTest) (any, error)
		}{
			{
				name: "query",
				f: func(dbt *DBTest) (any, error) {
					return dbt.query(failingQuery)
				},
			},
			{
				name: "exec",
				f: func(dbt *DBTest) (any, error) {
					return dbt.exec(failingExec)
				},
			},
		}

		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := tc.f(dbt)
				if err == nil {
					t.Error("should have failed")
				}
				var snowflakeError *SnowflakeError
				if !errors.As(err, &snowflakeError) {
					t.Error("should be a SnowflakeError")
				}
				if snowflakeError.QueryID == "" {
					t.Error("QueryID should be set")
				}
			})
		}
	})
}

func TestSetFailedQueryId(t *testing.T) {
	ctx := context.Background()
	failingQuery := "SELECTT 1"
	failingExec := "INSERT 1 INTO NON_EXISTENT_TABLE"

	runDBTest(t, func(dbt *DBTest) {
		testcases := []struct {
			name  string
			query string
			f     func(stmt driver.Stmt) (any, error)
		}{
			{
				name:  "query",
				query: failingQuery,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.Query(nil)
				},
			},
			{
				name:  "exec",
				query: failingExec,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.Exec(nil)
				},
			},
			{
				name:  "queryContext",
				query: failingQuery,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
				},
			},
			{
				name:  "execContext",
				query: failingExec,
				f: func(stmt driver.Stmt) (any, error) {
					return stmt.(driver.StmtExecContext).ExecContext(ctx, nil)
				},
			},
		}

		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				err := dbt.conn.Raw(func(x any) error {
					stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, tc.query)
					if err != nil {
						t.Error(err)
					}
					if stmt.(SnowflakeStmt).GetQueryID() != "" {
						t.Error("queryId should be empty before executing any query")
					}
					if _, err := tc.f(stmt); err == nil {
						t.Error("should have failed to execute the query")
					}
					if stmt.(SnowflakeStmt).GetQueryID() == "" {
						t.Error("should have set the query id")
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

func TestAsyncFailQueryId(t *testing.T) {
	ctx := WithAsyncMode(context.Background())
	runDBTest(t, func(dbt *DBTest) {
		err := dbt.conn.Raw(func(x any) error {
			stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, "SELECTT 1")
			if err != nil {
				t.Error(err)
			}
			if stmt.(SnowflakeStmt).GetQueryID() != "" {
				t.Error("queryId should be empty before executing any query")
			}
			rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
			if err != nil {
				t.Error("should not fail the initial request")
			}
			if rows.(SnowflakeRows).GetStatus() != QueryStatusInProgress {
				t.Error("should be in progress")
			}
			// Wait for the query to complete
			assertNotNilE(t, rows.Next(nil))
			if rows.(SnowflakeRows).GetStatus() != QueryFailed {
				t.Error("should have failed")
			}
			if rows.(SnowflakeRows).GetQueryID() != stmt.(SnowflakeStmt).GetQueryID() {
				t.Error("last query id should be the same as rows query id")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestGetQueryID(t *testing.T) {
	ctx := context.Background()
	conn := openConn(t)
	defer conn.Close()

	if err := conn.Raw(func(x interface{}) error {
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
	runDBTest(t, func(dbt *DBTest) {
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
	runDBTest(t, func(dbt *DBTest) {
		ctx := WithDescribeOnly(context.Background())
		rows := dbt.mustQueryContext(ctx, selectVariousTypes)
		defer rows.Close()
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
	runDBTest(t, func(dbt *DBTest) {
		in1 := float64(1)
		in2 := string("[2,3]")
		expected := "1 \"[2,3]\" [2,3]"
		var out string

		dbt.mustExec("ALTER SESSION SET USE_STATEMENT_TYPE_CALL_FOR_STORED_PROC_CALLS = true")

		dbt.mustExec("create or replace procedure " +
			"TEST_SP_CALL_STMT_ENABLED(in1 float, in2 variant) " +
			"returns string language javascript as $$ " +
			"let res = snowflake.execute({sqlText: 'select ? c1, ? c2', binds:[IN1, JSON.stringify(IN2)]}); " +
			"res.next(); " +
			"return res.getColumnValueAsString(1) + ' ' + res.getColumnValueAsString(2) + ' ' + IN2; " +
			"$$;")

		stmt, err := dbt.conn.PrepareContext(context.Background(), "call TEST_SP_CALL_STMT_ENABLED(?, to_variant(?))")
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
	ctx := context.Background()
	conn := openConn(t)
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `create or replace table test_table(col1 int, col2 int)`); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	if err := conn.Raw(func(x interface{}) error {
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

	if _, err := conn.ExecContext(ctx, "drop table if exists test_table"); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
}

func TestStmtExec_Error(t *testing.T) {
	ctx := context.Background()
	conn := openConn(t)
	defer conn.Close()

	// Create a test table
	if _, err := conn.ExecContext(ctx, `create or replace table test_table(col1 int, col2 int)`); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Attempt to execute an invalid statement
	if err := conn.Raw(func(x interface{}) error {
		stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, "insert into test_table values (?, ?)")
		if err != nil {
			t.Fatalf("failed to prepare statement: %v", err)
		}

		// Intentionally passing a string instead of an integer to cause an error
		_, err = stmt.(*snowflakeStmt).Exec([]driver.Value{"invalid_data", 2})
		if err == nil {
			t.Errorf("expected an error, but got none")
		}

		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Drop the test table
	if _, err := conn.ExecContext(ctx, "drop table if exists test_table"); err != nil {
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

func TestStatementQueryIdForQueries(t *testing.T) {
	ctx := context.Background()
	conn := openConn(t)
	defer conn.Close()

	testcases := []struct {
		name string
		f    func(stmt driver.Stmt) (driver.Rows, error)
	}{
		{
			"query",
			func(stmt driver.Stmt) (driver.Rows, error) {
				return stmt.Query(nil)
			},
		},
		{
			"queryContext",
			func(stmt driver.Stmt) (driver.Rows, error) {
				return stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := conn.Raw(func(x any) error {
				stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, "SELECT 1")
				if err != nil {
					t.Fatal(err)
				}
				if stmt.(SnowflakeStmt).GetQueryID() != "" {
					t.Error("queryId should be empty before executing any query")
				}
				firstQuery, err := tc.f(stmt)
				if err != nil {
					t.Fatal(err)
				}
				if stmt.(SnowflakeStmt).GetQueryID() == "" {
					t.Error("queryId should not be empty after executing query")
				}
				if stmt.(SnowflakeStmt).GetQueryID() != firstQuery.(SnowflakeRows).GetQueryID() {
					t.Error("queryId should be equal among query result and prepared statement")
				}
				secondQuery, err := tc.f(stmt)
				if err != nil {
					t.Fatal(err)
				}
				if stmt.(SnowflakeStmt).GetQueryID() == "" {
					t.Error("queryId should not be empty after executing query")
				}
				if stmt.(SnowflakeStmt).GetQueryID() != secondQuery.(SnowflakeRows).GetQueryID() {
					t.Error("queryId should be equal among query result and prepared statement")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStatementQuery(t *testing.T) {
	ctx := context.Background()
	conn := openConn(t)
	defer conn.Close()

	testcases := []struct {
		name    string
		query   string
		f       func(stmt driver.Stmt) (driver.Rows, error)
		wantErr bool
	}{
		{
			"validQuery",
			"SELECT 1",
			func(stmt driver.Stmt) (driver.Rows, error) {
				return stmt.Query(nil)
			},
			false,
		},
		{
			"validQueryContext",
			"SELECT 1",
			func(stmt driver.Stmt) (driver.Rows, error) {
				return stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
			},
			false,
		},
		{
			"invalidQuery",
			"SELECT * FROM non_existing_table",
			func(stmt driver.Stmt) (driver.Rows, error) {
				return stmt.Query(nil)
			},
			true,
		},
		{
			"invalidQueryContext",
			"SELECT * FROM non_existing_table",
			func(stmt driver.Stmt) (driver.Rows, error) {
				return stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
			},
			true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := conn.Raw(func(x any) error {
				stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, tc.query)
				if err != nil {
					if tc.wantErr {
						return nil // expected error
					}
					t.Fatal(err)
				}

				_, err = tc.f(stmt)
				if (err != nil) != tc.wantErr {
					t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
				}

				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStatementQueryIdForExecs(t *testing.T) {
	ctx := context.Background()
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE TestStatementQueryIdForExecs (v INTEGER)")
		defer dbt.mustExec("DROP TABLE IF EXISTS TestStatementQueryIdForExecs")

		testcases := []struct {
			name string
			f    func(stmt driver.Stmt) (driver.Result, error)
		}{
			{
				"exec",
				func(stmt driver.Stmt) (driver.Result, error) {
					return stmt.Exec(nil)
				},
			},
			{
				"execContext",
				func(stmt driver.Stmt) (driver.Result, error) {
					return stmt.(driver.StmtExecContext).ExecContext(ctx, nil)
				},
			},
		}

		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				err := dbt.conn.Raw(func(x any) error {
					stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, "INSERT INTO TestStatementQueryIdForExecs VALUES (1)")
					if err != nil {
						t.Fatal(err)
					}
					if stmt.(SnowflakeStmt).GetQueryID() != "" {
						t.Error("queryId should be empty before executing any query")
					}
					firstExec, err := tc.f(stmt)
					if err != nil {
						t.Fatal(err)
					}
					if stmt.(SnowflakeStmt).GetQueryID() == "" {
						t.Error("queryId should not be empty after executing query")
					}
					if stmt.(SnowflakeStmt).GetQueryID() != firstExec.(SnowflakeResult).GetQueryID() {
						t.Error("queryId should be equal among query result and prepared statement")
					}
					secondExec, err := tc.f(stmt)
					if err != nil {
						t.Fatal(err)
					}
					if stmt.(SnowflakeStmt).GetQueryID() == "" {
						t.Error("queryId should not be empty after executing query")
					}
					if stmt.(SnowflakeStmt).GetQueryID() != secondExec.(SnowflakeResult).GetQueryID() {
						t.Error("queryId should be equal among query result and prepared statement")
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

func TestStatementQueryExecs(t *testing.T) {
	ctx := context.Background()
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE TestStatementQueryExecs (v INTEGER)")
		defer dbt.mustExec("DROP TABLE IF EXISTS TestStatementForExecs")

		testcases := []struct {
			name    string
			query   string
			f       func(stmt driver.Stmt) (driver.Result, error)
			wantErr bool
		}{
			{
				"validExec",
				"INSERT INTO TestStatementQueryExecs VALUES (1)",
				func(stmt driver.Stmt) (driver.Result, error) {
					return stmt.Exec(nil)
				},
				false,
			},
			{
				"validExecContext",
				"INSERT INTO TestStatementQueryExecs VALUES (1)",
				func(stmt driver.Stmt) (driver.Result, error) {
					return stmt.(driver.StmtExecContext).ExecContext(ctx, nil)
				},
				false,
			},
			{
				"invalidExec",
				"INSERT INTO TestStatementQueryExecs VALUES ('invalid_data')",
				func(stmt driver.Stmt) (driver.Result, error) {
					return stmt.Exec(nil)
				},
				true,
			},
			{
				"invalidExecContext",
				"INSERT INTO TestStatementQueryExecs VALUES ('invalid_data')",
				func(stmt driver.Stmt) (driver.Result, error) {
					return stmt.(driver.StmtExecContext).ExecContext(ctx, nil)
				},
				true,
			},
		}

		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				err := dbt.conn.Raw(func(x any) error {
					stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, tc.query)
					if err != nil {
						if tc.wantErr {
							return nil // expected error
						}
						t.Fatal(err)
					}

					_, err = tc.f(stmt)
					if (err != nil) != tc.wantErr {
						t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
					}

					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

func TestWithQueryTag(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		testQueryTag := "TEST QUERY TAG"
		ctx := WithQueryTag(context.Background(), testQueryTag)

		// This query itself will be part of the history and will have the query tag
		rows := dbt.mustQueryContext(
			ctx,
			"SELECT QUERY_TAG FROM table(information_schema.query_history_by_session())")
		defer rows.Close()

		assertTrueF(t, rows.Next())
		var tag sql.NullString
		err := rows.Scan(&tag)
		assertNilF(t, err)
		assertTrueF(t, tag.Valid, "no QUERY_TAG set")
		assertEqualF(t, tag.String, testQueryTag)
	})
}
