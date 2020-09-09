// Copyright (c) 2020 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
)

func TestMultiStatementExecuteNoResultSet(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	_, err = db.Exec("drop table if exists test_multi_statement_txn")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	_, err = db.Exec("create or replace table test_multi_statement_txn(c1 number, c2 string)" +
		"as select 10, 'z'")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	ctx, _ := WithMultiStatement(context.Background(), 4)
	multiStmtQuery := "begin;\n" +
		"delete from test_multi_statement_txn;\n" +
		"insert into test_multi_statement_txn values (1, 'a'), (2, 'b');\n" +
		"commit;"
	res, err := db.ExecContext(ctx, multiStmtQuery)
	if err != nil {
		t.Fatalf("failed to execute multiple statements: %v", err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 affected rows, got %d", count)
	}

	_, err = db.Exec("drop table if exists test_multi_statement_txn")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
}

func TestMultiStatementQueryResultSet(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	multiStmtQuery := "select 123;\n" +
		"select 456;\n" +
		"select 789;\n" +
		"select '000';"

	ctx, _ := WithMultiStatement(context.Background(), 4)
	rows, err := db.QueryContext(ctx, multiStmtQuery)
	if err != nil {
		t.Fatalf("failed to query multiple statements: %v", err)
	}
	defer rows.Close()
	var v1, v2, v3 int64
	var v4 string

	// first statement
	if rows.Next() {
		err = rows.Scan(&v1)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v1 != 123 {
			t.Fatalf("failed to fetch. value: %v", v1)
		}
	} else {
		t.Error("failed to query")
	}

	// second statement
	if !rows.NextResultSet() {
		t.Error("failed to retrieve next result set")
	}
	if rows.Next() {
		err = rows.Scan(&v2)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v2 != 456 {
			t.Fatalf("failed to fetch. value: %v", v2)
		}
	} else {
		t.Error("failed to query")
	}

	// third statement
	if !rows.NextResultSet() {
		t.Error("failed to retrieve next result set")
	}
	if rows.Next() {
		err = rows.Scan(&v3)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v3 != 789 {
			t.Fatalf("failed to fetch. value: %v", v3)
		}
	} else {
		t.Error("failed to query")
	}

	// fourth statement
	if !rows.NextResultSet() {
		t.Error("failed to retrieve next result set")
	}
	if rows.Next() {
		err = rows.Scan(&v4)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v4 != "000" {
			t.Fatalf("failed to fetch. value: %v", v4)
		}
	} else {
		t.Error("failed to query")
	}
}

func TestMultiStatementExecuteResultSet(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	_, err = db.Exec("drop table if exists test_multi_statement_txn_rb")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	_, err = db.Exec("create or replace table test_multi_statement_txn_rb(c1 number, c2 string)" +
		"as select 10, 'z'")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	ctx, _ := WithMultiStatement(context.Background(), 6)
	multiStmtQuery := "begin;\n" +
		"delete from test_multi_statement_txn_rb;\n" +
		"insert into test_multi_statement_txn_rb values (1, 'a'), (2, 'b');\n" +
		"select 1;\n" +
		"select 2;\n" +
		"rollback;"

	res, err := db.ExecContext(ctx, multiStmtQuery)
	if err != nil {
		t.Fatalf("failed to execute statement: %v", err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %s", err.Error())
	}
	if count != 3 {
		t.Fatalf("expected 3 affected rows, got %d", count)
	}

	_, err = db.Exec("drop table if exists test_multi_statement_txn_rb")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
}

func TestMultiStatementQueryNoResultSet(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	_, err = db.Exec("drop table if exists test_multi_statement_txn")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	_, err = db.Exec("create or replace table test_multi_statement_txn(c1 number, c2 string)" +
		"as select 10, 'z'")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	ctx, _ := WithMultiStatement(context.Background(), 4)
	multiStmtQuery := "begin;\n" +
		"delete from test_multi_statement_txn;\n" +
		"insert into test_multi_statement_txn values (1, 'a'), (2, 'b');\n" +
		"commit;"
	_, err = db.QueryContext(ctx, multiStmtQuery)
	if err != nil {
		t.Fatalf("failed to query multiple statements: %v", err)
	}

	_, err = db.Exec("drop table if exists test_multi_statement_txn")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
}

func TestMultiStatementExecuteMix(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx, _ := WithMultiStatement(context.Background(), 3)
	multiStmtQuery := "create or replace temporary table test_multi (cola int);\n" +
		"insert into test_multi values (1), (2);\n" +
		"select cola from test_multi order by cola asc;"
	res, err := db.ExecContext(ctx, multiStmtQuery)
	if err != nil {
		t.Fatal("failed to execute statement: ", err)
	}

	count, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %s", err.Error())
	}
	if count != 2 {
		t.Fatalf("expected 3 affected rows, got %d", count)
	}
}

func TestMultiStatementQueryMix(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx, _ := WithMultiStatement(context.Background(), 3)
	multiStmtQuery := "create or replace temporary table test_multi (cola int);\n" +
		"insert into test_multi values (1), (2);\n" +
		"select cola from test_multi order by cola asc;"
	rows, err := db.QueryContext(ctx, multiStmtQuery)
	if err != nil {
		t.Fatal("failed to execute statement: ", err)
	}
	defer rows.Close()

	// first statement
	if !rows.Next() {
		t.Error("failed to query")
	}

	var count, v int
	// second statement
	rows.NextResultSet()
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if count != 2 {
			t.Fatalf("expected 2 affected rows, got %d", count)
		}
	}

	expected := 1
	// third statement
	rows.NextResultSet()
	for rows.Next() {
		err = rows.Scan(&v)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v != expected {
			t.Fatalf("failed to fetch. value: %v", v)
		}
		expected++
	}
}

func TestMultiStatementCountZero(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx, _ := WithMultiStatement(context.Background(), 0)
	var v1 int
	var v2 string
	var v3 float64
	var v4 bool

	// first query
	multiStmtQuery1 := "select 123;\n" +
		"select '456';"
	rows1, err := db.QueryContext(ctx, multiStmtQuery1)
	if err != nil {
		t.Fatalf("failed to query multiple statements: %v", err)
	}
	defer rows1.Close()
	// first statement
	if rows1.Next() {
		err = rows1.Scan(&v1)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v1 != 123 {
			t.Fatalf("failed to fetch. value: %v", v1)
		}
	} else {
		t.Error("failed to query")
	}

	// second statement
	if !rows1.NextResultSet() {
		t.Error("failed to retrieve next result set")
	}
	if rows1.Next() {
		err = rows1.Scan(&v2)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v2 != "456" {
			t.Fatalf("failed to fetch. value: %v", v2)
		}
	} else {
		t.Error("failed to query")
	}

	// second query
	multiStmtQuery2 := "select 789;\n" +
		"select 'foo';\n" +
		"select 0.123;\n" +
		"select true;"
	rows2, err := db.QueryContext(ctx, multiStmtQuery2)
	if err != nil {
		t.Fatalf("failed to query multiple statements: %v", err)
	}
	defer rows2.Close()
	// first statement
	if rows2.Next() {
		err = rows2.Scan(&v1)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v1 != 789 {
			t.Fatalf("failed to fetch. value: %v", v1)
		}
	} else {
		t.Error("failed to query")
	}

	// second statement
	if !rows2.NextResultSet() {
		t.Error("failed to retrieve next result set")
	}
	if rows2.Next() {
		err = rows2.Scan(&v2)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v2 != "foo" {
			t.Fatalf("failed to fetch. value: %v", v2)
		}
	} else {
		t.Error("failed to query")
	}

	// third statement
	if !rows2.NextResultSet() {
		t.Error("failed to retrieve next result set")
	}
	if rows2.Next() {
		err = rows2.Scan(&v3)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v3 != 0.123 {
			t.Fatalf("failed to fetch. value: %v", v3)
		}
	} else {
		t.Error("failed to query")
	}

	// fourth statement
	if !rows2.NextResultSet() {
		t.Error("failed to retrieve next result set")
	}
	if rows2.Next() {
		err = rows2.Scan(&v4)
		if err != nil {
			t.Errorf("failed to scan: %#v", err)
		}
		if v4 != true {
			t.Fatalf("failed to fetch. value: %v", v4)
		}
	} else {
		t.Error("failed to query")
	}
}

func TestMultiStatementCountMismatch(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	multiStmtQuery := "select 123;\n" +
		"select 456;\n" +
		"select 789;\n" +
		"select '000';"

	ctx, _ := WithMultiStatement(context.Background(), 3)
	_, err = db.QueryContext(ctx, multiStmtQuery)
	if err == nil {
		t.Fatal("should have failed to query multiple statements")
	}
}

func TestWithAsyncMode(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx, _ := WithAsyncMode(context.Background())
	query := "SELECT SEQ8(), RANDSTR(1000, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT=>1000000))"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Error("failed query")
	}
	rows.Close()
	var idx int
	var v string
	for rows.Next() {
		err := rows.Scan(&idx, &v)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestGetQueryID(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx := context.TODO()
	conn, _ := db.Conn(ctx)

	err = conn.Raw(func(x interface{}) error {
		stmt, err := x.(driver.ConnPrepareContext).PrepareContext(ctx, "select 1")
		if err != nil {
			return err
		}
		rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
		if err != nil {
			return err
		}
		defer rows.Close()
		qid := rows.(SnowflakeResult).QueryID()
		if qid == "" {
			t.Fatal("should have returned a query ID string")
		}

		stmt, err = x.(driver.ConnPrepareContext).PrepareContext(ctx, "selectt 1")
		if err != nil {
			return err
		}
		rows, err = stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
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
		if rows != nil {
			t.Fatal("rows should be empty")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to prepare statement. err: %v", err)
	}
}
