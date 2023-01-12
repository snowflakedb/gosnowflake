// Copyright (c) 2020-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"io"
	"os"
	"testing"
	"time"
)

func TestMultiStatementExecuteNoResultSet(t *testing.T) {
	ctx, _ := WithMultiStatement(context.Background(), 4)
	multiStmtQuery := "begin;\n" +
		"delete from test_multi_statement_txn;\n" +
		"insert into test_multi_statement_txn values (1, 'a'), (2, 'b');\n" +
		"commit;"

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("drop table if exists test_multi_statement_txn")
		dbt.mustExec(`create or replace table test_multi_statement_txn(
			c1 number, c2 string) as select 10, 'z'`)
		defer dbt.mustExec("drop table if exists test_multi_statement_txn")

		res := dbt.mustExecContext(ctx, multiStmtQuery)
		count, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("res.RowsAffected() returned error: %v", err)
		}
		if count != 3 {
			t.Fatalf("expected 3 affected rows, got %d", count)
		}
	})
}

func TestMultiStatementQueryResultSet(t *testing.T) {
	ctx, _ := WithMultiStatement(context.Background(), 4)
	multiStmtQuery := "select 123;\n" +
		"select 456;\n" +
		"select 789;\n" +
		"select '000';"

	var v1, v2, v3 int64
	var v4 string
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(ctx, multiStmtQuery)

		// first statement
		if rows.Next() {
			if err := rows.Scan(&v1); err != nil {
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
			if err := rows.Scan(&v2); err != nil {
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
			if err := rows.Scan(&v3); err != nil {
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
			if err := rows.Scan(&v4); err != nil {
				t.Errorf("failed to scan: %#v", err)
			}
			if v4 != "000" {
				t.Fatalf("failed to fetch. value: %v", v4)
			}
		} else {
			t.Error("failed to query")
		}
	})
}

func TestMultiStatementExecuteResultSet(t *testing.T) {
	ctx, _ := WithMultiStatement(context.Background(), 6)
	multiStmtQuery := "begin;\n" +
		"delete from test_multi_statement_txn_rb;\n" +
		"insert into test_multi_statement_txn_rb values (1, 'a'), (2, 'b');\n" +
		"select 1;\n" +
		"select 2;\n" +
		"rollback;"

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("drop table if exists test_multi_statement_txn_rb")
		dbt.mustExec(`create or replace table test_multi_statement_txn_rb(
			c1 number, c2 string) as select 10, 'z'`)
		defer dbt.mustExec("drop table if exists test_multi_statement_txn_rb")

		res := dbt.mustExecContext(ctx, multiStmtQuery)
		count, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("res.RowsAffected() returned error: %v", err)
		}
		if count != 3 {
			t.Fatalf("expected 3 affected rows, got %d", count)
		}
	})
}

func TestMultiStatementQueryNoResultSet(t *testing.T) {
	ctx, _ := WithMultiStatement(context.Background(), 4)
	multiStmtQuery := "begin;\n" +
		"delete from test_multi_statement_txn;\n" +
		"insert into test_multi_statement_txn values (1, 'a'), (2, 'b');\n" +
		"commit;"

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("drop table if exists test_multi_statement_txn")
		dbt.mustExec(`create or replace table test_multi_statement_txn(
			c1 number, c2 string) as select 10, 'z'`)
		defer dbt.mustExec("drop table if exists tfmuest_multi_statement_txn")

		dbt.mustQueryContext(ctx, multiStmtQuery)
	})
}

func TestMultiStatementExecuteMix(t *testing.T) {
	ctx, _ := WithMultiStatement(context.Background(), 3)
	multiStmtQuery := "create or replace temporary table test_multi (cola int);\n" +
		"insert into test_multi values (1), (2);\n" +
		"select cola from test_multi order by cola asc;"

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("drop table if exists test_multi_statement_txn")
		dbt.mustExec(`create or replace table test_multi_statement_txn(
			c1 number, c2 string) as select 10, 'z'`)
		defer dbt.mustExec("drop table if exists test_multi_statement_txn")

		res := dbt.mustExecContext(ctx, multiStmtQuery)
		count, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("res.RowsAffected() returned error: %v", err)
		}
		if count != 2 {
			t.Fatalf("expected 2 affected rows, got %d", count)
		}
	})
}

func TestMultiStatementQueryMix(t *testing.T) {
	ctx, _ := WithMultiStatement(context.Background(), 3)
	multiStmtQuery := "create or replace temporary table test_multi (cola int);\n" +
		"insert into test_multi values (1), (2);\n" +
		"select cola from test_multi order by cola asc;"

	var count, v int
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("drop table if exists test_multi_statement_txn")
		dbt.mustExec(`create or replace table test_multi_statement_txn(
			c1 number, c2 string) as select 10, 'z'`)
		defer dbt.mustExec("drop table if exists test_multi_statement_txn")

		rows := dbt.mustQueryContext(ctx, multiStmtQuery)
		defer rows.Close()

		// first statement
		if !rows.Next() {
			t.Error("failed to query")
		}

		// second statement
		rows.NextResultSet()
		if rows.Next() {
			if err := rows.Scan(&count); err != nil {
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
			if err := rows.Scan(&v); err != nil {
				t.Errorf("failed to scan: %#v", err)
			}
			if v != expected {
				t.Fatalf("failed to fetch. value: %v", v)
			}
			expected++
		}
	})
}

func TestMultiStatementCountZero(t *testing.T) {
	ctx, _ := WithMultiStatement(context.Background(), 0)
	var v1 int
	var v2 string
	var v3 float64
	var v4 bool

	runTests(t, dsn, func(dbt *DBTest) {
		// first query
		multiStmtQuery1 := "select 123;\n" +
			"select '456';"
		rows1 := dbt.mustQueryContext(ctx, multiStmtQuery1)
		defer rows1.Close()
		// first statement
		if rows1.Next() {
			if err := rows1.Scan(&v1); err != nil {
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
			if err := rows1.Scan(&v2); err != nil {
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
		rows2 := dbt.mustQueryContext(ctx, multiStmtQuery2)
		defer rows2.Close()
		// first statement
		if rows2.Next() {
			if err := rows2.Scan(&v1); err != nil {
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
			if err := rows2.Scan(&v2); err != nil {
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
			if err := rows2.Scan(&v3); err != nil {
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
			if err := rows2.Scan(&v4); err != nil {
				t.Errorf("failed to scan: %#v", err)
			}
			if v4 != true {
				t.Fatalf("failed to fetch. value: %v", v4)
			}
		} else {
			t.Error("failed to query")
		}
	})
}

func TestMultiStatementCountMismatch(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	multiStmtQuery := "select 123;\n" +
		"select 456;\n" +
		"select 789;\n" +
		"select '000';"

	ctx, _ := WithMultiStatement(context.Background(), 3)
	if _, err := db.QueryContext(ctx, multiStmtQuery); err == nil {
		t.Fatal("should have failed to query multiple statements")
	}
}

func TestMultiStatementVaryingColumnCount(t *testing.T) {
	multiStmtQuery := "select c1 from test_tbl;\n" +
		"select c1,c2 from test_tbl;"
	ctx, _ := WithMultiStatement(context.Background(), 0)

	var v1, v2 int
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("create or replace table test_tbl(c1 int, c2 int)")
		dbt.mustExec("insert into test_tbl values(1, 0)")
		defer dbt.mustExec("drop table if exists test_tbl")

		rows := dbt.mustQueryContext(ctx, multiStmtQuery)
		defer rows.Close()

		if rows.Next() {
			if err := rows.Scan(&v1); err != nil {
				t.Errorf("failed to scan: %#v", err)
			}
			if v1 != 1 {
				t.Fatalf("failed to fetch. value: %v", v1)
			}
		} else {
			t.Error("failed to query")
		}

		if !rows.NextResultSet() {
			t.Error("failed to retrieve next result set")
		}

		if rows.Next() {
			if err := rows.Scan(&v1, &v2); err != nil {
				t.Errorf("failed to scan: %#v", err)
			}
			if v1 != 1 || v2 != 0 {
				t.Fatalf("failed to fetch. value: %v, %v", v1, v2)
			}
		} else {
			t.Error("failed to query")
		}
	})
}

// The total completion time should be similar to the duration of the query on Snowflake UI.
func TestMultiStatementExecutePerformance(t *testing.T) {
	ctx, _ := WithMultiStatement(context.Background(), 100)
	runTests(t, dsn, func(dbt *DBTest) {
		file, err := os.Open("test_data/multistatements.sql")
		if err != nil {
			t.Fatalf("failed opening file: %s", err)
		}
		defer file.Close()
		statements, err := io.ReadAll(file)
		if err != nil {
			t.Fatalf("failed reading file: %s", err)
		}

		sql := string(statements)

		start := time.Now()
		res := dbt.mustExecContext(ctx, sql)
		duration := time.Since(start)

		count, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("res.RowsAffected() returned error: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0 affected rows, got %d", count)
		}
		t.Logf("The total completion time was %v", duration)

		file, err = os.Open("test_data/multistatements_drop.sql")
		if err != nil {
			t.Fatalf("failed opening file: %s", err)
		}
		defer file.Close()
		statements, err = io.ReadAll(file)
		if err != nil {
			t.Fatalf("failed reading file: %s", err)
		}
		sql = string(statements)
		dbt.mustExecContext(ctx, sql)
	})
}
