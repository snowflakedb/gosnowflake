package gosnowflake

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestMultiStatementTransaction(t *testing.T) {
	var db *sql.DB
	var err error
	ndsn, err := AddConnParameter(dsn, MultiStatementCount, "0")
	if err != nil {
		t.Fatalf("failed to add connection parameter. err: %v", err)
	}
	if db, err = sql.Open("snowflake", ndsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	fmt.Println(dsn)
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

	multiStmtQuery := "begin;\n" +
		"delete from test_multi_statement_txn;\n" +
		"insert into test_multi_statement_txn values (1, 'a'), (2, 'b');\n" +
		"commit;"
	db.Exec(multiStmtQuery)

	_, err = db.Exec("drop table if exists test_multi_statement_txn")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
}

func TestMultiStatementQuery(t *testing.T) {
	var db *sql.DB
	var err error
	ndsn, err := AddConnParameter(dsn, MultiStatementCount, "0")
	if err != nil {
		t.Fatalf("failed to add connection parameter. err: %v", err)
	}
	if db, err = sql.Open("snowflake", ndsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	multiStmtQuery := "select 123;\n" +
		"select 456;\n" +
		"select 789;\n" +
		"select '000';"

	rows, _ := db.Query(multiStmtQuery)
	defer rows.Close()
	var v1, v2, v3 int64
	var v4 string
	if rows.Next() {
		err = rows.Scan(&v1)
		if v1 != 123 {
			t.Error(err)
		}
	} else {
		t.Error("no data")
	}
	if !rows.NextResultSet() {
		t.Fatal(err)
	}

	if rows.Next() {
		err = rows.Scan(&v2)
		if v2 != 456 {
			t.Error(err)
		}
	} else {
		t.Error("no data")
	}
	if !rows.NextResultSet() {
		t.Fatal(err)
	}

	if rows.Next() {
		err = rows.Scan(&v3)
		if v3 != 789 {
			t.Error(err)
		}
	} else {
		t.Errorf("no data")
	}
	if !rows.NextResultSet() {
		t.Fatal(err)
	}

	if rows.Next() {
		err = rows.Scan(&v4)
		if v4 != "000" {
			t.Error(err)
		}
	} else {
		t.Errorf("no data")
	}

}

func TestMultiStatementRollback(t *testing.T) {
	var db *sql.DB
	var err error
	ndsn, err := AddConnParameter(dsn, MultiStatementCount, "0")
	if err != nil {
		t.Fatalf("failed to add connection parameter. err: %v", err)
	}
	if db, err = sql.Open("snowflake", ndsn); err != nil {
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

	multiStmtQuery := "begin;\n" +
		"delete from test_multi_statement_txn_rb;\n" +
		"insert into test_multi_statement_txn_rb values (1, 'a'), (2, 'b');\n" +
		"rollback;"

	_, err = db.Exec(multiStmtQuery)
	if err != nil {
		fmt.Println(err)
		t.Fatalf("failed to execute statement: %v", err)
	}

	_, err = db.Exec("drop table if exists test_multi_statement_txn_rb")
	if err != nil {
		fmt.Println(err)
		t.Fatalf("failed to drop table: %v", err)
	}

}

func TestMultiStatementExecute(t *testing.T) {
	var db *sql.DB
	var err error
	ndsn, err := AddConnParameter(dsn, MultiStatementCount, "0")
	if err != nil {
		t.Fatalf("failed to add connection parameter. err: %v", err)
	}
	if db, err = sql.Open("snowflake", ndsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	multiStmtQuery := "create or replace temporary table test_multi (cola int);\n" +
		"insert into test_multi values (1), (2);\n" +
		"select cola from test_multi order by cola asc;"
	rows, err := db.Query(multiStmtQuery)
	if err != nil {
		t.Fatal("failed to execute statement: ", err)
	}
	defer rows.Close()

	var v1, v2 int64
	if rows.Next() {
		err = rows.Scan(&v1)
		if v1 != 1 {
			t.Error(err)
		}
	}
	if rows.Next() {
		err = rows.Scan(&v2)
		if v2 != 2 {
			t.Error(err)
		}
	}
}
