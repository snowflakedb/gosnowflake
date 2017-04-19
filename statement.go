package gosnowflake

import (
	"database/sql/driver"
	"log"
)

type snowflakeStmt struct {
	sc    *snowflakeConn
	query string
}

func (stmt *snowflakeStmt) Close() error {
	log.Println("Stmt.Close")
	return nil
}

func (stmt *snowflakeStmt) NumInput() int {
	log.Println("Stmt.NumInput")
	return -1
}

func (stmt *snowflakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	log.Println("Stmt.Exec")
	return stmt.sc.Exec(stmt.query, args)
}

func (stmt *snowflakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	log.Println("Stmt.Query")
	return stmt.sc.Query(stmt.query, args)
}
