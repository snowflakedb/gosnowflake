// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"database/sql/driver"

	"github.com/golang/glog"
)

type snowflakeStmt struct {
	sc    *snowflakeConn
	query string
}

func (stmt *snowflakeStmt) Close() error {
	glog.V(2).Infoln("Stmt.Close")
	return nil
}

func (stmt *snowflakeStmt) NumInput() int {
	glog.V(2).Infoln("Stmt.NumInput")
	return -1
}

func (stmt *snowflakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	glog.V(2).Infoln("Stmt.Exec")
	return stmt.sc.Exec(stmt.query, args)
}

func (stmt *snowflakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	glog.V(2).Infoln("Stmt.Query")
	return stmt.sc.Query(stmt.query, args)
}
