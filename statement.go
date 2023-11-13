// Copyright (c) 2017-2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
)

// SnowflakeStmt represents the prepared statement in driver.
type SnowflakeStmt interface {
	GetQueryID() string
}

type snowflakeStmt struct {
	sc          *snowflakeConn
	query       string
	lastQueryID string
}

func (stmt *snowflakeStmt) Close() error {
	logger.WithContext(stmt.sc.ctx).Infoln("Stmt.Close")
	// noop
	return nil
}

func (stmt *snowflakeStmt) NumInput() int {
	logger.WithContext(stmt.sc.ctx).Infoln("Stmt.NumInput")
	// Go Snowflake doesn't know the number of binding parameters.
	return -1
}

func (stmt *snowflakeStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	logger.WithContext(stmt.sc.ctx).Infoln("Stmt.ExecContext")
	result, err := stmt.sc.ExecContext(ctx, stmt.query, args)
	if err != nil {
		var snowflakeError *SnowflakeError
		if errors.As(err, &snowflakeError) {
			stmt.lastQueryID = snowflakeError.QueryID
		}
		return nil, err
	}
	r, ok := result.(SnowflakeResult)
	if !ok {
		return nil, fmt.Errorf("interface convertion. expected type SnowflakeResult but got %T", result)
	}
	stmt.lastQueryID = r.GetQueryID()
	return result, err
}

func (stmt *snowflakeStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	logger.WithContext(stmt.sc.ctx).Infoln("Stmt.QueryContext")
	rows, err := stmt.sc.QueryContext(ctx, stmt.query, args)
	if err != nil {
		var snowflakeError *SnowflakeError
		if errors.As(err, &snowflakeError) {
			stmt.lastQueryID = snowflakeError.QueryID
		}
		return nil, err
	}
	r, ok := rows.(SnowflakeRows)
	if !ok {
		return nil, fmt.Errorf("interface convertion. expected type SnowflakeRows but got %T", rows)
	}
	stmt.lastQueryID = r.GetQueryID()
	return rows, nil
}

func (stmt *snowflakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	logger.WithContext(stmt.sc.ctx).Infoln("Stmt.Exec")
	result, err := stmt.sc.Exec(stmt.query, args)
	if err != nil {
		var snowflakeError *SnowflakeError
		if errors.As(err, &snowflakeError) {
			stmt.lastQueryID = snowflakeError.QueryID
		}
		return nil, err
	}
	r, ok := result.(SnowflakeResult)
	if !ok {
		return nil, fmt.Errorf("interface convertion. expected type SnowflakeResult but got %T", result)
	}
	stmt.lastQueryID = r.GetQueryID()
	return result, err
}

func (stmt *snowflakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	logger.WithContext(stmt.sc.ctx).Infoln("Stmt.Query")
	rows, err := stmt.sc.Query(stmt.query, args)
	if err != nil {
		var snowflakeError *SnowflakeError
		if errors.As(err, &snowflakeError) {
			stmt.lastQueryID = snowflakeError.QueryID
		}
		return nil, err
	}
	r, ok := rows.(SnowflakeRows)
	if !ok {
		return nil, fmt.Errorf("interface convertion. expected type SnowflakeRows but got %T", rows)
	}
	stmt.lastQueryID = r.GetQueryID()
	return rows, err
}

func (stmt *snowflakeStmt) GetQueryID() string {
	return stmt.lastQueryID
}
