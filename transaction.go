// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import "context"

type snowflakeTx struct {
	sc *snowflakeConn
}

func (tx *snowflakeTx) Commit() (err error) {
	if tx.sc == nil || tx.sc.rest == nil {
		return ErrInvalidConn
	}
	_, err = tx.sc.exec(context.TODO(), "COMMIT", false, false, nil)
	if err != nil {
		return
	}
	tx.sc = nil
	return
}

func (tx *snowflakeTx) Rollback() (err error) {
	if tx.sc == nil || tx.sc.rest == nil {
		return ErrInvalidConn
	}
	_, err = tx.sc.exec(context.TODO(), "ROLLBACK", false, false, nil)
	if err != nil {
		return
	}
	tx.sc = nil
	return
}
