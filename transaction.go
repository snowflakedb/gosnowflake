// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
)

type snowflakeTx struct {
	sc  *snowflakeConn
	ctx context.Context
}

func (tx *snowflakeTx) Commit() (err error) {
	if tx.sc == nil || tx.sc.rest == nil {
		return driver.ErrBadConn
	}
	_, err = tx.sc.exec(tx.ctx, "COMMIT", false /* noResult */, false /* isInternal */, false /* describeOnly */, nil)
	if err != nil {
		return
	}
	tx.sc = nil
	return
}

func (tx *snowflakeTx) Rollback() (err error) {
	if tx.sc == nil || tx.sc.rest == nil {
		return driver.ErrBadConn
	}
	_, err = tx.sc.exec(tx.ctx, "ROLLBACK", false /* noResult */, false /* isInternal */, false /* describeOnly */, nil)
	if err != nil {
		return
	}
	tx.sc = nil
	return
}
