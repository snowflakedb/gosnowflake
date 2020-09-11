// Copyright (c) 2017-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"database/sql/driver"
)

type snowflakeTx struct {
	sc *snowflakeConn
}

func (tx *snowflakeTx) Commit() (err error) {
	if tx.sc == nil || tx.sc.rest == nil {
		return driver.ErrBadConn
	}
	_, err = tx.sc.exec(tx.sc.ctx, "COMMIT", /* noResult */ false, /* isInternal */ false, /* describeOnly */ false, nil)
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
	_, err = tx.sc.exec(tx.sc.ctx, "ROLLBACK", /* noResult */ false, /* isInternal */ false, /* describeOnly */ false, nil)
	if err != nil {
		return
	}
	tx.sc = nil
	return
}
