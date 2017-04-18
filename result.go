// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2012-2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

type snowflakeResult struct {
	affectedRows int64
	insertId     int64 // Snowflake doesn't support last insert id
}

func (res *snowflakeResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

func (res *snowflakeResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
