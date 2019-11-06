// Copyright (c) 2017-2018 Snowflake Computing Inc. All right reserved.

package gosnowflake

// SnowflakeResult provide their associated query ID
//
// The driver.Rows and driver.Result values returned by this driver
// both implement this interface.
type SnowflakeResult interface {
	QueryID() string
}

type snowflakeResult struct {
	affectedRows int64
	insertID     int64 // Snowflake doesn't support last insert id
	queryID      string
}

func (res *snowflakeResult) LastInsertId() (int64, error) {
	return res.insertID, nil
}

func (res *snowflakeResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}

func (res *snowflakeResult) QueryID() string {
	return res.queryID
}
