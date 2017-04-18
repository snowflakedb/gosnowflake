// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//

package gosnowflake

import (
	"log"
)

type snowflakeField struct {
	tableName string
	name      string
	fieldType byte
	decimals  byte
}

type snowflakeResultSet struct {
	columns []snowflakeField
	done    bool
}
type snowflakeRows struct {
	sc *snowflakeConn
	rs snowflakeResultSet
}

func (rows *snowflakeRows) Columns() []string {
	log.Println("Rows.Columns")
	ret := make([]string, 1)
	ret[0] = "test"
	return ret
}

func (rows *snowflakeRows) Close() (err error) {
	log.Println("Rows.Close")
	return nil
}

func (rows *snowflakeRows) HasNextResultSet() (b bool) {
	log.Println("Rows.HasNextResultSet")
	return true
}

func (rows *snowflakeRows) nextResultSet() (int, error) {
	log.Println("Rows.nextResultSet")
	return 0, nil
}

func (rows *snowflakeRows) nextNotEmptyResultSet() (int, error) {
	log.Println("Rows.nextNotEmptyResultSet")
	return 0, nil
}

func (rows *snowflakeRows) NextResultSet() (err error) {
	return nil
}
