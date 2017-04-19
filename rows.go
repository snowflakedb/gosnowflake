// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//

package gosnowflake

import (
	"database/sql/driver"
	"io"
	"log"
)

type snowflakeRows struct {
	sc              *snowflakeConn
	RowType         []ExecResponseRowType
	Total           int64
	TotalRowIndex   int64
	CurrentIndex    int
	CurrentRowCount int
	CurrentRowSet   [][]*string
}

func (rows *snowflakeRows) Close() (err error) {
	log.Println("Rows.Close")
	return nil
}

func (rows *snowflakeRows) Columns() []string {
	log.Println("Rows.Columns")
	ret := make([]string, len(rows.RowType))
	for i, n := 0, len(rows.RowType); i < n; i++ {
		ret[i] = rows.RowType[i].Name
	}
	return ret
}

func (rows *snowflakeRows) HasNextResultSet() (b bool) {
	log.Println("Rows.HasNextResultSet")
	return true
}

func (rows *snowflakeRows) NextResultSet() (err error) {
	log.Println("Rows.NextResultSet")
	return nil
}

func (rows *snowflakeRows) Next(dest []driver.Value) (err error) {
	log.Println("Rows.Next")
	rows.TotalRowIndex += 1
	if rows.TotalRowIndex >= rows.Total {
		return io.EOF
	}
	rows.CurrentIndex += 1
	if rows.CurrentIndex >= rows.CurrentRowCount {
		// TODO: fetch next chunk set
	}
	for i, n := 0, len(rows.CurrentRowSet[rows.CurrentIndex]); i < n; i++ {
		err := stringToValue(&dest[i], rows.RowType[i], rows.CurrentRowSet[rows.CurrentIndex][i])
		if err != nil {
			return err
		}
	}
	return err
}
