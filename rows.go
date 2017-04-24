// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"database/sql/driver"
	"log"
)

type snowflakeRows struct {
	sc              *snowflakeConn
	RowType         []ExecResponseRowType
	ChunkDownloader *snowflakeChunkDownloader
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

func (rows *snowflakeRows) Next(dest []driver.Value) (err error) {
	// log.Println("Rows.Next")

	row, err := rows.ChunkDownloader.Next()
	if err != nil {
		// includes io.EOF
		return err
	}
	// log.Printf("ROW: %v", row)
	for i, n := 0, len(row); i < n; i++ {
		err := stringToValue(&dest[i], rows.RowType[i], row[i])
		if err != nil {
			return err
		}
	}
	return err
}
