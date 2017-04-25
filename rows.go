// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"database/sql/driver"

	"github.com/golang/glog"
)

type snowflakeRows struct {
	sc              *snowflakeConn
	RowType         []execResponseRowType
	ChunkDownloader *snowflakeChunkDownloader
}

func (rows *snowflakeRows) Close() (err error) {
	glog.V(2).Infoln("Rows.Close")
	return nil
}

/*
// ColumnTypeDatabaseTypeName returns the database column name.
func (rows *snowflakeRows) ColumnTypeDatabaseTypeName(index int) string {
	// TODO: is this canonical name or can be Snowflake specific name?
	return strings.ToUpper(rows.RowType[index].Name)
}
*/

func (rows *snowflakeRows) Columns() []string {
	glog.V(2).Infoln("Rows.Columns")
	ret := make([]string, len(rows.RowType))
	for i, n := 0, len(rows.RowType); i < n; i++ {
		ret[i] = rows.RowType[i].Name
	}
	return ret
}

func (rows *snowflakeRows) Next(dest []driver.Value) (err error) {
	// glog.V(2).Infoln("Rows.Next")
	row, err := rows.ChunkDownloader.Next()
	if err != nil {
		// includes io.EOF
		return err
	}
	for i, n := 0, len(row); i < n; i++ {
		err := stringToValue(&dest[i], rows.RowType[i], row[i])
		if err != nil {
			return err
		}
	}
	return err
}
