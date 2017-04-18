// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidConn = errors.New("invalid connection")
)

type SnowflakeError struct {
	Number         int
	SqlState       string
	QueryId        string
	Message        string
	IncludeQueryId bool
}

func (se *SnowflakeError) Error() string {
	if se.IncludeQueryId {
		return fmt.Sprintf("%06d (%s): %s: %s", se.Number, se.SqlState, se.QueryId, se.Message)

	} else {
		return fmt.Sprintf("%06d (%s): %s", se.Number, se.SqlState, se.Message)

	}
}
