// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"errors"
	"fmt"
)

// SnowflakeError is a error type including various Snowflake specific information.
type SnowflakeError struct {
	Number         int
	SQLState       string
	QueryID        string
	Message        string
	IncludeQueryID bool // TODO: populate this in connection
}

func (se *SnowflakeError) Error() string {
	if se.IncludeQueryID {
		return fmt.Sprintf("%06d (%s): %s: %s", se.Number, se.SQLState, se.QueryID, se.Message)
	}
	return fmt.Sprintf("%06d (%s): %s", se.Number, se.SQLState, se.Message)
}

var (
	// ErrEmptyAccount is returned if a DNS doesn't include account parameter.
	ErrEmptyAccount = errors.New("account is empty")
	// ErrEmptyUsername is returned if a DNS doesn't include user parameter.
	ErrEmptyUsername = errors.New("user is empty")
	// ErrEmptyPassword is returned if a DNS doesn't include password parameter.
	ErrEmptyPassword = errors.New("password is empty")
	// ErrInvalidConn is returned if a connection is not available or in invalid state.
	ErrInvalidConn = errors.New("invalid connection")
)
