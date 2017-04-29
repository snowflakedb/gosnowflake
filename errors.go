// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"fmt"
)

// SnowflakeError is a error type including various Snowflake specific information.
type SnowflakeError struct {
	Number         int
	SQLState       string
	QueryID        string
	Message        string
	MessageArgs    []interface{}
	IncludeQueryID bool // TODO: populate this in connection
}

func (se *SnowflakeError) Error() string {
	message := se.Message
	if len(se.MessageArgs) > 0 {
		message = fmt.Sprintf(se.Message, se.MessageArgs)
	}
	if se.IncludeQueryID {
		return fmt.Sprintf("%06d (%s): %s: %s", se.Number, se.SQLState, se.QueryID, message)
	}
	return fmt.Sprintf("%06d (%s): %s", se.Number, se.SQLState, message)
}

const (
	// connection

	// ErrCodeInvalidConnCode is an error code for the case where a connection is not available or in invalid state.
	ErrCodeInvalidConnCode = 260000
	// ErrCodeEmptyAccountCode is an error code for the case where a DNS doesn't include account parameter
	ErrCodeEmptyAccountCode = 260001
	// ErrCodeEmptyUsernameCode is an error code for the case where a DNS doesn't include user parameter
	ErrCodeEmptyUsernameCode = 260002
	// ErrCodeEmptyPasswordCode is an error code for the case where a DNS doesn't include password parameter
	ErrCodeEmptyPasswordCode = 260003
	// ErrCodeFailedToParsePort is an error code for the case where a DNS includes an invalid port number
	ErrCodeFailedToParsePort = 260004

	// ErrInvalidTimestampTz is an error code for the case where a returned TIMESTAMP_TZ internal value is invalid
	ErrInvalidTimestampTz = 261001
)

const (
	// ErrMsgFailedToParsePort is an error message for the where a DNS includes an invalid port number
	ErrMsgFailedToParsePort = "failed to parse a port number. port: %v"
)

var (
	// preformatted errors

	// ErrInvalidConn is returned if a connection is not available or in invalid state.
	ErrInvalidConn = &SnowflakeError{
		Number:  ErrCodeInvalidConnCode,
		Message: "invalid connection",
	}
	// ErrEmptyAccount is returned if a DNS doesn't include account parameter.
	ErrEmptyAccount = &SnowflakeError{
		Number:  ErrCodeEmptyAccountCode,
		Message: "account is empty",
	}
	// ErrEmptyUsername is returned if a DNS doesn't include user parameter.
	ErrEmptyUsername = &SnowflakeError{
		Number:  ErrCodeEmptyUsernameCode,
		Message: "user is empty",
	}
	// ErrEmptyPassword is returned if a DNS doesn't include password parameter.
	ErrEmptyPassword = &SnowflakeError{
		Number:  ErrCodeEmptyPasswordCode,
		Message: "password is empty"}
)
