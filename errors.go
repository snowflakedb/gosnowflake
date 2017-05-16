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
	/* connection */

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
	// ErrCodeIdpConnectionError is an error code for the case where a IDP connection failed
	ErrCodeIdpConnectionError = 260005

	/* network */

	// ErrFailedToPostQuery is an error code for the case where HTTP POST failed.
	ErrFailedToPostQuery = 261001
	// ErrFailedToRenewSession is an error code for the case where session renewal failed.
	ErrFailedToRenewSession = 261002
	// ErrFailedToCancelQuery is an error code for the case where cancel query failed.
	ErrFailedToCancelQuery = 261003
	// ErrFailedToCloseSession is an error code for the case where close session failed.
	ErrFailedToCloseSession = 261004
	// ErrFailedToAuth is an error code for the case where authentication failed for unknown reason
	ErrFailedToAuth = 261005

	/* rows */

	// ErrFailedToGetChunk is an error code for the case where it failed to get chunk of result set
	ErrFailedToGetChunk = 262001

	/* converter */

	// ErrInvalidTimestampTz is an error code for the case where a returned TIMESTAMP_TZ internal value is invalid
	ErrInvalidTimestampTz = 268001
	// ErrInvalidOffsetStr is an error code for the case where a offset string is invalid. The input string must
	// consist of sHHMI where one sign character '+'/'-' followed by zero filled hours and minutes
	ErrInvalidOffsetStr = 268001
	// ErrInvalidBinaryHexForm is an error code for the case where a binary data in hex form is invalid.
	ErrInvalidBinaryHexForm = 268002
)

const (
	errMsgFailedToParsePort    = "failed to parse a port number. port: %v"
	errMsgInvalidOffsetStr     = "offset must be a string consist of sHHMI where one sign character '+'/'-' followed by zero filled hours and minutes: %v"
	errMsgInvalidByteArray     = "invalid byte array: %v"
	errMsgIdpConnectionError   = "failed to verify URLs. authenticator: %v, token URL:%v, SSO URL:%v"
	errMsgFailedToGetChunk     = "failed to get a chunk of result sets. idx: %v"
	errMsgFailedToPostQuery    = "failed to POST. HTTP: %v, URL: %v"
	errMsgFailedToRenew        = "failed to renew session. HTTP: %v, URL: %v"
	errMsgFailedToCancelQuery  = "failed to cancel query. HTTP: %v, URL: %v"
	errMsgFailedToCloseSession = "failed to close session. HTTP: %v, URL: %v"
	errMsgFailedToAuth         = "failed to auth. HTTP: %v, URL: %v"
)

var (
	// errors

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
