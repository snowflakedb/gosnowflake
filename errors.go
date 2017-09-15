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
	if se.SQLState != "" {
		if se.IncludeQueryID {
			return fmt.Sprintf("%06d (%s): %s: %s", se.Number, se.SQLState, se.QueryID, message)
		}
		return fmt.Sprintf("%06d (%s): %s", se.Number, se.SQLState, message)
	}
	if se.IncludeQueryID {
		return fmt.Sprintf("%06d: %s: %s", se.Number, se.QueryID, message)
	}
	return fmt.Sprintf("%06d: %s", se.Number, message)
}

const (
	/* connection */

	// ErrCodeEmptyAccountCode is an error code for the case where a DNS doesn't include account parameter
	ErrCodeEmptyAccountCode = 260000
	// ErrCodeEmptyUsernameCode is an error code for the case where a DNS doesn't include user parameter
	ErrCodeEmptyUsernameCode
	// ErrCodeEmptyPasswordCode is an error code for the case where a DNS doesn't include password parameter
	ErrCodeEmptyPasswordCode
	// ErrCodeFailedToParseHost is an error code for the case where a DNS includes an invalid host name
	ErrCodeFailedToParseHost
	// ErrCodeFailedToParsePort is an error code for the case where a DNS includes an invalid port number
	ErrCodeFailedToParsePort
	// ErrCodeIdpConnectionError is an error code for the case where a IDP connection failed
	ErrCodeIdpConnectionError
	// ErrCodeSSOURLNotMatch is an error code for the case where a SSO URL doesn't match
	ErrCodeSSOURLNotMatch
	// ErrServiceUnavailable is an error code for the case where service is unavailable.
	ErrServiceUnavailable
	// ErrFailedToConnect is an error code for the case where a DB connection failed due to wrong account name
	ErrFailedToConnect

	/* network */

	// ErrFailedToPostQuery is an error code for the case where HTTP POST failed.
	ErrFailedToPostQuery = 261000
	// ErrFailedToRenewSession is an error code for the case where session renewal failed.
	ErrFailedToRenewSession
	// ErrFailedToCancelQuery is an error code for the case where cancel query failed.
	ErrFailedToCancelQuery
	// ErrFailedToCloseSession is an error code for the case where close session failed.
	ErrFailedToCloseSession
	// ErrFailedToAuth is an error code for the case where authentication failed for unknown reason.
	ErrFailedToAuth
	// ErrFailedToAuthSAML is an error code for the case where authentication via SAML failed for unknown reason.
	ErrFailedToAuthSAML
	// ErrFailedToAuthOKTA is an error code for the case where authentication via OKTA failed for unknown reason.
	ErrFailedToAuthOKTA
	// ErrFailedToGetSSO is an error code for the case where authentication via OKTA failed for unknown reason.
	ErrFailedToGetSSO

	/* rows */

	// ErrFailedToGetChunk is an error code for the case where it failed to get chunk of result set
	ErrFailedToGetChunk = 262001

	/* transaction*/

	// ErrNoReadOnlyTransaction is an error code for the case where readonly mode is specified.
	ErrNoReadOnlyTransaction = 263001
	// ErrNoDefaultTransactionIsolationLevel is an error code for the case where non default isolation level is specified.
	ErrNoDefaultTransactionIsolationLevel

	/* converter */

	// ErrInvalidTimestampTz is an error code for the case where a returned TIMESTAMP_TZ internal value is invalid
	ErrInvalidTimestampTz = 268001
	// ErrInvalidOffsetStr is an error code for the case where a offset string is invalid. The input string must
	// consist of sHHMI where one sign character '+'/'-' followed by zero filled hours and minutes
	ErrInvalidOffsetStr
	// ErrInvalidBinaryHexForm is an error code for the case where a binary data in hex form is invalid.
	ErrInvalidBinaryHexForm
)

const (
	errMsgFailedToParseHost                  = "failed to parse a host name. host: %v"
	errMsgFailedToParsePort                  = "failed to parse a port number. port: %v"
	errMsgInvalidOffsetStr                   = "offset must be a string consist of sHHMI where one sign character '+'/'-' followed by zero filled hours and minutes: %v"
	errMsgInvalidByteArray                   = "invalid byte array: %v"
	errMsgIdpConnectionError                 = "failed to verify URLs. authenticator: %v, token URL:%v, SSO URL:%v"
	errMsgSSOURLNotMatch                     = "SSO URL didn't match. expected: %v, got: %v"
	errMsgFailedToGetChunk                   = "failed to get a chunk of result sets. idx: %v"
	errMsgFailedToPostQuery                  = "failed to POST. HTTP: %v, URL: %v"
	errMsgFailedToRenew                      = "failed to renew session. HTTP: %v, URL: %v"
	errMsgFailedToCancelQuery                = "failed to cancel query. HTTP: %v, URL: %v"
	errMsgFailedToCloseSession               = "failed to close session. HTTP: %v, URL: %v"
	errMsgFailedToAuth                       = "failed to auth for unknown reason. HTTP: %v, URL: %v"
	errMsgFailedToAuthSAML                   = "failed to auth via SAML for unknown reason. HTTP: %v, URL: %v"
	errMsgFailedToAuthOKTA                   = "failed to auth via OKTA for unknown reason. HTTP: %v, URL: %v"
	errMsgFailedToGetSSO                     = "failed to auth via OKTA for unknown reason. HTTP: %v, URL: %v"
	errMsgNoReadOnlyTransaction              = "no readonly mode is supported"
	errMsgNoDefaultTransactionIsolationLevel = "no default isolation transaction level is supported"
	errMsgServiceUnavailable                 = "service is unavailable. check your connectivity. you may need a proxy server. HTTP: %v, URL: %v"
	errMsgFailedToConnect                    = "failed to connect to db. verify account name is correct. HTTP: %v, URL: %v"
)

var (
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
