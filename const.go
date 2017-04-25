// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"fmt"
	"time"
)

const (
	defaultLoginTimeout   = 60 * time.Second
	defaultConnectTimeout = 60 * time.Second
)
const (
	headerSnowflakeToken   = "Snowflake Token=\"%v\""
	headerAuthorizationKey = "Authorization"

	headerSseCAlgorithm = "x-amz-server-side-encryption-customer-algorithm"
	headerSseCKey       = "x-amz-server-side-encryption-customer-key"
	headerSseCAes       = "AES256"

	headerContentTypeApplicationJSON    = "application/json"
	headerAcceptTypeAppliationSnowflake = "application/snowflake"

	sessionExpiredCode       = "390112"
	queryInProgressCode      = "333333"
	queryInProgressAsyncCode = "333334"

	clientType    = "Go"
	clientVersion = "0.1"  // TODO: should be updated at build time
	osVersion     = "0.11" // TODO: should be retrieved
)

// UserAgent shows up in User-Agent HTTP header
var UserAgent string = fmt.Sprintf("%v %v", clientType, clientVersion)

const (
	statementTypeIDDml              = int64(0x3000)
	statementTypeIDInsert           = statementTypeIDDml + int64(0x100)
	statementTypeIDUpdate           = statementTypeIDDml + int64(0x200)
	statementTypeIDDelete           = statementTypeIDDml + int64(0x300)
	statementTypeIDMerge            = statementTypeIDDml + int64(0x400)
	statementTypeIDMultiTableInsert = statementTypeIDDml + int64(0x500)
)
