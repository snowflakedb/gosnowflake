package gosnowflake

// This file just exports types, functions and methods as needed

import (
	"database/sql/driver"
	"time"
)

// The following comments make the linter happy...

// ExecResponse exports execResponse
type ExecResponse = execResponse

// ExecResponseRowType exports execResponseRowType
type ExecResponseRowType = execResponseRowType

// ExecResponseChunk exports execResponseChunk
type ExecResponseChunk = execResponseChunk

// RawSnowflakeRows exports the "raw" underlying snowflakeRows
type RawSnowflakeRows = snowflakeRows

// SnowflakeRestful exports snowflakeRestful
type SnowflakeRestful = snowflakeRestful

// SnowflakeValue exports snowflakeValue
type SnowflakeValue = snowflakeValue

// ChunkRowType exports chunkRowType
type ChunkRowType = chunkRowType

// SimpleTokenAccessor exports simpleTokenAccessor
type SimpleTokenAccessor = simpleTokenAccessor

// ArrowToValue exports arrowToValue
var ArrowToValue = arrowToValue

// GetExecResponse returns the ExecResponse
func (sr *snowflakeRows) GetExecResponse() *ExecResponse {
	return sr.execResp
}

// GetExecResponse returns the ExecResponse
func (sr *snowflakeResult) GetExecResponse() *ExecResponse {
	return sr.execResp
}

// Setter method for unit testing
func (sr *snowflakeRows) SetExecResponse(er *ExecResponse) {
	sr.execResp = er
}

// Setter method for unit testing
func (sr *snowflakeResult) SetExecResponse(er *ExecResponse) {
	sr.execResp = er
}

// StringToValue exports stringToValue
func StringToValue(dest *driver.Value, srcColumnMeta execResponseRowType, srcValue *string, loc *time.Location) error {
	return stringToValue(dest, srcColumnMeta, srcValue, loc)
}
