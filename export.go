package gosnowflake

// This file just exports types, functions and methods as needed

import (
	"database/sql/driver"
	"time"
)

// Types
type ExecResponse = execResponse
type ExecResponseRowType = execResponseRowType
type ExecResponseChunk = execResponseChunk
type SnowflakeRows = snowflakeRows
type SnowflakeRestful = snowflakeRestful
type SnowflakeValue = snowflakeValue
type ChunkRowType = chunkRowType
type SimpleTokenAccessor = simpleTokenAccessor

// Methods

var ArrowToValue = arrowToValue

func (sr *snowflakeRows) GetExecResponse() *ExecResponse {
	return sr.execResp
}

func (sr *snowflakeResult) GetExecResponse() *ExecResponse {
	return sr.execResp
}

// Setter methods for unit testing
func (sr *snowflakeRows) SetExecResponse(er *ExecResponse) {
	sr.execResp = er
}

func (sr *snowflakeResult) SetExecResponse(er *ExecResponse) {
	sr.execResp = er
}

func StringToValue(dest *driver.Value, srcColumnMeta execResponseRowType, srcValue *string, loc *time.Location) error {
	return stringToValue(dest, srcColumnMeta, srcValue, loc)
}
