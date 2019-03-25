package gosnowflake

// This file just exports types, functions and methods as needed

// Types
type ExecResponse = execResponse
type ExecResponseRowType = execResponseRowType
type ExecResponseChunk = execResponseChunk
type SnowflakeRows = snowflakeRows

// Methods
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
