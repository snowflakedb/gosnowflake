// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"encoding/json"
)

type ExecRequest struct {
	SqlText    string `json:"sqlText"`
	AsyncExec  bool `json:"asyncExec"`
	SequenceId uint64 `json:"sequenceId"`
	IsInternal bool `json:"isInternal"`
	Parameters map[string]string `json:"parameters,omitempty"`
}

type ExecResponseRowType struct {
	Name       string `json:"name"`
	ByteLength int64 `json:"byteLength"` // TODO: check type
	Length     int64 `json:"length"`     // TODO: check type
	Type       string `json:"type"`
	Scale      int64 `json:"scale"`
	Precision  int64 `json:"precision"`
	Nullable   bool `json:"nullable"`
}

type ExecResponseData struct {
	Parameters         json.RawMessage `json:"parameters"`
	RowType            []ExecResponseRowType `json:"rowtype"`
	RowSet             [][]string `json:"rowset"`
	Total              int64 `json:"total"`    // TODO check type
	Returned           int64 `json:"returned"` // TODO check type
	QueryId            string `json:"queryId"`
	SqlState           string `json:"sqlState"`
	DatabaseProvider   string `json:"databaseProvider"`
	FinalDatabaseName  string `json:"finalDatabaseName"`
	FinalSchemaName    string `json:"finalSchemaName"`
	FinalWarehouseName string `json:"finalWarehouseName"`
	FinalRoleName      string `json:"finalRoleName"`
	NumberOfBinds      int64 `json:"numberOfBinds"`    // TODO check type
	StatementTypeId    int64  `json:"statementTypeId"` // TODO check type
	Version            uint `json:"version"`           // TODO check type
}

type ExecResponse struct {
	Data    ExecResponseData `json:"Data"`
	Message string `json:"message"`
	Code    string `json:"code"` // TODO: check type
	Success bool `json:"success"`
}
