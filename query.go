// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"encoding/json"
)

type ExecBindParameter struct {
	Type  string `json:"type"`
	Value *string `json:"value"`
}

type ExecRequest struct {
	SqlText    string `json:"sqlText"`
	AsyncExec  bool `json:"asyncExec"`
	SequenceId uint64 `json:"sequenceId"`
	IsInternal bool `json:"isInternal"`
	Parameters map[string]string `json:"parameters,omitempty"`
	Bindings   map[string]ExecBindParameter `json:"bindings,omitempty"`
}
type ExecResponseRowType struct {
	Name       string `json:"name"`
	ByteLength int64 `json:"byteLength"` // TODO: check type
	Length     int64 `json:"length"`     // TODO: check type
	Type       string `json:"type"`
	Scale      int `json:"scale"`
	Precision  int `json:"precision"`
	Nullable   bool `json:"nullable"`
}

type ExecResponseChunk struct {
	Url      string `json:"url"`
	RowCount int `json:"rowCount"`
}

type ExecResponseData struct {
	Parameters         json.RawMessage `json:"parameters"`
	RowType            []ExecResponseRowType `json:"rowtype"`
	RowSet             [][]*string `json:"rowset"`
	Total              int64 `json:"total"`    // java:long
	Returned           int64 `json:"returned"` // java:long
	QueryId            string `json:"queryId"`
	SqlState           string `json:"sqlState"`
	DatabaseProvider   string `json:"databaseProvider"`
	FinalDatabaseName  string `json:"finalDatabaseName"`
	FinalSchemaName    string `json:"finalSchemaName"`
	FinalWarehouseName string `json:"finalWarehouseName"`
	FinalRoleName      string `json:"finalRoleName"`
	NumberOfBinds      int `json:"numberOfBinds"`      // java:int
	StatementTypeId    int64  `json:"statementTypeId"` // java:long
	Version            int64 `json:"version"`          // java:long
	Chunks             []ExecResponseChunk `json:"chunks,omitempty"`
	Qrmk               string `json:"qrmk,omitempty"`
}

type ExecResponse struct {
	Data    ExecResponseData `json:"Data"`
	Message string `json:"message"`
	Code    string `json:"code"`
	Success bool `json:"success"`
}
