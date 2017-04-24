// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"encoding/json"
)

type execBindParameter struct {
	Type  string `json:"type"`
	Value *string `json:"value"`
}

type ExecRequest struct {
	SQLText    string `json:"sqlText"`
	AsyncExec  bool `json:"asyncExec"`
	SequenceID uint64 `json:"sequenceId"`
	IsInternal bool `json:"isInternal"`
	Parameters map[string]string `json:"parameters,omitempty"`
	Bindings   map[string]execBindParameter `json:"bindings,omitempty"`
}
type execResponseRowType struct {
	Name       string `json:"name"`
	ByteLength int64 `json:"byteLength"` // TODO: check type
	Length     int64 `json:"length"`     // TODO: check type
	Type       string `json:"type"`
	Scale      int `json:"scale"`
	Precision  int `json:"precision"`
	Nullable   bool `json:"nullable"`
}

type execResponseChunk struct {
	URL      string `json:"url"`
	RowCount int `json:"rowCount"`
}

type ExecResponseData struct {
	Parameters         json.RawMessage `json:"parameters"`
	RowType            []execResponseRowType `json:"rowtype"`
	RowSet             [][]*string `json:"rowset"`
	Total              int64 `json:"total"`    // java:long
	Returned           int64 `json:"returned"` // java:long
	QueryID            string `json:"queryId"`
	SQLState           string `json:"sqlState"`
	DatabaseProvider   string `json:"databaseProvider"`
	FinalDatabaseName  string `json:"finalDatabaseName"`
	FinalSchemaName    string `json:"finalSchemaName"`
	FinalWarehouseName string `json:"finalWarehouseName"`
	FinalRoleName      string `json:"finalRoleName"`
	NumberOfBinds      int `json:"numberOfBinds"`      // java:int
	StatementTypeID    int64  `json:"statementTypeId"` // java:long
	Version            int64 `json:"version"`          // java:long
	Chunks             []execResponseChunk `json:"chunks,omitempty"`
	Qrmk               string `json:"qrmk,omitempty"`
}

type ExecResponse struct {
	Data    ExecResponseData `json:"Data"`
	Message string `json:"message"`
	Code    string `json:"code"`
	Success bool `json:"success"`
}
