// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"database/sql/driver"
	"encoding/json"
	"net/url"
	"strconv"
	"sync/atomic"

	"context"

	"github.com/golang/glog"
)

type snowflakeConn struct {
	cfg            *Config
	rest           *snowflakeRestful
	SequeceCounter uint64
	QueryID        string
	SQLState       string
}

func (sc *snowflakeConn) isDml(v int64) bool {
	switch v {
	case statementTypeIDDml, statementTypeIDInsert,
		statementTypeIDUpdate, statementTypeIDDelete,
		statementTypeIDMerge, statementTypeIDMultiTableInsert:
		return true
	}
	return false
}

func (sc *snowflakeConn) exec(
	ctx context.Context,
	query string, noResult bool, isInternal bool, parameters []driver.NamedValue) (*execResponse, error) {
	var err error
	counter := atomic.AddUint64(&sc.SequeceCounter, 1)

	req := execRequest{
		SQLText:    query,
		AsyncExec:  noResult,
		SequenceID: counter,
	}
	req.IsInternal = isInternal
	if len(parameters) > 0 {
		req.Bindings = make(map[string]execBindParameter, len(parameters))
		for i, n := 0, len(parameters); i < n; i++ {
			v1, err := valueToString(parameters[i].Value)
			if err != nil {
				return nil, err
			}
			req.Bindings[strconv.Itoa(parameters[i].Ordinal)] = execBindParameter{
				Type:  goTypeToSnowflake(parameters[i].Value),
				Value: v1,
			}
		}
	}
	params := &url.Values{} // TODO: delete?

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeAppliationSnowflake // TODO: change to JSON in case of PUT/GET
	headers["User-Agent"] = UserAgent

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	var data *execResponse
	data, err = sc.rest.PostQuery(ctx, params, headers, jsonBody, sc.rest.RequestTimeout)
	if err != nil {
		return nil, err
	}
	var code int
	if data.Code != "" {
		code, err = strconv.Atoi(data.Code)
		if err != nil {
			code = -1
			return nil, err
		}
	} else {
		code = -1
	}
	glog.V(2).Infof("Success: %v, Code: %v", data.Success, code)
	if !data.Success {
		return nil, &SnowflakeError{
			Number:   code,
			SQLState: data.Data.SQLState,
			Message:  data.Message,
			QueryID:  data.Data.QueryID,
		}
	}
	glog.V(2).Info("Exec/Query SUCCESS")
	sc.cfg.Database = data.Data.FinalDatabaseName
	sc.cfg.Schema = data.Data.FinalSchemaName
	sc.cfg.Role = data.Data.FinalRoleName
	sc.cfg.Warehouse = data.Data.FinalWarehouseName
	sc.QueryID = data.Data.QueryID
	sc.SQLState = data.Data.SQLState
	return data, err
}

func (sc *snowflakeConn) Begin() (driver.Tx, error) {
	glog.V(2).Info("Begin")
	_, err := sc.exec(context.TODO(), "BEGIN", false, false, nil)
	if err != err {
		return nil, err
	}
	return &snowflakeTx{sc}, err
}
func (sc *snowflakeConn) Close() (err error) {
	glog.V(2).Infoln("Close")
	err = sc.rest.closeSession()
	if err != nil {
		glog.Warning(err)
	}
	glog.Flush() // must flush log buffer while the process is running.
	return nil
}
func (sc *snowflakeConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	glog.V(2).Infoln("Prepare")
	stmt := &snowflakeStmt{
		sc:    sc,
		query: query,
	}
	return stmt, nil
}

func (sc *snowflakeConn) Prepare(query string) (driver.Stmt, error) {
	return sc.PrepareContext(context.TODO(), query)
}

func (sc *snowflakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	glog.V(2).Infof("Exec: %#v, %v", query, args)
	// TODO: handle noResult and isInternal
	data, err := sc.exec(ctx, query, false, false, args)
	if err != nil {
		return nil, err
	}
	var updatedRows int64
	if sc.isDml(data.Data.StatementTypeID) {
		// collects all values from the returned row sets
		updatedRows = 0
		for i, n := 0, len(data.Data.RowType); i < n; i++ {
			v, err := strconv.ParseInt(*data.Data.RowSet[0][i], 10, 64)
			if err != nil {
				return nil, err
			}
			updatedRows += v
		}
	}
	glog.V(2).Infof("number of updated rows: %#v", updatedRows)
	return &snowflakeResult{
		affectedRows: updatedRows,
		insertID:     -1}, nil // last insert id is not supported by Snowflake
}

func (sc *snowflakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	glog.V(2).Infoln("Query")
	// TODO: handle noResult and isInternal
	data, err := sc.exec(ctx, query, false, false, args)
	if err != nil {
		glog.V(2).Infof("You got error: %v", err)
		return nil, err
	}

	rows := new(snowflakeRows)
	rows.sc = sc
	rows.RowType = data.Data.RowType
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		CurrentChunk:  data.Data.RowSet,
		ChunkMetas:    data.Data.Chunks,
		Total:         int64(data.Data.Total),
		TotalRowIndex: int64(-1),
		Qrmk:          data.Data.Qrmk,
	}
	rows.ChunkDownloader.Start(ctx)
	return rows, err
}

func (sc *snowflakeConn) Exec(
	query string,
	args []driver.Value) (
	driver.Result, error) {
	return sc.ExecContext(context.TODO(), query, toNamedValues(args))
}

func (sc *snowflakeConn) Query(
	query string,
	args []driver.Value) (
	driver.Rows, error) {
	return sc.QueryContext(context.TODO(), query, toNamedValues(args))
}
