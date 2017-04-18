// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//

package gosnowflake

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/satori/go.uuid"
	"log"
	"net/url"
	"strconv"
	"sync/atomic"
)

type snowflakeConn struct {
	cfg            *Config
	rest           *snowflakeRestful
	SequeceCounter uint64
	QueryId        string
	SqlState       string
}

func (sc *snowflakeConn) exec(
  query string, noResult bool, isInternal bool, parameters map[string]string) (*ExecResponse, error) {
	var err error
	counter := atomic.AddUint64(&sc.SequeceCounter, 1)

	req := ExecRequest{
		SqlText:    query,
		AsyncExec:  noResult,
		SequenceId: counter,
	}
	req.IsInternal = isInternal
	if len(parameters) > 0 {
		req.Parameters = parameters
	}
	params := &url.Values{}
	params.Add("requestId", uuid.NewV4().String())

	headers := make(map[string]string)
	headers["Content-Type"] = ContentTypeApplicationJson
	headers["accept"] = AcceptTypeAppliationSnowflake // TODO: change to JSON in case of PUT/GET
	headers["User-Agent"] = UserAgent

	if sc.rest.Token != "" {
		headers[HeaderAuthorizationKey] = fmt.Sprintf(HeaderSnowflakeToken, sc.rest.Token)
	}

	var json_body []byte
	json_body, err = json.Marshal(req)
	if err != nil {
		return nil, err
	}

	var data *ExecResponse
	data, err = sc.rest.PostQuery(params, headers, json_body, sc.rest.RequestTimeout)
	if err != nil {
		return nil, err
	}
	if !data.Success {
		errno, err := strconv.Atoi(data.Code)
		if err != nil {
			errno = -1
		}
		return nil, &SnowflakeError{
			Number:   errno,
			SqlState: data.Data.SqlState,
			Message:  data.Message,
			QueryId:  data.Data.QueryId,
		}
	} else {
		log.Println("Exec SUCCESS")
		sc.cfg.Database = data.Data.FinalDatabaseName
		sc.cfg.Schema = data.Data.FinalSchemaName
		sc.cfg.Role = data.Data.FinalRoleName
		sc.cfg.Warehouse = data.Data.FinalWarehouseName
		sc.QueryId = data.Data.QueryId
		sc.SqlState = data.Data.SqlState
	}
	return data, err
}

func (sc *snowflakeConn) Begin() (driver.Tx, error) {
	log.Println("Begin")
	return nil, nil
}
func (sc *snowflakeConn) Close() (err error) {
	log.Println("Close")
	return nil
}
func (sc *snowflakeConn) Prepare(query string) (driver.Stmt, error) {
	log.Println("Prepare")
	return nil, nil
}
func (sc *snowflakeConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	log.Printf("Exec: %s, %s", query, args)
	// TODO: Binding
	parameters := make(map[string]string)
	// TODO: handle noResult and isInternal
	data, err := sc.exec(query, false, false, parameters)
	if err != nil {
		return nil, err
	}
	return &snowflakeResult{
		affectedRows: data.Data.Returned,
		insertId:     -1}, nil // TODO: is -1 is appropriate?
}

func (sc *snowflakeConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	log.Println("Query")
	parameters := make(map[string]string)
	// TODO: handle noResult and isInternal
	data, err := sc.exec(query, false, false, parameters)
	if err != nil {
		return nil, err
	}

	rows := new(snowflakeRows)
	rows.sc = sc
	rows.RowType = data.Data.RowType
	rows.Total = int64(data.Data.Total)
	rows.TotalRowIndex = int64(-1)
	rows.CurrentRowSet = data.Data.RowSet
	rows.CurrentIndex = -1
	rows.CurrentRowCount = len(rows.CurrentRowSet)
	return rows, err
}
