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
	"sync/atomic"
)

type snowflakeConn struct {
	cfg            *Config
	Rest           *snowflakeRestful
	SequeceCounter uint64
	QueryId        string
	SqlState       string
	RowType        []ExecResponseRowType
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

	if sc.Rest.Token != "" {
		headers[HeaderAuthorizationKey] = fmt.Sprintf(HeaderSnowflakeToken, sc.Rest.Token)
	}

	var json_body []byte
	json_body, err = json.Marshal(req)
	if err != nil {
		return nil, err
	}

	var data *ExecResponse
	data, err = sc.Rest.PostQuery(params, headers, json_body, sc.Rest.RequestTimeout)
	if err != nil {
		return nil, err
	}
	if !data.Success {
		log.Fatalln("Exec FAILED")
	} else {
		log.Println("Exec SUCCESS")
		sc.cfg.Database = data.Data.FinalDatabaseName
		sc.cfg.Schema = data.Data.FinalSchemaName
		sc.cfg.Role = data.Data.FinalRoleName
		sc.cfg.Warehouse = data.Data.FinalWarehouseName
		sc.QueryId = data.Data.QueryId
		sc.SqlState = data.Data.SqlState
		sc.RowType = data.Data.RowType
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
		insertId:     -1}, nil // TODO: is -1 is correct?
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
	if data.Data.Total == 0 {
		rows.rs.done = true
	}
	log.Printf("len: %d", data.Data.Total)
	log.Printf("data: %s", data.Data.RowSet)
	return nil, err
}
