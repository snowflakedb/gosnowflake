// Copyright (c) 2017-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	statementTypeIDMulti = int64(0x1000)

	statementTypeIDDml              = int64(0x3000)
	statementTypeIDInsert           = statementTypeIDDml + int64(0x100)
	statementTypeIDUpdate           = statementTypeIDDml + int64(0x200)
	statementTypeIDDelete           = statementTypeIDDml + int64(0x300)
	statementTypeIDMerge            = statementTypeIDDml + int64(0x400)
	statementTypeIDMultiTableInsert = statementTypeIDDml + int64(0x500)
)

const (
	sessionClientSessionKeepAlive          = "client_session_keep_alive"
	sessionClientValidateDefaultParameters = "CLIENT_VALIDATE_DEFAULT_PARAMETERS"
	sessionArrayBindStageThreshold         = "client_stage_array_binding_threshold"
	serviceName                            = "service_name"
)

type resultType string

const (
	snowflakeResultType paramKey   = "snowflakeResultType"
	execResultType      resultType = "exec"
	queryResultType     resultType = "query"
)

type snowflakeConn struct {
	ctx             context.Context
	cfg             *Config
	rest            *snowflakeRestful
	SequenceCounter uint64
	QueryID         string
	SQLState        string
}

// isDml returns true if the statement type code is in the range of DML.
func (sc *snowflakeConn) isDml(v int64) bool {
	return statementTypeIDDml <= v && v <= statementTypeIDMultiTableInsert
}

// isMultiStmt returns true if the statement type code is of type multistatement
// Note that the statement type code is also equivalent to type INSERT, so an additional check of the name is required
func (sc *snowflakeConn) isMultiStmt(data *execResponseData) bool {
	return data.StatementTypeID == statementTypeIDMulti && data.RowType[0].Name == "multiple statement execution"
}

func (sc *snowflakeConn) exec(
	ctx context.Context,
	query string,
	noResult bool,
	isInternal bool,
	describeOnly bool,
	bindings []driver.NamedValue) (
	*execResponse, error) {
	var err error
	counter := atomic.AddUint64(&sc.SequenceCounter, 1) // query sequence counter

	req := execRequest{
		SQLText:      query,
		AsyncExec:    noResult,
		Parameters:   map[string]interface{}{},
		IsInternal:   isInternal,
		DescribeOnly: describeOnly,
		SequenceID:   counter,
	}
	if key := ctx.Value(MultiStatementCount); key != nil {
		req.Parameters[string(MultiStatementCount)] = key
	}
	logger.WithContext(ctx).Infof("parameters: %v", req.Parameters)

	requestID := getOrGenerateRequestIDFromContext(ctx)
	uploader := bindUploader{
		sc:        sc,
		ctx:       ctx,
		stagePath: "@" + stageName + "/" + requestID.String(),
	}
	numBinds := uploader.arrayBindValueCount(bindings)
	var bindStagePath string
	arrayBindThreshold := sc.getArrayBindStageThreshold()
	if 0 < arrayBindThreshold && arrayBindThreshold <= numBinds &&
		!describeOnly && uploader.isArrayBind(bindings) {
		uploader.upload(bindings)
		bindStagePath = uploader.stagePath
	}

	if bindStagePath != "" {
		// bulk array inserts binding
		req.Bindings = nil
		req.BindStage = bindStagePath
	} else {
		// traditional binding
		req.Bindings, err = uploader.getBindValues(bindings)
		if err != nil {
			return nil, err
		}
		req.BindStage = ""
	}
	if numBinds > 0 {
		counter := 0
		if uploader.isArrayBind(bindings) {
			numRowsPrinted := maxBindingParamsForLogging / len(bindings)
			if numRowsPrinted <= 0 {
				numRowsPrinted = 1
			}
			for _, bind := range bindings {
				_, bindRows := snowflakeArrayToString(&bind)
				if numRowsPrinted >= len(bindRows) {
					numRowsPrinted = len(bindRows)
				}
				var rows strings.Builder
				rows.WriteString("[")
				for i := 0; i < numRowsPrinted; i++ {
					rows.WriteString(bindRows[i] + ", ")
				}
				rows.WriteString("]")
				logger.Infof("column: %v", rows.String())
				counter += numRowsPrinted
				if counter >= maxBindingParamsForLogging {
					break
				}
			}
		} else {
			for _, bind := range bindings {
				if counter >= maxBindingParamsForLogging {
					break
				}
				counter++
				logger.Infof("column %v: %v", bind.Name, bind.Value)
			}
		}
	}
	logger.WithContext(ctx).Infof("bindings: %v", req.Bindings)

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	if isFileTransfer(query) {
		headers["accept"] = headerContentTypeApplicationJSON
	}
	headers["User-Agent"] = userAgent
	if serviceName, ok := sc.cfg.Params[serviceName]; ok {
		headers["X-Snowflake-Service"] = *serviceName
	}

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	var data *execResponse
	data, err = sc.rest.FuncPostQuery(ctx, sc.rest, &url.Values{}, headers, jsonBody, sc.rest.RequestTimeout, requestID, sc.cfg)
	if err != nil {
		return data, err
	}
	var code int
	if data.Code != "" {
		code, err = strconv.Atoi(data.Code)
		if err != nil {
			code = -1
			return data, err
		}
	} else {
		code = -1
	}
	logger.WithContext(ctx).Infof("Success: %v, Code: %v", data.Success, code)
	if !data.Success {
		return nil, &SnowflakeError{
			Number:   code,
			SQLState: data.Data.SQLState,
			Message:  data.Message,
			QueryID:  data.Data.QueryID,
		}
	}
	logger.WithContext(ctx).Info("Exec/Query SUCCESS")
	sc.cfg.Database = data.Data.FinalDatabaseName
	sc.cfg.Schema = data.Data.FinalSchemaName
	sc.cfg.Role = data.Data.FinalRoleName
	sc.cfg.Warehouse = data.Data.FinalWarehouseName
	sc.QueryID = data.Data.QueryID
	sc.SQLState = data.Data.SQLState
	sc.populateSessionParameters(data.Data.Parameters)
	return data, err
}

func (sc *snowflakeConn) Begin() (driver.Tx, error) {
	return sc.BeginTx(sc.ctx, driver.TxOptions{})
}

func (sc *snowflakeConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	logger.WithContext(ctx).Info("BeginTx")
	if opts.ReadOnly {
		return nil, &SnowflakeError{
			Number:   ErrNoReadOnlyTransaction,
			SQLState: SQLStateFeatureNotSupported,
			Message:  errMsgNoReadOnlyTransaction,
		}
	}
	if int(opts.Isolation) != int(sql.LevelDefault) {
		return nil, &SnowflakeError{
			Number:   ErrNoDefaultTransactionIsolationLevel,
			SQLState: SQLStateFeatureNotSupported,
			Message:  errMsgNoDefaultTransactionIsolationLevel,
		}
	}
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	_, err := sc.exec(ctx, "BEGIN", false /* noResult */, false /* isInternal */, false /* describeOnly */, nil)
	if err != nil {
		return nil, err
	}
	return &snowflakeTx{sc}, err
}

func (sc *snowflakeConn) cleanup() {
	// must flush log buffer while the process is running.
	sc.rest = nil
	sc.cfg = nil
}

func (sc *snowflakeConn) Close() (err error) {
	logger.WithContext(sc.ctx).Infoln("Close")
	sc.stopHeartBeat()

	err = sc.rest.FuncCloseSession(sc.ctx, sc.rest, sc.rest.RequestTimeout)
	if err != nil {
		logger.Error(err)
	}
	sc.cleanup()
	return nil
}

func (sc *snowflakeConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	logger.WithContext(sc.ctx).Infoln("Prepare")
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	noResult, err := isAsyncMode(ctx)
	if err != nil {
		return nil, err
	}
	data, err := sc.exec(ctx, query, noResult, false /* isInternal */, true /* describeOnly */, []driver.NamedValue{})
	if err != nil {
		if data != nil {
			code, err := strconv.Atoi(data.Code)
			if err != nil {
				return nil, err
			}
			return nil, &SnowflakeError{
				Number:   code,
				SQLState: data.Data.SQLState,
				Message:  err.Error(),
				QueryID:  data.Data.QueryID,
			}
		}
		return nil, err
	}
	stmt := &snowflakeStmt{
		sc:    sc,
		query: query,
	}
	return stmt, nil
}

func (sc *snowflakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	logger.WithContext(ctx).Infof("Exec: %#v, %v", query, args)
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	noResult, err := isAsyncMode(ctx)
	if err != nil {
		return nil, err
	}
	// TODO handle isInternal
	ctx = setResultType(ctx, execResultType)
	data, err := sc.exec(ctx, query, noResult, false /* isInternal */, false /* describeOnly */, args)
	if err != nil {
		logger.WithContext(ctx).Infof("error: %v", err)
		if data != nil {
			code, err := strconv.Atoi(data.Code)
			if err != nil {
				return nil, err
			}
			return nil, &SnowflakeError{
				Number:   code,
				SQLState: data.Data.SQLState,
				Message:  err.Error(),
				QueryID:  data.Data.QueryID}
		}
		return nil, err
	}

	// if async exec, return result object right away
	if noResult {
		return data.Data.AsyncResult, nil
	}

	if sc.isDml(data.Data.StatementTypeID) {
		// collects all values from the returned row sets
		updatedRows, err := updateRows(data.Data)
		if err != nil {
			return nil, err
		}
		logger.WithContext(ctx).Debugf("number of updated rows: %#v", updatedRows)
		return &snowflakeResult{
			affectedRows: updatedRows,
			insertID:     -1,
			queryID:      sc.QueryID,
		}, nil // last insert id is not supported by Snowflake
	} else if sc.isMultiStmt(&data.Data) {
		return sc.handleMultiExec(ctx, data.Data)
	}
	logger.Debug("DDL")
	return driver.ResultNoRows, nil
}

func (sc *snowflakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	logger.WithContext(ctx).Infof("Query: %#v, %v", query, args)
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}

	noResult, err := isAsyncMode(ctx)
	if err != nil {
		return nil, err
	}
	ctx = setResultType(ctx, queryResultType)
	// TODO: handle isInternal
	data, err := sc.exec(ctx, query, noResult, false /* isInternal */, false /* describeOnly */, args)
	if err != nil {
		logger.WithContext(ctx).Errorf("error: %v", err)
		if data != nil {
			code, err := strconv.Atoi(data.Code)
			if err != nil {
				return nil, err
			}
			return nil, &SnowflakeError{
				Number:   code,
				SQLState: data.Data.SQLState,
				Message:  err.Error(),
				QueryID:  data.Data.QueryID}
		}
		return nil, err
	}

	// if async query, return row object right away
	if noResult {
		return data.Data.AsyncRows, nil
	}

	rows := new(snowflakeRows)
	rows.sc = sc
	rows.ChunkDownloader = populateChunkDownloader(ctx, sc, data.Data)
	rows.queryID = sc.QueryID

	if sc.isMultiStmt(&data.Data) {
		err := sc.handleMultiQuery(ctx, data.Data, rows)
		if err != nil {
			return nil, err
		}
	}

	rows.ChunkDownloader.start()
	return rows, err
}

func (sc *snowflakeConn) Prepare(query string) (driver.Stmt, error) {
	return sc.PrepareContext(sc.ctx, query)
}

func (sc *snowflakeConn) Exec(
	query string,
	args []driver.Value) (
	driver.Result, error) {
	return sc.ExecContext(sc.ctx, query, toNamedValues(args))
}

func (sc *snowflakeConn) Query(
	query string,
	args []driver.Value) (
	driver.Rows, error) {
	return sc.QueryContext(sc.ctx, query, toNamedValues(args))
}

func (sc *snowflakeConn) Ping(ctx context.Context) error {
	logger.WithContext(ctx).Infoln("Ping")
	if sc.rest == nil {
		return driver.ErrBadConn
	}
	noResult, err := isAsyncMode(ctx)
	if err != nil {
		return err
	}
	// TODO: handle isInternal
	_, err = sc.exec(ctx, "SELECT 1", noResult, false /* isInternal */, false /* describeOnly */, []driver.NamedValue{})
	return err
}

// CheckNamedValue determines which types are handled by this driver aside from
// the instances captured by driver.Value
func (sc *snowflakeConn) CheckNamedValue(nv *driver.NamedValue) error {
	if supported := supportedArrayBind(nv); !supported {
		return driver.ErrSkip
	}
	return nil
}

func (sc *snowflakeConn) populateSessionParameters(parameters []nameValueParameter) {
	// other session parameters (not all)
	logger.WithContext(sc.ctx).Infof("params: %#v", parameters)
	for _, param := range parameters {
		v := ""
		switch param.Value.(type) {
		case int64:
			if vv, ok := param.Value.(int64); ok {
				v = strconv.FormatInt(vv, 10)
			}
		case float64:
			if vv, ok := param.Value.(float64); ok {
				v = strconv.FormatFloat(vv, 'g', -1, 64)
			}
		case bool:
			if vv, ok := param.Value.(bool); ok {
				v = strconv.FormatBool(vv)
			}
		default:
			if vv, ok := param.Value.(string); ok {
				v = vv
			}
		}
		logger.Debugf("parameter. name: %v, value: %v", param.Name, v)
		sc.cfg.Params[strings.ToLower(param.Name)] = &v
	}
}

func (sc *snowflakeConn) isClientSessionKeepAliveEnabled() bool {
	v, ok := sc.cfg.Params[sessionClientSessionKeepAlive]
	if !ok {
		return false
	}
	return strings.Compare(*v, "true") == 0
}

func (sc *snowflakeConn) getArrayBindStageThreshold() int {
	v, ok := sc.cfg.Params[sessionArrayBindStageThreshold]
	if !ok {
		return 0
	}
	num, err := strconv.Atoi(*v)
	if err != nil {
		return 0
	}
	return num
}

func (sc *snowflakeConn) startHeartBeat() {
	if !sc.isClientSessionKeepAliveEnabled() {
		return
	}
	sc.rest.HeartBeat = &heartbeat{
		restful: sc.rest,
	}
	sc.rest.HeartBeat.start()
}

func (sc *snowflakeConn) stopHeartBeat() {
	if !sc.isClientSessionKeepAliveEnabled() {
		return
	}
	sc.rest.HeartBeat.stop()
}

func (sc *snowflakeConn) handleMultiExec(ctx context.Context, data execResponseData) (driver.Result, error) {
	var updatedRows int64
	childResults := getChildResults(data.ResultIDs, data.ResultTypes)
	for _, child := range childResults {
		resultPath := fmt.Sprintf("/queries/%s/result", child.id)
		childData, err := sc.getQueryResult(ctx, resultPath)
		if err != nil {
			logger.Errorf("error: %v", err)
			code, err := strconv.Atoi(childData.Code)
			if err != nil {
				return nil, err
			}
			if childData != nil {
				return nil, &SnowflakeError{
					Number:   code,
					SQLState: childData.Data.SQLState,
					Message:  err.Error(),
					QueryID:  childData.Data.QueryID}
			}
			return nil, err
		}
		if sc.isDml(childData.Data.StatementTypeID) {
			count, err := updateRows(childData.Data)
			if err != nil {
				logger.WithContext(ctx).Errorf("error: %v", err)
				if childData != nil {
					code, err := strconv.Atoi(childData.Code)
					if err != nil {
						return nil, err
					}
					return nil, &SnowflakeError{
						Number:   code,
						SQLState: childData.Data.SQLState,
						Message:  err.Error(),
						QueryID:  childData.Data.QueryID}
				}
				return nil, err
			}
			updatedRows += count
		}
	}
	logger.WithContext(ctx).Infof("number of updated rows: %#v", updatedRows)
	return &snowflakeResult{
		affectedRows: updatedRows,
		insertID:     -1,
		queryID:      sc.QueryID,
	}, nil
}

func (sc *snowflakeConn) handleMultiQuery(ctx context.Context, data execResponseData, rows *snowflakeRows) error {
	childResults := getChildResults(data.ResultIDs, data.ResultTypes)
	var nextChunkDownloader chunkDownloader
	firstResultSet := false

	for _, child := range childResults {
		resultPath := fmt.Sprintf("/queries/%s/result", child.id)
		childData, err := sc.getQueryResult(ctx, resultPath)
		if err != nil {
			logger.WithContext(ctx).Errorf("error: %v", err)
			if childData != nil {
				code, err := strconv.Atoi(childData.Code)
				if err != nil {
					return err
				}
				return &SnowflakeError{
					Number:   code,
					SQLState: childData.Data.SQLState,
					Message:  err.Error(),
					QueryID:  childData.Data.QueryID}
			}
			return err
		}
		if !firstResultSet {
			// populate rows.ChunkDownloader with the first child
			rows.ChunkDownloader = populateChunkDownloader(ctx, sc, childData.Data)
			nextChunkDownloader = rows.ChunkDownloader
			firstResultSet = true
		} else {
			nextChunkDownloader.setNextChunkDownloader(populateChunkDownloader(ctx, sc, childData.Data))
			nextChunkDownloader = nextChunkDownloader.getNextChunkDownloader()
		}
	}
	return nil
}

func setResultType(ctx context.Context, resType resultType) context.Context {
	return context.WithValue(ctx, snowflakeResultType, resType)
}

func getResultType(ctx context.Context) resultType {
	return ctx.Value(snowflakeResultType).(resultType)
}

func updateRows(data execResponseData) (int64, error) {
	var count int64
	for i, n := 0, len(data.RowType); i < n; i++ {
		v, err := strconv.ParseInt(*data.RowSet[0][i], 10, 64)
		if err != nil {
			return -1, err
		}
		count += v
	}
	return count, nil
}

type childResult struct {
	id  string
	typ string
}

func getChildResults(IDs string, types string) []childResult {
	if IDs == "" {
		return nil
	}
	queryIDs := strings.Split(IDs, ",")
	resultTypes := strings.Split(types, ",")
	res := make([]childResult, len(queryIDs))
	for i, id := range queryIDs {
		res[i] = childResult{id, resultTypes[i]}
	}
	return res
}

func (sc *snowflakeConn) getQueryResult(ctx context.Context, resultPath string) (*execResponse, error) {
	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	headers["User-Agent"] = userAgent
	if serviceName, ok := sc.cfg.Params[serviceName]; ok {
		headers["X-Snowflake-Service"] = *serviceName
	}
	param := make(url.Values)
	param.Add(requestIDKey, getOrGenerateRequestIDFromContext(ctx).String())
	param.Add("clientStartTime", strconv.FormatInt(time.Now().Unix(), 10))
	param.Add(requestGUIDKey, uuid.New().String())
	token, _, _ := sc.rest.TokenAccessor.GetTokens()
	if token != "" {
		headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, token)
	}
	url := sc.rest.getFullURL(resultPath, &param)
	res, err := sc.rest.FuncGet(ctx, sc.rest, url, headers, sc.rest.RequestTimeout)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to get response. err: %v", err)
		return nil, err
	}
	var respd *execResponse
	err = json.NewDecoder(res.Body).Decode(&respd)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
		return nil, err
	}
	return respd, nil
}

func isAsyncMode(ctx context.Context) (bool, error) {
	val := ctx.Value(asyncMode)
	if val == nil {
		return false, nil
	}
	boolVal, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("failed to cast val %+v to bool", val)
	}
	return boolVal, nil
}

func getAsync(
	ctx context.Context,
	sr *snowflakeRestful,
	headers map[string]string,
	URL *url.URL,
	timeout time.Duration,
	res *snowflakeResult,
	rows *snowflakeRows,
	cfg *Config,
) {
	resType := getResultType(ctx)
	var errChannel chan error
	sfError := &SnowflakeError{
		Number: -1,
	}
	if resType == execResultType {
		errChannel = res.errChannel
		sfError.QueryID = res.queryID
	} else {
		errChannel = rows.errChannel
		sfError.QueryID = rows.queryID
	}
	defer close(errChannel)
	token, _, _ := sr.TokenAccessor.GetTokens()
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, token)
	resp, err := sr.FuncGet(ctx, sr, URL, headers, timeout)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to get response. err: %v", err)
		sfError.Message = err.Error()
		errChannel <- sfError
		close(errChannel)
		return
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	respd := execResponse{}
	err = json.NewDecoder(resp.Body).Decode(&respd)
	resp.Body.Close()
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
		sfError.Message = err.Error()
		errChannel <- sfError
		close(errChannel)
		return
	}

	sc := &snowflakeConn{rest: sr, cfg: cfg}
	if respd.Success {
		if resType == execResultType {
			res.insertID = -1
			if sc.isDml(respd.Data.StatementTypeID) {
				res.affectedRows, _ = updateRows(respd.Data)
			} else if sc.isMultiStmt(&respd.Data) {
				r, err := sc.handleMultiExec(ctx, respd.Data)
				if err != nil {
					res.errChannel <- err
					close(errChannel)
					return
				}
				res.affectedRows, err = r.RowsAffected()
				if err != nil {
					res.errChannel <- err
					close(errChannel)
					return
				}
			}
			res.queryID = respd.Data.QueryID
			res.errChannel <- nil // mark exec status complete
		} else {
			rows.sc = sc
			rows.ChunkDownloader = populateChunkDownloader(ctx, sc, respd.Data)
			rows.queryID = respd.Data.QueryID
			if sc.isMultiStmt(&respd.Data) {
				err = sc.handleMultiQuery(ctx, respd.Data, rows)
				if err != nil {
					rows.errChannel <- err
					close(errChannel)
					return
				}
			}
			rows.ChunkDownloader.start()
			rows.errChannel <- nil // mark query status complete
		}
	} else {
		var code int
		if respd.Code != "" {
			code, err = strconv.Atoi(respd.Code)
			if err != nil {
				code = -1
			}
		} else {
			code = -1
		}
		errChannel <- &SnowflakeError{
			Number:   code,
			SQLState: respd.Data.SQLState,
			Message:  respd.Message,
			QueryID:  respd.Data.QueryID,
		}
	}
}

func getQueryIDChan(ctx context.Context) chan<- string {
	v := ctx.Value(QueryIDChan)
	if v == nil {
		return nil
	}
	c, _ := v.(chan<- string)
	return c
}

// returns snowflake chunk downloader by default or stream based chunk
// downloader if option provided through context
func populateChunkDownloader(ctx context.Context, sc *snowflakeConn, data execResponseData) chunkDownloader {
	if useStreamDownloader(ctx) {
		fetcher := &httpStreamChunkFetcher{
			ctx:      ctx,
			client:   sc.rest.Client,
			clientIP: sc.cfg.ClientIP,
			headers:  data.ChunkHeaders,
			qrmk:     data.Qrmk,
		}
		return newStreamChunkDownloader(ctx, fetcher, data.Total, data.RowType, data.RowSet, data.Chunks)
	}

	return &snowflakeChunkDownloader{
		sc:                 sc,
		ctx:                ctx,
		CurrentChunk:       make([]chunkRowType, len(data.RowSet)),
		ChunkMetas:         data.Chunks,
		Total:              data.Total,
		TotalRowIndex:      int64(-1),
		CellCount:          len(data.RowType),
		Qrmk:               data.Qrmk,
		QueryResultFormat:  data.QueryResultFormat,
		ChunkHeader:        data.ChunkHeaders,
		FuncDownload:       downloadChunk,
		FuncDownloadHelper: downloadChunkHelper,
		FuncGet:            getChunk,
		RowSet: rowSetType{
			RowType:      data.RowType,
			JSON:         data.RowSet,
			RowSetBase64: data.RowSetBase64,
		},
	}
}
