// Copyright (c) 2017-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	httpHeaderContentType      = "Content-Type"
	httpHeaderAccept           = "accept"
	httpHeaderUserAgent        = "User-Agent"
	httpHeaderServiceName      = "X-Snowflake-Service"
	httpHeaderContentLength    = "Content-Length"
	httpHeaderHost             = "Host"
	httpHeaderValueOctetStream = "application/octet-stream"
	httpHeaderContentEncoding  = "Content-Encoding"
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
	snowflakeResultType contextKey = "snowflakeResultType"
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
	internal        InternalClient
}

var queryIDPattern = `[\w\-_]+`
var queryIDRegexp = regexp.MustCompile(queryIDPattern)

const (
	urlQueriesResultFmt string = "/queries/%s/result"
)

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
	if key := ctx.Value(multiStatementCount); key != nil {
		req.Parameters[string(multiStatementCount)] = key
	}
	logger.WithContext(ctx).Infof("parameters: %v", req.Parameters)

	requestID := getOrGenerateRequestIDFromContext(ctx)
	if len(bindings) > 0 {
		arrayBindThreshold := sc.getArrayBindStageThreshold()
		numBinds := arrayBindValueCount(bindings)
		if 0 < arrayBindThreshold && arrayBindThreshold <= numBinds && !describeOnly && isArrayBind(bindings) {
			// bulk array insert binding
			uploader := bindUploader{
				sc:        sc,
				ctx:       ctx,
				stagePath: "@" + bindStageName + "/" + requestID.String(),
			}
			uploader.upload(bindings)
			req.Bindings = nil
			req.BindStage = uploader.stagePath
		} else {
			// variable or array binding
			req.Bindings, err = getBindValues(bindings)
			if err != nil {
				return nil, err
			}
			req.BindStage = ""
		}
	}
	logger.WithContext(ctx).Infof("bindings: %v", req.Bindings)

	headers := getHeaders()
	if isFileTransfer(query) {
		headers[httpHeaderAccept] = headerContentTypeApplicationJSON
	}
	if serviceName, ok := sc.cfg.Params[serviceName]; ok {
		headers[httpHeaderServiceName] = *serviceName
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
	if isFileTransfer(query) {
		sfa := snowflakeFileTransferAgent{
			sc:      sc,
			data:    &data.Data,
			command: query,
			options: new(SnowflakeFileTransferOptions),
		}
		if fs := getFileStream(ctx); fs != nil {
			sfa.sourceStream = fs
			if isInternal {
				sfa.data.AutoCompress = false
			}
		}
		if op := getFileTransferOptions(ctx); op != nil {
			sfa.options = op
		}
		if sfa.options.multiPartThreshold == 0 {
			sfa.options.multiPartThreshold = dataSizeThreshold
		}
		if err = sfa.execute(); err != nil {
			return nil, err
		}
		data, err = sfa.result()
		if err != nil {
			return nil, err
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

	if !sc.cfg.KeepSessionAlive {
		err = sc.rest.FuncCloseSession(sc.ctx, sc.rest, sc.rest.RequestTimeout)
		if err != nil {
			logger.Error(err)
		}
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
	qid, err := getResumeQueryID(ctx)
	if err != nil {
		return nil, err
	}
	if qid == "" {
		return sc.queryContextInternal(ctx, query, args)
	}

	// first we will check the status of this particular query to find out if there is result to fetch
	err = sc.checkQueryStatus(ctx, qid)
	if err == nil || (err != nil && err.(*SnowflakeError).Number == ErrQueryIsRunning) {
		// the query is running. Rows object will be returned from here.
		return sc.buildRowsForRunningQuery(ctx, qid)
	}

	return nil, err
}

func (sc *snowflakeConn) queryContextInternal(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
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
	rows.queryID = sc.QueryID

	if sc.isMultiStmt(&data.Data) {
		// handleMultiQuery is responsible to fill rows with childResults
		err := sc.handleMultiQuery(ctx, data.Data, rows)
		if err != nil {
			return nil, err
		}
	} else {
		rows.addDownloader(populateChunkDownloader(ctx, sc, data.Data))
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
		resultPath := fmt.Sprintf(urlQueriesResultFmt, child.id)
		childData, err := sc.getQueryResultResp(ctx, resultPath)
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

// Fill the correspondent rows and add chunk downloader into the rows when iterate the childResults
func (sc *snowflakeConn) handleMultiQuery(ctx context.Context, data execResponseData, rows *snowflakeRows) error {
	childResults := getChildResults(data.ResultIDs, data.ResultTypes)

	for _, child := range childResults {
		err := sc.rowsForRunningQuery(ctx, child.id, rows)
		if err != nil {
			return err
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

func (sc *snowflakeConn) getQueryResultResp(ctx context.Context, resultPath string) (*execResponse, error) {
	headers := getHeaders()
	if serviceName, ok := sc.cfg.Params[serviceName]; ok {
		headers[httpHeaderServiceName] = *serviceName
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

// checkQueryStatus return error==nil means the query completed successfully and there is complete query result to fetch.
// when the GS could not return a status (when a query was just submitted GS might not be able to return a status)
// an ErrQueryStatus will be returned.
// Other than error==nil, There are three error types will be returned:
// 1. ErrQueryStatus, when GS could not return any status or report error due to any reason - connection,
// permission, etc.
// 2, ErrQueryReportedError, if the requested query was terminated, aborted, and GS returned
// an error status included in query.sfqueryStatusError
// 3, ErrQueryIsRunning, if the requested query is still running and might have complete result later, these statuses
// were listed in query.sfqueryStatusRunning
func (sc *snowflakeConn) checkQueryStatus(ctx context.Context, qid string) error {
	headers := make(map[string]string)
	param := make(url.Values)
	param.Add(requestGUIDKey, uuid.New().String())
	if tok, _, _ := sc.rest.TokenAccessor.GetTokens(); tok != "" {
		headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, tok)
	}
	resultPath := fmt.Sprintf("/monitoring/queries/%s", qid)
	url := sc.rest.getFullURL(resultPath, &param)

	res, err := sc.rest.FuncGet(ctx, sc.rest, url, headers, sc.rest.RequestTimeout)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to get response. err: %v", err)
		return err
	}
	var statusResp = statusResponse{}

	err = json.NewDecoder(res.Body).Decode(&statusResp)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
		return err
	}

	if !statusResp.Success || len(statusResp.Data.Queries) == 0 {
		logger.WithContext(ctx).Errorf("status query returned not-success or no status returned.")
		return &SnowflakeError{
			Number:  ErrQueryStatus,
			Message: "status query returned not-success or no status returned. Please retry"}
	}

	var queryRet = statusResp.Data.Queries[0]
	if queryRet.ErrorCode != 0 {
		return &SnowflakeError{
			Number: ErrQueryStatus,
			Message: fmt.Sprintf("server ErrorCode=%d, ErrorMessage=%s",
				queryRet.ErrorCode, queryRet.ErrorMessage),
			IncludeQueryID: true,
			QueryID:        qid,
		}
	}

	// returned errorCode is 0. Now check what is the returned status of the query.
	var qstatus = strToSFQueryStatus(queryRet.Status)
	if sfqStatusIsAnError(qstatus) {
		return &SnowflakeError{
			Number: ErrQueryReportedError,
			Message: fmt.Sprintf("%s: status from server: [%s]",
				queryRet.ErrorMessage, queryRet.Status),
			IncludeQueryID: true,
			QueryID:        qid,
		}
	}

	if sfqStatusIsStillRunning(qstatus) {
		return &SnowflakeError{
			Number: ErrQueryIsRunning,
			Message: fmt.Sprintf("%s: status from server: [%s]",
				queryRet.ErrorMessage, queryRet.Status),
			IncludeQueryID: true,
			QueryID:        qid,
		}
	}

	//success
	return nil
}

// Fetch query result for a query id from /queries/<qid>/result endpoint.
func (sc *snowflakeConn) rowsForRunningQuery(ctx context.Context, qid string, rows *snowflakeRows) error {
	resultPath := fmt.Sprintf(urlQueriesResultFmt, qid)
	resp, err := sc.getQueryResultResp(ctx, resultPath)
	if err != nil {
		logger.WithContext(ctx).Errorf("error: %v", err)
		if resp != nil {
			code, err := strconv.Atoi(resp.Code)
			if err != nil {
				return err
			}
			return &SnowflakeError{
				Number:   code,
				SQLState: resp.Data.SQLState,
				Message:  err.Error(),
				QueryID:  resp.Data.QueryID}
		}
		return err
	}
	rows.addDownloader(populateChunkDownloader(ctx, sc, resp.Data))
	return nil
}

// prepare a Rows object to return for query of 'qid'
func (sc *snowflakeConn) buildRowsForRunningQuery(ctx context.Context, qid string) (driver.Rows, error) {
	rows := new(snowflakeRows)
	rows.sc = sc
	rows.queryID = qid
	err := sc.rowsForRunningQuery(ctx, qid, rows)
	if err != nil {
		return nil, err
	}
	rows.ChunkDownloader.start()
	return rows, err
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

func getResumeQueryID(ctx context.Context) (string, error) {
	val := ctx.Value(fetchResultByID)
	if val == nil {
		return "", nil
	}
	strVal, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("failed to cast val %+v to string", val)
	}
	// so there is a queryID in context for which we want to fetch the result
	if !queryIDRegexp.MatchString(strVal) {
		return strVal, &SnowflakeError{
			Number:  ErrQueryIDFormat,
			Message: "Invalid QID",
			QueryID: strVal}
	}
	return strVal, nil
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
			rows.queryID = respd.Data.QueryID
			if sc.isMultiStmt(&respd.Data) {
				err = sc.handleMultiQuery(ctx, respd.Data, rows)
				if err != nil {
					rows.errChannel <- err
					close(errChannel)
					return
				}
			} else {
				rows.addDownloader(populateChunkDownloader(ctx, sc, respd.Data))
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
	v := ctx.Value(queryIDChannel)
	if v == nil {
		return nil
	}
	c, _ := v.(chan<- string)
	return c
}

func getFileStream(ctx context.Context) *bytes.Buffer {
	s := ctx.Value(fileStreamFile)
	r, ok := s.(io.Reader)
	if !ok {
		return nil
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	return buf
}

func getFileTransferOptions(ctx context.Context) *SnowflakeFileTransferOptions {
	v := ctx.Value(fileTransferOptions)
	if v == nil {
		return nil
	}
	o, ok := v.(*SnowflakeFileTransferOptions)
	if !ok {
		return nil
	}
	return o
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

func buildSnowflakeConn(ctx context.Context, config Config) (*snowflakeConn, error) {
	sc := &snowflakeConn{
		SequenceCounter: 0,
		ctx:             ctx,
		cfg:             &config,
	}
	var st http.RoundTripper = SnowflakeTransport
	if sc.cfg.Transporter == nil {
		if sc.cfg.InsecureMode {
			// no revocation check with OCSP. Think twice when you want to enable this option.
			st = snowflakeInsecureTransport
		} else {
			// set OCSP fail open mode
			ocspResponseCacheLock.Lock()
			atomic.StoreUint32((*uint32)(&ocspFailOpen), uint32(sc.cfg.OCSPFailOpen))
			ocspResponseCacheLock.Unlock()
		}
	} else {
		// use the custom transport
		st = sc.cfg.Transporter
	}
	var tokenAccessor TokenAccessor
	if sc.cfg.TokenAccessor != nil {
		tokenAccessor = sc.cfg.TokenAccessor
	} else {
		tokenAccessor = getSimpleTokenAccessor()
	}
	// authenticate
	sc.rest = &snowflakeRestful{
		Host:     sc.cfg.Host,
		Port:     sc.cfg.Port,
		Protocol: sc.cfg.Protocol,
		Client: &http.Client{
			// request timeout including reading response body
			Timeout:   sc.cfg.ClientTimeout,
			Transport: st,
		},
		TokenAccessor:       tokenAccessor,
		LoginTimeout:        sc.cfg.LoginTimeout,
		RequestTimeout:      sc.cfg.RequestTimeout,
		FuncPost:            postRestful,
		FuncGet:             getRestful,
		FuncPostQuery:       postRestfulQuery,
		FuncPostQueryHelper: postRestfulQueryHelper,
		FuncRenewSession:    renewRestfulSession,
		FuncPostAuth:        postAuth,
		FuncCloseSession:    closeSession,
		FuncCancelQuery:     cancelQuery,
		FuncPostAuthSAML:    postAuthSAML,
		FuncPostAuthOKTA:    postAuthOKTA,
		FuncGetSSO:          getSSO,
	}
	return sc, nil
}
