package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/propagation"
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
	httpClientAppID            = "CLIENT_APP_ID"
	httpClientAppVersion       = "CLIENT_APP_VERSION"
)

const (
	statementTypeIDSelect           = int64(0x1000)
	statementTypeIDDml              = int64(0x3000)
	statementTypeIDMultiTableInsert = statementTypeIDDml + int64(0x500)
	statementTypeIDMultistatement   = int64(0xA000)
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

type execKey string

const (
	executionType          execKey = "executionType"
	executionTypeStatement string  = "statement"
)

// snowflakeConn manages its own context.
// External cancellation should not be supported because the connection
// may be reused after the original query/request has completed.
type snowflakeConn struct {
	ctx                 context.Context
	cfg                 *Config
	rest                *snowflakeRestful
	SequenceCounter     uint64
	telemetry           *snowflakeTelemetry
	internal            InternalClient
	queryContextCache   *queryContextCache
	currentTimeProvider currentTimeProvider
}

var (
	queryIDPattern = `[\w\-_]+`
	queryIDRegexp  = regexp.MustCompile(queryIDPattern)
)

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
	_, _, sessionID := safeGetTokens(sc.rest)
	ctx = context.WithValue(ctx, SFSessionIDKey, sessionID)
	queryContext, err := buildQueryContext(sc.queryContextCache)
	if err != nil {
		logger.WithContext(ctx).Errorf("error while building query context: %v", err)
	}
	req := execRequest{
		SQLText:      query,
		AsyncExec:    noResult,
		Parameters:   map[string]interface{}{},
		IsInternal:   isInternal,
		DescribeOnly: describeOnly,
		SequenceID:   counter,
		QueryContext: queryContext,
	}
	if key := ctx.Value(multiStatementCount); key != nil {
		req.Parameters[string(multiStatementCount)] = key
	}
	if tag := ctx.Value(queryTag); tag != nil {
		req.Parameters[string(queryTag)] = tag
	}
	logger.WithContext(ctx).Infof("parameters: %v", req.Parameters)

	// handle bindings, if required
	requestID := getOrGenerateRequestIDFromContext(ctx)
	if len(bindings) > 0 {
		if err = sc.processBindings(ctx, bindings, describeOnly, requestID, &req); err != nil {
			return nil, err
		}
	}
	logger.WithContext(ctx).Infof("bindings: %v", req.Bindings)

	// populate headers
	headers := getHeaders()
	if isFileTransfer(query) {
		headers[httpHeaderAccept] = headerContentTypeApplicationJSON
	}

	// propagate traceID and spanID via traceparent header. this is a no-op if invalid IDs
	propagator := propagation.TraceContext{}
	propagator.Inject(ctx, propagation.MapCarrier(headers))

	paramsMutex.Lock()
	if serviceName, ok := sc.cfg.Params[serviceName]; ok {
		headers[httpHeaderServiceName] = *serviceName
	}
	paramsMutex.Unlock()

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	data, err := sc.rest.FuncPostQuery(ctx, sc.rest, &url.Values{}, headers,
		jsonBody, sc.rest.RequestTimeout, requestID, sc.cfg)
	if err != nil {
		return data, err
	}
	code := -1
	if data.Code != "" {
		code, err = strconv.Atoi(data.Code)
		if err != nil {
			return data, err
		}
	}
	logger.WithContext(ctx).Infof("Success: %v, Code: %v", data.Success, code)
	if !data.Success {
		err = (populateErrorFields(code, data)).exceptionTelemetry(sc)
		return nil, err
	}

	if !sc.cfg.DisableQueryContextCache && data.Data.QueryContext != nil {
		queryContext, err := extractQueryContext(data)
		if err != nil {
			logger.WithContext(ctx).Errorf("error while decoding query context: %v", err)
		} else {
			sc.queryContextCache.add(sc, queryContext.Entries...)
		}
	}

	// handle PUT/GET commands
	fileTransferChan := make(chan error, 1)
	if isFileTransfer(query) {
		go func() {
			data, err = sc.processFileTransfer(ctx, data, query, isInternal)
			fileTransferChan <- err
		}()

		select {
		case <-ctx.Done():
			logger.WithContext(ctx).Info("File transfer has been cancelled")
			return nil, ctx.Err()
		case err := <-fileTransferChan:
			if err != nil {
				return nil, err
			}
		}
	}

	logger.WithContext(ctx).Infof("Exec/Query SUCCESS with total=%v, returned=%v", data.Data.Total, data.Data.Returned)
	if data.Data.FinalDatabaseName != "" {
		sc.cfg.Database = data.Data.FinalDatabaseName
	}
	if data.Data.FinalSchemaName != "" {
		sc.cfg.Schema = data.Data.FinalSchemaName
	}
	if data.Data.FinalWarehouseName != "" {
		sc.cfg.Warehouse = data.Data.FinalWarehouseName
	}
	if data.Data.FinalRoleName != "" {
		sc.cfg.Role = data.Data.FinalRoleName
	}
	sc.populateSessionParameters(data.Data.Parameters)
	return data, err
}

func extractQueryContext(data *execResponse) (queryContext, error) {
	var queryContext queryContext
	err := json.Unmarshal(data.Data.QueryContext, &queryContext)
	return queryContext, err
}

func buildQueryContext(qcc *queryContextCache) (requestQueryContext, error) {
	rqc := requestQueryContext{}
	if qcc == nil || len(qcc.entries) == 0 {
		logger.Debugf("empty qcc")
		return rqc, nil
	}
	for _, qce := range qcc.entries {
		contextData := contextData{}
		if qce.Context == "" {
			contextData.Base64Data = qce.Context
		}
		rqc.Entries = append(rqc.Entries, requestQueryContextEntry{
			ID:        qce.ID,
			Priority:  qce.Priority,
			Timestamp: qce.Timestamp,
			Context:   contextData,
		})
	}
	return rqc, nil
}

func (sc *snowflakeConn) Begin() (driver.Tx, error) {
	return sc.BeginTx(sc.ctx, driver.TxOptions{})
}

func (sc *snowflakeConn) BeginTx(
	ctx context.Context,
	opts driver.TxOptions) (
	driver.Tx, error) {
	logger.WithContext(ctx).Info("BeginTx")
	if opts.ReadOnly {
		return nil, (&SnowflakeError{
			Number:   ErrNoReadOnlyTransaction,
			SQLState: SQLStateFeatureNotSupported,
			Message:  errMsgNoReadOnlyTransaction,
		}).exceptionTelemetry(sc)
	}
	if int(opts.Isolation) != int(sql.LevelDefault) {
		return nil, (&SnowflakeError{
			Number:   ErrNoDefaultTransactionIsolationLevel,
			SQLState: SQLStateFeatureNotSupported,
			Message:  errMsgNoDefaultTransactionIsolationLevel,
		}).exceptionTelemetry(sc)
	}
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	isDesc := isDescribeOnly(ctx)
	isInternal := isInternal(ctx)
	if _, err := sc.exec(ctx, "BEGIN", false, /* noResult */
		isInternal, isDesc, nil); err != nil {
		return nil, err
	}
	return &snowflakeTx{sc, ctx}, nil
}

func (sc *snowflakeConn) cleanup() {
	// must flush log buffer while the process is running.
	logger.WithContext(sc.ctx).Debugln("Snowflake connection closing.")
	if sc.rest != nil && sc.rest.Client != nil {
		sc.rest.Client.CloseIdleConnections()
	}
}

func (sc *snowflakeConn) Close() (err error) {
	logger.WithContext(sc.ctx).Infoln("Close")
	if err := sc.telemetry.sendBatch(); err != nil {
		logger.WithContext(sc.ctx).Warnf("error while sending telemetry. %v", err)
	}
	sc.stopHeartBeat()
	sc.rest.HeartBeat = nil
	defer sc.cleanup()

	if sc.cfg != nil && !sc.cfg.KeepSessionAlive {
		// we have to replace context with background, otherwise we can use a one that is cancelled or timed out
		if err = sc.rest.FuncCloseSession(context.Background(), sc.rest, sc.rest.RequestTimeout); err != nil {
			logger.WithContext(sc.ctx).Error(err)
		}
	}
	return nil
}

func (sc *snowflakeConn) PrepareContext(
	ctx context.Context,
	query string) (
	driver.Stmt, error) {
	logger.WithContext(sc.ctx).Infoln("Prepare")
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	stmt := &snowflakeStmt{
		sc:    sc,
		query: query,
	}
	return stmt, nil
}

func (sc *snowflakeConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue) (
	driver.Result, error) {
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}
	_, _, sessionID := safeGetTokens(sc.rest)
	ctx = context.WithValue(ctx, SFSessionIDKey, sessionID)
	logger.WithContext(ctx).Infof("Exec: %#v, %v", query, args)
	noResult := isAsyncMode(ctx)
	isDesc := isDescribeOnly(ctx)
	isInternal := isInternal(ctx)
	ctx = setResultType(ctx, execResultType)
	data, err := sc.exec(ctx, query, noResult, isInternal, isDesc, args)
	if err != nil {
		logger.WithContext(ctx).Infof("error: %v", err)
		if data != nil {
			code, e := strconv.Atoi(data.Code)
			if e != nil {
				return nil, e
			}
			return nil, (&SnowflakeError{
				Number:   code,
				SQLState: data.Data.SQLState,
				Message:  err.Error(),
				QueryID:  data.Data.QueryID,
			}).exceptionTelemetry(sc)
		}
		return nil, err
	}

	// if async exec, return result object right away
	if noResult {
		return data.Data.AsyncResult, nil
	}

	if isDml(data.Data.StatementTypeID) {
		// collects all values from the returned row sets
		updatedRows, err := updateRows(data.Data)
		if err != nil {
			return nil, err
		}
		logger.WithContext(ctx).Debugf("number of updated rows: %#v", updatedRows)
		return &snowflakeResult{
			affectedRows: updatedRows,
			insertID:     -1,
			queryID:      data.Data.QueryID,
		}, nil // last insert id is not supported by Snowflake
	} else if isMultiStmt(&data.Data) {
		return sc.handleMultiExec(ctx, data.Data)
	} else if isDql(&data.Data) {
		logger.WithContext(ctx).Debugf("DQL")
		if isStatementContext(ctx) {
			return &snowflakeResultNoRows{queryID: data.Data.QueryID}, nil
		}
		return driver.ResultNoRows, nil
	}
	logger.WithContext(ctx).Debug("DDL")
	if isStatementContext(ctx) {
		return &snowflakeResultNoRows{queryID: data.Data.QueryID}, nil
	}
	return driver.ResultNoRows, nil
}

func (sc *snowflakeConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue) (
	driver.Rows, error) {
	qid, err := getResumeQueryID(ctx)
	if err != nil {
		return nil, err
	}
	if qid == "" {
		return sc.queryContextInternal(ctx, query, args)
	}

	// check the query status to find out if there is a result to fetch
	_, err = sc.checkQueryStatus(ctx, qid)
	snowflakeErr, isSnowflakeError := err.(*SnowflakeError)
	if err == nil || (isSnowflakeError && snowflakeErr.Number == ErrQueryIsRunning) {
		// the query is running. Rows object will be returned from here.
		return sc.buildRowsForRunningQuery(ctx, qid)
	}
	return nil, err
}

func (sc *snowflakeConn) queryContextInternal(
	ctx context.Context,
	query string,
	args []driver.NamedValue) (
	driver.Rows, error) {
	if sc.rest == nil {
		return nil, driver.ErrBadConn
	}

	_, _, sessionID := safeGetTokens(sc.rest)
	ctx = context.WithValue(setResultType(ctx, queryResultType), SFSessionIDKey, sessionID)
	logger.WithContext(ctx).Infof("Query: %#v, %v", query, args)
	noResult := isAsyncMode(ctx)
	isDesc := isDescribeOnly(ctx)
	isInternal := isInternal(ctx)
	data, err := sc.exec(ctx, query, noResult, isInternal, isDesc, args)
	if err != nil {
		logger.WithContext(ctx).Errorf("error: %v", err)
		if data != nil {
			code, e := strconv.Atoi(data.Code)
			if e != nil {
				return nil, e
			}
			return nil, (&SnowflakeError{
				Number:   code,
				SQLState: data.Data.SQLState,
				Message:  err.Error(),
				QueryID:  data.Data.QueryID,
			}).exceptionTelemetry(sc)
		}
		return nil, err
	}

	// if async query, return row object right away
	if noResult {
		return data.Data.AsyncRows, nil
	}

	rows := new(snowflakeRows)
	rows.sc = sc
	rows.queryID = data.Data.QueryID
	rows.ctx = ctx
	rows.format = resultFormat(data.Data.QueryResultFormat)

	if isMultiStmt(&data.Data) {
		// handleMultiQuery is responsible to fill rows with childResults
		if err = sc.handleMultiQuery(ctx, data.Data, rows); err != nil {
			return nil, err
		}
	} else {
		rows.addDownloader(populateChunkDownloader(ctx, sc, data.Data))
	}

	err = rows.ChunkDownloader.start()
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
	noResult := isAsyncMode(ctx)
	isDesc := isDescribeOnly(ctx)
	isInternal := isInternal(ctx)
	ctx = setResultType(ctx, execResultType)
	_, err := sc.exec(ctx, "SELECT 1", noResult, isInternal,
		isDesc, []driver.NamedValue{})
	return err
}

// CheckNamedValue determines which types are handled by this driver aside from
// the instances captured by driver.Value
func (sc *snowflakeConn) CheckNamedValue(nv *driver.NamedValue) error {
	if supportedNullBind(nv) || supportedArrayBind(nv) || supportedStructuredObjectWriterBind(nv) || supportedStructuredArrayBind(nv) || supportedStructuredMapBind(nv) {
		return nil
	}
	return driver.ErrSkip
}

func (sc *snowflakeConn) GetQueryStatus(
	ctx context.Context,
	queryID string) (
	*SnowflakeQueryStatus, error) {
	queryRet, err := sc.checkQueryStatus(ctx, queryID)
	if err != nil {
		return nil, err
	}
	return &SnowflakeQueryStatus{
		queryRet.SQLText,
		queryRet.StartTime,
		queryRet.EndTime,
		queryRet.ErrorCode,
		queryRet.ErrorMessage,
		queryRet.Stats.ScanBytes,
		queryRet.Stats.ProducedRows,
	}, nil
}

// gzip.Reader.Close does NOT close the underlying reader, so we
// need to wrap with wrapReader so that closing will close the
// response body (or any other reader that we want to gzip uncompress)
type wrapReader struct {
	io.Reader
	wrapped io.ReadCloser
}

func (w *wrapReader) Close() error {
	if cl, ok := w.Reader.(io.ReadCloser); ok {
		if err := cl.Close(); err != nil {
			return err
		}
	}
	return w.wrapped.Close()
}

// buildSnowflakeConn creates a new snowflakeConn.
// The provided context is used only for establishing the initial connection.
func buildSnowflakeConn(ctx context.Context, config Config) (*snowflakeConn, error) {
	sc := &snowflakeConn{
		SequenceCounter:     0,
		ctx:                 ctx,
		cfg:                 &config,
		queryContextCache:   (&queryContextCache{}).init(),
		currentTimeProvider: defaultTimeProvider,
	}
	err := initEasyLogging(config.ClientConfigFile)
	if err != nil {
		return nil, err
	}

	telemetry := &snowflakeTelemetry{}
	if config.DisableTelemetry {
		telemetry.enabled = false
	} else {
		telemetry.flushSize = defaultFlushSize
		telemetry.sr = sc.rest
		telemetry.mutex = &sync.Mutex{}
		telemetry.enabled = true
	}

	transportFactory := newTransportFactory(&config, telemetry)
	st, err := transportFactory.createTransport()
	if err != nil {
		return nil, err
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
		JWTClient: &http.Client{
			Timeout:   sc.cfg.JWTClientTimeout,
			Transport: st,
		},
		TokenAccessor:       tokenAccessor,
		LoginTimeout:        sc.cfg.LoginTimeout,
		RequestTimeout:      sc.cfg.RequestTimeout,
		MaxRetryCount:       sc.cfg.MaxRetryCount,
		FuncPost:            postRestful,
		FuncGet:             getRestful,
		FuncAuthPost:        postAuthRestful,
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

	telemetry.sr = sc.rest
	sc.telemetry = telemetry

	return sc, nil
}
