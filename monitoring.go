// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"time"
)

const urlQueriesResultFmt = "/queries/%s/result"

// queryResultStatus is status returned from server
type queryResultStatus int

// Query Status defined at server side
const (
	SFQueryRunning queryResultStatus = iota
	SFQueryAborting
	SFQuerySuccess
	SFQueryFailedWithError
	SFQueryAborted
	SFQueryQueued
	SFQueryFailedWithIncident
	SFQueryDisconnected
	SFQueryResumingWarehouse
	// SFQueryQueueRepairingWarehouse present in QueryDTO.java.
	SFQueryQueueRepairingWarehouse
	SFQueryRestarted
	// SFQueryBlocked is when a statement is waiting on a lock on resource held
	// by another statement.
	SFQueryBlocked
	SFQueryNoData
)

func (qs queryResultStatus) String() string {
	return [...]string{"RUNNING", "ABORTING", "SUCCESS", "FAILED_WITH_ERROR",
		"ABORTED", "QUEUED", "FAILED_WITH_INCIDENT", "DISCONNECTED",
		"RESUMING_WAREHOUSE", "QUEUED_REPAIRING_WAREHOUSE", "RESTARTED",
		"BLOCKED", "NO_DATA"}[qs]
}

func (qs queryResultStatus) isRunning() bool {
	switch qs {
	case SFQueryRunning, SFQueryResumingWarehouse, SFQueryQueued,
		SFQueryQueueRepairingWarehouse, SFQueryNoData:
		return true
	default:
		return false
	}
}

func (qs queryResultStatus) isError() bool {
	switch qs {
	case SFQueryAborting, SFQueryFailedWithError, SFQueryAborted,
		SFQueryFailedWithIncident, SFQueryDisconnected, SFQueryBlocked:
		return true
	default:
		return false
	}
}

var strQueryStatusMap = map[string]queryResultStatus{"RUNNING": SFQueryRunning,
	"ABORTING": SFQueryAborting, "SUCCESS": SFQuerySuccess,
	"FAILED_WITH_ERROR": SFQueryFailedWithError, "ABORTED": SFQueryAborted,
	"QUEUED": SFQueryQueued, "FAILED_WITH_INCIDENT": SFQueryFailedWithIncident,
	"DISCONNECTED":               SFQueryDisconnected,
	"RESUMING_WAREHOUSE":         SFQueryResumingWarehouse,
	"QUEUED_REPAIRING_WAREHOUSE": SFQueryQueueRepairingWarehouse,
	"RESTARTED":                  SFQueryRestarted,
	"BLOCKED":                    SFQueryBlocked, "NO_DATA": SFQueryNoData}

type retStatus struct {
	Status       string   `json:"status"`
	SQLText      string   `json:"sqlText"`
	StartTime    int64    `json:"startTime"`
	EndTime      int64    `json:"endTime"`
	ErrorCode    string   `json:"errorCode"`
	ErrorMessage string   `json:"errorMessage"`
	Stats        retStats `json:"stats"`
}

type retStats struct {
	ScanBytes    int64 `json:"scanBytes"`
	ProducedRows int64 `json:"producedRows"`
}

type statusResponse struct {
	Data struct {
		Queries []retStatus `json:"queries"`
	} `json:"data"`
	Message string `json:"message"`
	Code    string `json:"code"`
	Success bool   `json:"success"`
}

func strToQueryStatus(in string) queryResultStatus {
	return strQueryStatusMap[in]
}

// SnowflakeQueryStatus is the query status metadata of a snowflake query
type SnowflakeQueryStatus struct {
	SQLText      string
	StartTime    int64
	EndTime      int64
	ErrorCode    string
	ErrorMessage string
	ScanBytes    int64
	ProducedRows int64
}

// SnowflakeConnection is a wrapper to snowflakeConn that exposes API functions
type SnowflakeConnection interface {
	GetQueryStatus(ctx context.Context, queryID string) (*SnowflakeQueryStatus, error)
}

// checkQueryStatus returns the status given the query ID. If successful,
// the error will be nil, indicating there is a complete query result to fetch.
// Other than nil, there are three error types that can be returned:
// 1. ErrQueryStatus, if GS cannot return any kind of status due to any reason,
// i.e. connection, permission, if a query was just submitted, etc.
// 2, ErrQueryReportedError, if the requested query was terminated or aborted
// and GS returned an error status included in query. SFQueryFailedWithError
// 3, ErrQueryIsRunning, if the requested query is still running and might have
// a complete result later, these statuses were listed in query. SFQueryRunning
func (sc *snowflakeConn) checkQueryStatus(
	ctx context.Context,
	qid string) (
	*retStatus, error) {
	var statusResp statusResponse

	err := sc.getMonitoringResult(ctx, "queries", qid, &statusResp)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to get response. err: %v", err)
		return nil, err
	}
	defer res.Body.Close()
	var statusResp = statusResponse{}
	if err = json.NewDecoder(res.Body).Decode(&statusResp); err != nil {
		logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
		return nil, err
	}

	if !statusResp.Success || len(statusResp.Data.Queries) == 0 {
		logger.WithContext(ctx).Errorf("status query returned not-success or no status returned.")
		return nil, (&SnowflakeError{
			Number:  ErrQueryStatus,
			Message: "status query returned not-success or no status returned. Please retry",
		}).exceptionTelemetry(sc)
	}

	queryRet := statusResp.Data.Queries[0]
	if queryRet.ErrorCode != "" {
		return &queryRet, (&SnowflakeError{
			Number:         ErrQueryStatus,
			Message:        errMsgQueryStatus,
			MessageArgs:    []interface{}{queryRet.ErrorCode, queryRet.ErrorMessage},
			IncludeQueryID: true,
			QueryID:        qid,
		}).exceptionTelemetry(sc)
	}

	// returned errorCode is 0. Now check what is the returned status of the query.
	qStatus := strToQueryStatus(queryRet.Status)
	if qStatus.isError() {
		return &queryRet, (&SnowflakeError{
			Number: ErrQueryReportedError,
			Message: fmt.Sprintf("%s: status from server: [%s]",
				queryRet.ErrorMessage, queryRet.Status),
			IncludeQueryID: true,
			QueryID:        qid,
		}).exceptionTelemetry(sc)
	}

	if qStatus.isRunning() {
		return &queryRet, (&SnowflakeError{
			Number: ErrQueryIsRunning,
			Message: fmt.Sprintf("%s: status from server: [%s]",
				queryRet.ErrorMessage, queryRet.Status),
			IncludeQueryID: true,
			QueryID:        qid,
		}).exceptionTelemetry(sc)
	}
	//success
	return &queryRet, nil
}

// Waits 45 seconds for a query response; return early if query finishes
func (sc *snowflakeConn) getQueryResultResp(
	ctx context.Context,
	resultPath string,
) (*execResponse, error) {
	if respd, ok := sc.execRespCache.load(resultPath); ok {
		return respd, nil
	}

	headers := getHeaders()
	paramsMutex.Lock()
	if serviceName, ok := sc.cfg.Params[serviceName]; ok {
		headers[httpHeaderServiceName] = *serviceName
	}
	paramsMutex.Unlock()
	param := make(url.Values)
	param.Add(requestIDKey, getOrGenerateRequestIDFromContext(ctx).String())
	param.Add("clientStartTime", strconv.FormatInt(time.Now().Unix(), 10))
	param.Add(requestGUIDKey, NewUUID().String())
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
	defer res.Body.Close()
	var respd *execResponse
	if err = json.NewDecoder(res.Body).Decode(&respd); err != nil {
		logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
		return nil, err
	}

	sc.execRespCache.store(resultPath, respd)
	return respd, nil
}

// Waits for the query to complete, then returns the response
func (sc *snowflakeConn) waitForCompletedQueryResultResp(
	ctx context.Context,
	resultPath string,
	qid string,
) (*execResponse, error) {
	// if we already have the response; return that
	cachedResponse, ok := sc.execRespCache.load(resultPath)
	logger.WithContext(ctx).Errorf("use cache: %v", ok)
	if ok {
		return cachedResponse, nil
	}
	requestID := getOrGenerateRequestIDFromContext(ctx)
	headers := getHeaders()
	if serviceName, ok := sc.cfg.Params[serviceName]; ok {
		headers[httpHeaderServiceName] = *serviceName
	}
	param := make(url.Values)
	param.Add(requestIDKey, requestID.String())
	param.Add("clientStartTime", strconv.FormatInt(time.Now().Unix(), 10))
	param.Add(requestGUIDKey, NewUUID().String())
	token, _, _ := sc.rest.TokenAccessor.GetTokens()
	if token != "" {
		headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, token)
	}
	url := sc.rest.getFullURL(resultPath, &param)

	// internally, pulls on FuncGet until we have a result at the result location (queryID)
	var response *execResponse
	var err error

	startTime := time.Now()
	for response == nil || isQueryInProgress(response) {
		response, err = sc.rest.getAsyncOrStatus(WithReportAsyncError(ctx), url, headers, sc.rest.RequestTimeout)

		// if the context is canceled, we have to cancel it manually now
		if err != nil {
			logger.WithContext(ctx).Errorf("failed to get response. err: %v", err)
			if err == context.Canceled || err == context.DeadlineExceeded {
				// use the default top level 1 sec timeout for cancellation as throughout the driver
				if err := cancelQuery(context.TODO(), sc.rest, requestID, time.Second); err != nil {
					logger.WithContext(ctx).Errorf("failed to cancel async query, err: %v", err)
				}
			}
			return nil, err
		}
	} 

	if ! response.Success {
		logEverything(ctx, qid, response, startTime)
		_, statusErr := sc.checkQueryStatus(ctx, qid)
		logger.WithContext(ctx).Errorf("failed queryId: %v, statusErr: %v", qid, statusErr)
		retryResponse, retryErr := sc.rest.getAsyncOrStatus(WithReportAsyncError(ctx), url, headers, sc.rest.RequestTimeout)
		couldRetry := shouldRetry(ctx, response, err)
		logger.WithContext(ctx).Errorf("failed queryId: %v, couldRetry: %v, retryResponseSuccess: %v, retryErr: %v", qid, couldRetry, retryResponse.Success, retryErr)
	}

	sc.execRespCache.store(resultPath, response)
	return response, nil
}

// we want to retry if the query was not successful, but also did not fail
func shouldRetry(ctx context.Context, response *execResponse, err error) bool {
	// if deadline has passed dont retty
	deadline, ok := ctx.Deadline()
	if ok && (deadline.Before(deadline)) {
		return false
	}

	// if context has been canceled dont retry
	select {
	case <-ctx.Done():
		return false
	}

	// if there is a response succeeds, dont retry 
	if response.Success {
		return false
	}

	// if there is a response message dont retry 
	if response.Message != "" {
		return false
	}

	// ig there is a response code dont retry
	if response.Code != "" {
		return false 
	}

	return true
}

func logEverything(ctx context.Context, qid string, response *execResponse, startTime time.Time) {
	deadline, ok := ctx.Deadline()
	logger.WithContext(ctx).Errorf("failed queryId: %v, deadline: %v, ok: %v", qid, deadline, ok)
	logger.WithContext(ctx).Errorf("failed queryId: %v, runtime: %v", qid, time.Now().Sub(startTime))

	var pcs [32]uintptr
	stackEntries := runtime.Callers(1, pcs[:])
	stackTrace := pcs[0:stackEntries]
	logger.WithContext(ctx).Errorf("failed queryId: %v, stackTrace: %v", qid, stackTrace)

	select {
	case <-ctx.Done():
		cancelReason := ctx.Err()
		logger.WithContext(ctx).Errorf("failed queryId: %v, cancel reason: %v", qid, cancelReason)
	default:
		logger.WithContext(ctx).Errorf("failed queryId: %v, query not canceled", qid)
	}

	logger.WithContext(ctx).Errorf("failed queryId: %v, response message: %v", qid, response.Message)
	logger.WithContext(ctx).Errorf("failed queryId: %v, response code: %v", qid, response.Code)
}

// Fetch query result for a query id from /queries/<qid>/result endpoint.
func (sc *snowflakeConn) rowsForRunningQuery(
	ctx context.Context, qid string,
	rows *snowflakeRows) error {
	resultPath := fmt.Sprintf(urlQueriesResultFmt, qid)
	resp, err := sc.getQueryResultResp(ctx, resultPath)
	if err != nil {
		logger.WithContext(ctx).Errorf("error: %v", err)
		if resp != nil {
			code, err := strconv.Atoi(resp.Code)
			if err != nil {
				return err
			}
			return (&SnowflakeError{
				Number:   code,
				SQLState: resp.Data.SQLState,
				Message:  err.Error(),
				QueryID:  resp.Data.QueryID,
			}).exceptionTelemetry(sc)
		}
		return err
	}
	if !resp.Success {
		message := resp.Message
		code, err := strconv.Atoi(resp.Code)
		if err != nil {
			code = ErrQueryStatus
			message = fmt.Sprintf("%s: (failed to parse original code: %s: %s)", message, resp.Code, err.Error())
		}
		return (&SnowflakeError{
			Number:   code,
			SQLState: resp.Data.SQLState,
			Message:  message,
			QueryID:  resp.Data.QueryID,
		}).exceptionTelemetry(sc)
	}
	rows.addDownloader(populateChunkDownloader(ctx, sc, resp.Data))
	return nil
}

// Wait for query to complete from a query id from /queries/<qid>/result endpoint.
func (sc *snowflakeConn) blockOnRunningQuery(
	ctx context.Context, qid string) error {
	resultPath := fmt.Sprintf(urlQueriesResultFmt, qid)
	resp, err := sc.waitForCompletedQueryResultResp(ctx, resultPath, qid)
	if err != nil {
		logger.WithContext(ctx).Errorf("error: %v", err)
		if resp != nil {
			code := -1
			if resp.Code != "" {
				code, err = strconv.Atoi(resp.Code)
				if err != nil {
					return err
				}
			}
			if code == -1 {
				ok, deadline := ctx.Deadline()
				logger.WithContext(ctx).Errorf("deadline: %v, ok: %v, queryId: %v", deadline, ok, resp.Data.QueryID)
				logger.WithContext(ctx).Errorf("resp.success: %v, message: %v, error: %v, queryId: %v", resp.Success, resp.Message, err, resp.Data.QueryID)
				if sc.rest == nil {
					logger.WithContext(ctx).Errorf("sullSnowflakeRestful")
				}
			}
			return (&SnowflakeError{
				Number:   code,
				SQLState: resp.Data.SQLState,
				Message:  err.Error(),
				QueryID:  resp.Data.QueryID,
			}).exceptionTelemetry(sc)
		}
		return err
	}
	if !resp.Success {
		message := resp.Message
		code := -1
		if resp.Code != "" {
			code, err = strconv.Atoi(resp.Code)
			if err != nil {
				code = ErrQueryStatus
				message = fmt.Sprintf("%s: (failed to parse original code: %s: %s)", message, resp.Code, err.Error())
			}
		}
		if code == -1 {
			ok, deadline := ctx.Deadline()
			logger.WithContext(ctx).Errorf("deadline: %v, ok: %v, queryId: %v", deadline, ok, resp.Data.QueryID)
			logger.WithContext(ctx).Errorf("resp.success: %v, message: %v, error: %v, queryId: %v", resp.Success, resp.Message, err, resp.Data.QueryID)
			if sc.rest == nil {
				logger.WithContext(ctx).Errorf("sullSnowflakeRestful")
			}
		}
		return (&SnowflakeError{
			Number:   code,
			SQLState: resp.Data.SQLState,
			Message:  message,
			QueryID:  resp.Data.QueryID,
		}).exceptionTelemetry(sc)
	}
	return nil
}

// prepare a Rows object to return for query of 'qid'
func (sc *snowflakeConn) buildRowsForRunningQuery(
	ctx context.Context,
	qid string) (
	driver.Rows, error) {
	rows := new(snowflakeRows)
	rows.sc = sc
	rows.queryID = qid
	if err := sc.rowsForRunningQuery(ctx, qid, rows); err != nil {
		return nil, err
	}
	if err := rows.ChunkDownloader.start(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (sc *snowflakeConn) blockOnQueryCompletion(
	ctx context.Context,
	qid string,
) error {
	if err := sc.blockOnRunningQuery(ctx, qid); err != nil {
		return err
	}
	return nil
}

func mkMonitoringFetcher(sc *snowflakeConn, qid string, runtime time.Duration) *monitoringResult {
	// Exit early if this was a "fast" query
	if runtime < sc.cfg.MonitoringFetcher.QueryRuntimeThreshold {
		return nil
	}

	queryGraphChan := make(chan *QueryGraphData, 1)
	go queryGraph(sc, qid, queryGraphChan)

	monitoringChan := make(chan *QueryMonitoringData, 1)
	go monitoring(sc, qid, monitoringChan)

	return &monitoringResult{
		monitoringChan: monitoringChan,
		queryGraphChan: queryGraphChan,
	}
}

func monitoring(
	sc *snowflakeConn,
	qid string,
	resp chan<- *QueryMonitoringData,
) {
	defer close(resp)

	ctx, cancel := context.WithTimeout(context.Background(), sc.cfg.MonitoringFetcher.MaxDuration)
	defer cancel()

	var queryMonitoringData *QueryMonitoringData
	for {
		var m monitoringResponse
		if err := sc.getMonitoringResult(ctx, "queries", qid, &m); err != nil {
			break
		}

		if len(m.Data.Queries) == 1 {
			queryMonitoringData = &m.Data.Queries[0]
			if !strToQueryStatus(queryMonitoringData.Status).isRunning() {
				break
			}
		}

		time.Sleep(sc.cfg.MonitoringFetcher.RetrySleepDuration)
	}

	if queryMonitoringData != nil {
		resp <- queryMonitoringData
	}

	return
}

func queryGraph(
	sc *snowflakeConn,
	qid string,
	resp chan<- *QueryGraphData,
) {
	defer close(resp)

	// Bound the GET request to 1 second in the absolute worst case.
	ctx, cancel := context.WithTimeout(context.Background(), sc.cfg.MonitoringFetcher.MaxDuration)
	defer cancel()

	var qg queryGraphResponse
	err := sc.getMonitoringResult(ctx, "query-plan-data", qid, &qg)
	if err == nil && qg.Success {
		resp <- &qg.Data
	}
}

// getMonitoringResult fetches the result at /monitoring/queries/qid and
// deserializes it into the provided res (which is given as a generic interface
// to allow different callers to request different views on the raw response)
func (sc *snowflakeConn) getMonitoringResult(ctx context.Context, endpoint, qid string, res interface{}) error {
	sc.restMu.RLock()
	defer sc.restMu.RUnlock()
	headers := make(map[string]string)
	param := make(url.Values)
	param.Add(requestGUIDKey, NewUUID().String())
	if tok, _, _ := sc.rest.TokenAccessor.GetTokens(); tok != "" {
		headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, tok)
	}
	resultPath := fmt.Sprintf("/monitoring/%s/%s", endpoint, qid)
	url := sc.rest.getFullURL(resultPath, &param)

	resp, err := sc.rest.FuncGet(ctx, sc.rest, url, headers, sc.rest.RequestTimeout)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to get response for %s. err: %v", endpoint, err)
		return err
	}

	err = json.NewDecoder(resp.Body).Decode(res)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
		return err
	}

	return nil
}
