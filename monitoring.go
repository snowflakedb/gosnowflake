// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const urlQueriesResultFmt = "/queries/%s/result"

// QueryStatus is status returned from server
type QueryStatus int

// Query Status defined at server side
const (
	SFQueryRunning QueryStatus = iota
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

func (qs QueryStatus) String() string {
	return [...]string{"RUNNING", "ABORTING", "SUCCESS", "FAILED_WITH_ERROR",
		"ABORTED", "QUEUED", "FAILED_WITH_INCIDENT", "DISCONNECTED",
		"RESUMING_WAREHOUSE", "QUEUED_REPAIRING_WAREHOUSE", "RESTARTED",
		"BLOCKED", "NO_DATA"}[qs]
}

func (qs QueryStatus) isRunning() bool {
	switch qs {
	case SFQueryRunning, SFQueryResumingWarehouse, SFQueryQueued,
		SFQueryQueueRepairingWarehouse, SFQueryNoData:
		return true
	default:
		return false
	}
}

func (qs QueryStatus) isError() bool {
	switch qs {
	case SFQueryAborting, SFQueryFailedWithError, SFQueryAborted,
		SFQueryFailedWithIncident, SFQueryDisconnected, SFQueryBlocked:
		return true
	default:
		return false
	}
}

var strQueryStatusMap = map[string]QueryStatus{"RUNNING": SFQueryRunning,
	"ABORTING": SFQueryAborting, "SUCCESS": SFQuerySuccess,
	"FAILED_WITH_ERROR": SFQueryFailedWithError, "ABORTED": SFQueryAborted,
	"QUEUED": SFQueryQueued, "FAILED_WITH_INCIDENT": SFQueryFailedWithIncident,
	"DISCONNECTED":               SFQueryDisconnected,
	"RESUMING_WAREHOUSE":         SFQueryResumingWarehouse,
	"QUEUED_REPAIRING_WAREHOUSE": SFQueryQueueRepairingWarehouse,
	"RESTARTED":                  SFQueryRestarted,
	"BLOCKED":                    SFQueryBlocked, "NO_DATA": SFQueryNoData}

type retStatus struct {
	Status       string `json:"status"`
	ErrorMessage string `json:"errorMessage"`
	ErrorCode    int    `json:"errorCode"`
}

type statusResponse struct {
	Data struct {
		Queries []retStatus `json:"queries"`
	} `json:"data"`
	Message string `json:"message"`
	Code    string `json:"code"`
	Success bool   `json:"success"`
}

func strToQueryStatus(in string) QueryStatus {
	return strQueryStatusMap[in]
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

	if err = json.NewDecoder(res.Body).Decode(&statusResp); err != nil {
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
	var qStatus = strToQueryStatus(queryRet.Status)
	if qStatus.isError() {
		return &SnowflakeError{
			Number: ErrQueryReportedError,
			Message: fmt.Sprintf("%s: status from server: [%s]",
				queryRet.ErrorMessage, queryRet.Status),
			IncludeQueryID: true,
			QueryID:        qid,
		}
	}

	if qStatus.isRunning() {
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

func (sc *snowflakeConn) getQueryResultResp(
	ctx context.Context,
	resultPath string) (
	*execResponse, error) {
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
	if err = json.NewDecoder(res.Body).Decode(&respd); err != nil {
		logger.WithContext(ctx).Errorf("failed to decode JSON. err: %v", err)
		return nil, err
	}
	return respd, nil
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
	rows.ChunkDownloader.start()
	return rows, nil
}
