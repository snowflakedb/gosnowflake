// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

func isAsyncModeNoFetch(ctx context.Context) bool {
	if flag, ok := ctx.Value(asyncModeNoFetch).(bool); ok && flag {
		return true
	}

	return false
}

func (sr *snowflakeRestful) processAsync(
	ctx context.Context,
	respd *execResponse,
	headers map[string]string,
	timeout time.Duration,
	cfg *Config,
	requestID UUID) (*execResponse, error) {
	// placeholder object to return to user while retrieving results
	rows := new(snowflakeRows)
	res := new(snowflakeResult)
	switch resType := getResultType(ctx); resType {
	case execResultType:
		res.queryID = respd.Data.QueryID
		res.status = QueryStatusInProgress
		res.errChannel = make(chan error)
		respd.Data.AsyncResult = res
	case queryResultType:
		rows.queryID = respd.Data.QueryID
		rows.status = QueryStatusInProgress
		rows.errChannel = make(chan error)
		respd.Data.AsyncRows = rows
	default:
		return respd, nil
	}
	// spawn goroutine to retrieve asynchronous results
	go func() {
		_ = sr.getAsync(ctx, headers, sr.getFullURL(respd.Data.GetResultURL, nil), timeout, res, rows, requestID, cfg)
	}()
	return respd, nil
}

func (sr *snowflakeRestful) getAsync(
	ctx context.Context,
	headers map[string]string,
	URL *url.URL,
	timeout time.Duration,
	res *snowflakeResult,
	rows *snowflakeRows,
	requestID UUID,
	cfg *Config) error {
	resType := getResultType(ctx)
	var errChannel chan error
	sfError := &SnowflakeError{
		Number: ErrAsync,
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

	// the get call pulling for result status is
	var response *execResponse
	var err error
	for response == nil || isQueryInProgress(response) {
		response, err = sr.getAsyncOrStatus(ctx, URL, headers, timeout)

		if err != nil {
			logger.WithContext(ctx).Errorf("failed to get response. err: %v", err)
			if err == context.Canceled || err == context.DeadlineExceeded {
				// use the default top level 1 sec timeout for cancellation as throughout the driver
				if err := cancelQuery(context.TODO(), sr, requestID, time.Second); err != nil {
					logger.WithContext(ctx).Errorf("failed to cancel async query, err: %v", err)
				}
			}

			sfError.Message = err.Error()
			errChannel <- sfError
			return err
		}
	}

	sc := &snowflakeConn{rest: sr, cfg: cfg}
	if response.Success {
		if resType == execResultType {
			res.insertID = -1
			if isDml(response.Data.StatementTypeID) {
				res.affectedRows, err = updateRows(response.Data)
				if err != nil {
					return err
				}
			} else if isMultiStmt(&response.Data) {
				r, err := sc.handleMultiExec(ctx, response.Data)
				if err != nil {
					res.errChannel <- err
					return err
				}
				res.affectedRows, err = r.RowsAffected()
				if err != nil {
					res.errChannel <- err
					return err
				}
			}
			res.queryID = response.Data.QueryID
			res.errChannel <- nil // mark exec status complete
		} else {
			rows.sc = sc
			rows.queryID = response.Data.QueryID

			if !isAsyncModeNoFetch(ctx) {
				if isMultiStmt(&response.Data) {
					if err = sc.handleMultiQuery(ctx, response.Data, rows); err != nil {
						rows.errChannel <- err
						close(errChannel)
						return err
					}
				} else {
					rows.addDownloader(populateChunkDownloader(ctx, sc, response.Data))
				}
				if err := rows.ChunkDownloader.start(); err != nil {
					rows.errChannel <- err
					close(errChannel)
					return err
				}
			}
			rows.errChannel <- nil // mark query status complete
		}
	} else {
		errChannel <- &SnowflakeError{
			Number:   parseCode(response.Code),
			SQLState: response.Data.SQLState,
			Message:  response.Message,
			QueryID:  response.Data.QueryID,
		}
	}
	return nil
}

func isQueryInProgress(execResponse *execResponse) bool {
	if !execResponse.Success {
		return false
	}

	switch parseCode(execResponse.Code) {
	case ErrQueryExecutionInProgress, ErrAsyncExecutionInProgress:
		return true
	default:
		return false
	}
}

func parseCode(codeStr string) int {
	if code, err := strconv.Atoi(codeStr); err == nil {
		return code
	}

	return -1
}

func (sr *snowflakeRestful) getAsyncOrStatus(
	ctx context.Context,
	url *url.URL,
	headers map[string]string,
	timeout time.Duration) (*execResponse, error) {
	resp, err := sr.FuncGet(ctx, sr, url, headers, timeout)
	if err != nil {
		return nil, err
	}
	if resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}

	response := &execResponse{}
	if err = json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}

	return response, nil
}
