// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"time"
)

type queryStatus string

const (
	// QueryStatusInProgress denotes a query execution in progress
	QueryStatusInProgress queryStatus = "queryStatusInProgress"
	// QueryStatusComplete denotes a completed query execution
	QueryStatusComplete queryStatus = "queryStatusComplete"
	// QueryFailed denotes a failed query
	QueryFailed queryStatus = "queryFailed"
)

// SnowflakeResult provides an API for methods exposed to the clients
type SnowflakeResult interface {
	GetQueryID() string
	GetStatus() queryStatus
	Monitoring(time.Duration) *QueryMonitoringData
	QueryGraph(time.Duration) *QueryGraphData
}

type snowflakeResult struct {
	affectedRows int64
	insertID     int64 // Snowflake doesn't support last insert id
	queryID      string
	status       queryStatus
	err          error
	errChannel   chan error
	monitoring   *monitoringResult
}

type monitoringResult struct {
	monitoringChan <-chan *QueryMonitoringData
	queryGraphChan <-chan *QueryGraphData

	monitoring *QueryMonitoringData
	queryGraph *QueryGraphData
}

func (res *snowflakeResult) LastInsertId() (int64, error) {
	if err := res.waitForAsyncExecStatus(); err != nil {
		return -1, err
	}
	return res.insertID, nil
}

func (res *snowflakeResult) RowsAffected() (int64, error) {
	if err := res.waitForAsyncExecStatus(); err != nil {
		return -1, err
	}
	return res.affectedRows, nil
}

func (res *snowflakeResult) GetQueryID() string {
	return res.queryID
}

func (res *snowflakeResult) GetStatus() queryStatus {
	return res.status
}

func (res *snowflakeResult) GetArrowBatches() ([]*ArrowBatch, error) {
	return nil, &SnowflakeError{
		Number:  ErrNotImplemented,
		Message: errMsgNotImplemented,
	}
}

func (res *snowflakeResult) waitForAsyncExecStatus() error {
	// if async exec, block until execution is finished
	if res.status == QueryStatusInProgress {
		err := <-res.errChannel
		res.status = QueryStatusComplete
		if err != nil {
			res.status = QueryFailed
			res.err = err
			return err
		}
	} else if res.status == QueryFailed {
		return res.err
	}
	return nil
}

func (res *snowflakeResult) Monitoring(wait time.Duration) *QueryMonitoringData {
	return res.monitoring.Monitoring(wait)
}
func (res *snowflakeResult) QueryGraph(wait time.Duration) *QueryGraphData {
	return res.monitoring.QueryGraph(wait)
}

func (m *monitoringResult) Monitoring(wait time.Duration) *QueryMonitoringData {
	if m == nil {
		return nil
	} else if m.monitoring != nil {
		return m.monitoring
	}

	select {
	case v := <-m.monitoringChan:
		m.monitoring = v
		return v
	case <-time.After(wait):
		return nil
	}
}

func (m *monitoringResult) QueryGraph(wait time.Duration) *QueryGraphData {
	if m == nil {
		return nil
	} else if m.queryGraph != nil {
		return m.queryGraph
	}

	select {
	case v := <-m.queryGraphChan:
		m.queryGraph = v
		return v
	case <-time.After(wait):
		return nil
	}
}
