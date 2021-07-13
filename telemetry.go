// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const (
	telemetryPath    = "/telemetry/send"
	defaultFlushSize = 100
)

type telemetryMessage struct {
	Type    string `json:"type,omitempty"`
	QueryID string `json:"QueryID,omitempty"`
}

type telemetryData struct {
	Timestamp int64             `json:"timestamp,omitempty"`
	Message   *telemetryMessage `json:"message,omitempty"`
}

type snowflakeTelemetry struct {
	logs      []*telemetryData
	flushSize int
	sr        *snowflakeRestful
	mutex     *sync.Mutex
	enabled   bool
}

func (st *snowflakeTelemetry) addLog(data *telemetryData) error {
	if !st.enabled {
		return fmt.Errorf("telemetry disabled; not adding log")
	}
	st.mutex.Lock()
	st.logs = append(st.logs, data)
	st.mutex.Unlock()
	if len(st.logs) >= st.flushSize {
		if err := st.sendBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (st *snowflakeTelemetry) sendBatch() error {
	if !st.enabled {
		return fmt.Errorf("telemetry disabled; not sending log")
	}
	type telemetry struct {
		Logs []*telemetryData `json:"logs"`
	}

	st.mutex.Lock()
	logsToSend := st.logs
	st.logs = make([]*telemetryData, 0)
	st.mutex.Unlock()

	s := &telemetry{logsToSend}
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	headers := getHeaders()
	if token, _, _ := st.sr.TokenAccessor.GetTokens(); token != "" {
		headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, token)
	}
	resp, err := st.sr.FuncPost(context.Background(), st.sr, st.sr.getFullURL(telemetryPath, nil), headers, body, 10*time.Second, true)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		st.enabled = false
		return fmt.Errorf("failed to upload metrics to telemetry")
	}
	var respd telemetryResponse
	if err = json.NewDecoder(resp.Body).Decode(&respd); err != nil {
		st.enabled = false
		return err
	}
	if !respd.Success {
		st.enabled = false
		return fmt.Errorf("telemetry send failed with error code: %v, message: %v", respd.Code, respd.Message)
	}
	return nil
}
