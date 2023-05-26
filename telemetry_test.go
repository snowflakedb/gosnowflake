// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"
)

func TestTelemetryAddLog(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}

	st := &snowflakeTelemetry{
		sr:        sc.rest,
		mutex:     &sync.Mutex{},
		enabled:   true,
		flushSize: defaultFlushSize,
	}
	rand.Seed(time.Now().UnixNano())
	randNum := rand.Int() % 10000
	for i := 0; i < randNum; i++ {
		if err = st.addLog(&telemetryData{
			Message: map[string]string{
				typeKey:    "client_telemetry_type",
				queryIDKey: "123",
			},
			Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(st.logs) != randNum%defaultFlushSize {
		t.Errorf("length of remaining logs does not match. expected: %v, got: %v",
			randNum%defaultFlushSize, len(st.logs))
	}
	if err = st.sendBatch(); err != nil {
		t.Fatal(err)
	}
}

func TestTelemetrySQLException(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}

	st := &snowflakeTelemetry{
		sr:        sc.rest,
		mutex:     &sync.Mutex{},
		enabled:   true,
		flushSize: defaultFlushSize,
	}
	sc.telemetry = st
	sfa := &snowflakeFileTransferAgent{
		sc:          sc,
		commandType: uploadCommand,
		srcFiles:    make([]string, 0),
		data: &execResponseData{
			SrcLocations: make([]string, 0),
		},
	}
	if err = sfa.initFileMetadata(); err == nil {
		t.Fatal("this should have thrown an error")
	}
	if len(st.logs) != 1 {
		t.Errorf("there should be 1 telemetry data in log. found: %v", len(st.logs))
	}
	if sendErr := st.sendBatch(); sendErr != nil {
		t.Fatal(sendErr)
	}
	if len(st.logs) != 0 {
		t.Errorf("there should be no telemetry data in log. found: %v", len(st.logs))
	}
}

func TestDisableTelemetry(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	config.DisableTelemetry = true
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	if !sc.cfg.DisableTelemetry {
		t.Errorf("DisableTelemetry should be true. DisableTelemetry: %v", sc.cfg.DisableTelemetry)
	}
	if sc.telemetry.enabled {
		t.Errorf("telemetry should be disabled.")
	}
}

func TestEnableTelemetry(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	config.DisableTelemetry = false
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	if sc.cfg.DisableTelemetry {
		t.Errorf("DisableTelemetry should be false. DisableTelemetry: %v", sc.cfg.DisableTelemetry)
	}
	if !sc.telemetry.enabled {
		t.Errorf("telemetry should be enabled.")
	}
}

func funcPostTelemetryRespFail(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return nil, errors.New("failed to upload metrics to telemetry")
}

func TestTelemetryError(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	sr := &snowflakeRestful{
		FuncPost:      funcPostTelemetryRespFail,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	st := &snowflakeTelemetry{
		sr:        sr,
		mutex:     &sync.Mutex{},
		enabled:   true,
		flushSize: defaultFlushSize,
	}

	if err = st.addLog(&telemetryData{
		Message: map[string]string{
			typeKey:    "client_telemetry_type",
			queryIDKey: "123",
		},
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}

	err = st.sendBatch()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestTelemetryDisabledOnBadResponse(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	sr := &snowflakeRestful{
		FuncPost:      postTestAppBadGatewayError,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	st := &snowflakeTelemetry{
		sr:        sr,
		mutex:     &sync.Mutex{},
		enabled:   true,
		flushSize: defaultFlushSize,
	}

	if err = st.addLog(&telemetryData{
		Message: map[string]string{
			typeKey:    "client_telemetry_type",
			queryIDKey: "123",
		},
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	err = st.sendBatch()
	if err == nil {
		t.Fatal("should have failed")
	}
	if st.enabled == true {
		t.Fatal("telemetry should be disabled")
	}

	st.enabled = true
	st.sr.FuncPost = postTestQueryNotExecuting
	if err = st.addLog(&telemetryData{
		Message: map[string]string{
			typeKey:    "client_telemetry_type",
			queryIDKey: "123",
		},
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	err = st.sendBatch()
	if err == nil {
		t.Fatal("should have failed")
	}
	if st.enabled == true {
		t.Fatal("telemetry should be disabled")
	}

	st.enabled = true
	st.sr.FuncPost = postTestSuccessButInvalidJSON
	if err = st.addLog(&telemetryData{
		Message: map[string]string{
			typeKey:    "client_telemetry_type",
			queryIDKey: "123",
		},
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	err = st.sendBatch()
	if err == nil {
		t.Fatal("should have failed")
	}
	if st.enabled == true {
		t.Fatal("telemetry should be disabled")
	}
}

func TestTelemetryDisabled(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	sr := &snowflakeRestful{
		FuncPost:      postTestAppBadGatewayError,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	st := &snowflakeTelemetry{
		sr:        sr,
		mutex:     &sync.Mutex{},
		enabled:   false, // disable
		flushSize: defaultFlushSize,
	}
	if err = st.addLog(&telemetryData{
		Message: map[string]string{
			typeKey:    "client_telemetry_type",
			queryIDKey: "123",
		},
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}); err == nil {
		t.Fatal("should have failed")
	}
	st.enabled = true
	if err = st.addLog(&telemetryData{
		Message: map[string]string{
			typeKey:    "client_telemetry_type",
			queryIDKey: "123",
		},
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	st.enabled = false
	err = st.sendBatch()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestAddLogError(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}

	sr := &snowflakeRestful{
		FuncPost:      funcPostTelemetryRespFail,
		TokenAccessor: getSimpleTokenAccessor(),
	}

	st := &snowflakeTelemetry{
		sr:        sr,
		mutex:     &sync.Mutex{},
		enabled:   true,
		flushSize: 1,
	}

	if err = st.addLog(&telemetryData{
		Message: map[string]string{
			typeKey:    "client_telemetry_type",
			queryIDKey: "123",
		},
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}); err == nil {
		t.Fatal("should have failed")
	}
}
