// Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestTelemetryAddLog(t *testing.T) {
	config, _ := ParseDSN(dsn)
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
			Timestamp: time.Now().UnixNano(),
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
	config, _ := ParseDSN(dsn)
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
	config, _ := ParseDSN(dsn)
	config.DisableTelemetry = true
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	if _, err = sc.Query("select 1", nil); err != nil {
		t.Fatal(err)
	}
}
