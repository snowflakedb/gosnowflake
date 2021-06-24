// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

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
			Message: &telemetryMessage{
				"client_telemetry_type",
				"123",
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
	if err := st.sendBatch(); err != nil {
		t.Fatal(err)
	}
}
