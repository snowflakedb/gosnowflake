// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"testing"
)

func TestUnitPostHeartbeat(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		// send heartbeat call and renew expired session
		sr := &snowflakeRestful{
			FuncPost:         postTestRenew,
			FuncRenewSession: renewSessionTest,
			TokenAccessor:    getSimpleTokenAccessor(),
			RequestTimeout:   0,
		}
		heartbeat := &heartbeat{
			restful: sr,
		}
		err := heartbeat.heartbeatMain()
		if err != nil {
			t.Fatalf("failed to heartbeat and renew session. err: %v", err)
		}

		heartbeat.restful.FuncPost = postTestSuccessButInvalidJSON
		err = heartbeat.heartbeatMain()
		if err == nil {
			t.Fatal("should have failed")
		}

		heartbeat.restful.FuncPost = postTestAppForbiddenError
		err = heartbeat.heartbeatMain()
		if err == nil {
			t.Fatal("should have failed")
		}
		driverErr, ok := err.(*SnowflakeError)
		if !ok {
			t.Fatalf("should be snowflake error. err: %v", err)
		}
		if driverErr.Number != ErrFailedToHeartbeat {
			t.Fatalf("unexpected error code. expected: %v, got: %v", ErrFailedToHeartbeat, driverErr.Number)
		}
	})
}

func TestHeartbeatStartAndStop(t *testing.T) {
	createDSNWithClientSessionKeepAlive()
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("failed to parse dsn. err: %v", err)
	}
	driver := SnowflakeDriver{}
	db, err := driver.OpenWithConfig(context.Background(), *config)
	if err != nil {
		t.Fatalf("failed to open with config. config: %v, err: %v", config, err)
	}

	conn, ok := db.(*snowflakeConn)
	if ok && conn.isHeartbeatNil() {
		t.Fatalf("heartbeat should not be nil")
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("should not cause error in Close. err: %v", err)
	}
	if ok && !conn.isHeartbeatNil() {
		t.Fatalf("heartbeat should be nil")
	}
}
