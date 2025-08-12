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
		assertNilF(t, err, "failed to heartbeat and renew session")

		heartbeat.restful.FuncPost = postTestError
		err = heartbeat.heartbeatMain()
		assertNotNilF(t, err, "should have failed to start heartbeat")
		assertEqualE(t, err.Error(), "failed to run post method")

		heartbeat.restful.FuncPost = postTestSuccessButInvalidJSON
		err = heartbeat.heartbeatMain()
		assertNotNilF(t, err, "should have failed to start heartbeat")
		assertHasPrefixE(t, err.Error(), "invalid character")

		heartbeat.restful.FuncPost = postTestAppForbiddenError
		err = heartbeat.heartbeatMain()
		assertNotNilF(t, err, "should have failed to start heartbeat")
		driverErr, ok := err.(*SnowflakeError)
		assertTrueF(t, ok, "connection should be snowflakeConn")
		assertEqualE(t, driverErr.Number, ErrFailedToHeartbeat)
	})
}

func TestHeartbeatStartAndStop(t *testing.T) {
	createDSNWithClientSessionKeepAlive()
	config, err := ParseDSN(dsn)
	assertNilF(t, err, "failed to parse dsn")
	driver := SnowflakeDriver{}
	db, err := driver.OpenWithConfig(context.Background(), *config)
	assertNilF(t, err, "failed to open with config")

	conn, ok := db.(*snowflakeConn)
	assertTrueF(t, ok, "connection should be snowflakeConn")
	assertNotNilF(t, conn.rest, "heartbeat should not be nil")
	assertNotNilF(t, conn.rest.HeartBeat, "heartbeat should not be nil")

	err = db.Close()
	assertNilF(t, err, "should not cause error in Close")
	assertNilF(t, conn.rest.HeartBeat, "heartbeat should be nil")
}
