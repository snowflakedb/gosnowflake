// Copyright (c) 2020-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
	"time"
)

type noopTestDriver struct {
	config Config
	conn   *snowflakeConn
}

func (d *noopTestDriver) Open(_ string) (driver.Conn, error) {
	return nil, nil
}

func (d *noopTestDriver) OpenWithConfig(_ context.Context, config Config) (driver.Conn, error) {
	d.config = config
	return d.conn, nil
}

func TestConnector(t *testing.T) {
	conn := snowflakeConn{}
	mock := noopTestDriver{conn: &conn}
	createDSN("UTC")
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal("Failed to parse dsn")
	}
	connector := NewConnector(&mock, *config)
	connection, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect error %s", err)
	}
	if connection != &conn {
		t.Fatalf("Connection mismatch %s", connection)
	}
	assertNilF(t, fillMissingConfigParameters(config))
	if reflect.DeepEqual(config, mock.config) {
		t.Fatalf("Config should be equal, expected %v, actual %v", config, mock.config)
	}
	if connector.Driver() == nil {
		t.Fatalf("Missing driver")
	}
}

func TestConnectorWithMissingConfig(t *testing.T) {
	conn := snowflakeConn{}
	mock := noopTestDriver{conn: &conn}
	config := Config{
		User:     "u",
		Password: "p",
		Account:  "",
	}
	expectedErr := errEmptyAccount()

	connector := NewConnector(&mock, config)
	_, err := connector.Connect(context.Background())
	assertNotNilF(t, err, "the connection should have failed due to empty account.")

	driverErr, ok := err.(*SnowflakeError)
	assertTrueF(t, ok, "should be a SnowflakeError")
	assertEqualE(t, driverErr.Number, expectedErr.Number)
	assertEqualE(t, driverErr.Message, expectedErr.Message)
}

func TestConnectorCancelContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// restore the logger output after the test is complete.
	logger := GetLogger().(*defaultLogger)
	initialOutput := logger.inner.Out
	defer logger.SetOutput(initialOutput)

	// write logs to temp buffer so we can assert log output.
	var buf bytes.Buffer
	logger.SetOutput(&buf)

	// pass in our context which should only be used for establishing the initial connection; not persisted.
	sfConn, err := buildSnowflakeConn(ctx, Config{Params: make(map[string]*string)})
	assertNilF(t, err)

	// patch close handler
	sfConn.rest = &snowflakeRestful{
		FuncCloseSession: func(ctx context.Context, sr *snowflakeRestful, d time.Duration) error {
			return ctx.Err()
		},
	}

	// cancel context BEFORE closing the connection.
	// this may occur if the *snowflakeConn was spawned by a QueryContext(), and the query has completed.
	cancel()
	assertNilF(t, sfConn.Close())

	// if the following log is emitted, the connection is holding onto context that it shouldn't be.
	assertFalseF(t, strings.Contains(buf.String(), "context canceled"))
}
