// Copyright (c) 2020-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"reflect"
	"testing"
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
	fillMissingConfigParameters(config)
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
	if !ok {
		t.Fatalf("Snowflake error is expected. err: %v", err.Error())
	}
	if driverErr.Number != expectedErr.Number || driverErr.Message != expectedErr.Message {
		t.Fatalf("Snowflake error did not match. expected: %v, got: %v", expectedErr, driverErr)
	}
}
