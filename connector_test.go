// Copyright (c) 2020 Snowflake Computing Inc. All right reserved.

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

func (d noopTestDriver) Open(_ string) (driver.Conn, error) {
	return nil, nil
}

func (d noopTestDriver) OpenWithConfig(_ context.Context, config Config) (driver.Conn, error) {
	d.config = config
	return d.conn, nil
}

func TestConnector(t *testing.T) {
	conn := snowflakeConn{}
	mock := noopTestDriver{conn: &conn}
	createDSN("UTC")
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("Failed to parse dsn %s", dsn)
	}
	connector := NewConnector(mock, *config)
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
