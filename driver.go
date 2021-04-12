// Copyright (c) 2017-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// SnowflakeDriver is a context of Go Driver
type SnowflakeDriver struct {
}

// Open creates a new connection.
func (d SnowflakeDriver) Open(dsn string) (driver.Conn, error) {
	logger.Info("Open")
	ctx := context.TODO()
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return d.OpenWithConfig(ctx, *cfg)
}

// OpenWithConfig creates a new connection with the given Config.
func (d SnowflakeDriver) OpenWithConfig(ctx context.Context, config Config) (driver.Conn, error) {
	logger.Info("OpenWithConfig")
	var err error
	sc, err := buildSnowflakeConn(ctx, config)
	if err != nil {
		return nil, err
	}

	err = authenticateWithConfig(sc)
	if err != nil {
		return nil, err
	}
	sc.startHeartBeat()
	sc.internal = &httpClient{sr: sc.rest}
	return sc, nil
}

var logger = CreateDefaultLogger()

func init() {
	sql.Register("snowflake", &SnowflakeDriver{})
	logger.SetLogLevel("error")
}
