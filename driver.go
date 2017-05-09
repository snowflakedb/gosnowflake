// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/http"
	"time"

	"github.com/golang/glog"
)

// SnowflakeDriver is a context of Go Driver
type SnowflakeDriver struct{}

// Open creates a new connection.
func (d SnowflakeDriver) Open(dsn string) (driver.Conn, error) {
	glog.V(2).Info("Open")
	var err error
	sc := &snowflakeConn{
		SequeceCounter: 0,
	}
	sc.cfg, err = ParseDSN(dsn)
	if err != nil {
		sc.cleanup()
		return nil, err
	}
	st := snowflakeTransport
	if sc.cfg.InsecureMode {
		// no revocation check with OCSP. Think twice when you want to enable this option.
		st = snowflakeInsecureTransport
	}
	proxyURL, err := proxyURL(proxyHost, proxyPort, proxyUser, proxyPassword)
	if err != nil {
		return nil, err
	}
	if proxyURL != nil {
		st.Proxy = http.ProxyURL(proxyURL)
		glog.V(2).Infof("proxy: %v", proxyURL)
	}
	// authenticate
	sc.rest = &snowflakeRestful{
		Host:     sc.cfg.Host,
		Port:     sc.cfg.Port,
		Protocol: sc.cfg.Protocol,
		Client: &http.Client{
			Timeout:   60 * time.Second, // each request timeout
			Transport: st,
		},
		Authenticator:  sc.cfg.Authenticator,
		LoginTimeout:   sc.cfg.LoginTimeout,
		ConnectTimeout: sc.cfg.ConnectTimeout,
		RequestTimeout: sc.cfg.RequestTimeout,
	}
	var sessionInfo *AuthResponseSessionInfo
	var samlResponse []byte
	if sc.cfg.Authenticator != "snowflake" {
		samlResponse, err = authenticateBySAML(sc.rest, sc.cfg.Authenticator, sc.cfg.Application, sc.cfg.Account, sc.cfg.User, sc.cfg.Password)
		if err != nil {
			// TODO Better error handing?
			sc.cleanup()
			return nil, err
		}
	}
	sessionInfo, err = authenticate(
		sc.rest,
		sc.cfg.User,
		sc.cfg.Password,
		sc.cfg.Account,
		sc.cfg.Database,
		sc.cfg.Schema,
		sc.cfg.Warehouse,
		sc.cfg.Role,
		sc.cfg.Passcode,
		sc.cfg.PasscodeInPassword,
		sc.cfg.Application,
		samlResponse,
		"",
		"",
	)
	if err != nil {
		// TODO Better error handling
		sc.cleanup()
		return nil, err
	}

	// snowflakeTransport

	glog.V(2).Infof("SessionInfo: %v", sessionInfo)
	sc.cfg.Database = sessionInfo.DatabaseName
	sc.cfg.Schema = sessionInfo.SchemaName
	sc.cfg.Role = sessionInfo.RoleName
	sc.cfg.Warehouse = sessionInfo.WarehouseName

	v := sc.cfg.Params["timezone"]
	if v != "" {
		glog.V(2).Infof("Setting Timezone: %s", sc.cfg.Params["timezone"])
		p := make([]driver.Value, 0)
		_, err := sc.Exec(fmt.Sprintf("ALTER SESSION SET TIMEZONE='%s'", sc.cfg.Params["timezone"]), p)
		if err != nil {
			sc.cleanup()
			return nil, err
		}
	}
	return sc, nil
}

func init() {
	sql.Register("snowflake", &SnowflakeDriver{})
}
