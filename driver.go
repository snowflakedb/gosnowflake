// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"database/sql"
	"database/sql/driver"
	"net/http"
	"time"
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
	st := SnowflakeTransport
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
		Authenticator:       sc.cfg.Authenticator,
		LoginTimeout:        sc.cfg.LoginTimeout,
		RequestTimeout:      sc.cfg.RequestTimeout,
		FuncPost:            postRestful,
		FuncGet:             getRestful,
		FuncPostQuery:       postRestfulQuery,
		FuncPostQueryHelper: postRestfulQueryHelper,
		FuncRenewSession:    renewRestfulSession,
		FuncPostAuth:        postAuth,
		FuncCloseSession:    closeSession,
		FuncCancelQuery:     cancelQuery,
		FuncPostAuthSAML:    postAuthSAML,
		FuncPostAuthOKTA:    postAuthOKTA,
		FuncGetSSO:          getSSO,
	}
	var authData *authResponseMain
	var samlResponse []byte
	if sc.cfg.Authenticator != "snowflake" {
		samlResponse, err = authenticateBySAML(sc.rest, sc.cfg.Authenticator, sc.cfg.Application, sc.cfg.Account, sc.cfg.User, sc.cfg.Password)
		if err != nil {
			sc.cleanup()
			return nil, err
		}
	}
	authData, err = authenticate(
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
		sc.cfg.Params,
		samlResponse,
		"",
		"",
	)
	if err != nil {
		sc.cleanup()
		return nil, err
	}
	glog.V(2).Infof("Auth Data: %v", authData)
	sc.cfg.Database = authData.SessionInfo.DatabaseName
	sc.cfg.Schema = authData.SessionInfo.SchemaName
	sc.cfg.Role = authData.SessionInfo.RoleName
	sc.cfg.Warehouse = authData.SessionInfo.WarehouseName
	sc.populateSessionParameters(authData.Parameters)
	return sc, nil
}

func init() {
	sql.Register("snowflake", &SnowflakeDriver{})
}
