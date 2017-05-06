// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
)

const (
	defaultLoginTimeout   = 60 * time.Second
	defaultConnectTimeout = 60 * time.Second
)

// Config is a configuration parsed from a DSN string
type Config struct {
	Account   string            // Account name
	User      string            // Username
	Password  string            // Password (requires User)
	Database  string            // Database name
	Schema    string            // Schema
	Warehouse string            // Warehouse
	Role      string            // Role
	Params    map[string]string // other connection parameters

	Protocol string // http or https (optional)
	Host     string // hostname (optional)
	Port     int    // port (optional)

	Authenticator      string // snowflake or okta
	Passcode           string
	PasscodeInPassword bool

	ConnectTimeout time.Duration // Dial timeout
	RequestTimeout time.Duration // Request read time
	LoginTimeout   time.Duration // Login timeout

	Application  string // application name.
	InsecureMode bool   // driver doesn't check certificate revocation status

}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (cfg *Config, err error) {
	// New config with some default values
	cfg = &Config{}

	// user[:password]@account/database/schema[?param1=value1&paramN=valueN]
	// or
	// user[:password]@account/database[?param1=value1&paramN=valueN]
	// or
	// user[:password]@host:port/database/schema?account=user_account[?param1=value1&paramN=valueN]

	foundSlash := false
	secondSlash := false
	done := false
	var i int
	posQuestion := len(dsn)
	for i = len(dsn) - 1; i >= 0; i-- {
		switch {
		case dsn[i] == '/':
			foundSlash = true

			// left part is empty if i <= 0
			var j int
			posSecondSlash := i
			if i > 0 {
				for j = i - 1; j >= 0; j-- {
					switch {
					case dsn[j] == '/':
						// second slash
						secondSlash = true
						posSecondSlash = j
					case dsn[j] == '@':
						// username[:password]@...
						cfg.User, cfg.Password = parseUserPassword(j, dsn)
					}
					if dsn[j] == '@' {
						break
					}
				}

				// account or host:port
				cfg.Account, cfg.Host, cfg.Port, err = parseAccountHostPort(j, posSecondSlash, dsn)
				if err != nil {
					return
				}
			}
			// [?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			err = parseParams(cfg, i, dsn)
			if err != nil {
				return
			}
			if secondSlash {
				cfg.Database = dsn[posSecondSlash+1 : i]
				cfg.Schema = dsn[i+1 : posQuestion]
			} else {
				cfg.Database = dsn[posSecondSlash+1 : posQuestion]
				cfg.Schema = "public"
			}
			done = true
		case dsn[i] == '?':
			posQuestion = i
		}
		if done {
			break
		}
	}
	if !foundSlash {
		// no db or schema is specified
		var j int
		for j = len(dsn) - 1; j >= 0; j-- {
			switch {
			case dsn[j] == '@':
				cfg.User, cfg.Password = parseUserPassword(j, dsn)
			case dsn[j] == '?':
				posQuestion = j
			}
			if dsn[j] == '@' {
				break
			}
		}
		cfg.Account, cfg.Host, cfg.Port, err = parseAccountHostPort(j, posQuestion, dsn)
		if err != nil {
			return nil, err
		}
		err = parseParams(cfg, posQuestion-1, dsn)
		if err != nil {
			return
		}
	}

	if cfg.Protocol == "" {
		cfg.Protocol = "https"
	}
	if cfg.Port == 0 {
		cfg.Port = 443
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	if cfg.LoginTimeout == 0 {
		cfg.LoginTimeout = defaultLoginTimeout
	}
	if cfg.Account == "" {
		return nil, ErrEmptyAccount
	}
	if cfg.User == "" {
		return nil, ErrEmptyUsername
	}
	if cfg.Password == "" {
		return nil, ErrEmptyPassword
	}
	if cfg.Application == "" {
		cfg.Application = clientType
	}
	if cfg.Authenticator == "" {
		cfg.Authenticator = "snowflake"
	}

	// unescape parameters
	var s string
	s, err = url.QueryUnescape(cfg.Database)
	if err != nil {
		return nil, err
	}
	cfg.Database = s
	s, err = url.QueryUnescape(cfg.Schema)
	if err != nil {
		return nil, err
	}
	cfg.Schema = s
	s, err = url.QueryUnescape(cfg.Role)
	if err != nil {
		return nil, err
	}
	cfg.Role = s
	s, err = url.QueryUnescape(cfg.Warehouse)
	if err != nil {
		return nil, err
	}
	cfg.Warehouse = s
	glog.V(2).Infof("ParseDSN: %v\n", cfg) // TODO: hide password
	return cfg, nil
}

// parseAccountHostPort parses the DSN string to attempt to get account or host and port.
func parseAccountHostPort(posAt, posSlash int, dsn string) (account, host string, port int, err error) {
	// account or host:port
	var k int
	for k = posAt + 1; k < posSlash; k++ {
		if dsn[k] == ':' {
			port, err = strconv.Atoi(dsn[k+1 : posSlash])
			if err != nil {
				err = &SnowflakeError{
					Number:      ErrCodeFailedToParsePort,
					Message:     errMsgFailedToParsePort,
					MessageArgs: []interface{}{dsn[k+1 : posSlash]},
				}
				return
			}
			break
		}
	}
	host = dsn[posAt+1 : k]
	if port == 0 && !strings.HasSuffix(host, "snowflakecomputing.com") {
		// account name is specified instead of host:port
		account = host
		host = account + ".snowflakecomputing.com"
		port = 443
		posDot := strings.Index(account, ".")
		if posDot >= 0 {
			account = account[:posDot]
		}
	}
	return
}

// parseUserPassword pases the DSN string for username and password
func parseUserPassword(posAt int, dsn string) (user, password string) {
	var k int
	for k = 0; k < posAt; k++ {
		if dsn[k] == ':' {
			password = dsn[k+1 : posAt]
			break
		}
	}
	user = dsn[:k]
	return
}

// parseParams parse parameters
func parseParams(cfg *Config, posQuestion int, dsn string) (err error) {
	for j := posQuestion + 1; j < len(dsn); j++ {
		if dsn[j] == '?' {
			if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
				return
			}
			break
		}
	}
	return
}

// parseDSNParams parses the DSN "query string". Values must be url.QueryEscape'ed
func parseDSNParams(cfg *Config, params string) (err error) {
	glog.V(2).Infof("Query String: %v\n", params)
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}
		var value string
		value, err = url.QueryUnescape(param[1])
		if err != nil {
			return err
		}
		switch param[0] {
		// Disable INFILE whitelist / enable all files
		case "account":
			cfg.Account = value
		case "warehouse":
			cfg.Warehouse = value
		case "database":
			cfg.Database = value
		case "schema":
			cfg.Schema = value
		case "role":
			cfg.Role = value
		case "protocol":
			cfg.Protocol = value
		case "passcode":
			cfg.Passcode = value
		case "passcodeInPassword":
			var vv bool
			vv, err = strconv.ParseBool(value)
			if err != nil {
				return
			}
			cfg.PasscodeInPassword = vv
		case "loginTimeout":
			var vv int64
			vv, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return
			}
			cfg.LoginTimeout = time.Duration(vv * int64(time.Second))
		case "application":
			cfg.Application = value
		case "authenticator":
			cfg.Authenticator = value
		case "insecureMode":
			var vv bool
			vv, err = strconv.ParseBool(value)
			if err != nil {
				return
			}
			cfg.InsecureMode = vv
		default:
			if cfg.Params == nil {
				cfg.Params = make(map[string]string)
			}
			cfg.Params[param[0]] = value
		}
	}
	return
}
