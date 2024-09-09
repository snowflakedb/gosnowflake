// Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"encoding/base64"
	"errors"
	"os"
	path "path/filepath"
	"strconv"
	"strings"
	"time"

	toml "github.com/BurntSushi/toml"
)

// LoadConnectionConfig returns connection configs loaded from the toml file.
// By default, SNOWFLAKE_HOME(toml file path) is os.home/snowflake
// and SNOWFLAKE_DEFAULT_CONNECTION_NAME(DSN) is 'default'
func LoadConnectionConfig() (*Config, error) {
	cfg := &Config{
		Params:        make(map[string]*string),
		Authenticator: AuthTypeSnowflake, // Default to snowflake
	}
	var dsn string = getConnectionDSN(os.Getenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME"))
	snowflakeConfigDir, err := getTomlFilePath(os.Getenv("SNOWFLAKE_HOME"))
	if err != nil {
		return nil, err
	}
	tomlFilePath := path.Join(snowflakeConfigDir, "connections.toml")
	err = validateFilePermission(tomlFilePath)
	if err != nil {
		return nil, err
	}
	var tomlInfo = make(map[string]interface{})

	_, err = toml.DecodeFile(tomlFilePath, &tomlInfo)
	if err != nil {
		return nil, err
	}
	connectionName, exist := tomlInfo[dsn]
	if !exist {
		err = &SnowflakeError{
			Number:  ErrCodeFailedToFindDSNInToml,
			Message: errMsgFailedToFindDSNInTomlFile,
		}
		return nil, err
	}
	connectionConfig, ok := connectionName.(map[string]interface{})
	if !ok {
		return nil, err
	}

	err = parseToml(cfg, connectionConfig)
	if err != nil {
		return nil, err
	}
	return cfg, err
}

func parseToml(cfg *Config, connection map[string]interface{}) error {
	var ok, vv bool
	var err, parsingErr error
	err = &SnowflakeError{
		Number:  ErrCodeTomlFileParsingFailed,
		Message: errMsgFailedToParseTomlFile,
	}
	var v, tokenPath string
	for key, value := range connection {
		switch strings.ToLower(key) {
		case "user", "username":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.User = value.(string)
		case "password":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Password = value.(string)
		case "host":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Host = value.(string)
		case "account":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Account = value.(string)
		case "warehouse":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Warehouse = value.(string)
		case "database":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Database = value.(string)
		case "schema":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Schema = value.(string)
		case "role":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Role = value.(string)
		case "region":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Region = value.(string)
		case "protocol":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Protocol = value.(string)
		case "passcode":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Passcode = value.(string)
		case "port":

			if cfg.Port, parsingErr = parseInt(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "passcodeinpassword":

			if cfg.PasscodeInPassword, parsingErr = parseBool(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "clienttimeout":
			if cfg.ClientTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "jwtclienttimeout":
			if cfg.JWTClientTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "logintimeout":

			if cfg.LoginTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "requesttimeout":
			if cfg.RequestTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "jwttimeout":
			if cfg.JWTExpireTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "externalbrowsertimeout":
			if cfg.ExternalBrowserTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "maxretrycount":

			if cfg.MaxRetryCount, parsingErr = parseInt(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "application":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Application = value.(string)
		case "authenticator":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			v = value.(string)
			err = determineAuthenticatorType(cfg, v)
			if err != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "insecuremode":
			if cfg.InsecureMode, parsingErr = parseBool(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "ocspfailopen":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.OCSPFailOpen = OCSPFailOpenTrue
			} else {
				cfg.OCSPFailOpen = OCSPFailOpenFalse
			}

		case "token":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Token = value.(string)
		case "privatekey":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			v = value.(string)
			var decodeErr error
			block, decodeErr := base64.URLEncoding.DecodeString(v)
			if decodeErr != nil {
				err = &SnowflakeError{
					Number:  ErrCodePrivateKeyParseError,
					Message: "Base64 decode failed",
				}
				return err
			}
			cfg.PrivateKey, err = parsePKCS8PrivateKey(block)
			if err != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
		case "validatedefaultparameters":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.ValidateDefaultParameters = ConfigBoolTrue
			} else {
				cfg.ValidateDefaultParameters = ConfigBoolFalse
			}
		case "clientrequestmfatoken":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.ClientRequestMfaToken = ConfigBoolTrue
			} else {
				cfg.ClientRequestMfaToken = ConfigBoolFalse
			}
		case "clientstoretemporarycredential":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.ClientStoreTemporaryCredential = ConfigBoolTrue
			} else {
				cfg.ClientStoreTemporaryCredential = ConfigBoolFalse
			}
		case "tracing":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.Tracing = value.(string)
		case "tmpdirpath":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.TmpDirPath = value.(string)
		case "disablequerycontextcache":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.DisableQueryContextCache = vv
		case "includeretryreason":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.IncludeRetryReason = ConfigBoolTrue
			} else {
				cfg.IncludeRetryReason = ConfigBoolFalse
			}
		case "clientconfigfile":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.ClientConfigFile = value.(string)
		case "disableconsolelogin":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.DisableConsoleLogin = ConfigBoolTrue
			} else {
				cfg.DisableConsoleLogin = ConfigBoolFalse
			}
		case "disablesamlurlcheck":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.DisableSamlURLCheck = ConfigBoolTrue
			} else {
				cfg.DisableSamlURLCheck = ConfigBoolFalse
			}
		case "token_file_path":
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			tokenPath = value.(string)
		default:
			var param string
			if _, ok = value.(string); !ok {
				err.(*SnowflakeError).MessageArgs = []interface{}{key, value}
				return err
			}
			param = value.(string)
			cfg.Params[urlDecodeIfNeeded(key)] = &param
		}
	}
	if shouldReadTokenFromFile(cfg) {
		v, err := readToken(tokenPath)
		if err != nil {
			return err
		}
		cfg.Token = v
	}
	return nil
}

func parseInt(i interface{}) (int, error) {
	var v string
	var ok bool
	var num int = 0
	var err error = errors.New("parse Error")
	if _, ok = i.(string); !ok {
		if _, ok = i.(int); !ok {
			return num, err
		}
		num = i.(int)
		return num, nil
	}
	v = i.(string)

	if num, err = strconv.Atoi(v); err != nil {
		return num, err
	}
	return num, nil
}

func parseBool(i interface{}) (bool, error) {
	var v string
	var ok, vv bool
	var err, parseErr error
	parseErr = &SnowflakeError{
		Number:      ErrCodeTomlFileParsingFailed,
		Message:     errMsgFailedToParseTomlFile,
		MessageArgs: []interface{}{i},
	}
	if _, ok = i.(string); !ok {
		if _, ok = i.(bool); !ok {
			return false, parseErr
		}
		vv = i.(bool)
		return vv, nil
	}
	v = i.(string)
	vv, err = strconv.ParseBool(v)
	if err != nil {
		return false, parseErr
	}
	return vv, nil
}

func parseDuration(i interface{}) (time.Duration, error) {
	var v string
	var ok bool
	var num int
	var t int64
	var err, parseErr error
	parseErr = &SnowflakeError{
		Number:      ErrCodeTomlFileParsingFailed,
		Message:     errMsgFailedToParseTomlFile,
		MessageArgs: []interface{}{i},
	}
	if _, ok = i.(string); !ok {
		if num, err = parseInt(i); err != nil {
			return time.Duration(0), parseErr
		}
		t = int64(num)
		return time.Duration(t * int64(time.Second)), nil
	}
	v = i.(string)
	t, err = strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Duration(0), parseErr
	}
	return time.Duration(t * int64(time.Second)), nil
}

func readToken(tokenPath string) (string, error) {
	if tokenPath == "" {
		tokenPath = "./snowflake/session/token"
	}

	if !path.IsAbs(tokenPath) {
		snowflakeConfigDir, err := getTomlFilePath(os.Getenv("SNOWFLAKE_HOME"))
		if err != nil {
			return "", err
		}
		tokenPath = path.Join(snowflakeConfigDir, tokenPath)
	}
	err := validateFilePermission(tokenPath)
	if err != nil {
		return "", err
	}
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		return "", err
	}
	return string(token), nil
}

func getTomlFilePath(filePath string) (string, error) {
	var dir string
	if len(filePath) != 0 {
		dir = filePath
		if path.IsAbs(dir) {
			return dir, nil
		}
	} else {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		dir = path.Join(homeDir, "snowflake")
	}
	absDir, err := path.Abs(dir)
	if err != nil {
		return "", err
	}
	return absDir, nil
}

func getConnectionDSN(dsn string) string {
	if len(dsn) != 0 {
		return dsn
	}
	return "default"
}

func validateFilePermission(filePath string) error {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	permission := fileInfo.Mode().Perm()
	if permission != 0o600 {
		return err
	}
	return nil
}

func shouldReadTokenFromFile(cfg *Config) bool {
	return cfg != nil && cfg.Authenticator == AuthTypeOAuth && len(cfg.Token) == 0
}
