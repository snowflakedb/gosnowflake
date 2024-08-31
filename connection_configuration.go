// Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"encoding/base64"
	"os"
	"strconv"
	"strings"
	"time"

	path "path/filepath"

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
	var err error = &SnowflakeError{
		Number:      ErrCodeTomlFileParsingFailed,
		Message:     errMsgFailedToParseTomlFile,
		MessageArgs: []interface{}{cfg.Host},
	}
	var v, tokenPath string
	for key, value := range connection {
		switch strings.ToLower(key) {
		case "user", "username":
			cfg.User, ok = value.(string)
			if !ok {
				// //errorinterface
				return err
			}
		case "password":
			cfg.Password, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "host":
			cfg.Host, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "account":
			cfg.Account, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "warehouse":
			cfg.Warehouse, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "database":
			cfg.Database, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "schema":
			cfg.Schema, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "role":
			cfg.Role, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "region":
			cfg.Region, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "protocol":
			cfg.Protocol, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "passcode":
			cfg.Passcode, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "port":
			cfg.Port, err = parseInt(value)
			if err != nil {
				//errorinterface
				return err
			}
		case "passcodeInPassword":
			cfg.PasscodeInPassword, err = parseBool(value)
			if err != nil {
				//errorinterface
				return err
			}
		case "clientTimeout":
			cfg.ClientTimeout, err = parseDuration(value)
			if err != nil {
				//errorinterface
				return err
			}
		case "jwtClientTimeout":
			cfg.JWTClientTimeout, err = parseDuration(value)
			if err != nil {
				//errorinterface
				return err
			}
		case "loginTimeout":
			cfg.LoginTimeout, err = parseDuration(value)
			if err != nil {
				//errorinterface
				return err
			}
		case "requestTimeout":
			cfg.RequestTimeout, err = parseDuration(value)
			if err != nil {
				//errorinterface
				return err
			}
		case "jwtTimeout":
			cfg.JWTExpireTimeout, err = parseDuration(value)
			if err != nil {
				//errorinterface
				return err
			}
		case "externalBrowserTimeout":
			cfg.ExternalBrowserTimeout, err = parseDuration(value)
			if err != nil {
				//errorinterface
				return err
			}
		case "maxRetryCount":
			cfg.MaxRetryCount, err = parseInt(value)
			if err != nil {
				//errorinterface
				return err
			}
		case "application":
			cfg.Application, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "authenticator":
			v, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
			err = determineAuthenticatorType(cfg, v)
			if err != nil {
				//errorinterface
				return err
			}
		case "insecureMode":
			cfg.InsecureMode, err = parseBool(value)
			if err != nil {
				//errorinterface
				return err
			}
		case "ocspFailOpen":
			vv, err = parseBool(value)
			if err != nil {
				//errorinterface
				return err
			}
			if vv {
				cfg.OCSPFailOpen = OCSPFailOpenTrue
			} else {
				cfg.OCSPFailOpen = OCSPFailOpenFalse
			}

		case "token":
			cfg.Token, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "privateKey":
			v, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
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
				//errorinterface
				return err
			}
		case "validateDefaultParameters":
			vv, err = parseBool(value)
			if err != nil {
				//errorinterface
				return err
			}
			if vv {
				cfg.ValidateDefaultParameters = ConfigBoolTrue
			} else {
				cfg.ValidateDefaultParameters = ConfigBoolFalse
			}
		case "clientRequestMfaToken":
			vv, err = parseBool(value)
			if err != nil {
				//errorinterface
				return err
			}
			if vv {
				cfg.ClientRequestMfaToken = ConfigBoolTrue
			} else {
				cfg.ClientRequestMfaToken = ConfigBoolFalse
			}
		case "clientStoreTemporaryCredential":
			vv, err = parseBool(value)
			if err != nil {
				//errorinterface
				return err
			}
			if vv {
				cfg.ClientStoreTemporaryCredential = ConfigBoolTrue
			} else {
				cfg.ClientStoreTemporaryCredential = ConfigBoolFalse
			}
		case "tracing":
			cfg.Tracing, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "tmpDirPath":
			cfg.TmpDirPath, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "disableQueryContextCache":
			vv, err = parseBool(value)
			if err != nil {
				//errorinterface
				return err
			}
			cfg.DisableQueryContextCache = vv
		case "includeRetryReason":
			vv, err = parseBool(value)
			if err != nil {
				//errorinterface
				return err
			}
			if vv {
				cfg.IncludeRetryReason = ConfigBoolTrue
			} else {
				cfg.IncludeRetryReason = ConfigBoolFalse
			}
		case "clientConfigFile":
			cfg.ClientConfigFile, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		case "disableConsoleLogin":
			vv, err = parseBool(value)
			if err != nil {
				//errorinterface
				return err
			}
			if vv {
				cfg.DisableConsoleLogin = ConfigBoolTrue
			} else {
				cfg.DisableConsoleLogin = ConfigBoolFalse
			}
		case "disableSamlURLCheck":
			vv, err = parseBool(value)
			if err != nil {
				//errorinterface
				return err
			}
			if vv {
				cfg.DisableSamlURLCheck = ConfigBoolTrue
			} else {
				cfg.DisableSamlURLCheck = ConfigBoolFalse
			}
		case "token_file_path":
			tokenPath, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
		default:
			var param string
			param, ok = value.(string)
			if !ok {
				//errorinterface
				return err
			}
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
	var num int
	var err, parseErr error
	parseErr = &SnowflakeError{
		Number:      ErrCodeTomlFileParsingFailed,
		Message:     errMsgFailedToParseTomlFile,
		MessageArgs: []interface{}{i},
	}
	if v, ok = i.(string); !ok {
		if num, ok = i.(int); !ok {
			return 0, parseErr
		}
		return num, nil
	}
	num, err = strconv.Atoi(v)
	if err != nil {
		return 0, parseErr
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
	if v, ok = i.(string); !ok {
		if vv, ok = i.(bool); !ok {
			return false, parseErr
		}
		return vv, nil
	}
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
	if v, ok = i.(string); !ok {
		if num, err = parseInt(i); err != nil {
			return time.Duration(0), parseErr
		}
		t = int64(num)
		return time.Duration(t * int64(time.Second)), nil
	}
	t, err = strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Duration(0), parseErr
	}
	return time.Duration(t * int64(time.Second)), nil
}

func readToken(tokenPath string) (string, error) {
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
