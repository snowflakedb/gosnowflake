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
	dsn := getConnectionDSN(os.Getenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME"))
	snowflakeConfigDir, err := getTomlFilePath(os.Getenv("SNOWFLAKE_HOME"))
	if err != nil {
		return nil, err
	}
	tomlFilePath := path.Join(snowflakeConfigDir, "connections.toml")
	err = validateFilePermission(tomlFilePath)
	if err != nil {
		return nil, err
	}
	tomlInfo := make(map[string]interface{})
	_, err = toml.DecodeFile(tomlFilePath, &tomlInfo)
	if err != nil {
		return nil, err
	}
	connectionName, exist := tomlInfo[dsn]
	if !exist {
		return nil, &SnowflakeError{
			Number:  ErrCodeFailedToFindDSNInToml,
			Message: errMsgFailedToFindDSNInTomlFile,
		}
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
	var v, tokenPath string
	var parsingErr error
	var vv bool
	err := &SnowflakeError{
		Number:  ErrCodeTomlFileParsingFailed,
		Message: errMsgFailedToParseTomlFile,
	}
	for key, value := range connection {
		switch strings.ToLower(key) {
		case "user", "username":
			cfg.User, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "password":
			cfg.Password, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "host":
			cfg.Host, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "account":
			cfg.Account, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "warehouse":
			cfg.Warehouse, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "database":
			cfg.Database, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "schema":
			cfg.Schema, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "role":
			cfg.Role, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "region":
			cfg.Region, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "protocol":
			cfg.Protocol, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "passcode":
			cfg.Passcode, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "port":
			if cfg.Port, parsingErr = parseInt(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "passcodeinpassword":
			if cfg.PasscodeInPassword, parsingErr = parseBool(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "clienttimeout":
			if cfg.ClientTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "jwtclienttimeout":
			if cfg.JWTClientTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "logintimeout":
			if cfg.LoginTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "requesttimeout":
			if cfg.RequestTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "jwttimeout":
			if cfg.JWTExpireTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "externalbrowsertimeout":
			if cfg.ExternalBrowserTimeout, parsingErr = parseDuration(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "maxretrycount":
			if cfg.MaxRetryCount, parsingErr = parseInt(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "application":
			cfg.Application, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "authenticator":
			v, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
			parsingErr = determineAuthenticatorType(cfg, v)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "insecuremode":
			if cfg.InsecureMode, parsingErr = parseBool(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "ocspfailopen":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.OCSPFailOpen = OCSPFailOpenTrue
			} else {
				cfg.OCSPFailOpen = OCSPFailOpenFalse
			}

		case "token":
			cfg.Token, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "privatekey":
			v, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
			block, decodeErr := base64.URLEncoding.DecodeString(v)
			if decodeErr != nil {
				err = &SnowflakeError{
					Number:  ErrCodePrivateKeyParseError,
					Message: "Base64 decode failed",
				}
				return err
			}
			cfg.PrivateKey, parsingErr = parsePKCS8PrivateKey(block)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "validatedefaultparameters":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.ValidateDefaultParameters = ConfigBoolTrue
			} else {
				cfg.ValidateDefaultParameters = ConfigBoolFalse
			}
		case "clientrequestmfatoken":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.ClientRequestMfaToken = ConfigBoolTrue
			} else {
				cfg.ClientRequestMfaToken = ConfigBoolFalse
			}
		case "clientstoretemporarycredential":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.ClientStoreTemporaryCredential = ConfigBoolTrue
			} else {
				cfg.ClientStoreTemporaryCredential = ConfigBoolFalse
			}
		case "tracing":
			cfg.Tracing, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "tmpdirpath":
			cfg.TmpDirPath, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "disablequerycontextcache":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
			cfg.DisableQueryContextCache = vv
		case "includeretryreason":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.IncludeRetryReason = ConfigBoolTrue
			} else {
				cfg.IncludeRetryReason = ConfigBoolFalse
			}
		case "clientconfigfile":
			cfg.ClientConfigFile, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		case "disableconsolelogin":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.DisableConsoleLogin = ConfigBoolTrue
			} else {
				cfg.DisableConsoleLogin = ConfigBoolFalse
			}
		case "disablesamlurlcheck":
			if vv, parsingErr = parseBool(value); parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
			if vv {
				cfg.DisableSamlURLCheck = ConfigBoolTrue
			} else {
				cfg.DisableSamlURLCheck = ConfigBoolFalse
			}
		case "token_file_path":
			tokenPath, parsingErr = parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
				return err
			}
		default:
			param, parsingErr := parseString(value)
			if parsingErr != nil {
				err.MessageArgs = []interface{}{key, value}
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
	if _, ok := i.(string); !ok {
		if _, ok := i.(int); !ok {
			return 0, errors.New("failed to parse the value to integer")
		}
		num := i.(int)
		return num, nil
	}
	v := i.(string)
	num, err := strconv.Atoi(v)

	if err != nil {
		return num, err
	}
	return num, nil
}

func parseBool(i interface{}) (bool, error) {
	v, ok := i.(string)
	if !ok {
		if _, ok := i.(bool); !ok {
			return false, errors.New("failed to parse the value to boolean")
		}
		vv := i.(bool)
		return vv, nil
	}
	vv, err := strconv.ParseBool(v)
	if err != nil {
		return false, errors.New("failed to parse the value to boolean")
	}
	return vv, nil
}

func parseDuration(i interface{}) (time.Duration, error) {
	v, ok := i.(string)
	if !ok {
		num, err := parseInt(i)
		if err != nil {
			return time.Duration(0), err
		}
		t := int64(num)
		return time.Duration(t * int64(time.Second)), nil
	}
	t, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Duration(0), err
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

func parseString(i interface{}) (string, error) {
	v, ok := i.(string)
	if !ok {
		return "", errors.New("failed to convert the value to string")
	}
	return v, nil
}

func getTomlFilePath(filePath string) (string, error) {
	if len(filePath) != 0 {
		if path.IsAbs(filePath) {
			return filePath, nil
		}
	} else {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		filePath = path.Join(homeDir, "snowflake")
	}
	absDir, err := path.Abs(filePath)
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
	if isWindows {
		return nil
	}
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	if permission := fileInfo.Mode().Perm(); permission != os.FileMode(0600) {
		return errors.New("your access to the file was denied")
	}
	return nil
}

func shouldReadTokenFromFile(cfg *Config) bool {
	return cfg != nil && cfg.Authenticator == AuthTypeOAuth && len(cfg.Token) == 0
}
