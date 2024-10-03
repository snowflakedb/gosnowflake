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

const (
	snowflakeConnectionName = "SNOWFLAKE_DEFAULT_CONNECTION_NAME"
	snowflakeHome           = "SNOWFLAKE_HOME"
	defaultTokenPath        = "/snowflake/session/token"
)

// LoadConnectionConfig returns connection configs loaded from the toml file.
// By default, SNOWFLAKE_HOME(toml file path) is os.snowflakeHome/.snowflake
// and SNOWFLAKE_DEFAULT_CONNECTION_NAME(DSN) is 'default'
func loadConnectionConfig() (*Config, error) {
	cfg := &Config{
		Params:        make(map[string]*string),
		Authenticator: AuthTypeSnowflake, // Default to snowflake
	}
	dsn := getConnectionDSN(os.Getenv(snowflakeConnectionName))
	snowflakeConfigDir, err := getTomlFilePath(os.Getenv(snowflakeHome))
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
	dsnMap, exist := tomlInfo[dsn]
	if !exist {
		return nil, &SnowflakeError{
			Number:  ErrCodeFailedToFindDSNInToml,
			Message: errMsgFailedToFindDSNInTomlFile,
		}
	}
	connectionConfig, ok := dsnMap.(map[string]interface{})
	if !ok {
		return nil, err
	}
	err = parseToml(cfg, connectionConfig)
	if err != nil {
		return nil, err
	}
	err = fillMissingConfigParameters(cfg)
	if err != nil {
		return nil, err
	}
	return cfg, err
}

func parseToml(cfg *Config, connection map[string]interface{}) error {
	for key, value := range connection {
		if err := handleSingleParam(cfg, key, value); err != nil {
			return err
		}
	}
	if shouldReadTokenFromFile(cfg) {
		v, err := readToken("")
		if err != nil {
			return err
		}
		cfg.Token = v
	}
	return nil
}

func handleSingleParam(cfg *Config, key string, value interface{}) error {
	var parsingErr error
	var v, tokenPath string
	switch strings.ToLower(key) {
	case "user", "username":
		cfg.User, parsingErr = parseString(value)
	case "password":
		cfg.Password, parsingErr = parseString(value)
	case "host":
		cfg.Host, parsingErr = parseString(value)
	case "account":
		cfg.Account, parsingErr = parseString(value)
	case "warehouse":
		cfg.Warehouse, parsingErr = parseString(value)
	case "database":
		cfg.Database, parsingErr = parseString(value)
	case "schema":
		cfg.Schema, parsingErr = parseString(value)
	case "role":
		cfg.Role, parsingErr = parseString(value)
	case "region":
		cfg.Region, parsingErr = parseString(value)
	case "protocol":
		cfg.Protocol, parsingErr = parseString(value)
	case "passcode":
		cfg.Passcode, parsingErr = parseString(value)
	case "port":
		cfg.Port, parsingErr = parseInt(value)
	case "passcodeinpassword":
		cfg.PasscodeInPassword, parsingErr = parseBool(value)
	case "clienttimeout":
		cfg.ClientTimeout, parsingErr = parseDuration(value)
	case "jwtclienttimeout":
		cfg.JWTClientTimeout, parsingErr = parseDuration(value)
	case "logintimeout":
		cfg.LoginTimeout, parsingErr = parseDuration(value)
	case "requesttimeout":
		cfg.RequestTimeout, parsingErr = parseDuration(value)
	case "jwttimeout":
		cfg.JWTExpireTimeout, parsingErr = parseDuration(value)
	case "externalbrowsertimeout":
		cfg.ExternalBrowserTimeout, parsingErr = parseDuration(value)
	case "maxretrycount":
		cfg.MaxRetryCount, parsingErr = parseInt(value)
	case "application":
		cfg.Application, parsingErr = parseString(value)
	case "authenticator":
		v, parsingErr = parseString(value)
		if err := checkParsingError(parsingErr, key, value); err != nil {
			return err
		}
		parsingErr = determineAuthenticatorType(cfg, v)
	case "insecuremode":
		cfg.InsecureMode, parsingErr = parseBool(value)
	case "ocspfailopen":
		var vv ConfigBool
		vv, parsingErr = parseConfigBool(value)
		if err := checkParsingError(parsingErr, key, value); err != nil {
			return err
		}
		cfg.OCSPFailOpen = OCSPFailOpenMode(vv)
	case "token":
		cfg.Token, parsingErr = parseString(value)
	case "privatekey":
		v, parsingErr = parseString(value)
		if err := checkParsingError(parsingErr, key, value); err != nil {
			return err
		}
		block, decodeErr := base64.URLEncoding.DecodeString(v)
		if decodeErr != nil {
			return &SnowflakeError{
				Number:  ErrCodePrivateKeyParseError,
				Message: "Base64 decode failed",
			}
		}
		cfg.PrivateKey, parsingErr = parsePKCS8PrivateKey(block)
	case "validatedefaultparameters":
		cfg.ValidateDefaultParameters, parsingErr = parseConfigBool(value)
	case "clientrequestmfatoken":
		cfg.ClientRequestMfaToken, parsingErr = parseConfigBool(value)
	case "clientstoretemporarycredential":
		cfg.ClientStoreTemporaryCredential, parsingErr = parseConfigBool(value)
	case "tracing":
		cfg.Tracing, parsingErr = parseString(value)
	case "tmpdirpath":
		cfg.TmpDirPath, parsingErr = parseString(value)
	case "disablequerycontextcache":
		cfg.DisableQueryContextCache, parsingErr = parseBool(value)
	case "includeretryreason":
		cfg.IncludeRetryReason, parsingErr = parseConfigBool(value)
	case "clientconfigfile":
		cfg.ClientConfigFile, parsingErr = parseString(value)
	case "disableconsolelogin":
		cfg.DisableConsoleLogin, parsingErr = parseConfigBool(value)
	case "disablesamlurlcheck":
		cfg.DisableSamlURLCheck, parsingErr = parseConfigBool(value)
	case "token_file_path":
		tokenPath, parsingErr = parseString(value)
		if err := checkParsingError(parsingErr, key, value); err != nil {
			return err
		}
		v, err := readToken(tokenPath)
		if err != nil {
			return err
		}
		cfg.Token = v
	default:
		param, parsingErr := parseString(value)
		if err := checkParsingError(parsingErr, key, value); err != nil {
			return err
		}
		cfg.Params[urlDecodeIfNeeded(key)] = &param
	}
	return checkParsingError(parsingErr, key, value)
}

func checkParsingError(parsingErr error, key string, value interface{}) error {
	if parsingErr != nil {
		err := &SnowflakeError{
			Number:      ErrCodeTomlFileParsingFailed,
			Message:     errMsgFailedToParseTomlFile,
			MessageArgs: []interface{}{key, value},
		}
		return err
	}
	return nil
}

func parseInt(i interface{}) (int, error) {
	v, ok := i.(string)
	if !ok {
		num, ok := i.(int)
		if !ok {
			return 0, errors.New("failed to parse the value to integer")
		}
		return num, nil
	}
	return strconv.Atoi(v)
}

func parseBool(i interface{}) (bool, error) {
	v, ok := i.(string)
	if !ok {
		vv, ok := i.(bool)
		if !ok {
			return false, errors.New("failed to parse the value to boolean")
		}
		return vv, nil
	}
	return strconv.ParseBool(v)
}

func parseConfigBool(i interface{}) (ConfigBool, error) {
	vv, err := parseBool(i)
	if err != nil {
		return ConfigBoolFalse, err
	}
	if vv {
		return ConfigBoolTrue, nil
	}
	return ConfigBoolFalse, nil
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
	return parseTimeout(v)
}

func readToken(tokenPath string) (string, error) {
	if tokenPath == "" {
		tokenPath = defaultTokenPath
	}
	if !path.IsAbs(tokenPath) {
		var err error
		tokenPath, err = path.Abs(tokenPath)
		if err != nil {
			return "", err
		}
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
	if len(filePath) == 0 {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		filePath = path.Join(homeDir, ".snowflake")
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
		return &SnowflakeError{
			Number:      ErrCodeInvalidFilePermission,
			Message:     errMsgInvalidPermissionToTomlFile,
			MessageArgs: []interface{}{permission},
		}
	}
	return nil
}

func shouldReadTokenFromFile(cfg *Config) bool {
	return cfg != nil && cfg.Authenticator == AuthTypeOAuth && len(cfg.Token) == 0
}
