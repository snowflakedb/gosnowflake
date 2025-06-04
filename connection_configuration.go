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

func parseToml(cfg *Config, connectionMap map[string]interface{}) error {
	for key, value := range connectionMap {
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
	var err error
	var v, tokenPath string
	switch strings.ToLower(key) {
	case "user", "username":
		cfg.User, err = parseString(value)
	case "password":
		cfg.Password, err = parseString(value)
	case "host":
		cfg.Host, err = parseString(value)
	case "account":
		cfg.Account, err = parseString(value)
	case "warehouse":
		cfg.Warehouse, err = parseString(value)
	case "database":
		cfg.Database, err = parseString(value)
	case "schema":
		cfg.Schema, err = parseString(value)
	case "role":
		cfg.Role, err = parseString(value)
	case "region":
		cfg.Region, err = parseString(value)
	case "protocol":
		cfg.Protocol, err = parseString(value)
	case "passcode":
		cfg.Passcode, err = parseString(value)
	case "port":
		cfg.Port, err = parseInt(value)
	case "passcodeinpassword", "passcode_in_password":
		cfg.PasscodeInPassword, err = parseBool(value)
	case "clienttimeout", "client_timeout":
		cfg.ClientTimeout, err = parseDuration(value)
	case "jwtclienttimeout", "jwt_client_timeout":
		cfg.JWTClientTimeout, err = parseDuration(value)
	case "logintimeout", "login_timeout":
		cfg.LoginTimeout, err = parseDuration(value)
	case "requesttimeout", "request_timeout":
		cfg.RequestTimeout, err = parseDuration(value)
	case "jwttimeout", "jwt_timeout":
		cfg.JWTExpireTimeout, err = parseDuration(value)
	case "externalbrowsertimeout", "external_browser_timeout":
		cfg.ExternalBrowserTimeout, err = parseDuration(value)
	case "maxretrycount", "max_retry_count":
		cfg.MaxRetryCount, err = parseInt(value)
	case "application":
		cfg.Application, err = parseString(value)
	case "authenticator":
		v, err = parseString(value)
		if err = checkParsingError(err, key, value); err != nil {
			return err
		}
		err = determineAuthenticatorType(cfg, v)
	case "disableocspchecks", "disable_ocsp_checks":
		cfg.DisableOCSPChecks, err = parseBool(value)
	case "insecuremode", "insecure_mode":
		logInsecureModeDeprecationInfo()
		cfg.InsecureMode, err = parseBool(value)
	case "ocspfailopen", "ocsp_fail_open":
		var vv ConfigBool
		vv, err = parseConfigBool(value)
		if err := checkParsingError(err, key, value); err != nil {
			return err
		}
		cfg.OCSPFailOpen = OCSPFailOpenMode(vv)
	case "token":
		cfg.Token, err = parseString(value)
	case "privatekey", "private_key":
		v, err = parseString(value)
		if err = checkParsingError(err, key, value); err != nil {
			return err
		}
		block, decodeErr := base64.URLEncoding.DecodeString(v)
		if decodeErr != nil {
			return &SnowflakeError{
				Number:  ErrCodePrivateKeyParseError,
				Message: "Base64 decode failed",
			}
		}
		cfg.PrivateKey, err = parsePKCS8PrivateKey(block)
	case "validatedefaultparameters", "validate_default_parameters":
		cfg.ValidateDefaultParameters, err = parseConfigBool(value)
	case "clientrequestmfatoken", "client_request_mfa_token":
		cfg.ClientRequestMfaToken, err = parseConfigBool(value)
	case "clientstoretemporarycredential", "client_store_temporary_credential":
		cfg.ClientStoreTemporaryCredential, err = parseConfigBool(value)
	case "tracing":
		cfg.Tracing, err = parseString(value)
	case "tmpdirpath", "tmp_dir_path":
		cfg.TmpDirPath, err = parseString(value)
	case "disablequerycontextcache", "disable_query_context_cache":
		cfg.DisableQueryContextCache, err = parseBool(value)
	case "includeretryreason", "include_retry_reason":
		cfg.IncludeRetryReason, err = parseConfigBool(value)
	case "clientconfigfile", "client_config_file":
		cfg.ClientConfigFile, err = parseString(value)
	case "disableconsolelogin", "disable_console_login":
		cfg.DisableConsoleLogin, err = parseConfigBool(value)
	case "disablesamlurlcheck", "disable_saml_url_check":
		cfg.DisableSamlURLCheck, err = parseConfigBool(value)
	case "oauth_authorization_url":
		cfg.OauthAuthorizationURL, err = parseString(value)
	case "oauth_client_id":
		cfg.OauthClientID, err = parseString(value)
	case "oauth_client_secret":
		cfg.OauthClientSecret, err = parseString(value)
	case "oauth_token_request_url":
		cfg.OauthTokenRequestURL, err = parseString(value)
	case "oauth_redirect_uri":
		cfg.OauthRedirectURI, err = parseString(value)
	case "oauth_scope":
		cfg.OauthScope, err = parseString(value)
	case "workload_identity_provider":
		cfg.WorkloadIdentityProvider, err = parseString(value)
	case "workload_identity_entra_resource":
		cfg.WorkloadIdentityEntraResource, err = parseString(value)

	case "token_file_path":
		tokenPath, err = parseString(value)
		if err = checkParsingError(err, key, value); err != nil {
			return err
		}
		v, err := readToken(tokenPath)
		if err != nil {
			return err
		}
		cfg.Token = v
	default:
		param, err := parseString(value)
		if err = checkParsingError(err, key, value); err != nil {
			return err
		}
		cfg.Params[urlDecodeIfNeeded(key)] = &param
	}
	return checkParsingError(err, key, value)
}

func checkParsingError(err error, key string, value interface{}) error {
	if err != nil {
		err = &SnowflakeError{
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
