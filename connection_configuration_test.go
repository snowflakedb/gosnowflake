package gosnowflake

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	path "path/filepath"
	"testing"
	"time"
)

func TestTokenFilePermission(t *testing.T) {
	if isWindows {
		return
	}
	os.Setenv(snowflakeHome, "./test_data")

	connectionsStat, err := os.Stat("./test_data/connections.toml")
	assertNilF(t, err, "The error should not occur")

	tokenStat, err := os.Stat("./test_data/snowflake/session/token")
	assertNilF(t, err, "The error should not occur")

	defer func() {
		err = os.Chmod("./test_data/connections.toml", connectionsStat.Mode())
		assertNilF(t, err, "The error should not occur")

		err = os.Chmod("./test_data/snowflake/session/token", tokenStat.Mode())
		assertNilF(t, err, "The error should not occur")
	}()

	t.Run("test warning logger for readable outside owner", func(t *testing.T) {
		var originalLogger = logger
		logger = CreateDefaultLogger()
		buf := &bytes.Buffer{}
		logger.SetOutput(buf)

		defer func() {
			logger = originalLogger
		}()

		err = os.Chmod("./test_data/connections.toml", 0644)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")

		_, err = loadConnectionConfig()
		assertNilF(t, err, "The error should not occur")

		connectionsAbsolutePath, err2 := path.Abs("./test_data/connections.toml")
		assertNilF(t, err2, "The error should not occur")

		expectedWarn := fmt.Sprintf("level=warning msg=\"file '%v' is readable by someone other than the owner. "+
			"Your Permission: -rw-r--r--. If you want to disable this warning, either remove read permissions from group "+
			"and others or set the environment variable SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE to true\"", connectionsAbsolutePath)
		assertStringContainsF(t, buf.String(), expectedWarn)
	})

	t.Run("test warning skipped logger for readable outside owner", func(t *testing.T) {
		os.Setenv(skipWarningForReadPermissionsEnv, "true")
		defer func() {
			os.Unsetenv(skipWarningForReadPermissionsEnv)
		}()

		var originalLogger = logger
		logger = CreateDefaultLogger()
		buf := &bytes.Buffer{}
		logger.SetOutput(buf)

		defer func() {
			logger = originalLogger
		}()

		err = os.Chmod("./test_data/connections.toml", 0644)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")

		_, err = loadConnectionConfig()
		assertNilF(t, err, "The error should not occur")

		assertEmptyStringE(t, buf.String())
	})

	t.Run("test writable connection file other than owner", func(t *testing.T) {
		err = os.Chmod("./test_data/connections.toml", 0666)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")
		_, err := loadConnectionConfig()
		assertNotNilF(t, err, "The error should occur because the file is writable by anyone but the owner")
		driverErr, ok := err.(*SnowflakeError)
		assertTrueF(t, ok, "This should be a Snowflake Error")
		assertEqualF(t, driverErr.Number, ErrCodeInvalidFilePermission)
	})

	t.Run("test writable token file other than owner", func(t *testing.T) {
		err = os.Chmod("./test_data/snowflake/session/token", 0666)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")
		_, err := readToken("./test_data/snowflake/session/token")
		assertNotNilF(t, err, "The error should occur because the file is writable by anyone but the owner")
		driverErr, ok := err.(*SnowflakeError)
		assertTrueF(t, ok, "This should be a Snowflake Error")
		assertEqualF(t, driverErr.Number, ErrCodeInvalidFilePermission)
	})

	t.Run("test executable connection file", func(t *testing.T) {
		err = os.Chmod("./test_data/connections.toml", 0100)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")
		_, err := loadConnectionConfig()
		assertNotNilF(t, err, "The error should occur because the file is executable")
		driverErr, ok := err.(*SnowflakeError)
		assertTrueF(t, ok, "This should be a Snowflake Error")
		assertEqualF(t, driverErr.Number, ErrCodeInvalidFilePermission)
	})

	t.Run("test executable token file", func(t *testing.T) {
		err = os.Chmod("./test_data/snowflake/session/token", 0010)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")
		_, err := readToken("./test_data/snowflake/session/token")
		assertNotNilF(t, err, "The error should occur because the file is executable")
		driverErr, ok := err.(*SnowflakeError)
		assertTrueF(t, ok, "This should be a Snowflake Error")
		assertEqualF(t, driverErr.Number, ErrCodeInvalidFilePermission)
	})

	t.Run("test valid file permission for connection config and token file", func(t *testing.T) {
		err = os.Chmod("./test_data/connections.toml", 0600)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")

		err = os.Chmod("./test_data/snowflake/session/token", 0600)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")

		_, err := loadConnectionConfig()
		assertNilF(t, err, "The error occurred because the permission is not 0600")

		_, err = readToken("./test_data/snowflake/session/token")
		assertNilF(t, err, "The error occurred because the permission is not 0600")
	})
}

func TestLoadConnectionConfigForStandardAuth(t *testing.T) {
	err := os.Chmod("./test_data/connections.toml", 0600)
	assertNilF(t, err, "The error occurred because you cannot change the file permission")

	os.Setenv(snowflakeHome, "./test_data")

	cfg, err := loadConnectionConfig()
	assertNilF(t, err, "The error should not occur")
	assertEqualF(t, cfg.Account, "snowdriverswarsaw.us-west-2.aws")
	assertEqualF(t, cfg.User, "test_default_user")
	assertEqualF(t, cfg.Password, "test_default_pass")
	assertEqualF(t, cfg.Warehouse, "testw_default")
	assertEqualF(t, cfg.Database, "test_default_db")
	assertEqualF(t, cfg.Schema, "test_default_go")
	assertEqualF(t, cfg.Protocol, "https")
	assertEqualF(t, cfg.Port, 300)
}

func TestLoadConnectionConfigForOAuth(t *testing.T) {
	err := os.Chmod("./test_data/connections.toml", 0600)
	assertNilF(t, err, "The error occurred because you cannot change the file permission")

	os.Setenv(snowflakeHome, "./test_data")
	os.Setenv(snowflakeConnectionName, "aws-oauth")

	cfg, err := loadConnectionConfig()
	assertNilF(t, err, "The error should not occur")
	assertEqualF(t, cfg.Account, "snowdriverswarsaw.us-west-2.aws")
	assertEqualF(t, cfg.User, "test_oauth_user")
	assertEqualF(t, cfg.Password, "test_oauth_pass")
	assertEqualF(t, cfg.Warehouse, "testw_oauth")
	assertEqualF(t, cfg.Database, "test_oauth_db")
	assertEqualF(t, cfg.Schema, "test_oauth_go")
	assertEqualF(t, cfg.Protocol, "https")
	assertEqualF(t, cfg.Authenticator, AuthTypeOAuth)
	assertEqualF(t, cfg.Token, "token_value")
	assertEqualF(t, cfg.Port, 443)
	assertEqualE(t, cfg.DisableOCSPChecks, true)
}

func TestLoadConnectionConfigForSnakeCaseConfiguration(t *testing.T) {
	err := os.Chmod("./test_data/connections.toml", 0600)
	assertNilF(t, err, "The error occurred because you cannot change the file permission")

	os.Setenv(snowflakeHome, "./test_data")
	os.Setenv(snowflakeConnectionName, "snake-case")

	cfg, err := loadConnectionConfig()
	assertNilF(t, err, "The error should not occur")
	assertEqualE(t, cfg.OCSPFailOpen, OCSPFailOpenTrue)
}

func TestReadTokenValueWithTokenFilePath(t *testing.T) {
	err := os.Chmod("./test_data/connections.toml", 0600)
	assertNilF(t, err, "The error occurred because you cannot change the file permission")

	err = os.Chmod("./test_data/snowflake/session/token", 0600)
	assertNilF(t, err, "The error occurred because you cannot change the file permission")

	os.Setenv(snowflakeHome, "./test_data")
	os.Setenv(snowflakeConnectionName, "read-token")

	cfg, err := loadConnectionConfig()
	assertNilF(t, err, "The error should not occur")
	assertEqualF(t, cfg.Authenticator, AuthTypeOAuth)
	assertEqualF(t, cfg.Token, "mock_token123456")
	assertEqualE(t, cfg.InsecureMode, true)
}

func TestLoadConnectionConfigWitNonExistingDSN(t *testing.T) {
	err := os.Chmod("./test_data/connections.toml", 0600)
	assertNilF(t, err, "The error occurred because you cannot change the file permission")

	os.Setenv(snowflakeHome, "./test_data")
	os.Setenv(snowflakeConnectionName, "unavailableDSN")

	_, err = loadConnectionConfig()
	assertNotNilF(t, err, "The error should occur")

	driverErr, ok := err.(*SnowflakeError)
	assertTrueF(t, ok, "This should be a Snowflake Error")
	assertEqualF(t, driverErr.Number, ErrCodeFailedToFindDSNInToml)
}

func TestLoadConnectionConfigWithTokenFileNotExist(t *testing.T) {
	err := os.Chmod("./test_data/connections.toml", 0600)
	assertNilF(t, err, "The error occurred because you cannot change the file permission")

	os.Setenv(snowflakeHome, "./test_data")
	os.Setenv(snowflakeConnectionName, "aws-oauth-file")

	_, err = loadConnectionConfig()
	assertNotNilF(t, err, "The error should occur")

	_, ok := err.(*(fs.PathError))
	assertTrueF(t, ok, "This error should be a path error")
}

func TestParseInt(t *testing.T) {
	var i interface{}

	i = 20
	num, err := parseInt(i)
	assertNilF(t, err, "This value should be parsed")
	assertEqualF(t, num, 20)

	i = "40"
	num, err = parseInt(i)
	assertNilF(t, err, "This value should be parsed")
	assertEqualF(t, num, 40)

	i = "wrong_num"
	_, err = parseInt(i)
	assertNotNilF(t, err, "should have failed")
}

func TestParseBool(t *testing.T) {
	var i interface{}

	i = true
	b, err := parseBool(i)
	assertNilF(t, err, "This value should be parsed")
	assertEqualF(t, b, true)

	i = "false"
	b, err = parseBool(i)
	assertNilF(t, err, "This value should be parsed")
	assertEqualF(t, b, false)

	i = "wrong_bool"
	_, err = parseInt(i)
	assertNotNilF(t, err, "should have failed")
}

func TestParseDuration(t *testing.T) {
	var i interface{}

	i = 300
	dur, err := parseDuration(i)
	assertNilF(t, err, "This value should be parsed")
	assertEqualF(t, dur, time.Duration(300*int64(time.Second)))

	i = "30"
	dur, err = parseDuration(i)
	assertNilF(t, err, "This value should be parsed")
	assertEqualF(t, dur, time.Duration(int64(time.Minute)/2))

	i = false
	_, err = parseDuration(i)
	assertNotNilF(t, err, "should have failed")
}

type paramList struct {
	testParams []string
	values     []interface{}
}

func TestParseToml(t *testing.T) {
	testCases := []paramList{
		{
			testParams: []string{"user", "password", "host", "account", "warehouse", "database",
				"schema", "role", "region", "protocol", "passcode", "application", "token",
				"tracing", "tmpDirPath", "tmp_dir_path", "clientConfigFile", "client_config_file", "oauth_authorization_url", "oauth_client_id",
				"oauth_client_secret", "oauth_token_request_url", "oauth_redirect_uri", "oauth_scope",
				"workload_identity_provider", "workload_identity_entra_resource"},
			values: []interface{}{"value"},
		},
		{
			testParams: []string{"privatekey", "private_key"},
			values:     []interface{}{generatePKCS8StringSupress(testPrivKey)},
		},
		{
			testParams: []string{"port", "maxRetryCount", "max_retry_count", "clientTimeout", "client_timeout", "jwtClientTimeout", "jwt_client_timeout", "loginTimeout",
				"login_timeout", "requestTimeout", "request_timeout", "jwtTimeout", "jwt_timeout", "externalBrowserTimeout", "external_browser_timeout"},
			values: []interface{}{"300", 500},
		},
		{
			testParams: []string{"ocspFailOpen", "ocsp_fail_open", "insecureMode", "insecure_mode", "PasscodeInPassword", "passcode_in_password", "validateDEFAULTParameters", "validate_default_parameters",
				"clientRequestMFAtoken", "client_request_mfa_token", "clientStoreTemporaryCredential", "client_store_temporary_credential", "disableQueryContextCache", "disable_query_context_cache", "disable_ocsp_checks",
				"includeRetryReason", "include_retry_reason", "disableConsoleLogin", "disable_console_login", "disableSamlUrlCheck", "disable_saml_url_check"},
			values: []interface{}{true, "true", false, "false"},
		},
		{
			testParams: []string{"connectionDiagnosticsEnabled", "connection_diagnostics_enabled"},
			values:     []interface{}{true, false},
		},
		{
			testParams: []string{"connectionDiagnosticsAllowlistFile", "connection_diagnostics_allowlist_file"},
			values:     []interface{}{"myallowlist.json"},
		},
	}

	for _, testCase := range testCases {
		for _, param := range testCase.testParams {
			for _, value := range testCase.values {
				t.Run(param, func(t *testing.T) {
					cfg := &Config{}
					connectionMap := make(map[string]interface{})
					connectionMap[param] = value
					err := parseToml(cfg, connectionMap)
					assertNilF(t, err, "The value should be parsed")
				})
			}
		}
	}
}

func TestParseTomlWithWrongValue(t *testing.T) {
	testCases := []paramList{
		{
			testParams: []string{"user", "password", "host", "account", "warehouse", "database",
				"schema", "role", "region", "protocol", "passcode", "application", "token", "privateKey",
				"tracing", "tmpDirPath", "clientConfigFile", "wrongParams", "token_file_path"},
			values: []interface{}{1, false},
		},
		{
			testParams: []string{"port", "maxRetryCount", "clientTimeout", "jwtClientTimeout", "loginTimeout",
				"requestTimeout", "jwtTimeout", "externalBrowserTimeout", "authenticator"},
			values: []interface{}{"wrong_value", false},
		},
		{
			testParams: []string{"ocspFailOpen", "insecureMode", "PasscodeInPassword", "validateDEFAULTParameters", "clientRequestMFAtoken",
				"clientStoreTemporaryCredential", "disableQueryContextCache", "includeRetryReason", "disableConsoleLogin", "disableSamlUrlCheck"},
			values: []interface{}{"wrong_value", 1},
		},
	}

	for _, testCase := range testCases {
		for _, param := range testCase.testParams {
			for _, value := range testCase.values {
				t.Run(param, func(t *testing.T) {
					cfg := &Config{}
					connectionMap := make(map[string]interface{})
					connectionMap[param] = value
					err := parseToml(cfg, connectionMap)
					assertNotNilF(t, err, "should have failed")
					driverErr, ok := err.(*SnowflakeError)
					assertTrueF(t, ok, "This should be a Snowflake Error")
					assertEqualF(t, driverErr.Number, ErrCodeTomlFileParsingFailed)
				})

			}
		}
	}
}

func TestGetTomlFilePath(t *testing.T) {
	skipOnMissingHome(t)
	dir, err := getTomlFilePath("")
	assertNilF(t, err, "should not have failed")
	homeDir, err := os.UserHomeDir()
	assertNilF(t, err, "The connection cannot find the user home directory")
	assertEqualF(t, dir, path.Join(homeDir, ".snowflake"))

	location := "../user//somelocation///b"
	dir, err = getTomlFilePath(location)
	assertNilF(t, err, "should not have failed")
	result, err := path.Abs(location)
	assertNilF(t, err, "should not have failed")
	assertEqualF(t, dir, result)

	//Absolute path for windows can be varied depend on which disk the driver is located.
	// As a result, this test is available on non-Window machines.
	if !isWindows {
		result = "/user/somelocation/b"
		location = "/user//somelocation///b"
		dir, err = getTomlFilePath(location)
		assertNilF(t, err, "should not have failed")
		assertEqualF(t, dir, result)
	}
}
