package config

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"os"
	path "path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	sferrors "github.com/snowflakedb/gosnowflake/v2/internal/errors"
	sflogger "github.com/snowflakedb/gosnowflake/v2/internal/logger"
)

func TestTokenFilePermission(t *testing.T) {
	if runtime.GOOS == "windows" {
		return
	}
	os.Setenv(snowflakeHome, "../../test_data")

	connectionsStat, err := os.Stat("../../test_data/connections.toml")
	if err != nil {
		t.Fatalf("Failed to stat connections.toml file: %v", err)
	}

	tokenStat, err := os.Stat("../../test_data/snowflake/session/token")
	if err != nil {
		t.Fatalf("Failed to stat token file: %v", err)
	}

	defer func() {
		err = os.Chmod("../../test_data/connections.toml", connectionsStat.Mode())
		if err != nil {
			t.Errorf("Failed to restore connections.toml file permission: %v", err)
		}

		err = os.Chmod("../../test_data/snowflake/session/token", tokenStat.Mode())
		if err != nil {
			t.Errorf("Failed to restore token file permission: %v", err)
		}
	}()

	t.Run("test warning logger for readable outside owner", func(t *testing.T) {
		originalGlobalLogger := sflogger.GetLogger()
		newLogger := sflogger.CreateDefaultLogger()
		sflogger.SetLogger(newLogger)
		buf := &bytes.Buffer{}
		sflogger.GetLogger().SetOutput(buf)

		defer func() {
			sflogger.SetLogger(originalGlobalLogger)
		}()

		err = os.Chmod("../../test_data/connections.toml", 0644)
		if err != nil {
			t.Fatalf("Failed to change connections.toml file permission: %v", err)
		}

		_, err = LoadConnectionConfig()
		if err != nil {
			t.Fatalf("Failed to load connection config: %v", err)
		}

		connectionsAbsolutePath, err := path.Abs("../../test_data/connections.toml")
		if err != nil {
			t.Fatalf("Failed to get absolute path of connections.toml file: %v", err)
		}

		expectedWarn := fmt.Sprintf("msg=\"file '%v' is readable by someone other than the owner. "+
			"Your Permission: -rw-r--r--. If you want to disable this warning, either remove read permissions from group "+
			"and others or set the environment variable SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE to true\"", connectionsAbsolutePath)
		if !strings.Contains(buf.String(), expectedWarn) {
			t.Errorf("Expected warning message not found in logs.\nGot: %v\nWant substring: %v", buf.String(), expectedWarn)
		}
	})

	t.Run("test warning skipped logger for readable outside owner", func(t *testing.T) {
		os.Setenv(skipWarningForReadPermissionsEnv, "true")
		defer func() {
			os.Unsetenv(skipWarningForReadPermissionsEnv)
		}()

		originalGlobalLogger := sflogger.GetLogger()
		newLogger := sflogger.CreateDefaultLogger()
		sflogger.SetLogger(newLogger)
		buf := &bytes.Buffer{}
		sflogger.GetLogger().SetOutput(buf)

		defer func() {
			sflogger.SetLogger(originalGlobalLogger)
		}()

		err = os.Chmod("../../test_data/connections.toml", 0644)
		if err != nil {
			t.Fatalf("Failed to change connections.toml file permission: %v", err)
		}

		_, err = LoadConnectionConfig()
		if err != nil {
			t.Fatalf("Failed to load connection config: %v", err)
		}
	})

	t.Run("test writable connection file other than owner", func(t *testing.T) {
		err = os.Chmod("../../test_data/connections.toml", 0666)
		if err != nil {
			t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
		}
		_, err := LoadConnectionConfig()
		if err == nil {
			t.Fatal("The error should occur because the file is writable by anyone but the owner")
		}
		driverErr, ok := err.(*sferrors.SnowflakeError)
		if !ok {
			t.Fatalf("This should be a Snowflake Error, got: %T", err)
		}
		if driverErr.Number != sferrors.ErrCodeInvalidFilePermission {
			t.Fatalf("Expected error code %d, got %d", sferrors.ErrCodeInvalidFilePermission, driverErr.Number)
		}
	})

	t.Run("test skip verification bypasses writable connection file", func(t *testing.T) {
		os.Setenv(SkipTokenFilePermissionsVerificationEnv, "true")
		defer os.Unsetenv(SkipTokenFilePermissionsVerificationEnv)

		err = os.Chmod("../../test_data/connections.toml", 0666)
		if err != nil {
			t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
		}
		if _, err := LoadConnectionConfig(); err != nil {
			t.Fatalf("LoadConnectionConfig should succeed when %v is set, got: %v", SkipTokenFilePermissionsVerificationEnv, err)
		}
	})

	t.Run("test skip verification does not leak to workload identity token file", func(t *testing.T) {
		os.Setenv(SkipTokenFilePermissionsVerificationEnv, "true")
		defer os.Unsetenv(SkipTokenFilePermissionsVerificationEnv)

		err = os.Chmod("../../test_data/snowflake/session/token", 0666)
		if err != nil {
			t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
		}
		_, err := ReadToken("../../test_data/snowflake/session/token")
		if err == nil {
			t.Fatal("ReadToken must still fail on writable token file even with bypass env set")
		}
		driverErr, ok := err.(*sferrors.SnowflakeError)
		if !ok {
			t.Fatalf("This should be a Snowflake Error, got: %T", err)
		}
		if driverErr.Number != sferrors.ErrCodeInvalidFilePermission {
			t.Fatalf("Expected error code %d, got %d", sferrors.ErrCodeInvalidFilePermission, driverErr.Number)
		}
	})

	t.Run("test writable token file other than owner", func(t *testing.T) {
		err = os.Chmod("../../test_data/snowflake/session/token", 0666)
		if err != nil {
			t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
		}
		_, err := ReadToken("../../test_data/snowflake/session/token")
		if err == nil {
			t.Fatal("The error should occur because the file is writable by anyone but the owner")
		}
		driverErr, ok := err.(*sferrors.SnowflakeError)
		if !ok {
			t.Fatalf("This should be a Snowflake Error, got: %T", err)
		}
		if driverErr.Number != sferrors.ErrCodeInvalidFilePermission {
			t.Fatalf("Expected error code %d, got %d", sferrors.ErrCodeInvalidFilePermission, driverErr.Number)
		}
	})

	t.Run("test executable connection file", func(t *testing.T) {
		err = os.Chmod("../../test_data/connections.toml", 0100)
		if err != nil {
			t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
		}
		_, err := LoadConnectionConfig()
		if err == nil {
			t.Fatal("The error should occur because the file is executable")
		}
		driverErr, ok := err.(*sferrors.SnowflakeError)
		if !ok {
			t.Fatalf("This should be a Snowflake Error, got: %T", err)
		}
		if driverErr.Number != sferrors.ErrCodeInvalidFilePermission {
			t.Fatalf("Expected error code %d, got %d", sferrors.ErrCodeInvalidFilePermission, driverErr.Number)
		}
	})

	t.Run("test executable token file", func(t *testing.T) {
		err = os.Chmod("../../test_data/snowflake/session/token", 0010)
		if err != nil {
			t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
		}
		_, err := ReadToken("../../test_data/snowflake/session/token")
		if err == nil {
			t.Fatal("The error should occur because the file is executable")
		}
		driverErr, ok := err.(*sferrors.SnowflakeError)
		if !ok {
			t.Fatalf("This should be a Snowflake Error, got: %T", err)
		}
		if driverErr.Number != sferrors.ErrCodeInvalidFilePermission {
			t.Fatalf("Expected error code %d, got %d", sferrors.ErrCodeInvalidFilePermission, driverErr.Number)
		}
	})

	t.Run("test valid file permission for connection config and token file", func(t *testing.T) {
		err = os.Chmod("../../test_data/connections.toml", 0600)
		if err != nil {
			t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
		}

		err = os.Chmod("../../test_data/snowflake/session/token", 0600)
		if err != nil {
			t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
		}

		_, err := LoadConnectionConfig()
		if err != nil {
			t.Fatalf("The error occurred because the permission is not 0600: %v", err)
		}

		_, err = ReadToken("../../test_data/snowflake/session/token")
		if err != nil {
			t.Fatalf("The error occurred because the permission is not 0600: %v", err)
		}
	})
}

func TestLoadConnectionConfigForStandardAuth(t *testing.T) {
	err := os.Chmod("../../test_data/connections.toml", 0600)
	if err != nil {
		t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
	}

	os.Setenv(snowflakeHome, "../../test_data")

	cfg, err := LoadConnectionConfig()
	if err != nil {
		t.Fatalf("The error should not occur: %v", err)
	}
	assertEqual(t, cfg.Account, "snowdriverswarsaw.us-west-2.aws")
	assertEqual(t, cfg.User, "test_default_user")
	assertEqual(t, cfg.Password, "test_default_pass")
	assertEqual(t, cfg.Warehouse, "testw_default")
	assertEqual(t, cfg.Database, "test_default_db")
	assertEqual(t, cfg.Schema, "test_default_go")
	assertEqual(t, cfg.Protocol, "https")
	if cfg.Port != 300 {
		t.Fatalf("Expected port 300, got %d", cfg.Port)
	}
}

func TestLoadConnectionConfigForOAuth(t *testing.T) {
	err := os.Chmod("../../test_data/connections.toml", 0600)
	if err != nil {
		t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
	}

	os.Setenv(snowflakeHome, "../../test_data")
	os.Setenv(snowflakeConnectionName, "aws-oauth")

	cfg, err := LoadConnectionConfig()
	if err != nil {
		t.Fatalf("The error should not occur: %v", err)
	}
	assertEqual(t, cfg.Account, "snowdriverswarsaw.us-west-2.aws")
	assertEqual(t, cfg.User, "test_oauth_user")
	assertEqual(t, cfg.Password, "test_oauth_pass")
	assertEqual(t, cfg.Warehouse, "testw_oauth")
	assertEqual(t, cfg.Database, "test_oauth_db")
	assertEqual(t, cfg.Schema, "test_oauth_go")
	assertEqual(t, cfg.Protocol, "https")
	if cfg.Authenticator != AuthTypeOAuth {
		t.Fatalf("Expected authenticator %v, got %v", AuthTypeOAuth, cfg.Authenticator)
	}
	assertEqual(t, cfg.Token, "token_value")
	if cfg.Port != 443 {
		t.Fatalf("Expected port 443, got %d", cfg.Port)
	}
	if cfg.DisableOCSPChecks != true {
		t.Fatalf("Expected DisableOCSPChecks true, got %v", cfg.DisableOCSPChecks)
	}
}

func TestLoadConnectionConfigForSnakeCaseConfiguration(t *testing.T) {
	err := os.Chmod("../../test_data/connections.toml", 0600)
	if err != nil {
		t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
	}

	os.Setenv(snowflakeHome, "../../test_data")
	os.Setenv(snowflakeConnectionName, "snake-case")

	cfg, err := LoadConnectionConfig()
	if err != nil {
		t.Fatalf("The error should not occur: %v", err)
	}
	if cfg.OCSPFailOpen != OCSPFailOpenTrue {
		t.Fatalf("Expected OCSPFailOpen %v, got %v", OCSPFailOpenTrue, cfg.OCSPFailOpen)
	}
}

func TestReadTokenValueWithTokenFilePath(t *testing.T) {
	err := os.Chmod("../../test_data/connections.toml", 0600)
	if err != nil {
		t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
	}

	err = os.Chmod("../../test_data/snowflake/session/token", 0600)
	if err != nil {
		t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
	}

	os.Setenv(snowflakeHome, "../../test_data")
	os.Setenv(snowflakeConnectionName, "read-token")

	cfg, err := LoadConnectionConfig()
	if err != nil {
		t.Fatalf("The error should not occur: %v", err)
	}
	if cfg.Authenticator != AuthTypeOAuth {
		t.Fatalf("Expected authenticator %v, got %v", AuthTypeOAuth, cfg.Authenticator)
	}
	// The token_file_path in the TOML is relative ("./test_data/snowflake/session/token"),
	// so GetToken resolves it relative to CWD. Use an absolute path instead.
	absTokenPath, err := path.Abs("../../test_data/snowflake/session/token")
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}
	cfg.TokenFilePath = absTokenPath
	token, err := GetToken(cfg)
	if err != nil {
		t.Fatalf("Failed to get token: %v", err)
	}
	assertEqual(t, token, "mock_token123456")
	if cfg.DisableOCSPChecks != true {
		t.Fatalf("Expected DisableOCSPChecks true, got %v", cfg.DisableOCSPChecks)
	}
}

func TestLoadConnectionConfigWitNonExistingDSN(t *testing.T) {
	err := os.Chmod("../../test_data/connections.toml", 0600)
	if err != nil {
		t.Fatalf("The error occurred because you cannot change the file permission: %v", err)
	}

	os.Setenv(snowflakeHome, "../../test_data")
	os.Setenv(snowflakeConnectionName, "unavailableDSN")

	_, err = LoadConnectionConfig()
	if err == nil {
		t.Fatal("The error should occur")
	}

	driverErr, ok := err.(*sferrors.SnowflakeError)
	if !ok {
		t.Fatalf("This should be a Snowflake Error, got: %T", err)
	}
	if driverErr.Number != sferrors.ErrCodeFailedToFindDSNInToml {
		t.Fatalf("Expected error code %d, got %d", sferrors.ErrCodeFailedToFindDSNInToml, driverErr.Number)
	}
}

func TestParseInt(t *testing.T) {
	var i any

	i = 20
	num, err := ParseInt(i)
	if err != nil {
		t.Fatalf("This value should be parsed: %v", err)
	}
	if num != 20 {
		t.Fatalf("Expected 20, got %d", num)
	}

	i = "40"
	num, err = ParseInt(i)
	if err != nil {
		t.Fatalf("This value should be parsed: %v", err)
	}
	if num != 40 {
		t.Fatalf("Expected 40, got %d", num)
	}

	i = "wrong_num"
	_, err = ParseInt(i)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestParseBool(t *testing.T) {
	var i any

	i = true
	b, err := ParseBool(i)
	if err != nil {
		t.Fatalf("This value should be parsed: %v", err)
	}
	if b != true {
		t.Fatalf("Expected true, got %v", b)
	}

	i = "false"
	b, err = ParseBool(i)
	if err != nil {
		t.Fatalf("This value should be parsed: %v", err)
	}
	if b != false {
		t.Fatalf("Expected false, got %v", b)
	}

	i = "wrong_bool"
	_, err = ParseBool(i)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestParseDuration(t *testing.T) {
	var i any

	i = 300
	dur, err := ParseDuration(i)
	if err != nil {
		t.Fatalf("This value should be parsed: %v", err)
	}
	if dur != time.Duration(300*int64(time.Second)) {
		t.Fatalf("Expected %v, got %v", time.Duration(300*int64(time.Second)), dur)
	}

	i = "30"
	dur, err = ParseDuration(i)
	if err != nil {
		t.Fatalf("This value should be parsed: %v", err)
	}
	if dur != time.Duration(int64(time.Minute)/2) {
		t.Fatalf("Expected %v, got %v", time.Duration(int64(time.Minute)/2), dur)
	}

	i = false
	_, err = ParseDuration(i)
	if err == nil {
		t.Fatal("should have failed")
	}
}

type paramList struct {
	testParams []string
	values     []any
}

func testGeneratePKCS8String(key *rsa.PrivateKey) string {
	tmpBytes, _ := x509.MarshalPKCS8PrivateKey(key)
	return base64.URLEncoding.EncodeToString(tmpBytes)
}

func TestParseToml(t *testing.T) {
	localTestKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate test private key: %s", err.Error())
	}

	testCases := []paramList{
		{
			testParams: []string{"user", "password", "host", "account", "warehouse", "database",
				"schema", "role", "region", "protocol", "passcode", "application", "token",
				"tracing", "tmpDirPath", "tmp_dir_path", "clientConfigFile", "client_config_file", "oauth_authorization_url", "oauth_client_id",
				"oauth_client_secret", "oauth_token_request_url", "oauth_redirect_uri", "oauth_scope",
				"workload_identity_provider", "workload_identity_entra_resource", "proxyHost", "noProxy", "proxyUser", "proxyPassword", "proxyProtocol"},
			values: []any{"value"},
		},
		{
			testParams: []string{"privatekey", "private_key"},
			values:     []any{testGeneratePKCS8String(localTestKey)},
		},
		{
			testParams: []string{"port", "maxRetryCount", "max_retry_count", "clientTimeout", "client_timeout", "jwtClientTimeout", "jwt_client_timeout", "loginTimeout",
				"login_timeout", "requestTimeout", "request_timeout", "jwtTimeout", "jwt_timeout", "externalBrowserTimeout", "external_browser_timeout", "proxyPort"},
			values: []any{"300", 500},
		},
		{
			testParams: []string{"ocspFailOpen", "ocsp_fail_open", "PasscodeInPassword", "passcode_in_password", "validateDEFAULTParameters", "validate_default_parameters",
				"clientRequestMFAtoken", "client_request_mfa_token", "clientStoreTemporaryCredential", "client_store_temporary_credential", "disableQueryContextCache", "disable_query_context_cache", "disable_ocsp_checks",
				"includeRetryReason", "include_retry_reason", "disableConsoleLogin", "disable_console_login", "disableSamlUrlCheck", "disable_saml_url_check"},
			values: []any{true, "true", false, "false"},
		},
		{
			testParams: []string{"connectionDiagnosticsEnabled", "connection_diagnostics_enabled"},
			values:     []any{true, false},
		},
		{
			testParams: []string{"connectionDiagnosticsAllowlistFile", "connection_diagnostics_allowlist_file"},
			values:     []any{"myallowlist.json"},
		},
	}

	for _, testCase := range testCases {
		for _, param := range testCase.testParams {
			for _, value := range testCase.values {
				t.Run(param, func(t *testing.T) {
					cfg := &Config{}
					connectionMap := make(map[string]any)
					connectionMap[param] = value
					err := ParseToml(cfg, connectionMap)
					if err != nil {
						t.Fatalf("The value should be parsed: %v", err)
					}
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
				"tracing", "tmpDirPath", "clientConfigFile", "wrongParams", "token_file_path", "proxyhost", "noproxy", "proxyUser", "proxyPassword", "proxyProtocol"},
			values: []any{1, false},
		},
		{
			testParams: []string{"port", "maxRetryCount", "clientTimeout", "jwtClientTimeout", "loginTimeout",
				"requestTimeout", "jwtTimeout", "externalBrowserTimeout", "authenticator"},
			values: []any{"wrong_value", false},
		},
		{
			testParams: []string{"ocspFailOpen", "PasscodeInPassword", "validateDEFAULTParameters", "clientRequestMFAtoken",
				"clientStoreTemporaryCredential", "disableQueryContextCache", "includeRetryReason", "disableConsoleLogin", "disableSamlUrlCheck"},
			values: []any{"wrong_value", 1},
		},
	}

	for _, testCase := range testCases {
		for _, param := range testCase.testParams {
			for _, value := range testCase.values {
				t.Run(param, func(t *testing.T) {
					cfg := &Config{}
					connectionMap := make(map[string]any)
					connectionMap[param] = value
					err := ParseToml(cfg, connectionMap)
					if err == nil {
						t.Fatal("should have failed")
					}
					driverErr, ok := err.(*sferrors.SnowflakeError)
					if !ok {
						t.Fatalf("This should be a Snowflake Error, got: %T", err)
					}
					if driverErr.Number != sferrors.ErrCodeTomlFileParsingFailed {
						t.Fatalf("Expected error code %d, got %d", sferrors.ErrCodeTomlFileParsingFailed, driverErr.Number)
					}
				})
			}
		}
	}
}

func TestGetTomlFilePath(t *testing.T) {
	if (runtime.GOOS == "linux" || runtime.GOOS == "darwin") && os.Getenv("HOME") == "" {
		t.Skip("skipping on missing HOME environment variable")
	}
	dir, err := GetTomlFilePath("")
	if err != nil {
		t.Fatalf("should not have failed: %v", err)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("The connection cannot find the user home directory: %v", err)
	}
	assertEqual(t, dir, path.Join(homeDir, ".snowflake"))

	location := "../user//somelocation///b"
	dir, err = GetTomlFilePath(location)
	if err != nil {
		t.Fatalf("should not have failed: %v", err)
	}
	result, err := path.Abs(location)
	if err != nil {
		t.Fatalf("should not have failed: %v", err)
	}
	assertEqual(t, dir, result)

	//Absolute path for windows can be varied depend on which disk the driver is located.
	// As a result, this test is available on non-Window machines.
	if !(runtime.GOOS == "windows") {
		result = "/user/somelocation/b"
		location = "/user//somelocation///b"
		dir, err = GetTomlFilePath(location)
		if err != nil {
			t.Fatalf("should not have failed: %v", err)
		}
		assertEqual(t, dir, result)
	}
}

// assertEqual is a simple test helper for string comparison.
func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
