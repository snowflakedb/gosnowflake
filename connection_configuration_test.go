// Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"io/fs"
	"os"
	path "path/filepath"
	"testing"
	"time"
)

func TestTokenFilePermission(t *testing.T) {
	if !isWindows {
		os.Setenv(snowflakeHome, "./test_data")
		_, err := loadConnectionConfig()
		assertNotNilF(t, err, "The error should occur because the permission is not 0600")
		driverErr, ok := err.(*SnowflakeError)
		assertTrueF(t, ok, "This should be a Snowflake Error")
		assertEqualF(t, driverErr.Number, ErrCodeInvalidFilePermission)

		_, err = readToken("./test_data/snowflake/session")
		assertNotNilF(t, err, "The error should occur because the permission is not 0600")
		driverErr, ok = err.(*SnowflakeError)
		assertTrueF(t, ok, "This should be a Snowflake Error")
		assertEqualF(t, driverErr.Number, ErrCodeInvalidFilePermission)

		err = os.Chmod("./test_data/connections.toml", 0666)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")

		err = os.Chmod("./test_data/snowflake/session/token", 0666)
		assertNilF(t, err, "TThe error occurred because you cannot change the file permission")

		_, err = loadConnectionConfig()
		assertNotNilF(t, err, "The error should occur because the permission is not 0600")
		driverErr, ok = err.(*SnowflakeError)
		assertTrueF(t, ok, "This should be a Snowflake Error")
		assertEqualF(t, driverErr.Number, ErrCodeInvalidFilePermission)

		_, err = readToken("./test_data/snowflake/session")
		assertNotNilF(t, err, "The error should occur because the permission is not 0600")
		driverErr, ok = err.(*SnowflakeError)
		assertTrueF(t, ok, "This should be a Snowflake Error")
		assertEqualF(t, driverErr.Number, ErrCodeInvalidFilePermission)

		err = os.Chmod("./test_data/connections.toml", 0600)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")

		err = os.Chmod("./test_data/snowflake/session/token", 0600)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")

		_, err = loadConnectionConfig()
		assertNilF(t, err, "The error occurred because the permission is not 0600")

		_, err = readToken("./test_data/snowflake/session/token")
		assertNilF(t, err, "The error occurred because the permission is not 0600")
	}
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
				"tracing", "tmpDirPath", "clientConfigFile"},
			values: []interface{}{"value"},
		},
		{
			testParams: []string{"privatekey"},
			values:     []interface{}{generatePKCS8StringSupress(testPrivKey)},
		},
		{
			testParams: []string{"port", "maxRetryCount", "clientTimeout", "jwtClientTimeout", "loginTimeout",
				"requestTimeout", "jwtTimeout", "externalBrowserTimeout"},
			values: []interface{}{"300", 500},
		},
		{
			testParams: []string{"ocspFailOpen", "insecureMode", "PasscodeInPassword", "validateDEFAULTParameters", "clientRequestMFAtoken",
				"clientStoreTemporaryCredential", "disableQueryContextCache", "includeRetryReason", "disableConsoleLogin", "disableSamlUrlCheck"},
			values: []interface{}{true, "true", false, "false"},
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
