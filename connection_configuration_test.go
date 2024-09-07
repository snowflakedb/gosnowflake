package gosnowflake

import (
	"io/fs"
	"os"
	path "path/filepath"
	"testing"
	"time"
)

func TestLoadConnectionConfig_Default(t *testing.T) {
	os.Setenv("SNOWFLAKE_HOME", "./")

	cfg, err := LoadConnectionConfig()

	assertNilF(t, err, "The error should not occur")
	assertEqualF(t, cfg.Account, "snowdriverswarsaw.us-west-2.aws")
	assertEqualF(t, cfg.User, "test_user")
	assertEqualF(t, cfg.Password, "test_pass")
	assertEqualF(t, cfg.Warehouse, "testw")
	assertEqualF(t, cfg.Database, "test_db")
	assertEqualF(t, cfg.Schema, "test_go")
	assertEqualF(t, cfg.Protocol, "https")
	assertEqualF(t, cfg.Port, 443)
}

func TestLoadConnectionConfig_OAuth(t *testing.T) {
	os.Setenv("SNOWFLAKE_HOME", "./")
	os.Setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "aws-oauth")
	cfg, err := LoadConnectionConfig()

	assertNilF(t, err, "The error should not occur")
	assertEqualF(t, cfg.Account, "snowdriverswarsaw.us-west-2.aws")
	assertEqualF(t, cfg.User, "test_user")
	assertEqualF(t, cfg.Password, "test_pass")
	assertEqualF(t, cfg.Warehouse, "testw")
	assertEqualF(t, cfg.Database, "test_db")
	assertEqualF(t, cfg.Schema, "test_go")
	assertEqualF(t, cfg.Protocol, "https")
	assertEqualF(t, cfg.Authenticator, AuthTypeOAuth)
	assertEqualF(t, cfg.Token, "token_value")
	assertEqualF(t, cfg.Port, 443)
}

func TestLoadConnectionConfigWitNonExisitngDSN(t *testing.T) {
	os.Setenv("SNOWFLAKE_HOME", "./")
	os.Setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "unavailableDSN")

	_, err := LoadConnectionConfig()
	assertNotNilF(t, err, "The error should occur")

	driverErr, ok := err.(*SnowflakeError)
	assertTrueF(t, ok, "This should be a Snowflake Error")
	assertEqualF(t, driverErr.Number, ErrCodeFailedToFindDSNInToml)
}

func TestLoadConnectionConfigWithTokenFileNotExist(t *testing.T) {
	os.Setenv("SNOWFLAKE_HOME", "./")
	os.Setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "aws-oauth-file")

	_, err := LoadConnectionConfig()
	assertNotNilF(t, err, "The error should occur")

	_, ok := err.(*(fs.PathError))
	assertTrueF(t, ok, "This error should be a path error")
}

func TestParseInt(t *testing.T) {
	var i interface{}
	var num int
	var err error

	i = 20
	num, err = parseInt(i)
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
	var b bool
	var err error

	i = true
	b, err = parseBool(i)
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
	var dur time.Duration
	var err error

	i = 300
	dur, err = parseDuration(i)
	assertNilF(t, err, "This value should be parsed")
	assertEqualF(t, dur, time.Duration(5*int64(time.Minute)))

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
					var connectionMap = make(map[string]interface{})
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
				"tracing", "tmpDirPath", "clientConfigFile", "wrongParams"},
			values: []interface{}{1},
		},
		{
			testParams: []string{"port", "maxRetryCount", "clientTimeout", "jwtClientTimeout", "loginTimeout",
				"requestTimeout", "jwtTimeout", "externalBrowserTimeout"},
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
					var connectionMap = make(map[string]interface{})
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
	assertEqualF(t, dir, path.Join(homeDir, "snowflake"))

	var location string = "../user//somelocation///b"
	dir, err = getTomlFilePath(location)
	assertNilF(t, err, "should not have failed")
	result, err := path.Abs(location)
	assertNilF(t, err, "should not have failed")
	assertEqualF(t, dir, result)
}
