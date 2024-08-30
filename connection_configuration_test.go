package gosnowflake

import (
	"io/fs"
	"os"
	"testing"
	"time"
)

func TestLoadConnectionConfig_Default(t *testing.T) {
	os.Setenv("SNOWFLAKE_HOME", "./")

	cfg, err := LoadConnectionConfig()

	if err != nil {
		t.Fatalf("err: %v", err)
	}

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

	if err != nil {
		t.Fatalf("err: %v", err)
	}

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

	if err == nil {
		t.Fatal("should have failed")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("should be snowflake error. err: %v", err)
	}
	if driverErr.Number != ErrCodeFailedToFindDSNInToml {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrCodeFailedToFindDSNInToml, driverErr.Number)
	}
}

func TestLoadConnectionConfigWithTokenFileNotExist(t *testing.T) {
	os.Setenv("SNOWFLAKE_HOME", "./")
	os.Setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "aws-oauth-file")

	_, err := LoadConnectionConfig()

	_, ok := err.(*(fs.PathError))
	if !ok {
		t.Fatalf("should be io/fs error. err: %v", err)
	}
}

func TestParseInt(t *testing.T) {
	var i interface{}
	var num int
	var err error

	i = 20
	num, err = parseInt(i)
	if err != nil {
		t.Fatalf("should be parsed: %v", err)
	}
	assertEqualF(t, num, 20)

	i = "40"
	num, err = parseInt(i)
	if err != nil {
		t.Fatalf("should be parsed: %v", err)
	}
	assertEqualF(t, num, 40)

	i = "wrong_num"
	_, err = parseInt(i)
	if err == nil {
		t.Fatal("should have failed")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("should be snowflake error. err: %v", err)
	}
	if driverErr.Number != ErrCodeTomlFileParsingFailed {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrCodeTomlFileParsingFailed, driverErr.Number)
	}
}

func TestParseBool(t *testing.T) {
	var i interface{}
	var b bool
	var err error

	i = true
	b, err = parseBool(i)
	if err != nil {
		t.Fatalf("should be parsed: %v", err)
	}
	assertEqualF(t, b, true)

	i = "false"
	b, err = parseBool(i)
	if err != nil {
		t.Fatalf("should be parsed: %v", err)
	}
	assertEqualF(t, b, false)

	i = "wrong_bool"
	_, err = parseInt(i)
	if err == nil {
		t.Fatal("should have failed")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("should be snowflake error. err: %v", err)
	}
	if driverErr.Number != ErrCodeTomlFileParsingFailed {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrCodeTomlFileParsingFailed, driverErr.Number)
	}
}

func TestParseDuration(t *testing.T) {
	var i interface{}
	var dur time.Duration
	var err error

	i = 300
	dur, err = parseDuration(i)
	if err != nil {
		t.Fatalf("should be parsed: %v", err)
	}
	assertEqualF(t, dur, time.Duration(5*int64(time.Minute)))

	i = "30"
	dur, err = parseDuration(i)
	if err != nil {
		t.Fatalf("should be parsed: %v", err)
	}
	assertEqualF(t, dur, time.Duration(int64(time.Minute)/2))

	i = false
	_, err = parseDuration(i)
	if err == nil {
		t.Fatal("should have failed")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("should be snowflake error. err: %v", err)
	}
	if driverErr.Number != ErrCodeTomlFileParsingFailed {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrCodeTomlFileParsingFailed, driverErr.Number)
	}
}
