package gosnowflake

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/golang/glog"
)

type tcParseDSN struct {
	dsn    string
	config *Config
	err    error
}

func TestParseDSN(t *testing.T) {
	testcases := []tcParseDSN{
		{
			dsn: "user:pass@account",
			config: &Config{
				Account: "account", User: "user", Password: "pass", Region: "",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
			},
			err: nil,
		},
		{
			dsn: "user:pass@account.eu-faraway",
			config: &Config{
				Account: "account", User: "user", Password: "pass", Region: "eu-faraway",
				Protocol: "https", Host: "account.eu-faraway.snowflakecomputing.com", Port: 443,
			},
			err: nil,
		},
		{
			dsn: "user:pass@account?region=eu-faraway",
			config: &Config{
				Account: "account", User: "user", Password: "pass", Region: "eu-faraway",
				Protocol: "https", Host: "account.eu-faraway.snowflakecomputing.com", Port: 443,
			},
			err: nil,
		},
		{
			dsn: "user:pass@account/db",
			config: &Config{
				Account: "account", User: "user", Password: "pass",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "public",
			},
			err: nil,
		},
		{
			dsn: "user:pass@host:123/db/schema?account=ac&protocol=http",
			config: &Config{
				Account: "ac", User: "user", Password: "pass",
				Protocol: "http", Host: "host", Port: 123,
				Database: "db", Schema: "schema",
			},
			err: nil,
		},
		{
			dsn: "user@host:123/db/schema?account=ac&protocol=http",
			config: &Config{
				Account: "ac", User: "user", Password: "pass",
				Protocol: "http", Host: "host", Port: 123,
				Database: "db", Schema: "schema",
			},
			err: ErrEmptyPassword,
		},
		{
			dsn: "@host:123/db/schema?account=ac&protocol=http",
			config: &Config{
				Account: "ac", User: "user", Password: "pass",
				Protocol: "http", Host: "host", Port: 123,
				Database: "db", Schema: "schema",
			},
			err: ErrEmptyUsername,
		},
		{
			dsn: "user:p@host:123/db/schema?protocol=http",
			config: &Config{
				Account: "ac", User: "user", Password: "pass",
				Protocol: "http", Host: "host", Port: 123,
				Database: "db", Schema: "schema",
			},
			err: ErrEmptyAccount,
		},
		{
			dsn: "u:p@a.snowflakecomputing.com/db/pa?account=a&protocol=https&role=r&timezone=UTC&warehouse=w",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "pa", Role: "r", Warehouse: "w",
			},
			err: nil,
		},
		{
			dsn: "u:p@snowflake.local:9876?account=a&protocol=http",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "http", Host: "snowflake.local", Port: 9876,
			},
			err: nil,
		},
		{
			dsn: "u:p@snowflake.local:NNNN?account=a&protocol=http",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "http", Host: "snowflake.local", Port: 9876,
			},
			err: &SnowflakeError{
				Message:     errMsgFailedToParsePort,
				MessageArgs: []interface{}{"NNNN"},
				Number:      ErrCodeFailedToParsePort,
			},
		},
		{
			dsn: "u:p@a?database=d&schema=s&role=r&application=aa&authenticator=snowflake&insecureMode=true&passcode=pp&passcodeInPassword=true",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "s", Role: "r", Authenticator: "snowflake", Application: "aa",
				InsecureMode: true, Passcode: "pp", PasscodeInPassword: true,
			},
			err: nil,
		},
		{
			// schema should be ignored as no value is specified.
			dsn: "u:p@a?database=d&schema",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "",
			},
			err: nil,
		},
		{
			dsn:    "u:p@a?database= %Sd",
			config: &Config{},
			err:    url.EscapeError(`invalid URL escape`),
		},
		{
			dsn:    "u:p@a?schema= %Sd",
			config: &Config{},
			err:    url.EscapeError(`invalid URL escape`),
		},
		{
			dsn:    "u:p@a?warehouse= %Sd",
			config: &Config{},
			err:    url.EscapeError(`invalid URL escape`),
		},
		{
			dsn:    "u:p@a?role= %Sd",
			config: &Config{},
			err:    url.EscapeError(`invalid URL escape`),
		},
	}
	for i, test := range testcases {
		glog.V(2).Infof("#%v\n", i)
		cfg, err := ParseDSN(test.dsn)
		switch {
		case test.err == nil:
			if err != nil {
				t.Fatalf("Failed to parse the DSN: %v", err)
			}
			if test.config.Account != cfg.Account {
				t.Fatalf("Failed to match account. expected: %v, got: %v",
					test.config.Account, cfg.Account)
			}
			if test.config.User != cfg.User {
				t.Fatalf("Failed to match user. expected: %v, got: %v",
					test.config.User, cfg.User)
			}
			if test.config.Password != cfg.Password {
				t.Fatalf("Failed to match password. expected: %v, got: %v",
					test.config.Password, cfg.Password)
			}
			if test.config.Database != cfg.Database {
				t.Fatalf("Failed to match database. expected: %v, got: %v",
					test.config.Database, cfg.Database)
			}
			if test.config.Schema != cfg.Schema {
				t.Fatalf("Failed to match schema. expected: %v, got: %v",
					test.config.Schema, cfg.Schema)
			}
			if test.config.Warehouse != cfg.Warehouse {
				t.Fatalf("Failed to match warehouse. expected: %v, got: %v",
					test.config.Warehouse, cfg.Warehouse)
			}
			if test.config.Role != cfg.Role {
				t.Fatalf("Failed to match role. expected: %v, got: %v",
					test.config.Role, cfg.Role)
			}
			if test.config.Region != cfg.Region {
				t.Fatalf("Failed to match region. expected: %v, got: %v",
					test.config.Region, cfg.Region)
			}
			if test.config.Protocol != cfg.Protocol {
				t.Fatalf("Failed to match protocol. expected: %v, got: %v",
					test.config.Protocol, cfg.Protocol)
			}
			if test.config.Passcode != cfg.Passcode {
				t.Fatalf("Failed to match passcode. expected: %v, got: %v",
					test.config.Passcode, cfg.Passcode)
			}
			if test.config.PasscodeInPassword != cfg.PasscodeInPassword {
				t.Fatalf("Failed to match passcodeInPassword. expected: %v, got: %v",
					test.config.PasscodeInPassword, cfg.PasscodeInPassword)
			}
		case test.err != nil:
			driverErrE, okE := test.err.(*SnowflakeError)
			driverErrG, okG := err.(*SnowflakeError)
			if okE && !okG || !okE && okG {
				t.Fatalf("Wrong error. expected: %v, got: %v", test.err, err)
			}
			if okE && okG {
				if driverErrE.Number != driverErrG.Number {
					t.Fatalf("Wrong error number. expected: %v, got: %v", driverErrE.Number, driverErrG.Number)
				}
			} else {
				t1 := reflect.TypeOf(err)
				t2 := reflect.TypeOf(test.err)
				if t1 != t2 {
					t.Fatalf("Wrong error. expected: %T:%v, got: %T:%v", test.err, test.err, err, err)
				}
			}
		}
	}
}
