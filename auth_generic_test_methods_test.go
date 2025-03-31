package gosnowflake

import (
	"fmt"
	"testing"
)

func getAuthTestConfigFromEnv() (*Config, error) {
	return GetConfigFromEnv([]*ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_AUTH_TEST_OKTA_USER", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_AUTH_TEST_OKTA_PASS", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
		{Name: "Protocol", EnvName: "SNOWFLAKE_AUTH_TEST_PROTOCOL", FailOnMissing: false},
		{Name: "Role", EnvName: "SNOWFLAKE_TEST_ROLE", FailOnMissing: false},
		{Name: "Warehouse", EnvName: "SNOWFLAKE_TEST_WAREHOUSE", FailOnMissing: false},
	})
}

func getAuthTestsConfig(t *testing.T, authMethod AuthType) (*Config, error) {
	cfg, err := getAuthTestConfigFromEnv()
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))

	cfg.Authenticator = authMethod

	return cfg, nil
}

func getAuthLoginTestCredentials(t *testing.T) *Config {
	cfg, err := GetConfigFromEnv([]*ConfigParam{
		{Name: "User", EnvName: "SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_USER_PASSWORD", FailOnMissing: true},
	})
	assertNilF(t, err, fmt.Sprintf("failed to get login credentials config: %v", err))

	return cfg
}
