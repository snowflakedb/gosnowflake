package gosnowflake

import (
	"fmt"
	"testing"
)

func setupSoteriaOauthSnowflakeClientCredentialsTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	cfg, err := getAuthTestsConfig(t, AuthTypeOAuthClientCredentials)
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))

	cfg.OauthClientID, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", true)
	cfg.OauthClientSecret, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_SECRET", true)
	cfg.OauthTokenRequestURL, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_TOKEN", true)
	cfg.User, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", true)
	assertNilF(t, err, fmt.Sprintf("failed to parse: %v", err))
	return cfg
}
