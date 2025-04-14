package gosnowflake

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestSoteriaOauthOktaClientsCredentialsSuccessful(t *testing.T) {
	cfg := setupSoteriaOauthOktaClientCredentialsTest(t)
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("failed to connect. err: %v", err))
}

func TestSoteriaOauthOktaClientsCredentialsMismatchedUsername(t *testing.T) {
	cfg := setupSoteriaOauthOktaClientCredentialsTest(t)
	cfg.User = "invalidUser"
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)

	var snowflakeErr *SnowflakeError
	assertTrueF(t, errors.As(err, &snowflakeErr))
	assertEqualE(t, snowflakeErr.Number, 390309, fmt.Sprintf("Expected 390309, but got %v", snowflakeErr.Number))
}

func TestSoteriaOauthOktaClientsCredentialsUnauthorized(t *testing.T) {
	cfg := setupSoteriaOauthOktaClientCredentialsTest(t)
	cfg.OauthClientID = "invalidClientID"
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNotNilF(t, err, "Expected an error but got nil")
	assertTrueF(t, strings.Contains(err.Error(), "invalid_client"), fmt.Sprintf("Expected error to contain 'invalid_client', but got: %v", err.Error()))
}

func setupSoteriaOauthOktaClientCredentialsTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	cfg, err := getAuthTestsConfig(t, AuthTypeOAuthClientCredentials)
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))

	cfg.OauthClientID, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", true)
	assertNilF(t, err, fmt.Sprintf("failed to setup config: %v", err))

	cfg.OauthClientSecret, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_SECRET", true)
	assertNilF(t, err, fmt.Sprintf("failed to setup config: %v", err))

	cfg.OauthTokenRequestURL, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_TOKEN", true)
	assertNilF(t, err, fmt.Sprintf("failed to setup config: %v", err))

	cfg.User, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", true)
	assertNilF(t, err, fmt.Sprintf("failed to setup config: %v", err))

	cfg.Role, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_ROLE", true)
	assertNilF(t, err, fmt.Sprintf("failed to setup config: %v", err))

	return cfg
}
