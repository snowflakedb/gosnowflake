package gosnowflake

import (
	"fmt"
	"strings"
	"testing"
)

func TestOauthOktaClientCredentialsSuccessful(t *testing.T) {
	cfg := setupOauthOktaClientCredentialsTest(t)
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("failed to connect. err: %v", err))
}

func TestOauthOktaClientCredentialsMismatchedUsername(t *testing.T) {
	cfg := setupOauthOktaClientCredentialsTest(t)
	cfg.User = "invalidUser"
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)

	var snowflakeErr *SnowflakeError
	assertErrorsAsF(t, err, &snowflakeErr)
	assertEqualE(t, snowflakeErr.Number, 390309, fmt.Sprintf("Expected 390309, but got %v", snowflakeErr.Number))
}

func TestOauthOktaClientCredentialsUnauthorized(t *testing.T) {
	cfg := setupOauthOktaClientCredentialsTest(t)
	cfg.OauthClientID = "invalidClientID"
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNotNilF(t, err, "Expected an error but got nil")
	assertTrueF(t, strings.Contains(err.Error(), "invalid_client"), fmt.Sprintf("Expected error to contain 'invalid_client', but got: %v", err.Error()))
}

func setupOauthOktaClientCredentialsTest(t *testing.T) *Config {
	skipAuthTests(t, "Skipping Okta Client Credentials tests")

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
