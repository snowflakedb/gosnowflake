package gosnowflake

import (
	"errors"
	"fmt"
	"testing"
)

func TestSoteriaOauthOktaClientCredentialsSuccessful(t *testing.T) {
	cfg := setupSoteriaOauthSnowflakeClientCredentialsTest(t)
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
}

func TestSoteriaOauthOktaClientCredentialsMismatchedUser(t *testing.T) {
	cfg := setupSoteriaOauthSnowflakeClientCredentialsTest(t)
	cfg.User = "differentUsername"
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	var snowflakeErr *SnowflakeError
	assertTrueF(t, errors.As(err, &snowflakeErr))
	assertEqualE(t, snowflakeErr.Number, 390309, fmt.Sprintf("Expected 390191, but got %v", snowflakeErr.Number))
}

//func TestSoteriaOauthOktaClientCredentialsUnauthorized(t *testing.T) {
//	cfg := setupSoteriaOauthSnowflakeClientCredentialsTest(t)
//	cfg.OauthClientID = "invalidClientID"
//	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
//	fmt.Printf(err.Error())
//	assertEqualE(t, err.Error(), "cannot fetch token: 400 Bad Request")
//}

func setupSoteriaOauthSnowflakeClientCredentialsTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	cfg, err := getAuthTestsConfig(t, AuthTypeOAuthClientCredentials)
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))

	cfg.OauthClientID, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", true)
	cfg.OauthClientSecret, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_SECRET", true)
	cfg.OauthTokenRequestURL, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_TOKEN", true)
	cfg.User, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", true)
	cfg.Role, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_ROLE", true)
	assertNilF(t, err, fmt.Sprintf("failed to parse: %v", err))
	return cfg
}
