package gosnowflake

import (
	"fmt"
	"sync"
	"testing"
)

func TestOauthSnowflakeAuthorizationCodeSuccessful(t *testing.T) {
	cfg := setupOauthSnowflakeAuthorizationCodeTest(t)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.OauthSnowflakeSuccess, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
	}()
	wg.Wait()
}

func setupOauthSnowflakeAuthorizationCodeTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	cfg, err := getAuthTestsConfig(t, AuthTypeOAuthAuthorizationCode)
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))

	cfg.OauthClientID, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_CLIENT_ID", true)
	cfg.OauthClientSecret, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_CLIENT_SECRET", true)
	cfg.OauthRedirectURI, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_REDIRECT_URI", true)
	cfg.User, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", true)
	cfg.Role, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_ROLE", true)
	return cfg
}
