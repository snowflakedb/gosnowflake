package gosnowflake

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestOauthOktaAuthorizationCodeSuccessful(t *testing.T) {
	cfg := setupOauthOktaAuthorizationCodeTest(t)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.OauthOktaSuccess, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
	}()
	wg.Wait()
}

func TestOauthOktaAuthorizationCodeMismatchedUsername(t *testing.T) {
	cfg := setupOauthOktaAuthorizationCodeTest(t)
	cfg.User = "fakeUser@snowflake.com"

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.OauthOktaSuccess, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		var snowflakeErr *SnowflakeError
		assertTrueF(t, errors.As(err, &snowflakeErr))
		assertEqualE(t, snowflakeErr.Number, 390309, fmt.Sprintf("Expected 390309, but got %v", snowflakeErr.Number))
	}()
	wg.Wait()
}

func TestOauthOktaAuthorizationCodeOktaTimeout(t *testing.T) {
	cfg := setupOauthOktaAuthorizationCodeTest(t)
	cfg.ExternalBrowserTimeout = time.Duration(1) * time.Second
	cfg.LoginTimeout = time.Duration(1) * time.Second
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
}

func TestOauthOktaAuthorizationCodeUsingTokenCache(t *testing.T) {
	cfg := setupOauthOktaAuthorizationCodeTest(t)
	// cfg.TURN_ON_CACHE = true
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.OauthOktaSuccess, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
	}()
	wg.Wait()

	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
}

func setupOauthOktaAuthorizationCodeTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	cfg, err := getAuthTestsConfig(t, AuthTypeOAuthAuthorizationCode)
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))

	cfg.Host, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_HOST", true)
	cfg.Account, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_ACCOUNT", true)
	cfg.OauthClientID, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", true)
	cfg.OauthClientSecret, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_SECRET", true)
	cfg.OauthRedirectURI, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_REDIRECT_URI", true)
	cfg.OauthAuthorizationURL, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_AUTH_URL", true)
	cfg.OauthTokenRequestURL, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_TOKEN", true)
	cfg.User, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_BROWSER_USER", true)
	cfg.Password, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_OKTA_PASS", true)
	cfg.Role, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_ROLE", true)
	cfg.Warehouse, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_WAREHOUSE", true)
	assertNilF(t, err, fmt.Sprintf("failed to parse: %v", err))

	return cfg

}
