package gosnowflake

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSoteriaOauthOktaAuthorizationCodeSuccessful(t *testing.T) {
	cfg := setupSoteriaOauthOktaAuthorizationCodeTest(t)
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

func TestSoteriaOauthOktaAuthorizationCodeMismatchedUsername(t *testing.T) {
	user := setupSoteriaOauthOktaAuthorizationCodeTest(t).User
	cfg := setupSoteriaOauthOktaAuthorizationCodeTest(t)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.OauthOktaSuccess, user, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		cfg.User = "fakeUser@snowflake.com"
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		var snowflakeErr *SnowflakeError
		assertTrueF(t, errors.As(err, &snowflakeErr))
		assertEqualE(t, snowflakeErr.Number, 390309, fmt.Sprintf("Expected 390309, but got %v", snowflakeErr.Number))
	}()
	wg.Wait()
}

func TestSoteriaOauthOktaAuthorizationCodeOktaTimeout(t *testing.T) {
	cfg := setupSoteriaOauthOktaAuthorizationCodeTest(t)
	cfg.ExternalBrowserTimeout = time.Duration(1) * time.Second
	cfg.LoginTimeout = time.Duration(1) * time.Second
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
}

func TestSoteriaOauthOktaAuthorizationCodeUsingTokenCache(t *testing.T) {
	cfg := setupSoteriaOauthOktaAuthorizationCodeTest(t)
	//cfg.TURN_ON_CACHE = true
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

	cleanupBrowserProcesses(t)

	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
}

func setupSoteriaOauthOktaAuthorizationCodeTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")
	cfg, err := getAuthTestsConfig(t, AuthTypeOAuthAuthorizationCode)

	cleanupBrowserProcesses(t)

	cfg.OauthClientID, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", true)
	cfg.OauthClientSecret, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_SECRET", true)
	cfg.OauthRedirectURI, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_REDIRECT_URI", true)
	cfg.OauthAuthorizationURL, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_AUTH_URL", true)
	cfg.OauthTokenRequestURL, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_TOKEN", true)

	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))

	return cfg

}
