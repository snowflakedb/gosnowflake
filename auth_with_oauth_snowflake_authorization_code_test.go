package gosnowflake

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSoteriaOauthSnowflakeAuthorizationCodeSuccessful(t *testing.T) {
	cfg := setupSoteriaOauthSnowflakeAuthorizationCodeTest(t)
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

func TestSoteriaOauthSnowflakeAuthorizationCodeMismatchedUsername(t *testing.T) {
	user := setupSoteriaOauthSnowflakeAuthorizationCodeTest(t).User
	cfg := setupSoteriaOauthSnowflakeAuthorizationCodeTest(t)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.OauthSnowflakeSuccess, user, cfg.Password)
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

func TestSoteriaOauthSnowflakeAuthorizationCodeOktaTimeout(t *testing.T) {
	cfg := setupSoteriaOauthSnowflakeAuthorizationCodeTest(t)
	cfg.ExternalBrowserTimeout = time.Duration(1) * time.Second
	cfg.LoginTimeout = time.Duration(1) * time.Second
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
}

func TestSoteriaOauthSnowflakeAuthorizationCodeUsingTokenCache(t *testing.T) {
	cfg := setupSoteriaOauthSnowflakeAuthorizationCodeTest(t)
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

	cleanupBrowserProcesses(t)

	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
}

func TestSoteriaOauthSnowflakeAuthorizationCodeWithoutTokenCache(t *testing.T) {
	cfg := setupSoteriaOauthSnowflakeAuthorizationCodeTest(t)
	var wg sync.WaitGroup
	cfg.DisableQueryContextCache = true
	cfg.LoginTimeout = time.Duration(1) * time.Second

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

	cleanupBrowserProcesses(t)

	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
}

func setupSoteriaOauthSnowflakeAuthorizationCodeTest(t *testing.T) *Config {
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
