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
	browserCfg, err := getSoteriaOauthSnowflakeAuthorizationCodeTestCredentials()
	assertNilF(t, err, fmt.Sprintf("failed to get browser config: %v", err))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.OauthSnowflakeSuccess, browserCfg.User, browserCfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
	}()
	wg.Wait()
}

func TestSoteriaOauthSnowflakeAuthorizationCodeMismatchedUsername(t *testing.T) {
	cfg := setupSoteriaOauthSnowflakeAuthorizationCodeTest(t)
	browserCfg, err := getSoteriaOauthSnowflakeAuthorizationCodeTestCredentials()
	assertNilF(t, err, fmt.Sprintf("failed to get browser config: %v", err))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.OauthSnowflakeSuccess, browserCfg.User, browserCfg.Password)
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

//func TestSoteriaOauthSnowflakeAuthorizationCodeOktaTimeout(t *testing.T) {
//	cfg := setupSoteriaOauthSnowflakeAuthorizationCodeTest(t)
//	cfg.ExternalBrowserTimeout = time.Duration(1) * time.Second
//	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
//  assertEqualE(t, err.Error(), "authentication via browser timed out", fmt.Sprintf("Expecteed timeout, but got %v", err))
//}

func TestSoteriaOauthSnowflakeAuthorizationCodeUsingTokenCache(t *testing.T) {
	cfg := setupSoteriaOauthSnowflakeAuthorizationCodeTest(t)
	browserCfg, err := getSoteriaOauthSnowflakeAuthorizationCodeTestCredentials()
	assertNilF(t, err, fmt.Sprintf("failed to get browser config: %v", err))

	cfg.ClientStoreTemporaryCredential = 1
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.OauthSnowflakeSuccess, browserCfg.User, browserCfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
	}()
	wg.Wait()

	cleanupBrowserProcesses(t)
	cfg.ExternalBrowserTimeout = time.Duration(1) * time.Second

	err = verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
}

func TestSoteriaOauthSnowflakeAuthorizationCodeWithoutTokenCache(t *testing.T) {
	cfg := setupSoteriaOauthSnowflakeAuthorizationCodeTest(t)
	browserCfg, err := getSoteriaOauthSnowflakeAuthorizationCodeTestCredentials()
	assertNilF(t, err, fmt.Sprintf("failed to get browser config: %v", err))
	cfg.ClientStoreTemporaryCredential = 2

	var wg sync.WaitGroup
	cfg.DisableQueryContextCache = true

	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.OauthSnowflakeSuccess, browserCfg.User, browserCfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
	}()
	wg.Wait()

	cleanupBrowserProcesses(t)
	cfg.ExternalBrowserTimeout = time.Duration(1) * time.Second

	err = verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertEqualE(t, err.Error(), "authentication via browser timed out", fmt.Sprintf("Expecteed timeout, but got %v", err))
}

func setupSoteriaOauthSnowflakeAuthorizationCodeTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	cfg, err := getAuthTestsConfig(t, AuthTypeOAuthAuthorizationCode)
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))

	cleanupBrowserProcesses(t)

	cfg.OauthClientID, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_CLIENT_ID", true)
	cfg.OauthClientSecret, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_CLIENT_SECRET", true)
	cfg.OauthRedirectURI, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_REDIRECT_URI", true)
	cfg.User, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", true)
	cfg.Role, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_ROLE", true)
	cfg.ClientStoreTemporaryCredential = 2
	return cfg
}

func getSoteriaOauthSnowflakeAuthorizationCodeTestCredentials() (*Config, error) {
	return GetConfigFromEnv([]*ConfigParam{
		{Name: "User", EnvName: "SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_USER_PASSWORD", FailOnMissing: true},
	})
}
