package gosnowflake

import (
	"errors"
	"fmt"
	"net/url"
	"testing"
)

func TestOktaSuccessful(t *testing.T) {
	cfg := setupOktaTest(t)
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("failed to connect. err: %v", err))
}

func TestOktaWrongCredentials(t *testing.T) {
	cfg := setupOktaTest(t)
	cfg.Password = "fakePassword"
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)

	var snowflakeErr *SnowflakeError
	assertTrueF(t, errors.As(err, &snowflakeErr))
	assertEqualE(t, snowflakeErr.Number, 261006, fmt.Sprintf("Expected 261006, but got %v", snowflakeErr.Number))
}

func TestOktaWrongAuthenticator(t *testing.T) {
	cfg := setupOktaTest(t)
	invalidAddress, err := url.Parse("https://fake-account-0000.okta.com")
	assertNilF(t, err, fmt.Sprintf("failed to parse: %v", err))

	cfg.OktaURL = invalidAddress
	err = verifyConnectionToSnowflakeAuthTests(t, cfg)

	var snowflakeErr *SnowflakeError
	assertTrueF(t, errors.As(err, &snowflakeErr))
	assertEqualE(t, snowflakeErr.Number, 390139, fmt.Sprintf("Expected 390139, but got %v", snowflakeErr.Number))
}

func setupOktaTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	urlEnv, err := GetFromEnv("SNOWFLAKE_AUTH_TEST_OKTA_AUTH", true)
	assertNilF(t, err, fmt.Sprintf("failed to get env: %v", err))

	cfg, err := getAuthTestsConfig(t, AuthTypeOkta)
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))

	cfg.OktaURL, err = url.Parse(urlEnv)
	assertNilF(t, err, fmt.Sprintf("failed to parse: %v", err))

	return cfg
}
