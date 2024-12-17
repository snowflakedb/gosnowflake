package gosnowflake

import (
	"fmt"
	"net/url"
	"testing"
)

func TestOktaSuccessful(t *testing.T) {
	cfg := setupOktaTest(t)
	err := connectToSnowflake(cfg, "SELECT 1", true)
	assertNilF(t, err, fmt.Sprintf("failed to connect. err: %v", err))
}

func TestOktaWrongCredentials(t *testing.T) {
	cfg := setupOktaTest(t)
	cfg.Password = "fakePassword"
	errMsg := fmt.Sprintf("261006 (08004): failed to auth via OKTA for unknown reason. HTTP: 401, "+
		"URL: %vapi/v1/authn", cfg.OktaURL)

	err := connectToSnowflake(cfg, "SELECT 1", false)

	assertTrueF(t, err.Error() == errMsg, fmt.Sprintf("Expected %v, but got %v", errMsg, err.Error()))
}

func TestOktaWrongAuthenticator(t *testing.T) {
	cfg := setupOktaTest(t)
	invalidAddress, err := url.Parse("https://fake-account-0000.okta.com")
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	cfg.OktaURL = invalidAddress
	errMsg := "390139 (08004): The specified authenticator is not accepted by your Snowflake account configuration.  " +
		"Please contact your local system administrator to get the correct URL to use."

	err = connectToSnowflake(cfg, "SELECT 1", false)
	assertTrueF(t, err.Error() == errMsg, fmt.Sprintf("Expected %v, but got %v", errMsg, err.Error()))
}

func TestOktaWrongURL(t *testing.T) {
	cfg := setupOktaTest(t)
	invalidAddress, err := url.Parse("https://fake-account-0000.okta.com")
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	cfg.OktaURL = invalidAddress
	errMsg := "390139 (08004): The specified authenticator is not accepted by your Snowflake account configuration.  " +
		"Please contact your local system administrator to get the correct URL to use."

	err = connectToSnowflake(cfg, "SELECT 1", false)
	assertTrueF(t, err.Error() == errMsg, fmt.Sprintf("Expected %v, but got %v", errMsg, err.Error()))
}

func setupOktaTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	urlEnv, err := GetFromEnv("SNOWFLAKE_AUTH_TEST_OKTA_AUTH", true)
	assertNilF(t, err, fmt.Sprintf("failed to get env: %v", err))

	cfg, err := getAuthTestsConfig(AuthTypeOkta)
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))

	cfg.OktaURL, err = url.Parse(urlEnv)
	assertNilF(t, err, fmt.Sprintf("failed to parse: %v", err))

	return cfg
}
