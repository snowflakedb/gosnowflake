package gosnowflake

import (
	"fmt"
	"net/url"
	"testing"
)

func TestOktaSuccessful(t *testing.T) {
	cfg := setupOktaTest(t)
	conn, err := connectToSnowflake(cfg, "SELECT 1", true)
	if err != nil {
		t.Fatalf("failed to connect. err: %v", err)
	}
	defer conn.Close()
}

func TestOktaWrongCredentials(t *testing.T) {
	cfg := setupOktaTest(t)
	cfg.Password = "fakePassword"
	errMsg := fmt.Sprintf("261006 (08004): failed to auth via OKTA for unknown reason. HTTP: 401, "+
		"URL: %vapi/v1/authn", cfg.OktaURL)

	_, err := connectToSnowflake(cfg, "SELECT 1", false)
	if err.Error() != errMsg {
		t.Fatalf("failed, expected: %v, but got: %v", errMsg, err.Error())
	}

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

	_, err = connectToSnowflake(cfg, "SELECT 1", false)
	if err.Error() != errMsg {
		t.Fatalf("failed, expected: %v, but got: %v", errMsg, err.Error())
	}

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

	_, err = connectToSnowflake(cfg, "SELECT 1", false)
	if err.Error() != errMsg {
		t.Fatalf("failed, expected: %v, but got: %v", errMsg, err.Error())
	}

}

func setupOktaTest(t *testing.T) *Config {
	if runningOnGithubAction() {
		t.Skip("Running only on Docker container")
	}
	skipOnJenkins(t, "Running only on Docker container")

	urlEnv, err := GetFromEnv("SNOWFLAKE_AUTH_TEST_OKTA_AUTH", true)
	if err != nil {
		return nil
	}

	cfg, err := getAuthTestsConfig(AuthTypeOkta)
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	cfg.OktaURL, err = url.Parse(urlEnv)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	return cfg
}
