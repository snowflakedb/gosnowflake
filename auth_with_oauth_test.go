package gosnowflake

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestOauthSuccessful(t *testing.T) {
	cfg := setupOauthTest(t)
	token, err := getOauthTestToken(t, cfg)
	assertNilE(t, err, fmt.Sprintf("failed to get token. err: %v", err))
	cfg.Token = token
	err = verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("failed to connect. err: %v", err))
}

func TestOauthInvalidToken(t *testing.T) {
	cfg := setupOauthTest(t)
	cfg.Token = "invalid_token"

	err := verifyConnectionToSnowflakeAuthTests(t, cfg)

	var snowflakeErr *SnowflakeError
	assertTrueF(t, errors.As(err, &snowflakeErr))
	assertEqualE(t, snowflakeErr.Number, 390303, fmt.Sprintf("Expected 390303, but got %v", snowflakeErr.Number))
}

func TestOauthMismatchedUser(t *testing.T) {
	cfg := setupOauthTest(t)
	token, err := getOauthTestToken(t, cfg)
	assertNilE(t, err, fmt.Sprintf("failed to get token. err: %v", err))
	cfg.Token = token
	cfg.User = "fakeaccount"

	err = verifyConnectionToSnowflakeAuthTests(t, cfg)

	var snowflakeErr *SnowflakeError
	assertTrueF(t, errors.As(err, &snowflakeErr))
	assertEqualE(t, snowflakeErr.Number, 390309, fmt.Sprintf("Expected 390309, but got %v", snowflakeErr.Number))
}

func setupOauthTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	cfg, err := getAuthTestsConfig(t, AuthTypeOAuth)
	assertNilF(t, err, fmt.Sprintf("failed to connect. err: %v", err))

	return cfg
}

func getOauthTestToken(t *testing.T, cfg *Config) (string, error) {

	client := &http.Client{}

	authURL, err := GetFromEnv("SNOWFLAKE_AUTH_TEST_OAUTH_URL", true)
	assertNilF(t, err, "SNOWFLAKE_AUTH_TEST_OAUTH_URL is not set")

	oauthClientID, err := GetFromEnv("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_ID", true)
	assertNilF(t, err, "SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_ID is not set")

	oauthClientSecret, err := GetFromEnv("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_SECRET", true)
	assertNilF(t, err, "SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_SECRET is not set")

	inputData := formData(cfg)

	req, err := http.NewRequest("POST", authURL, strings.NewReader(inputData.Encode()))
	assertNilF(t, err, fmt.Sprintf("Request failed %v", err))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
	req.SetBasicAuth(oauthClientID, oauthClientSecret)
	resp, err := client.Do(req)

	assertNilF(t, err, fmt.Sprintf("Response failed %v", err))

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get access token, status code: %d", resp.StatusCode)
	}

	defer resp.Body.Close()

	var response OAuthTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", fmt.Errorf("failed to decode response: %v", err)
	}

	return response.Token, err
}

func formData(cfg *Config) url.Values {
	data := url.Values{}
	data.Set("username", cfg.User)
	data.Set("password", cfg.Password)
	data.Set("grant_type", "password")
	data.Set("scope", fmt.Sprintf("session:role:%s", strings.ToLower(cfg.Role)))

	return data

}

type OAuthTokenResponse struct {
	Type       string `json:"token_type"`
	Expiration int    `json:"expires_in"`
	Token      string `json:"access_token"`
	Scope      string `json:"scope"`
}
