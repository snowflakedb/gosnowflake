package gosnowflake

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestOauthSuccessful(t *testing.T) {
	cfg := setupOauthTest(t)
	token, _ := getToken(t, cfg)
	cfg.Token = token
	err := connectToSnowflake(cfg, "SELECT 1", true)
	assertNilF(t, err, fmt.Sprintf("failed to connect. err: %v", err))
}

func TestOauthInvalidToken(t *testing.T) {
	cfg := setupOauthTest(t)
	expErr := "390303 (08004): Invalid OAuth access token. "
	cfg.Token = "invalid_token"
	err := connectToSnowflake(cfg, "SELECT 1", false)
	assertTrueF(t, err.Error() == expErr, fmt.Sprintf("Expected %v, but got %v", expErr, err))
}

func TestOauthMismatchedUser(t *testing.T) {
	cfg := setupOauthTest(t)
	token, _ := getToken(t, cfg)
	cfg.Token = token
	cfg.User = "fakeaccount"
	expErr := "390309 (08004): The user you were trying to authenticate " +
		"as differs from the user tied to the access token."
	err := connectToSnowflake(cfg, "SELECT 1", false)
	assertTrueF(t, err.Error() == expErr, fmt.Sprintf("Expected %v, but got %v", expErr, err))
}

func setupOauthTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	cfg, err := getAuthTestsConfig(AuthTypeOAuth)
	assertNilF(t, err, fmt.Sprintf("failed to connect. err: %v", err))

	return cfg
}

func getToken(t *testing.T, cfg *Config) (string, error) {

	client := &http.Client{}
	authURL, oauthClientID, oauthClientSecret := getCredentials()

	inputData := formData(cfg)

	req, _ := http.NewRequest("POST", authURL, strings.NewReader(inputData.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
	req.SetBasicAuth(oauthClientID, oauthClientSecret)
	resp, err := client.Do(req)

	assertNilF(t, err, fmt.Sprintf("Response failed %v", err))

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get access token, status code: %d", resp.StatusCode)
	}

	defer resp.Body.Close()

	var response Response
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

func getCredentials() (string, string, string) {
	authURL, _ := GetFromEnv("SNOWFLAKE_AUTH_TEST_OAUTH_URL", true)
	oauthClientID, _ := GetFromEnv("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_ID", true)
	oauthClientSecret, _ := GetFromEnv("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_SECRET", true)

	return authURL, oauthClientID, oauthClientSecret
}

type Response struct {
	Type       string `json:"token_type"`
	Expiration int    `json:"expires_in"`
	Token      string `json:"access_token"`
	Scope      string `json:"scope"`
}
