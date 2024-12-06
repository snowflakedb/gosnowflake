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
	token, _ := getToken(cfg)
	cfg.Token = token
	conn, err := connectToSnowflake(cfg, "SELECT 1", true)
	if err != nil {
		t.Fatalf("failed to connect. err: %v", err)
	}
	defer conn.Close()
}

func TestOauthInvalidToken(t *testing.T) {
	cfg := setupOauthTest(t)
	expErr := "390303 (08004): Invalid OAuth access token. "
	cfg.Token = "invalid_token"

	_, err := connectToSnowflake(cfg, "SELECT 1", false)
	if err.Error() != expErr {
		t.Fatalf("Expected %v, but got %v", expErr, err)
	}
}

func TestOauthMismatchedUser(t *testing.T) {
	cfg := setupOauthTest(t)
	token, _ := getToken(cfg)
	cfg.Token = token
	cfg.User = "fakeaccount"
	expErr := "390309 (08004): The user you were trying to authenticate " +
		"as differs from the user tied to the access token."

	_, err := connectToSnowflake(cfg, "SELECT 1", false)
	if err.Error() != expErr {
		t.Fatalf("Expected %v, but got %v", expErr, err)
	}
}

func setupOauthTest(t *testing.T) *Config {
	if runningOnGithubAction() {
		t.Skip("Running only on Docker container")
	}
	skipOnJenkins(t, "Running only on Docker container")

	cfg, err := getConfig(AuthTypeOAuth)
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	return cfg
}

func getToken(cfg *Config) (string, error) {

	client := &http.Client{}
	authURL, oauthClientID, oauthClientSecret := getCredentials()

	inputData := formData(cfg)

	req, _ := http.NewRequest("POST", authURL, strings.NewReader(inputData.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
	req.SetBasicAuth(oauthClientID, oauthClientSecret)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Response failed %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get access token, status code: %d", resp.StatusCode)
	}

	defer resp.Body.Close()

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		panic(err)
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
