package gosnowflake

import (
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"testing"
)

func TestMfaSuccessful(t *testing.T) {
	cfg := setupMfaTest(t)

	// Enable MFA token caching
	cfg.ClientRequestMfaToken = ConfigBoolTrue

	//Provide your own TOTP code/codes here, to test manually
	//totpKeys := []string{"222222", "333333", "444444"}

	totpKeys := getTOPTcodes(t)

	verifyConnectionToSnowflakeUsingTotpCodes(t, cfg, totpKeys)
	log.Printf("Testing MFA token caching with second connection...")

	// Clear the passcode to force use of cached MFA token
	cfg.Passcode = ""

	// Attempt to connect using cached MFA token
	cacheErr := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilF(t, cacheErr, "Failed to connect with cached MFA token")
}

func setupMfaTest(t *testing.T) *Config {
	skipAuthTests(t, "Skipping MFA tests")
	cfg, err := getAuthTestsConfig(t, AuthTypeUsernamePasswordMFA)
	assertNilF(t, err, "failed to get config")

	cfg.User, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_MFA_USER", true)
	assertNilF(t, err, "failed to get MFA user from environment")

	cfg.Password, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_MFA_PASSWORD", true)
	assertNilF(t, err, "failed to get MFA password from environment")

	return cfg
}

func getTOPTcodes(t *testing.T) []string {
	if isTestRunningInDockerContainer() {
		const provideTotpPath = "/externalbrowser/totpGenerator.js"
		output, err := exec.Command("node", provideTotpPath).CombinedOutput()
		assertNilF(t, err, fmt.Sprintf("failed to execute command: %v", err))
		totpCodes := strings.Fields(string(output))
		return totpCodes
	}
	return []string{}
}

func verifyConnectionToSnowflakeUsingTotpCodes(t *testing.T, cfg *Config, totpKeys []string) {
	if len(totpKeys) == 0 {
		t.Fatalf("no TOTP codes provided")
	}

	var lastError error

	for i, totpKey := range totpKeys {
		cfg.Passcode = totpKey

		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		if err == nil {
			return
		}

		lastError = err
		errorMsg := err.Error()

		log.Printf("TOTP code %d failed: %v", i+1, errorMsg)

		var snowflakeErr *SnowflakeError
		if errors.As(err, &snowflakeErr) && (snowflakeErr.Number == 394633 || snowflakeErr.Number == 394507) {
			log.Printf("MFA error detected (%d), trying next code...", snowflakeErr.Number)
			continue
		} else {
			log.Printf("Non-MFA error detected: %v", errorMsg)
			break
		}
	}

	assertNilF(t, lastError, "failed to connect with any TOTP code")
}
