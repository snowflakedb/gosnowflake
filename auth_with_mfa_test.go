package gosnowflake

import (
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
	log.Printf("MFA token caching enabled")

	//Provide your own TOTP code/codes here, to test manually
	//totpKeys := []string{"222222", "333333", "444444"}

	totpKeys := getTOPTcodes(t)
	log.Printf("Got %d TOTP codes to try", len(totpKeys))

	err := verifyConnectionToSnowflakeUsingTotpCodes(t, cfg, totpKeys)
	if err != nil {
		t.Fatalf("Failed to connect with any of the %d TOTP codes: %v", len(totpKeys), err)
		return
	}

	log.Printf("Testing MFA token caching with second connection...")

	// Clear the passcode to force use of cached MFA token
	cfg.Passcode = ""

	// Attempt to connect using cached MFA token
	cacheErr := verifyConnectionToSnowflakeAuthTests(t, cfg)
	if cacheErr != nil {
		t.Fatalf("Failed to connect with cached MFA token: %v", cacheErr)
		return
	}

	log.Printf("Successfully verified MFA token caching works")
}

func setupMfaTest(t *testing.T) *Config {
	skipAuthTests(t, "Skipping MFA tests")
	cfg, err := getAuthTestsConfig(t, AuthTypeUsernamePasswordMFA)
	assertEqualE(t, err, nil, fmt.Sprintf("failed to get config: %v", err))

	cfg.User, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_MFA_USER", true)
	assertNilF(t, err, fmt.Sprintf("failed to setup config: %v", err))

	cfg.Password, err = GetFromEnv("SNOWFLAKE_AUTH_TEST_MFA_PASSWORD", true)
	assertNilF(t, err, fmt.Sprintf("failed to setup config: %v", err))

	return cfg
}

func getTOPTcodes(t *testing.T) []string {
	if isTestRunningInDockerContainer() {
		const provideTotpPath = "/externalbrowser/totpGenerator.js"
		output, err := exec.Command("node", provideTotpPath).CombinedOutput()
		assertNilE(t, err, fmt.Sprintf("failed to execute command: %v", err))
		totpCodes := strings.Fields(string(output))
		return totpCodes
	}
	return []string{}
}

func verifyConnectionToSnowflakeUsingTotpCodes(t *testing.T, cfg *Config, totpKeys []string) error {
	if len(totpKeys) == 0 {
		return fmt.Errorf("no TOTP codes provided")
	}

	var lastError error

	for i, totpKey := range totpKeys {
		log.Printf("Trying TOTP code %d/%d", i+1, len(totpKeys))

		cfg.Passcode = totpKey

		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		if err == nil {
			log.Printf("Successfully connected with TOTP code %d", i+1)
			return nil
		}

		lastError = err
		errorMsg := err.Error()

		log.Printf("TOTP code %d failed: %v", i+1, errorMsg)

		if strings.Contains(strings.ToLower(errorMsg), "TOTP Invalid") {
			log.Printf("MFA error detected, trying next code...")
			continue
		} else {
			log.Printf("Non-MFA error detected: %v", errorMsg)
			break
		}
	}

	if lastError != nil {
		return fmt.Errorf("failed to connect with any TOTP code: %v", lastError)
	}
	return fmt.Errorf("all TOTP codes failed without specific error")
}
