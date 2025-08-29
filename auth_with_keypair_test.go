package gosnowflake

import (
	"crypto/rsa"
	"fmt"
	"golang.org/x/crypto/ssh"
	"os"
	"testing"
)

func TestKeypairSuccessful(t *testing.T) {
	cfg := setupKeyPairTest(t)
	cfg.PrivateKey = loadRsaPrivateKeyForKeyPair(t, "SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH")

	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("failed to connect. err: %v", err))
}

func TestKeypairInvalidKey(t *testing.T) {
	cfg := setupKeyPairTest(t)
	cfg.PrivateKey = loadRsaPrivateKeyForKeyPair(t, "SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH")
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	var snowflakeErr *SnowflakeError
	assertErrorsAsF(t, err, &snowflakeErr)
	assertEqualE(t, snowflakeErr.Number, 390144, fmt.Sprintf("Expected 390144, but got %v", snowflakeErr.Number))
}

func setupKeyPairTest(t *testing.T) *Config {
	skipAuthTests(t, "Skipping KeyPair tests")
	cfg, err := getAuthTestsConfig(t, AuthTypeJwt)
	assertEqualE(t, err, nil, fmt.Sprintf("failed to get config: %v", err))

	return cfg
}

func loadRsaPrivateKeyForKeyPair(t *testing.T, envName string) *rsa.PrivateKey {
	filePath, err := GetFromEnv(envName, true)
	assertNilF(t, err, fmt.Sprintf("failed to get env: %v", err))

	bytes, err := os.ReadFile(filePath)
	assertNilF(t, err, fmt.Sprintf("failed to read file: %v", err))

	key, err := ssh.ParseRawPrivateKey(bytes)
	assertNilF(t, err, fmt.Sprintf("failed to parse private key: %v", err))

	return key.(*rsa.PrivateKey)
}
