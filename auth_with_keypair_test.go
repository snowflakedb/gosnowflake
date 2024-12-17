package gosnowflake

import (
	"crypto/rsa"
	"fmt"
	"golang.org/x/crypto/ssh"
	"os"
	"strings"
	"testing"
)

func TestKeypairSuccessful(t *testing.T) {
	cfg := setupKeyPairTest(t)
	cfg.PrivateKey = loadRsaPrivateKeyForKeyPair(t, "SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH")

	err := connectToSnowflake(cfg, "SELECT 1", true)
	assertNilF(t, err, fmt.Sprintf("failed to connect. err: %v", err))
}

func TestKeypairInvalidKey(t *testing.T) {
	cfg := setupKeyPairTest(t)
	cfg.PrivateKey = loadRsaPrivateKeyForKeyPair(t, "SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH")
	var errParts string
	errMsg := "390144 (08004): JWT token is invalid."
	err := connectToSnowflake(cfg, "SELECT 1", false)
	if err != nil {
		errParts = strings.Split(err.Error(), " [")[0]
	}
	assertTrueF(t, err != nil, fmt.Sprintf("Expected error, but got nil"))
	assertTrueF(t, errParts == errMsg, fmt.Sprintf("Expected %v, but got %v", errMsg, errParts))
}

func setupKeyPairTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")

	cfg, err := getAuthTestsConfig(AuthTypeJwt)
	assertTrueF(t, err == nil, fmt.Sprintf("failed to get config: %v", err))

	return cfg
}

func loadRsaPrivateKeyForKeyPair(t *testing.T, envName string) *rsa.PrivateKey {
	filePath, err := GetFromEnv(envName, true)

	bytes, err := os.ReadFile(filePath)
	assertNilF(t, err, fmt.Sprintf("failed to read file: %v", err))

	key, err := ssh.ParseRawPrivateKey(bytes)
	assertNilF(t, err, fmt.Sprintf("failed to parse private key: %v", err))

	return key.(*rsa.PrivateKey)
}
