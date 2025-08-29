package gosnowflake

// For compile concern, should any newly added variables or functions here must also be added with same
// name or signature but with default or empty content in the priv_key_test.go(See addParseDSNTest)

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

// helper function to generate PKCS8 encoded base64 string of a private key
func generatePKCS8StringSupress(key *rsa.PrivateKey) string {
	// Error would only be thrown when the private key type is not supported
	// We would be safe as long as we are using rsa.PrivateKey
	tmpBytes, _ := x509.MarshalPKCS8PrivateKey(key)
	privKeyPKCS8 := base64.URLEncoding.EncodeToString(tmpBytes)
	return privKeyPKCS8
}

// helper function to generate PKCS1 encoded base64 string of a private key
func generatePKCS1String(key *rsa.PrivateKey) string {
	tmpBytes := x509.MarshalPKCS1PrivateKey(key)
	privKeyPKCS1 := base64.URLEncoding.EncodeToString(tmpBytes)
	return privKeyPKCS1
}

// helper function to set up private key for testing
func setupPrivateKey() {
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	privKeyPath := env("SNOWFLAKE_TEST_PRIVATE_KEY", "")
	if privKeyPath == "" {
		customPrivateKey = false
		testPrivKey, _ = rsa.GenerateKey(rand.Reader, 2048)
	} else {
		// path to the DER file
		customPrivateKey = true
		data, _ := os.ReadFile(privKeyPath)
		block, _ := pem.Decode(data)
		if block == nil || block.Type != "PRIVATE KEY" {
			panic(fmt.Sprintf("%v is not a public key in PEM format.", privKeyPath))
		}
		privKey, _ := x509.ParsePKCS8PrivateKey(block.Bytes)
		testPrivKey = privKey.(*rsa.PrivateKey)
	}
}

func TestJWTTokenTimeout(t *testing.T) {
	brt := newBlockingRoundTripper(http.DefaultTransport, 2000*time.Millisecond)
	localTestKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assertNilF(t, err, "Failed to generate test private key")
	cfg := &Config{
		User:             "user",
		Host:             "localhost",
		Port:             wiremock.port,
		Account:          "jwtAuthTokenTimeout",
		JWTClientTimeout: 10 * time.Millisecond,
		PrivateKey:       localTestKey,
		Authenticator:    AuthTypeJwt,
		MaxRetryCount:    1,
		Transporter:      brt,
	}

	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *cfg))
	defer db.Close()
	ctx := context.Background()
	_, err = db.Conn(ctx)
	assertNotNilF(t, err)
	assertErrIsE(t, err, context.DeadlineExceeded)
}
