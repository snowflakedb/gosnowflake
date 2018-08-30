// +build go1.10

// This file contains variables or functions of test cases that we want to run for go version >= 1.10

// For compile concern, should any newly added variables or functions here must also be added with same
// name or signature but with default or empty content in the optional_go10-_test.go(See addParseDSNTest)

package gosnowflake

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strconv"
	"testing"
)

func addParseDSNTest(parseDSNTests []tcParseDSN) []tcParseDSN {
	privKeyPKCS8 := generatePKCS8StringSupress(TestPrivKey)
	privKeyPKCS1 := generatePKCS1String(TestPrivKey)

	optParseDSNTests := []tcParseDSN{
		{
			dsn: fmt.Sprintf("u:p@ac.snowflake.local:9876?account=ac&protocol=http&authenticator=SNOWFLAKE_JWT&privateKey=%v", privKeyPKCS8),
			config: &Config{
				Account: "ac", User: "u", Password: "p",
				Authenticator: authenticatorJWT, PrivateKey: TestPrivKey,
				Protocol: "http", Host: "snowflake.local", Port: 9876,
			},
			err: nil,
		},
		{
			dsn: fmt.Sprintf("u:p@a.snowflake.local:9876?account=a&protocol=http&authenticator=SNOWFLAKE_JWT&privateKey=%v", privKeyPKCS1),
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Authenticator: authenticatorJWT, PrivateKey: TestPrivKey,
				Protocol: "http", Host: "snowflake.local", Port: 9876,
			},
			err: &SnowflakeError{Number: ErrCodePrivateKeyParseError},
		}}
	parseDSNTests = append(parseDSNTests, optParseDSNTests...)
	return parseDSNTests
}

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

// Test the JWT authentication is working
func TestJWTAuthentication(t *testing.T) {
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("error creating a connection object: %s", err.Error())
	}

	// Load server's public key to database
	pubKeyByte, err := x509.MarshalPKIXPublicKey(TestPrivKey.Public())
	if err != nil {
		t.Fatalf("error marshaling public key: %s", err.Error())
	}
	_, err = db.Exec("USE ROLE ACCOUNTADMIN")
	if err != nil {
		t.Fatalf("error changin role: %s", err.Error())
	}
	_, err = db.Exec(fmt.Sprintf("ALTER USER %v set rsa_public_key='%v'",
		user, base64.StdEncoding.EncodeToString(pubKeyByte)))
	if err != nil {
		t.Fatalf("error setting server's public key: %s", err.Error())
	}
	db.Close()

	// Test that a valid private key can pass
	portNum, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("Invalid port number %s", port)
	}
	config := Config{
		User:          user,
		Host:          purehost,
		Password:      pass,
		Port:          portNum,
		Account:       account,
		Authenticator: authenticatorJWT,
		Protocol:      "http",
		PrivateKey:    TestPrivKey,
	}
	jwtDSN, err := DSN(&config)
	if err != nil {
		t.Fatalf("Error parsing DSN %s", err.Error())
	}
	db, err = sql.Open("snowflake", jwtDSN)
	if err != nil {
		t.Fatalf("error creating a connection object: %s", err.Error())
	}
	_, err = db.Exec("SELECT 1")
	if err != nil {
		t.Fatalf("error executing: %s", err.Error())
	}
	db.Close()

	// Test that an invalid private key cannot pass
	invalidPrivateKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	config.PrivateKey = invalidPrivateKey
	jwtDSN, _ = DSN(&config)
	db, _ = sql.Open("snowflake", jwtDSN)
	_, err = db.Exec("SELECT 1")
	if err == nil {
		t.Fatalf("An invalid jwt token can pass")
	}

	db.Close()
}
