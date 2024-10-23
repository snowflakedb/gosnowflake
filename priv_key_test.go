// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

// For compile concern, should any newly added variables or functions here must also be added with same
// name or signature but with default or empty content in the priv_key_test.go(See addParseDSNTest)

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
	"testing"
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

// Helper function to add encoded private key to dsn
func appendPrivateKeyString(dsn *string, key *rsa.PrivateKey) string {
	var b bytes.Buffer
	b.WriteString(*dsn)
	b.WriteString(fmt.Sprintf("&authenticator=%v", AuthTypeJwt.String()))
	b.WriteString(fmt.Sprintf("&privateKey=%s", generatePKCS8StringSupress(key)))
	return b.String()
}

// Integration test for the JWT authentication function
func TestJWTAuthentication(t *testing.T) {
	// For private key generated on the fly, we want to load the public key to the server first
	if !customPrivateKey {
		conn := openConn(t)
		defer conn.Close()
		// Load server's public key to database
		pubKeyByte, err := x509.MarshalPKIXPublicKey(testPrivKey.Public())
		if err != nil {
			t.Fatalf("error marshaling public key: %s", err.Error())
		}
		if _, err = conn.ExecContext(context.Background(), "USE ROLE ACCOUNTADMIN"); err != nil {
			t.Fatalf("error changin role: %s", err.Error())
		}
		encodedKey := base64.StdEncoding.EncodeToString(pubKeyByte)
		if _, err = conn.ExecContext(context.Background(), fmt.Sprintf("ALTER USER %v set rsa_public_key='%v'", username, encodedKey)); err != nil {
			t.Fatalf("error setting server's public key: %s", err.Error())
		}
	}

	// Test that a valid private key can pass
	jwtDSN := appendPrivateKeyString(&dsn, testPrivKey)
	db, err := sql.Open("snowflake", jwtDSN)
	if err != nil {
		t.Fatalf("error creating a connection object: %s", err.Error())
	}
	if _, err = db.Exec("SELECT 1"); err != nil {
		t.Fatalf("error executing: %s", err.Error())
	}
	db.Close()

	// Test that an invalid private key cannot pass
	invalidPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Error(err)
	}
	jwtDSN = appendPrivateKeyString(&dsn, invalidPrivateKey)
	db, err = sql.Open("snowflake", jwtDSN)
	if err != nil {
		t.Error(err)
	}
	if _, err = db.Exec("SELECT 1"); err == nil {
		t.Fatalf("An invalid jwt token can pass")
	}

	db.Close()
}

func TestJWTTokenTimeout(t *testing.T) {
	resetHTTPMocks(t)

	dsn := "user:pass@localhost:12345/db/schema?account=jwtAuthTokenTimeout&protocol=http&jwtClientTimeout=1"
	dsn = appendPrivateKeyString(&dsn, testPrivKey)
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer conn.Close()

	invocations := getMocksInvocations(t)
	if invocations != 3 {
		t.Errorf("Unexpected number of invocations, expected 3, got %v", invocations)
	}
}
