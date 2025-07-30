package gosnowflake

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
)

func TestRegisterTLSConfig(t *testing.T) {
	// Clean up any existing configs after testing
	defer func() {
		tlsConfigLock.Lock()
		tlsConfigRegistry = make(map[string]*tls.Config)
		tlsConfigLock.Unlock()
	}()

	testConfig := tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "test-server",
	}

	// Test successful registration
	err := RegisterTLSConfig("test", &testConfig)
	if err != nil {
		t.Fatalf("RegisterTLSConfig failed: %v", err)
	}

	// Verify config was registered
	retrieved, exists := getTLSConfig("test")
	if !exists {
		t.Fatal("TLS config was not registered")
	}

	// Verify the retrieved config matches the original
	if retrieved.InsecureSkipVerify != testConfig.InsecureSkipVerify {
		t.Fatal("Retrieved config has different InsecureSkipVerify value")
	}
	if retrieved.ServerName != testConfig.ServerName {
		t.Fatal("Retrieved config has different ServerName value")
	}
}

func TestDeregisterTLSConfig(t *testing.T) {
	// Clean up any existing configs after testing
	defer func() {
		tlsConfigLock.Lock()
		tlsConfigRegistry = make(map[string]*tls.Config)
		tlsConfigLock.Unlock()
	}()

	testConfig := tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "test-server",
	}

	// Register a config
	err := RegisterTLSConfig("test", &testConfig)
	if err != nil {
		t.Fatalf("RegisterTLSConfig failed: %v", err)
	}

	// Verify it exists
	_, exists := getTLSConfig("test")
	if !exists {
		t.Fatal("TLS config should exist after registration")
	}

	// Deregister it
	DeregisterTLSConfig("test")

	// Verify it's gone
	_, exists = getTLSConfig("test")
	if exists {
		t.Fatal("TLS config should not exist after deregistration")
	}
}

func TestGetTLSConfigNonExistent(t *testing.T) {
	_, exists := getTLSConfig("nonexistent")
	if exists {
		t.Fatal("getTLSConfig should return false for non-existent config")
	}
}

func TestRegisterTLSConfigWithCustomRootCAs(t *testing.T) {
	// Clean up any existing configs after testing
	defer func() {
		tlsConfigLock.Lock()
		tlsConfigRegistry = make(map[string]*tls.Config)
		tlsConfigLock.Unlock()
	}()

	// Create a test cert pool
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM([]byte(`-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKHHH1pVqGqNMA0GCSqGSIb3DQEBCwUAMBQxEjAQBgNVBAMMCWxv
Y2FsaG9zdDAeFw0yMzAxMDEwMDAwMDBaFw0yNDAxMDEwMDAwMDBaMBQxEjAQBgNV
BAMMCWxvY2FsaG9zdDBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC1/2...
-----END CERTIFICATE-----`))

	testConfig := tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: false,
	}

	err := RegisterTLSConfig("custom-ca", &testConfig)
	if err != nil {
		t.Fatalf("RegisterTLSConfig failed: %v", err)
	}

	// Retrieve and verify
	retrieved, exists := getTLSConfig("custom-ca")
	if !exists {
		t.Fatal("TLS config should exist")
	}

	// The retrieved should have the same certificates as the original
	if !retrieved.RootCAs.Equal(testConfig.RootCAs) {
		t.Fatal("RootCAs differs between original and retrieved")
	}
}

func TestMultipleTLSConfigs(t *testing.T) {
	// Clean up any existing configs after testing
	defer func() {
		tlsConfigLock.Lock()
		tlsConfigRegistry = make(map[string]*tls.Config)
		tlsConfigLock.Unlock()
	}()

	configs := map[string]*tls.Config{
		"insecure": {InsecureSkipVerify: true},
		"secure":   {InsecureSkipVerify: false, ServerName: "secure.example.com"},
	}

	// Register multiple configs
	for name, config := range configs {
		err := RegisterTLSConfig(name, config)
		if err != nil {
			t.Fatalf("RegisterTLSConfig failed for %s: %v", name, err)
		}
	}

	// Verify all can be retrieved
	for name, original := range configs {
		retrieved, exists := getTLSConfig(name)
		if !exists {
			t.Fatalf("Config %s should exist", name)
		}
		if retrieved.InsecureSkipVerify != original.InsecureSkipVerify {
			t.Fatalf("Config %s has wrong InsecureSkipVerify", name)
		}
		if retrieved.ServerName != original.ServerName {
			t.Fatalf("Config %s has wrong ServerName", name)
		}
	}

	// Test overwriting
	newConfig := tls.Config{InsecureSkipVerify: false, ServerName: "new.example.com"}
	err := RegisterTLSConfig("insecure", &newConfig)
	if err != nil {
		t.Fatalf("RegisterTLSConfig should allow overwriting: %v", err)
	}

	retrieved, _ := getTLSConfig("insecure")
	if retrieved.ServerName != "new.example.com" {
		t.Fatal("Config should have been overwritten")
	}
}
