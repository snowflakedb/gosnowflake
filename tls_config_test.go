// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"crypto/tls"
	"crypto/x509"
	"sync"
	"testing"
)

func TestRegisterTLSConfig(t *testing.T) {
	// Clean up any existing configs before testing
	defer func() {
		tlsConfigLock.Lock()
		tlsConfigRegistry = make(map[string]*tls.Config)
		tlsConfigLock.Unlock()
	}()

	testConfig := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "test-server",
	}

	// Test successful registration
	err := RegisterTLSConfig("test", testConfig)
	if err != nil {
		t.Fatalf("RegisterTLSConfig failed: %v", err)
	}

	// Verify config was registered
	clone, exists := getTLSConfigClone("test")
	if !exists {
		t.Fatal("TLS config was not registered")
	}

	// Verify the clone is correct but not the same object (to prevent modification)
	if clone == testConfig {
		t.Fatal("getTLSConfigClone should return a clone, not the original")
	}
	if clone.InsecureSkipVerify != testConfig.InsecureSkipVerify {
		t.Fatal("Cloned config has different InsecureSkipVerify value")
	}
	if clone.ServerName != testConfig.ServerName {
		t.Fatal("Cloned config has different ServerName value")
	}
}

func TestRegisterTLSConfigNil(t *testing.T) {
	err := RegisterTLSConfig("test", nil)
	if err == nil {
		t.Fatal("RegisterTLSConfig should fail with nil config")
	}
	if err.Error() != "config is nil" {
		t.Fatalf("Expected 'config is nil' error, got: %v", err)
	}
}

func TestDeregisterTLSConfig(t *testing.T) {
	// Clean up
	defer func() {
		tlsConfigLock.Lock()
		tlsConfigRegistry = make(map[string]*tls.Config)
		tlsConfigLock.Unlock()
	}()

	testConfig := &tls.Config{InsecureSkipVerify: true}

	// Register a config
	err := RegisterTLSConfig("test", testConfig)
	if err != nil {
		t.Fatalf("RegisterTLSConfig failed: %v", err)
	}

	// Verify it exists
	_, exists := getTLSConfigClone("test")
	if !exists {
		t.Fatal("TLS config should exist after registration")
	}

	// Deregister it
	DeregisterTLSConfig("test")

	// Verify it's gone
	_, exists = getTLSConfigClone("test")
	if exists {
		t.Fatal("TLS config should not exist after deregistration")
	}
}

func TestGetTLSConfigCloneNonExistent(t *testing.T) {
	_, exists := getTLSConfigClone("nonexistent")
	if exists {
		t.Fatal("getTLSConfigClone should return false for non-existent config")
	}
}

func TestTLSConfigConcurrency(t *testing.T) {
	// Clean up
	defer func() {
		tlsConfigLock.Lock()
		tlsConfigRegistry = make(map[string]*tls.Config)
		tlsConfigLock.Unlock()
	}()

	// Test concurrent access to ensure thread safety
	var wg sync.WaitGroup
	numGoroutines := 10

	// Concurrently register configs
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			config := &tls.Config{
				ServerName: "server" + string(rune(index)),
			}
			err := RegisterTLSConfig("test"+string(rune(index)), config)
			if err != nil {
				t.Errorf("RegisterTLSConfig failed: %v", err)
			}
		}(i)
	}

	// Concurrently read configs
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, exists := getTLSConfigClone("test" + string(rune(index)))
			if !exists {
				t.Errorf("Config test%d should exist", index)
			}
		}(i)
	}

	wg.Wait()
}

func TestDSNParsingWithTLSConfig(t *testing.T) {
	testCases := []struct {
		name     string
		dsn      string
		expected string
	}{
		{
			name:     "Basic TLS config parameter",
			dsn:      "user:pass@account/db?tls=custom",
			expected: "custom",
		},
		{
			name:     "TLS config with other parameters",
			dsn:      "user:pass@account/db?tls=custom&warehouse=wh&role=admin",
			expected: "custom",
		},
		{
			name:     "No TLS config parameter",
			dsn:      "user:pass@account/db?warehouse=wh",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseDSN(tc.dsn)
			if err != nil {
				t.Fatalf("ParseDSN failed: %v", err)
			}
			if cfg.TLSConfig != tc.expected {
				t.Fatalf("Expected TLSConfig %q, got %q", tc.expected, cfg.TLSConfig)
			}
		})
	}
}

func TestRegisterTLSConfigWithCustomRootCAs(t *testing.T) {
	// Clean up
	defer func() {
		tlsConfigLock.Lock()
		tlsConfigRegistry = make(map[string]*tls.Config)
		tlsConfigLock.Unlock()
	}()

	// Create a custom certificate pool
	certPool := x509.NewCertPool()
	// Add a dummy certificate for testing
	certPool.AppendCertsFromPEM([]byte(`-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BsSuuPiQi4d+NhU2BvQHmEQUAABOBG1k5
YBNNm/hbJbr7kWLFGo2jYzBhMA4GA1UdDwEB/wQEAwIBBjAPBgNVHRMBAf8EBTAD
AQH/MB0GA1UdDgQWBBSY0fhuILkXkz3Gjq1XvdOO/+7tSDAOBgNVHQ8BAf8EBAMC
AQYwDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiEAuK3VWfXWWF0b
C0wjdl3sP3p7J2Vt8mEqGLFQX4Nk8B0CIGCOJl7VebrF1S2A/l7s6r4z9M8Yw4oJ
2HGFo1ISJL4g
-----END CERTIFICATE-----`))

	testConfig := &tls.Config{
		RootCAs: certPool,
	}

	// Register the config
	err := RegisterTLSConfig("custom-ca", testConfig)
	if err != nil {
		t.Fatalf("RegisterTLSConfig failed: %v", err)
	}

	// Verify the config was registered with the custom RootCAs
	clone, exists := getTLSConfigClone("custom-ca")
	if !exists {
		t.Fatal("TLS config was not registered")
	}

	if clone.RootCAs == nil {
		t.Fatal("RootCAs was not preserved in cloned config")
	}

	// The clone should have the same number of certificates
	if len(clone.RootCAs.Subjects()) != len(testConfig.RootCAs.Subjects()) {
		t.Fatal("RootCAs certificate count differs between original and clone")
	}
}

func TestMultipleTLSConfigs(t *testing.T) {
	// Clean up
	defer func() {
		tlsConfigLock.Lock()
		tlsConfigRegistry = make(map[string]*tls.Config)
		tlsConfigLock.Unlock()
	}()

	configs := map[string]*tls.Config{
		"insecure": {InsecureSkipVerify: true},
		"strict":   {InsecureSkipVerify: false, ServerName: "strict-server"},
		"custom":   {MinVersion: tls.VersionTLS12},
	}

	// Register all configs
	for name, config := range configs {
		err := RegisterTLSConfig(name, config)
		if err != nil {
			t.Fatalf("RegisterTLSConfig failed for %s: %v", name, err)
		}
	}

	// Verify all configs exist and are correct
	for name, originalConfig := range configs {
		clone, exists := getTLSConfigClone(name)
		if !exists {
			t.Fatalf("Config %s should exist", name)
		}

		// Verify key properties are preserved
		if clone.InsecureSkipVerify != originalConfig.InsecureSkipVerify {
			t.Fatalf("InsecureSkipVerify mismatch for %s", name)
		}
		if clone.ServerName != originalConfig.ServerName {
			t.Fatalf("ServerName mismatch for %s", name)
		}
		if clone.MinVersion != originalConfig.MinVersion {
			t.Fatalf("MinVersion mismatch for %s", name)
		}
	}

	// Test overwriting a config
	newConfig := &tls.Config{InsecureSkipVerify: false}
	err := RegisterTLSConfig("insecure", newConfig)
	if err != nil {
		t.Fatalf("RegisterTLSConfig should allow overwriting: %v", err)
	}

	clone, _ := getTLSConfigClone("insecure")
	if clone.InsecureSkipVerify != false {
		t.Fatal("Config should have been overwritten")
	}
}
