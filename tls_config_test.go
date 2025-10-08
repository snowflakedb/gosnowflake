package gosnowflake

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"testing"
)

// assertTLSConfigsEqual compares two TLS configurations, excluding function fields
// like VerifyPeerCertificate which may point to different but equivalent functions
func assertTLSConfigsEqual(t *testing.T, expected, actual *tls.Config, msg string) {
	if expected == nil && actual == nil {
		return
	}
	assertNotNilF(t, expected, "Expected TLS config should not be nil in %s", msg)
	assertNotNilF(t, actual, "Actual TLS config should not be nil in %s", msg)

	// Compare non-function fields
	assertEqualF(t, expected.InsecureSkipVerify, actual.InsecureSkipVerify, "%s InsecureSkipVerify", msg)
	assertEqualF(t, expected.ServerName, actual.ServerName, "%s ServerName", msg)
	assertEqualF(t, expected.MinVersion, actual.MinVersion, "%s MinVersion", msg)
	assertEqualF(t, expected.MaxVersion, actual.MaxVersion, "%s MaxVersion", msg)

	// For VerifyPeerCertificate, just check presence/absence since function pointers can't be compared
	expectedHasVerifier := expected.VerifyPeerCertificate != nil
	actualHasVerifier := actual.VerifyPeerCertificate != nil
	assertEqualF(t, expectedHasVerifier, actualHasVerifier, "%s VerifyPeerCertificate presence", msg)
}

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
	assertNilE(t, err, "RegisterTLSConfig failed")

	// Verify config was registered
	retrieved, exists := getTLSConfig("test")
	assertTrueE(t, exists, "TLS config was not registered")

	// Verify the retrieved config matches the original
	assertEqualE(t, retrieved.InsecureSkipVerify, testConfig.InsecureSkipVerify, "InsecureSkipVerify mismatch")
	assertEqualE(t, retrieved.ServerName, testConfig.ServerName, "ServerName mismatch")
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
	assertNilE(t, err, "RegisterTLSConfig failed")

	// Verify it exists
	_, exists := getTLSConfig("test")
	assertTrueE(t, exists, "TLS config should exist after registration")

	// Deregister it
	err = DeregisterTLSConfig("test")
	assertNilE(t, err, "DeregisterTLSConfig failed")

	// Verify it's gone
	_, exists = getTLSConfig("test")
	assertFalseE(t, exists, "TLS config should not exist after deregistration")
}

func TestGetTLSConfigNonExistent(t *testing.T) {
	_, exists := getTLSConfig("nonexistent")
	assertFalseE(t, exists, "getTLSConfig should return false for non-existent config")
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

	testConfig := tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: false,
	}

	err := RegisterTLSConfig("custom-ca", &testConfig)
	assertNilE(t, err, "RegisterTLSConfig failed")

	// Retrieve and verify
	retrieved, exists := getTLSConfig("custom-ca")
	assertTrueE(t, exists, "TLS config should exist")

	// The retrieved should have the same certificates as the original
	assertTrueE(t, retrieved.RootCAs.Equal(testConfig.RootCAs), "RootCAs should match")
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
		assertNilE(t, err, "RegisterTLSConfig failed for "+name)
	}

	// Verify all can be retrieved
	for name, original := range configs {
		retrieved, exists := getTLSConfig(name)
		assertTrueE(t, exists, "Config "+name+" should exist")
		assertEqualE(t, retrieved.InsecureSkipVerify, original.InsecureSkipVerify, name+" InsecureSkipVerify mismatch")
		assertEqualE(t, retrieved.ServerName, original.ServerName, name+" ServerName mismatch")
	}

	// Test overwriting
	newConfig := tls.Config{InsecureSkipVerify: false, ServerName: "new.example.com"}
	err := RegisterTLSConfig("insecure", &newConfig)
	assertNilE(t, err, "RegisterTLSConfig should allow overwriting")

	retrieved, _ := getTLSConfig("insecure")
	assertEqualE(t, retrieved.ServerName, "new.example.com", "Config should have been overwritten")
}

func TestShouldSetUpTlsConfig(t *testing.T) {
	tlsConfig := wiremockHTTPS.tlsConfig(t)
	err := RegisterTLSConfig("wiremock", tlsConfig)
	assertNilF(t, err)
	wiremockHTTPS.registerMappings(t, newWiremockMapping("auth/password/successful_flow.json"))

	for _, dbFunc := range []func() *sql.DB{
		func() *sql.DB {
			cfg := wiremockHTTPS.connectionConfig(t)
			cfg.TLSConfigName = "wiremock"
			cfg.Transporter = nil
			return sql.OpenDB(NewConnector(SnowflakeDriver{}, *cfg))
		},
		func() *sql.DB {
			cfg := wiremockHTTPS.connectionConfig(t)
			cfg.TLSConfigName = "wiremock"
			cfg.Transporter = nil
			dsn, err := DSN(cfg)
			assertNilF(t, err)
			db, err := sql.Open("snowflake", dsn)
			assertNilF(t, err)
			return db
		},
	} {
		t.Run("", func(t *testing.T) {
			db := dbFunc()
			defer db.Close()
			// mock connection, no need to close
			_, err := db.Conn(context.Background())
			assertNilF(t, err)
		})
	}
}
