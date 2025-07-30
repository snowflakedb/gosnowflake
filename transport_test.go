package gosnowflake

import (
	"crypto/tls"
	"testing"
)

func TestTransportFactoryErrorHandling(t *testing.T) {
	// Test CreateCustomTLSTransport with conflicting OCSP and CRL settings
	conflictingConfig := &Config{
		DisableOCSPChecks:       false,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckEnabled,
		TLSConfig:               &tls.Config{InsecureSkipVerify: true},
	}

	factory := newTransportFactory(conflictingConfig)

	transport, _, err := factory.createTransport()
	assertNotNilF(t, err, "Expected error for conflicting OCSP and CRL configuration")
	assertNilF(t, transport, "Expected nil transport when error occurs")
	expectedError := "both OCSP and CRL cannot be enabled at the same time, please disable one of them"
	assertEqualF(t, err.Error(), expectedError, "Expected specific error message")
}

func TestCreateStandardTransportErrorHandling(t *testing.T) {
	// Test CreateStandardTransport with conflicting settings
	conflictingConfig := &Config{
		DisableOCSPChecks:       false,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckEnabled,
	}

	factory := newTransportFactory(conflictingConfig)

	transport, _, err := factory.createTransport()
	assertNotNilF(t, err, "Expected error for conflicting OCSP and CRL configuration")
	assertNilF(t, transport, "Expected nil transport when error occurs")
}

func TestCreateCustomTLSTransportSuccess(t *testing.T) {
	// Test successful creation with valid config
	validConfig := &Config{
		DisableOCSPChecks:       true,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
		TLSConfig:               &tls.Config{InsecureSkipVerify: true},
	}

	factory := newTransportFactory(validConfig)

	transport, _, err := factory.createTransport()
	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport for valid configuration")
}

func TestCreateStandardTransportSuccess(t *testing.T) {
	// Test successful creation with valid config
	validConfig := &Config{
		DisableOCSPChecks:       true,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
	}

	factory := newTransportFactory(validConfig)

	transport, _, err := factory.createTransport()
	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport for valid configuration")
}

func TestDirectTLSConfigUsage(t *testing.T) {
	// Test the new direct TLS config approach
	customTLS := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "custom.example.com",
	}

	config := &Config{
		TLSConfig:               customTLS, // Direct TLS config
		DisableOCSPChecks:       true,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
	}

	factory := newTransportFactory(config)
	transport, crlValidator, err := factory.createTransport()

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")
	assertNilF(t, crlValidator, "Expected nil CRL validator for this configuration")
}

func TestRegisteredTLSConfigUsage(t *testing.T) {
	// Test registered TLS config approach through DSN parsing

	// Clean up any existing registry
	tlsConfigLock.Lock()
	tlsConfigRegistry = make(map[string]*tls.Config)
	tlsConfigLock.Unlock()

	// Register a custom TLS config
	customTLS := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "registered.example.com",
	}
	err := RegisterTLSConfig("test-direct", customTLS)
	assertNilF(t, err, "Failed to register TLS config")
	defer func() {
		err := DeregisterTLSConfig("test-direct")
		assertNilF(t, err, "Failed to deregister test TLS config")
	}()

	// Parse DSN that references the registered config
	dsn := "user:pass@account/db?tls=test-direct&ocspFailOpen=false&disableOCSPChecks=true"
	config, err2 := ParseDSN(dsn)
	assertNilF(t, err2, "Failed to parse DSN")

	config.CertRevocationCheckMode = CertRevocationCheckDisabled

	factory := newTransportFactory(config)
	transport, crlValidator, err := factory.createTransport()

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")
	assertNilF(t, crlValidator, "Expected nil CRL validator for this configuration")
}

func TestDirectTLSConfigOnly(t *testing.T) {
	// Test that direct TLS config works without any registration

	// Create a direct TLS config
	directTLS := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "direct.example.com",
	}

	config := &Config{
		TLSConfig:               directTLS, // Direct config
		DisableOCSPChecks:       true,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
	}

	factory := newTransportFactory(config)
	transport, crlValidator, err := factory.createTransport()

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")
	assertNilF(t, crlValidator, "Expected nil CRL validator for this configuration")
}

func TestTLSConfigNotFound(t *testing.T) {
	// Test error handling when registered TLS config doesn't exist in DSN parsing
	dsn := "user:pass@account/db?tls=nonexistent"

	_, err := ParseDSN(dsn)
	assertNotNilF(t, err, "Expected error for nonexistent TLS config in DSN")

	expectedError := "TLS config not found: nonexistent"
	assertEqualF(t, err.Error(), expectedError, "Expected specific error message")
}
