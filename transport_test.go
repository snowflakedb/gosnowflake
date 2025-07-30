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
	}

	factory := newTransportFactory(conflictingConfig)
	customTLS := &tls.Config{InsecureSkipVerify: true}

	transport, err := factory.CreateCustomTLSTransport(customTLS)
	if err == nil {
		t.Fatal("Expected error for conflicting OCSP and CRL configuration")
	}
	if transport != nil {
		t.Fatal("Expected nil transport when error occurs")
	}
	expectedError := "both OCSP and CRL cannot be enabled at the same time, please disable one of them"
	if err.Error() != expectedError {
		t.Fatalf("Expected error message: %s, got: %s", expectedError, err.Error())
	}
}

func TestCreateStandardTransportErrorHandling(t *testing.T) {
	// Test CreateStandardTransport with conflicting settings
	conflictingConfig := &Config{
		DisableOCSPChecks:       false,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckEnabled,
	}

	factory := newTransportFactory(conflictingConfig)

	transport, err := factory.CreateStandardTransport()
	if err == nil {
		t.Fatal("Expected error for conflicting OCSP and CRL configuration")
	}
	if transport != nil {
		t.Fatal("Expected nil transport when error occurs")
	}
}

func TestCreateCustomTLSTransportSuccess(t *testing.T) {
	// Test successful creation with valid config
	validConfig := &Config{
		DisableOCSPChecks:       true,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
	}

	factory := newTransportFactory(validConfig)
	customTLS := &tls.Config{InsecureSkipVerify: true}

	transport, err := factory.CreateCustomTLSTransport(customTLS)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if transport == nil {
		t.Fatal("Expected non-nil transport for valid configuration")
	}
}

func TestCreateStandardTransportSuccess(t *testing.T) {
	// Test successful creation with valid config
	validConfig := &Config{
		DisableOCSPChecks:       true,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
	}

	factory := newTransportFactory(validConfig)

	transport, err := factory.CreateStandardTransport()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if transport == nil {
		t.Fatal("Expected non-nil transport for valid configuration")
	}
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

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if transport == nil {
		t.Fatal("Expected non-nil transport")
	}
	if crlValidator != nil {
		t.Fatal("Expected nil CRL validator for this configuration")
	}
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
	if err != nil {
		t.Fatalf("Failed to register TLS config: %v", err)
	}
	defer DeregisterTLSConfig("test-direct")

	// Parse DSN that references the registered config
	dsn := "user:pass@account/db?tls=test-direct&ocspFailOpen=false&disableOCSPChecks=true"
	config, err2 := ParseDSN(dsn)
	if err2 != nil {
		t.Fatalf("Failed to parse DSN: %v", err2)
	}

	config.CertRevocationCheckMode = CertRevocationCheckDisabled

	factory := newTransportFactory(config)
	transport, crlValidator, err := factory.createTransport()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if transport == nil {
		t.Fatal("Expected non-nil transport")
	}
	if crlValidator != nil {
		t.Fatal("Expected nil CRL validator for this configuration")
	}
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

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if transport == nil {
		t.Fatal("Expected non-nil transport")
	}
	if crlValidator != nil {
		t.Fatal("Expected nil CRL validator for this configuration")
	}
}

func TestTLSConfigNotFound(t *testing.T) {
	// Test error handling when registered TLS config doesn't exist in DSN parsing
	dsn := "user:pass@account/db?tls=nonexistent"

	_, err := ParseDSN(dsn)
	if err == nil {
		t.Fatal("Expected error for nonexistent TLS config in DSN")
	}

	expectedError := "TLS config not found: nonexistent"
	if err.Error() != expectedError {
		t.Fatalf("Expected error message: %s, got: %s", expectedError, err.Error())
	}
}
