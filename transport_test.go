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

	factory := NewTransportFactory(conflictingConfig)
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

	factory := NewTransportFactory(conflictingConfig)

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

	factory := NewTransportFactory(validConfig)
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

	factory := NewTransportFactory(validConfig)

	transport, err := factory.CreateStandardTransport()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if transport == nil {
		t.Fatal("Expected non-nil transport for valid configuration")
	}
}
