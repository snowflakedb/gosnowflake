package gosnowflake

import (
	"crypto/tls"
	"net/http"
	"testing"
)

// assertTransportsEqual compares two transports, excluding function fields and other non-comparable fields
// that may vary between instances but represent equivalent configurations
func assertTransportsEqual(t *testing.T, expected, actual *http.Transport, msg string) {
	if expected == nil && actual == nil {
		return
	}
	assertNotNilF(t, expected, "Expected transport should not be nil in %s", msg)
	assertNotNilF(t, actual, "Actual transport should not be nil in %s", msg)

	// Compare TLS configurations
	assertTLSConfigsEqual(t, expected.TLSClientConfig, actual.TLSClientConfig, msg+" TLS config")

	// Compare other relevant transport fields (excluding function fields)
	assertEqualF(t, expected.MaxIdleConns, actual.MaxIdleConns, "%s MaxIdleConns", msg)
	assertEqualF(t, expected.MaxIdleConnsPerHost, actual.MaxIdleConnsPerHost, "%s MaxIdleConnsPerHost", msg)
	assertEqualF(t, expected.MaxConnsPerHost, actual.MaxConnsPerHost, "%s MaxConnsPerHost", msg)
	assertEqualF(t, expected.IdleConnTimeout, actual.IdleConnTimeout, "%s IdleConnTimeout", msg)
	assertEqualF(t, expected.ResponseHeaderTimeout, actual.ResponseHeaderTimeout, "%s ResponseHeaderTimeout", msg)
	assertEqualF(t, expected.ExpectContinueTimeout, actual.ExpectContinueTimeout, "%s ExpectContinueTimeout", msg)
	assertEqualF(t, expected.TLSHandshakeTimeout, actual.TLSHandshakeTimeout, "%s TLSHandshakeTimeout", msg)
	assertEqualF(t, expected.DisableKeepAlives, actual.DisableKeepAlives, "%s DisableKeepAlives", msg)
	assertEqualF(t, expected.DisableCompression, actual.DisableCompression, "%s DisableCompression", msg)
	assertEqualF(t, expected.ForceAttemptHTTP2, actual.ForceAttemptHTTP2, "%s ForceAttemptHTTP2", msg)
}

func TestTransportFactoryErrorHandling(t *testing.T) {
	// Test CreateCustomTLSTransport with conflicting OCSP and CRL settings
	conflictingConfig := &Config{
		DisableOCSPChecks:       false,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckEnabled,
		tlsConfig:               &tls.Config{InsecureSkipVerify: true},
	}

	factory := newTransportFactory(conflictingConfig, nil)

	transport, err := factory.createTransport()
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

	factory := newTransportFactory(conflictingConfig, nil)

	transport, err := factory.createTransport()
	assertNotNilF(t, err, "Expected error for conflicting OCSP and CRL configuration")
	assertNilF(t, transport, "Expected nil transport when error occurs")
}

func TestCreateCustomTLSTransportSuccess(t *testing.T) {
	// Test successful creation with valid config
	validConfig := &Config{
		DisableOCSPChecks:       true,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
		tlsConfig:               &tls.Config{InsecureSkipVerify: true},
	}

	factory := newTransportFactory(validConfig, nil)

	transport, err := factory.createTransport()
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

	factory := newTransportFactory(validConfig, nil)

	transport, err := factory.createTransport()
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
		DisableOCSPChecks:       true,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
		tlsConfig:               customTLS, // Direct TLS config
	}

	factory := newTransportFactory(config, nil)
	transport, err := factory.createTransport()

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")
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

	factory := newTransportFactory(config, nil)
	transport, err := factory.createTransport()

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")
}

func TestDirectTLSConfigOnly(t *testing.T) {
	// Test that direct TLS config works without any registration

	// Create a direct TLS config
	directTLS := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "direct.example.com",
	}

	config := &Config{
		DisableOCSPChecks:       true,
		InsecureMode:            false,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
		tlsConfig:               directTLS, // Direct config
	}

	factory := newTransportFactory(config, nil)
	transport, err := factory.createTransport()

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")
}

func TestProxyTransportCreation(t *testing.T) {
	proxyTests := []struct {
		config   *Config
		proxyURL string
	}{
		{
			config: &Config{
				ProxyProtocol: "http",
				ProxyHost:     "proxy.connection.com",
				ProxyPort:     1234,
			},
			proxyURL: "http://proxy.connection.com:1234",
		},
		{
			config: &Config{
				ProxyProtocol: "http",
				ProxyHost:     "proxy.connection.com",
				ProxyPort:     1234,
			},
			proxyURL: "http://proxy.connection.com:1234",
		},

		{
			config: &Config{
				ProxyProtocol: "https",
				ProxyHost:     "proxy.connection.com",
				ProxyPort:     1234,
			},
			proxyURL: "https://proxy.connection.com:1234",
		},
		{
			config: &Config{
				ProxyProtocol: "http",
				ProxyHost:     "proxy.connection.com",
				ProxyPort:     1234,
				NoProxy:       "*.snowflakecomputing.com,ocsp.testing.com",
			},
			proxyURL: "",
		},
	}

	for _, test := range proxyTests {

		factory := newTransportFactory(test.config, nil)
		proxyFunc := factory.createProxy()

		req, _ := http.NewRequest("GET", "https://testing.snowflakecomputing.com", nil)
		proxyURL, _ := proxyFunc(req)

		if test.proxyURL == "" {
			assertNilF(t, proxyURL, "Expected nil proxy for https request")
		} else {
			assertEqualF(t, proxyURL.String(), test.proxyURL)
		}

		req, _ = http.NewRequest("GET", "http://ocsp.testing.com", nil)
		proxyURL, _ = proxyFunc(req)

		if test.proxyURL == "" {
			assertNilF(t, proxyURL, "Expected nil proxy for https request")
		} else {
			assertEqualF(t, proxyURL.String(), test.proxyURL)
		}
	}
}
