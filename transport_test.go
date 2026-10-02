package gosnowflake

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"testing"

	sfconfig "github.com/snowflakedb/gosnowflake/v2/internal/config"
)

func TestTransportFactoryErrorHandling(t *testing.T) {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	assertNilF(t, RegisterTLSConfig("TestTransportFactoryErrorHandlingTlsConfig", tlsConfig))
	// Test CreateCustomTLSTransport with conflicting OCSP and CRL settings
	conflictingConfig := &Config{
		DisableOCSPChecks:       false,
		OCSPFailOpen:            OCSPFailOpenTrue,
		CertRevocationCheckMode: CertRevocationCheckEnabled,
		TLSConfigName:           "TestTransportFactoryErrorHandlingTlsConfig",
	}

	factory := newTransportFactory(conflictingConfig, nil)

	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))
	assertNotNilF(t, err, "Expected error for conflicting OCSP and CRL configuration")
	assertNilF(t, transport, "Expected nil transport when error occurs")
	expectedError := "both OCSP and CRL cannot be enabled at the same time, please disable one of them"
	assertEqualF(t, err.Error(), expectedError, "Expected specific error message")
}

func TestCreateStandardTransportErrorHandling(t *testing.T) {
	// Test CreateStandardTransport with conflicting settings
	conflictingConfig := &Config{
		DisableOCSPChecks:       false,
		OCSPFailOpen:            OCSPFailOpenTrue,
		CertRevocationCheckMode: CertRevocationCheckEnabled,
	}

	factory := newTransportFactory(conflictingConfig, nil)

	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))
	assertNotNilF(t, err, "Expected error for conflicting OCSP and CRL configuration")
	assertNilF(t, transport, "Expected nil transport when error occurs")
}

func TestCreateCustomTLSTransportSuccess(t *testing.T) {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	assertNilF(t, RegisterTLSConfig("TestCreateCustomTLSTransportSuccessTlsConfig", tlsConfig))
	// Test successful creation with valid config
	validConfig := &Config{
		DisableOCSPChecks:       true,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
		TLSConfigName:           "TestCreateCustomTLSTransportSuccessTlsConfig",
	}

	factory := newTransportFactory(validConfig, nil)

	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))
	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport for valid configuration")
}

func TestCreateStandardTransportSuccess(t *testing.T) {
	// Test successful creation with valid config
	validConfig := &Config{
		DisableOCSPChecks:       true,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
	}

	factory := newTransportFactory(validConfig, nil)

	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))
	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport for valid configuration")
}

func TestDirectTLSConfigUsage(t *testing.T) {
	// Test the new direct TLS config approach
	customTLS := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "custom.example.com",
	}
	assertNilF(t, RegisterTLSConfig("TestDirectTLSConfigUsageTlsConfig", customTLS))

	config := &Config{
		DisableOCSPChecks:       true,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
		TLSConfigName:           "TestDirectTLSConfigUsageTlsConfig",
	}

	factory := newTransportFactory(config, nil)
	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")
}

func TestRegisteredTLSConfigUsage(t *testing.T) {
	// Test registered TLS config approach through DSN parsing

	// Clean up any existing registry
	sfconfig.ResetTLSConfigRegistry()

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
	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")
}

// TestDisableOCSPChecksPreservesRegisteredTLSConfig is a regression test for
// SNOW-3649867: disabling OCSP revocation checking must NOT discard a
// user-registered TLS config (e.g. certificate pinning). The registered
// config's fields must survive onto the resulting transport's TLSClientConfig
// instead of falling back to Go's default verification (nil TLSClientConfig).
func TestDisableOCSPChecksPreservesRegisteredTLSConfig(t *testing.T) {
	const name = "TestDisableOCSPChecksPreservesRegisteredTLSConfig"
	pinned := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: "pinned.example.com",
	}
	assertNilF(t, RegisterTLSConfig(name, pinned))
	defer func() { assertNilF(t, DeregisterTLSConfig(name)) }()

	config := &Config{
		DisableOCSPChecks:       true,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
		TLSConfigName:           name,
	}
	factory := newTransportFactory(config, nil)
	rt, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))
	assertNilF(t, err)

	transport, ok := rt.(*http.Transport)
	assertTrueF(t, ok, "expected *http.Transport")
	assertNotNilF(t, transport.TLSClientConfig, "registered TLS config must not be dropped when OCSP is disabled")
	assertEqualE(t, transport.TLSClientConfig.MinVersion, uint16(tls.VersionTLS13))
	assertEqualE(t, transport.TLSClientConfig.ServerName, "pinned.example.com")
}

func TestDirectTLSConfigOnly(t *testing.T) {
	// Test that direct TLS config works without any registration

	// Create a direct TLS config
	directTLS := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "direct.example.com",
	}
	assertNilF(t, RegisterTLSConfig("TestDirectTLSConfigOnlyTlsConfig", directTLS))

	config := &Config{
		DisableOCSPChecks:       true,
		CertRevocationCheckMode: CertRevocationCheckDisabled,
		TLSConfigName:           "TestDirectTLSConfigOnlyTlsConfig",
	}

	factory := newTransportFactory(config, nil)
	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")
}

func TestProxyTransportCreation(t *testing.T) {
	proxyTests := []struct {
		config       *Config
		proxyURL     string
		disableProxy bool
	}{
		{
			config: &Config{
				ProxyProtocol: "http",
				ProxyHost:     "proxy.connection.com",
				ProxyPort:     1234,
			},
			disableProxy: true,
			proxyURL:     "",
		},
		{
			config: &Config{
				ProxyProtocol: "https",
				ProxyHost:     "proxy.connection.com",
				ProxyPort:     1234,
			},
			disableProxy: true,
			proxyURL:     "",
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
		{
			// A literal IPv6 proxy host has to be bracketed, otherwise the
			// resulting proxy URL cannot be parsed at all.
			config: &Config{
				ProxyProtocol: "http",
				ProxyHost:     "::1",
				ProxyPort:     1234,
			},
			proxyURL: "http://[::1]:1234",
		},
		{
			config: &Config{
				ProxyProtocol: "https",
				ProxyHost:     "2001:db8::1",
				ProxyPort:     1234,
			},
			proxyURL: "https://[2001:db8::1]:1234",
		},
		{
			// Already bracketed by the user - must not be double-bracketed.
			config: &Config{
				ProxyProtocol: "http",
				ProxyHost:     "[fe80::1]",
				ProxyPort:     1234,
			},
			proxyURL: "http://[fe80::1]:1234",
		},
	}

	for _, test := range proxyTests {
		t.Run(test.proxyURL, func(t *testing.T) {
			factory := newTransportFactory(test.config, nil)
			proxyFunc := factory.createProxy(&transportConfig{DisableProxy: test.disableProxy})

			if test.disableProxy {
				assertNilF(t, proxyFunc, "Expected nil proxy function when proxy is disabled")
				return
			}

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
		})
	}
}

func TestWrapDialContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assertNilF(t, err, "Cannot listen for the test connections")
	defer listener.Close()

	for _, test := range []struct {
		name   string
		config *Config
	}{
		{"OCSP", &Config{DisableOCSPChecks: false, CertRevocationCheckMode: CertRevocationCheckDisabled}},
		{"CRL", &Config{DisableOCSPChecks: true, CertRevocationCheckMode: CertRevocationCheckEnabled}},
		{"NoRevocation", &Config{DisableOCSPChecks: true, CertRevocationCheckMode: CertRevocationCheckDisabled}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var wrapped, dialed bool
			test.config.WrapDialContext = func(dial DialFunc) DialFunc {
				wrapped = true
				return func(ctx context.Context, network, addr string) (net.Conn, error) {
					dialed = true
					return dial(ctx, network, addr)
				}
			}

			factory := newTransportFactory(test.config, nil)

			transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))
			assertNilF(t, err, "Unexpected error")
			assertTrueF(t, wrapped, "Expected the dial function to be wrapped by WrapDialContext")

			conn, err := transport.(*http.Transport).DialContext(context.Background(), "tcp", listener.Addr().String())
			assertNilF(t, err, "Unexpected error")
			defer conn.Close()
			assertTrueF(t, dialed, "Expected the connection to be dialed by the wrapping dial function")
		})
	}
}

func TestWrapDialContextIgnoredWithTransporter(t *testing.T) {
	transporter := &http.Transport{}
	config := &Config{
		DisableOCSPChecks: true,
		Transporter:       transporter,
		WrapDialContext: func(DialFunc) DialFunc {
			t.Fatal("Expected WrapDialContext not to be called when Transporter is set")
			return nil
		},
	}

	factory := newTransportFactory(config, nil)

	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))
	assertNilF(t, err, "Unexpected error")
	assertEqualF(t, transport, http.RoundTripper(transporter), "Expected the transport set as Transporter")
}

func createTestNoRevocationTransport() http.RoundTripper {
	transport, err := newTransportFactory(&Config{}, nil).createNoRevocationTransport(defaultTransportConfigs.forTransportType(transportTypeSnowflake))
	if err != nil {
		panic(fmt.Sprintf("failed to create test transport: %v", err))
	}
	return transport
}
