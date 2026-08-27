package gosnowflake

import (
	"crypto/tls"
	"net/http"
	"testing"
)

func TestMinTLSVersionWithCustomTransporter(t *testing.T) {
	// Set the env var
	t.Setenv("SNOWFLAKE_MIN_TLS_VERSION", "1.3")

	// Create a custom transport with TLS 1.2
	customTransport := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	config := &Config{
		Transporter: customTransport,
	}

	factory := newTransportFactory(config, nil)
	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")

	// Verify the TLS config was updated to TLS 1.3 (ceiling behavior)
	httpTransport, ok := transport.(*http.Transport)
	assertTrueF(t, ok, "Expected *http.Transport")
	assertEqualF(t, httpTransport.TLSClientConfig.MinVersion, uint16(tls.VersionTLS13), "Expected TLS 1.3")
}

func TestMinTLSVersionWithCustomTransporterNoEnvVar(t *testing.T) {
	// No env var set
	t.Setenv("SNOWFLAKE_MIN_TLS_VERSION", "")

	// Create a custom transport with TLS 1.2
	customTransport := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	config := &Config{
		Transporter: customTransport,
	}

	factory := newTransportFactory(config, nil)
	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")

	// Verify the TLS config was NOT changed
	httpTransport, ok := transport.(*http.Transport)
	assertTrueF(t, ok, "Expected *http.Transport")
	assertEqualF(t, httpTransport.TLSClientConfig.MinVersion, uint16(tls.VersionTLS12), "Expected TLS 1.2")
}

func TestMinTLSVersionCeilingBehavior(t *testing.T) {
	// Set env var to TLS 1.2
	t.Setenv("SNOWFLAKE_MIN_TLS_VERSION", "1.2")

	// Create a custom transport with TLS 1.3 (higher than env var)
	customTransport := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS13,
		},
	}

	config := &Config{
		Transporter: customTransport,
	}

	factory := newTransportFactory(config, nil)
	transport, err := factory.createTransport(transportConfigFor(transportTypeSnowflake))

	assertNilF(t, err, "Unexpected error")
	assertNotNilF(t, transport, "Expected non-nil transport")

	// Verify the TLS config kept TLS 1.3 (ceiling behavior - keep the higher value)
	httpTransport, ok := transport.(*http.Transport)
	assertTrueF(t, ok, "Expected *http.Transport")
	assertEqualF(t, httpTransport.TLSClientConfig.MinVersion, uint16(tls.VersionTLS13), "Expected TLS 1.3 (kept higher value)")
}
