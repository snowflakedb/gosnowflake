package gosnowflake

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// transportConfig holds the configuration for creating HTTP transports
type transportConfig struct {
	MaxIdleConns    int
	IdleConnTimeout time.Duration
	DialTimeout     time.Duration
	KeepAlive       time.Duration
}

// defaultTransportConfig returns the standard transport configuration
func defaultTransportConfig() *transportConfig {
	return &transportConfig{
		MaxIdleConns:    10,
		IdleConnTimeout: 30 * time.Minute,
		DialTimeout:     30 * time.Second,
		KeepAlive:       30 * time.Second,
	}
}

// crlTransportConfig returns the transport configuration for CRL validation
// Uses more conservative timeouts for CRL operations
func crlTransportConfig() *transportConfig {
	return &transportConfig{
		MaxIdleConns:    5,
		IdleConnTimeout: 5 * time.Minute,
		DialTimeout:     5 * time.Second,
		KeepAlive:       0, // No keep-alive for CRL operations
	}
}

// TransportFactory handles creation of HTTP transports with different validation modes
type transportFactory struct {
	config *Config
}

// NewTransportFactory creates a new transport factory
func newTransportFactory(config *Config) *transportFactory {
	return &transportFactory{config: config}
}

// createBaseTransport creates a base HTTP transport with the given configuration
func (tf *transportFactory) createBaseTransport(transportConfig *transportConfig, tlsConfig *tls.Config) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   transportConfig.DialTimeout,
		KeepAlive: transportConfig.KeepAlive,
	}

	return &http.Transport{
		TLSClientConfig: tlsConfig,
		MaxIdleConns:    transportConfig.MaxIdleConns,
		IdleConnTimeout: transportConfig.IdleConnTimeout,
		Proxy:           http.ProxyFromEnvironment,
		DialContext:     dialer.DialContext,
	}
}

// createOCSPTransport creates a transport with OCSP validation
func (tf *transportFactory) createOCSPTransport() *http.Transport {
	tlsConfig := &tls.Config{
		RootCAs:               certPool,
		VerifyPeerCertificate: verifyPeerCertificateSerial,
	}
	return tf.createBaseTransport(defaultTransportConfig(), tlsConfig)
}

// createNoRevocationTransport creates a transport without certificate revocation checking
func (tf *transportFactory) createNoRevocationTransport() *http.Transport {
	return tf.createBaseTransport(defaultTransportConfig(), nil)
}

// createCRLTransport creates a transport with CRL validation
func (tf *transportFactory) createCRLValidator() (*crlValidator, error) {
	allowCertificatesWithoutCrlURL := tf.config.CrlAllowCertificatesWithoutCrlURL == ConfigBoolTrue
	client := &http.Client{
		Timeout: getConfigDuration(tf.config.CrlHTTPClientTimeout, defaultCrlHTTPClientTimeout),
	}

	crlValidator, err := newCrlValidator(
		tf.config.CertRevocationCheckMode,
		allowCertificatesWithoutCrlURL,
		tf.config.CrlCacheValidityTime,
		tf.config.CrlInMemoryCacheDisabled,
		tf.config.CrlOnDiskCacheDisabled,
		tf.config.CrlOnDiskCacheDir,
		tf.config.CrlOnDiskCacheRemovalDelay,
		client,
	)
	if err != nil {
		return nil, err
	}

	return crlValidator, nil
}

// createCustomTLSTransport creates a transport with custom TLS configuration and returns the validator
func (tf *transportFactory) createCustomTLSTransport(customTLSConfig *tls.Config) (http.RoundTripper, *crlValidator, error) {
	// Handle CRL validation path
	if tf.config.CertRevocationCheckMode != CertRevocationCheckDisabled {
		cv, err := tf.createCRLValidator()
		if err != nil {
			return nil, nil, err
		}

		// Chain CRL verification with custom TLS config
		crlVerify := cv.verifyPeerCertificates
		customTLSConfig.VerifyPeerCertificate = tf.chainVerificationCallbacks(customTLSConfig.VerifyPeerCertificate, crlVerify)

		// Create transport with custom TLS config and CRL transport settings
		transport := tf.createBaseTransport(defaultTransportConfig(), customTLSConfig)
		return transport, cv, nil
	}

	// Handle OCSP validation path
	if !tf.config.DisableOCSPChecks && !tf.config.InsecureMode {
		customTLSConfig.VerifyPeerCertificate = tf.chainVerificationCallbacks(customTLSConfig.VerifyPeerCertificate, verifyPeerCertificateSerial)
	}

	return tf.createBaseTransport(defaultTransportConfig(), customTLSConfig), nil, nil
}

// createTransport is the main entry point for creating transports
func (tf *transportFactory) createTransport() (http.RoundTripper, *crlValidator, error) {
	if tf.config.Transporter != nil {
		return tf.config.Transporter, nil, nil
	}

	// Validate configuration
	if err := tf.validateRevocationConfig(); err != nil {
		return nil, nil, err
	}

	// Handle custom TLS configuration path
	if tf.config.TLSConfig != nil {
		customTLSConfig := tf.config.TLSConfig
		return tf.createCustomTLSTransport(customTLSConfig)
	}

	// Handle CRL validation path
	if tf.config.CertRevocationCheckMode != CertRevocationCheckDisabled {
		crlValidator, err := tf.createCRLValidator()
		if err != nil {
			return nil, nil, err
		}
		tlsConfig := &tls.Config{
			VerifyPeerCertificate: crlValidator.verifyPeerCertificates,
		}
		return tf.createBaseTransport(crlTransportConfig(), tlsConfig), crlValidator, nil
	}

	// Handle no revocation checking path
	if tf.config.DisableOCSPChecks || tf.config.InsecureMode {
		return tf.createNoRevocationTransport(), nil, nil
	}

	// Default OCSP path - set OCSP fail open mode
	ocspResponseCacheLock.Lock()
	atomic.StoreUint32((*uint32)(&ocspFailOpen), uint32(tf.config.OCSPFailOpen))
	ocspResponseCacheLock.Unlock()
	return tf.createOCSPTransport(), nil, nil
}

// validateRevocationConfig checks for conflicting revocation settings
func (tf *transportFactory) validateRevocationConfig() error {
	if !tf.config.DisableOCSPChecks && !tf.config.InsecureMode && tf.config.CertRevocationCheckMode != CertRevocationCheckDisabled {
		return errors.New("both OCSP and CRL cannot be enabled at the same time, please disable one of them")
	}
	return nil
}

// chainVerificationCallbacks chains a user's custom verification with the provided verification function
func (tf *transportFactory) chainVerificationCallbacks(orignalVerificationFunc func([][]byte, [][]*x509.Certificate) error, verificationFunc func([][]byte, [][]*x509.Certificate) error) func([][]byte, [][]*x509.Certificate) error {
	if orignalVerificationFunc == nil {
		return verificationFunc
	}

	// Chain the existing verification with the new one
	newVerify := func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		// Run the user's custom verification first
		if err := orignalVerificationFunc(rawCerts, verifiedChains); err != nil {
			return err
		}
		// Then run the provided verification
		return verificationFunc(rawCerts, verifiedChains)
	}
	return newVerify
}

// getConfigDuration returns the config duration if non-zero, otherwise returns the default
func getConfigDuration(configValue, defaultValue time.Duration) time.Duration {
	if configValue != 0 {
		return configValue
	}
	return defaultValue
}
