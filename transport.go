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

// TransportConfig holds the configuration for creating HTTP transports
type TransportConfig struct {
	MaxIdleConns    int
	IdleConnTimeout time.Duration
	DialTimeout     time.Duration
	KeepAlive       time.Duration
}

// DefaultTransportConfig returns the standard transport configuration
func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		MaxIdleConns:    10,
		IdleConnTimeout: 30 * time.Minute,
		DialTimeout:     30 * time.Second,
		KeepAlive:       30 * time.Second,
	}
}

// CRLTransportConfig returns the transport configuration for CRL validation
// Uses more conservative timeouts for CRL operations
func CRLTransportConfig() TransportConfig {
	return TransportConfig{
		MaxIdleConns:    5,
		IdleConnTimeout: 5 * time.Minute,
		DialTimeout:     5 * time.Second,
		KeepAlive:       0, // No keep-alive for CRL operations
	}
}

// TransportFactory handles creation of HTTP transports with different validation modes
type TransportFactory struct {
	config *Config
}

// NewTransportFactory creates a new transport factory
func NewTransportFactory(config *Config) *TransportFactory {
	return &TransportFactory{config: config}
}

// createBaseTransport creates a base HTTP transport with the given configuration
func (tf *TransportFactory) createBaseTransport(transportConfig TransportConfig, tlsConfig *tls.Config) *http.Transport {
	dialer := &net.Dialer{
		Timeout: transportConfig.DialTimeout,
	}
	if transportConfig.KeepAlive > 0 {
		dialer.KeepAlive = transportConfig.KeepAlive
	}

	return &http.Transport{
		TLSClientConfig: tlsConfig,
		MaxIdleConns:    transportConfig.MaxIdleConns,
		IdleConnTimeout: transportConfig.IdleConnTimeout,
		Proxy:           http.ProxyFromEnvironment,
		DialContext:     dialer.DialContext,
	}
}

// CreateOCSPTransport creates a transport with OCSP validation
func (tf *TransportFactory) CreateOCSPTransport() *http.Transport {
	tlsConfig := &tls.Config{
		RootCAs:               certPool,
		VerifyPeerCertificate: verifyPeerCertificateSerial,
	}
	return tf.createBaseTransport(DefaultTransportConfig(), tlsConfig)
}

// CreateNoRevocationTransport creates a transport without certificate revocation checking
func (tf *TransportFactory) CreateNoRevocationTransport() *http.Transport {
	return tf.createBaseTransport(DefaultTransportConfig(), nil)
}

// CreateCRLTransport creates a transport with CRL validation
func (tf *TransportFactory) CreateCRLTransport() (*http.Transport, error) {
	transport, _, err := tf.createCRLTransportInternal()
	return transport, err
}

// createCRLTransportInternal creates a transport with CRL validation and returns the validator
func (tf *TransportFactory) createCRLTransportInternal() (*http.Transport, *crlValidator, error) {
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
		return nil, nil, err
	}

	tlsConfig := &tls.Config{
		VerifyPeerCertificate: crlValidator.verifyPeerCertificates,
	}

	transport := tf.createBaseTransport(CRLTransportConfig(), tlsConfig)
	return transport, crlValidator, nil
}

// CreateCustomTLSTransport creates a transport with custom TLS configuration
func (tf *TransportFactory) CreateCustomTLSTransport(customTLSConfig *tls.Config) http.RoundTripper {
	transport, _, _ := tf.createCustomTLSTransportInternal(customTLSConfig)
	return transport
}

// createCustomTLSTransportInternal creates a transport with custom TLS configuration and returns the validator
func (tf *TransportFactory) createCustomTLSTransportInternal(customTLSConfig *tls.Config) (http.RoundTripper, *crlValidator, error) {
	// Validate configuration
	if err := tf.validateRevocationConfig(); err != nil {
		return nil, nil, err
	}

	// Handle CRL validation path
	if tf.config.CertRevocationCheckMode != CertRevocationCheckDisabled {
		return tf.createCustomTLSWithCRL(customTLSConfig)
	}

	// Handle OCSP validation path
	if !tf.config.DisableOCSPChecks && !tf.config.InsecureMode {
		return tf.createCustomTLSWithOCSP(customTLSConfig), nil, nil
	}

	// No revocation checking - just use the custom TLS config
	return tf.createBaseTransport(DefaultTransportConfig(), customTLSConfig), nil, nil
}

// createCustomTLSWithCRL creates a transport combining custom TLS config with CRL validation
func (tf *TransportFactory) createCustomTLSWithCRL(customTLSConfig *tls.Config) (http.RoundTripper, *crlValidator, error) {
	// Create CRL validator
	_, cv, err := tf.createCRLTransportInternal()
	if err != nil {
		return nil, nil, err
	}

	// Chain CRL verification with custom TLS config
	crlVerify := cv.verifyPeerCertificates
	tf.chainVerificationCallbacks(customTLSConfig, crlVerify)

	// Create transport with custom TLS config and CRL transport settings
	transport := tf.createBaseTransport(CRLTransportConfig(), customTLSConfig)
	return transport, cv, nil
}

// createCustomTLSWithOCSP creates a transport combining custom TLS config with OCSP validation
func (tf *TransportFactory) createCustomTLSWithOCSP(customTLSConfig *tls.Config) http.RoundTripper {
	// Chain OCSP verification with custom TLS config
	tf.chainVerificationCallbacks(customTLSConfig, verifyPeerCertificateSerial)
	return tf.createBaseTransport(DefaultTransportConfig(), customTLSConfig)
}

// CreateStandardTransport creates a transport without custom TLS configuration
func (tf *TransportFactory) CreateStandardTransport() http.RoundTripper {
	transport, _, _ := tf.createStandardTransportInternal()
	return transport
}

// createStandardTransportInternal creates a transport without custom TLS configuration and returns the validator
func (tf *TransportFactory) createStandardTransportInternal() (http.RoundTripper, *crlValidator, error) {
	// Validate configuration
	if err := tf.validateRevocationConfig(); err != nil {
		return nil, nil, err
	}

	// Handle CRL validation path
	if tf.config.CertRevocationCheckMode != CertRevocationCheckDisabled {
		return tf.createCRLTransportInternal()
	}

	// Handle no revocation checking path
	if tf.config.DisableOCSPChecks || tf.config.InsecureMode {
		return tf.CreateNoRevocationTransport(), nil, nil
	}

	// Default OCSP path - set OCSP fail open mode
	ocspResponseCacheLock.Lock()
	atomic.StoreUint32((*uint32)(&ocspFailOpen), uint32(tf.config.OCSPFailOpen))
	ocspResponseCacheLock.Unlock()
	return tf.CreateOCSPTransport(), nil, nil
}

// createTransport is the main entry point for creating transports (internal use)
func (tf *TransportFactory) createTransport() (http.RoundTripper, *crlValidator, error) {
	// Early return: Use custom transporter if provided
	if tf.config.Transporter != nil {
		return tf.config.Transporter, nil, nil
	}

	// Handle custom TLS configuration path
	if tf.config.TLSConfig != "" {
		customTLSConfig, ok := getTLSConfigClone(tf.config.TLSConfig)
		if !ok {
			return nil, nil, errors.New("TLS config not found: " + tf.config.TLSConfig)
		}
		return tf.createCustomTLSTransportInternal(customTLSConfig)
	}

	// Handle standard transport configuration
	return tf.createStandardTransportInternal()
}

// CreateFileTransferTransport creates a transport for file transfer operations
// This replaces the getTransport function
func (tf *TransportFactory) CreateFileTransferTransport() (http.RoundTripper, error) {
	if tf.config == nil {
		// should never happen in production, only in tests
		logger.Warn("CreateFileTransferTransport: got nil Config, using default one")
		return tf.CreateNoRevocationTransport(), nil
	}

	// if user configured a custom Transporter, prioritize that
	if tf.config.Transporter != nil {
		logger.Debug("CreateFileTransferTransport: using Transporter configured by the user")
		return tf.config.Transporter, nil
	}

	if tf.config.CertRevocationCheckMode != CertRevocationCheckDisabled {
		transport, err := tf.CreateCRLTransport()
		if err != nil {
			return nil, err
		}
		return transport, nil
	}

	if tf.config.DisableOCSPChecks || tf.config.InsecureMode {
		logger.Debug("CreateFileTransferTransport: skipping OCSP validation for cloud storage")
		return tf.CreateNoRevocationTransport(), nil
	}

	logger.Debug("CreateFileTransferTransport: will perform OCSP validation for cloud storage")
	return tf.CreateOCSPTransport(), nil
}

// validateRevocationConfig checks for conflicting revocation settings
func (tf *TransportFactory) validateRevocationConfig() error {
	if !tf.config.DisableOCSPChecks && !tf.config.InsecureMode && tf.config.CertRevocationCheckMode != CertRevocationCheckDisabled {
		return errors.New("both OCSP and CRL cannot be enabled at the same time, please disable one of them")
	}
	return nil
}

// chainVerificationCallbacks chains a user's custom verification with the provided verification function
func (tf *TransportFactory) chainVerificationCallbacks(customTLSConfig *tls.Config, verificationFunc func([][]byte, [][]*x509.Certificate) error) {
	if customTLSConfig.VerifyPeerCertificate == nil {
		customTLSConfig.VerifyPeerCertificate = verificationFunc
		return
	}

	// Chain the existing verification with the new one
	originalVerify := customTLSConfig.VerifyPeerCertificate
	customTLSConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		// Run the user's custom verification first
		if err := originalVerify(rawCerts, verifiedChains); err != nil {
			return err
		}
		// Then run the provided verification
		return verificationFunc(rawCerts, verifiedChains)
	}
}

// getConfigDuration returns the config duration if non-zero, otherwise returns the default
func getConfigDuration(configValue, defaultValue time.Duration) time.Duration {
	if configValue != 0 {
		return configValue
	}
	return defaultValue
}
