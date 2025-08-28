package gosnowflake

import (
	"cmp"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/http/httpproxy"
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
	config    *Config
	telemetry *snowflakeTelemetry
}

// NewTransportFactory creates a new transport factory
func newTransportFactory(config *Config, telemetry *snowflakeTelemetry) *transportFactory {
	return &transportFactory{config: config, telemetry: telemetry}
}

func (tf *transportFactory) createProxy() func(*http.Request) (*url.URL, error) {
	logger.Info("Initializing proxy configuration")
	if tf.config == nil || tf.config.ProxyHost == "" {
		logger.Info("Config is empty or ProxyHost is not set. Using proxy settings from environment variables.")
		return http.ProxyFromEnvironment
	}

	connectionProxy := &url.URL{
		Scheme: tf.config.ProxyProtocol,
		Host:   fmt.Sprintf("%s:%d", tf.config.ProxyHost, tf.config.ProxyPort),
	}
	if tf.config.ProxyUser != "" && tf.config.ProxyPassword != "" {
		connectionProxy.User = url.UserPassword(tf.config.ProxyUser, tf.config.ProxyPassword)
		logger.Infof("Connection Proxy is configured: Connection proxy %s:****@%s NoProxy:", tf.config.ProxyUser, connectionProxy.Host, tf.config.NoProxy)
	} else {
		logger.Infof("Connection Proxy is configured: Connection proxy: %s NoProxy:", connectionProxy.Host, tf.config.NoProxy)
	}

	cfg := httpproxy.Config{
		HTTPSProxy: connectionProxy.String(),
		HTTPProxy:  connectionProxy.String(),
		NoProxy:    tf.config.NoProxy,
	}
	proxyURLFunc := cfg.ProxyFunc()

	return func(req *http.Request) (*url.URL, error) {
		return proxyURLFunc(req.URL)
	}
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
		Proxy:           tf.createProxy(),
		DialContext:     dialer.DialContext,
	}
}

// createOCSPTransport creates a transport with OCSP validation
func (tf *transportFactory) createOCSPTransport() *http.Transport {
	// Chain OCSP verification with custom TLS config
	ov := newOcspValidator(tf.config)
	tlsConfig := tf.config.tlsConfig
	if tlsConfig != nil {
		tlsConfig.VerifyPeerCertificate = tf.chainVerificationCallbacks(tlsConfig.VerifyPeerCertificate, ov.verifyPeerCertificateSerial)
	} else {
		tlsConfig = &tls.Config{
			VerifyPeerCertificate: ov.verifyPeerCertificateSerial,
		}
	}
	return tf.createBaseTransport(defaultTransportConfig(), tlsConfig)
}

// createNoRevocationTransport creates a transport without certificate revocation checking
func (tf *transportFactory) createNoRevocationTransport() http.RoundTripper {
	if tf.config != nil && tf.config.Transporter != nil {
		return tf.config.Transporter
	}
	return tf.createBaseTransport(defaultTransportConfig(), nil)
}

func createTestNoRevocationTransport() http.RoundTripper {
	return newTransportFactory(&Config{}, nil).createNoRevocationTransport()
}

// createCRLValidator creates a CRL validator
func (tf *transportFactory) createCRLValidator() (*crlValidator, error) {
	allowCertificatesWithoutCrlURL := tf.config.CrlAllowCertificatesWithoutCrlURL == ConfigBoolTrue
	client := &http.Client{
		Timeout: cmp.Or(tf.config.CrlHTTPClientTimeout, defaultCrlHTTPClientTimeout),
	}
	return newCrlValidator(
		tf.config.CertRevocationCheckMode,
		allowCertificatesWithoutCrlURL,
		tf.config.CrlInMemoryCacheDisabled,
		tf.config.CrlOnDiskCacheDisabled,
		client,
		tf.telemetry,
	)
}

// createTransport is the main entry point for creating transports
func (tf *transportFactory) createTransport() (http.RoundTripper, error) {
	if tf.config == nil {
		// should never happen in production, only in tests
		logger.Warn("createTransport: got nil Config, using default one")
		return tf.createNoRevocationTransport(), nil
	}

	// if user configured a custom Transporter, prioritize that
	if tf.config.Transporter != nil {
		logger.Debug("createTransport: using Transporter configured by the user")
		return tf.config.Transporter, nil
	}

	// Validate configuration
	if err := tf.validateRevocationConfig(); err != nil {
		return nil, err
	}

	// Handle CRL validation path
	if tf.config.CertRevocationCheckMode != CertRevocationCheckDisabled {
		logger.Debug("createTransport: will perform CRL validation")
		crlValidator, err := tf.createCRLValidator()
		if err != nil {
			return nil, err
		}
		crlCacheCleaner.startPeriodicCacheCleanup()
		// Chain CRL verification with custom TLS config
		tlsConfig := tf.config.tlsConfig
		if tlsConfig != nil {
			crlVerify := crlValidator.verifyPeerCertificates
			tlsConfig.VerifyPeerCertificate = tf.chainVerificationCallbacks(tlsConfig.VerifyPeerCertificate, crlVerify)
		} else {
			tlsConfig = &tls.Config{
				VerifyPeerCertificate: crlValidator.verifyPeerCertificates,
			}
		}

		return tf.createBaseTransport(crlTransportConfig(), tlsConfig), nil
	}

	// Handle no revocation checking path
	if tf.config.DisableOCSPChecks || tf.config.InsecureMode {
		logger.Debug("createTransport: skipping OCSP validation")
		return tf.createNoRevocationTransport(), nil
	}

	logger.Debug("createTransport: will perform OCSP validation")
	return tf.createOCSPTransport(), nil
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
