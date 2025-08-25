package gosnowflake

import (
	"cmp"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"
)

type transportConfigs interface {
	forTransportType(transportType transportType) *transportConfig
}

type transportType int

const (
	transportTypeOAuth transportType = iota
	transportTypeCloudProvider
	transportTypeCRL
	transportTypeSnowflake
	transportTypeWIF
)

var defaultTransportConfigs transportConfigs = newDefaultTransportConfigs()

// transportConfig holds the configuration for creating HTTP transports
type transportConfig struct {
	MaxIdleConns    int
	IdleConnTimeout time.Duration
	DialTimeout     time.Duration
	KeepAlive       time.Duration
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
func (tf *transportFactory) createOCSPTransport(transportConfig *transportConfig) *http.Transport {
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
	return tf.createBaseTransport(transportConfig, tlsConfig)
}

// createNoRevocationTransport creates a transport without certificate revocation checking
func (tf *transportFactory) createNoRevocationTransport(transportConfig *transportConfig) *http.Transport {
	return tf.createBaseTransport(transportConfig, nil)
}

// createCRLValidator creates a CRL validator
func (tf *transportFactory) createCRLValidator() (*crlValidator, error) {
	allowCertificatesWithoutCrlURL := tf.config.CrlAllowCertificatesWithoutCrlURL == ConfigBoolTrue
	client := &http.Client{
		Timeout:   cmp.Or(tf.config.CrlHTTPClientTimeout, defaultCrlHTTPClientTimeout),
		Transport: tf.createNoRevocationTransport(tf.config.transportConfigFor(transportTypeCRL)),
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
func (tf *transportFactory) createTransport(transportConfig *transportConfig) (http.RoundTripper, error) {
	if tf.config == nil {
		// should never happen in production, only in tests
		logger.Warn("createTransport: got nil Config, using default one")
		return tf.createNoRevocationTransport(transportConfig), nil
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

		return tf.createBaseTransport(transportConfig, tlsConfig), nil
	}

	// Handle no revocation checking path
	if tf.config.DisableOCSPChecks || tf.config.InsecureMode {
		logger.Debug("createTransport: skipping OCSP validation")
		return tf.createNoRevocationTransport(transportConfig), nil
	}

	logger.Debug("createTransport: will perform OCSP validation")
	return tf.createOCSPTransport(transportConfig), nil
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

type defaultTransportConfigsType struct {
	oauthTransportConfig         *transportConfig
	cloudProviderTransportConfig *transportConfig
	crlTransportConfig           *transportConfig
	snowflakeTransportConfig     *transportConfig
	wifTransportConfig           *transportConfig
}

func newDefaultTransportConfigs() *defaultTransportConfigsType {
	return &defaultTransportConfigsType{
		oauthTransportConfig: &transportConfig{
			MaxIdleConns:    3,
			IdleConnTimeout: 30 * time.Second,
			DialTimeout:     30 * time.Second,
			KeepAlive:       0,
		},
		cloudProviderTransportConfig: &transportConfig{
			MaxIdleConns:    10,
			IdleConnTimeout: 20 * time.Second,
			DialTimeout:     30 * time.Second,
			KeepAlive:       30 * time.Second,
		},
		crlTransportConfig: &transportConfig{
			MaxIdleConns:    3,
			IdleConnTimeout: 5 * time.Second,
			DialTimeout:     5 * time.Second,
			KeepAlive:       0,
		},
		snowflakeTransportConfig: &transportConfig{
			MaxIdleConns:    10,
			IdleConnTimeout: 60 * time.Second,
			DialTimeout:     30 * time.Second,
			KeepAlive:       30 * time.Second,
		},
		wifTransportConfig: &transportConfig{
			MaxIdleConns:    3,
			IdleConnTimeout: 30 * time.Second,
			DialTimeout:     30 * time.Second,
			KeepAlive:       0,
		},
	}
}

func (dtc *defaultTransportConfigsType) forTransportType(transportType transportType) *transportConfig {
	switch transportType {
	case transportTypeOAuth:
		return dtc.oauthTransportConfig
	case transportTypeCloudProvider:
		return dtc.cloudProviderTransportConfig
	case transportTypeCRL:
		return dtc.crlTransportConfig
	case transportTypeSnowflake:
		return dtc.snowflakeTransportConfig
	case transportTypeWIF:
		return dtc.wifTransportConfig
	}
	panic("unknown transport type: " + strconv.Itoa(int(transportType)))
}
