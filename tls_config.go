package gosnowflake

import (
	"crypto/tls"
	"errors"
	"sync"
)

var (
	tlsConfigLock     sync.RWMutex
	tlsConfigRegistry map[string]*tls.Config
)

func init() {
	tlsConfigRegistry = make(map[string]*tls.Config)
}

// RegisterTLSConfig registers a custom tls.Config to be used with sql.Open.
// Use the key as a value in the DSN where tls=value.
//
// The custom TLS config allows you to specify custom root CAs, client certificates,
// and other TLS settings while maintaining Snowflake's certificate revocation
// checking (OCSP/CRL) unless explicitly disabled.
//
// Certificate Revocation Checking:
//   - OCSP validation is preserved unless DisableOCSPChecks=true or InsecureMode=true
//   - CRL validation is preserved if CertRevocationCheckMode is enabled
//   - If you provide a custom VerifyPeerCertificate callback, it will be chained
//     with Snowflake's revocation checking (your callback runs first)
//
// Note: The provided tls.Config is exclusively owned by the driver after
// registering it.
//
// Example:
//
//	rootCertPool := x509.NewCertPool()
//	pem, err := os.ReadFile("/path/ca-cert.pem")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
//	    log.Fatal("Failed to append PEM.")
//	}
//
//	gosnowflake.RegisterTLSConfig("custom", &tls.Config{
//	    RootCAs: rootCertPool,
//	    // OCSP/CRL validation will be automatically added by the driver
//	})
//
//	db, err := sql.Open("snowflake", "user:pass@account/db?tls=custom")
func RegisterTLSConfig(key string, config *tls.Config) error {
	if config == nil {
		return errors.New("config is nil")
	}

	tlsConfigLock.Lock()
	tlsConfigRegistry[key] = config
	tlsConfigLock.Unlock()
	return nil
}

// DeregisterTLSConfig removes the tls.Config associated with key.
func DeregisterTLSConfig(key string) {
	tlsConfigLock.Lock()
	delete(tlsConfigRegistry, key)
	tlsConfigLock.Unlock()
}

// getTLSConfigClone returns a clone of a TLS config from the registry.
// This prevents the user config from being modified by the driver.
func getTLSConfigClone(key string) (*tls.Config, bool) {
	tlsConfigLock.RLock()
	tlsConfig, ok := tlsConfigRegistry[key]
	tlsConfigLock.RUnlock()

	if !ok {
		return nil, false
	}

	// Clone the config to prevent modification
	return tlsConfig.Clone(), true
}
