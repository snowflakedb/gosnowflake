package gosnowflake

import (
	"crypto/tls"
	"sync"
)

var (
	tlsConfigLock     sync.RWMutex
	tlsConfigRegistry = make(map[string]*tls.Config)
)

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
// TODO(snoonan): Logging
func RegisterTLSConfig(key string, config *tls.Config) error {
	tlsConfigLock.Lock()
	tlsConfigRegistry[key] = config.Clone()
	tlsConfigLock.Unlock()
	return nil
}

// DeregisterTLSConfig removes the tls.Config associated with key.
func DeregisterTLSConfig(key string) error {
	tlsConfigLock.Lock()
	delete(tlsConfigRegistry, key)
	tlsConfigLock.Unlock()
	return nil
}

// getTLSConfig returns a TLS config from the registry.
func getTLSConfig(key string) (*tls.Config, bool) {
	tlsConfigLock.RLock()
	tlsConfig, ok := tlsConfigRegistry[key]
	tlsConfigLock.RUnlock()
	if !ok {
		return nil, false
	}
	// Clone to prevent modification and to handle the internal mutex properly.
	return tlsConfig.Clone(), true
}
