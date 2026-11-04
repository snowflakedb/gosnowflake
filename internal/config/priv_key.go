package config

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"fmt"

	sferrors "github.com/snowflakedb/gosnowflake/v2/internal/errors"
)

// ParsePKCS8Signer parses a PKCS8 encoded private key that can be used for keypair authentication:
// an RSA key, or an ECDSA key on P-256, P-384 or P-521.
func ParsePKCS8Signer(block []byte) (crypto.Signer, error) {
	privKey, err := x509.ParsePKCS8PrivateKey(block)
	if err != nil {
		return nil, &sferrors.SnowflakeError{
			Number:  sferrors.ErrCodePrivateKeyParseError,
			Message: "Error decoding private key using PKCS8.",
		}
	}
	return keyPairSigner(privKey)
}

// keyPairSigner checks that a key parsed by x509.ParsePKCS8PrivateKey can be used for keypair authentication.
func keyPairSigner(privKey any) (crypto.Signer, error) {
	switch key := privKey.(type) {
	case *rsa.PrivateKey:
		return key, nil
	case *ecdsa.PrivateKey:
		if !IsSupportedKeyPairCurve(key.Curve) {
			return nil, &sferrors.SnowflakeError{
				Number:  sferrors.ErrCodePrivateKeyParseError,
				Message: fmt.Sprintf("Unsupported elliptic curve %v. Supported curves: P-256, P-384, P-521.", key.Curve.Params().Name),
			}
		}
		return key, nil
	default:
		return nil, &sferrors.SnowflakeError{
			Number:  sferrors.ErrCodePrivateKeyParseError,
			Message: fmt.Sprintf("Unsupported private key type %T. Use an RSA or ECDSA key.", privKey),
		}
	}
}

// IsSupportedKeyPairCurve reports whether Snowflake keypair authentication supports the curve.
func IsSupportedKeyPairCurve(curve elliptic.Curve) bool {
	return curve == elliptic.P256() || curve == elliptic.P384() || curve == elliptic.P521()
}

// setPrivateKey stores a parsed key in PrivateKey (RSA) or PrivateKeySigner (anything else),
// clearing the other field so that only one of them is set.
func setPrivateKey(cfg *Config, signer crypto.Signer) {
	if rsaKey, ok := signer.(*rsa.PrivateKey); ok {
		cfg.PrivateKey = rsaKey
		cfg.PrivateKeySigner = nil
		return
	}
	cfg.PrivateKey = nil
	cfg.PrivateKeySigner = signer
}

// MarshalPKCS8PrivateKey marshals a private key to PKCS8 format.
func MarshalPKCS8PrivateKey(key any) ([]byte, error) {
	keyInBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, &sferrors.SnowflakeError{
			Number:  sferrors.ErrCodePrivateKeyParseError,
			Message: "Error encoding private key using PKCS8."}
	}
	return keyInBytes, nil
}
