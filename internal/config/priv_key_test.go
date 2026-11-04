package config

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	sferrors "github.com/snowflakedb/gosnowflake/v2/internal/errors"
)

var supportedTestCurves = []elliptic.Curve{elliptic.P256(), elliptic.P384(), elliptic.P521()}

func generateECKey(t *testing.T, curve elliptic.Curve) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	assertNilF(t, err)
	return key
}

func pkcs8(t *testing.T, key any) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	assertNilF(t, err)
	return der
}

func assertPrivateKeyParseError(t *testing.T, err error) {
	t.Helper()
	var sfErr *sferrors.SnowflakeError
	if !errors.As(err, &sfErr) || sfErr.Number != sferrors.ErrCodePrivateKeyParseError {
		t.Fatalf("expected ErrCodePrivateKeyParseError, got %v", err)
	}
}

func TestParsePKCS8SignerECDSA(t *testing.T) {
	for _, curve := range supportedTestCurves {
		t.Run(curve.Params().Name, func(t *testing.T) {
			key := generateECKey(t, curve)
			signer, err := ParsePKCS8Signer(pkcs8(t, key))
			assertNilF(t, err)
			ecKey, ok := signer.(*ecdsa.PrivateKey)
			assertTrueF(t, ok, fmt.Sprintf("expected *ecdsa.PrivateKey, got %T", signer))
			assertTrueF(t, ecKey.Equal(key), "parsed key does not equal the generated key")
		})
	}
}

func TestParsePKCS8SignerRSA(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	assertNilF(t, err)
	signer, err := ParsePKCS8Signer(pkcs8(t, key))
	assertNilF(t, err)
	rsaKey, ok := signer.(*rsa.PrivateKey)
	assertTrueF(t, ok, fmt.Sprintf("expected *rsa.PrivateKey, got %T", signer))
	assertTrueF(t, rsaKey.Equal(key), "parsed key does not equal the generated key")
}

func TestParsePKCS8SignerRejectsUnsupportedKeys(t *testing.T) {
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	assertNilF(t, err)
	p224Key := generateECKey(t, elliptic.P224())

	for name, der := range map[string][]byte{
		"ed25519": pkcs8(t, edKey),
		"P-224":   pkcs8(t, p224Key),
		"garbage": []byte("not a key"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParsePKCS8Signer(der)
			assertPrivateKeyParseError(t, err)
		})
	}
}

// Before EC support, any non-RSA PKCS8 key in the DSN hit an unchecked type assertion and panicked.
func TestParseDSNWithUnsupportedPrivateKeyReturnsError(t *testing.T) {
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	assertNilF(t, err)
	dsn := fmt.Sprintf("u@a.snowflake.local:9876?account=a&authenticator=SNOWFLAKE_JWT&privateKey=%v",
		base64.URLEncoding.EncodeToString(pkcs8(t, edKey)))
	_, err = ParseDSN(dsn)
	assertPrivateKeyParseError(t, err)
}

func TestParseDSNWithECPrivateKey(t *testing.T) {
	for _, curve := range supportedTestCurves {
		t.Run(curve.Params().Name, func(t *testing.T) {
			key := generateECKey(t, curve)
			dsn := fmt.Sprintf("u@a.snowflake.local:9876?account=a&protocol=http&authenticator=SNOWFLAKE_JWT&privateKey=%v",
				base64.URLEncoding.EncodeToString(pkcs8(t, key)))
			cfg, err := ParseDSN(dsn)
			assertNilF(t, err)
			assertNilF(t, cfg.PrivateKey, "PrivateKey should stay empty for an EC key")
			ecKey, ok := cfg.PrivateKeySigner.(*ecdsa.PrivateKey)
			assertTrueF(t, ok, fmt.Sprintf("expected *ecdsa.PrivateKey in PrivateKeySigner, got %T", cfg.PrivateKeySigner))
			assertTrueF(t, ecKey.Equal(key), "parsed key does not equal the generated key")
		})
	}
}

func TestDSNRoundTripWithECPrivateKeySigner(t *testing.T) {
	for _, curve := range supportedTestCurves {
		t.Run(curve.Params().Name, func(t *testing.T) {
			key := generateECKey(t, curve)
			dsn, err := DSN(&Config{Account: "a", User: "u", Authenticator: AuthTypeJwt, PrivateKeySigner: key})
			assertNilF(t, err)
			cfg, err := ParseDSN(dsn)
			assertNilF(t, err)
			ecKey, ok := cfg.PrivateKeySigner.(*ecdsa.PrivateKey)
			assertTrueF(t, ok, fmt.Sprintf("expected *ecdsa.PrivateKey in PrivateKeySigner, got %T", cfg.PrivateKeySigner))
			assertTrueF(t, ecKey.Equal(key), "round-tripped key does not equal the original key")
			assertEqualE(t, cfg.Authenticator, AuthTypeJwt)
		})
	}
}

func TestDSNRoundTripWithRSAPrivateKeySigner(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	assertNilF(t, err)
	dsn, err := DSN(&Config{Account: "a", User: "u", Authenticator: AuthTypeJwt, PrivateKeySigner: key})
	assertNilF(t, err)
	cfg, err := ParseDSN(dsn)
	assertNilF(t, err)
	// an RSA key read back from a DSN lands in PrivateKey, as it always has
	assertNilF(t, cfg.PrivateKeySigner)
	assertTrueF(t, cfg.PrivateKey != nil && cfg.PrivateKey.Equal(key), "round-tripped key does not equal the original key")
}

type opaqueSigner struct {
	crypto.Signer
}

func TestDSNRejectsUnsupportedPrivateKeyConfig(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assertNilF(t, err)
	ecKey := generateECKey(t, elliptic.P256())

	_, err = DSN(&Config{Account: "a", User: "u", Authenticator: AuthTypeJwt, PrivateKey: rsaKey, PrivateKeySigner: ecKey})
	assertNotNilF(t, err, "setting both PrivateKey and PrivateKeySigner should fail")
	assertTrueF(t, strings.Contains(err.Error(), "only one of PrivateKey and PrivateKeySigner"), err.Error())

	_, err = DSN(&Config{Account: "a", User: "u", Authenticator: AuthTypeJwt, PrivateKeySigner: opaqueSigner{ecKey}})
	assertNotNilF(t, err, "a signer without an exportable key cannot be put in a DSN")
	assertTrueF(t, strings.Contains(err.Error(), "cannot be encoded in a DSN"), err.Error())
}

func writeECKeyFile(t *testing.T, key *ecdsa.PrivateKey) string {
	t.Helper()
	pemData := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8(t, key)})
	return createTmpFile(t, "ecKey.p8", pemData)
}

func TestParsePrivateKeyFromFileEC(t *testing.T) {
	key := generateECKey(t, elliptic.P384())
	keyFile := writeECKeyFile(t, key)
	defer os.Remove(keyFile)

	signer, err := parsePrivateKeyFromFile(keyFile)
	assertNilF(t, err)
	ecKey, ok := signer.(*ecdsa.PrivateKey)
	assertTrueF(t, ok && ecKey.Equal(key), "parsed key does not equal the generated key")
}

func TestParseTomlECPrivateKey(t *testing.T) {
	key := generateECKey(t, elliptic.P256())
	keyFile := writeECKeyFile(t, key)
	defer os.Remove(keyFile)

	for name, value := range map[string]string{
		"private_key":      base64.URLEncoding.EncodeToString(pkcs8(t, key)),
		"private_key_file": keyFile,
		"private_key_path": keyFile,
	} {
		t.Run(name, func(t *testing.T) {
			cfg := &Config{}
			assertNilF(t, ParseToml(cfg, map[string]any{name: value}))
			assertNilF(t, cfg.PrivateKey)
			ecKey, ok := cfg.PrivateKeySigner.(*ecdsa.PrivateKey)
			assertTrueF(t, ok && ecKey.Equal(key), "parsed key does not equal the generated key")
		})
	}
}

func TestGetConfigFromEnvECPrivateKey(t *testing.T) {
	key := generateECKey(t, elliptic.P521())
	keyFile := writeECKeyFile(t, key)
	defer os.Remove(keyFile)
	t.Setenv("SF_TEST_EC_PRIVATE_KEY_PATH", keyFile)

	cfg, err := GetConfigFromEnv([]*Param{{Name: "PrivateKey", EnvName: "SF_TEST_EC_PRIVATE_KEY_PATH", FailOnMissing: true}})
	assertNilF(t, err)
	assertNilF(t, cfg.PrivateKey)
	ecKey, ok := cfg.PrivateKeySigner.(*ecdsa.PrivateKey)
	assertTrueF(t, ok && ecKey.Equal(key), "parsed key does not equal the generated key")
}
