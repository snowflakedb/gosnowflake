package gosnowflake

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	sfconfig "github.com/snowflakedb/gosnowflake/v2/internal/config"
)

// opaqueTestSigner hides the concrete key type, the way a KMS- or HSM-backed crypto.Signer would.
type opaqueTestSigner struct {
	crypto.Signer
}

type keyPairTestCase struct {
	name   string
	signer crypto.Signer
	alg    string
}

func keyPairTestCases(t *testing.T) []keyPairTestCase {
	t.Helper()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assertNilF(t, err)
	cases := []keyPairTestCase{
		{name: "RSA", signer: rsaKey, alg: "RS256"},
		{name: "RSA opaque signer", signer: opaqueTestSigner{rsaKey}, alg: "RS256"},
	}
	for _, c := range []struct {
		curve elliptic.Curve
		alg   string
	}{{elliptic.P256(), "ES256"}, {elliptic.P384(), "ES384"}, {elliptic.P521(), "ES512"}} {
		key, err := ecdsa.GenerateKey(c.curve, rand.Reader)
		assertNilF(t, err)
		name := c.curve.Params().Name
		cases = append(cases,
			keyPairTestCase{name: name, signer: key, alg: c.alg},
			keyPairTestCase{name: name + " opaque signer", signer: opaqueTestSigner{key}, alg: c.alg})
	}
	return cases
}

// verifyKeyPairJWT checks the token with golang-jwt's standard verification and the expected
// algorithm, and returns its claims.
func verifyKeyPairJWT(t *testing.T, tokenString string, pub crypto.PublicKey, alg string) jwt.MapClaims {
	t.Helper()
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (any, error) {
		return pub, nil
	}, jwt.WithValidMethods([]string{alg}))
	assertNilF(t, err, "token should verify with the public key")
	assertEqualE(t, token.Header["alg"], alg)
	return claims
}

func expectedIssuer(t *testing.T, pub crypto.PublicKey) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	assertNilF(t, err)
	hash := sha256.Sum256(der)
	return "ACCOUNT.USER.SHA256:" + base64.StdEncoding.EncodeToString(hash[:])
}

func TestPrepareJWTTokenWithPrivateKeySigner(t *testing.T) {
	for _, tc := range keyPairTestCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Account: "account", User: "user", PrivateKeySigner: tc.signer, JWTExpireTimeout: time.Minute}
			tokenString, err := prepareJWTToken(cfg)
			assertNilF(t, err)

			claims := verifyKeyPairJWT(t, tokenString, tc.signer.Public(), tc.alg)
			assertEqualE(t, claims["iss"], expectedIssuer(t, tc.signer.Public()))
			assertEqualE(t, claims["sub"], "ACCOUNT.USER")
		})
	}
}

// The existing PrivateKey field must keep producing RS256 tokens exactly as before.
func TestPrepareJWTTokenWithRSAPrivateKeyIsUnchanged(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assertNilF(t, err)
	tokenString, err := prepareJWTToken(&Config{Account: "account", User: "user", PrivateKey: rsaKey, JWTExpireTimeout: time.Minute})
	assertNilF(t, err)
	claims := verifyKeyPairJWT(t, tokenString, rsaKey.Public(), "RS256")
	assertEqualE(t, claims["iss"], expectedIssuer(t, rsaKey.Public()))

	// RSA PKCS #1 v1.5 signatures are deterministic, so the crypto.Signer path must produce the
	// same signature as golang-jwt's RS256 implementation for the same signing input.
	signingString := "header.payload"
	viaJWT, err := jwt.SigningMethodRS256.Sign(signingString, rsaKey)
	assertNilF(t, err)
	viaSigner, err := (&cryptoSignerSigningMethod{alg: "RS256", hash: crypto.SHA256}).Sign(signingString, opaqueTestSigner{rsaKey})
	assertNilF(t, err)
	assertBytesEqualE(t, viaSigner, viaJWT)
}

func TestPrepareJWTTokenRejectsInvalidKeyConfig(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assertNilF(t, err)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assertNilF(t, err)
	p224Key, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	assertNilF(t, err)
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	assertNilF(t, err)

	for name, tc := range map[string]struct {
		cfg     *Config
		message string
	}{
		"no key":          {&Config{}, "PrivateKey was not provided"},
		"both keys":       {&Config{PrivateKey: rsaKey, PrivateKeySigner: ecKey}, "both PrivateKey and PrivateKeySigner"},
		"P-224":           {&Config{PrivateKeySigner: p224Key}, "unsupported elliptic curve P-224"},
		"P-224 opaque":    {&Config{PrivateKeySigner: opaqueTestSigner{p224Key}}, "unsupported elliptic curve P-224"},
		"ed25519":         {&Config{PrivateKeySigner: edKey}, "unsupported public key type ed25519.PublicKey"},
		"ed25519 opaque":  {&Config{PrivateKeySigner: opaqueTestSigner{edKey}}, "unsupported public key type ed25519.PublicKey"},
		"signer failure":  {&Config{PrivateKeySigner: failingTestSigner{ecKey}}, "signer unavailable"},
		"bad ECDSA sig":   {&Config{PrivateKeySigner: fixedSignatureSigner{ecKey, []byte{0x30, 0x00}}}, "invalid ECDSA signature"},
		"oversized ECDSA": {&Config{PrivateKeySigner: fixedSignatureSigner{ecKey, oversizedECDSASignature(t)}}, "does not match the key size"},
	} {
		t.Run(name, func(t *testing.T) {
			tc.cfg.Account, tc.cfg.User = "account", "user"
			_, err := prepareJWTToken(tc.cfg)
			assertNotNilF(t, err)
			assertStringContainsE(t, err.Error(), tc.message)
		})
	}
}

type failingTestSigner struct {
	crypto.Signer
}

func (failingTestSigner) Sign(_ io.Reader, _ []byte, _ crypto.SignerOpts) ([]byte, error) {
	return nil, errors.New("signer unavailable")
}

type fixedSignatureSigner struct {
	crypto.Signer
	signature []byte
}

func (s fixedSignatureSigner) Sign(_ io.Reader, _ []byte, _ crypto.SignerOpts) ([]byte, error) {
	return s.signature, nil
}

func oversizedECDSASignature(t *testing.T) []byte {
	t.Helper()
	tooBig := new(big.Int).Lsh(big.NewInt(1), 300) // 38 bytes, more than the 32 bytes of P-256
	der, err := asn1.Marshal(struct{ R, S *big.Int }{tooBig, big.NewInt(1)})
	assertNilF(t, err)
	return der
}

func TestECDSAASN1ToJWSPadsShortValues(t *testing.T) {
	der, err := asn1.Marshal(struct{ R, S *big.Int }{big.NewInt(0x0102), big.NewInt(0x03)})
	assertNilF(t, err)
	sig, err := ecdsaASN1ToJWS(der, 4)
	assertNilF(t, err)
	assertBytesEqualE(t, sig, []byte{0, 0, 1, 2, 0, 0, 0, 3})

	_, err = ecdsaASN1ToJWS(append(der, 0), 4)
	assertNotNilF(t, err, "trailing data after the signature should be rejected")
}

// The ECDSA signature produced through a generic crypto.Signer must verify with ecdsa.Verify using
// the r and s values from the JWS signature, for every supported curve.
func TestCryptoSignerSigningMethodECDSASignatureVerifies(t *testing.T) {
	for _, tc := range keyPairTestCases(t) {
		ecKey, ok := tc.signer.(*ecdsa.PrivateKey)
		if !ok {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			method, err := jwtSigningMethod(opaqueTestSigner{ecKey})
			assertNilF(t, err)
			signingString := "header.payload"
			sig, err := method.Sign(signingString, opaqueTestSigner{ecKey})
			assertNilF(t, err)
			keySize := (ecKey.Curve.Params().BitSize + 7) / 8
			assertEqualF(t, len(sig), 2*keySize)

			hasher := method.(*cryptoSignerSigningMethod).hash.New()
			hasher.Write([]byte(signingString))
			r := new(big.Int).SetBytes(sig[:keySize])
			s := new(big.Int).SetBytes(sig[keySize:])
			assertTrueF(t, ecdsa.Verify(&ecKey.PublicKey, hasher.Sum(nil), r, s), "signature should verify")
		})
	}
}

func TestUnitAuthenticateJWTWithECPrivateKeySigner(t *testing.T) {
	ecKey, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	assertNilF(t, err)

	var receivedToken string
	sr := &snowflakeRestful{
		FuncPostAuth: func(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
			var ar authRequest
			jsonBody, _ := bodyCreator()
			if err := json.Unmarshal(jsonBody, &ar); err != nil {
				return nil, err
			}
			if ar.Data.Authenticator != AuthTypeJwt.String() {
				return nil, fmt.Errorf("unexpected authenticator %v", ar.Data.Authenticator)
			}
			receivedToken = ar.Data.Token
			return &authResponse{
				Success: true,
				Data: authResponseMain{
					Token:       "t",
					MasterToken: "m",
					SessionInfo: authResponseSessionInfo{DatabaseName: "dbn"},
				},
			}, nil
		},
		TokenAccessor: getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeJwt
	sc.cfg.JWTExpireTimeout = time.Duration(sfconfig.DefaultJWTTimeout)
	sc.cfg.PrivateKeySigner = ecKey
	sc.rest = sr

	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	assertNilF(t, err)
	claims := verifyKeyPairJWT(t, receivedToken, ecKey.Public(), "ES384")
	assertTrueF(t, strings.HasSuffix(claims["iss"].(string), strings.TrimPrefix(expectedIssuer(t, ecKey.Public()), "ACCOUNT.USER")))
}
