//go:build !linux

package gosnowflake

import (
	"testing"

	"github.com/99designs/keyring"
)

func TestTokenKeyringConfigUsesSharedServiceName(t *testing.T) {
	cfg := tokenKeyringConfig()
	assertEqualE(t, cfg.ServiceName, "snowflake_credential_cache")
	assertEqualE(t, cfg.WinCredPrefix, "snowflake_credential_cache")
}

func TestKeyringStoresTokenUnderCacheKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec secureTokenSpec
	}{
		{string(mfaToken), newMfaTokenSpec(&Config{Host: "testhost", User: "testuser"})},
		{string(idToken), newIDTokenSpec(&Config{Host: "testhost", User: "testuser"})},
		{string(oauthAccessToken), newOAuthAccessTokenSpec(&Config{OauthTokenRequestURL: "https://idp.example.com/token", Host: "testhost", User: "testuser", Role: "testrole"})},
		{string(oauthRefreshToken), newOAuthRefreshTokenSpec(&Config{OauthTokenRequestURL: "https://idp.example.com/token", Host: "testhost", User: "testuser", Role: "testrole"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ring := keyring.NewArrayKeyring(nil)
			ssm := &keyringSecureStorageManager{openKeyring: func() (keyring.Keyring, error) { return ring, nil }}
			cacheKey, err := tc.spec.buildKey()
			assertNilF(t, err)

			ssm.setCredential(tc.spec, "token-value")
			keys, err := ring.Keys()
			assertNilF(t, err)
			assertDeepEqualE(t, keys, []string{cacheKey})
			item, err := ring.Get(cacheKey)
			assertNilF(t, err)
			assertEqualE(t, string(item.Data), "token-value")
			assertEqualE(t, ssm.getCredential(tc.spec), "token-value")

			ssm.deleteCredential(tc.spec)
			keys, err = ring.Keys()
			assertNilF(t, err)
			assertEqualE(t, len(keys), 0)
		})
	}
}
