package gosnowflake

import (
	"testing"
)

func TestSetAndGetCredential(t *testing.T) {
	skipOnMissingHome(t)
	for _, tokenSpec := range []*secureTokenSpec{
		newMfaTokenSpec("testhost", "testuser"),
		newIDTokenSpec("testhost", "testuser"),
	} {
		t.Run(string(tokenSpec.tokenType), func(t *testing.T) {
			skipOnMac(t, "keyring asks for password")
			fakeMfaToken := "test token"
			tokenSpec := newMfaTokenSpec("testHost", "testUser")
			credentialsStorage.setCredential(tokenSpec, fakeMfaToken)
			assertEqualE(t, credentialsStorage.getCredential(tokenSpec), fakeMfaToken)

			// delete credential and check it no longer exists
			credentialsStorage.deleteCredential(tokenSpec)
			assertEqualE(t, credentialsStorage.getCredential(tokenSpec), "")
		})
	}
}

func TestSkipStoringCredentialIfUserIsEmpty(t *testing.T) {
	tokenSpecs := []*secureTokenSpec{
		newMfaTokenSpec("mfaHost.com", ""),
		newIDTokenSpec("idHost.com", ""),
	}

	for _, tokenSpec := range tokenSpecs {
		t.Run(tokenSpec.host, func(t *testing.T) {
			credentialsStorage.setCredential(tokenSpec, "non-empty-value")
			assertEqualE(t, credentialsStorage.getCredential(tokenSpec), "")
		})
	}
}

func TestSkipStoringCredentialIfHostIsEmpty(t *testing.T) {
	tokenSpecs := []*secureTokenSpec{
		newMfaTokenSpec("", "mfaUser"),
		newIDTokenSpec("", "idUser"),
	}

	for _, tokenSpec := range tokenSpecs {
		t.Run(tokenSpec.user, func(t *testing.T) {
			credentialsStorage.setCredential(tokenSpec, "non-empty-value")
			assertEqualE(t, credentialsStorage.getCredential(tokenSpec), "")
		})
	}
}

func TestBuildCredentialsKey(t *testing.T) {
	testcases := []struct {
		host     string
		user     string
		credType tokenType
		out      string
	}{
		{"testaccount.snowflakecomputing.com", "testuser", "mfaToken", "c4e781475e7a5e74aca87cd462afafa8cc48ebff6f6ccb5054b894dae5eb6345"}, // pragma: allowlist secret
		{"testaccount.snowflakecomputing.com", "testuser", "IdToken", "5014e26489992b6ea56b50e936ba85764dc51338f60441bdd4a69eac7e15bada"},  // pragma: allowlist secret
	}
	for _, test := range testcases {
		target, err := buildCredentialsKey(test.host, test.user, test.credType)
		assertNilF(t, err)
		if target != test.out {
			t.Fatalf("failed to convert target. expected: %v, but got: %v", test.out, target)
		}
	}
}
