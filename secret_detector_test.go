package gosnowflake

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	longToken = "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=UKyJH9DS=nFzzWnfZKGV+C7GopWC" + // pragma: allowlist secret
		"GD4LjOLLFZKOE26LXHDt3pTi4iI1qwKuSpf/FmClCMBSissVsU3Ei590FP0lPQQhcSG" + // pragma: allowlist secret
		"cDu69ZL_1X6e9h5z62t/iY7ZkII28n2qU=nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKjZ" + // pragma: allowlist secret
		"BAx4w6=_lqtt67bIA=o7D=oUSjfywsRFoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5XdJY" + // pragma: allowlist secret
		"uI8vhg=f0bKSq7AhQ2Bh"
	randomPassword     = `Fh[+2J~AcqeqW%?`
	falsePositiveToken = "2020-04-30 23:06:04,069 - MainThread auth.py:397" +
		" - write_temporary_credential() - DEBUG - no ID token is given when " +
		"try to store temporary credential"
)

// generateTestJWT creates a test JWT token for masking tests using the JWT library
func generateTestJWT(t *testing.T) string {
	// Create claims for the test JWT
	claims := jwt.MapClaims{
		"sub":  "test123",
		"name": "Test User",
		"exp":  time.Now().Add(time.Hour).Unix(),
		"iat":  time.Now().Unix(),
	}

	// Create the token with HS256 signing method
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// Sign the token with a test secret
	testSecret := []byte("test-secret-for-masking-validation")
	tokenString, err := token.SignedString(testSecret)
	if err != nil {
		// Fallback to a simple test JWT if signing fails
		t.Fatalf("Failed to generate test JWT: %s", err)
	}

	return tokenString
}

func TestSecretsDetector(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		// Token masking tests
		{"Token with equals", fmt.Sprintf("Token =%s", longToken), "Token =****"},
		{"idToken with colon space", fmt.Sprintf("idToken : %s", longToken), "idToken : ****"},
		{"sessionToken with colon space", fmt.Sprintf("sessionToken : %s", longToken), "sessionToken : ****"},
		{"masterToken with colon space", fmt.Sprintf("masterToken : %s", longToken), "masterToken : ****"},
		{"accessToken with colon space", fmt.Sprintf("accessToken : %s", longToken), "accessToken : ****"},
		{"refreshToken with colon space", fmt.Sprintf("refreshToken : %s", longToken), "refreshToken : ****"},
		{"programmaticAccessToken with colon space", fmt.Sprintf("programmaticAccessToken : %s", longToken), "programmaticAccessToken : ****"},
		{"programmatic_access_token with colon space", fmt.Sprintf("programmatic_access_token : %s", longToken), "programmatic_access_token : ****"},
		{"JWT - with Bearer prefix", fmt.Sprintf("Bearer %s", generateTestJWT(t)), "Bearer ****"},
		{"JWT - with JWT prefix", fmt.Sprintf("JWT %s", generateTestJWT(t)), "JWT ****"},

		// Password masking tests
		{"password with colon", fmt.Sprintf("password:%s", randomPassword), "password:****"},
		{"PASSWORD uppercase with colon", fmt.Sprintf("PASSWORD:%s", randomPassword), "PASSWORD:****"},
		{"PaSsWoRd mixed case with colon", fmt.Sprintf("PaSsWoRd:%s", randomPassword), "PaSsWoRd:****"},
		{"password with equals and spaces", fmt.Sprintf("password = %s", randomPassword), "password = ****"},
		{"pwd with colon", fmt.Sprintf("pwd:%s", randomPassword), "pwd:****"},

		// Mixed token and password tests
		{
			"token and password mixed",
			fmt.Sprintf("token=%s foo bar baz password:%s", longToken, randomPassword),
			"token=**** foo bar baz password:****",
		},
		{
			"PWD and TOKEN mixed",
			fmt.Sprintf("PWD = %s blah blah blah TOKEN:%s", randomPassword, longToken),
			"PWD = **** blah blah blah TOKEN:****",
		},

		// Client secret tests
		{"clientSecret with values", "clientSecret abc oauthClientSECRET=def", "clientSecret **** oauthClientSECRET=****"},

		// False positive test
		{"false positive should not be masked", falsePositiveToken, falsePositiveToken},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := maskSecrets(tc.input)
			assertEqualE(t, result, tc.expected)
		})
	}
}
