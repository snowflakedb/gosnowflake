package logger

import (
	"fmt"
	"net/http"
	"strings"
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
		{"Token with equals", "Token =" + longToken, "Token =****"},
		{"idToken with colon space", "idToken : " + longToken, "idToken : ****"},
		{"sessionToken with colon space", "sessionToken : " + longToken, "sessionToken : ****"},
		{"masterToken with colon space", "masterToken : " + longToken, "masterToken : ****"},
		{"accessToken with colon space", "accessToken : " + longToken, "accessToken : ****"},
		{"refreshToken with colon space", "refreshToken : " + longToken, "refreshToken : ****"},
		{"programmaticAccessToken with colon space", "programmaticAccessToken : " + longToken, "programmaticAccessToken : ****"},
		{"programmatic_access_token with colon space", "programmatic_access_token : " + longToken, "programmatic_access_token : ****"},
		{"JWT - with Bearer prefix", "Bearer " + generateTestJWT(t), "Bearer ****"},
		{"JWT - with JWT prefix", "JWT " + generateTestJWT(t), "JWT ****"},

		// Password masking tests
		{"password with colon", "password:" + randomPassword, "password:****"},
		{"PASSWORD uppercase with colon", "PASSWORD:" + randomPassword, "PASSWORD:****"},
		{"PaSsWoRd mixed case with colon", "PaSsWoRd:" + randomPassword, "PaSsWoRd:****"},
		{"password with equals and spaces", "password = " + randomPassword, "password = ****"},
		{"pwd with colon", "pwd:" + randomPassword, "pwd:****"},

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
			result := MaskSecrets(tc.input)
			if result != tc.expected {
				t.Errorf("expected %q to be equal to %q but was not", result, tc.expected)
			}
		})
	}
}

// TestSecretsDetectorMasksSSECKeyAndSigV4Params checks that the SSE-C customer-key
// header and the X-Amz-Credential / X-Amz-Security-Token query parameters are masked
// (SNOW-3649835).
func TestSecretsDetectorMasksSSECKeyAndSigV4Params(t *testing.T) {
	const (
		// base64 SSE-C customer key
		sseCKey = "dGhpc2lzYTMyYnl0ZXNlY3JldGtleWZvcnNzZWMxMjM0NTY3OD0=" // pragma: allowlist secret
		// X-Amz-Security-Token contains '%' separators
		securityToken = "FQoGZXIvYXdzEABCDEF%2Fwgr1234567890abcdefGHIJKLmnop%2BqrstUVWxyz%3D" // pragma: allowlist secret
		credential    = "AKIAIOSFODNN7EXAMPLE%2F20260612%2Fus-east-1%2Fs3%2Faws4_request"     // pragma: allowlist secret
	)
	testCases := []struct {
		name   string
		input  string
		secret string
	}{
		{
			"SSE-C key from chunk header debug line",
			"adding header: x-amz-server-side-encryption-customer-key, value: " + sseCKey,
			sseCKey,
		},
		{
			"SSE-C key with equals separator",
			"x-amz-server-side-encryption-customer-key=" + sseCKey,
			sseCKey,
		},
		{
			// The value contains '%'; assert the whole value is masked.
			"X-Amz-Security-Token query parameter",
			"https://sfc.s3.amazonaws.com/chunk?X-Amz-Security-Token=" + securityToken + "&X-Amz-SignedHeaders=host",
			"qrstUVWxyz",
		},
		{
			"X-Amz-Credential query parameter",
			"https://sfc.s3.amazonaws.com/chunk?X-Amz-Credential=" + credential + "&X-Amz-Date=20260612T000000Z",
			credential,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := MaskSecrets(tc.input)
			if strings.Contains(result, tc.secret) {
				t.Errorf("secret leaked through MaskSecrets.\nInput:  %q\nOutput: %q\nSecret should have been masked: %q", tc.input, result, tc.secret)
			}
			if !strings.Contains(result, "****") {
				t.Errorf("expected masked output to contain the **** marker, got %q", result)
			}
		})
	}
}

// TestSecretsDetectorMasksPresignedSignature checks that signed-URL query parameters
// (S3 / GCS / Azure) and password= values are fully masked (SNOW-3649773).
func TestSecretsDetectorMasksPresignedSignature(t *testing.T) {
	const (
		s3Sig    = "4a8b3c2d1e0f9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b" // pragma: allowlist secret
		gcsSig   = "9f8e7d6c5b4a39281706f5e4d3c2b1a0ffeeddccbbaa99887766554433221100" // pragma: allowlist secret
		azureSig = "Q3JhYmJ5bWFzdGVyc2lnSGV4QWJjMTIzNDU2Nzg5MHF3ZXJ0eQ"               // pragma: allowlist secret
		password = "SuperSecretPass1234"                                              // pragma: allowlist secret
	)
	testCases := []struct {
		name   string
		input  string
		secret string
	}{
		{
			"S3 X-Amz-Signature",
			"https://sfc-results.s3.amazonaws.com/chunk0?X-Amz-Signature=" + s3Sig + "&X-Amz-SignedHeaders=host",
			s3Sig,
		},
		{
			"GCS X-Goog-Signature",
			"https://storage.googleapis.com/sfc/chunk0?X-Goog-Signature=" + gcsSig + "&X-Goog-SignedHeaders=host",
			gcsSig,
		},
		{
			"Azure sig",
			"https://sfcresults.blob.core.windows.net/chunk0?sv=2021-08-06&sig=" + azureSig + "&se=2026-06-12",
			azureSig,
		},
		{
			"password with equals (sasTokenPattern, not passwordPattern)",
			"password=" + password,
			password,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := MaskSecrets(tc.input)
			if strings.Contains(result, tc.secret) {
				t.Errorf("secret re-emitted by MaskSecrets.\nInput:  %q\nOutput: %q\nSecret should have been masked: %q", tc.input, result, tc.secret)
			}
			if !strings.Contains(result, "****") {
				t.Errorf("expected masked output to contain the **** marker, got %q", result)
			}
		})
	}
}

// TestSecretsDetectorMasksSnowflakeSessionToken pins every documented Snowflake
// session-token format. Per GlobalServices SecurityToken.java the formats are
//
//	V1  ver:1-hint:<keyId>-<encrypted>
//	V2  ver:2-hint:<keyId>-did:<deployId>-<encrypted>
//	V3  ver:3-hint:<keyId>-<encrypted>
//	V4  ver:4-hint:<keyId>-did:<deployId>-<encrypted>
//
// V3 and V4 are the formats currently minted; V1 and V2 remain accepted, and V2/V4
// carry a second ':' in the "-did:" segment. Every one of them begins "ver:", which
// is why the value class of connectionTokenPattern has to admit ':': without it the
// match ended after "ver", three characters in and below the 8-character minimum, so
// the pattern could not match any version of the format it is named for.
func TestSecretsDetectorMasksSnowflakeSessionToken(t *testing.T) {
	const encrypted = "ETMsDgAAAZ1kQ2hpABRBRVMvQ0JDL1BLQ1M1UGFkZGluZw" // pragma: allowlist secret
	for _, tv := range []struct {
		version string
		token   string
	}{
		{"V1", "ver:1-hint:9823-" + encrypted},
		{"V2", "ver:2-hint:9823-did:42-" + encrypted},
		{"V3", "ver:3-hint:9823-" + encrypted},
		{"V4", "ver:4-hint:9823-did:42-" + encrypted},
	} {
		for _, tc := range []struct {
			name  string
			input string
		}{
			{"quoted authorization value", `Authorization: Snowflake Token="` + tv.token + `"`},
			{"bare token assignment", "token=" + tv.token},
			{"token with colon separator", "token: " + tv.token},
		} {
			t.Run(tv.version+"/"+tc.name, func(t *testing.T) {
				masked := MaskSecrets(tc.input)
				if strings.Contains(masked, encrypted) {
					t.Errorf("session token must not survive masking.\n input: %v\nmasked: %v", tc.input, masked)
				}
				if !strings.Contains(masked, "****") {
					t.Errorf("expected a masked marker in %v", masked)
				}
			})
		}
	}
}

// TestSecretsDetectorOverRenderedHeaderMap pins the shape produced by formatting an
// http.Header with %v, which is map[Name:[value]].
//
// This shape is the reason header values are no longer logged at all: the '[' that
// %v places between the colon and the value prevents the header-specific patterns
// from matching, so masking cannot be relied on here. The test documents which
// values do and do not survive, so that anyone reintroducing a header dump sees the
// consequence rather than assuming the masking covers it.
func TestSecretsDetectorOverRenderedHeaderMap(t *testing.T) {
	for _, tc := range []struct {
		name       string
		header     http.Header
		secret     string
		wantMasked bool
	}{
		{
			"bearer JWT is covered by jwtTokenPattern",
			http.Header{"Authorization": []string{"Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dQw4w9WgXcQabcdef"}},
			"eyJzdWIiOiIxMjM0NTY3ODkwIn0",
			true,
		},
		{
			"SSE-C customer key is NOT covered in this rendering",
			http.Header{"X-Amz-Server-Side-Encryption-Customer-Key": []string{"dGhpc2lzYTMyYnl0ZXNlY3JldGtleWZvcnNzZWMxMjM0NTY3OD0="}}, // pragma: allowlist secret
			"dGhpc2lzYTMyYnl0ZXNlY3JldGtleWZvcnNzZWMxMjM0NTY3OD0=",
			false,
		},
		{
			"opaque session header is NOT covered in this rendering",
			http.Header{"X-Snowflake-Session": []string{"AbCdEf1234567890XyZQwErTy"}},
			"AbCdEf1234567890XyZQwErTy",
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			masked := MaskSecrets(fmt.Sprintf("Header: %v", tc.header))
			survived := strings.Contains(masked, tc.secret)
			if tc.wantMasked && survived {
				t.Errorf("expected %v to be masked, got %v", tc.secret, masked)
			}
			if !tc.wantMasked && !survived {
				t.Logf("NOTE: %v is now masked in the rendered-header shape; masking coverage improved. "+
					"Header values are still omitted at the log sites by design - see headerNames.", tc.secret)
			}
		})
	}
}
