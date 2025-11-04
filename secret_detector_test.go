package gosnowflake

import (
	"strings"
	"sync"
	"testing"
)

const (
	longToken = "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=UKyJH9DS=nFzzWnfZKGV+C7GopWC" + // pragma: allowlist secret
		"GD4LjOLLFZKOE26LXHDt3pTi4iI1qwKuSpf/FmClCMBSissVsU3Ei590FP0lPQQhcSG" + // pragma: allowlist secret
		"cDu69ZL_1X6e9h5z62t/iY7ZkII28n2qU=nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKjZ" + // pragma: allowlist secret
		"BAx4w6=_lqtt67bIA=o7D=oUSjfywsRFoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5XdJY" + // pragma: allowlist secret
		"uI8vhg=f0bKSq7AhQ2Bh"
	randomPassword = `Fh[+2J~AcqeqW%?`
)

func TestMaskToken(t *testing.T) {
	if text := maskSecrets("Token =" + longToken); strings.Compare(text, "Token =****") != 0 {
		t.Errorf("mask unsuccessful. expected: Token=****, got: %v", text)
	}
	if text := maskSecrets("idToken : " + longToken); strings.Compare(text, "idToken : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: idToken : ****, got: %v", text)
	}
	if text := maskSecrets("sessionToken : " + longToken); strings.Compare(text, "sessionToken : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: sessionToken : ****, got: %v", text)
	}
	if text := maskSecrets("masterToken : " + longToken); strings.Compare(text, "masterToken : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: masterToken : ****, got: %v", text)
	}
	if text := maskSecrets("accessToken : " + longToken); strings.Compare(text, "accessToken : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: accessToken : ****, got: %v", text)
	}
	if text := maskSecrets("refreshToken : " + longToken); strings.Compare(text, "refreshToken : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: refreshToken : ****, got: %v", text)
	}
	if text := maskSecrets("programmaticAccessToken : " + longToken); strings.Compare(text, "programmaticAccessToken : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: programmaticAccessToken : ****, got: %v", text)
	}
	if text := maskSecrets("programmatic_access_token : " + longToken); strings.Compare(text, "programmatic_access_token : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: programmatic_access_token : ****, got: %v", text)
	}

	falsePositiveToken := "2020-04-30 23:06:04,069 - MainThread auth.py:397" +
		" - write_temporary_credential() - DEBUG - no ID token is given when " +
		"try to store temporary credential"
	if text := maskSecrets(falsePositiveToken); strings.Compare(text, falsePositiveToken) != 0 {
		t.Errorf("mask token %v should not have changed value. got: %v", falsePositiveToken, text)
	}
}

func TestMaskPassword(t *testing.T) {
	if text := maskSecrets("password:" + randomPassword); strings.Compare(text, "password:****") != 0 {
		t.Errorf("mask unsuccessful. expected: password:****, got: %v", text)
	}
	if text := maskSecrets("PASSWORD:" + randomPassword); strings.Compare(text, "PASSWORD:****") != 0 {
		t.Errorf("mask unsuccessful. expected: PASSWORD:****, got: %v", text)
	}
	if text := maskSecrets("PaSsWoRd:" + randomPassword); strings.Compare(text, "PaSsWoRd:****") != 0 {
		t.Errorf("mask unsuccessful. expected: PaSsWoRd:****, got: %v", text)
	}
	if text := maskSecrets("password = " + randomPassword); strings.Compare(text, "password = ****") != 0 {
		t.Errorf("mask unsuccessful. expected: password = ****, got: %v", text)
	}
	if text := maskSecrets("pwd:" + randomPassword); strings.Compare(text, "pwd:****") != 0 {
		t.Errorf("mask unsuccessful. expected: pwd:****, got: %v", text)
	}
}

func TestTokenPassword(t *testing.T) {
	text := maskSecrets("token=" + longToken + " foo bar baz " + "password:" + randomPassword)
	expected := "token=**** foo bar baz password:****"
	if strings.Compare(text, expected) != 0 {
		t.Errorf("mask unsuccessful. expected: %v, got: %v", expected, text)
	}
	text = maskSecrets("PWD = " + randomPassword + " blah blah blah " + "TOKEN:" + longToken)
	expected = "PWD = **** blah blah blah TOKEN:****"
	if strings.Compare(text, expected) != 0 {
		t.Errorf("mask unsuccessful. expected: %v, got: %v", expected, text)
	}
}

func TestClientSecret(t *testing.T) {
	text := maskSecrets("clientSecret abc oauthClientSECRET=def")
	expected := "clientSecret **** oauthClientSECRET=****"
	assertEqualE(t, text, expected)
}

func TestMaskSecretsThreadSafety(t *testing.T) {
	// Function to create isolated test cases for each goroutine
	createTestCases := func() []struct {
		name     string
		input    string
		expected string
	} {
		return []struct {
			name     string
			input    string
			expected string
		}{
			{"Token", "Token =" + longToken, "Token =****"},
			{"Password", "password:" + randomPassword, "password:****"},
			{"Client Secret", "clientSecret abc", "clientSecret ****"},
			{"Mixed", "token=" + longToken + " password:" + randomPassword, "token=**** password:****"},
			{"JWT Token", "jwt: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c", "jwt ****"},
			{"Access Token", "accessToken : " + longToken, "accessToken : ****"},
			{"Master Token", "masterToken : " + longToken, "masterToken : ****"},
		}
	}

	const numGoroutines = 50
	const iterationsPerGoroutine = 100

	var wg sync.WaitGroup
	errorsChan := make(chan error, numGoroutines*iterationsPerGoroutine)

	// Start multiple goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Create isolated test cases for this goroutine
			testCases := createTestCases()

			// Each goroutine runs multiple iterations
			for j := 0; j < iterationsPerGoroutine; j++ {
				// Test each case
				for _, tc := range testCases {
					result := maskSecrets(tc.input)
					if result != tc.expected {
						errorsChan <- &testError{
							goroutineID: goroutineID,
							iteration:   j,
							testCase:    tc.name,
							input:       tc.input,
							expected:    tc.expected,
							actual:      result,
						}
						return
					}
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errorsChan)

	// Check for any errors
	var errors []*testError
	for err := range errorsChan {
		errors = append(errors, err.(*testError))
	}

	if len(errors) > 0 {
		t.Errorf("Thread safety test failed with %d errors:", len(errors))
		for _, err := range errors {
			t.Errorf("Goroutine %d, iteration %d, test case '%s': expected '%s', got '%s' for input '%s'",
				err.goroutineID, err.iteration, err.testCase, err.expected, err.actual, err.input)
		}
	}

	// Calculate total calls (7 test cases per iteration)
	totalCalls := numGoroutines * iterationsPerGoroutine * 7
	t.Logf("Successfully completed %d goroutines with %d iterations each (%d total maskSecrets calls)",
		numGoroutines, iterationsPerGoroutine, totalCalls)
}

// testError is a custom error type for capturing test failures in goroutines
type testError struct {
	goroutineID int
	iteration   int
	testCase    string
	input       string
	expected    string
	actual      string
}

func (e *testError) Error() string {
	return "maskSecrets thread safety test failure"
}
