package gosnowflake

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func assertNilE(t *testing.T, actual any, descriptions ...string) {
	errorOnNonEmpty(t, validateNil(actual, descriptions...))
}

func assertNilF(t *testing.T, actual any, descriptions ...string) {
	fatalOnNonEmpty(t, validateNil(actual, descriptions...))
}

func assertNotNilE(t *testing.T, actual any, descriptions ...string) {
	errorOnNonEmpty(t, validateNotNil(actual, descriptions...))
}

func assertNotNilF(t *testing.T, actual any, descriptions ...string) {
	fatalOnNonEmpty(t, validateNotNil(actual, descriptions...))
}

func assertErrIsF(t *testing.T, actual, expected error, descriptions ...string) {
	fatalOnNonEmpty(t, validateErrIs(actual, expected, descriptions...))
}

func assertErrIsE(t *testing.T, actual, expected error, descriptions ...string) {
	errorOnNonEmpty(t, validateErrIs(actual, expected, descriptions...))
}

func assertEqualE(t *testing.T, actual any, expected any, descriptions ...string) {
	errorOnNonEmpty(t, validateEqual(actual, expected, descriptions...))
}

func assertEqualF(t *testing.T, actual any, expected any, descriptions ...string) {
	fatalOnNonEmpty(t, validateEqual(actual, expected, descriptions...))
}

func assertEqualIgnoringWhitespaceE(t *testing.T, actual string, expected string, descriptions ...string) {
	errorOnNonEmpty(t, validateEqualIgnoringWhitespace(actual, expected, descriptions...))
}

func assertDeepEqualE(t *testing.T, actual any, expected any, descriptions ...string) {
	errorOnNonEmpty(t, validateDeepEqual(actual, expected, descriptions...))
}

func assertNotEqualF(t *testing.T, actual any, expected any, descriptions ...string) {
	fatalOnNonEmpty(t, validateNotEqual(actual, expected, descriptions...))
}

func assertNotEqualE(t *testing.T, actual any, expected any, descriptions ...string) {
	errorOnNonEmpty(t, validateNotEqual(actual, expected, descriptions...))
}

func assertBytesEqualE(t *testing.T, actual []byte, expected []byte, descriptions ...string) {
	errorOnNonEmpty(t, validateBytesEqual(actual, expected, descriptions...))
}

func assertTrueF(t *testing.T, actual bool, descriptions ...string) {
	fatalOnNonEmpty(t, validateEqual(actual, true, descriptions...))
}

func assertTrueE(t *testing.T, actual bool, descriptions ...string) {
	errorOnNonEmpty(t, validateEqual(actual, true, descriptions...))
}

func assertFalseF(t *testing.T, actual bool, descriptions ...string) {
	fatalOnNonEmpty(t, validateEqual(actual, false, descriptions...))
}

func assertFalseE(t *testing.T, actual bool, descriptions ...string) {
	errorOnNonEmpty(t, validateEqual(actual, false, descriptions...))
}

func assertStringContainsE(t *testing.T, actual string, expectedToContain string, descriptions ...string) {
	errorOnNonEmpty(t, validateStringContains(actual, expectedToContain, descriptions...))
}

func assertStringContainsF(t *testing.T, actual string, expectedToContain string, descriptions ...string) {
	fatalOnNonEmpty(t, validateStringContains(actual, expectedToContain, descriptions...))
}

func assertEmptyStringE(t *testing.T, actual string, descriptions ...string) {
	errorOnNonEmpty(t, validateEmptyString(actual, descriptions...))
}

func assertHasPrefixE(t *testing.T, actual string, expectedPrefix string, descriptions ...string) {
	errorOnNonEmpty(t, validateHasPrefix(actual, expectedPrefix, descriptions...))
}

func assertBetweenE(t *testing.T, value float64, min float64, max float64, descriptions ...string) {
	errorOnNonEmpty(t, validateValueBetween(value, min, max, descriptions...))
}

func assertBetweenInclusiveE(t *testing.T, value float64, min float64, max float64, descriptions ...string) {
	errorOnNonEmpty(t, validateValueBetweenInclusive(value, min, max, descriptions...))
}

func assertEmptyE[T any](t *testing.T, actual []T, descriptions ...string) {
	errorOnNonEmpty(t, validateEmpty(actual, descriptions...))
}

func fatalOnNonEmpty(t *testing.T, errMsg string) {
	if errMsg != "" {
		t.Fatal(formatErrorMessage(errMsg))
	}
}

func errorOnNonEmpty(t *testing.T, errMsg string) {
	if errMsg != "" {
		t.Error(formatErrorMessage(errMsg))
	}
}

func formatErrorMessage(errMsg string) string {
	return fmt.Sprintf("%s. Thrown from %s", errMsg, thrownFrom())
}

func validateNil(actual any, descriptions ...string) string {
	if isNil(actual) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be nil but was not. %s", actual, desc)
}

func validateNotNil(actual any, descriptions ...string) string {
	if !isNil(actual) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected to be not nil but was not. %s", desc)
}

func validateErrIs(actual, expected error, descriptions ...string) string {
	if errors.Is(actual, expected) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected %v to be %v. %s", actual, expected, desc)
}

func validateEqual(actual any, expected any, descriptions ...string) string {
	if expected == actual {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be equal to \"%s\" but was not. %s", actual, expected, desc)
}

func removeWhitespaces(s string) string {
	pattern, err := regexp.Compile(`\s+`)
	if err != nil {
		panic(err)
	}
	return pattern.ReplaceAllString(s, "")
}

func validateEqualIgnoringWhitespace(actual string, expected string, descriptions ...string) string {
	if removeWhitespaces(expected) == removeWhitespaces(actual) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be equal to \"%s\" but was not. %s", actual, expected, desc)
}

func validateDeepEqual(actual any, expected any, descriptions ...string) string {
	if reflect.DeepEqual(actual, expected) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be equal to \"%s\" but was not. %s", actual, expected, desc)
}

func validateNotEqual(actual any, expected any, descriptions ...string) string {
	if expected != actual {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" not to be equal to \"%s\" but they were the same. %s", actual, expected, desc)
}

func validateBytesEqual(actual []byte, expected []byte, descriptions ...string) string {
	if bytes.Equal(actual, expected) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be equal to \"%s\" but was not. %s", actual, expected, desc)
}

func validateStringContains(actual string, expectedToContain string, descriptions ...string) string {
	if strings.Contains(actual, expectedToContain) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to contain \"%s\" but did not. %s", actual, expectedToContain, desc)
}

func validateEmptyString(actual string, descriptions ...string) string {
	if actual == "" {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be empty, but was not. %s", actual, desc)
}

func validateHasPrefix(actual string, expectedPrefix string, descriptions ...string) string {
	if strings.HasPrefix(actual, expectedPrefix) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to start with \"%s\" but did not. %s", actual, expectedPrefix, desc)
}

func validateValueBetween(value float64, min float64, max float64, descriptions ...string) string {
	if value > min && value < max {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%f\" should be between \"%f\" and  \"%f\" but did not. %s", value, min, max, desc)
}

func validateValueBetweenInclusive(value float64, min float64, max float64, descriptions ...string) string {
	if value >= min && value <= max {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%f\" should be between \"%f\" and  \"%f\" inclusively but did not. %s", value, min, max, desc)
}

func validateEmpty[T any](value []T, descriptions ...string) string {
	if len(value) == 0 {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%v\" to be empty. %s", value, desc)
}

func joinDescriptions(descriptions ...string) string {
	return strings.Join(descriptions, " ")
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	val := reflect.ValueOf(value)
	return slices.Contains([]reflect.Kind{reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface}, val.Kind()) && val.IsNil()
}

func thrownFrom() string {
	buf := make([]byte, 1024)
	size := runtime.Stack(buf, false)
	stack := string(buf[0:size])
	lines := strings.Split(stack, "\n\t")
	for i, line := range lines {
		if i > 0 && !strings.Contains(line, "assert_test.go") {
			return line
		}
	}
	return stack
}

// castToTransport safely casts http.RoundTripper to *http.Transport
// Returns nil if the cast fails
func castToTransport(rt http.RoundTripper) *http.Transport {
	if transport, ok := rt.(*http.Transport); ok {
		return transport
	}
	return nil
}

// assertTransportsEqual compares two transports, excluding function fields and other non-comparable fields
// that may vary between instances but represent equivalent configurations
func assertTransportsEqual(t *testing.T, expected, actual *http.Transport, msg string) {
	if expected == nil && actual == nil {
		return
	}
	assertNotNilF(t, expected, "Expected transport should not be nil in %s", msg)
	assertNotNilF(t, actual, "Actual transport should not be nil in %s", msg)

	// Compare TLS configurations
	assertTLSConfigsEqual(t, expected.TLSClientConfig, actual.TLSClientConfig, msg+" TLS config")

	// Compare other relevant transport fields (excluding function fields)
	assertEqualF(t, expected.MaxIdleConns, actual.MaxIdleConns, "%s MaxIdleConns", msg)
	assertEqualF(t, expected.MaxIdleConnsPerHost, actual.MaxIdleConnsPerHost, "%s MaxIdleConnsPerHost", msg)
	assertEqualF(t, expected.MaxConnsPerHost, actual.MaxConnsPerHost, "%s MaxConnsPerHost", msg)
	assertEqualF(t, expected.IdleConnTimeout, actual.IdleConnTimeout, "%s IdleConnTimeout", msg)
	assertEqualF(t, expected.ResponseHeaderTimeout, actual.ResponseHeaderTimeout, "%s ResponseHeaderTimeout", msg)
	assertEqualF(t, expected.ExpectContinueTimeout, actual.ExpectContinueTimeout, "%s ExpectContinueTimeout", msg)
	assertEqualF(t, expected.TLSHandshakeTimeout, actual.TLSHandshakeTimeout, "%s TLSHandshakeTimeout", msg)
	assertEqualF(t, expected.DisableKeepAlives, actual.DisableKeepAlives, "%s DisableKeepAlives", msg)
	assertEqualF(t, expected.DisableCompression, actual.DisableCompression, "%s DisableCompression", msg)
	assertEqualF(t, expected.ForceAttemptHTTP2, actual.ForceAttemptHTTP2, "%s ForceAttemptHTTP2", msg)
}

// assertTLSConfigsEqual compares two TLS configurations, excluding function fields
// like VerifyPeerCertificate which may point to different but equivalent functions
func assertTLSConfigsEqual(t *testing.T, expected, actual *tls.Config, msg string) {
	if expected == nil && actual == nil {
		return
	}
	assertNotNilF(t, expected, "Expected TLS config should not be nil in %s", msg)
	assertNotNilF(t, actual, "Actual TLS config should not be nil in %s", msg)

	// Compare non-function fields
	assertEqualF(t, expected.InsecureSkipVerify, actual.InsecureSkipVerify, "%s InsecureSkipVerify", msg)
	assertEqualF(t, expected.ServerName, actual.ServerName, "%s ServerName", msg)
	assertEqualF(t, expected.MinVersion, actual.MinVersion, "%s MinVersion", msg)
	assertEqualF(t, expected.MaxVersion, actual.MaxVersion, "%s MaxVersion", msg)

	// For VerifyPeerCertificate, just check presence/absence since function pointers can't be compared
	expectedHasVerifier := expected.VerifyPeerCertificate != nil
	actualHasVerifier := actual.VerifyPeerCertificate != nil
	assertEqualF(t, expectedHasVerifier, actualHasVerifier, "%s VerifyPeerCertificate presence", msg)
}
