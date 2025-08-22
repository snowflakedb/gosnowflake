package gosnowflake

import (
	"bytes"
	"errors"
	"fmt"
	"math"
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

func assertErrorsAsF(t *testing.T, err error, target any, descriptions ...string) {
	fatalOnNonEmpty(t, validateErrorsAs(err, target, descriptions...))
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

func assertEqualEpsilonE(t *testing.T, actual, expected, epsilon float64, descriptions ...string) {
	errorOnNonEmpty(t, validateEqualEpsilon(actual, expected, epsilon, descriptions...))
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

func assertHasPrefixF(t *testing.T, actual string, expectedPrefix string, descriptions ...string) {
	fatalOnNonEmpty(t, validateHasPrefix(actual, expectedPrefix, descriptions...))
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
	return fmt.Sprintf("%s. Thrown from %s", maskSecrets(errMsg), thrownFrom())
}

func validateNil(actual any, descriptions ...string) string {
	if isNil(actual) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be nil but was not. %s", maskSecrets(fmt.Sprintf("%v", actual)), desc)
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
	actualStr := "nil"
	expectedStr := "nil"
	if actual != nil {
		actualStr = maskSecrets(actual.Error())
	}
	if expected != nil {
		expectedStr = maskSecrets(expected.Error())
	}
	return fmt.Sprintf("expected %v to be %v. %s", actualStr, expectedStr, desc)
}

func validateErrorsAs(err error, target any, descriptions ...string) string {
	if errors.As(err, target) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	errStr := "nil"
	if err != nil {
		errStr = maskSecrets(err.Error())
	}
	targetType := reflect.TypeOf(target)
	return fmt.Sprintf("expected error %v to be assignable to %v but was not. %s", errStr, targetType, desc)
}

func validateEqual(actual any, expected any, descriptions ...string) string {
	if expected == actual {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be equal to \"%s\" but was not. %s",
		maskSecrets(fmt.Sprintf("%v", actual)),
		maskSecrets(fmt.Sprintf("%v", expected)),
		desc)
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
	return fmt.Sprintf("expected \"%s\" to be equal to \"%s\" but was not. %s",
		maskSecrets(actual),
		maskSecrets(expected),
		desc)
}

func validateEqualEpsilon(actual, expected, epsilon float64, descriptions ...string) string {
	if math.Abs(actual-expected) < epsilon {
		return ""
	}
	return fmt.Sprintf("expected \"%f\" to be equal to \"%f\" within epsilon \"%f\" but was not. %s", actual, expected, epsilon, joinDescriptions(descriptions...))
}

func validateDeepEqual(actual any, expected any, descriptions ...string) string {
	if reflect.DeepEqual(actual, expected) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be equal to \"%s\" but was not. %s",
		maskSecrets(fmt.Sprintf("%v", actual)),
		maskSecrets(fmt.Sprintf("%v", expected)),
		desc)
}

func validateNotEqual(actual any, expected any, descriptions ...string) string {
	if expected != actual {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" not to be equal to \"%s\" but they were the same. %s",
		maskSecrets(fmt.Sprintf("%v", actual)),
		maskSecrets(fmt.Sprintf("%v", expected)),
		desc)
}

func validateBytesEqual(actual []byte, expected []byte, descriptions ...string) string {
	if bytes.Equal(actual, expected) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be equal to \"%s\" but was not. %s",
		maskSecrets(string(actual)),
		maskSecrets(string(expected)),
		desc)
}

func validateStringContains(actual string, expectedToContain string, descriptions ...string) string {
	if strings.Contains(actual, expectedToContain) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to contain \"%s\" but did not. %s",
		maskSecrets(actual),
		maskSecrets(expectedToContain),
		desc)
}

func validateEmptyString(actual string, descriptions ...string) string {
	if actual == "" {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to be empty, but was not. %s", maskSecrets(actual), desc)
}

func validateHasPrefix(actual string, expectedPrefix string, descriptions ...string) string {
	if strings.HasPrefix(actual, expectedPrefix) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to start with \"%s\" but did not. %s",
		maskSecrets(actual),
		maskSecrets(expectedPrefix),
		desc)
}

func validateValueBetween(value float64, min float64, max float64, descriptions ...string) string {
	if value > min && value < max {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" should be between \"%s\" and  \"%s\" but did not. %s",
		fmt.Sprintf("%f", value),
		fmt.Sprintf("%f", min),
		fmt.Sprintf("%f", max),
		desc)
}

func validateValueBetweenInclusive(value float64, min float64, max float64, descriptions ...string) string {
	if value >= min && value <= max {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" should be between \"%s\" and  \"%s\" inclusively but did not. %s",
		fmt.Sprintf("%f", value),
		fmt.Sprintf("%f", min),
		fmt.Sprintf("%f", max),
		desc)
}

func validateEmpty[T any](value []T, descriptions ...string) string {
	if len(value) == 0 {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%v\" to be empty. %s", maskSecrets(fmt.Sprintf("%v", value)), desc)
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
			return maskSecrets(line)
		}
	}
	return maskSecrets(stack)
}
