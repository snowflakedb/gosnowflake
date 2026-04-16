package config

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	sflogger "github.com/snowflakedb/gosnowflake/v2/internal/logger"
)

// TODO temporary - move this to a common test utils package when we have one
func maskSecrets(text string) string {
	return sflogger.MaskSecrets(text)
}

func assertNilE(t *testing.T, actual any, descriptions ...string) {
	t.Helper()
	errorOnNonEmpty(t, validateNil(actual, descriptions...))
}

func assertNilF(t *testing.T, actual any, descriptions ...string) {
	t.Helper()
	fatalOnNonEmpty(t, validateNil(actual, descriptions...))
}

func assertNotNilF(t *testing.T, actual any, descriptions ...string) {
	t.Helper()
	fatalOnNonEmpty(t, validateNotNil(actual, descriptions...))
}

func assertEqualE(t *testing.T, actual any, expected any, descriptions ...string) {
	t.Helper()
	errorOnNonEmpty(t, validateEqual(actual, expected, descriptions...))
}

func assertEqualF(t *testing.T, actual any, expected any, descriptions ...string) {
	t.Helper()
	fatalOnNonEmpty(t, validateEqual(actual, expected, descriptions...))
}

func assertTrueE(t *testing.T, actual bool, descriptions ...string) {
	t.Helper()
	errorOnNonEmpty(t, validateEqual(actual, true, descriptions...))
}

func assertTrueF(t *testing.T, actual bool, descriptions ...string) {
	t.Helper()
	fatalOnNonEmpty(t, validateEqual(actual, true, descriptions...))
}

func assertFalseE(t *testing.T, actual bool, descriptions ...string) {
	t.Helper()
	errorOnNonEmpty(t, validateEqual(actual, false, descriptions...))
}

func fatalOnNonEmpty(t *testing.T, errMsg string) {
	if errMsg != "" {
		t.Helper()
		t.Fatal(formatErrorMessage(errMsg))
	}
}

func errorOnNonEmpty(t *testing.T, errMsg string) {
	if errMsg != "" {
		t.Helper()
		t.Error(formatErrorMessage(errMsg))
	}
}

func formatErrorMessage(errMsg string) string {
	return fmt.Sprintf("[%s] %s", time.Now().Format(time.RFC3339Nano), maskSecrets(errMsg))
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

func joinDescriptions(descriptions ...string) string {
	return strings.Join(descriptions, " ")
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	val := reflect.ValueOf(value)
	return slices.Contains([]reflect.Kind{reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface, reflect.Func}, val.Kind()) && val.IsNil()
}
