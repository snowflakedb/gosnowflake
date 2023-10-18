// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func assertNilF(t *testing.T, actual any, descriptions ...string) {
	fatalOnNonEmpty(t, validateNil(actual, descriptions...))
}

func assertNotNilF(t *testing.T, actual any, descriptions ...string) {
	fatalOnNonEmpty(t, validateNotNil(actual, descriptions...))
}

func assertEqualE(t *testing.T, actual any, expected any, descriptions ...string) {
	errorOnNonEmpty(t, validateEqual(actual, expected, descriptions...))
}

func assertEqualF(t *testing.T, actual any, expected any, descriptions ...string) {
	fatalOnNonEmpty(t, validateEqual(actual, expected, descriptions...))
}

func assertStringContainsE(t *testing.T, actual string, expectedToContain string, descriptions ...string) {
	errorOnNonEmpty(t, validateStringContains(actual, expectedToContain, descriptions...))
}

func assertHasPrefixE(t *testing.T, actual string, expectedPrefix string, descriptions ...string) {
	errorOnNonEmpty(t, validateHasPrefix(actual, expectedPrefix, descriptions...))
}

func fatalOnNonEmpty(t *testing.T, errMsg string) {
	if errMsg != "" {
		t.Fatal(errMsg)
	}
}

func errorOnNonEmpty(t *testing.T, errMsg string) {
	if errMsg != "" {
		t.Error(errMsg)
	}
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

func validateEqual(actual any, expected any, descriptions ...string) string {
	if expected == actual {
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

func validateHasPrefix(actual string, expectedPrefix string, descriptions ...string) string {
	if strings.HasPrefix(actual, expectedPrefix) {
		return ""
	}
	desc := joinDescriptions(descriptions...)
	return fmt.Sprintf("expected \"%s\" to start with \"%s\" but did not. %s", actual, expectedPrefix, desc)
}

func joinDescriptions(descriptions ...string) string {
	return strings.Join(descriptions, " ")
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	val := reflect.ValueOf(value)
	return val.Kind() == reflect.Pointer && val.IsNil()
}
