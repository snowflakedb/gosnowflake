// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func assertNil(t *testing.T, actual any, descriptions ...string) {
	if !isNil(actual) {
		desc := joinDescriptions(descriptions...)
		errMsg := fmt.Sprintf("expected \"%s\" to be nil but was not. %s", actual, desc)
		t.Fatal(errMsg)
	}
}

func assertNotNil(t *testing.T, actual any, descriptions ...string) {
	if isNil(actual) {
		desc := joinDescriptions(descriptions...)
		errMsg := fmt.Sprintf("expected to be not nil but was not. %s", desc)
		t.Fatal(errMsg)
	}
}

func assertEqual(t *testing.T, actual string, expected string, descriptions ...string) {
	if expected != actual {
		desc := joinDescriptions(descriptions...)
		errMsg := fmt.Sprintf("expected \"%s\" to be equal to \"%s\" but was not. %s", actual, expected, desc)
		t.Fatal(errMsg)
	}
}

func assertStringContains(t *testing.T, actual string, expectedToContain string, descriptions ...string) {
	if !strings.Contains(actual, expectedToContain) {
		desc := joinDescriptions(descriptions...)
		errMsg := fmt.Sprintf("expected \"%s\" to contain \"%s\" but did not. %s", actual, expectedToContain, desc)
		t.Fatal(errMsg)
	}
}

func assertHasPrefix(t *testing.T, actual string, expectedPrefix string, descriptions ...string) {
	if !strings.HasPrefix(actual, expectedPrefix) {
		desc := joinDescriptions(descriptions...)
		errMsg := fmt.Sprintf("expected \"%s\" to start with \"%s\" but did not. %s", actual, expectedPrefix, desc)
		t.Fatal(errMsg)
	}
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
