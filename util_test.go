// Copyright (c) 2017-2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

type tcIntMinMax struct {
	v1  int
	v2  int
	out int
}

type tcUUID struct {
	uuid string
}

type constTypeProvider struct {
	constTime int64
}

func (ctp *constTypeProvider) currentTime() int64 {
	return ctp.constTime
}

func constTimeProvider(constTime int64) *constTypeProvider {
	return &constTypeProvider{constTime: constTime}
}

func TestSimpleTokenAccessor(t *testing.T) {
	accessor := getSimpleTokenAccessor()
	token, masterToken, sessionID := accessor.GetTokens()
	if token != "" {
		t.Errorf("unexpected token %v", token)
	}
	if masterToken != "" {
		t.Errorf("unexpected master token %v", masterToken)
	}
	if sessionID != -1 {
		t.Errorf("unexpected session id %v", sessionID)
	}

	expectedToken, expectedMasterToken, expectedSessionID := "token123", "master123", int64(123)
	accessor.SetTokens(expectedToken, expectedMasterToken, expectedSessionID)
	token, masterToken, sessionID = accessor.GetTokens()
	if token != expectedToken {
		t.Errorf("unexpected token %v", token)
	}
	if masterToken != expectedMasterToken {
		t.Errorf("unexpected master token %v", masterToken)
	}
	if sessionID != expectedSessionID {
		t.Errorf("unexpected session id %v", sessionID)
	}
}

func TestSimpleTokenAccessorGetTokensSynchronization(t *testing.T) {
	accessor := getSimpleTokenAccessor()
	var wg sync.WaitGroup
	failed := false
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			// set a random session and token
			session := rand.Int63()
			sessionStr := strconv.FormatInt(session, 10)
			accessor.SetTokens("t"+sessionStr, "m"+sessionStr, session)

			// read back session and token and verify that invariant still holds
			token, masterToken, session := accessor.GetTokens()
			sessionStr = strconv.FormatInt(session, 10)
			if "t"+sessionStr != token || "m"+sessionStr != masterToken {
				failed = true
			}
			wg.Done()
		}()
	}
	// wait for all competing goroutines to finish setting and getting tokens
	wg.Wait()
	if failed {
		t.Fail()
	}
}

func TestGetRequestIDFromContext(t *testing.T) {
	expectedRequestID := NewUUID()
	ctx := WithRequestID(context.Background(), expectedRequestID)
	requestID := getOrGenerateRequestIDFromContext(ctx)
	if requestID != expectedRequestID {
		t.Errorf("unexpected request id: %v, expected: %v", requestID, expectedRequestID)
	}
	ctx = WithRequestID(context.Background(), nilUUID)
	requestID = getOrGenerateRequestIDFromContext(ctx)
	if requestID == nilUUID {
		t.Errorf("unexpected request id, should not be nil")
	}
}

func TestGenerateRequestID(t *testing.T) {
	firstRequestID := getOrGenerateRequestIDFromContext(context.Background())
	otherRequestID := getOrGenerateRequestIDFromContext(context.Background())
	if firstRequestID == otherRequestID {
		t.Errorf("request id should not be the same")
	}
}

func TestIntMin(t *testing.T) {
	testcases := []tcIntMinMax{
		{1, 3, 1},
		{5, 100, 5},
		{321, 3, 3},
		{123, 123, 123},
	}
	for _, test := range testcases {
		t.Run(fmt.Sprintf("%v_%v_%v", test.v1, test.v2, test.out), func(t *testing.T) {
			a := intMin(test.v1, test.v2)
			if test.out != a {
				t.Errorf("failed int min. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
			}
		})
	}
}
func TestIntMax(t *testing.T) {
	testcases := []tcIntMinMax{
		{1, 3, 3},
		{5, 100, 100},
		{321, 3, 321},
		{123, 123, 123},
	}
	for _, test := range testcases {
		t.Run(fmt.Sprintf("%v_%v_%v", test.v1, test.v2, test.out), func(t *testing.T) {
			a := intMax(test.v1, test.v2)
			if test.out != a {
				t.Errorf("failed int max. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
			}
		})
	}
}

type tcDurationMinMax struct {
	v1  time.Duration
	v2  time.Duration
	out time.Duration
}

func TestDurationMin(t *testing.T) {
	testcases := []tcDurationMinMax{
		{1 * time.Second, 3 * time.Second, 1 * time.Second},
		{5 * time.Second, 100 * time.Second, 5 * time.Second},
		{321 * time.Second, 3 * time.Second, 3 * time.Second},
		{123 * time.Second, 123 * time.Second, 123 * time.Second},
	}
	for _, test := range testcases {
		t.Run(fmt.Sprintf("%v_%v_%v", test.v1, test.v2, test.out), func(t *testing.T) {
			a := durationMin(test.v1, test.v2)
			if test.out != a {
				t.Errorf("failed duratoin max. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
			}
		})
	}
}

func TestDurationMax(t *testing.T) {
	testcases := []tcDurationMinMax{
		{1 * time.Second, 3 * time.Second, 3 * time.Second},
		{5 * time.Second, 100 * time.Second, 100 * time.Second},
		{321 * time.Second, 3 * time.Second, 321 * time.Second},
		{123 * time.Second, 123 * time.Second, 123 * time.Second},
	}
	for _, test := range testcases {
		t.Run(fmt.Sprintf("%v_%v_%v", test.v1, test.v2, test.out), func(t *testing.T) {
			a := durationMax(test.v1, test.v2)
			if test.out != a {
				t.Errorf("failed duratoin max. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
			}
		})
	}
}

type tcNamedValues struct {
	values []driver.Value
	out    []driver.NamedValue
}

func compareNamedValues(v1 []driver.NamedValue, v2 []driver.NamedValue) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}
	if len(v1) != len(v2) {
		return false
	}
	for i := range v1 {
		if v1[i] != v2[i] {
			return false
		}
	}
	return true
}

func TestToNamedValues(t *testing.T) {
	testcases := []tcNamedValues{
		{
			values: []driver.Value{},
			out:    []driver.NamedValue{},
		},
		{
			values: []driver.Value{1},
			out:    []driver.NamedValue{{Name: "", Ordinal: 1, Value: 1}},
		},
		{
			values: []driver.Value{1, "test1", 9.876, nil},
			out: []driver.NamedValue{
				{Name: "", Ordinal: 1, Value: 1},
				{Name: "", Ordinal: 2, Value: "test1"},
				{Name: "", Ordinal: 3, Value: 9.876},
				{Name: "", Ordinal: 4, Value: nil}},
		},
	}
	for _, test := range testcases {
		t.Run("", func(t *testing.T) {
			a := toNamedValues(test.values)

			if !compareNamedValues(test.out, a) {
				t.Errorf("failed int max. v1: %v, v2: %v, expected: %v, got: %v", test.values, test.out, test.out, a)
			}
		})
	}
}

type tcIntArrayMin struct {
	in  []int
	out int
}

func TestGetMin(t *testing.T) {
	testcases := []tcIntArrayMin{
		{[]int{1, 2, 3, 4, 5}, 1},
		{[]int{10, 25, 15, 5, 20}, 5},
		{[]int{15, 12, 9, 6, 3}, 3},
		{[]int{123, 123, 123, 123, 123}, 123},
		{[]int{}, -1},
	}
	for _, test := range testcases {
		t.Run(fmt.Sprintf("%v", test.out), func(t *testing.T) {
			a := getMin(test.in)
			if test.out != a {
				t.Errorf("failed get min. in: %v, expected: %v, got: %v", test.in, test.out, a)
			}
		})
	}
}

type tcURLList struct {
	in  string
	out bool
}

func TestValidURL(t *testing.T) {
	testcases := []tcURLList{
		{"https://ssoTestURL.okta.com", true},
		{"https://ssoTestURL.okta.com:8080", true},
		{"https://ssoTestURL.okta.com/testpathvalue", true},
		{"-a calculator", false},
		{"This is a random test", false},
		{"file://TestForFile", false},
	}
	for _, test := range testcases {
		t.Run(test.in, func(t *testing.T) {
			result := isValidURL(test.in)
			if test.out != result {
				t.Errorf("Failed to validate URL, input :%v, expected: %v, got: %v", test.in, test.out, result)
			}
		})
	}
}

type tcEncodeList struct {
	in  string
	out string
}

func TestEncodeURL(t *testing.T) {
	testcases := []tcEncodeList{
		{"Hello @World", "Hello+%40World"},
		{"Test//String", "Test%2F%2FString"},
	}

	for _, test := range testcases {
		t.Run(test.in, func(t *testing.T) {
			result := urlEncode(test.in)
			if test.out != result {
				t.Errorf("Failed to encode string, input %v, expected: %v, got: %v", test.in, test.out, result)
			}
		})
	}
}

func TestParseUUID(t *testing.T) {
	testcases := []tcUUID{
		{"6ba7b812-9dad-11d1-80b4-00c04fd430c8"},
		{"00302010-0504-0706-0809-0a0b0c0d0e0f"},
	}

	for _, test := range testcases {
		t.Run(test.uuid, func(t *testing.T) {
			requestID := ParseUUID(test.uuid)
			if requestID.String() != test.uuid {
				t.Fatalf("failed to parse uuid")
			}
		})
	}
}

type tcEscapeCsv struct {
	in  string
	out string
}

func TestEscapeForCSV(t *testing.T) {
	testcases := []tcEscapeCsv{
		{"", "\"\""},
		{"\n", "\"\n\""},
		{"test\\", "\"test\\\""},
	}

	for _, test := range testcases {
		t.Run(test.out, func(t *testing.T) {
			result := escapeForCSV(test.in)
			if test.out != result {
				t.Errorf("Failed to escape string, input %v, expected: %v, got: %v", test.in, test.out, result)
			}
		})
	}
}

func TestGetFromEnv(t *testing.T) {
	os.Setenv("SF_TEST", "test")
	defer os.Unsetenv("SF_TEST")
	result, err := GetFromEnv("SF_TEST", true)

	if err != nil {
		t.Error("failed to read SF_TEST environment variable")
	}
	if result != "test" {
		t.Errorf("incorrect value read for SF_TEST. Expected: test, read %v", result)
	}
}

func TestGetFromEnvFailOnMissing(t *testing.T) {
	_, err := GetFromEnv("SF_TEST_MISSING", true)
	if err == nil {
		t.Error("should report error when there is missing env parameter")
	}
}

type tcContains[T comparable] struct {
	arr      []T
	e        T
	expected bool
}

func TestContains(t *testing.T) {
	performContainsTestcase(tcContains[int]{[]int{1, 2, 3, 5}, 4, false}, t)
	performContainsTestcase(tcContains[string]{[]string{"a", "b", "C", "F"}, "C", true}, t)
	performContainsTestcase(tcContains[int]{[]int{1, 2, 3, 5}, 2, true}, t)
	performContainsTestcase(tcContains[string]{[]string{"a", "b", "C", "F"}, "f", false}, t)
}

func performContainsTestcase[S comparable](tc tcContains[S], t *testing.T) {
	result := contains(tc.arr, tc.e)
	if result != tc.expected {
		t.Errorf("contains failed; arr: %v, e: %v, should be %v but was %v", tc.arr, tc.e, tc.expected, result)
	}
}

func skipOnJenkins(t *testing.T, message string) {
	if os.Getenv("JENKINS_HOME") != "" {
		t.Skip("Skipping test on Jenkins: " + message)
	}
}

func runOnlyOnDockerContainer(t *testing.T, message string) {
	if os.Getenv("AUTHENTICATION_TESTS_ENV") == "" {
		t.Skip("Running only on Docker container: " + message)
	}
}

func skipOnMac(t *testing.T, reason string) {
	if runtime.GOOS == "darwin" && runningOnGithubAction() {
		t.Skip("skipped on Mac: " + reason)
	}
}

func randomString(n int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	alpha := []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, n)
	for i := range b {
		b[i] = alpha[r.Intn(len(alpha))]
	}
	return string(b)
}

func TestWithLowerKeys(t *testing.T) {
	m := make(map[string]string)
	m["abc"] = "def"
	m["GHI"] = "KLM"
	lowerM := withLowerKeys(m)
	assertEqualE(t, lowerM["abc"], "def")
	assertEqualE(t, lowerM["ghi"], "KLM")
}

func TestFindByPrefix(t *testing.T) {
	nonEmpty := []string{"aaa", "bbb", "ccc"}
	assertEqualE(t, findByPrefix(nonEmpty, "a"), 0)
	assertEqualE(t, findByPrefix(nonEmpty, "aa"), 0)
	assertEqualE(t, findByPrefix(nonEmpty, "aaa"), 0)
	assertEqualE(t, findByPrefix(nonEmpty, "bb"), 1)
	assertEqualE(t, findByPrefix(nonEmpty, "ccc"), 2)
	assertEqualE(t, findByPrefix(nonEmpty, "dd"), -1)
	assertEqualE(t, findByPrefix([]string{}, "dd"), -1)
}

func TestInternal(t *testing.T) {
	ctx := context.Background()
	assertFalseE(t, isInternal(ctx))
	ctx = WithInternal(ctx)
	assertTrueE(t, isInternal(ctx))
}
