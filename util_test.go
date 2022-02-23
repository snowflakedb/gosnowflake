// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"math/rand"
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
		a := intMin(test.v1, test.v2)
		if test.out != a {
			t.Errorf("failed int min. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
		}
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
		a := intMax(test.v1, test.v2)
		if test.out != a {
			t.Errorf("failed int max. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
		}
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
		a := durationMin(test.v1, test.v2)
		if test.out != a {
			t.Errorf("failed duratoin max. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
		}
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
		a := durationMax(test.v1, test.v2)
		if test.out != a {
			t.Errorf("failed duratoin max. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
		}
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
		a := toNamedValues(test.values)

		if !compareNamedValues(test.out, a) {
			t.Errorf("failed int max. v1: %v, v2: %v, expected: %v, got: %v", test.values, test.out, test.out, a)
		}
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
	}
	for _, test := range testcases {
		a := getMin(test.in)
		if test.out != a {
			t.Errorf("failed get min. in: %v, expected: %v, got: %v", test.in, test.out, a)
		}
	}
}
