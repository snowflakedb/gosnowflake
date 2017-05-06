// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"database/sql/driver"
	"testing"
	"time"
)

type tcIntMinMax struct {
	v1  int
	v2  int
	out int
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

type tcIntMinMax64 struct {
	v1  int64
	v2  int64
	out int64
}

func TestIntMin64(t *testing.T) {
	testcases := []tcIntMinMax64{
		{1, 3, 1},
		{5, 100, 5},
		{321, 3, 3},
		{123, 123, 123},
	}
	for _, test := range testcases {
		a := intMin64(test.v1, test.v2)
		if test.out != a {
			t.Errorf("failed int64 min. v1: %v, v2: %v, expected: %v, got: %v", test.v1, test.v2, test.out, a)
		}
	}
}

type tcDurationMax struct {
	v1  time.Duration
	v2  time.Duration
	out time.Duration
}

func TestDurationMax(t *testing.T) {
	testcases := []tcDurationMax{
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
