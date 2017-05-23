// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"strings"
	"testing"
)

func TestErrorMessage(t *testing.T) {
	var e error
	e = &SnowflakeError{
		Number:  1,
		Message: "test message",
	}
	if !strings.Contains(e.Error(), "test message") {
		t.Errorf("failed to format error. %v", e)
	}
	e = &SnowflakeError{
		Number:      1,
		Message:     "test message: %v, %v",
		MessageArgs: []interface{}{"C1", "C2"},
	}
	if !strings.Contains(e.Error(), "test message") {
		t.Errorf("failed to format error. %v", e)
	}
}
