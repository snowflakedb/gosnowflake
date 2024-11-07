// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"io"
	"net/http"
	"strconv"
	"testing"
)

/** This file contains helper functions for tests only. **/

func resetHTTPMocks(t *testing.T) {
	_, err := http.Post("http://localhost:12345/reset", "text/plain", nil)
	if err != nil {
		t.Fatalf("Cannot reset HTTP mocks")
	}
}

func getMocksInvocations(t *testing.T) int {
	resp, err := http.Get("http://localhost:12345/invocations")
	if err != nil {
		t.Fatal(err.Error())
	}
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err.Error())
	}
	ret, err := strconv.Atoi(string(bytes))
	if err != nil {
		t.Fatal(err.Error())
	}
	return ret
}
