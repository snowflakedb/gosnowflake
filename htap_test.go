package gosnowflake

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestMarshallAndDecodeOpaqueContext(t *testing.T) {
	testcases := []struct {
		json string
		qc   queryContextEntry
	}{
		{
			json: `{
				"id": 1,
				"timestamp": 2,
				"priority": 3
			}`,
			qc: queryContextEntry{1, 2, 3, nil},
		},
		{
			json: `{
				"id": 1,
				"timestamp": 2,
				"priority": 3,
				"context": "abc"
			}`,
			qc: queryContextEntry{1, 2, 3, "abc"},
		},
		{
			json: `{
				"id": 1,
				"timestamp": 2,
				"priority": 3,
				"context": {
					"val": "abc"
				}
			}`,
			qc: queryContextEntry{1, 2, 3, map[string]interface{}{"val": "abc"}},
		},
		{
			json: `{
				"id": 1,
				"timestamp": 2,
				"priority": 3,
				"context": [
					"abc"
				]
			}`,
			qc: queryContextEntry{1, 2, 3, []any{"abc"}},
		},
		{
			json: `{
				"id": 1,
				"timestamp": 2,
				"priority": 3,
				"context": [
					{
						"val": "abc"
					}
				]
			}`,
			qc: queryContextEntry{1, 2, 3, []any{map[string]interface{}{"val": "abc"}}},
		},
	}

	for _, tc := range testcases {
		t.Run(trimWhitespaces(tc.json), func(t *testing.T) {
			var qc queryContextEntry

			err := json.NewDecoder(strings.NewReader(tc.json)).Decode(&qc)
			if err != nil {
				t.Fatalf("failed to decode json. %v", err)
			}

			if !reflect.DeepEqual(tc.qc, qc) {
				t.Errorf("failed to decode json. expected: %v, got: %v", tc.qc, qc)
			}

			bytes, err := json.Marshal(qc)
			if err != nil {
				t.Fatalf("failed to encode json. %v", err)
			}

			resultJSON := string(bytes)
			if resultJSON != trimWhitespaces(tc.json) {
				t.Errorf("failed to encode json. epxected: %v, got: %v", trimWhitespaces(tc.json), resultJSON)
			}
		})
	}
}

func trimWhitespaces(s string) string {
	return strings.ReplaceAll(
		strings.ReplaceAll(
			strings.ReplaceAll(s, "\t", ""),
			" ", ""),
		"\n", "",
	)
}
