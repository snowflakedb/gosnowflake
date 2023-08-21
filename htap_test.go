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

func TestAddingQueryContextCacheEntry(t *testing.T) {
	runSnowflakeConnTest(t, func(sc *snowflakeConn) {
		t.Run("First query (may be on empty cache)", func(t *testing.T) {
			entriesBefore := sc.queryContextCache.entries
			if _, err := sc.Query("SELECT 1", nil); err != nil {
				t.Fatalf("cannot query. %v", err)
			}
			entriesAfter := sc.queryContextCache.entries

			if !containsNewEntries(entriesAfter, entriesBefore) {
				t.Error("no new entries added to the query context cache")
			}
		})

		t.Run("Second query (cache should not be empty)", func(t *testing.T) {
			entriesBefore := sc.queryContextCache.entries
			if len(entriesBefore) == 0 {
				t.Fatalf("cache should not be empty after first query")
			}
			if _, err := sc.Query("SELECT 1", nil); err != nil {
				t.Fatalf("cannot query. %v", err)
			}
			entriesAfter := sc.queryContextCache.entries

			if !containsNewEntries(entriesAfter, entriesBefore) {
				t.Error("no new entries added to the query context cache")
			}
		})
	})
}

func containsNewEntries(entriesAfter []queryContextEntry, entriesBefore []queryContextEntry) bool {
	if len(entriesAfter) > len(entriesBefore) {
		return true
	}

	for _, entryAfter := range entriesAfter {
		for _, entryBefore := range entriesBefore {
			if !reflect.DeepEqual(entryBefore, entryAfter) {
				return true
			}
		}
	}

	return false
}
