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

func TestPruneBySessionValue(t *testing.T) {
	qce1 := queryContextEntry{1, 1, 1, nil}
	qce2 := queryContextEntry{2, 2, 2, nil}
	qce3 := queryContextEntry{3, 3, 3, nil}

	testcases := []struct {
		size     string
		expected []queryContextEntry
	}{
		{
			size:     "1",
			expected: []queryContextEntry{qce1},
		},
		{
			size:     "2",
			expected: []queryContextEntry{qce1, qce2},
		},
		{
			size:     "3",
			expected: []queryContextEntry{qce1, qce2, qce3},
		},
		{
			size:     "4",
			expected: []queryContextEntry{qce1, qce2, qce3},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.size, func(t *testing.T) {
			sc := &snowflakeConn{
				cfg: &Config{
					Params: map[string]*string{
						queryContextCacheSizeParamName: &tc.size,
					},
				},
			}

			qcc := (&queryContextCache{}).init()

			qcc.add(sc, qce1)
			qcc.add(sc, qce2)
			qcc.add(sc, qce3)

			if !reflect.DeepEqual(qcc.entries, tc.expected) {
				t.Errorf("unexpected cache entries. expected: %v, got: %v", tc.expected, qcc.entries)
			}
		})
	}
}

func TestPruneByDefaultValue(t *testing.T) {
	qce1 := queryContextEntry{1, 1, 1, nil}
	qce2 := queryContextEntry{2, 2, 2, nil}
	qce3 := queryContextEntry{3, 3, 3, nil}
	qce4 := queryContextEntry{4, 4, 4, nil}
	qce5 := queryContextEntry{5, 5, 5, nil}
	qce6 := queryContextEntry{6, 6, 6, nil}

	sc := &snowflakeConn{
		cfg: &Config{
			Params: map[string]*string{},
		},
	}

	qcc := (&queryContextCache{}).init()
	qcc.add(sc, qce1)
	qcc.add(sc, qce2)
	qcc.add(sc, qce3)
	qcc.add(sc, qce4)
	qcc.add(sc, qce5)

	if len(qcc.entries) != 5 {
		t.Fatalf("Expected 5 elements, got: %v", len(qcc.entries))
	}

	qcc.add(sc, qce6)
	if len(qcc.entries) != 5 {
		t.Fatalf("Expected 5 elements, got: %v", len(qcc.entries))
	}
}

func TestNoQcesClearsCache(t *testing.T) {
	qce1 := queryContextEntry{1, 1, 1, nil}

	sc := &snowflakeConn{
		cfg: &Config{
			Params: map[string]*string{},
		},
	}

	qcc := (&queryContextCache{}).init()
	qcc.add(sc, qce1)

	if len(qcc.entries) != 1 {
		t.Fatalf("improperly inited cache")
	}

	qcc.add(sc)

	if len(qcc.entries) != 0 {
		t.Errorf("after adding empty context list cache should be cleared")
	}
}
