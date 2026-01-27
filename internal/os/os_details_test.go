package os

import (
	"testing"
)

func TestReadOsRelease(t *testing.T) {
	result := readOsRelease("test_data/sample_os_release")
	if result == nil {
		t.Fatal("expected non-nil result from sample_os_release")
	}

	// Verify only allowed keys are parsed (8 keys expected)
	// Note: test file also contains lines with spaces only, spaces+tabs,
	// and comments - all should be ignored
	expectedEntries := map[string]string{
		"NAME":          "Ubuntu",
		"PRETTY_NAME":   "Ubuntu 22.04.3 LTS",
		"ID":            "ubuntu",
		"VERSION_ID":    "22.04",
		"VERSION":       "22.04.3 LTS (Jammy Jellyfish)",
		"BUILD_ID":      "20231115",
		"IMAGE_ID":      "ubuntu-jammy",
		"IMAGE_VERSION": "1.0.0",
	}

	// Check correct number of entries (no extra keys parsed)
	if len(result) != len(expectedEntries) {
		t.Errorf("expected %d entries, got %d. Result: %v", len(expectedEntries), len(result), result)
	}

	// Verify each expected entry
	for key, expectedValue := range expectedEntries {
		actualValue, exists := result[key]
		if !exists {
			t.Errorf("expected key %q not found in result", key)
			continue
		}
		if actualValue != expectedValue {
			t.Errorf("key %q: expected %q, got %q", key, expectedValue, actualValue)
		}
	}

	// Verify all keys are expected
	for key := range result {
		_, exists := expectedEntries[key]
		if !exists {
			t.Errorf("expected to not contain key %v", key)
		}
	}
}
