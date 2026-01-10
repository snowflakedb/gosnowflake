package gosnowflake

import (
	"os"
	"testing"
	"time"
)

// TestOcspCacheClearerNoRaceCondition tests that the stop() method properly
// synchronizes with the goroutine and doesn't have a race condition.
// This test verifies the fix for issue #1669.
func TestOcspCacheClearerNoRaceCondition(t *testing.T) {
	// Save original environment variable
	origValue := os.Getenv(ocspResponseCacheClearingIntervalInSecondsEnv)
	defer func() {
		StopOCSPCacheClearer()
		os.Setenv(ocspResponseCacheClearingIntervalInSecondsEnv, origValue)
		initOCSPCache()
		StartOCSPCacheClearer()
	}()

	// Set a short interval for testing
	os.Setenv(ocspResponseCacheClearingIntervalInSecondsEnv, "10")

	// Start the cache clearer
	StartOCSPCacheClearer()

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Stop it - this should block until the goroutine acknowledges
	StopOCSPCacheClearer()

	// Verify it's actually stopped by checking the running flag
	ocspCacheClearer.mu.Lock()
	running := ocspCacheClearer.running
	ocspCacheClearer.mu.Unlock()

	if running {
		t.Fatal("Cache clearer should be stopped but is still running")
	}

	// Try to stop again - should be a no-op since it's not running
	StopOCSPCacheClearer()

	// Start and stop multiple times to ensure no goroutine leaks
	for i := 0; i < 5; i++ {
		StartOCSPCacheClearer()
		time.Sleep(50 * time.Millisecond)
		StopOCSPCacheClearer()

		ocspCacheClearer.mu.Lock()
		running := ocspCacheClearer.running
		ocspCacheClearer.mu.Unlock()

		if running {
			t.Fatalf("Iteration %d: Cache clearer should be stopped but is still running", i)
		}
	}
}

