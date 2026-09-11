package gosnowflake

import "testing"

func TestQueryResultStatusRestartedIsRunning(t *testing.T) {
	if !SFQueryRestarted.isRunning() {
		t.Fatal("RESTARTED should be treated as still running")
	}
	if SFQueryRestarted.isError() {
		t.Fatal("RESTARTED should not be treated as an error status")
	}
}
