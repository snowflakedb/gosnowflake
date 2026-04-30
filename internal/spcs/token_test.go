package spcs

import (
	"context"
	"os"
	path "path/filepath"
	"testing"
)

func writeTempToken(t *testing.T, contents string) string {
	t.Helper()
	p := path.Join(t.TempDir(), "spcs_token")
	if err := os.WriteFile(p, []byte(contents), 0600); err != nil {
		t.Fatalf("failed to write temp token: %v", err)
	}
	return p
}

func TestReadSpcsTokenFromDiskEnvUnset(t *testing.T) {
	t.Setenv(RunningInsideEnv, "")

	p := writeTempToken(t, "should-not-be-read")

	got := readSpcsTokenFromDisk(context.Background(), p)
	if got != "" {
		t.Fatalf("expected empty string when env unset, got %q", got)
	}
}

func TestReadSpcsTokenFromDiskValid(t *testing.T) {
	t.Setenv(RunningInsideEnv, "1")
	p := writeTempToken(t, "abc123\n")

	got := readSpcsTokenFromDisk(context.Background(), p)
	if got != "abc123" {
		t.Fatalf("expected %q, got %q", "abc123", got)
	}
}

func TestReadSpcsTokenFromDiskSurroundingWhitespace(t *testing.T) {
	t.Setenv(RunningInsideEnv, "1")
	p := writeTempToken(t, " \t token \n")

	got := readSpcsTokenFromDisk(context.Background(), p)
	if got != "token" {
		t.Fatalf("expected %q, got %q", "token", got)
	}
}

func TestReadSpcsTokenFromDiskWhitespaceOnly(t *testing.T) {
	t.Setenv(RunningInsideEnv, "1")
	p := writeTempToken(t, "   \n\t")

	got := readSpcsTokenFromDisk(context.Background(), p)
	if got != "" {
		t.Fatalf("expected empty string for whitespace-only token, got %q", got)
	}
}

func TestReadSpcsTokenFromDiskEmptyFile(t *testing.T) {
	t.Setenv(RunningInsideEnv, "1")
	p := writeTempToken(t, "")

	got := readSpcsTokenFromDisk(context.Background(), p)
	if got != "" {
		t.Fatalf("expected empty string for empty file, got %q", got)
	}
}
