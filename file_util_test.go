package gosnowflake

import (
	"os/user"
	"path/filepath"
	"runtime"
	"testing"
)

func TestGetDigestAndSizeForInvalidDir(t *testing.T) {
	fileUtil := new(snowflakeFileUtil)
	digest, size, err := fileUtil.getDigestAndSizeForFile("/home/file.txt")
	if digest != "" {
		t.Fatal("should be empty")
	}
	if size != 0 {
		t.Fatal("should be 0")
	}
	if err == nil {
		t.Fatal("should have failed")
	}
}

type tcBaseName struct {
	in  string
	out string
}

func TestBaseName(t *testing.T) {
	testcases := []tcBaseName{
		{"/tmp", "tmp"},
		{"/home/desktop/.", ""},
		{"/home/desktop/..", ""},
		{".", ""},
		{"..", ""},
		{"/", ""},
		{"/home/desktop/", "desktop"},
		{"archive.tar.gz", "archive.tar.gz"},
		{"/path/to/archive.tar.gz", "archive.tar.gz"},
		{"trailing-dot.tar.gz.", "trailing-dot.tar.gz."},
		{"/path/to/trailing-dot.tar.gz.", "trailing-dot.tar.gz."},
	}

	for _, test := range testcases {
		t.Run(test.in, func(t *testing.T) {
			actual := baseName(test.in)
			assertEqualE(t, actual, test.out, "baseName(%q)", test.in)
		})
	}
}

func TestBaseNameWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows-specific path tests")
	}
	testcases := []tcBaseName{
		{`C:\Users\file.txt`, "file.txt"},
		{`C:\Users\`, "Users"},
		{`C:\`, `\`},
		{`C:\Users\trailing-dot.txt.`, "trailing-dot.txt."},
		{`C:\path\to\.`, ""},
		{`C:\path\to\..`, ""},
	}

	for _, test := range testcases {
		t.Run(test.in, func(t *testing.T) {
			actual := baseName(test.in)
			assertEqualE(t, actual, test.out, "baseName(%q)", test.in)
		})
	}
}

func TestExpandUser(t *testing.T) {
	skipOnMissingHome(t)
	usr, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	homeDir := usr.HomeDir
	user, err := expandUser("~")
	if err != nil {
		t.Fatal(err)
	}
	if homeDir != user {
		t.Fatalf("failed to expand user, expected: %v, got: %v", homeDir, user)
	}

	user, err = expandUser("~/storage")
	if err != nil {
		t.Fatal(err)
	}
	expectedPath := filepath.Join(homeDir, "storage")
	if expectedPath != user {
		t.Fatalf("failed to expand user, expected: %v, got: %v", expectedPath, user)
	}
}
