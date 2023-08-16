package gosnowflake

import (
	"os/user"
	"path/filepath"
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
	}

	for _, test := range testcases {
		t.Run(test.in, func(t *testing.T) {
			base := baseName(test.in)
			if test.out != base {
				t.Errorf("Failed to get base, input %v, expected: %v, got: %v", test.in, test.out, base)
			}
		})
	}
}

func TestExpandUser(t *testing.T) {
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
