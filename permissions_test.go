//go:build !windows

package gosnowflake

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path"
	"testing"
)

func TestConfigPermissions(t *testing.T) {
	testCases := []struct {
		filePerm int
		isValid  bool
	}{
		{filePerm: 0700, isValid: true},
		{filePerm: 0600, isValid: true},
		{filePerm: 0500, isValid: true},
		{filePerm: 0400, isValid: true},
		{filePerm: 0300, isValid: true},
		{filePerm: 0200, isValid: true},
		{filePerm: 0100, isValid: true},
		{filePerm: 0707, isValid: false},
		{filePerm: 0706, isValid: false},
		{filePerm: 0705, isValid: true},
		{filePerm: 0704, isValid: true},
		{filePerm: 0703, isValid: false},
		{filePerm: 0702, isValid: false},
		{filePerm: 0701, isValid: true},
		{filePerm: 0770, isValid: false},
		{filePerm: 0760, isValid: false},
		{filePerm: 0750, isValid: true},
		{filePerm: 0740, isValid: true},
		{filePerm: 0730, isValid: false},
		{filePerm: 0720, isValid: false},
		{filePerm: 0710, isValid: true},
	}

	oldMask := unix.Umask(0000)
	defer unix.Umask(oldMask)

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("0%o", tc.filePerm), func(t *testing.T) {
			tempFile := path.Join(t.TempDir(), fmt.Sprintf("filePerm_%o", tc.filePerm))
			err := os.WriteFile(tempFile, nil, os.FileMode(tc.filePerm))
			assertNilE(t, err)
			defer os.Remove(tempFile)
			err = validateCfgPerm(tempFile)
			if err != nil && tc.isValid {
				t.Error(err)
			}
		})
	}
}

func TestLogDirectoryPermissions(t *testing.T) {
	testCases := []struct {
		dirPerm       int
		limitedToUser bool
	}{
		{dirPerm: 0700, limitedToUser: true},
		{dirPerm: 0600, limitedToUser: false},
		{dirPerm: 0500, limitedToUser: false},
		{dirPerm: 0400, limitedToUser: false},
		{dirPerm: 0300, limitedToUser: false},
		{dirPerm: 0200, limitedToUser: false},
		{dirPerm: 0100, limitedToUser: false},
		{dirPerm: 0707, limitedToUser: false},
		{dirPerm: 0706, limitedToUser: false},
		{dirPerm: 0705, limitedToUser: false},
		{dirPerm: 0704, limitedToUser: false},
		{dirPerm: 0703, limitedToUser: false},
		{dirPerm: 0702, limitedToUser: false},
		{dirPerm: 0701, limitedToUser: false},
		{dirPerm: 0770, limitedToUser: false},
		{dirPerm: 0760, limitedToUser: false},
		{dirPerm: 0750, limitedToUser: false},
		{dirPerm: 0740, limitedToUser: false},
		{dirPerm: 0730, limitedToUser: false},
		{dirPerm: 0720, limitedToUser: false},
		{dirPerm: 0710, limitedToUser: false},
	}

	oldMask := unix.Umask(0000)
	defer unix.Umask(oldMask)

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("0%o", tc.dirPerm), func(t *testing.T) {
			tempDir := path.Join(t.TempDir(), fmt.Sprintf("filePerm_%o", tc.dirPerm))
			err := os.Mkdir(tempDir, os.FileMode(tc.dirPerm))
			assertNilE(t, err)
			defer os.Remove(tempDir)
			result, _, err := isDirAccessCorrect(tempDir)
			if err != nil && tc.limitedToUser {
				t.Error(err)
			}
			assertEqualE(t, result, tc.limitedToUser)
		})
	}
}
