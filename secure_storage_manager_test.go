// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"errors"
	"io"
	"os"
	"runtime"
	"testing"
)

type tcTargets struct {
	host     string
	user     string
	credType string
	out      string
}

type tcCredentials struct {
	credType string
	token    string
}

func TestSetAndGetCredentialMfa(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("MacOS requires keychain password to be manually entered.")
	} else {
		fakeMfaToken := "fakeMfaToken"
		expectedMfaToken := "fakeMfaToken"
		sc := getDefaultSnowflakeConn()
		sc.cfg.Host = "testhost"
		setCredential(sc, mfaToken, fakeMfaToken)
		getCredential(sc, mfaToken)

		if sc.cfg.MfaToken != expectedMfaToken {
			t.Fatalf("Expected mfa token %v but got %v", expectedMfaToken, sc.cfg.MfaToken)
		}

		// delete credential and check it no longer exists
		deleteCredential(sc, mfaToken)
		getCredential(sc, mfaToken)
		if sc.cfg.MfaToken != "" {
			t.Fatalf("Expected mfa token to be empty but got %v", sc.cfg.MfaToken)
		}
	}
}

func TestSetAndGetCredentialIdToken(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("MacOS requires keychain password to be manually entered.")
	} else {
		fakeIDToken := "fakeIDToken"
		expectedIDToken := "fakeIDToken"
		sc := getDefaultSnowflakeConn()
		sc.cfg.Host = "testhost"
		setCredential(sc, idToken, fakeIDToken)
		getCredential(sc, idToken)

		if sc.cfg.IDToken != expectedIDToken {
			t.Fatalf("Expected id token %v but got %v", expectedIDToken, sc.cfg.IDToken)
		}

		// delete credential and check it no longer exists
		deleteCredential(sc, idToken)
		getCredential(sc, idToken)
		if sc.cfg.IDToken != "" {
			t.Fatalf("Expected id token to be empty but got %v", sc.cfg.IDToken)
		}
	}
}
func TestCreateCredentialCache(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("cannot write to github file system")
	}
	dirName, err := os.UserHomeDir()
	if err != nil {
		t.Error(err)
	}
	srcFileName := dirName + "/.cache/snowflake/temporary_credential.json"
	tmpFileName := srcFileName + "_tmp"
	dst, err := os.Create(tmpFileName)
	if err != nil {
		t.Error(err)
	}
	defer dst.Close()

	var src *os.File
	if _, err = os.Stat(srcFileName); errors.Is(err, os.ErrNotExist) {
		// file does not exist
		if err = os.MkdirAll(dirName+"/.cache/snowflake/", os.ModePerm); err != nil {
			t.Error(err)
		}
		if _, err = os.Create(srcFileName); err != nil {
			t.Error(err)
		}
	} else if err != nil {
		t.Error(err)
	} else {
		// file exists
		src, err = os.Open(srcFileName)
		if err != nil {
			t.Error(err)
		}
		defer src.Close()
		// copy original contents to temporary file
		if _, err = io.Copy(dst, src); err != nil {
			t.Error(err)
		}
		if err = os.Remove(srcFileName); err != nil {
			t.Error(err)
		}
	}

	createCredentialCacheDir()
	if _, err = os.Stat(srcFileName); errors.Is(err, os.ErrNotExist) {
		t.Error(err)
	} else if err != nil {
		t.Error(err)
	}

	// cleanup
	src, _ = os.Open(tmpFileName)
	defer src.Close()
	dst, _ = os.OpenFile(srcFileName, os.O_WRONLY, os.ModePerm)
	defer dst.Close()
	// copy temporary file contents back to original file
	if _, err = io.Copy(dst, src); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(tmpFileName); err != nil {
		t.Error(err)
	}
}

func TestStoreTemporaryCredental(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("cannot write to github file system")
	}

	testcases := []tcCredentials{
		{"mfaToken", "598ghFnjfh8BBgmf45mmhgkfRR45mgkt5"},
		{"IdToken", "090Arftf54Jk3gh57ggrVvf09lJa3DD"},
	}
	createCredentialCacheDir()
	if credCache == "" {
		t.Fatalf("failed to create credential cache")
	}
	sc := getDefaultSnowflakeConn()
	for _, test := range testcases {
		writeTemporaryCredential(sc, test.credType, test.token)
		target := convertTarget(sc.cfg.Host, sc.cfg.User, test.credType)
		_, ok := localCredCache[target]
		if !ok {
			t.Fatalf("failed to write credential to local cache")
		}
		tmpCred := readTemporaryCredential(sc, test.credType)
		if tmpCred == "" {
			t.Fatalf("failed to read credential from temporary cache")
		} else {
			deleteTemporaryCredential(sc, test.credType)
		}
	}
}

func TestConvertTarget(t *testing.T) {
	testcases := []tcTargets{
		{"testaccount.snowflakecomputing.com", "testuser", "mfaToken", "TESTACCOUNT.SNOWFLAKECOMPUTING.COM:TESTUSER:SNOWFLAKE-GO-DRIVER:MFATOKEN"},
		{"testaccount.snowflakecomputing.com", "testuser", "IdToken", "TESTACCOUNT.SNOWFLAKECOMPUTING.COM:TESTUSER:SNOWFLAKE-GO-DRIVER:IDTOKEN"},
	}
	for _, test := range testcases {
		target := convertTarget(test.host, test.user, test.credType)
		if target != test.out {
			t.Fatalf("failed to convert target. expected: %v, but got: %v", test.out, target)
		}
	}
}
