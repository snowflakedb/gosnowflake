// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
)

func TestParseConfiguration(t *testing.T) {
	dir := t.TempDir()
	testCases := []struct {
		testName         string
		fileName         string
		fileContents     string
		expectedLogLevel string
		expectedLogPath  string
	}{
		{
			testName: "TestWithLogLevelUpperCase",
			fileName: "config_1.json",
			fileContents: `{
				"common": {
					"log_level" : "INFO",
					"log_path" : "/some-path/some-directory"
				}
			}`,
			expectedLogLevel: "INFO",
			expectedLogPath:  "/some-path/some-directory",
		},
		{
			testName: "TestWithLogLevelLowerCase",
			fileName: "config_2.json",
			fileContents: `{
				"common": {
					"log_level" : "info",
					"log_path" : "/some-path/some-directory"
				}
			}`,
			expectedLogLevel: "info",
			expectedLogPath:  "/some-path/some-directory",
		},
		{
			testName: "TestWithMissingValues",
			fileName: "config_3.json",
			fileContents: `{
				"common": {}
			}`,
			expectedLogLevel: "",
			expectedLogPath:  "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			fileName := createFile(t, tc.fileName, tc.fileContents, dir)

			config, err := parseClientConfiguration(fileName)

			if err != nil {
				t.Fatalf("Error should be nil but was %s", err)
			}
			if config.Common.LogLevel != tc.expectedLogLevel {
				t.Errorf("Log level should be %s but was %s", tc.expectedLogLevel, config.Common.LogLevel)
			}
			if config.Common.LogPath != tc.expectedLogPath {
				t.Errorf("Log path should be %s but was %s", tc.expectedLogPath, config.Common.LogPath)
			}
		})
	}
}

func TestParseAllLogLevels(t *testing.T) {
	dir := t.TempDir()
	for _, logLevel := range []string{"OFF", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"} {
		t.Run(logLevel, func(t *testing.T) {
			fileContents := fmt.Sprintf(`{
				"common": {
					"log_level" : "%s",
					"log_path" : "/some-path/some-directory"
				}
			}`, logLevel)
			fileName := createFile(t, fmt.Sprintf("config_%s.json", logLevel), fileContents, dir)

			config, err := parseClientConfiguration(fileName)

			if err != nil {
				t.Fatalf("Error should be nil but was: %s", err)
			}
			if config.Common.LogLevel != logLevel {
				t.Errorf("Log level should be %s but was %s", logLevel, config.Common.LogLevel)
			}
		})
	}
}

func TestParseConfigurationFails(t *testing.T) {
	dir := t.TempDir()
	testCases := []struct {
		testName                      string
		fileName                      string
		FileContents                  string
		expectedErrorMessageToContain string
	}{
		{
			testName: "TestWithWrongLogLevel",
			fileName: "config_1.json",
			FileContents: `{
				"common": {
					"log_level" : "something weird",
					"log_path" : "/some-path/some-directory"
				}
			}`,
			expectedErrorMessageToContain: "unknown log level",
		},
		{
			testName: "TestWithWrongTypeOfLogLevel",
			fileName: "config_2.json",
			FileContents: `{
				"common": {
					"log_level" : 15,
					"log_path" : "/some-path/some-directory"
				}
			}`,
			expectedErrorMessageToContain: "ClientConfigCommonProps.common.log_level",
		},
		{
			testName: "TestWithWrongTypeOfLogPath",
			fileName: "config_3.json",
			FileContents: `{
				"common": {
					"log_level" : "INFO",
					"log_path" : true
				}
			}`,
			expectedErrorMessageToContain: "ClientConfigCommonProps.common.log_path",
		},
		{
			testName:                      "TestWithoutCommon",
			fileName:                      "config_4.json",
			FileContents:                  "{}",
			expectedErrorMessageToContain: "common section in client config not found",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			fileName := createFile(t, tc.fileName, tc.FileContents, dir)

			_, err := parseClientConfiguration(fileName)

			if err == nil {
				t.Fatal("Error should not be nil but was nil")
			}
			errMessage := fmt.Sprint(err)
			expectedPrefix := "parsing client config failed"
			if !strings.HasPrefix(errMessage, expectedPrefix) {
				t.Errorf("Error message: \"%s\" should start with prefix: \"%s\"", errMessage, expectedPrefix)
			}
			if !strings.Contains(errMessage, tc.expectedErrorMessageToContain) {
				t.Errorf("Error message: \"%s\" should contain given phrase: \"%s\"", errMessage, tc.expectedErrorMessageToContain)
			}
		})
	}
}

func createFile(t *testing.T, fileName string, fileContents string, directory string) string {
	fullFileName := path.Join(directory, fileName)
	err := os.WriteFile(fullFileName, []byte(fileContents), 0644)
	if err != nil {
		t.Fatal("Could not create file")
	}
	return fullFileName
}
