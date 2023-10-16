// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"fmt"
	"os"
	"path"
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

			assertNil(t, err, "parse client configuration error")
			assertEqual(t, config.Common.LogLevel, tc.expectedLogLevel, "log level")
			assertEqual(t, config.Common.LogPath, tc.expectedLogPath, "log path")
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

			assertNil(t, err, "parse client config error")
			assertEqual(t, config.Common.LogLevel, logLevel, "log level")
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

			assertNotNil(t, err, "parse client configuration error")
			errMessage := fmt.Sprint(err)
			expectedPrefix := "parsing client config failed"
			assertHasPrefix(t, errMessage, expectedPrefix, "error message")
			assertStringContains(t, errMessage, tc.expectedErrorMessageToContain, "error message")
		})
	}
}

func createFile(t *testing.T, fileName string, fileContents string, directory string) string {
	fullFileName := path.Join(directory, fileName)
	err := os.WriteFile(fullFileName, []byte(fileContents), 0644)
	assertNil(t, err, "create file error")
	return fullFileName
}
