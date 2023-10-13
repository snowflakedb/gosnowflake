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
		Name             string
		FileName         string
		FileContents     string
		ExpectedLogLevel string
		ExpectedLogPath  string
	}{
		{
			Name:     "TestWithLogLevelUpperCase",
			FileName: "config_1.json",
			FileContents: `{
				"common": {
					"log_level" : "INFO",
					"log_path" : "/some-path/some-directory"
				}
			}`,
			ExpectedLogLevel: "INFO",
			ExpectedLogPath:  "/some-path/some-directory",
		},
		{
			Name:     "TestWithLogLevelLowerCase",
			FileName: "config_2.json",
			FileContents: `{
				"common": {
					"log_level" : "info",
					"log_path" : "/some-path/some-directory"
				}
			}`,
			ExpectedLogLevel: "info",
			ExpectedLogPath:  "/some-path/some-directory",
		},
		{
			Name:     "TestWithMissingValues",
			FileName: "config_3.json",
			FileContents: `{
				"common": {}
			}`,
			ExpectedLogLevel: "",
			ExpectedLogPath:  "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			fileName := CreateFile(t, tc.FileName, tc.FileContents, dir)

			config, err := parseClientConfiguration(fileName)

			if err != nil {
				t.Fatalf("Error should be nil but was %s", err)
			}
			if config.Common.LogLevel != tc.ExpectedLogLevel {
				t.Errorf("Log level should be %s but was %s", tc.ExpectedLogLevel, config.Common.LogLevel)
			}
			if config.Common.LogPath != tc.ExpectedLogPath {
				t.Errorf("Log path should be %s but was %s", tc.ExpectedLogPath, config.Common.LogPath)
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
			fileName := CreateFile(t, fmt.Sprintf("config_%s.json", logLevel), fileContents, dir)

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
		Name                          string
		FileName                      string
		FileContents                  string
		ExpectedErrorMessageToContain string
	}{
		{
			Name:     "TestWithWrongLogLevel",
			FileName: "config_1.json",
			FileContents: `{
				"common": {
					"log_level" : "something weird",
					"log_path" : "/some-path/some-directory"
				}
			}`,
			ExpectedErrorMessageToContain: "unknown log level",
		},
		{
			Name:     "TestWithWrongTypeOfLogLevel",
			FileName: "config_2.json",
			FileContents: `{
				"common": {
					"log_level" : 15,
					"log_path" : "/some-path/some-directory"
				}
			}`,
			ExpectedErrorMessageToContain: "ClientConfigCommonProps.common.log_level",
		},
		{
			Name:     "TestWithWrongTypeOfLogPath",
			FileName: "config_3.json",
			FileContents: `{
				"common": {
					"log_level" : "INFO",
					"log_path" : true
				}
			}`,
			ExpectedErrorMessageToContain: "ClientConfigCommonProps.common.log_path",
		},
		{
			Name:                          "TestWithoutCommon",
			FileName:                      "config_4.json",
			FileContents:                  "{}",
			ExpectedErrorMessageToContain: "common section in client config not found",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			fileName := CreateFile(t, tc.FileName, tc.FileContents, dir)

			_, err := parseClientConfiguration(fileName)

			if err == nil {
				t.Fatal("Error should not be nil but was nil")
			}
			errMessage := fmt.Sprint(err)
			expectedPrefix := "parsing client config failed"
			if !strings.HasPrefix(errMessage, expectedPrefix) {
				t.Errorf("Error message: \"%s\" should start with prefix: \"%s\"", errMessage, expectedPrefix)
			}
			if !strings.Contains(errMessage, tc.ExpectedErrorMessageToContain) {
				t.Errorf("Error message: \"%s\" should contain given phrase: \"%s\"", errMessage, tc.ExpectedErrorMessageToContain)
			}
		})
	}
}

func CreateFile(t *testing.T, fileName string, fileContents string, directory string) string {
	fullFileName := path.Join(directory, fileName)
	writeErr := os.WriteFile(fullFileName, []byte(fileContents), 0644)
	if writeErr != nil {
		t.Fatal("Could not create file")
	}
	return fullFileName
}
