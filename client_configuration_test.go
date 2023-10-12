// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"fmt"
	"github.com/stretchr/testify/assert"
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

			assert := assert.New(t)
			assert.Equal(nil, err, "Error should be nil")
			assert.Equal(tc.ExpectedLogLevel, config.Common.LogLevel, "Log level should be as expected")
			assert.Equal(tc.ExpectedLogPath, config.Common.LogPath, "Log path should be as expected")
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

			assert := assert.New(t)
			assert.Equal(nil, err, "Error should be nil")
			assert.Equal(logLevel, config.Common.LogLevel, "Log level should be as expected")
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

			assert := assert.New(t)
			assert.Equal(err != nil, true, "Error should not be nil")
			errMessage := fmt.Sprint(err)
			expectedPrefix := "parsing client config failed"
			assert.Equal(strings.HasPrefix(errMessage, expectedPrefix), true,
				fmt.Sprintf("Error message: \"%s\" should start with prefix: \"%s\"", errMessage, expectedPrefix))
			assert.Equal(strings.Contains(errMessage, tc.ExpectedErrorMessageToContain), true,
				fmt.Sprintf("Error message: \"%s\" should contain given phrase: \"%s\"", errMessage, tc.ExpectedErrorMessageToContain))
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
