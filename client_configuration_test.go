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

type PositiveTestCase struct {
	Name             string
	FileName         string
	FileContents     string
	ExpectedLogLevel *string
	ExpectedLogPath  *string
}

type NegativeTestCase struct {
	Name                          string
	FileName                      string
	FileContents                  string
	ExpectedErrorMessageToContain string
}

func TestThatParsesConfiguration(t *testing.T) {
	// given
	dir := CreateTempDirectory(t, "conf_parse_tests_")
	testCases := []PositiveTestCase{
		{
			Name:     "TestWithLogLevelUpperCase",
			FileName: "config.json",
			FileContents: `{
				"common": {
					"log_level" : "INFO",
					"log_path" : "/some-path/some-directory"
				}
			}`,
			ExpectedLogLevel: toStringPointer("INFO"),
			ExpectedLogPath:  toStringPointer("/some-path/some-directory"),
		},
		{
			Name:     "TestWithLogLevelLowerCase",
			FileName: "config.json",
			FileContents: `{
				"common": {
					"log_level" : "info",
					"log_path" : "/some-path/some-directory"
				}
			}`,
			ExpectedLogLevel: toStringPointer("info"),
			ExpectedLogPath:  toStringPointer("/some-path/some-directory"),
		},
		{
			Name:     "TestWithMissingValues",
			FileName: "config.json",
			FileContents: `{
				"common": {}
			}`,
			ExpectedLogLevel: nil,
			ExpectedLogPath:  nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			fileName := CreateFile(t, testCase.FileName, testCase.FileContents, dir)

			// when
			config, err := parseClientConfiguration(fileName)

			// then
			assert := assert.New(t)
			assert.Equal(nil, err, "Error should be nil")
			assert.Equal(testCase.ExpectedLogLevel, config.Common.LogLevel, "Log level should be as expected")
			assert.Equal(testCase.ExpectedLogPath, config.Common.LogPath, "Log path should be as expected")
		})
	}
}

func TestThatFailsToParse(t *testing.T) {
	// given
	dir := CreateTempDirectory(t, "conf_negative_parse_tests_")
	testCases := []NegativeTestCase{
		{
			Name:     "TestWithWrongLogLevel",
			FileName: "config.json",
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
			FileName: "config.json",
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
			FileName: "config.json",
			FileContents: `{
				"common": {
					"log_level" : "INFO",
					"log_path" : true
				}
			}`,
			ExpectedErrorMessageToContain: "ClientConfigCommonProps.common.log_path",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			fileName := CreateFile(t, testCase.FileName, testCase.FileContents, dir)

			// when
			_, err := parseClientConfiguration(fileName)

			// then
			assert := assert.New(t)
			assert.Equal(err != nil, true, "Error should not be nil")
			errMessage := fmt.Sprint(err)
			expectedPrefix := "parsing client config failed"
			assert.Equal(strings.HasPrefix(errMessage, expectedPrefix), true,
				fmt.Sprintf("Error message: \"%s\" should start with prefix: \"%s\"", errMessage, expectedPrefix))
			assert.Equal(strings.Contains(errMessage, testCase.ExpectedErrorMessageToContain), true,
				fmt.Sprintf("Error message: \"%s\" should contain given phrase: \"%s\"", errMessage, testCase.ExpectedErrorMessageToContain))
		})
	}
}

func toStringPointer(value string) *string {
	var copyOfValue = value
	return &copyOfValue
}

func CreateFile(t *testing.T, fileName string, fileContents string, directory string) string {
	fullFileName := path.Join(directory, fileName)
	writeErr := os.WriteFile(fullFileName, []byte(fileContents), 0644)
	if writeErr != nil {
		t.Error("Could not create file")
	}
	return fullFileName
}

func CreateTempDirectory(t *testing.T, dirPattern string) string {
	dir, dirErr := os.MkdirTemp(os.TempDir(), dirPattern)
	if dirErr != nil {
		t.Error("Failed to create test directory")
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}
