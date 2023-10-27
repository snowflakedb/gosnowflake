// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
)

func TestFindConfigFileFromConnectionParameters(t *testing.T) {
	dirs := createTestDirectories(t)
	connParameterConfigPath := createFile(t, "conn_parameters_config.json", "random content", dirs.dir)
	envConfigPath := createFile(t, "env_var_config.json", "random content", dirs.dir)
	t.Setenv(clientConfEnvName, envConfigPath)
	createFile(t, defaultConfigName, "random content", dirs.predefinedDir1)
	createFile(t, defaultConfigName, "random content", dirs.predefinedDir2)

	clientConfigFilePath, err := findClientConfigFilePath(connParameterConfigPath, predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, connParameterConfigPath, "config file path")
}

func TestFindConfigFileFromEnvVariable(t *testing.T) {
	dirs := createTestDirectories(t)
	envConfigPath := createFile(t, "env_var_config.json", "random content", dirs.dir)
	t.Setenv(clientConfEnvName, envConfigPath)
	createFile(t, defaultConfigName, "random content", dirs.predefinedDir1)
	createFile(t, defaultConfigName, "random content", dirs.predefinedDir2)

	clientConfigFilePath, err := findClientConfigFilePath("", predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, envConfigPath, "config file path")
}

func TestFindConfigFileFromFirstPredefinedDir(t *testing.T) {
	dirs := createTestDirectories(t)
	configPath := createFile(t, defaultConfigName, "random content", dirs.predefinedDir1)
	createFile(t, defaultConfigName, "random content", dirs.predefinedDir2)

	clientConfigFilePath, err := findClientConfigFilePath("", predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, configPath, "config file path")
}

func TestFindConfigFileFromSubsequentDirectoryIfNotFoundInPreviousOne(t *testing.T) {
	dirs := createTestDirectories(t)
	createFile(t, "wrong_file_name.json", "random content", dirs.predefinedDir1)
	configPath := createFile(t, defaultConfigName, "random content", dirs.predefinedDir2)

	clientConfigFilePath, err := findClientConfigFilePath("", predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, configPath, "config file path")
}

func TestNotFindConfigFileWhenNotDefined(t *testing.T) {
	dirs := createTestDirectories(t)
	createFile(t, "wrong_file_name.json", "random content", dirs.predefinedDir1)
	createFile(t, "wrong_file_name.json", "random content", dirs.predefinedDir2)

	clientConfigFilePath, err := findClientConfigFilePath("", predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, "", "config file path")
}

func TestCreatePredefinedDirs(t *testing.T) {
	homeDir, err := os.UserHomeDir()
	assertNilF(t, err, "get home dir error")

	locations := clientConfigPredefinedDirs()

	assertEqualF(t, len(locations), 3, "size")
	assertEqualE(t, locations[0], ".", "driver directory")
	assertEqualE(t, locations[1], homeDir, "home directory")
	assertEqualE(t, locations[2], os.TempDir(), "temp directory")
}

func TestGetClientConfig(t *testing.T) {
	dir := t.TempDir()
	fileName := "config.json"
	configContents := createClientConfigContent("INFO", "/some-path/some-directory")
	createFile(t, fileName, configContents, dir)
	filePath := path.Join(dir, fileName)

	clientConfigFilePath, err := getClientConfig(filePath)

	assertNilF(t, err)
	assertNotNilF(t, clientConfigFilePath)
	assertEqualE(t, clientConfigFilePath.Common.LogLevel, "INFO", "log level")
	assertEqualE(t, clientConfigFilePath.Common.LogPath, "/some-path/some-directory", "log path")
}

func TestNoResultForGetClientConfigWhenNoFileFound(t *testing.T) {
	clientConfigFilePath, err := getClientConfig("")

	assertNilF(t, err)
	assertNilF(t, clientConfigFilePath)
}

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
			testName:         "TestWithLogLevelUpperCase",
			fileName:         "config_1.json",
			fileContents:     createClientConfigContent("INFO", "/some-path/some-directory"),
			expectedLogLevel: "INFO",
			expectedLogPath:  "/some-path/some-directory",
		},
		{
			testName:         "TestWithLogLevelLowerCase",
			fileName:         "config_2.json",
			fileContents:     createClientConfigContent("info", "/some-path/some-directory"),
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

			assertNilF(t, err, "parse client configuration error")
			assertEqualE(t, config.Common.LogLevel, tc.expectedLogLevel, "log level")
			assertEqualE(t, config.Common.LogPath, tc.expectedLogPath, "log path")
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

			assertNilF(t, err, "parse client config error")
			assertEqualE(t, config.Common.LogLevel, logLevel, "log level")
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
			testName:                      "TestWithWrongLogLevel",
			fileName:                      "config_1.json",
			FileContents:                  createClientConfigContent("something weird", "/some-path/some-directory"),
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

			assertNotNilF(t, err, "parse client configuration error")
			errMessage := fmt.Sprint(err)
			expectedPrefix := "parsing client config failed"
			assertHasPrefixE(t, errMessage, expectedPrefix, "error message")
			assertStringContainsE(t, errMessage, tc.expectedErrorMessageToContain, "error message")
		})
	}
}

func createFile(t *testing.T, fileName string, fileContents string, directory string) string {
	fullFileName := path.Join(directory, fileName)
	err := os.WriteFile(fullFileName, []byte(fileContents), 0644)
	assertNilF(t, err, "create file error")
	return fullFileName
}

func createTestDirectories(t *testing.T) struct {
	dir            string
	predefinedDir1 string
	predefinedDir2 string
} {
	dir := t.TempDir()
	predefinedDir1 := path.Join(dir, "dir1")
	err := os.Mkdir(predefinedDir1, 0755)
	assertNilF(t, err, "predefined dir1 error")
	predefinedDir2 := path.Join(dir, "dir2")
	err = os.Mkdir(predefinedDir2, 0755)
	assertNilF(t, err, "predefined dir2 error")
	return struct {
		dir            string
		predefinedDir1 string
		predefinedDir2 string
	}{
		dir:            dir,
		predefinedDir1: predefinedDir1,
		predefinedDir2: predefinedDir2,
	}
}

func predefinedTestDirs(dirs struct {
	dir            string
	predefinedDir1 string
	predefinedDir2 string
}) []string {
	return []string{dirs.predefinedDir1, dirs.predefinedDir2}
}

func createClientConfigContent(logLevel string, logPath string) string {
	return fmt.Sprintf(`{
			"common": {
				"log_level" : "%s",
				"log_path" : "%s"
			}
		}`,
		logLevel,
		strings.ReplaceAll(logPath, "\\", "\\\\"),
	)
}
