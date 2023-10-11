// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"os"
	"path"
	"testing"
)

func TestFindConfigFileFromConnectionParameters(t *testing.T) {
	mock := existsFileCheckerMock{}
	mock.mockFileFromConnectionParameters()
	mock.mockFileFromEnvVariable(t)
	mock.mockFileFromDriverDir()
	mock.mockFileFromHomeDir(t)
	mock.mockFileFromTempDir()

	clientConfig, err := findClientConfig(mock, parameterConfigPath())

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, parameterConfigPath(), "config file path")
}

func TestFindConfigFileFromEnvVariable(t *testing.T) {
	mock := existsFileCheckerMock{}
	mock.mockFileFromEnvVariable(t)
	mock.mockFileFromDriverDir()
	mock.mockFileFromHomeDir(t)
	mock.mockFileFromTempDir()

	clientConfig, err := findClientConfig(mock, "")

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, envConfigPath(), "config file path")
}

func TestFindConfigFileFromDriverDirectory(t *testing.T) {
	mock := existsFileCheckerMock{}
	mock.mockFileFromDriverDir()
	mock.mockFileFromHomeDir(t)
	mock.mockFileFromTempDir()

	clientConfig, err := findClientConfig(mock, "")

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, driverConfigPath(), "config file path")
}

func TestFindConfigFileFromHomeDirectory(t *testing.T) {
	mock := existsFileCheckerMock{}
	mock.mockFileFromHomeDir(t)
	mock.mockFileFromTempDir()

	clientConfig, err := findClientConfig(mock, "")

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, homeConfigPath(t), "config file path")
}

func TestFindConfigFileFromTempDirectory(t *testing.T) {
	mock := existsFileCheckerMock{}
	mock.mockFileFromTempDir()

	clientConfig, err := findClientConfig(mock, "")

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, tempConfigPath(), "config file path")
}

func TestNotFindConfigFileWhenNotDefined(t *testing.T) {
	mock := existsFileCheckerMock{}

	clientConfig, err := findClientConfig(mock, "")

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, "", "config file path")
}

func TestNotFindNotExistingFile(t *testing.T) {
	exists, err := efc.existsFile("not-existing-file")

	assertNilF(t, err, "exists file error")
	assertEqualE(t, exists, false, "exists result")
}

func TestFindExistingFile(t *testing.T) {
	dir := t.TempDir()
	fileName := "config.json"
	createFile(t, fileName, "random content", dir)
	filePath := path.Join(dir, fileName)

	exists, err := efc.existsFile(filePath)

	assertNilF(t, err, "exists file error")
	assertEqualE(t, exists, true, "exists result")
}

func TestGetClientConfig(t *testing.T) {
	dir := t.TempDir()
	fileName := "config.json"
	configContents := `{
		"common": {
			"log_level" : "INFO",
			"log_path" : "/some-path/some-directory"
		}
	}`
	createFile(t, fileName, configContents, dir)
	filePath := path.Join(dir, fileName)

	clientConfig, err := getClientConfig(filePath)

	assertNilF(t, err)
	assertNotNilF(t, clientConfig)
	assertEqualE(t, clientConfig.Common.LogLevel, "INFO", "log level")
	assertEqualE(t, clientConfig.Common.LogPath, "/some-path/some-directory", "log path")
}

func TestNoResultForGetClientConfigWhenNoFileFound(t *testing.T) {
	clientConfig, err := getClientConfig("")

	assertNilF(t, err)
	assertNilF(t, clientConfig)
}

type existsFileCheckerMock struct {
	filePaths []string
}

func (e existsFileCheckerMock) existsFile(filePath string) (bool, error) {
	fileOnList := false
	for _, existingFilePath := range e.filePaths {
		if filePath == existingFilePath {
			fileOnList = true
		}
	}
	return fileOnList, nil
}

func (e *existsFileCheckerMock) mockFileFromConnectionParameters() {
	e.mockFile(parameterConfigPath())
}

func (e *existsFileCheckerMock) mockFileFromEnvVariable(t *testing.T) {
	envConfigPath := envConfigPath()
	t.Setenv(clientConfEnvName, envConfigPath)
	e.mockFile(envConfigPath)
}

func (e *existsFileCheckerMock) mockFileFromDriverDir() {
	e.mockFile(driverConfigPath())
}

func (e *existsFileCheckerMock) mockFileFromHomeDir(t *testing.T) {
	e.mockFile(homeConfigPath(t))
}

func (e *existsFileCheckerMock) mockFileFromTempDir() {
	e.mockFile(tempConfigPath())
}

func (e *existsFileCheckerMock) mockFile(filePath string) {
	e.filePaths = append(e.filePaths, filePath)
}

func parameterConfigPath() string {
	return path.Join("some-directory", "config.json")
}

func envConfigPath() string {
	return path.Join("some-other-directory", "config.json")
}

func driverConfigPath() string {
	return path.Join(".", defaultConfigName)
}

func tempConfigPath() string {
	return path.Join(os.TempDir(), defaultConfigName)
}

func homeConfigPath(t *testing.T) string {
	homeDir, err := os.UserHomeDir()
	assertNilF(t, err)
	return path.Join(homeDir, defaultConfigName)
}
