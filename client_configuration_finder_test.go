// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"os"
	"path"
	"testing"
)

func TestFindConfigFileFromConnectionParameters(t *testing.T) {
	dir := t.TempDir()
	connParameterConfigPath := createFile(t, "conn_parameters_config.json", "random content", dir)
	envConfigPath := createFile(t, "env_var_config.json", "random content", dir)
	t.Setenv(clientConfEnvName, envConfigPath)
	driverConfigPath := createFile(t, "driver_dir_config.json", "random content", dir)
	homeConfigPath := createFile(t, "home_dir_config.json", "random content", dir)
	tempConfigPath := createFile(t, "temp_dir_config.json", "random content", dir)
	dirPredefinedPaths := []string{driverConfigPath, homeConfigPath, tempConfigPath}

	clientConfig, err := findClientConfig(connParameterConfigPath, dirPredefinedPaths)

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, connParameterConfigPath, "config file path")
}

func TestFindConfigFileFromEnvVariable(t *testing.T) {
	dir := t.TempDir()
	envConfigPath := createFile(t, "env_var_config.json", "random content", dir)
	t.Setenv(clientConfEnvName, envConfigPath)
	driverConfigPath := createFile(t, "driver_dir_config.json", "random content", dir)
	homeConfigPath := createFile(t, "home_dir_config.json", "random content", dir)
	tempConfigPath := createFile(t, "temp_dir_config.json", "random content", dir)
	dirPredefinedPaths := []string{driverConfigPath, homeConfigPath, tempConfigPath}

	clientConfig, err := findClientConfig("", dirPredefinedPaths)

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, envConfigPath, "config file path")
}

func TestFindConfigFileFromDriverDirectory(t *testing.T) {
	dir := t.TempDir()
	driverConfigPath := createFile(t, "driver_dir_config.json", "random content", dir)
	homeConfigPath := createFile(t, "home_dir_config.json", "random content", dir)
	tempConfigPath := createFile(t, "temp_dir_config.json", "random content", dir)
	dirPredefinedPaths := []string{driverConfigPath, homeConfigPath, tempConfigPath}

	clientConfig, err := findClientConfig("", dirPredefinedPaths)

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, driverConfigPath, "config file path")
}

func TestFindConfigFileFromHomeDirectory(t *testing.T) {
	dir := t.TempDir()
	driverConfigPath := path.Join(dir, "driver_dir_config.json")
	homeConfigPath := createFile(t, "home_dir_config.json", "random content", dir)
	tempConfigPath := createFile(t, "temp_dir_config.json", "random content", dir)
	dirPredefinedPaths := []string{driverConfigPath, homeConfigPath, tempConfigPath}

	clientConfig, err := findClientConfig("", dirPredefinedPaths)

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, homeConfigPath, "config file path")
}

func TestFindConfigFileFromTempDirectory(t *testing.T) {
	dir := t.TempDir()
	driverConfigPath := path.Join(dir, "driver_dir_config.json")
	homeConfigPath := path.Join(dir, "home_dir_config.json")
	tempConfigPath := createFile(t, "temp_dir_config.json", "random content", dir)
	dirPredefinedPaths := []string{driverConfigPath, homeConfigPath, tempConfigPath}

	clientConfig, err := findClientConfig("", dirPredefinedPaths)

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, tempConfigPath, "config file path")
}

func TestNotFindConfigFileWhenNotDefined(t *testing.T) {
	dir := t.TempDir()
	driverConfigPath := path.Join(dir, "driver_dir_config.json")
	homeConfigPath := path.Join(dir, "home_dir_config.json")
	tempConfigPath := path.Join(dir, "temp_dir_config.json")
	dirPredefinedPaths := []string{driverConfigPath, homeConfigPath, tempConfigPath}

	clientConfig, err := findClientConfig("", dirPredefinedPaths)

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfig, "", "config file path")
}

func TestCreatePredefinedLocationPaths(t *testing.T) {
	driverConfigPath := path.Join(".", defaultConfigName)
	homeDir, err := os.UserHomeDir()
	homeConfigPath := path.Join(homeDir, defaultConfigName)
	tempConfigPath := path.Join(os.TempDir(), defaultConfigName)
	var locations []string

	locations, err = clientConfigPredefinedFilePaths()

	assertNilF(t, err, "error")
	assertEqualF(t, len(locations), 3, "size")
	assertEqualE(t, locations[0], driverConfigPath, "driver config path")
	assertEqualE(t, locations[1], homeConfigPath, "home config path")
	assertEqualE(t, locations[2], tempConfigPath, "temp config path")
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
