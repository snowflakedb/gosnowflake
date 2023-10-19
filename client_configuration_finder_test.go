// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"os"
	"path"
	"testing"
)

func TestFindConfigFileFromConnectionParameters(t *testing.T) {
	dirs := createTestDirectories(t)
	connParameterConfigPath := createFile(t, "conn_parameters_config.json", "random content", dirs.dir)
	envConfigPath := createFile(t, "env_var_config.json", "random content", dirs.dir)
	t.Setenv(clientConfEnvName, envConfigPath)
	createFile(t, defaultConfigName, "random content", dirs.driverDir)
	createFile(t, defaultConfigName, "random content", dirs.homeDir)
	createFile(t, defaultConfigName, "random content", dirs.tempDir)

	clientConfigFilePath, err := findClientConfigFilePath(connParameterConfigPath, predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, connParameterConfigPath, "config file path")
}

func TestFindConfigFileFromEnvVariable(t *testing.T) {
	dirs := createTestDirectories(t)
	envConfigPath := createFile(t, "env_var_config.json", "random content", dirs.dir)
	t.Setenv(clientConfEnvName, envConfigPath)
	createFile(t, defaultConfigName, "random content", dirs.driverDir)
	createFile(t, defaultConfigName, "random content", dirs.homeDir)
	createFile(t, defaultConfigName, "random content", dirs.tempDir)

	clientConfigFilePath, err := findClientConfigFilePath("", predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, envConfigPath, "config file path")
}

func TestFindConfigFileFromDriverDirectory(t *testing.T) {
	dirs := createTestDirectories(t)
	driverConfigPath := createFile(t, defaultConfigName, "random content", dirs.driverDir)
	createFile(t, defaultConfigName, "random content", dirs.homeDir)
	createFile(t, defaultConfigName, "random content", dirs.tempDir)

	clientConfigFilePath, err := findClientConfigFilePath("", predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, driverConfigPath, "config file path")
}

func TestFindConfigFileFromHomeDirectory(t *testing.T) {
	dirs := createTestDirectories(t)
	createFile(t, "wrong_file_name.json", "random content", dirs.driverDir)
	homeConfigPath := createFile(t, defaultConfigName, "random content", dirs.homeDir)
	createFile(t, defaultConfigName, "random content", dirs.tempDir)

	clientConfigFilePath, err := findClientConfigFilePath("", predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, homeConfigPath, "config file path")
}

func TestFindConfigFileFromTempDirectory(t *testing.T) {
	dirs := createTestDirectories(t)
	createFile(t, "wrong_file_name.json", "random content", dirs.driverDir)
	createFile(t, "wrong_file_name.json", "random content", dirs.homeDir)
	tempConfigPath := createFile(t, defaultConfigName, "random content", dirs.tempDir)

	clientConfigFilePath, err := findClientConfigFilePath("", predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, tempConfigPath, "config file path")
}

func TestNotFindConfigFileWhenNotDefined(t *testing.T) {
	dirs := createTestDirectories(t)
	createFile(t, "wrong_file_name.json", "random content", dirs.driverDir)
	createFile(t, "wrong_file_name.json", "random content", dirs.homeDir)
	createFile(t, "wrong_file_name.json", "random content", dirs.tempDir)

	clientConfigFilePath, err := findClientConfigFilePath("", predefinedTestDirs(dirs))

	assertNilF(t, err, "get client config error")
	assertEqualE(t, clientConfigFilePath, "", "config file path")
}

func TestCreatePredefinedDirs(t *testing.T) {
	homeDir, err := os.UserHomeDir()
	assertNilF(t, err, "get home dir error")
	var locations []string

	locations, err = clientConfigPredefinedDirs()

	assertNilF(t, err, "error")
	assertEqualF(t, len(locations), 3, "size")
	assertEqualE(t, locations[0], ".", "driver directory")
	assertEqualE(t, locations[1], homeDir, "home directory")
	assertEqualE(t, locations[2], os.TempDir(), "temp directory")
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

func createTestDirectories(t *testing.T) struct {
	dir       string
	driverDir string
	homeDir   string
	tempDir   string
} {
	dir := t.TempDir()
	driverDir := path.Join(dir, "driver") // we pretend "." to be a folder inside t.TempDir() not to interfere with user's real directories
	err := os.Mkdir(driverDir, 0755)
	assertNilF(t, err, "make driver dir error")
	homeDir := path.Join(dir, "home") // we pretend home directory to be a folder inside t.TempDir() not to interfere with user's real directories
	err = os.Mkdir(homeDir, 0755)
	assertNilF(t, err, "make home dir error")
	tempDir := path.Join(dir, "temp") // we pretend temp directory to be a folder inside t.TempDir() not to interfere with user's real directories
	err = os.Mkdir(tempDir, 0755)
	assertNilF(t, err, "make temp dir error")
	return struct {
		dir       string
		driverDir string
		homeDir   string
		tempDir   string
	}{
		dir:       dir,
		driverDir: driverDir,
		homeDir:   homeDir,
		tempDir:   tempDir,
	}
}

func predefinedTestDirs(dirs struct {
	dir       string
	driverDir string
	homeDir   string
	tempDir   string
}) []string {
	return []string{dirs.driverDir, dirs.homeDir, dirs.tempDir}
}
