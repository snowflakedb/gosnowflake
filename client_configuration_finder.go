// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"errors"
	"fmt"
	"os"
	"path"
)

const (
	defaultConfigName = "sf_client_config.json"
	clientConfEnvName = "SF_CLIENT_CONFIG_FILE"
)

type existsFileChecker interface {
	existsFile(filePath string) (bool, error)
}

type existsFileCheckerImpl struct {
}

func (existsFileCheckerImpl) existsFile(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

var efc = existsFileCheckerImpl{}

func getClientConfig(filePathFromConnectionString string) (*ClientConfig, error) {
	filePath, err := findClientConfig(efc, filePathFromConnectionString)
	if err != nil {
		return nil, err
	}
	if filePath == "" {
		return nil, nil
	}
	return parseClientConfiguration(filePath)
}

func findClientConfig(existsChecker existsFileChecker, filePathFromConnectionString string) (string, error) {
	if filePathFromConnectionString != "" {
		return filePathFromConnectionString, nil
	}
	envConfigFilePath := os.Getenv(clientConfEnvName)
	if envConfigFilePath != "" {
		return envConfigFilePath, nil
	}
	return searchInDirectories(existsChecker)
}

func searchInDirectories(existsChecker existsFileChecker) (string, error) {
	// search in driver's directory
	filePath, err := searchInDirectory(existsChecker, ".")
	if err != nil {
		return "", findingClientConfigError(err)
	}
	if filePath != "" {
		return filePath, nil
	}
	// search in home directory
	var homeDir string
	homeDir, err = os.UserHomeDir()
	if err != nil {
		return "", findingClientConfigError(err)
	}
	filePath, err = searchInDirectory(existsChecker, homeDir)
	if err != nil {
		return "", findingClientConfigError(err)
	}
	if filePath != "" {
		return filePath, nil
	}
	// search in temporary directory
	filePath, err = searchInDirectory(existsChecker, os.TempDir())
	if err != nil {
		return "", findingClientConfigError(err)
	}
	return filePath, nil
}

func searchInDirectory(existsChecker existsFileChecker, directory string) (string, error) {
	filePath := defaultConfigFile(directory)
	exists, err := existsChecker.existsFile(filePath)
	if err != nil {
		return "", err
	}
	if exists {
		return filePath, nil
	}
	return "", nil
}

func defaultConfigFile(directory string) string {
	return path.Join(directory, defaultConfigName)
}

func findingClientConfigError(err error) error {
	return fmt.Errorf("finding client config failed: %w", err)
}
