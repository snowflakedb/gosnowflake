// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"errors"
	"os"
	"path"
)

const (
	defaultConfigName = "sf_client_config.json"
	clientConfEnvName = "SF_CLIENT_CONFIG_FILE"
)

func getClientConfig(filePathFromConnectionString string) (*ClientConfig, error) {
	configPredefinedFilePaths, err := clientConfigPredefinedFilePaths()
	var filePath string
	filePath, err = findClientConfig(filePathFromConnectionString, configPredefinedFilePaths)
	if err != nil {
		return nil, err
	}
	if filePath == "" {
		return nil, nil
	}
	return parseClientConfiguration(filePath)
}

func findClientConfig(filePathFromConnectionString string, configPredefinedFilePaths []string) (string, error) {
	if filePathFromConnectionString != "" {
		return filePathFromConnectionString, nil
	}
	envConfigFilePath := os.Getenv(clientConfEnvName)
	if envConfigFilePath != "" {
		return envConfigFilePath, nil
	}
	return searchForFirstExistingFile(configPredefinedFilePaths)
}

func searchForFirstExistingFile(filePath []string) (string, error) {
	for _, filePath := range filePath {
		exists, err := existsFile(filePath)
		if err != nil {
			return "", err
		}
		if exists {
			return filePath, nil
		}
	}
	return "", nil
}

func existsFile(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func clientConfigPredefinedFilePaths() ([]string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	dirs := []string{
		path.Join(".", defaultConfigName),
		path.Join(homeDir, defaultConfigName),
		path.Join(os.TempDir(), defaultConfigName),
	}
	return dirs, nil
}
