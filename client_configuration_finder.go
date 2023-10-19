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
	configPredefinedFilePaths, err := clientConfigPredefinedDirs()
	var filePath string
	filePath, err = findClientConfigFilePath(filePathFromConnectionString, configPredefinedFilePaths)
	if err != nil {
		return nil, err
	}
	if filePath == "" { // we did not find a config file
		return nil, nil
	}
	return parseClientConfiguration(filePath)
}

func findClientConfigFilePath(filePathFromConnectionString string, configPredefinedDirs []string) (string, error) {
	if filePathFromConnectionString != "" {
		return filePathFromConnectionString, nil
	}
	envConfigFilePath := os.Getenv(clientConfEnvName)
	if envConfigFilePath != "" {
		return envConfigFilePath, nil
	}
	return searchForConfigFile(configPredefinedDirs)
}

func searchForConfigFile(directories []string) (string, error) {
	for _, dir := range directories {
		filePath := path.Join(dir, defaultConfigName)
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

func clientConfigPredefinedDirs() ([]string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	return []string{".", homeDir, os.TempDir()}, nil
}
