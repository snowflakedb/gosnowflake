// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	Off   string = "OFF"
	Error string = "ERROR"
	Warn  string = "WARN"
	Info  string = "INFO"
	Debug string = "DEBUG"
	Trace string = "TRACE"
)

type ClientConfig struct {
	Common *ClientConfigCommonProps `json:common`
}

type ClientConfigCommonProps struct {
	LogLevel *string `json:"log_level"`
	LogPath  *string `json:"log_path"`
}

func parseClientConfiguration(filePath string) (*ClientConfig, error) {
	if filePath == "" {
		return nil, nil
	}
	fileContents, readError := os.ReadFile(filePath)
	if readError != nil {
		return nil, parsingClientConfigError(readError)
	}
	var clientConfig ClientConfig
	parseError := json.Unmarshal(fileContents, &clientConfig)
	if parseError != nil {
		return nil, parsingClientConfigError(parseError)
	}
	validateError := validateClientConfiguration(&clientConfig)
	if validateError != nil {
		return nil, parsingClientConfigError(validateError)
	}
	return &clientConfig, nil
}

func parsingClientConfigError(err error) error {
	return fmt.Errorf("parsing client config failed: %w", err)
}

func validateClientConfiguration(clientConfig *ClientConfig) error {
	if clientConfig == nil {
		return errors.New("client config not found")
	}
	if clientConfig.Common == nil {
		return errors.New("common section in client config not found")
	}
	return validateLogLevel(clientConfig)
}

func validateLogLevel(clientConfig *ClientConfig) error {
	var logLevel = clientConfig.Common.LogLevel
	if logLevel != nil && *logLevel != "" {
		_, error := toLogLevel(*logLevel)
		if error != nil {
			return error
		}
	}
	return nil
}

func toLogLevel(logLevelString string) (*string, error) {
	var logLevel = strings.ToUpper(logLevelString)
	switch logLevel {
	case Off, Error, Warn, Info, Debug, Trace:
		return &logLevel, nil
	default:
		return nil, errors.New("unknown log level: " + logLevelString)
	}
}
