// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// log levels for easy logging
const (
	Off   string = "OFF"   // log level for logging switched off
	Error string = "ERROR" // error log level
	Warn  string = "WARN"  // warn log level
	Info  string = "INFO"  // info log level
	Debug string = "DEBUG" // debug log level
	Trace string = "TRACE" // trace log level
)

// ClientConfig config root
type ClientConfig struct {
	Common *ClientConfigCommonProps `json:"common"`
}

// ClientConfigCommonProps properties from "common" section
type ClientConfigCommonProps struct {
	LogLevel *string `json:"log_level"`
	LogPath  *string `json:"log_path"`
}

func parseClientConfiguration(filePath string) (*ClientConfig, error) {
	if filePath == "" {
		return nil, nil
	}
	fileContents, readErr := os.ReadFile(filePath)
	if readErr != nil {
		return nil, parsingClientConfigError(readErr)
	}
	var clientConfig ClientConfig
	parseErr := json.Unmarshal(fileContents, &clientConfig)
	if parseErr != nil {
		return nil, parsingClientConfigError(parseErr)
	}
	validateErr := validateClientConfiguration(&clientConfig)
	if validateErr != nil {
		return nil, parsingClientConfigError(validateErr)
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
