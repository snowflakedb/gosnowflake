package gosnowflake

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
)

func TestInitializeEasyLoggingOnlyOnceWhenConfigGivenAsAParameter(t *testing.T) {
	dir := t.TempDir()
	logLevel := levelError
	contents := createClientConfigContent(logLevel, dir)
	configFilePath := createFile(t, "config.json", contents, dir)
	easyLoggingInitTrials.reset()

	err := openWithClientConfigFile(t, configFilePath)

	assertNilF(t, err, "open config error")
	assertEqualE(t, toClientConfigLevel(logger.GetLogLevel()), logLevel, "error log level check")
	assertEqualE(t, easyLoggingInitTrials.configureCounter, 1)

	err = openWithClientConfigFile(t, "")
	assertNilF(t, err, "open config error")
	err = openWithClientConfigFile(t, configFilePath)
	assertNilF(t, err, "open config error")
	err = openWithClientConfigFile(t, "/another-config.json")
	assertNilF(t, err, "open config error")

	assertEqualE(t, toClientConfigLevel(logger.GetLogLevel()), logLevel, "error log level check")
	assertEqualE(t, easyLoggingInitTrials.configureCounter, 1)

	cleanUp()
}

func TestConfigureEasyLoggingOnlyOnceWhenInitializedWithoutConfigFilePath(t *testing.T) {
	dir := t.TempDir()
	logLevel := levelError
	contents := createClientConfigContent(logLevel, dir)
	configFilePath := createFile(t, defaultConfigName, contents, os.TempDir())
	defer os.Remove(configFilePath)
	easyLoggingInitTrials.reset()

	err := openWithClientConfigFile(t, "")
	assertNilF(t, err, "open config error")
	err = openWithClientConfigFile(t, "")
	assertNilF(t, err, "open config error")

	assertEqualE(t, toClientConfigLevel(logger.GetLogLevel()), logLevel, "error log level check")
	assertEqualE(t, easyLoggingInitTrials.configureCounter, 1)

	cleanUp()
}

func TestReconfigureEasyLoggingIfConfigPathWasNotGivenForTheFirstTime(t *testing.T) {
	dir := t.TempDir()
	tmpDirLogLevel := levelError
	tmpFileContent := createClientConfigContent(tmpDirLogLevel, dir)
	tmpDirConfigFilePath := createFile(t, defaultConfigName, tmpFileContent, os.TempDir())
	defer os.Remove(tmpDirConfigFilePath)
	customLogLevel := levelWarn
	customFileContent := createClientConfigContent(customLogLevel, dir)
	customConfigFilePath := createFile(t, "config.json", customFileContent, dir)
	easyLoggingInitTrials.reset()

	err := openWithClientConfigFile(t, "")

	assertNilF(t, err, "open config error")
	assertEqualE(t, toClientConfigLevel(logger.GetLogLevel()), tmpDirLogLevel, "tmp dir log level check")
	assertEqualE(t, easyLoggingInitTrials.configureCounter, 1)

	err = openWithClientConfigFile(t, customConfigFilePath)
	assertNilF(t, err, "open config error")
	assertEqualE(t, toClientConfigLevel(logger.GetLogLevel()), customLogLevel, "custom dir log level check")
	assertEqualE(t, easyLoggingInitTrials.configureCounter, 2)

	cleanUp()
}

func TestEasyLoggingFailOnUnknownLevel(t *testing.T) {
	dir := t.TempDir()
	easyLoggingInitTrials.reset()
	configContent := createClientConfigContent("something_unknown", dir)
	configFilePath := createFile(t, "config.json", configContent, dir)

	err := openWithClientConfigFile(t, configFilePath)

	assertNotNilF(t, err, "open config error")
	assertStringContainsE(t, fmt.Sprintf("%s", err), "parsing client config failed", "error message")

	cleanUp()
}

func TestEasyLoggingFailOnNotExistingConfigFile(t *testing.T) {
	easyLoggingInitTrials.reset()

	err := openWithClientConfigFile(t, "/not-existing-file.json")
	assertNotNilF(t, err, "open config error")

	cleanUp()
}

func TestLogToConfiguredFile(t *testing.T) {
	dir := t.TempDir()
	easyLoggingInitTrials.reset()
	configContent := createClientConfigContent(levelWarn, dir)
	configFilePath := createFile(t, "config.json", configContent, dir)
	logFilePath := path.Join(dir, "go", "snowflake.log")
	err := openWithClientConfigFile(t, configFilePath)
	assertNilF(t, err, "open config error")

	logger.Error("Error message")
	logger.Warn("Warning message")
	logger.Warning("Warning message")
	logger.Info("Info message")
	logger.Trace("Trace message")

	var logContents []byte
	logContents, err = os.ReadFile(logFilePath)
	assertNilF(t, err, "read file error")
	logs := filterStrings(strings.Split(strings.ReplaceAll(string(logContents), "\r\n", "\n"), "\n"), notEmpty)
	assertEqualE(t, len(logs), 3, "number of logs")
	errorLogs := filterStrings(logs, func(val string) bool {
		return strings.Contains(val, "level=error")
	})
	assertEqualE(t, len(errorLogs), 1, "error logs count")
	warningLogs := filterStrings(logs, func(val string) bool {
		return strings.Contains(val, "level=warning")
	})
	assertEqualE(t, len(warningLogs), 2, "warning logs count")

	cleanUp()
}

func cleanUp() {
	logger.Reset()
	easyLoggingInitTrials.reset()
}

func toClientConfigLevel(logLevel string) string {
	logLevelUpperCase := strings.ToUpper(logLevel)
	switch strings.ToUpper(logLevel) {
	case "WARNING":
		return levelWarn
	case levelOff, levelError, levelWarn, levelInfo, levelDebug, levelTrace:
		return logLevelUpperCase
	default:
		return ""
	}
}

func notEmpty(val string) bool {
	return val != ""
}

func filterStrings(values []string, keep func(string) bool) []string {
	filteredStrings := []string{}
	for _, val := range values {
		if keep(val) {
			filteredStrings = append(filteredStrings, val)
		}
	}
	return filteredStrings
}

func defaultConfig(t *testing.T) *Config {
	config, err := ParseDSN(dsn)
	assertNilF(t, err, "parse dsn error")
	return config
}

func openWithClientConfigFile(t *testing.T, clientConfigFile string) error {
	driver := SnowflakeDriver{}
	config := defaultConfig(t)
	config.ClientConfigFile = clientConfigFile
	_, err := driver.OpenWithConfig(context.Background(), *config)
	return err
}
