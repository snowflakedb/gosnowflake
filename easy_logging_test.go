package gosnowflake

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestInitializeEasyLoggingOnlyOnceWhenConfigGivenAsAParameter(t *testing.T) {
	defer cleanUp()
	logDir := t.TempDir()
	logLevel := levelError
	contents := createClientConfigContent(logLevel, logDir)
	configFilePath := createFile(t, "config.json", contents, logDir)
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
}

func TestConfigureEasyLoggingOnlyOnceWhenInitializedWithoutConfigFilePath(t *testing.T) {
	appExe, err := os.Executable()
	assertNilF(t, err, "application exe not accessible")
	userHome, err := os.UserHomeDir()
	assertNilF(t, err, "user home directory not accessible")

	testcases := []struct {
		name string
		dir  string
	}{
		{
			name: "user home directory",
			dir:  userHome,
		},
		{
			name: "application directory",
			dir:  filepath.Dir(appExe),
		},
	}

	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			defer cleanUp()
			logDir := t.TempDir()
			assertNilF(t, err, "user home directory error")
			logLevel := levelError
			contents := createClientConfigContent(logLevel, logDir)
			configFilePath := createFile(t, defaultConfigName, contents, test.dir)
			defer os.Remove(configFilePath)
			easyLoggingInitTrials.reset()

			err = openWithClientConfigFile(t, "")
			assertNilF(t, err, "open config error")
			err = openWithClientConfigFile(t, "")
			assertNilF(t, err, "open config error")

			assertEqualE(t, toClientConfigLevel(logger.GetLogLevel()), logLevel, "error log level check")
			assertEqualE(t, easyLoggingInitTrials.configureCounter, 1)
		})
	}
}

func TestReconfigureEasyLoggingIfConfigPathWasNotGivenForTheFirstTime(t *testing.T) {
	defer cleanUp()
	configDir, err := os.UserHomeDir()
	logDir := t.TempDir()
	assertNilF(t, err, "user home directory error")
	homeConfigLogLevel := levelError
	homeConfigContent := createClientConfigContent(homeConfigLogLevel, logDir)
	homeConfigFilePath := createFile(t, defaultConfigName, homeConfigContent, configDir)
	defer os.Remove(homeConfigFilePath)
	customLogLevel := levelWarn
	customFileContent := createClientConfigContent(customLogLevel, logDir)
	customConfigFilePath := createFile(t, "config.json", customFileContent, configDir)
	easyLoggingInitTrials.reset()

	err = openWithClientConfigFile(t, "")
	logger.Error("Error message")

	assertNilF(t, err, "open config error")
	assertEqualE(t, toClientConfigLevel(logger.GetLogLevel()), homeConfigLogLevel, "tmp dir log level check")
	assertEqualE(t, easyLoggingInitTrials.configureCounter, 1)

	err = openWithClientConfigFile(t, customConfigFilePath)
	logger.Error("Warning message")

	assertNilF(t, err, "open config error")
	assertEqualE(t, toClientConfigLevel(logger.GetLogLevel()), customLogLevel, "custom dir log level check")
	assertEqualE(t, easyLoggingInitTrials.configureCounter, 2)
	var logContents []byte
	logContents, err = os.ReadFile(path.Join(logDir, "go", "snowflake.log"))
	assertNilF(t, err, "read file error")
	logs := notEmptyLines(string(logContents))
	assertEqualE(t, len(logs), 2, "number of logs")
}

func TestEasyLoggingFailOnUnknownLevel(t *testing.T) {
	defer cleanUp()
	dir := t.TempDir()
	easyLoggingInitTrials.reset()
	configContent := createClientConfigContent("something_unknown", dir)
	configFilePath := createFile(t, "config.json", configContent, dir)

	err := openWithClientConfigFile(t, configFilePath)

	assertNotNilF(t, err, "open config error")
	assertStringContainsE(t, err.Error(), fmt.Sprint(ErrCodeClientConfigFailed), "error code")
	assertStringContainsE(t, err.Error(), "parsing client config failed", "error message")
}

func TestEasyLoggingFailOnNotExistingConfigFile(t *testing.T) {
	defer cleanUp()
	easyLoggingInitTrials.reset()

	err := openWithClientConfigFile(t, "/not-existing-file.json")

	assertNotNilF(t, err, "open config error")
	assertStringContainsE(t, err.Error(), fmt.Sprint(ErrCodeClientConfigFailed), "error code")
	assertStringContainsE(t, err.Error(), "parsing client config failed", "error message")
}

func TestLogToConfiguredFile(t *testing.T) {
	defer cleanUp()
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
	logs := notEmptyLines(string(logContents))
	assertEqualE(t, len(logs), 3, "number of logs")
	errorLogs := filterStrings(logs, func(val string) bool {
		return strings.Contains(val, "level=error")
	})
	assertEqualE(t, len(errorLogs), 1, "error logs count")
	warningLogs := filterStrings(logs, func(val string) bool {
		return strings.Contains(val, "level=warning")
	})
	assertEqualE(t, len(warningLogs), 2, "warning logs count")
}

func TestDataRace(t *testing.T) {
	n := 10
	wg := sync.WaitGroup{}
	wg.Add(n)

	for range make([]int, n) {
		go func() {
			defer wg.Done()

			err := initEasyLogging("")
			assertNilF(t, err, "no error from db")
		}()
	}

	wg.Wait()
}

func notEmptyLines(lines string) []string {
	notEmptyFunc := func(val string) bool {
		return val != ""
	}
	return filterStrings(strings.Split(strings.ReplaceAll(lines, "\r\n", "\n"), "\n"), notEmptyFunc)
}

func cleanUp() {
	newLogger := CreateDefaultLogger()
	logger.Replace(&newLogger)
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

func filterStrings(values []string, keep func(string) bool) []string {
	var filteredStrings []string
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

func (i *initTrials) reset() {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.everTriedToInitialize = false
	i.clientConfigFileInput = ""
	i.configureCounter = 0
}
