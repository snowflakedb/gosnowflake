package gosnowflake

import (
	"errors"
	"io"
	"os"
	"path"
	"strings"
)

type initTrials struct {
	everTriedToInitialize bool
	initParameter         string
	configureCounter      int
}

var easyLoggingInitTrials = initTrials{
	everTriedToInitialize: false,
	initParameter:         "",
	configureCounter:      0,
}

func (i *initTrials) setInitTrial(initParameter string) {
	i.everTriedToInitialize = true
	i.initParameter = initParameter
}

func (i *initTrials) increaseReconfigureCounter() {
	i.configureCounter++
}

func (i *initTrials) reset() {
	i.everTriedToInitialize = false
	i.initParameter = ""
	i.configureCounter = 0
}

func initEasyLogging(configFilePathFromConnectionString string) error {
	if !allowedToInitialize(configFilePathFromConnectionString) {
		return nil
	}
	config, err := getClientConfig(configFilePathFromConnectionString)
	if err != nil {
		return easyLoggingInitError(err)
	}
	if config == nil {
		easyLoggingInitTrials.setInitTrial(configFilePathFromConnectionString)
		return nil
	}
	var logLevel string
	logLevel, err = getLogLevel(config.Common.LogLevel)
	if err != nil {
		return easyLoggingInitError(err)
	}
	var logPath string
	logPath, err = getLogPath(config.Common.LogPath)
	if err != nil {
		return easyLoggingInitError(err)
	}
	reconfigureEasyLogging(logLevel, logPath)
	easyLoggingInitTrials.setInitTrial(configFilePathFromConnectionString)
	easyLoggingInitTrials.increaseReconfigureCounter()
	return nil
}

func easyLoggingInitError(err error) error {
	return &SnowflakeError{
		Number:      ErrCodeClientConfigFailed,
		Message:     errMsgClientConfigFailed,
		MessageArgs: []interface{}{err.Error()},
	}
}

func reconfigureEasyLogging(logLevel string, logPath string) error {
	err := logger.SetLogLevel(logLevel)
	if err != nil {
		return err
	}
	var output io.Writer
	var file *os.File
	output, file, err = createLogWriter(logPath)
	if err != nil {
		return err
	}
	logger.SetOutput(output)
	err = logger.CloseFileOnReset(file)
	if err != nil {
		logger.Errorf("%s", err)
	}
	return nil
}

func createLogWriter(logPath string) (io.Writer, *os.File, error) {
	if strings.EqualFold(logPath, "STDOUT") {
		return os.Stdout, nil, nil
	}
	logFileName := path.Join(logPath, "snowflake.log")
	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, nil, err
	}
	return io.MultiWriter(file, os.Stdout), file, nil
}

func allowedToInitialize(configFilePathFromConnectionString string) bool {
	triedToInitializeWithoutConfigFile := easyLoggingInitTrials.everTriedToInitialize && easyLoggingInitTrials.initParameter == ""
	isAllowedToInitialize := !easyLoggingInitTrials.everTriedToInitialize || (triedToInitializeWithoutConfigFile && configFilePathFromConnectionString != "")
	if !isAllowedToInitialize && easyLoggingInitTrials.initParameter != configFilePathFromConnectionString {
		logger.Warnf("Easy logging will not be configured for CLIENT_CONFIG_FILE=%s because it was previously configured for a different client config", configFilePathFromConnectionString)
	}
	return isAllowedToInitialize
}

func getLogLevel(logLevel string) (string, error) {
	if logLevel == "" {
		logger.Warn("LogLevel in client config not found. Using default value: OFF")
		return levelOff, nil
	}
	return toLogLevel(logLevel)
}

func getLogPath(logPath string) (string, error) {
	logPathOrDefault := logPath
	if logPath == "" {
		logPathOrDefault = os.TempDir()
		logger.Warnf("LogPath in client config not found. Using temporary directory as a default value: %s", logPathOrDefault)
	}
	pathWithGoSubdir := path.Join(logPathOrDefault, "go")
	exists, err := dirExists(pathWithGoSubdir)
	if err != nil {
		return "", err
	}
	if !exists {
		err = os.MkdirAll(pathWithGoSubdir, 0755)
		if err != nil {
			return "", err
		}
	}
	return pathWithGoSubdir, nil
}

func dirExists(dirPath string) (bool, error) {
	stat, err := os.Stat(dirPath)
	if err == nil {
		return stat.IsDir(), nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}
