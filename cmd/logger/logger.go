package main

import (
	"bytes"
	rlog "github.com/sirupsen/logrus"
	sf "github.com/snowflakedb/gosnowflake"
	"log"
	"strings"
)

type testLogger struct {
	rlog.Logger
}

func (log *testLogger) SetLogLevel(level string) error {
	actualLevel, err := rlog.ParseLevel(level)
	if err != nil {
		return err
	}
	log.Level = actualLevel
	return nil
}

func getLogger() *testLogger {
	var logging = testLogger{*rlog.New()}
	var formatter = rlog.JSONFormatter{CallerPrettyfier: sf.SFCallerPrettyfier}
	logging.SetReportCaller(true)
	logging.SetFormatter(&formatter)
	return &logging
}

func main() {
	buf := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}

	sf.GetLogger().SetOutput(buf)
	sf.GetLogger().Info("Hello I am default")
	sf.GetLogger().Info("Hello II amm default")
	sf.GetLogger().Debug("Default I am debug NOT SHOWN")
	sf.GetLogger().SetLogLevel("debug")
	sf.GetLogger().Debug("Default II amm debug TO SHOW")

	var testlg = getLogger()
	testlg.SetLogLevel("debug")
	testlg.SetOutput(buf2)
	sf.SetLogger(testlg)
	sf.GetLogger().Debug("test debug log is shown")
	sf.GetLogger().SetLogLevel("info")
	sf.GetLogger().Debug("test debug log is not shownII")
	log.Print("Expect all true values:")

	// verify logger switch
	log.Printf("%t:%t:%t:%t", strings.Contains(buf.String(), "I am default"),
		strings.Contains(buf.String(), "II amm default"),
		!strings.Contains(buf.String(), "test debug log is shown"),
		strings.Contains(buf2.String(), "test debug log is shown"))

	// verify log level switch
	log.Printf("%t:%t:%t:%t", !strings.Contains(buf.String(), "Default I am debug NOT SHOWN"),
		strings.Contains(buf.String(), "Default II amm debug TO SHOW"),
		strings.Contains(buf2.String(), "test debug log is shown"),
		!strings.Contains(buf2.String(), "test debug log is not shownII"))

}
