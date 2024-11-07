package main

import (
	"bytes"
	sf "github.com/snowflakedb/gosnowflake"
	"log"
	"strings"
)

func main() {
	buf := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}

	var mylog = sf.GetLogger()
	mylog.SetOutput(buf)
	mylog.Info("Hello I am default")
	mylog.Info("Hello II amm default")
	mylog.Debug("Default I am debug NOT SHOWN")
	_ = mylog.SetLogLevel("debug")
	mylog.Debug("Default II amm debug TO SHOW")

	var testlog = sf.CreateDefaultLogger()
	_ = testlog.SetLogLevel("debug")
	testlog.SetOutput(buf)
	testlog.SetOutput(buf2)
	sf.SetLogger(&testlog)

	var mylog2 = sf.GetLogger()
	mylog2.Debug("test debug log is shown")
	_ = mylog2.SetLogLevel("info")
	mylog2.Debug("test debug log is not shownII")
	log.Print("Expect all true values:")

	// verify logger switch
	var strbuf = buf.String()
	log.Printf("%t:%t:%t:%t", strings.Contains(strbuf, "I am default"),
		strings.Contains(strbuf, "II amm default"),
		!strings.Contains(strbuf, "test debug log is shown"),
		strings.Contains(buf2.String(), "test debug log is shown"))

	// verify log level switch
	log.Printf("%t:%t:%t:%t", !strings.Contains(strbuf, "Default I am debug NOT SHOWN"),
		strings.Contains(strbuf, "Default II amm debug TO SHOW"),
		strings.Contains(buf2.String(), "test debug log is shown"),
		!strings.Contains(buf2.String(), "test debug log is not shownII"))

}
