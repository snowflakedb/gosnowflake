// Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

var incorrectSecondsFractionRegex = regexp.MustCompile(`[^.,]FF`)
var correctSecondsFractionRegex = regexp.MustCompile(`FF(?P<fraction>\d?)`)

type formatReplacement struct {
	input  string
	output string
}

var formatReplacements = []formatReplacement{
	{input: "YYYY", output: "2006"},
	{input: "YY", output: "06"},
	{input: "MMMM", output: "January"},
	{input: "MM", output: "01"},
	{input: "MON", output: "Jan"},
	{input: "DD", output: "02"},
	{input: "DY", output: "Mon"},
	{input: "HH24", output: "15"},
	{input: "HH12", output: "03"},
	{input: "AM", output: "PM"},
	{input: "MI", output: "04"},
	{input: "SS", output: "05"},
	{input: "TZH", output: "Z07"},
	{input: "TZM", output: "00"},
}

func snowflakeFormatToGoFormat(sfFormat string) (string, error) {
	res := sfFormat
	for _, replacement := range formatReplacements {
		res = strings.Replace(res, replacement.input, replacement.output, -1)
	}

	if incorrectSecondsFractionRegex.MatchString(res) {
		return "", errors.New("incorrect second fraction - golang requires fraction to be preceded by comma or decimal point")
	}
	for {
		submatch := correctSecondsFractionRegex.FindStringSubmatch(res)
		if submatch == nil {
			break
		}
		fractionNumbers := 9
		if submatch[1] != "" {
			var err error
			fractionNumbers, err = strconv.Atoi(submatch[1])
			if err != nil {
				return "", err
			}
		}
		res = strings.Replace(res, submatch[0], strings.Repeat("0", fractionNumbers), -1)
	}
	return res, nil
}

func dateTimeFormatByType(dateTimeType string, params map[string]*string) (string, error) {
	paramsMutex.Lock()
	defer paramsMutex.Unlock()
	var format *string
	switch dateTimeType {
	case "date":
		format = params["date_output_format"]
	case "time":
		format = params["time_output_format"]
	case "timestamp_ltz":
		format = params["timestamp_ltz_output_format"]
		if format == nil || *format == "" {
			format = params["timestamp_output_format"]
		}
	case "timestamp_tz":
		format = params["timestamp_tz_output_format"]
		if format == nil || *format == "" {
			format = params["timestamp_output_format"]
		}
	case "timestamp_ntz":
		format = params["timestamp_ntz_output_format"]
		if format == nil || *format == "" {
			format = params["timestamp_output_format"]
		}
	}
	if format != nil {
		return *format, nil
	}
	return "", errors.New("not known format parameter for " + dateTimeType)
}
