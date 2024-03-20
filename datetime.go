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
