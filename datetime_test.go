package gosnowflake

import (
	"testing"
	"time"
)

func TestSnowflakeFormatToGoFormatUnitTest(t *testing.T) {
	location, err := time.LoadLocation("Europe/Warsaw")
	assertNilF(t, err)
	someTime1 := time.Date(2024, time.January, 19, 3, 42, 33, 123456789, location)
	someTime2 := time.Date(1973, time.December, 5, 13, 5, 3, 987000000, location)
	testcases := []struct {
		inputFormat string
		output      string
		formatted1  string
		formatted2  string
	}{
		{
			inputFormat: "YYYY-MM-DD HH24:MI:SS.FF TZH:TZM",
			output:      "2006-01-02 15:04:05.000000000 Z07:00",
			formatted1:  "2024-01-19 03:42:33.123456789 +01:00",
			formatted2:  "1973-12-05 13:05:03.987000000 +01:00",
		},
		{
			inputFormat: "YY-MM-DD HH12:MI:SS,FF5AM TZHTZM",
			output:      "06-01-02 03:04:05,00000PM Z0700",
			formatted1:  "24-01-19 03:42:33,12345AM +0100",
			formatted2:  "73-12-05 01:05:03,98700PM +0100",
		},
		{
			inputFormat: "MMMM DD, YYYY DY HH24:MI:SS.FF9 TZH:TZM",
			output:      "January 02, 2006 Mon 15:04:05.000000000 Z07:00",
			formatted1:  "January 19, 2024 Fri 03:42:33.123456789 +01:00",
			formatted2:  "December 05, 1973 Wed 13:05:03.987000000 +01:00",
		},
		{
			inputFormat: "MON DD, YYYY HH12:MI:SS,FF9PM TZH:TZM",
			output:      "Jan 02, 2006 03:04:05,000000000PM Z07:00",
			formatted1:  "Jan 19, 2024 03:42:33,123456789AM +01:00",
			formatted2:  "Dec 05, 1973 01:05:03,987000000PM +01:00",
		},
		{
			inputFormat: "HH24:MI:SS.FF3 HH12:MI:SS,FF9",
			output:      "15:04:05.000 03:04:05,000000000",
			formatted1:  "03:42:33.123 03:42:33,123456789",
			formatted2:  "13:05:03.987 01:05:03,987000000",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.inputFormat, func(t *testing.T) {
			goFormat, err := snowflakeFormatToGoFormat(tc.inputFormat)
			assertNilF(t, err)
			assertEqualE(t, tc.output, goFormat)
			assertEqualE(t, tc.formatted1, someTime1.Format(goFormat))
			assertEqualE(t, tc.formatted2, someTime2.Format(goFormat))
		})
	}
}

func TestIncorrectSecondsFraction(t *testing.T) {
	_, err := snowflakeFormatToGoFormat("HH24 MI SS FF")
	assertHasPrefixE(t, err.Error(), "incorrect second fraction")
}

func TestSnowflakeFormatToGoFormatIntegrationTest(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("ALTER SESSION SET TIME_OUTPUT_FORMAT = 'HH24:MI:SS.FF'")
		dbt.mustExec("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM'")
		dbt.mustExec("ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'")
		for _, forceFormat := range []string{forceJSON, forceARROW} {
			dbt.mustExec(forceFormat)

			for _, tc := range []struct {
				sfType          string
				formatParamName string
				sfFunction      string
			}{
				{
					sfType:          "TIMESTAMPLTZ",
					formatParamName: "TIMESTAMP_OUTPUT_FORMAT",
					sfFunction:      "CURRENT_TIMESTAMP",
				},
				{
					sfType:          "TIMESTAMPTZ",
					formatParamName: "TIMESTAMP_OUTPUT_FORMAT",
					sfFunction:      "CURRENT_TIMESTAMP",
				},
				{
					sfType:          "TIMESTAMPNTZ",
					formatParamName: "TIMESTAMP_NTZ_OUTPUT_FORMAT",
					sfFunction:      "CURRENT_TIMESTAMP",
				},
				{
					sfType:          "DATE",
					formatParamName: "DATE_OUTPUT_FORMAT",
					sfFunction:      "CURRENT_DATE",
				},
				{
					sfType:          "TIME",
					formatParamName: "TIME_OUTPUT_FORMAT",
					sfFunction:      "CURRENT_TIME",
				},
			} {
				t.Run(tc.sfType+"___"+forceFormat, func(t *testing.T) {
					params := dbt.mustQuery("show parameters like '" + tc.formatParamName + "'")
					defer params.Close()
					params.Next()
					defaultTimestampOutputFormat, err := ScanSnowflakeParameter(params.rows)
					assertNilF(t, err)

					rows := dbt.mustQuery("SELECT " + tc.sfFunction + "()::" + tc.sfType + ", " + tc.sfFunction + "()::" + tc.sfType + "::varchar")
					defer rows.Close()
					var t1 time.Time
					var t2 string
					rows.Next()
					err = rows.Scan(&t1, &t2)
					assertNilF(t, err)
					goFormat, err := snowflakeFormatToGoFormat(defaultTimestampOutputFormat.Value)
					assertNilF(t, err)
					assertEqualE(t, t1.Format(goFormat), t2)
					parseResult, err := time.Parse(goFormat, t2)
					assertNilF(t, err)
					if tc.sfType != "TIME" {
						assertEqualE(t, t1.UTC(), parseResult.UTC())
					} else {
						assertEqualE(t, t1.Hour(), parseResult.Hour())
						assertEqualE(t, t1.Minute(), parseResult.Minute())
						assertEqualE(t, t1.Second(), parseResult.Second())
					}
				})
			}
		}
	})
}
