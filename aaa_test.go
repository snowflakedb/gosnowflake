package gosnowflake

import (
	"testing"
)

func TestShowServerVersion(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQuery("SELECT CURRENT_VERSION()")
		defer func() {
			assertNilF(t, rows.Close())
		}()

		var version string
		rows.Next()
		assertNilF(t, rows.Scan(&version))
		println(version)

		dbt.mustExec("ALTER SESSION SET FEATURE_DECFLOAT = enabled")
		dbt.mustExec("CREATE TABLE test_decfloat (d DECFLOAT)")
	})
}
