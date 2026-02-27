package gosnowflake

import (
	"testing"
)

func TestShowServerVersion(t *testing.T) {
	runDBTestWithConfig(t, &testConfig{reuseConn: true}, func(dbt *DBTest) {
		rows := dbt.mustQuery("SELECT CURRENT_VERSION()")
		defer func() {
			assertNilF(t, rows.Close())
		}()

		var version string
		rows.Next()
		assertNilF(t, rows.Scan(&version))
		println(version)
	})
}
