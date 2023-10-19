package gosnowflake

import "testing"

func TestShowServerVersion(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQuery("SELECT CURRENT_VERSION()")
		defer rows.Close()

		var version string
		rows.Next()
		rows.Scan(&version)
		println(version)
	})
}
