package gosnowflake

import (
	"testing"
)

func TestShowServerVersion(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQuery("SELECT CURRENT_VERSION()")
		defer func(rows *RowsExtended) {
			err := rows.Close()
			assertNilF(t, err)
		}(rows)

		var version string
		rows.Next()
		err := rows.Scan(&version)
		assertNilF(t, err)
		println(version)
	})
}
