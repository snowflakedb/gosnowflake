package gosnowflake

import (
	"context"
	"database/sql/driver"
	"testing"

	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"
)

func TestChunkDownloaderDoesNotStartWhenArrowParsingCausesError(t *testing.T) {
	tcs := []string{
		"invalid base64",
		"aW52YWxpZCBhcnJvdw==", // valid base64, but invalid arrow
	}
	for _, tc := range tcs {
		t.Run(tc, func(t *testing.T) {
			scd := snowflakeChunkDownloader{
				ctx:               context.Background(),
				QueryResultFormat: "arrow",
				RowSet: rowSetType{
					RowSetBase64: tc,
				},
			}

			err := scd.start()

			assertNotNilF(t, err)
		})
	}
}

func TestWithArrowBatchesWhenQueryReturnsNoRowsWhenUsingNativeGoSQLInterface(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		var rows driver.Rows
		var err error
		err = dbt.conn.Raw(func(x interface{}) error {
			rows, err = x.(driver.QueryerContext).QueryContext(ia.EnableArrowBatches(context.Background()), "SELECT 1 WHERE 0 = 1", nil)
			return err
		})
		assertNilF(t, err)
		rows.Close()
	})
}

func TestWithArrowBatchesWhenQueryReturnsRowsAndReadingRows(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(ia.EnableArrowBatches(context.Background()), "SELECT 1")
		defer rows.Close()
		assertFalseF(t, rows.Next())
	})
}

func TestWithArrowBatchesWhenQueryReturnsNoRowsAndReadingRows(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(ia.EnableArrowBatches(context.Background()), "SELECT 1 WHERE 1 = 0")
		defer rows.Close()
		assertFalseF(t, rows.Next())
	})
}

func TestWithArrowBatchesWhenQueryReturnsNoRowsAndReadingArrowBatchData(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		var rows driver.Rows
		var err error
		err = dbt.conn.Raw(func(x any) error {
			rows, err = x.(driver.QueryerContext).QueryContext(ia.EnableArrowBatches(context.Background()), "SELECT 1 WHERE 1 = 0", nil)
			return err
		})
		assertNilF(t, err)
		defer rows.Close()
		provider := rows.(SnowflakeRows).(ia.BatchDataProvider)
		info, err := provider.GetArrowBatches()
		assertNilF(t, err)
		assertEmptyE(t, info.Batches)
	})
}
