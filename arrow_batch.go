//go:build !nobatch
// +build !nobatch

package gosnowflake

import (
	"github.com/apache/arrow-go/v18/arrow"
)

func (arc *arrowResultChunk) decodeArrowBatch(scd *snowflakeChunkDownloader) (*[]arrow.Record, error) {
	var records []arrow.Record
	defer arc.reader.Release()

	for arc.reader.Next() {
		rawRecord := arc.reader.Record()

		record, err := arrowToRecord(scd.ctx, rawRecord, arc.allocator, scd.RowSet.RowType, arc.loc)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	return &records, arc.reader.Err()
}
