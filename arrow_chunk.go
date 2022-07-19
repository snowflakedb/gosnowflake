// Copyright (c) 2020-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"encoding/base64"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type arrowResultChunk struct {
	reader    *ipc.Reader
	rowCount  int
	loc       *time.Location
	allocator memory.Allocator
}

func (arc *arrowResultChunk) decodeArrowChunk(rowType []execResponseRowType, highPrec bool) ([]chunkRowType, error) {
	logger.Debug("Arrow Decoder")
	var chunkRows []chunkRowType

	for arc.reader.Next() {
		record := arc.reader.Record()

		start := len(chunkRows)
		numRows := int(record.NumRows())
		columns := record.Columns()
		chunkRows = append(chunkRows, make([]chunkRowType, numRows)...)
		for i := start; i < start+numRows; i++ {
			chunkRows[i].ArrowRow = make([]snowflakeValue, len(columns))
		}

		for colIdx, col := range columns {
			values := make([]snowflakeValue, numRows)
			if err := arrowToValue(values, rowType[colIdx], col, arc.loc, highPrec); err != nil {
				return nil, err
			}

			for i := range values {
				chunkRows[start+i].ArrowRow[colIdx] = values[i]
			}
		}
		arc.rowCount += numRows
	}

	return chunkRows, arc.reader.Err()
}

func (arc *arrowResultChunk) decodeArrowBatch(scd *snowflakeChunkDownloader) (*[]arrow.Record, error) {
	var records []arrow.Record
	defer arc.reader.Release()

	for arc.reader.Next() {
		rawRecord := arc.reader.Record()

		record, err := arrowToRecord(rawRecord, arc.allocator, scd.RowSet.RowType, arc.loc)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	return &records, arc.reader.Err()
}

// Note(Qing): Previously, the gosnowflake driver decodes the raw arrow chunks fetched from snowflake by
// calling the decodeArrowBatch() function above. Instead of decoding here, we directly pass the raw records
// to evaluator, along with neccesary metadata needed.
func (arc *arrowResultChunk) passRawArrowBatch(scd *snowflakeChunkDownloader) (*[]arrow.Record, error) {
	var records []arrow.Record

	for {
		rawRecord, err := arc.reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		// Here we check all metadata from snowflake are preserved, so evaluator can decode accordingly
		for idx, field := range rawRecord.Schema().Fields() {
			// NOTE(Qing): Sometimes we see the rowtype metadata specify nullable as false but then still
			// reveive nullable arrow records. Given that, we do not check the nullability here. Also, no
			// need to compare names.
			if !checkMetadata(field.Metadata, scd.RowSet.RowType[idx]) {
				logger.Error("Lack or mismatch of necessary metadata to decode fetched raw arrow records")
				return nil, &SnowflakeError{
					Message: "Lack or mismatch of necessary metadata to decode fetched raw arrow records",
				}
			}
		}
		rawRecord.Retain()
		records = append(records, rawRecord)
	}
	return &records, nil
}

func checkMetadata(actual arrow.Metadata, expected execResponseRowType) bool {
	// LogicalType seems to be the only REALLY necessary metadata.
	var hasLogicalType bool

	for idx, key := range actual.Keys() {
		switch strings.ToUpper(key) {
		case "LOGICALTYPE":
			hasLogicalType = true
			if !strings.EqualFold(actual.Values()[idx], expected.Type) {
				return false
			}
		case "SCALE":
			switch strings.ToUpper(expected.Type) {
			case "FIXED", "TIME", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ":
				if i64, err := strconv.ParseInt(actual.Values()[idx], 10, 64); err != nil || i64 != expected.Scale {
					return false
				}
			default:
			}
		default:
		}
	}
	return hasLogicalType
}

// Build arrow chunk based on RowSet of base64
func buildFirstArrowChunk(rowsetBase64 string, loc *time.Location, alloc memory.Allocator) arrowResultChunk {
	rowSetBytes, err := base64.StdEncoding.DecodeString(rowsetBase64)
	if err != nil {
		return arrowResultChunk{}
	}
	rr, err := ipc.NewReader(bytes.NewReader(rowSetBytes), ipc.WithAllocator(alloc))
	if err != nil {
		return arrowResultChunk{}
	}

	return arrowResultChunk{rr, 0, loc, alloc}
}
