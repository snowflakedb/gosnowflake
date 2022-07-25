// Copyright (c) 2020-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"encoding/base64"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

type arrowResultChunk struct {
	reader           ipc.Reader
	rowCount         int
	uncompressedSize int
	allocator        memory.Allocator
	loc              *time.Location
}

func (arc *arrowResultChunk) decodeArrowChunk(rowType []execResponseRowType, highPrec bool) ([]chunkRowType, error) {
	logger.Debug("Arrow Decoder")
	var chunkRows []chunkRowType

	for {
		record, err := arc.reader.Read()
		if err == io.EOF {
			return chunkRows, nil
		} else if err != nil {
			return nil, err
		}

		numRows := int(record.NumRows())
		columns := record.Columns()
		tmpRows := make([]chunkRowType, numRows)

		for colIdx, col := range columns {
			destcol := make([]snowflakeValue, numRows)
			if err = arrowToValue(&destcol, rowType[colIdx], col, arc.loc, highPrec); err != nil {
				return nil, err
			}

			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				if colIdx == 0 {
					tmpRows[rowIdx] = chunkRowType{ArrowRow: make([]snowflakeValue, len(columns))}
				}
				tmpRows[rowIdx].ArrowRow[colIdx] = destcol[rowIdx]
			}
		}
		chunkRows = append(chunkRows, tmpRows...)
		arc.rowCount += numRows
	}
}

func (arc *arrowResultChunk) decodeArrowBatch(scd *snowflakeChunkDownloader) (*[]array.Record, error) {
	var records []array.Record

	for {
		rawRecord, err := arc.reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		record, err := arrowToRecord(rawRecord, scd.RowSet.RowType, arc.loc)
		rawRecord.Release()
		if err != nil {
			return nil, err
		}
		record.Retain()
		records = append(records, record)
	}
	return &records, nil
}

// Note(Qing): Previously, the gosnowflake driver decodes the raw arrow chunks fetched from snowflake by
// calling the decodeArrowBatch() function above. Instead of decoding here, we directly pass the raw records
// to evaluator, along with neccesary metadata needed.
func (arc *arrowResultChunk) passRawArrowBatch(scd *snowflakeChunkDownloader) (*[]array.Record, error) {
	var records []array.Record

	for {
		rawRecord, err := arc.reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		// Here we check all metadata from snowflake are preserved, so evaluator can decode accordingly
		for idx, field := range rawRecord.Schema().Fields() {
			if field.Nullable != scd.RowSet.RowType[idx].Nullable ||
				field.Name != scd.RowSet.RowType[idx].Name ||
				!compareMetadata(field.Metadata, scd.RowSet.RowType[idx]) {
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

func compareMetadata(actual arrow.Metadata, expected execResponseRowType) bool {
	for idx, key := range actual.Keys() {
		switch strings.ToUpper(key) {
		case "LOGICALTYPE":
			if !strings.EqualFold(actual.Values()[idx], expected.Type) {
				return false
			}
		case "PRECISION":
			if i64, err := strconv.ParseInt(actual.Values()[idx], 10, 64); err != nil || i64 != expected.Precision {
				return false
			}
		case "SCALE":
			if i64, err := strconv.ParseInt(actual.Values()[idx], 10, 64); err != nil || i64 != expected.Scale {
				return false
			}
		case "BYTELENGTH":
			if i64, err := strconv.ParseInt(actual.Values()[idx], 10, 64); err != nil || i64 != expected.ByteLength {
				return false
			}
		default:
		}
	}
	return true
}

// Build arrow chunk based on RowSet of base64
func buildFirstArrowChunk(rowsetBase64 string, loc *time.Location) arrowResultChunk {
	rowSetBytes, err := base64.StdEncoding.DecodeString(rowsetBase64)
	if err != nil {
		return arrowResultChunk{}
	}
	rr, err := ipc.NewReader(bytes.NewReader(rowSetBytes))
	if err != nil {
		return arrowResultChunk{}
	}

	return arrowResultChunk{*rr, 0, 0, memory.NewGoAllocator(), loc}
}
