// Copyright (c) 2020-2020 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

type arrowResultChunk struct {
	reader           ipc.Reader
	rowCount         int
	uncompressedSize int
	allocator        memory.Allocator
}

func (arc *arrowResultChunk) decodeArrowChunk(ctx context.Context, rowType []execResponseRowType, highPrec bool) ([]chunkRowType, error) {
	logger.Debug("Arrow Decoder")
	var chunkRows []chunkRowType
	if arrowRecordChan := getArrowRecordChan(ctx); arrowRecordChan != nil {
		numRows, err := arc.writeToArrowChan(arrowRecordChan)
		if err != nil {
			return nil, err
		}
		chunkRows = append(chunkRows, make([]chunkRowType, numRows)...)
		return chunkRows, nil
	} else {
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
				if err = arrowToValue(&destcol, rowType[colIdx], col, highPrec); err != nil {
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
}

/**
Build arrow chunk based on RowSet of base64
*/
func buildFirstArrowChunk(rowsetBase64 string) arrowResultChunk {
	rowSetBytes, err := base64.StdEncoding.DecodeString(rowsetBase64)
	if err != nil {
		return arrowResultChunk{}
	}
	rr, err := ipc.NewReader(bytes.NewReader(rowSetBytes))
	if err != nil {
		return arrowResultChunk{}
	}

	return arrowResultChunk{*rr, 0, 0, memory.NewGoAllocator()}
}

/**
Writes []array.Record to array record channel. Returns number of rows from records written to channel
*/
func (arc *arrowResultChunk) writeToArrowChan(ch chan<- []array.Record) (int, error) {
	var numRows int
	var records []array.Record

	for {
		record, err := arc.reader.Read()
		if err == io.EOF {
			ch <- records
			return numRows, nil
		} else if err != nil {
			return numRows, err
		}

		record.Retain()
		records = append(records, record)

		currentRows := int(record.NumRows())
		numRows += currentRows
		arc.rowCount += currentRows
	}
}

func getArrowRecordChan(ctx context.Context) chan<- []array.Record {
	v := ctx.Value(arrowRecordChannel)
	if v == nil {
		return nil
	}
	if c, ok := v.(chan []array.Record); ok {
		return c
	}
	return nil
}
