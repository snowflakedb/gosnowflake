package arrowbatches

import (
	"cmp"
	"context"
	"github.com/snowflakedb/gosnowflake/v2/internal/query"
	"github.com/snowflakedb/gosnowflake/v2/internal/types"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"
	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ArrowBatch represents a chunk of data retrievable in arrow.Record format.
type ArrowBatch struct {
	raw       ia.BatchRaw
	rowTypes  []query.ExecResponseRowType
	allocator memory.Allocator
	ctx       context.Context
}

// WithContext sets the context for subsequent Fetch calls on this batch.
func (rb *ArrowBatch) WithContext(ctx context.Context) *ArrowBatch {
	rb.ctx = ctx
	return rb
}

// Fetch returns an array of arrow.Record representing this batch's data.
// Records are transformed from Snowflake's internal format to standard Arrow types.
func (rb *ArrowBatch) Fetch() (*[]arrow.Record, error) {
	var rawRecords *[]arrow.Record
	ctx := cmp.Or(rb.ctx, context.Background())

	if rb.raw.Records != nil {
		rawRecords = rb.raw.Records
	} else if rb.raw.Download != nil {
		recs, rowCount, err := rb.raw.Download(ctx)
		if err != nil {
			return nil, err
		}
		rawRecords = recs
		rb.raw.Records = recs
		rb.raw.RowCount = rowCount
	}

	if rawRecords == nil || len(*rawRecords) == 0 {
		empty := make([]arrow.Record, 0)
		return &empty, nil
	}

	var transformed []arrow.Record
	for i, rec := range *rawRecords {
		newRec, err := arrowToRecord(ctx, rec, rb.allocator, rb.rowTypes, rb.raw.Location)
		if err != nil {
			for _, t := range transformed {
				t.Release()
			}
			for _, r := range (*rawRecords)[i:] {
				r.Release()
			}
			rb.raw.Records = nil
			return nil, err
		}
		transformed = append(transformed, newRec)
		rec.Release()
	}
	rb.raw.Records = nil
	rb.raw.RowCount = countArrowBatchRows(&transformed)
	return &transformed, nil
}

// GetRowCount returns the number of rows in this batch.
func (rb *ArrowBatch) GetRowCount() int {
	return rb.raw.RowCount
}

// GetLocation returns the timezone location for this batch.
func (rb *ArrowBatch) GetLocation() *time.Location {
	return rb.raw.Location
}

// GetRowTypes returns the column metadata for this batch.
func (rb *ArrowBatch) GetRowTypes() []query.ExecResponseRowType {
	return rb.rowTypes
}

// ArrowSnowflakeTimestampToTime converts an original Snowflake timestamp to time.Time.
func (rb *ArrowBatch) ArrowSnowflakeTimestampToTime(rec arrow.Record, colIdx int, recIdx int) *time.Time {
	scale := int(rb.rowTypes[colIdx].Scale)
	dbType := rb.rowTypes[colIdx].Type
	return ArrowSnowflakeTimestampToTime(rec.Column(colIdx), types.GetSnowflakeType(dbType), scale, recIdx, rb.raw.Location)
}

// GetArrowBatches retrieves arrow batches from SnowflakeRows.
// The rows must have been queried with arrowbatches.WithArrowBatches(ctx).
func GetArrowBatches(rows sf.SnowflakeRows) ([]*ArrowBatch, error) {
	provider, ok := rows.(ia.BatchDataProvider)
	if !ok {
		return nil, &sf.SnowflakeError{
			Number:  sf.ErrNotImplemented,
			Message: "rows do not support arrow batch data",
		}
	}

	info, err := provider.GetArrowBatches()
	if err != nil {
		return nil, err
	}

	batches := make([]*ArrowBatch, len(info.Batches))
	for i, raw := range info.Batches {
		batches[i] = &ArrowBatch{
			raw:       raw,
			rowTypes:  info.RowTypes,
			allocator: info.Allocator,
			ctx:       info.Ctx,
		}
	}
	return batches, nil
}

func countArrowBatchRows(recs *[]arrow.Record) (cnt int) {
	for _, r := range *recs {
		cnt += int(r.NumRows())
	}
	return
}

// GetAllocator returns the memory allocator for this batch.
func (rb *ArrowBatch) GetAllocator() memory.Allocator {
	return rb.allocator
}
